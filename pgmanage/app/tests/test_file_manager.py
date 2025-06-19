import os
from unittest.mock import mock_open, patch

from app.views.file_manager import (
    create,
    delete,
    download,
    get_directory,
    rename,
    upload,
)
from django.conf import settings
from django.contrib.auth.models import User
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client, TestCase
from django.urls import resolve, reverse


class FileManagerViewsTests(TestCase):
    def setUp(self):
        # Create a test user
        self.user = User.objects.create_user(username="testuser", password="testpass")
        self.client = Client()
        self.client.login(username="testuser", password="testpass")
        self.storage_path = os.path.join("user_data", self.user.username)

    @patch("app.file_manager.file_manager.FileManager.create")
    def test_create(self, mock_create):
        data = {"path": "test_dir", "name": "test_file.txt", "type": "file"}
        response = self.client.post(
            reverse("create_file_or_directory"), data, content_type="application/json"
        )
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json(), {"data": "created"})
        mock_create.assert_called_once_with(data["path"], data["name"], data["type"])

    def test_create_invalid_data(self):
        data = {"name": "test_file.txt", "type": "file"}  # Missing "path"
        response = self.client.post(
            reverse("create_file_or_directory"), data, content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_create_url_resolves_create_view(self):
        view = resolve("/file_manager/create/")

        self.assertEqual(view.func.__name__, create.__name__)

    @patch("app.file_manager.file_manager.FileManager.get_directory_content")
    def test_get_directory(self, mock_get_directory_content):
        mock_get_directory_content.return_value = {"files": []}
        data = {"current_path": "test_dir"}
        response = self.client.post(
            reverse("get_directory"), data, content_type="application/json"
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"files": []})
        mock_get_directory_content.assert_called_once_with(data["current_path"])

    @patch("app.file_manager.file_manager.FileManager.get_parent_directory_content")
    def test_get_parent_directory(self, mock_get_parent_directory_content):
        mock_get_parent_directory_content.return_value = {"files": []}
        data = {"current_path": "test_dir", "parent_dir": True}
        response = self.client.post(
            reverse("get_directory"), data, content_type="application/json"
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"files": []})
        mock_get_parent_directory_content.assert_called_once_with(data["current_path"])

    @patch("app.file_manager.file_manager.FileManager.get_directory_content")
    def test_get_directory_no_path(self, mock_get_directory_content):
        mock_get_directory_content.return_value = {"files": []}
        data = {}
        response = self.client.post(
            reverse("get_directory"), data, content_type="application/json"
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"files": []})
        mock_get_directory_content.assert_called_once_with(None)

    @patch("app.file_manager.file_manager.FileManager.get_directory_content")
    def test_get_directory_raises_error(self, mock_get_directory_content):
        mock_get_directory_content.side_effect = Exception("Test error")
        data = {"current_path": "test_dir"}
        response = self.client.post(
            reverse("get_directory"), data, content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"data": "Test error"})

    def test_get_directory_url_resolves_get_directory_view(self):
        view = resolve("/file_manager/get_directory/")

        self.assertEqual(view.func.__name__, get_directory.__name__)

    @patch("app.file_manager.file_manager.FileManager.rename")
    def test_rename(self, mock_rename):
        data = {"path": "test_dir/test_file.txt", "name": "new_name.txt"}
        response = self.client.post(
            reverse("rename_file_or_directory"), data, content_type="application/json"
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"data": "success"})
        mock_rename.assert_called_once_with(data["path"], data["name"])

    @patch("app.file_manager.file_manager.FileManager.rename")
    def test_rename_raises_error(self, mock_rename):
        mock_rename.side_effect = Exception("Test error")
        data = {"path": "test_dir/test_file.txt", "name": "new_name.txt"}
        response = self.client.post(
            reverse("rename_file_or_directory"), data, content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"data": "Test error"})
        mock_rename.assert_called_once_with(data["path"], data["name"])

    def test_rename_url_resolves_rename_view(self):
        view = resolve("/file_manager/rename/")

        self.assertEqual(view.func.__name__, rename.__name__)

    @patch("app.file_manager.file_manager.FileManager.delete")
    def test_delete(self, mock_delete):
        data = {"path": "test_dir/test_file.txt"}
        response = self.client.post(
            reverse("delete_file_or_directory"), data, content_type="application/json"
        )
        self.assertEqual(response.status_code, 204)
        mock_delete.assert_called_once_with(data["path"])

    @patch("app.file_manager.file_manager.FileManager.delete")
    def test_delete_file_not_found(self, mock_delete):
        mock_delete.side_effect = FileNotFoundError("File not found")
        data = {"path": "nonexistent_file.txt"}
        response = self.client.post(
            reverse("delete_file_or_directory"), data, content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"data": "File not found"})
        mock_delete.assert_called_once_with(data["path"])

    def test_delete_url_resolves_delete_view(self):
        view = resolve("/file_manager/delete/")

        self.assertEqual(view.func.__name__, delete.__name__)

    @patch("app.file_manager.file_manager.FileManager.assert_exists")
    @patch("app.file_manager.file_manager.FileManager.resolve_path")
    @patch("builtins.open", new_callable=mock_open, read_data="file content")
    @patch("app.file_manager.file_manager.FileManager.check_access_permission")
    def test_download(
        self,
        mock_check_permission,
        mock_open_file,
        mock_resolve_path,
        mock_assert_exists,
    ):
        mock_assert_exists.return_value = True
        rel_path = "test_dir/test_file.txt"
        abs_path = os.path.join(self.storage_path, rel_path)

        mock_resolve_path.return_value = abs_path

        data = {"path": rel_path}
        response = self.client.post(
            reverse("download_file"), data, content_type="application/json"
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.has_header("Content-Disposition"))
        self.assertIn(
            f'filename="{os.path.basename(abs_path)}"', response["Content-Disposition"]
        )
        mock_resolve_path.assert_called_once_with(rel_path)
        mock_check_permission.assert_called_once_with(abs_path)
        mock_open_file.assert_called_once_with(abs_path, "rb")

    def test_download_invalid_data(
        self,
    ):
        data = {"invalid_arg": "test_dir/test_file.txt"}
        response = self.client.post(
            reverse("download_file"), data, content_type="application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"data": "File path is required."})

    @patch("app.file_manager.file_manager.FileManager.resolve_path")
    def test_download_raises_error(self, mock_resolve_path):
        mock_resolve_path.side_effect = Exception("Test error")
        data = {"path": "test_dir/test_file.txt"}
        response = self.client.post(
            reverse("download_file"), data, content_type="application/json"
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"data": "Test error"})

    def test_download_url_resolves_download_view(self):
        view = resolve("/file_manager/download/")

        self.assertEqual(view.func.__name__, download.__name__)

    @patch("app.file_manager.file_manager.FileManager.check_access_permission")
    @patch("builtins.open", new_callable=mock_open, read_data="file content")
    @patch("os.path.abspath")
    def test_upload_file(self, mock_abspath, mock_open_file, mock_check_permission):
        file_content = b"Test file content"
        test_file = SimpleUploadedFile(
            "test.txt", file_content, content_type="text/plain"
        )
        mock_abspath.return_value = self.storage_path
        response = self.client.post(
            reverse("upload_file"),
            {"file": test_file, "path": "."},
        )
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["data"], "File uploaded successfully.")

        new_file_name = os.path.join(self.storage_path, test_file.name)

        mock_open_file.assert_called_once_with(new_file_name, "wb+")
        mock_check_permission.assert_called_once_with(new_file_name)

    def test_upload_no_file(self):
        response = self.client.post(reverse("upload_file"), {"path": "."})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["data"], "No file provided.")

    @patch("app.file_manager.file_manager.FileManager.resolve_path")
    @patch("builtins.open", new_callable=mock_open, read_data="file content")
    @patch("os.path.abspath")
    def test_upload_to_root(self, mock_abspath, mock_open_file, mock_resolve_path):
        file_content = b"Root test file"
        test_file = SimpleUploadedFile(
            "root_test.txt", file_content, content_type="text/plain"
        )
        mock_abspath.return_value = "/"
        response = self.client.post(
            reverse("upload_file"), {"file": test_file, "path": "/"}
        )

        self.assertEqual(response.status_code, 400)

    def test_upload_url_resolves_upload_view(self):
        view = resolve("/file_manager/upload/")

        self.assertEqual(view.func.__name__, upload.__name__)

    def test_upload_exceeds_size_limit(self):
        large_file_content = b"A" * (settings.MAX_UPLOAD_SIZE + 1)
        large_file = SimpleUploadedFile(
            "large_file.txt", large_file_content, content_type="text/plain"
        )

        response = self.client.post(
            reverse("upload_file"), {"file": large_file, "path": "."}
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("File size exceeds", response.json().get("data"))
