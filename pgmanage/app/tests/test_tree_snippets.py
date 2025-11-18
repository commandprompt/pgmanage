from datetime import datetime
from functools import partial
from unittest.mock import patch

from app.models.main import SnippetFile, SnippetFolder
from app.views.tree_snippets import (
    delete_node_snippet,
    get_all_snippets,
    get_node_children,
    get_snippet_text,
    new_node_snippet,
    rename_node_snippet,
    save_snippet_text,
)
from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import resolve
from django.utils.timezone import make_aware


class TreeSnippetsTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="testuser", password="testpass")
        self.client.login(username="testuser", password="testpass")
        session = self.client.session
        session["pgmanage_session"] = {"mock_variable": "test"}
        session.save()
        self.client.post = partial(self.client.post, content_type="application/json")

    def test_get_all_snippets_empty(self):
        response = self.client.post("/get_all_snippets/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"id": None, "files": [], "folders": []})

    def test_get_all_snippets_url_resolves_get_all_snippets_view(self):
        view = resolve("/get_all_snippets/")

        self.assertEqual(view.func.__name__, get_all_snippets.__name__)

    def test_new_node_snippet_folder_and_file(self):
        response = self.client.post(
            "/new_node_snippet/",
            data={"snippet_id": None, "mode": "folder", "name": "My Folder"},
        )
        self.assertEqual(response.status_code, 201)

        folder_id = SnippetFolder.objects.get(name="My Folder").id

        file_response = self.client.post(
            "/new_node_snippet/",
            data={"snippet_id": folder_id, "mode": "snippet", "name": "My Snippet"},
        )
        self.assertEqual(file_response.status_code, 201)

    def test_new_node_snippet_creation_error(self):
        with patch(
            "app.views.tree_snippets.SnippetFile.save",
            side_effect=Exception("DB Error"),
        ):
            response = self.client.post(
                "/new_node_snippet/",
                data={"snippet_id": None, "mode": "snippet", "name": "Should Fail"},
            )
        self.assertEqual(response.status_code, 400)
        self.assertIn("DB Error", response.json()["data"])

    def test_new_node_snippet_url_resolves_new_node_snippet_view(self):
        view = resolve("/new_node_snippet/")

        self.assertEqual(view.func.__name__, new_node_snippet.__name__)

    def test_get_node_children(self):
        parent = SnippetFolder.objects.create(
            user=self.user,
            name="Parent",
            create_date=make_aware(datetime.now()),
            modify_date=make_aware(datetime.now()),
        )
        SnippetFolder.objects.create(
            user=self.user,
            name="test folder",
            parent=parent,
            create_date=make_aware(datetime.now()),
            modify_date=make_aware(datetime.now()),
        )
        SnippetFile.objects.create(
            user=self.user,
            name="test file",
            parent=parent,
            text="",
            create_date=make_aware(datetime.now()),
            modify_date=make_aware(datetime.now()),
        )

        response = self.client.post(
            "/get_node_children/", data={"snippet_id": parent.id}
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data["folders"]), 1)
        self.assertEqual(len(data["snippets"]), 1)

    def test_get_node_children_url_resolves_get_node_children_view(self):
        view = resolve("/get_node_children/")

        self.assertEqual(view.func.__name__, get_node_children.__name__)

    def test_get_snippet_text(self):
        file = SnippetFile.objects.create(
            user=self.user,
            name="Test Snippet",
            text="Some text",
            create_date=make_aware(datetime.now()),
            modify_date=make_aware(datetime.now()),
        )
        response = self.client.post("/get_snippet_text/", data={"snippet_id": file.id})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"], "Some text")

    def test_get_snippet_text_invalid_id(self):
        response = self.client.post("/get_snippet_text/", data={"snippet_id": 9999})
        self.assertEqual(response.status_code, 400)
        self.assertIn("data", response.json())

    def test_get_snippet_text_url_resolves_get_snippet_text_view(self):
        view = resolve("/get_snippet_text/")

        self.assertEqual(view.func.__name__, get_snippet_text.__name__)

    def test_save_and_update_snippet_text(self):
        folder = SnippetFolder.objects.create(
            user=self.user,
            name="Parent",
            create_date=make_aware(datetime.now()),
            modify_date=make_aware(datetime.now()),
        )
        save_response = self.client.post(
            "/save_snippet_text/",
            data={
                "id": "",
                "name": "New Snippet",
                "parent_id": folder.id,
                "text": "Initial content",
            },
        )
        self.assertEqual(save_response.status_code, 200)
        snippet_id = save_response.json()["id"]

        update_response = self.client.post(
            "/save_snippet_text/",
            data={
                "id": snippet_id,
                "name": "Updated Snippet",
                "parent_id": folder.id,
                "text": "Updated content",
            },
        )
        self.assertEqual(update_response.status_code, 200)
        updated_file = SnippetFile.objects.get(id=snippet_id)
        self.assertEqual(updated_file.text, "Updated content")

    def test_save_snippet_text_with_db_exception(self):
        file = SnippetFile.objects.create(
            user=self.user,
            name="Existing Snippet",
            text="",
            create_date=make_aware(datetime.now()),
            modify_date=make_aware(datetime.now()),
        )
        with patch(
            "app.views.tree_snippets.SnippetFile.save",
            side_effect=Exception("DB Failure"),
        ):
            response = self.client.post(
                "/save_snippet_text/",
                data={
                    "id": file.id,
                    "name": "Fail Update",
                    "parent_id": None,
                    "text": "Crash here",
                },
            )
        self.assertEqual(response.status_code, 400)
        self.assertIn("DB Failure", response.json()["data"])

    def test_save_snippet_text_url_resolves_save_snippet_text_view(self):
        view = resolve("/save_snippet_text/")

        self.assertEqual(view.func.__name__, save_snippet_text.__name__)

    def test_rename_node_snippet(self):
        file = SnippetFile.objects.create(
            user=self.user,
            name="Old Name",
            text="",
            create_date=make_aware(datetime.now()),
            modify_date=make_aware(datetime.now()),
        )
        response = self.client.post(
            "/rename_node_snippet/",
            data={"id": file.id, "name": "New Name", "mode": "snippet"},
        )
        self.assertEqual(response.status_code, 200)
        file.refresh_from_db()
        self.assertEqual(file.name, "New Name")

    def test_rename_node_snippet_folder(self):
        folder = SnippetFolder.objects.create(
            user=self.user,
            name="Old Folder",
            create_date=make_aware(datetime.now()),
            modify_date=make_aware(datetime.now()),
        )
        response = self.client.post(
            "/rename_node_snippet/",
            data={"id": folder.id, "name": "Renamed Folder", "mode": "folder"},
        )
        self.assertEqual(response.status_code, 200)
        folder.refresh_from_db()
        self.assertEqual(folder.name, "Renamed Folder")

    def test_rename_node_snippet_invalid_id(self):
        response = self.client.post(
            "/rename_node_snippet/",
            data={"id": 9999, "name": "DoesNotExist", "mode": "snippet"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("data", response.json())

    def test_rename_node_snippet_url_resolves_rename_node_snippet_view(self):
        view = resolve("/rename_node_snippet/")

        self.assertEqual(view.func.__name__, rename_node_snippet.__name__)

    def test_delete_node_snippet(self):
        folder = SnippetFolder.objects.create(
            user=self.user,
            name="To Delete Folder",
            create_date=make_aware(datetime.now()),
            modify_date=make_aware(datetime.now()),
        )
        file = SnippetFile.objects.create(
            user=self.user,
            name="To Delete File",
            text="",
            create_date=make_aware(datetime.now()),
            modify_date=make_aware(datetime.now()),
        )

        folder_response = self.client.post(
            "/delete_node_snippet/", data={"id": folder.id, "mode": "folder"}
        )
        file_response = self.client.post(
            "/delete_node_snippet/", data={"id": file.id, "mode": "snippet"}
        )

        self.assertEqual(folder_response.status_code, 204)
        self.assertEqual(file_response.status_code, 204)
        self.assertFalse(SnippetFolder.objects.filter(id=folder.id).exists())
        self.assertFalse(SnippetFile.objects.filter(id=file.id).exists())

    def test_delete_node_snippet_invalid_id(self):
        file = SnippetFile.objects.create(
            user=self.user,
            name="Safe File",
            text="",
            create_date=make_aware(datetime.now()),
            modify_date=make_aware(datetime.now()),
        )
        response = self.client.post("/delete_node_snippet/", data={"id": 9999})
        self.assertEqual(response.status_code, 400)

    def test_delete_node_snippet_url_resolves_delete_node_snippet_view(self):
        view = resolve("/delete_node_snippet/")

        self.assertEqual(view.func.__name__, delete_node_snippet.__name__)

    def test_unauthenticated_access_redirect(self):
        self.client.logout()
        endpoints = [
            "/get_all_snippets/",
            "/get_node_children/",
            "/get_snippet_text/",
            "/new_node_snippet/",
            "/delete_node_snippet/",
            "/save_snippet_text/",
            "/rename_node_snippet/",
        ]
        for endpoint in endpoints:
            response = self.client.post(endpoint)
            self.assertEqual(response.status_code, 401)
