import os
import pathlib
import time

from pgmanage.settings import DESKTOP_MODE, HOME_DIR


class FileManager:
    def __init__(self, current_user):
        self.user = current_user
        self.storage = self._get_storage_directory()

    def _get_storage_directory(self):
        if not DESKTOP_MODE:
            storage_dir = os.path.join(HOME_DIR, "storage", self.user.username)

            if not os.path.exists(storage_dir):
                os.makedirs(storage_dir)

            return storage_dir

    def _create_file(self, path, name):
        with open(os.path.join(path, name), mode="w") as fp:
            pass

    def _create_dir(self, path, name):
        os.makedirs(os.path.join(path, name), exist_ok=True)

    def _assert_not_exists(self, path):
        if os.path.exists(path):
            raise FileExistsError("File or directory with given name already exists.")

    def _assert_exists(self, path):
        if not os.path.exists(path):
            raise FileNotFoundError("Invalid file or directory path.")

    def _format_size(self, num, suffix="B"):
        for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
            if abs(num) < 1024.0:
                return f"{num:3.1f}{unit}{suffix}"
            num /= 1024.0

        return f"{num:.1f} {suffix}"

    def create(self, path, name, file_type):
        self.check_access_permission(os.path.join(path, name))
        self._assert_not_exists(os.path.join(path, name))

        if file_type == "dir":
            self._create_dir(path, name)
        elif file_type == "file":
            self._create_file(path, name)

    def get_directory_content(self, path=None):

        if path is None:
            path = self.storage

        self.check_access_permission(path)

        data = {
            "parent": True if path != self.storage else False,
            "current_path": path,
            "files": [],
        }

        directory_content = os.listdir(path)
        if not directory_content:
            return data

        for file in directory_content:

            file_path = os.path.join(path, file)
            file_size = os.path.getsize(file_path)
            file_type = "dir" if os.path.isdir(file_path) else "file"
            created = os.path.getctime(file_path)
            modified = os.path.getmtime(file_path)
            dir_size = None
            if os.path.isdir(file_path):
                dir_size = len(os.listdir(file_path))

            data["files"].append(
                {
                    "file_name": file,
                    "file_path": file_path,
                    "file_size": self._format_size(file_size),
                    "file_type": file_type,
                    "created": time.ctime(created),
                    "modified": time.ctime(modified),
                    "dir_size": dir_size,
                }
            )

        return data

    def get_parent_directory_content(self, path):
        return self.get_directory_content(os.path.dirname(path))

    def rename(self, path, name):

        self._assert_exists(path)

        self.check_access_permission(path)

        dirpath, _ = os.path.split(path)

        new_path = os.path.join(dirpath, name)

        self._assert_not_exists(new_path)

        self.check_access_permission(new_path)

        os.rename(path, new_path)

    def delete(self, path):

        self._assert_exists(path)

        self.check_access_permission(path)

        if os.path.isdir(path):
            os.rmdir(path)

        elif os.path.isfile(path):
            os.remove(path)

    def check_access_permission(self, path):

        if DESKTOP_MODE:
            return

        abs_path = os.path.abspath(path)

        try:
            pathlib.Path(abs_path).relative_to(self.storage)
        except ValueError:
            raise PermissionError(f"Access denied: {abs_path}")
