import json

from app.file_manager.file_manager import FileManager
from app.utils.decorators import user_authenticated
from django.http import HttpResponse, JsonResponse


@user_authenticated
def create(request):
    file_manager = FileManager(request.user)

    data = json.loads(request.body) if request.body else {}

    try:
        file_manager.create(data.get("path"), data.get("name"), data.get("type"))
        return JsonResponse({"data": "created"}, status=201)
    except FileExistsError as exc:
        return JsonResponse({"data": str(exc)}, status=400)
    except PermissionError as exc:
        return JsonResponse({"data": str(exc)}, status=403)


@user_authenticated
def get_directory(request):
    file_manager = FileManager(request.user)

    data = json.loads(request.body) if request.body else {}
    try:
        if data.get("parent_dir"):
            files = file_manager.get_parent_directory_content(data["current_path"])
        else:
            files = file_manager.get_directory_content(data.get("current_path"))
        return JsonResponse(files)
    except PermissionError as exc:
        return JsonResponse({"data": str(exc)}, status=403)


@user_authenticated
def rename(request):
    file_manager = FileManager(request.user)

    data = json.loads(request.body) if request.body else {}

    try:
        file_manager.rename(data.get("path"), data.get("name"))
        return JsonResponse({"data": "success"})
    except (FileExistsError, FileNotFoundError) as exc:
        return JsonResponse({"data": str(exc)}, status=400)
    except PermissionError as exc:
        return JsonResponse({"data": str(exc)}, status=403)


@user_authenticated
def delete(request):
    file_manager = FileManager(request.user)

    data = json.loads(request.body) if request.body else {}

    try:
        file_manager.delete(data.get("path"))
        return HttpResponse(status=204)
    except FileNotFoundError as exc:
        return JsonResponse({"data": str(exc)}, status=400)
    except PermissionError as exc:
        return JsonResponse({"data": str(exc)}, status=403)
    except OSError as exc:
        return JsonResponse({"data": str(exc)}, status=400)
