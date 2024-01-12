from datetime import datetime
from typing import List, Optional

from app.models.main import SnippetFile, SnippetFolder
from app.utils.decorators import session_required
from django.core.exceptions import ObjectDoesNotExist
from django.http import HttpResponse, JsonResponse
from django.utils.timezone import make_aware


@session_required(include_session=False)
def get_all_snippets(request):
    folders = SnippetFolder.objects.filter(user=request.user)
    files = SnippetFile.objects.filter(user=request.user)

    root = {"id": None, "files": [], "folders": []}

    build_snippets_object_recursive(folders, files, root)

    return JsonResponse(root)


def build_snippets_object_recursive(
    folders: Optional[List[SnippetFolder]],
    files: Optional[List[SnippetFile]],
    current_object: dict,
) -> None:
    # Adding files
    for file in files:
        # Match
        if (file.parent is None and current_object["id"] is None) or (
            file.parent is not None and file.parent.id == current_object["id"]
        ):
            current_object["files"].append({"id": file.id, "name": file.name})
    # Adding folders
    for folder in folders:
        # Match
        if (folder.parent is None and current_object["id"] is None) or (
            folder.parent is not None and folder.parent.id == current_object["id"]
        ):
            folder_object = {
                "id": folder.id,
                "name": folder.name,
                "files": [],
                "folders": [],
            }

            build_snippets_object_recursive(folders, files, folder_object)

            current_object["folders"].append(folder_object)


@session_required(include_session=False)
def get_node_children(request):
    snippet_id = request.data.get("snippet_id")

    data = {"folders": [], "snippets": []}

    for folder in SnippetFolder.objects.filter(user=request.user, parent=snippet_id):
        node_data = {"id": folder.id, "name": folder.name}
        data["folders"].append(node_data)

    for file in SnippetFile.objects.filter(user=request.user, parent=snippet_id):
        node_data = {"id": file.id, "name": file.name}
        data["snippets"].append(node_data)

    return JsonResponse(data=data)


@session_required(include_session=False)
def get_snippet_text(request):
    snippet_id = request.data.get("snippet_id")

    try:
        snippet_text = SnippetFile.objects.get(id=snippet_id).text
    except ObjectDoesNotExist as exc:
        return JsonResponse({"data": str(exc)}, status=400)
    return JsonResponse({"data": snippet_text})


@session_required(include_session=False)
def new_node_snippet(request):
    data = request.data
    snippet_id = data.get("snippet_id")
    mode = data.get("mode")
    name = data.get("name")

    parent = SnippetFolder.objects.filter(id=snippet_id).first()

    try:
        new_date = make_aware(datetime.now())
        if mode == "folder":
            folder = SnippetFolder(
                user=request.user,
                parent=parent,
                name=name,
                create_date=new_date,
                modify_date=new_date,
            )
            folder.save()
        else:
            file = SnippetFile(
                user=request.user,
                parent=parent,
                name=name,
                create_date=new_date,
                modify_date=new_date,
                text="",
            )
            file.save()
    except Exception as exc:
        return JsonResponse({"data": str(exc)}, status=400)
    return JsonResponse({"data": "created"}, status=201)


@session_required(include_session=False)
def delete_node_snippet(request):
    data = request.data
    snippet_id = data.get("id")
    mode = data.get("mode")

    try:
        if mode == "folder":
            folder = SnippetFolder.objects.get(id=snippet_id)
            folder.delete()
        else:
            file = SnippetFile.objects.get(id=snippet_id)
            file.delete()

    except Exception as exc:
        return JsonResponse({"data": str(exc)}, status=400)
    return HttpResponse(status=204)


@session_required(include_session=False)
def save_snippet_text(request):
    data = request.data
    snippet_id = data.get("id")
    name = data.get("name")
    parent_id = data.get("parent_id")
    text = data.get("text")

    parent = SnippetFolder.objects.filter(id=parent_id).first()

    try:
        # new snippet
        new_date = make_aware(datetime.now())
        if not snippet_id:
            file = SnippetFile(
                user=request.user,
                parent=parent,
                name=name,
                create_date=new_date,
                modify_date=new_date,
                text=text,
            )
            file.save()
        # existing snippet
        else:
            file = SnippetFile.objects.get(id=snippet_id)
            file.text = text
            file.modify_date = new_date
            file.save()

        data = {
            "type": "snippet",
            "id": file.id,
            "parent": parent_id,
            "name": file.name,
        }

    except Exception as exc:
        return JsonResponse({"data": str(exc)}, status=400)
    return JsonResponse(data)


@session_required(include_session=False)
def rename_node_snippet(request):
    data = request.data
    node_id = data.get("id")
    name = data.get("name")
    mode = data.get("mode")

    try:
        if mode == "folder":
            folder = SnippetFolder.objects.get(id=node_id)
            folder.name = name
            folder.modify_date = make_aware(datetime.now())
            folder.save()
        else:
            file = SnippetFile.objects.get(id=node_id)
            file.name = name
            file.modify_date = make_aware(datetime.now())
            file.save()

    except Exception as exc:
        return JsonResponse({"data": str(exc)}, status=400)
    return JsonResponse({"data": "success"})
