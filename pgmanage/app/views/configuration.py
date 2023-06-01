import json

from app.models import ConfigHistory, Connection
from app.utils.conf import get_settings, get_settings_status, post_settings
from app.utils.decorators import database_required_new, user_authenticated
from django.core.exceptions import ValidationError
from django.db import DatabaseError
from django.db.models import Q
from django.http import HttpResponse, JsonResponse
from django.views.decorators.http import require_http_methods


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_configuration(request, database):
    data = json.loads(request.body) if request.body else {}
    exclude_read_only = data.get("exclude_read_only")
    grouped = data.get("grouped", True)
    try:
        settings = get_settings(database, grouped, exclude_read_only)
    except DatabaseError as exc:
        return JsonResponse(data={"data": str(exc)}, status=500)
    return JsonResponse({"settings": settings})


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_configuration_categories(request, database):
    try:
        query = database.QueryConfigCategories().Rows
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=500)
    categories = [l.pop() for l in query]
    return JsonResponse({"categories": categories})


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def save_configuration(request, database):
    data = json.loads(request.body) if request.body else {}
    update_data = data.get("settings")
    commit_comment = data.get("commit_comment")
    new_config = data.get("new_config")
    try:
        updated_settings = post_settings(
            request, database, update_data, commit_comment, new_config
        )
        return JsonResponse(data=updated_settings)
    except ValidationError as exc:
        return JsonResponse(data={"data": exc.message}, status=400)
    except DatabaseError as exc:
        return JsonResponse(data={"data": str(exc)}, status=500)


@user_authenticated
def get_configuration_history(request):
    data = json.loads(request.body) if request.body else {}
    config_history = ConfigHistory.objects.filter(
        Q(user=request.user)
        & Q(connection=Connection.objects.filter(id=data.get("database_index")).first())
    ).order_by("-start_time")

    data = []

    for config in config_history:
        data.append(
            {
                "id": config.id,
                "start_time": config.start_time,
                "user": config.user.username,
                "connection": config.connection.id,
                "config_snapshot": json.loads(config.config_snapshot),
                "commit_comment": config.commit_comment,
            }
        )

    return JsonResponse({"config_history": data})


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_status(request, database):
    try:
        settings_status = get_settings_status(database)
    except DatabaseError as exc:
        return JsonResponse(data={"data": str(exc)}, status=500)
    return JsonResponse(settings_status)


@require_http_methods(["DELETE"])
@user_authenticated
def delete_config(request, config_id):
    config = ConfigHistory.objects.filter(id=config_id).first()
    if config:
        if config.user.id != request.user.id:
            return JsonResponse(
                data={
                    "message": "You are not allowed to delete not yours configurations."
                },
                status=403,
            )

        config.delete()
    return HttpResponse(status=204)
