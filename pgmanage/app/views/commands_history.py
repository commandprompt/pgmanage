from typing import Literal, Optional, Union

from app.models.main import Connection, ConsoleHistory, QueryHistory
from app.utils.decorators import user_authenticated
from django.core.paginator import Paginator
from django.http import HttpResponse, JsonResponse

from pgmanage import settings


@user_authenticated
def clear_commands_history(request):
    data = request.data

    database_index: int = data["database_index"]
    command_contains: str = data["command_contains"]
    command_from: Optional[str] = data["command_from"]
    command_to: Optional[str] = data["command_to"]
    command_type: Union[Literal["Query"], Literal["Console"]] = data["command_type"]

    try:
        conn = Connection.objects.get(id=database_index)
        if command_type == "Query":
            query = QueryHistory.objects.filter(
                user=request.user, connection=conn, snippet__icontains=command_contains
            ).order_by("-start_time")
        elif command_type == "Console":
            query = ConsoleHistory.objects.filter(
                user=request.user, connection=conn, snippet__icontains=command_contains
            ).order_by("-start_time")

        if command_from:
            query = query.filter(start_time__gte=command_from)

        if command_to:
            query = query.filter(start_time__lte=command_to)

        query.delete()
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return HttpResponse(status=204)


@user_authenticated
def get_commands_history(request):
    data = request.data

    current_page: int = data["current_page"]
    database_index: int = data["database_index"]
    command_contains: str = data["command_contains"]
    command_from: Optional[str] = data["command_from"]
    command_to: Optional[str] = data["command_to"]
    command_type: Union[Literal["Query"], Literal["Console"]] = data["command_type"]

    try:
        conn = Connection.objects.get(id=database_index)

        if command_type == "Query":
            query = QueryHistory.objects.filter(
                user=request.user, connection=conn, snippet__icontains=command_contains
            ).order_by("-start_time")
        elif command_type == "Console":
            query = ConsoleHistory.objects.filter(
                user=request.user, connection=conn, snippet__icontains=command_contains
            ).order_by("-start_time")

        if command_from:
            query = query.filter(start_time__gte=command_from)

        if command_to:
            query = query.filter(start_time__lte=command_to)

        p = Paginator(query, settings.CH_CMDS_PER_PAGE)

        if current_page not in list(p.page_range):
            current_page = 1
        commands = p.page(current_page).object_list

    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    command_list = []

    for command in commands:
        if command_type == "Query":
            command_data = {
                "start_time": command.start_time,
                "end_time": command.end_time,
                "duration": command.duration,
                "status": command.status,
                "snippet": command.snippet,
            }
        elif command_type == "Console":
            command_data = {
                "start_time": command.start_time,
                "snippet": command.snippet,
            }
        command_list.append(command_data)

    return JsonResponse(data={"command_list": command_list, "pages": p.num_pages})
