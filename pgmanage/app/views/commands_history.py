from math import ceil

from app.models.main import Connection, ConsoleHistory, QueryHistory
from app.utils.decorators import user_authenticated
from app.utils.response_helpers import create_response_template, error_response
from django.http import JsonResponse

from pgmanage import settings


@user_authenticated
def get_command_list(request):
    response_data = create_response_template()

    data = request.data

    current_page = data["p_current_page"]
    database_index = data["p_database_index"]
    command_contains = data["p_command_contains"]
    command_from = data["p_command_from"]
    command_to = data["p_command_to"]

    try:
        conn = Connection.objects.get(id=database_index)

        query = QueryHistory.objects.filter(
            user=request.user, connection=conn, snippet__icontains=command_contains
        ).order_by("-start_time")

        if command_from is not None and command_from != "":
            query = query.filter(start_time__gte=command_from)

        if command_to is not None and command_to != "":
            query = query.filter(start_time__lte=command_to)

        count = query.count()

        offset = (current_page - 1) * settings.CH_CMDS_PER_PAGE

        commands = query[offset : offset + settings.CH_CMDS_PER_PAGE]

    except Exception as exc:
        return error_response(message=str(exc))

    command_list = []

    for command in commands:
        command_data_list = [command.start_time, command.end_time, command.duration]

        if command.status == "success":
            command_data_list.append(
                "<div class='text-center'><i title='Success' class='fas fa-check text-success action-grid action-status-ok'></i></div>"
            )
        else:
            command_data_list.append(
                "<div class='text-center'><i title='Error' class='fas fa-exclamation-circle text-danger action-grid action-status-error'></i></div>"
            )

        command_data_list.append(command.snippet)

        command_list.append(command_data_list)

    page = ceil(count / settings.CH_CMDS_PER_PAGE)
    if page == 0:
        page = 1

    response_data["v_data"] = {"commandList": command_list, "pages": page}

    return JsonResponse(response_data)


@user_authenticated
def clear_command_list(request):
    response_data = create_response_template()

    data = request.data

    database_index = data["p_database_index"]
    command_contains = data["p_command_contains"]
    command_from = data["p_command_from"]
    command_to = data["p_command_to"]

    try:
        conn = Connection.objects.get(id=database_index)

        query = QueryHistory.objects.filter(
            user=request.user, connection=conn, snippet__icontains=command_contains
        ).order_by("-start_time")

        if command_from is not None and command_from != "":
            query = query.filter(start_time__gte=command_from)

        if command_to is not None and command_to != "":
            query = query.filter(start_time__lte=command_to)

        query.delete()
    except Exception as exc:
        return error_response(message=str(exc))

    return JsonResponse(response_data)


@user_authenticated
def get_console_history(request):
    response_data = create_response_template()

    data = request.data

    current_page = data["p_current_page"]
    database_index = data["p_database_index"]
    command_contains = data["p_command_contains"]
    command_from = data["p_command_from"]
    command_to = data["p_command_to"]

    try:
        conn = Connection.objects.get(id=database_index)

        query = ConsoleHistory.objects.filter(
            user=request.user, connection=conn, snippet__icontains=command_contains
        ).order_by("-start_time")

        if command_from is not None and command_from != "":
            query = query.filter(start_time__gte=command_from)

        if command_to is not None and command_to != "":
            query = query.filter(start_time__lte=command_to)

        count = query.count()

        offset = (current_page - 1) * settings.CH_CMDS_PER_PAGE

        commands = query[offset : offset + settings.CH_CMDS_PER_PAGE]

    except Exception as exc:
        return error_response(message=str(exc))

    command_list = []

    for command in commands:
        command_data_list = [command.start_time, command.snippet]

        command_list.append(command_data_list)

    page = ceil(count / settings.CH_CMDS_PER_PAGE)
    if page == 0:
        page = 1

    response_data["v_data"] = {"commandList": command_list, "pages": page}

    return JsonResponse(response_data)


@user_authenticated
def clear_console_list(request):
    response_data = create_response_template()

    data = request.data

    database_index = data["p_database_index"]
    command_contains = data["p_console_contains"]
    command_from = data["p_console_from"]
    command_to = data["p_console_to"]

    try:
        conn = Connection.objects.get(id=database_index)

        query = ConsoleHistory.objects.filter(
            user=request.user, connection=conn, snippet__icontains=command_contains
        ).order_by("-start_time")

        if command_from is not None and command_from != "":
            query = query.filter(start_time__gte=command_from)

        if command_to is not None and command_to != "":
            query = query.filter(start_time__lte=command_to)

        query.delete()
    except Exception as exc:
        return error_response(message=str(exc))

    return JsonResponse(response_data)
