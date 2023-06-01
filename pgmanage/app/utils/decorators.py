import json

from app.client_manager import client_manager
from django.http import JsonResponse


def user_authenticated(function):
    def wrap(request, *args, **kwargs):
        # User not authenticated
        if request.user.is_authenticated:
            return function(request, *args, **kwargs)
        else:
            v_return = {"v_data": "", "v_error": True, "v_error_id": 1}
            return JsonResponse(v_return)

    wrap.__doc__ = function.__doc__
    wrap.__name__ = function.__name__
    return wrap


def database_required(p_check_timeout=True, p_open_connection=True):
    def decorator(function):
        def wrap(request, *args, **kwargs):
            v_return = {"v_data": "", "v_error": False, "v_error_id": -1}

            v_session = request.session.get("pgmanage_session")

            json_object = json.loads(request.POST.get("data", None))
            v_database_index = json_object["p_database_index"]
            v_tab_id = json_object["p_tab_id"]

            if v_database_index is not None:
                try:
                    if p_check_timeout:
                        # Check database prompt timeout
                        v_timeout = v_session.DatabaseReachPasswordTimeout(
                            int(v_database_index)
                        )
                        if v_timeout["timeout"]:
                            v_return["v_data"] = {
                                "password_timeout": True,
                                "message": v_timeout["message"],
                            }
                            v_return["v_error"] = True
                            return JsonResponse(v_return)

                    v_database = client_manager.get_database(
                        session=request.session,
                        conn_tab_id=v_tab_id,
                        database_index=v_database_index,
                        attempt_to_open_connection=p_open_connection,
                    )
                except Exception as exc:
                    v_return["v_data"] = {
                        "password_timeout": False,
                        "message": str(exc),
                    }
                    v_return["v_error"] = True
                    return JsonResponse(v_return)
            else:
                v_database = None

            return function(request, v_database, *args, **kwargs)

        wrap.__doc__ = function.__doc__
        wrap.__name__ = function.__name__
        return wrap

    return decorator


def database_required_new(check_timeout=True, open_connection=True):
    def decorator(function):
        def wrap(request, *args, **kwargs):
            session = request.session.get("pgmanage_session")

            json_object = json.loads(request.body) if request.body else {}
            database_index = json_object.get("database_index")
            tab_id = json_object.get("tab_id")

            if database_index is not None:
                try:
                    if check_timeout:
                        # Check database prompt timeout
                        timeout = session.DatabaseReachPasswordTimeout(
                            int(database_index)
                        )
                        if timeout["timeout"]:
                            data = {
                                "password_timeout": True,
                                "data": timeout["message"],
                            }
                            return JsonResponse(data=data, status=400)

                    database = client_manager.get_database(
                        session=request.session,
                        conn_tab_id=tab_id,
                        database_index=database_index,
                        attempt_to_open_connection=open_connection,
                    )
                except Exception as exc:
                    data = {"password_timeout": True, "data": str(exc)}
                    return JsonResponse(data=data, status=400)
            else:
                database = None

            return function(request, database, *args, **kwargs)

        wrap.__doc__ = function.__doc__
        wrap.__name__ = function.__name__
        return wrap

    return decorator


def superuser_required(function):
    def wrap(request, *args, **kwargs):
        v_session = request.session.get("pgmanage_session")

        if v_session.v_super_user:
            return function(request, *args, **kwargs)
        else:
            v_return = {
                "v_data": "You must be superuser to perform this operation",
                "v_error": True,
            }
            return JsonResponse(v_return)

    wrap.__doc__ = function.__doc__
    wrap.__name__ = function.__name__

    return wrap
