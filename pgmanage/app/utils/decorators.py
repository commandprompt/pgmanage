import json
from functools import partial, wraps
from typing import Any, Callable, Optional

from app.client_manager import client_manager
from django.http import JsonResponse


def user_authenticated(function):
    @wraps(function)
    def wrap(request, *args, **kwargs):
        # User not authenticated
        if request.user.is_authenticated:
            return function(request, *args, **kwargs)
        v_return = {"v_data": "", "v_error": True, "v_error_id": 1}
        return JsonResponse(v_return)

    return wrap


def database_required(p_check_timeout=True, p_open_connection=True):
    def decorator(function):
        @session_required(use_old_error_format=True)
        @wraps(function)
        def wrap(request, session, *args, **kwargs):
            v_return = {"v_data": "", "v_error": False, "v_error_id": -1}

            if request.content_type == 'application/json':
                json_object = json.loads(request.body)
            else:
                json_object = json.loads(request.POST.get("data", None))
            v_database_index = json_object["p_database_index"]
            v_tab_id = json_object["p_tab_id"]
            client = client_manager.get_or_create_client(
                client_id=request.session.session_key
            )

            if v_database_index is not None:
                try:
                    if p_check_timeout:
                        # Check database prompt timeout
                        v_timeout = session.DatabaseReachPasswordTimeout(
                            int(v_database_index)
                        )
                        if v_timeout["timeout"]:
                            v_return["v_data"] = {
                                "password_timeout": True,
                                "message": v_timeout["message"],
                            }
                            v_return["v_error"] = True
                            return JsonResponse(v_return)

                    v_database = client.get_main_tab_database(
                        session=session,
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

        return wrap

    return decorator


def database_required_new(check_timeout=True, open_connection=True):
    def decorator(function):
        @session_required
        @wraps(function)
        def wrap(request, session, *args, **kwargs):
            json_object = json.loads(request.body) if request.body else {}
            database_index = json_object.get("database_index")
            tab_id = json_object.get("tab_id")
            client = client_manager.get_or_create_client(
                client_id=request.session.session_key
            )

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

                    database = client.get_main_tab_database(
                        session=session,
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

        return wrap

    return decorator


def superuser_required(function):
    @session_required(use_old_error_format=True)
    @wraps(function)
    def wrap(request, session, *args, **kwargs):
        if session.v_super_user:
            return function(request, *args, **kwargs)
        v_return = {
            "v_data": "You must be superuser to perform this operation",
            "v_error": True,
        }
        return JsonResponse(v_return)

    return wrap


def session_required(
    func: Optional[Callable[..., Any]] = None,
    use_old_error_format: bool = False,
    include_session: bool = True,
) -> Callable[..., Any]:
    """
    Decorator that enforces the presence of a valid session in the request.

    Args:
        func (callable, optional): The function to be decorated.
        use_old_error_format (bool, optional): Flag indicating whether to use old error format or not.
            If True, returns a JsonResponse with old error format when session is invalid.
            If False, returns a JsonResponse with a "Invalid session" message and status code 401.
            Defaults to False.
        include_session (bool, optional): Flag indicating whether to include the session parameter
            when calling the decorated function. If True, the session parameter will be included.
            If False, the session parameter will be omitted. Defaults to True.

    Returns:
        callable: The decorated function or a decorator factory.

    Raises:
        None
    """

    if func is None:
        return partial(
            session_required,
            use_old_error_format=use_old_error_format,
            include_session=include_session,
        )

    @wraps(func)
    def containing_func(request, *args, **kwargs):
        session = request.session.get("pgmanage_session")
        if not session:
            if use_old_error_format:
                return JsonResponse({"v_data": "", "v_error": True, "v_error_id": 1})
            return JsonResponse({"data": "Invalid session"}, status=401)

        if include_session:
            return func(request, session, *args, **kwargs)
        return func(request, *args, **kwargs)

    return containing_func
