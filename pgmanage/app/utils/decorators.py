from functools import partial, wraps
from typing import Any, Callable, Optional

from app.client_manager import client_manager
from app.include.Spartacus.Database import InvalidPasswordException
from django.http import HttpResponse, JsonResponse


def user_authenticated(function):
    @wraps(function)
    def wrap(request, *args, **kwargs):
        # User not authenticated
        if request.user.is_authenticated:
            return function(request, *args, **kwargs)
        return HttpResponse(status=401)

    return wrap


def database_required(check_timeout=True, open_connection=True, prefer_database=None):
    def decorator(function):
        @session_required
        @wraps(function)
        def wrap(request, session, *args, **kwargs):
            data = request.data
            # FIXME: prefer_database is a temporary workaround to prevent
            # issues with DROP DATABASE caused by opening DB backends to
            # currently selected database when DB tree is loaded
            if prefer_database is not None:
                database_name = prefer_database
            else:
                database_name = data.get("database_name")
            database_index = data.get("database_index")
            tab_id = data.get("tab_id")
            workspace_id=data.get("workspace_id")
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
                                "kind": timeout.get("kind", "database")
                            }
                            return JsonResponse(data=data, status=400)
                    database = client.get_tab_database(
                        session=session,
                        workspace_id=workspace_id,
                        tab_id=tab_id,
                        database_index=database_index,
                        database_name=database_name,
                        attempt_to_open_connection=open_connection,
                    )
                except InvalidPasswordException as exc:
                    error_resp = str(exc)
                    if session.v_databases.get(database_index, {}).get(
                        "decryption_failed"
                    ):
                        error_resp = f"There was a decryption error with the stored password. Please try re-saving your password in Connection Manager and attempt again.\n{str(exc)}"
                    data = {"password_timeout": True, "data": error_resp}
                    return JsonResponse(data=data, status=400)
                except Exception as exc:
                    data = {"password_timeout": False, "data": str(exc)}
                    return JsonResponse(data=data, status=400)
            else:
                database = None

            return function(request, database, *args, **kwargs)

        return wrap

    return decorator


def superuser_required(function):
    @session_required
    @wraps(function)
    def wrap(request, session, *args, **kwargs):
        if request.user.is_superuser:
            return function(request, *args, **kwargs)
        return JsonResponse({"data": "You must be superuser to perform this operation"}, status=403)
    return wrap


def session_required(
    func: Optional[Callable[..., Any]] = None,
    include_session: bool = True,
) -> Callable[..., Any]:
    """
    Decorator that enforces the presence of a valid session in the request.

    Args:
        func (callable, optional): The function to be decorated.
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
            include_session=include_session,
        )

    @wraps(func)
    def containing_func(request, *args, **kwargs):
        session = request.session.get("pgmanage_session")
        if not session:
            return JsonResponse({"data": "Invalid session"}, status=401)

        if include_session:
            return func(request, *args, **kwargs, session=session)
        return func(request, *args, **kwargs)

    return containing_func
