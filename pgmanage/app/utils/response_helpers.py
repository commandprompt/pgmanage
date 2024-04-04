from typing import Any, Dict, Optional

from django.http import JsonResponse


def create_response_template() -> Dict[str, Any]:
    return {"v_data": "", "v_error": False, "v_error_id": -1}


def error_response(
    message: str, error_id: int = -1, password_timeout: Optional[bool] = None, status = 200
) -> JsonResponse:
    """
    Creates a JSON error response.

    Args:
        message (str): The error message.
        error_id (int, optional): The error ID. Defaults to -1.
        password_timeout (bool, optional): The password timeout flag. If None, the
            "password_timeout" key is excluded from the response. Defaults to None.
        status http (int, optional):  status code to be set
    Returns:
        JsonResponse: The JSON error response.
    """
    if password_timeout is None:
        error_data = message
    else:
        error_data = error_data = {
            "password_timeout": password_timeout,
            "message": message,
        }
    return JsonResponse(data={"v_data": error_data, "v_error": True, "v_error_id": error_id}, status=status)
