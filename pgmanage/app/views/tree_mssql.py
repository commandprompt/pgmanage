from app.utils.decorators import database_required, user_authenticated
from django.http import JsonResponse


@user_authenticated
@database_required(check_timeout=False, open_connection=True)
def get_tree_info(request, database):
    try:
        data = {
            "version": database.GetVersion(),
        }
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)
    return JsonResponse(data=data)