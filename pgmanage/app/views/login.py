import logging

from app.include.Session import Session
from app.models.main import UserDetails
from app.utils.decorators import user_authenticated
from app.utils.key_manager import key_manager
from django.contrib.auth import authenticate, login
from django.contrib.auth import logout as logout_django
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import redirect, render

from pgmanage import custom_settings, settings

logger = logging.getLogger(__name__)


@login_required
def check_session(request):
    user_details, _ = UserDetails.objects.get_or_create(user=request.user)

    # Invalid session
    if not request.session.get("pgmanage_session"):
        # creating session key to use it
        request.session.save()

        pgmanage_session = Session(
            request.user.id,
            request.user.username,
            "light",
            user_details.font_size,
            request.user.is_superuser,
            request.session.session_key,
            user_details.csv_encoding,
            user_details.csv_delimiter,
        )

        request.session["pgmanage_session"] = pgmanage_session

    return redirect(settings.PATH + "/workspace/")


def index(request):
    context = {
        "pgmanage_short_version": settings.PGMANAGE_SHORT_VERSION,
        "base_path": settings.PATH,
        "csrf_cookie_name": settings.CSRF_COOKIE_NAME,
    }

    user = request.GET.get("user", "")
    pwd = request.GET.get("pwd", "")

    if user and pwd:
        num_connections = sign_in_automatic(request, user, pwd)

        if num_connections >= 0:
            return redirect("/")
        return HttpResponse("INVALID APP TOKEN")

    return render(request, "app/login.html", context)


@user_authenticated
def logout(request):
    pgmanage_session = request.session.get("pgmanage_session")
    logger.info('User "%s" logged out.', pgmanage_session.v_user_name)
    key_manager.remove(request.user)
    logout_django(request)

    return redirect(settings.PATH + "/pgmanage_login")


def sign_in_automatic(request, username, pwd):

    token = request.GET.get("token", "")
    valid_token = custom_settings.APP_TOKEN

    if valid_token and token != valid_token:
        return -1

    user = authenticate(username=username, password=pwd)
    if user is not None:
        login(request, user)
    else:
        return -1

    logger.info('User "%s" logged in.', username)

    return 0


def sign_in(request):
    response_data = {"data": -1}

    valid_token = custom_settings.APP_TOKEN

    if valid_token:
        response_data["data"] = -2
        return JsonResponse(response_data)

    data = request.data
    username = data["username"]
    pwd = data["password"]

    user = authenticate(username=username, password=pwd)
    if user is not None:
        login(request, user)
    else:
        return JsonResponse(response_data)

    logger.info('User "%s" logged in.', username)

    response_data["data"] = 0

    return JsonResponse(response_data)
