from django.http import HttpResponse
from django.template import loader
from django.http import JsonResponse
from django.core import serializers
from django.shortcuts import redirect
from pgmanage import settings
import json

import sys

import app.include.Spartacus as Spartacus
import app.include.Spartacus.Database as Database
import app.include.Spartacus.Utils as Utils
import app.include.OmniDatabase as OmniDatabase
from app.include.Session import Session
from pgmanage import settings, custom_settings

from django.contrib.auth import authenticate, login
from django.contrib.auth import logout as logout_django

from app.models.main import *
from django.contrib.auth.models import User

from django.contrib.auth.decorators import login_required
from app.utils.decorators import user_authenticated
from app.utils.key_manager import key_manager

import logging

logger = logging.getLogger(__name__)

@login_required
def check_session(request):
    # User is authenticated, check if user details object exists.
    try:
        user_details = UserDetails.objects.get(user=request.user)
    # User details does not exist, create it.
    except Exception:
        user_details = UserDetails(user=request.user)
        user_details.save()

    #Invalid session
    if not request.session.get('pgmanage_session'):
        #creating session key to use it
        request.session.save()

        v_session = Session(
            request.user.id,
            request.user.username,
            'light',
            user_details.font_size,
            request.user.is_superuser,
            request.session.session_key,
            user_details.csv_encoding,
            user_details.csv_delimiter
        )

        request.session['pgmanage_session'] = v_session

    return redirect(settings.PATH + '/workspace')

def index(request):
    context = {
        'pgmanage_short_version': settings.PGMANAGE_SHORT_VERSION,
        'url_folder': settings.PATH,
        'csrf_cookie_name': settings.CSRF_COOKIE_NAME
    }

    user = request.GET.get('user', '')
    pwd = request.GET.get('pwd', '')

    if user and pwd:
        num_connections = sign_in_automatic(request,user,pwd)

        if num_connections >= 0:
            return redirect('/')
        else:
            return HttpResponse("INVALID APP TOKEN")

    template = loader.get_template('app/login.html')
    return HttpResponse(template.render(context, request))

@user_authenticated
def logout(request):

    v_session = request.session.get('pgmanage_session')
    logger.info('User "{0}" logged out.'.format(v_session.v_user_name))
    key_manager.remove(request.user)
    logout_django(request)

    return redirect(settings.PATH + '/pgmanage_login')

def check_session_message(request):

    v_return = {}
    v_return['v_data'] = ''
    v_return['v_error'] = False
    v_return['v_error_id'] = -1

    if request.session.get('omnidb_alert_message'):
        v_return['v_data'] = request.session.get('omnidb_alert_message')
        request.session['omnidb_alert_message'] = ''

    return JsonResponse(v_return)

def sign_in_automatic(request, username, pwd):

    token = request.GET.get('token', '')
    valid_token = custom_settings.APP_TOKEN

    if valid_token and token != valid_token:
        return -1

    user = authenticate(username=username, password=pwd)
    if user is not None:
        login(request, user)
    else:
        return -1

    logger.info('User "{0}" logged in.'.format(username))

    return 0

def create_user_session(request, user, user_details):
    #creating session key to use it
    request.session.save()

    v_session = Session(
        user.id,
        user.username,
        'light',
        user_details.font_size,
        request.user.is_superuser,
        request.session.session_key,
        user_details.csv_encoding,
        user_details.csv_delimiter
    )

    request.session['pgmanage_session'] = v_session


def sign_in(request):
    v_return = {}
    v_return['v_data'] = -1
    v_return['v_error'] = False
    v_return['v_error_id'] = -1

    valid_token = custom_settings.APP_TOKEN

    if valid_token:
        v_return['v_data'] = -2
        return JsonResponse(v_return)

    json_object = json.loads(request.POST.get('data', None))
    username = json_object['p_username']
    pwd = json_object['p_pwd']

    user = authenticate(username=username, password=pwd)
    if user is not None:
        login(request, user)
    else:
        return JsonResponse(v_return)

    logger.info('User "{0}" logged in.'.format(username))

    v_return['v_data'] = 0

    return JsonResponse(v_return)
