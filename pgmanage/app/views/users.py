from django.http import HttpResponse
from django.template import loader
from django.http import JsonResponse
from django.core import serializers
from django.shortcuts import redirect
import uuid

import sys

import app.include.Spartacus as Spartacus
import app.include.Spartacus.Database as Database
import app.include.Spartacus.Utils as Utils
import app.include.OmniDatabase as OmniDatabase
from app.include.Session import Session
from pgmanage import settings
from django.utils import timezone
from django.contrib.auth import update_session_auth_hash

from django.contrib.auth.models import User

from app.utils.decorators import superuser_required, user_authenticated
from app.utils.key_manager import key_manager
from app.utils.response_helpers import create_response_template, error_response

@superuser_required
@user_authenticated
def get_users(request):
    v_return = create_response_template()

    v_user_list = []
    v_user_id_list = []

    try:
        for user in User.objects.all():
            v_user_data_list = [user.username, '', 1 if user.is_superuser else 0,
                                '''<i title="Remove User" class='fas fa-times action-grid action-close' 
                                onclick='removeUser("{0}")'></i>'''.format(
                                    user.id)]

            v_user_list.append(v_user_data_list)
            v_user_id_list.append(user.id)
    except Exception as exc:
        None

    v_return['v_data'] = {
        'v_data': v_user_list,
        'v_user_ids': v_user_id_list
    }

    return JsonResponse(v_return)


@superuser_required
@user_authenticated
def new_user(request):
    v_return = create_response_template()

    try:
        for user in request.data:
            new_user = User.objects.create_user(
                username=user[0],
                password=user[1],
                email='',
                last_login=timezone.now(),
                is_superuser=False,
                first_name='',
                last_name='',
                is_staff=False,
                is_active=True,
                date_joined=timezone.now())
    except Exception as exc:
         return error_response(message=str(exc))

    return JsonResponse(v_return)


@superuser_required
@user_authenticated
def remove_user(request):
    v_return = create_response_template()

    v_id = request.data['p_id']

    try:
        user = User.objects.get(id=v_id)
        key_manager.remove(user)
        user.delete()
    except Exception as exc:
        return error_response(message=str(exc))

    return JsonResponse(v_return)


@superuser_required
@user_authenticated
def save_users(request):
    v_return = create_response_template()

    data = request.data

    try:
        v_data = data['p_data']

        # Creating new users.
        v_data_new = v_data['new']
        for user_item in v_data_new:
            new_user = User.objects.create_user(
                username=user_item[0],
                password=user_item[1],
                email='',
                last_login=timezone.now(),
                is_superuser=True if user_item[2] else False,
                first_name='',
                last_name='',
                is_staff=False,
                is_active=True,
                date_joined=timezone.now())

        # Editing users.
        v_data_edited = v_data['edited']
        v_user_id_list = data['p_user_id_list']
        for v_index, r in enumerate(v_data_edited):
            user = User.objects.get(id=v_user_id_list[v_index])
            user.username = r[0]
            if r[1]:
                user.set_password(r[1])
            user.is_superuser = True if r[2] else False
            user.save()

            if request.user == user and r[1]:
                update_session_auth_hash(request, user)

    except Exception as exc:
        return error_response(message=str(exc))

    return JsonResponse(v_return)
