from django.http import HttpResponse
from django.template import loader
from django.http import JsonResponse
from django.core import serializers

import sys

import app.include.Spartacus as Spartacus
import app.include.Spartacus.Database as Database
import app.include.Spartacus.Utils as Utils
from app.include.Session import Session

from django.contrib.auth.models import User
from app.models.main import *

from datetime import datetime
from django.utils.timezone import make_aware

from app.utils.decorators import session_required
from app.utils.response_helpers import create_response_template, error_response


@session_required(use_old_error_format=True, include_session=False)
def get_all_snippets(request):
    v_return = create_response_template()

    v_folders = SnippetFolder.objects.filter(user=request.user)
    v_files = SnippetFile.objects.filter(user=request.user)

    v_root = {
        'id': None,
        'files': [],
        'folders': []
    }

    build_snippets_object_recursive(v_folders,v_files,v_root)

    return JsonResponse(v_root)

def build_snippets_object_recursive(p_folders,p_files,p_current_object):
    # Adding files
    for file in p_files:
        # Match
        if ((file.parent == None and p_current_object['id'] == None) or (file.parent!=None and file.parent.id == p_current_object['id'])):
            p_current_object['files'].append(
            {
                'id': file.id,
                'name': file.name
            }
            )
    # Adding folders
    for folder in p_folders:
        # Match
        if ((folder.parent == None and p_current_object['id'] == None) or (folder.parent!=None and folder.parent.id == p_current_object['id'])):
            v_folder = {
                'id': folder.id,
                'name': folder.name,
                'files': [],
                'folders': []
            }

            build_snippets_object_recursive(p_folders,p_files,v_folder)

            p_current_object['folders'].append(v_folder)


@session_required(use_old_error_format=True, include_session=False)
def get_node_children(request):
    v_return = create_response_template()

    v_sn_id_parent = request.data['p_sn_id_parent']

    v_return['v_data'] = {
        'v_list_nodes': [],
        'v_list_texts': []
    }

    try:
        for folder in SnippetFolder.objects.filter(user=request.user,parent=v_sn_id_parent):
            v_node_data = {
                'v_id': folder.id,
                'v_name': folder.name
            }
            v_return['v_data']['v_list_nodes'].append(v_node_data)
    except Exception as exc:
        None

    try:
        for file in SnippetFile.objects.filter(user=request.user,parent=v_sn_id_parent):
            v_node_data = {
                'v_id': file.id,
                'v_name': file.name
            }
            v_return['v_data']['v_list_texts'].append(v_node_data)
    except Exception as exc:
        None

    return JsonResponse(v_return)

@session_required(use_old_error_format=True, include_session=False)
def get_snippet_text(request):
    v_return = create_response_template()

    v_st_id = request.data['p_st_id']

    try:
        v_return['v_data'] = SnippetFile.objects.get(id=v_st_id).text
    except Exception as exc:
        None

    return JsonResponse(v_return)

@session_required(use_old_error_format=True, include_session=False)
def new_node_snippet(request):
    v_return = create_response_template()

    data = request.data
    v_sn_id_parent = data['p_sn_id_parent']
    v_mode = data['p_mode']
    v_name = data['p_name']

    if v_sn_id_parent:
        v_parent = SnippetFolder.objects.get(id=v_sn_id_parent)
    else:
        v_parent = None

    try:
        new_date = make_aware(datetime.now())
        if v_mode == 'node':
            folder = SnippetFolder(
                user=request.user,
                parent=v_parent,
                name=v_name,
                create_date=new_date,
                modify_date=new_date
            )
            folder.save()
        else:
            file = SnippetFile(
                user=request.user,
                parent=v_parent,
                name=v_name,
                create_date=new_date,
                modify_date=new_date,
                text=''
            )
            file.save()
    except Exception as exc:
        return error_response(message=str(exc))
    return JsonResponse(v_return)


@session_required(use_old_error_format=True, include_session=False)
def delete_node_snippet(request):
    v_return = create_response_template()

    data = request.data
    v_id = data['p_id']
    v_mode = data['p_mode']

    try:
        if v_mode == 'node':
            folder = SnippetFolder.objects.get(id=v_id)
            folder.delete()
        else:
            file = SnippetFile.objects.get(id=v_id)
            file.delete()

    except Exception as exc:
        return error_response(message=str(exc))
    return JsonResponse(v_return)


@session_required(use_old_error_format=True, include_session=False)
def save_snippet_text(request):
    v_return = create_response_template()

    data = request.data
    v_id = data['p_id']
    v_name = data['p_name']
    v_parent_id = data['p_parent']
    v_text = data['p_text']

    if v_parent_id:
        v_parent = SnippetFolder.objects.get(id=v_parent_id)
    else:
        v_parent = None

    try:
        #new snippet
        new_date = make_aware(datetime.now())
        if not v_id:
            file = SnippetFile(
                user=request.user,
                parent=v_parent,
                name=v_name,
                create_date=new_date,
                modify_date=new_date,
                text=v_text
            )
            file.save()
        #existing snippet
        else:
            file = SnippetFile.objects.get(id=v_id)
            file.text = v_text.replace("'", "''")
            file.modify_date=new_date
            file.save()

        v_return['v_data'] = {
            'type': 'snippet',
            'id': file.id,
            'parent': v_parent_id,
            'name': file.name
        }


    except Exception as exc:
        return error_response(message=str(exc))
    return JsonResponse(v_return)


@session_required(use_old_error_format=True, include_session=False)
def rename_node_snippet(request):
    v_return = create_response_template()

    data = request.data
    v_id = data['p_id']
    v_name = data['p_name']
    v_mode = data['p_mode']

    try:
        #node
        if v_mode=='node':
            folder = SnippetFolder.objects.get(id=v_id)
            folder.name = v_name.replace("'", "''")
            folder.modify_date=make_aware(datetime.now())
            folder.save()
        #snippet
        else:
            file = SnippetFile.objects.get(id=v_id)
            file.name = v_name.replace("'", "''")
            file.modify_date=make_aware(datetime.now())
            file.save()


    except Exception as exc:
        return error_response(message=str(exc))
    return JsonResponse(v_return)
