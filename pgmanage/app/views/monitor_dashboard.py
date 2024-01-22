from app.models.main import (Connection, MonUnits, MonUnitsConnections,
                             Technology)
from app.utils.decorators import (database_required, database_required_new,
                                  session_required, user_authenticated)
from app.utils.response_helpers import create_response_template, error_response
from app.views.monitoring_units import mysql as mysql_units
from app.views.monitoring_units import postgresql as postgresql_units
from django.http import HttpResponse, JsonResponse
from RestrictedPython import compile_restricted, safe_builtins
from RestrictedPython.Eval import default_guarded_getitem

monitoring_units_database = {}
monitoring_units = {}

def get_units_data():
    try:
        for mon_unit in postgresql_units.monitoring_units:
            monitoring_units[(mon_unit['plugin_name'],mon_unit['id'])] = mon_unit
    except Exception as exc:
        None
    try:
        for mon_unit in mysql_units.monitoring_units:
            monitoring_units[(mon_unit['plugin_name'],mon_unit['id'])] = mon_unit
    except Exception as exc:
        None

    #Retrieving monitoring units from database to use as reference
    try:
        for mon_unit in MonUnits.objects.all():
            monitoring_units_database[mon_unit.id] = mon_unit

    # No mon units connections
    except Exception as exc:
        None

get_units_data()

def _hook_import(name, *args, **kwargs):
    if name =='os':
        raise RuntimeError('You cannot import os module in this sandbox.')
    return __import__(name,*args,**kwargs)

@user_authenticated
@database_required(p_check_timeout = False, p_open_connection = False)
def get_monitor_unit_list(request, v_database):
    v_return = create_response_template()

    v_mode = request.data['p_mode']
    v_return['v_data'] = []
    v_data = []
    v_id_list = []

    try:
        #plugins units
        for key, mon_unit in monitoring_units.items():
            if mon_unit['dbms'] == v_database.v_db_type:
                v_actions = False
                if v_mode == 0:
                    v_data.append({'id': mon_unit['id'],
                                   'actions': v_actions,
                                   'title': mon_unit['title'],
                                   'type': mon_unit['type'],
                                   'interval': mon_unit['interval'],
                                   'plugin_name': mon_unit['plugin_name']})
                else:
                    v_data.append({'plugin_name': mon_unit['plugin_name'],
                                   'title': mon_unit['title'],
                                   'type': mon_unit['type']})
                v_id_list.append(mon_unit['id'])

        try:
            for key, mon_unit in monitoring_units_database.items():
                actions = False
                # custom unit, add edit and delete actions
                if mon_unit.user is not None:
                    actions = True

                if v_mode == 0:
                    v_data.append({'id': mon_unit.id,
                                   'actions': actions,
                                   'title': mon_unit.title,
                                   'type': mon_unit.type,
                                   'interval': mon_unit.interval})
                else:
                    v_data.append({'plugin_name': '',
                                   'title': mon_unit.title,
                                   'type': mon_unit.type})

                v_id_list.append(mon_unit.id)
        # No mon units connections
        except Exception as exc:
            print(str(exc))

        v_return['v_data'] = {'id_list': v_id_list, 'data': v_data}
    except Exception as exc:
        return error_response(message=str(exc))

    return JsonResponse(v_return)

@user_authenticated
@session_required(use_old_error_format=True, include_session=False)
def get_monitor_unit_details(request):
    v_return = create_response_template()

    v_unit_id = request.data['p_unit_id']

    try:
        unit = MonUnits.objects.get(id=v_unit_id)
        v_return['v_data'] = { 'title': unit.title, 'type': unit.type, 'interval': unit.interval, 'script_chart': unit.script_chart, 'script_data': unit.script_data }

    except Exception as exc:
        return error_response(message=str(exc))

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = False, p_open_connection = False)
def get_monitor_units(request, v_database):
    v_return = create_response_template()

    v_database_index = request.data['p_database_index']

    v_return['v_data'] = []

    try:
        user_units = MonUnitsConnections.objects.filter(user=request.user,connection=v_database_index)

        # There are no units for this user/connection pair, create defaults
        if len(user_units)==0:
            conn_object = Connection.objects.get(id=v_database.v_conn_id)
            for key, mon_unit in monitoring_units.items():
                if mon_unit['default'] == True and mon_unit['dbms'] == v_database.v_db_type:
                    user_unit = MonUnitsConnections(
                        unit=mon_unit['id'],
                        user=request.user,
                        connection=conn_object,
                        interval=mon_unit['interval'],
                        plugin_name=mon_unit['plugin_name']
                    )
                    user_unit.save()

            # Retrieve user units again
            user_units = MonUnitsConnections.objects.filter(user=request.user,connection=v_database_index)

        for user_unit in user_units:
            if user_unit.plugin_name=='':
                try:
                    unit_default_data = MonUnits.objects.get(id=user_unit.unit)
                    v_unit_data = {
                        'v_saved_id': user_unit.id,
                        'v_id': unit_default_data.id,
                        'v_title': unit_default_data.title,
                        'v_plugin_name': '',
                        'v_interval': user_unit.interval
                    }
                    v_return['v_data'].append(v_unit_data)
                except:
                    user_unit.delete()
            else:
                #search plugin data
                unit_data = None
                found = False
                for key, mon_unit in monitoring_units.items():
                    if mon_unit['id'] == user_unit.unit and mon_unit['plugin_name'] == user_unit.plugin_name and mon_unit['dbms'] == v_database.v_db_type:
                        found = True
                        v_unit_data = {
                            'v_saved_id': user_unit.id,
                            'v_id': user_unit.unit,
                            'v_title': mon_unit['title'],
                            'v_plugin_name': user_unit.plugin_name,
                            'v_interval': user_unit.interval
                        }
                        v_return['v_data'].append(v_unit_data)
                        break
                if not found:
                    user_unit.delete()

    # No mon units connections
    except Exception as exc:
        print(str(exc))
        None

    return JsonResponse(v_return)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=False)
def get_monitor_widgets(request, database):
    database_index = request.data.get("database_index")
    widgets = []
    try:
        user_widgets = MonUnitsConnections.objects.filter(
            user=request.user, connection=database_index
        )

        # There are no units for this user/connection pair, create defaults

        if not len(user_widgets):
            conn_object = Connection.objects.get(id=database_index)

            for _, mon_unit in monitoring_units.items():
                if (
                    mon_unit.get("default") is True
                    and mon_unit.get("dbms") == database.v_db_type
                ):
                    user_widget = MonUnitsConnections(
                        unit=mon_unit.get("id"),
                        user=request.user,
                        connection=conn_object,
                        interval=mon_unit.get("interval"),
                        plugin_name=mon_unit.get("plugin_name"),
                    )
                    user_widget.save()

            # Retrieve user units again
            user_widgets = MonUnitsConnections.objects.filter(
                user=request.user, connection=database_index
            )

        for user_widget in user_widgets:
            if user_widget.plugin_name == "":
                try:
                    default_widget = MonUnits.objects.get(id=user_widget.unit)
                    widget = {
                        "saved_id": user_widget.id,
                        "id": default_widget.id,
                        "title": default_widget.title,
                        "plugin_name": "",
                        "interval": user_widget.interval,
                        "type": default_widget.type,
                        "widget_data": None,
                    }
                    widgets.append(widget)
                except Exception:
                    user_widget.delete()
            else:
                # search plugin data
                found = False

                for _, mon_unit in monitoring_units.items():
                    if (
                        mon_unit.get("id") == user_widget.unit
                        and mon_unit.get("plugin_name") == user_widget.plugin_name
                        and mon_unit.get("dbms") == database.v_db_type
                    ):
                        found = True
                        widget = {
                            "saved_id": user_widget.id,
                            "id": user_widget.unit,
                            "title": mon_unit.get("title"),
                            "plugin_name": user_widget.plugin_name,
                            "interval": user_widget.interval,
                            "type": mon_unit.get("type"),
                            "widget_data": None,
                        }
                        widgets.append(widget)
                        break
                if not found:
                    user_widget.delete()
    # No mon widgets connections
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)
    return JsonResponse(data={"widgets": widgets})


@user_authenticated
@session_required(use_old_error_format=True, include_session=False)
def get_monitor_unit_template(request):
    v_return = create_response_template()

    data = request.data
    v_unit_id = data['p_unit_id']
    v_unit_plugin_name = data['p_unit_plugin_name']

    if v_unit_plugin_name=='':

        v_return['v_data'] = ''

        try:
            unit = MonUnits.objects.get(id=v_unit_id)
            v_return['v_data'] = {
                'script_chart': unit.script_chart,
                'script_data': unit.script_data,
                'type': unit.type,
                'interval': unit.interval
            }

        except Exception as exc:
            None
    else:
        #search plugin data
        for key, mon_unit in monitoring_units.items():
            if mon_unit['id'] == v_unit_id and mon_unit['plugin_name'] == v_unit_plugin_name:
                unit_data = mon_unit
                v_return['v_data'] = {
                    'interval': unit_data['interval'],
                    'script_chart': unit_data['script_chart'],
                    'script_data': unit_data['script_data'],
                    'type': unit_data['type']
                }
                break

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = False, p_open_connection = False)
def save_monitor_unit(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_unit_id = data['p_unit_id']
    v_unit_name = data['p_unit_name']
    v_unit_type = data['p_unit_type']
    v_unit_interval = data['p_unit_interval']
    v_unit_script_chart = data['p_unit_script_chart']
    v_unit_script_data = data['p_unit_script_data']

    if v_unit_interval==None:
        v_unit_interval = 30

    try:
        #new unit
        if not v_unit_id:
            unit = MonUnits(
                user=request.user,
                technology=Technology.objects.get(name=v_database.v_db_type),
                script_chart=v_unit_script_chart,
                script_data=v_unit_script_data,
                type=v_unit_type,
                title=v_unit_name,
                is_default=False,
                interval=v_unit_interval
            )
            unit.save()
            v_return['v_data'] = unit.id
        #existing unit
        else:
            v_return['v_data'] = v_unit_id
            unit = MonUnits.objects.get(id=v_unit_id)
            unit.script_chart = v_unit_script_chart
            unit.script_data = v_unit_script_data
            unit.type = v_unit_type
            unit.title = v_unit_name
            unit.interval = v_unit_interval
            unit.save()

        monitoring_units_database[unit.id] = unit

    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@session_required(use_old_error_format=True, include_session=False)
def delete_monitor_unit(request):
    v_return = create_response_template()

    v_unit_id = request.data['p_unit_id']

    try:
        MonUnits.objects.get(id=v_unit_id).delete()
        del monitoring_units_database[v_unit_id]

    except Exception as exc:
        return error_response(message=str(exc))

    return JsonResponse(v_return)

@user_authenticated
@session_required(use_old_error_format=True, include_session=False)
def remove_saved_monitor_unit(request):
    v_return = create_response_template()

    v_saved_id = request.data['p_saved_id']
    try:
        MonUnitsConnections.objects.get(id=v_saved_id).delete()

    except Exception as exc:
        return error_response(message=str(exc))

    return JsonResponse(v_return)


@user_authenticated
@session_required(include_session=False)
def remove_saved_monitor_widget(request):
    saved_id = request.data.get("saved_id")

    try:
        MonUnitsConnections.objects.get(id=saved_id).delete()
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)
    return HttpResponse(status=204)


@user_authenticated
@session_required(use_old_error_format=True, include_session=False)
def update_saved_monitor_unit_interval(request):
    v_return = create_response_template()

    data = request.data
    v_saved_id = data['p_saved_id']
    v_interval = data['p_interval']

    try:
        unit = MonUnitsConnections.objects.get(id=v_saved_id)
        unit.interval = v_interval
        unit.save()
    except Exception as exc:
        return error_response(message=str(exc))

    return JsonResponse(v_return)


@user_authenticated
@session_required(include_session=False)
def update_saved_monitor_widget_interval(request):
    data = request.data
    saved_id = data.get("saved_id")
    interval = data.get("interval")

    try:
        unit = MonUnitsConnections.objects.get(id=saved_id)
        unit.interval = interval
        unit.save()
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)
    return HttpResponse(status=204)


@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def refresh_monitor_units(request, v_database):
    v_return = create_response_template()

    v_ids = request.data['p_ids']

    v_return['v_data'] = []

    if len(v_ids) > 0:
        v_first = True
        v_query = ''
        unit_counter = 0
        conn_object = Connection.objects.get(id=v_database.v_conn_id)

        for v_id in v_ids:
            #save new user/connection unit
            if v_id['saved_id'] == -1:
                try:
                    user_unit = MonUnitsConnections(
                        unit=v_id['id'],
                        user=request.user,
                        connection=conn_object,
                        interval=v_id['interval'],
                        plugin_name=v_id['plugin_name']
                    )
                    user_unit.save()
                    v_id['saved_id'] = user_unit.id
                except Exception as exc:
                    v_return['v_data'] = str(exc)
                    v_return['v_error'] = True
                    return JsonResponse(v_return)

            if v_id['plugin_name']=='':

                unit_data = monitoring_units_database[v_id['id']]

                script_data = unit_data.script_data
                script_chart = unit_data.script_chart

                v_unit_data = {
                    'v_saved_id': v_id['saved_id'],
                    'v_id': v_id['id'],
                    'v_sequence': v_id['sequence'],
                    'v_type': unit_data.type,
                    'v_title': unit_data.title,
                    'v_interval': unit_data.interval,
                    'v_object': None,
                    'v_error': False
                }

            #plugin unit
            else:
                #search plugin data
                unit_data = None
                for key, mon_unit in monitoring_units.items():
                    if mon_unit['id'] == v_id['id'] and mon_unit['plugin_name'] == v_id['plugin_name']:
                        unit_data = mon_unit
                        break

                script_data = unit_data['script_data']
                script_chart = unit_data['script_chart']

                v_unit_data = {
                    'v_saved_id': v_id['saved_id'],
                    'v_id': unit_data['id'],
                    'v_sequence': v_id['sequence'],
                    'v_type': unit_data['type'],
                    'v_title': unit_data['title'],
                    'v_interval': unit_data['interval'],
                    'v_object': None,
                    'v_error': False
                }

            try:
                v_unit_data = {
                    'v_saved_id': v_id['saved_id'],
                    'v_id': v_unit_data['v_id'],
                    'v_sequence': v_unit_data['v_sequence'],
                    'v_type': v_unit_data['v_type'],
                    'v_title': v_unit_data['v_title'],
                    'v_interval': v_unit_data['v_interval'],
                    'v_object': None,
                    'v_error': False
                }

                loc1 = {
                    "connection": v_database,
                    "previous_data": v_ids[unit_counter]['object_data']
                }

                loc2 = {
                    "connection": v_database,
                    "previous_data": v_ids[unit_counter]['object_data']
                }

                restricted_globals = dict(__builtins__=safe_builtins)
                restricted_globals['_getiter_'] = iter
                restricted_globals['_getattr_'] = getattr
                restricted_globals['_getitem_'] = default_guarded_getitem
                restricted_globals['__builtins__']['__import__'] = _hook_import

                byte_code = compile_restricted(script_data, '<inline>', 'exec')
                exec(byte_code, restricted_globals, loc1)
                data = loc1['result']
                
                if v_id["rendered"] == 1 and v_unit_data["v_type"] != "grid":
                    v_unit_data["v_object"] = data
                elif v_unit_data["v_type"]  == "grid":
                    v_unit_data["data"] = [dict(row) for row in data.get("data", [])]
                elif v_unit_data['v_type'] == 'graph':
                    byte_code = compile_restricted(script_chart, '<inline>', 'exec')
                    exec(byte_code, restricted_globals, loc2)
                    result = loc2['result']
                    result['elements'] = data
                    v_unit_data['v_object'] = result
                else:
                    byte_code = compile_restricted(script_chart, '<inline>', 'exec')
                    exec(byte_code, restricted_globals, loc2)
                    result = loc2['result']
                    result['data'] = data
                    v_unit_data['v_object'] = result

                v_return['v_data'].append(v_unit_data)
            except Exception as exc:
                v_unit_data = {
                    'v_saved_id': v_id['saved_id'],
                    'v_id': v_unit_data['v_id'],
                    'v_sequence': v_unit_data['v_sequence'],
                    'v_type': v_unit_data['v_type'],
                    'v_title': v_unit_data['v_title'],
                    'v_interval': v_unit_data['v_interval'],
                    'v_object': None,
                    'v_error': True,
                    'v_message': str(exc)
                }
                v_return['v_data'].append(v_unit_data)

            unit_counter = unit_counter + 1

        return JsonResponse(v_return)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def refresh_monitor_widget(request, database):
    widget = request.data.get('widget')

    conn_object = Connection.objects.get(id=database.v_conn_id)

    # save new user/connection unit
    if widget.get("saved_id") == -1:
        try:
            user_unit = MonUnitsConnections(
                unit=widget.get("id"),
                user=request.user,
                connection=conn_object,
                interval=widget.get("interval"),
                plugin_name=widget.get("plugin_name")
            )
            user_unit.save()
            widget["saved_id"] = user_unit.id
        except Exception as exc:
            return JsonResponse(data={"data": str(exc)}, status=400)

    if widget.get("plugin_name") == "":
        unit_data = monitoring_units_database.get(widget.get("id"))

        script_data = unit_data.script_data
        script_chart = unit_data.script_chart

        unit_data = {
            'saved_id': widget.get('saved_id'),
            'id': widget.get('id'),
            'type': unit_data.type,
            'title': unit_data.title,
            'interval': unit_data.interval,
        }

    # plugin unit
    else:
        # search plugin data

        unit_data = None

        for _, mon_unit in monitoring_units.items():
            if mon_unit.get('id') == widget.get('id') and mon_unit.get('plugin_name') == widget.get('plugin_name'):
                unit_data = mon_unit
                break
        
        script_data = unit_data['script_data']
        script_chart = unit_data['script_chart']

        unit_data = {
            'saved_id': widget['saved_id'],
            'id': unit_data['id'],
            'type': unit_data['type'],
            'title': unit_data['title'],
            'interval': unit_data['interval'],
        }

    try:
        unit_data = {
                    'saved_id': widget['saved_id'],
                    'id': unit_data['id'],
                    'type': unit_data['type'],
                    'title': unit_data['title'],
                    'interval': unit_data['interval'],
                }

        loc1 = {
            "connection": database,
            "previous_data": widget.get('widget_data')
        }

        loc2 = {
            "connection": database,
            "previous_data": widget.get('widget_data')
        }

        restricted_globals = dict(__builtins__=safe_builtins)
        restricted_globals['_getiter_'] = iter
        restricted_globals['_getattr_'] = getattr
        restricted_globals['_getitem_'] = default_guarded_getitem
        restricted_globals['__builtins__']['__import__'] = _hook_import

        byte_code = compile_restricted(script_data, '<inline>', 'exec')
        exec(byte_code, restricted_globals, loc1)
        data = loc1['result']

        if not widget.get("initial") and unit_data["type"] in ["chart", "timeseries"]:
            unit_data["object"] = data
        elif unit_data["type"]  == "grid":
            unit_data["data"] = [dict(row) for row in data.get("data", [])]
        elif unit_data['type'] == 'graph':
            byte_code = compile_restricted(script_chart, '<inline>', 'exec')
            exec(byte_code, restricted_globals, loc2)
            result = loc2['result']
            result['elements'] = data
            unit_data['object'] = result
        else:
            byte_code = compile_restricted(script_chart, '<inline>', 'exec')
            exec(byte_code, restricted_globals, loc2)
            result = loc2['result']
            result['data'] = data
            unit_data['object'] = result

    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)
    
    return JsonResponse(unit_data)


@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def test_monitor_script(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_script_chart = data['p_script_chart']
    v_script_data = data['p_script_data']
    v_type = data['p_type']


    v_return['v_data'] = {
        'v_object': None,
        'v_error': False
    }

    try:
        loc1 = {
            "connection": v_database,
            "previous_data": None
        }

        loc2 = {
            "connection": v_database,
            "previous_data": None
        }

        restricted_globals = dict(__builtins__=safe_builtins)
        restricted_globals['_getiter_'] = iter
        restricted_globals['_getattr_'] = getattr
        restricted_globals['_getitem_'] = default_guarded_getitem
        restricted_globals['__builtins__']['__import__'] = _hook_import

        byte_code = compile_restricted(v_script_data, '<inline>', 'exec')
        exec(byte_code, restricted_globals, loc1)
        data = loc1['result']

        if v_type  == 'grid':
            v_return['v_data']['data'] = [dict(row) for row in data.get("data", [])]
        elif v_type == 'graph':
            byte_code = compile_restricted(v_script_chart, '<inline>', 'exec')
            exec(byte_code, restricted_globals, loc2)
            result = loc2['result']
            result['elements'] = data
            v_return['v_data']['v_object'] = result
        else:
            byte_code = compile_restricted(v_script_chart, '<inline>', 'exec')
            exec(byte_code, restricted_globals, loc2)
            result = loc2['result']
            result['data'] = data
            v_return['v_data']['v_object'] = result

    except Exception as exc:
        v_unit_data = {
            'v_object': None,
            'v_error': True,
            'v_message': str(exc)
        }
        v_return['v_data'] = v_unit_data


    return JsonResponse(v_return)
