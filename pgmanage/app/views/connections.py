import io

import paramiko
from app.include import OmniDatabase
from app.models import Connection, Group, GroupConnection, Tab, Technology
from app.utils.crypto import decrypt, encrypt
from app.utils.decorators import session_required, user_authenticated
from app.utils.key_manager import key_manager
from django.db.models import Q
from django.http import JsonResponse
from sshtunnel import SSHTunnelForwarder


@user_authenticated
@session_required
def get_connections(request, session):
    response_data = {'data': [], 'status': 'success'}

    active_connection_ids = request.data.get('active_connection_ids',[])

    tech_list = [tech.name for tech in Technology.objects.all()] #convert to values_list

    connection_list = []
    connections = Connection.objects.filter(Q(user=request.user) | Q(public=True))

    if connections and key_manager.get(request.user):
        for conn in connections:
            # FIXME: refactor this into a proper join
            gc = GroupConnection.objects.filter(connection=conn).select_related().first()

            conn_object = {
                'id': conn.id,
                'locked': conn.id in active_connection_ids,
                'public': conn.public,
                'is_mine': conn.user.id == request.user.id,
                'technology': conn.technology.name,
                'group': gc.group.id if gc else None,
                'alias': conn.alias,
                'conn_string': '',
                'server': '',
                'port': '',
                'service': '',
                'user': '',
                'tunnel': {
                    'enabled': conn.use_tunnel,
                    'server': conn.ssh_server,
                    'port': conn.ssh_port,
                    'user': conn.ssh_user,
                    'password': '',
                    'password_set': False if conn.ssh_password.strip() == '' else True,
                    'key': '',
                    'key_set': False if conn.ssh_key.strip() == '' else True
                },
                'connection_params': conn.connection_params,
                'last_used_database': conn.last_used_database
            }
            database_object = session.v_databases.get(conn.id)

            if conn.technology.name == 'terminal':
                details = (
                    database_object["tunnel"]["user"]
                    + "@"
                    + database_object["tunnel"]["server"]
                    + ":"
                    + database_object["tunnel"]["port"]
                )
                conn_object['details1'] = details

            if conn.technology.name != 'terminal':
                database = database_object.get('database')

                details = database.PrintDatabaseDetails()
                if database_object["tunnel"]["enabled"]:
                    details += f" ({database_object['tunnel']['server']}:{database_object['tunnel']['port']})"
                conn_object['console_help'] = database.v_console_help
                conn_object['details1'] = database.PrintDatabaseInfo()
                conn_object['details2'] = details
                conn_object['conn_string'] = conn.conn_string
                conn_object['server'] = conn.server
                conn_object['port'] = conn.port
                conn_object['service'] = conn.database
                conn_object['user'] = conn.username
                conn_object['password'] = ''
                conn_object['password_set'] = False if conn.password.strip() == '' else True

            connection_list.append(conn_object)

    response_data['data'] = {
        'connections': connection_list,
        'technologies': tech_list
    }

    return JsonResponse(response_data)


@user_authenticated
@session_required(include_session=False)
def get_groups(request):
    response_data = {'data': [], 'status': 'success'}

    groups = Group.objects.filter(user=request.user)

    for group in groups:
        current_group_data = {
            'id': group.id,
            'name': group.name,
            'conn_list': list(GroupConnection.objects.filter(group=group).values_list('connection__id', flat=True))
        }

        response_data['data'].append(current_group_data)

    return JsonResponse(response_data)


@user_authenticated
def delete_group(request):
    response_data = {'data': '', 'status': 'success'}

    group_id = request.data['id']

    try:
        group = Group.objects.get(id=group_id)

        if group.user.id != request.user.id:
            response_data['data'] = 'This group does not belong to you.'
            response_data['status'] = 'failed'
            return JsonResponse(response_data, status=403)

        group.delete()

    except Exception as exc:
        response_data['data'] = str(exc)
        response_data['status'] = 'failed'
        return JsonResponse(response_data, status=400)

    return JsonResponse(response_data)


@user_authenticated
def save_group(request):
    response_data = {'data': '', 'status': 'success'}

    group_object = request.data
    group_id = group_object.get('id', None)
    group_name = group_object['name']

    if not group_name.strip():
        response_data['data'] = "Group name can not be empty."
        response_data['status'] = 'failed'

        return JsonResponse(response_data, status=400)

    try:
        # New group
        if group_id is None:
            group = Group(user=request.user, name=group_name)
            group.save()
        # update
        else:
            group = Group.objects.get(id=group_id)

            if group.user.id != request.user.id:
                response_data['data'] = 'This group does not belong to you.'
                response_data['status'] = 'failed'

                return JsonResponse(response_data, status=403)

            group.name = group_name
            group.save()

        # cleanup other group relations which reference same connection ids
        # sanitize incoming conn ids vs request.user
        conns = Connection.objects.filter(user=request.user, id__in=group_object['conn_list'])
        # delete all group members, then recreate from conns
        GroupConnection.objects.filter(group=group).delete()
        GroupConnection.objects.bulk_create(
            [GroupConnection(group=group, connection=conn) for conn in conns]
        )
        # create new group relations with group.id

    except Exception as exc:
        response_data['data'] = str(exc)
        response_data['status'] = 'failed'
        return JsonResponse(response_data, status=400)


    return JsonResponse(response_data)


@user_authenticated
@session_required(include_session=False)
def test_connection(request):
    response_data = {'data': '', 'status': 'success'}

    conn_object = request.data
    conn_id = conn_object['id']
    conn_type = conn_object['technology']

    conn_params = conn_object['connection_params']


    password = conn_object['password'].strip()
    ssh_password = conn_object['tunnel']['password'].strip()
    ssh_key = conn_object['tunnel']['key']
    key = key_manager.get(request.user)

    if conn_id:
        conn = Connection.objects.get(id=conn_id)
        if conn_object['password'].strip() == '':
            password = decrypt(conn.password, key) if conn.password else ''
        if conn_object['tunnel']['password'].strip() == '':
            ssh_password = decrypt(conn.ssh_password, key) if conn.ssh_password else ''
        if conn_object['tunnel']['key'].strip() == '':
            ssh_key = decrypt(conn.ssh_key, key) if conn.ssh_key else ''

    # if conn_object['temp_password']:
    #     password = conn_object['temp_password']

    if conn_type == 'terminal':

        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # ssh key provided
            if ssh_key.strip() != '':
                key = paramiko.RSAKey.from_private_key(io.StringIO(ssh_key), password=ssh_password)
                client.connect(hostname=conn_object['tunnel']['server'], username=conn_object['tunnel']['user'],
                               pkey=key, passphrase=ssh_password,
                               port=int(conn_object['tunnel']['port']))
            else:
                client.connect(hostname=conn_object['tunnel']['server'], username=conn_object['tunnel']['user'],
                               password=ssh_password, port=int(conn_object['tunnel']['port']))

            client.close()
            response_data['data'] = 'Connection successful.'
        except Exception as exc:
            response_data['data'] = str(exc)
            response_data['status'] = 'failed'
            return JsonResponse(response_data, status=400)
    else:

        database = OmniDatabase.Generic.InstantiateDatabase(
            conn_type,
            conn_object['server'],
            conn_object['port'],
            conn_object['service'],
            conn_object['user'],
            password,
            -1,
            '',
            p_conn_string=conn_object['conn_string'],
            p_parse_conn_string=True,
            connection_params=conn_params
        )

        # create tunnel if enabled
        if conn_object['tunnel']['enabled']:

            try:
                if ssh_key.strip() != '':
                    key = paramiko.RSAKey.from_private_key(io.StringIO(ssh_key), password=ssh_password)
                    server = SSHTunnelForwarder(
                        (conn_object['tunnel']['server'], int(conn_object['tunnel']['port'])),
                        ssh_username=conn_object['tunnel']['user'],
                        ssh_private_key_password=ssh_password,
                        ssh_pkey=key,
                        remote_bind_address=(database.v_active_server, int(database.v_active_port)),
                        logger=None
                    )
                else:
                    server = SSHTunnelForwarder(
                        (conn_object['tunnel']['server'], int(conn_object['tunnel']['port'])),
                        ssh_username=conn_object['tunnel']['user'],
                        ssh_password=ssh_password,
                        remote_bind_address=(database.v_active_server, int(database.v_active_port)),
                        logger=None
                    )
                server.set_keepalive = 120
                server.start()

                database.v_connection.v_host = '127.0.0.1'
                database.v_connection.v_port = server.local_bind_port

                message = database.TestConnection()
                server.close()
                response_data['data'] = message
                if message != 'Connection successful.':
                    response_data['status'] = 'failed'

            except Exception as exc:
                response_data['data'] = str(exc)
                response_data['status'] = 'failed'
                return JsonResponse(response_data, status=400)

        else:
            message = database.TestConnection()
            response_data['data'] = message
            if message != 'Connection successful.':
                response_data['status'] = 'failed'

    return JsonResponse(response_data)


@user_authenticated
@session_required
def save_connection(request, session):
    response_data = {'data': '', 'status': 'success'}
    key = key_manager.get(request.user)

    conn_object = request.data
    conn_id = conn_object['id']

    try:
        # New connection
        if conn_id is None:
            password = ''
            tunnel_password = ''
            tunnel_key = ''
            if conn_object['password']:
                password = encrypt(conn_object['password'], key)
            if conn_object['tunnel']['password'] != '':
                tunnel_password = encrypt(conn_object['tunnel']['password'], key)
            if conn_object['tunnel']['key'] != '':
                tunnel_key = encrypt(conn_object['tunnel']['key'], key)
            conn = Connection(
                user=request.user,
                technology=Technology.objects.get(name=conn_object['technology']),
                server=conn_object['server'],
                port=conn_object['port'],
                database=conn_object['service'],
                username=conn_object['user'],
                password=password,
                alias=conn_object['alias'],
                ssh_server=conn_object['tunnel']['server'],
                ssh_port=conn_object['tunnel']['port'],
                ssh_user=conn_object['tunnel']['user'],
                ssh_password=tunnel_password,
                ssh_key=tunnel_key,
                use_tunnel=conn_object['tunnel']['enabled'],
                conn_string=conn_object['conn_string'],
                public=False,
                connection_params=conn_object['connection_params']
            )
            conn.save()
        # update
        else:
            conn = Connection.objects.get(id=conn_id)

            if conn.user.id != request.user.id:
                response_data['data'] = 'This connection does not belong to you.'
                response_data['status'] = 'failed'
                return JsonResponse(response_data, status=403)

            conn.technology = Technology.objects.get(name=conn_object['technology'])
            conn.server = conn_object['server']
            conn.port = conn_object['port']
            conn.database = conn_object['service']
            conn.username = conn_object['user']
            if conn_object['password'] is not None:
                if conn_object['password'].strip() != '':
                    conn.password = encrypt(conn_object['password'], key)
            conn.alias = conn_object['alias']
            conn.ssh_server = conn_object['tunnel']['server']
            conn.ssh_port = conn_object['tunnel']['port']
            conn.ssh_user = conn_object['tunnel']['user']
            if conn_object['tunnel']['password'] is not None:
                if conn_object['tunnel']['password'].strip() != '':
                    conn.ssh_password = encrypt(conn_object['tunnel']['password'], key)
            if conn_object['tunnel']['key'].strip() != '':
                conn.ssh_key = encrypt(conn_object['tunnel']['key'], key)
            
            if conn.technology.name in ['mariadb', 'mysql']:
                conn.connection_params = {}

            for k, v in conn_object["connection_params"].items():
                conn.connection_params[k] = v
            
            conn.use_tunnel = conn_object['tunnel']['enabled']
            conn.conn_string = conn_object['conn_string']
            conn.save()


        # cleanup existing references (if any)
        GroupConnection.objects.filter(connection=conn).delete()

        if conn_object['group']:
            conn_group = Group.objects.filter(id=conn_object['group'], user=request.user).first()
            if conn_group:
                gc = GroupConnection(connection=conn, group=conn_group)
                gc.save()

        tunnel_information = {
            'enabled': conn.use_tunnel,
            'server': conn.ssh_server,
            'port': conn.ssh_port,
            'user': conn.ssh_user,
            'password': decrypt(conn.ssh_password, key) if conn.ssh_password else '',
            'key': decrypt(conn.ssh_key, key) if conn.ssh_key else ''
        }
        # this is for sqlite3 db connection because it has no password
        password = decrypt(conn.password, key) if conn.password else ''
        database = OmniDatabase.Generic.InstantiateDatabase(
            conn.technology.name,
            conn.server,
            conn.port,
            conn.database,
            conn.username,
            password,
            conn.id,
            conn.alias,
            p_conn_string=conn.conn_string,
            p_parse_conn_string=True,
            connection_params=conn.connection_params
        )

        prompt_password = conn.password == ''

        session.AddDatabase(conn.id, conn.technology.name, database, prompt_password, tunnel_information, conn.alias,
                              conn_object['public'])

    except Exception as exc:
        response_data['data'] = str(exc)
        response_data['status'] = 'failed'
        return JsonResponse(response_data, status=400)

    request.session['pgmanage_session'] = session

    return JsonResponse(response_data)


@user_authenticated
@session_required
def delete_connection(request, session):
    response_data = {'data': '', 'status': 'success'}

    conn_id = request.data['id']

    try:
        conn = Connection.objects.get(id=conn_id)

        if conn.user.id != request.user.id:
            response_data['data'] = 'This connection does not belong to you.'
            response_data['status'] = 'failed'
            return JsonResponse(response_data, status=403)

        conn.delete()
        session.RemoveDatabase(conn_id)
    except Exception as exc:
        response_data['data'] = str(exc)
        response_data['status'] = 'failed'
        return JsonResponse(response_data, status=400)

    request.session['pgmanage_session'] = session

    return JsonResponse(response_data)


@user_authenticated
@session_required
def get_existing_tabs(request, session):

    existing_tabs = []
    for tab in Tab.objects.filter(user=request.user).order_by("connection"):
        if tab.connection.public or tab.connection.user.id == request.user.id:
            existing_tabs.append(
                {
                    "index": tab.connection.id,
                    "snippet": tab.snippet,
                    "title": tab.title,
                    "tab_db_id": tab.id,
                    "database_name": tab.database
                }
            )

    request.session["pgmanage_session"] = session

    return JsonResponse({"existing_tabs": existing_tabs})
