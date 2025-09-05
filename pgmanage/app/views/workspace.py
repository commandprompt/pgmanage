import json
import os
import subprocess
import sys
from datetime import datetime, timezone

from app.client_manager import client_manager
from app.include.Session import Session
from app.models.main import Connection, ERDLayout, Shortcut, Tab, UserDetails
from app.utils.crypto import make_hash
from app.utils.decorators import database_required, user_authenticated
from app.utils.key_manager import key_manager
from app.utils.master_password import (reset_master_pass,
                                       set_masterpass_check_text,
                                       validate_master_password)
from app.views.connections import session_required
from django.contrib.auth import update_session_auth_hash
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.db import DatabaseError
from django.forms import model_to_dict
from django.http import HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.utils.decorators import method_decorator
from django.views import View

from pgmanage import settings


@login_required
def index(request):
    user_details, _ = UserDetails.objects.get_or_create(user=request.user)

    if not settings.MASTER_PASSWORD_REQUIRED and not key_manager.get(request.user):
        return redirect(settings.LOGIN_URL)

    # Invalid session
    if not request.session.get("pgmanage_session"):
        return redirect(settings.LOGIN_REDIRECT_URL)

    session: Session = request.session.get("pgmanage_session")

    if not settings.MASTER_PASSWORD_REQUIRED and user_details.masterpass_check == '':

        key = key_manager.get(request.user)

        set_masterpass_check_text(user_details, key)
    if key_manager.get(request.user):
        session.RefreshDatabaseList()

    context = {
        "super_user": request.user.is_superuser,
        "desktop_mode": settings.DESKTOP_MODE,
        "pgmanage_version": settings.PGMANAGE_VERSION,
        "pgmanage_short_version": settings.PGMANAGE_SHORT_VERSION,
        "base_path": settings.PATH,
        "csrf_cookie_name": settings.CSRF_COOKIE_NAME,
        "master_key": "new"
        if not bool(user_details.masterpass_check)
        else bool(key_manager.get(request.user)),
        'user_name': request.user.username,
    }

    # wiping saved tabs databases list
    session.tabs_databases = {}
    request.session["pgmanage_session"] = session

    client_manager.clear_client(client_id=request.session.session_key)

    return render(request, "app/workspace.html", context)




@method_decorator([user_authenticated, session_required], name="dispatch")
class SettingsView(View):
    def get(self, request, *args, **kwargs):
        user_details, _ = UserDetails.objects.get_or_create(user=request.user)

        user_settings = {
            **model_to_dict(
                user_details,
                exclude=[
                    "id",
                    "user",
                    "binary_path",
                    "pigz_path",
                    "masterpass_check",
                ],
            ),
            "binary_path": user_details.get_binary_path(),
            "pigz_path": user_details.get_pigz_path(),
            "editor_theme": user_details.get_editor_theme(),
            "max_upload_size": settings.MAX_UPLOAD_SIZE,
        }

        user_shortcuts = {}

        shortcuts_db = Shortcut.objects.filter(user=request.user)

        for shortcut in shortcuts_db:
            user_shortcuts[shortcut.code] = {
                **model_to_dict(shortcut, exclude=["id", "user", "code", "key"]),
                "shortcut_key": shortcut.key,
                "shortcut_code": shortcut.code,
            }

        return JsonResponse(
            data={"settings": user_settings, "shortcuts": user_shortcuts}
        )

    def post(self, request, *args, **kwargs):
        session: Session = kwargs.get("session")
        settings_data = request.data.get("settings")
        shortcut_list = request.data.get("shortcuts")
        current_os = request.data.get("current_os")

        session.theme = settings_data.get("theme")
        session.font_size = settings_data.get("font_size")
        session.csv_encoding = settings_data.get("csv_encoding")
        session.csv_delimiter = settings_data.get("csv_delimiter")
        try:
            user_details = UserDetails.objects.get(user=request.user)
            restore_tabs = settings_data.get('restore_tabs', None)
            erase_tabs = user_details.restore_tabs != restore_tabs

            for attr, value in settings_data.items():
                setattr(user_details, attr, value)
            user_details.save()

            if erase_tabs:
                Tab.objects.filter(user=request.user).delete()

            request.session["pgmanage_session"] = session

            # Delete existing user shortcuts
            Shortcut.objects.filter(user=request.user).delete()

            # Adding new user shortcuts
            for shortcut in shortcut_list:
                shortcut_object = Shortcut(
                    user=request.user,
                    code=shortcut["shortcut_code"],
                    os=current_os,
                    ctrl_pressed=shortcut["ctrl_pressed"],
                    shift_pressed=shortcut["shift_pressed"],
                    alt_pressed=shortcut["alt_pressed"],
                    meta_pressed=shortcut["meta_pressed"],
                    key=shortcut["shortcut_key"],
                )
                shortcut_object.save()
        except Exception as exc:
            return JsonResponse(data={"data": str(exc)}, status=400)
        return HttpResponse(status=200)


@user_authenticated
def save_user_password(request):
    password = request.data.get("password")

    if not password:
        return JsonResponse(data={"data": "Password can not be empty."}, status=400)

    try:
        user = User.objects.get(id=request.user.id)
        user.set_password(password)
        user.save()
        update_session_auth_hash(request, user)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=500)
    
    old_key = key_manager.get(request.user)
    key_manager.set(request.user, password)
    try:
        Connection.reencrypt_credentials(request.user.id, old_key, password)
    except DatabaseError as exc:
        return JsonResponse(data={"data": str(exc)}, status=500)

    return HttpResponse(status=200)


@user_authenticated
@session_required
def change_active_database(request, session: Session):
    data = request.data
    workspace_id = data["workspace_id"]
    new_database = data["database"]
    conn_id = data["database_index"]

    session.tabs_databases[workspace_id] = new_database

    conn = Connection.objects.get(id=conn_id)
    conn.last_used_database = new_database
    conn.last_access_date = datetime.now(tz=timezone.utc)
    conn.save()

    request.session["pgmanage_session"] = session

    return JsonResponse(data={"data": "database changed"})


@user_authenticated
@session_required
def renew_password(request, session: Session):
    data = request.data
    database_index = data.get("database_index")
    password = data.get("password")
    password_kind = data.get("password_kind", "database")

    database_object = session.databases[database_index]
    if password_kind == "database":
        database_object["database"].connection.password = password
    else:
        database_object["tunnel"]["password"] = password

    test = database_object["database"].TestConnection()

    if test != "Connection successful.":
        return JsonResponse({"data": test}, status=400)

    database_object["prompt_timeout"] = datetime.now()
    request.session["pgmanage_session"] = session

    return HttpResponse(status=200)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def draw_graph(request, database):
    data = request.data
    schema = data.get("schema", '')
    edge_dict = {}
    node_dict = {}

    try:
        tables = database.QueryTables(False, schema)

        for table in tables.Rows:
            node_data = {
                "id": table["table_name"],
                "label": table["table_name"],
                "group": 1,
                "columns": []
            }
            table_name = table.get('name_raw') or table["table_name"]
            table_columns = database.QueryTablesFields(
                table_name, False, schema
            ).Rows

            node_data['columns'] = list(({
                'name': c['column_name'],
                'type': c['data_type'],
                'cgid': None,
                'is_pk': False,
                'is_fk': False,
                } for c in table_columns))

            node_dict[table["table_name"]] = node_data

        q_fks = database.QueryTablesForeignKeys(None, False, schema)

        for fk in q_fks.Rows:
            # ensure that the new edge stays within the same schema and points to an existing table
            # table partitions are *not* included in the node list
            # FIXME: resolve FKs of partitioned table from its partitions
            if fk["r_table_schema"] == schema and fk["table_name"] in node_dict.keys():
                edge_dict[fk["constraint_name"]] = {
                    "from": fk["table_name"],
                    "to": fk["r_table_name"],
                    "from_col": None,
                    "to_col": None,
                    "label": "",
                    "arrows": "to",
                    "cgid": None
                }

        q_fkcols = database.QueryTablesForeignKeysColumns(list(edge_dict.keys()), None, False, schema)
        for fkcol in q_fkcols.Rows:
            cgid = fkcol['constraint_name']
            edge = edge_dict[fkcol['constraint_name']]
            edge['from_col'] = fkcol['column_name']
            edge['to_col'] = fkcol['r_column_name']
            edge['cgid'] = cgid
            table = node_dict.get(fkcol['table_name'])
            r_table = node_dict.get(fkcol['r_table_name'])
            if table and r_table:
                for col in table['columns']:
                    if col['name'] == fkcol['column_name']:
                        col['is_fk'] = True
                        col['cgid'] = cgid

                for col in r_table['columns']:
                    if col['name'] == fkcol['r_column_name']:
                        # FIXME: this is incomplete, seting PK based on FK constraints is not enough
                        # there may be unreferenced PKs which will be missed
                        col['is_pk'] = True
                        col['cgid'] = f"{fkcol['r_table_name']}-{fkcol['r_column_name']}"


        database_name = (
            "sqlite3" if database.db_type == "sqlite" else database.service
        )
        layout_name = f"{database_name}@{data.get('schema')}"

        layout_obj = ERDLayout.objects.filter(name=layout_name, connection=Connection.objects.get(id=data.get("database_index"))).first()

        if layout_obj:
            layout_data = layout_obj.layout

            layout_nodes = {node["data"]["id"]: node for node in layout_data.get('elements', {}).get('nodes', [])}
            layout_edges = {edge["data"]["cgid"]: edge for edge in layout_data.get("elements", {}).get("edges", []) if edge["data"]["cgid"] is not None}


            current_node_ids = set(node_dict.keys())
            current_edge_ids = set(edge_dict.keys())

            filtered_nodes = [node for id_, node in layout_nodes.items() if id_ in current_node_ids]


            new_nodes = [v for k, v in node_dict.items() if k not in layout_nodes.keys()]

            filtered_edges = [edge for id_, edge in layout_edges.items() if id_ in current_edge_ids]

            new_edges = [
                edge for edge in edge_dict.values()
                if  edge['cgid'] not in layout_edges.keys()
            ]

            layout_data["elements"] = {
                "nodes": filtered_nodes,
                "edges": filtered_edges
            }

            return JsonResponse(data={
                "layout": layout_data,
                "new_nodes": new_nodes,
                "new_edges": new_edges,
            })

        response_data = {
            "nodes": list(node_dict.values()),
            "edges": list(edge_dict.values()),
        }

    except Exception as exc:
        return JsonResponse(data={'data': str(exc)}, status=400)
    return JsonResponse(response_data)


@user_authenticated
def save_graph_state(request):
    try:
        data = request.data
        database_index = data.get("database_index")
        layout = data.get("layout")
        database_name = (
            "sqlite3"
            if os.path.isfile(data.get("database_name"))
            else data.get("database_name")
        )
        layout_name = f"{database_name}@{data.get('schema')}"
        conn = Connection.objects.get(id=database_index)

        layout_obj, _ = ERDLayout.objects.update_or_create(
                connection=conn,
                name=layout_name,
                defaults={"layout": layout},
            )
    except Exception as exc:
        return JsonResponse(data={'data': str(exc)}, status=400)

    return JsonResponse({"status": "saved"})


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_table_columns(request, database):
    data = request.data
    table = data["table"]

    if database.has_schema:
        schema = data["schema"]

    try:
        if database.has_schema:
            pk = database.QueryTablesPrimaryKeys(table, False, schema)
            columns = database.QueryTablesFields(table, False, schema)
        else:
            pk = database.QueryTablesPrimaryKeys(table)
            columns = database.QueryTablesFields(table)

        # generate ORDER BY from table PKs
        order_by = ''
        pk_column_names = []
        if pk is not None and len(pk.Rows) > 0:
            if database.has_schema:
                pk_cols = database.QueryTablesPrimaryKeysColumns(
                    pk.Rows[0]["constraint_name"], table, False, schema
                )
            else:
                pk_cols = database.QueryTablesPrimaryKeysColumns(table)

            cols = ', '.join(['t.'+x['column_name'] for x in pk_cols.Rows])
            order_by = f"ORDER BY {cols}" if cols else ""

            pk_column_names = [x['column_name'] for x in pk_cols.Rows]

        table_columns = []
        for column in columns.Rows:
            table_columns.append({
                "data_type": column['data_type'],
                "name": column['column_name'],
                "is_primary": column['column_name'] in pk_column_names,
            })

    except Exception as exc:
        return JsonResponse(data={'data': str(exc)}, status=400)

    return JsonResponse(data={'columns': table_columns, 'initial_orderby': order_by})


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_database_meta(request, database):
    response_data = {
        'schemas': None,
        'databases': []
    }

    schema_list = []

    try:
        if hasattr(database, "QueryDatabases"):
            databases = database.QueryDatabases()
            for database_object in databases.Rows:
                response_data["databases"].append(database_object[0])

        if database.db_type in ["mysql", "mariadb"]:
            schemas = [{"schema_name": database.service}]
        elif database.has_schema:
            schemas = database.QuerySchemas().Rows if hasattr(database, 'QuerySchemas') else [{"schema_name": database.schema}]
        else:
            schemas = [{'schema_name': '-noschema-'}]

        filtered_schemas = [
            schema for schema in schemas if schema.get("schema_name") not in {"information_schema", "pg_catalog"}
        ]
        for schema in filtered_schemas:
            schema_data = {
                "name": schema["schema_name"],
                "tables": [],
                "views": [],
            }

            tables = database.QueryTables(False, schema["schema_name"])
            for table in tables.Rows:
                table_data = {
                    "name": table["table_name"],
                    "columns": []
                }
                table_name = table.get('name_raw') or table["table_name"]
                table_columns = database.QueryTablesFields(
                    table_name, False, schema["schema_name"]
                ).Rows

                table_data['columns'] = list((c['column_name'] for c in table_columns))
                schema_data['tables'].append(table_data)
            
            if database.has_schema:
                views = database.QueryViews(all_schemas=False, schema=schema["schema_name"])
            else:
                views = database.QueryViews()

            for view in views.Rows:
                view_data = {
                    "name": view["table_name"],
                    "columns": []
                }
                view_name = view.get('name_raw') or view["table_name"]

                if database.has_schema:
                    view_columns = database.QueryViewFields(table=view_name, all_schemas=False, schema=schema["schema_name"])
                else:
                    view_columns = database.QueryViewFields(table_name=view_name)

                view_data['columns'] = list((c['column_name'] for c in view_columns.Rows))
                schema_data['views'].append(view_data)

            schema_list.append(schema_data)

        response_data["schemas"] = schema_list

    except Exception as exc:
        return JsonResponse(data={'data': str(exc)}, status=400)

    return JsonResponse(response_data)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def refresh_monitoring(request, database):
    sql = request.data.get("query")

    try:
        data = database.Query(sql, True, True)

        response_data = {
            "col_names": data.Columns,
            "data": json.loads(data.Jsonify()),
        }
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(response_data)


@user_authenticated
@session_required
def master_password(request, session: Session):
    """
    Set the master password and store in the memory
    This password will be used to encrypt/decrypt saved server passwords
    """

    data = request.data
    master_pass = data["master_password"]

    master_pass_hash = make_hash(master_pass, request.user)
    user_details = UserDetails.objects.get(user=request.user)

    # if master pass is set previously
    if user_details.masterpass_check and not validate_master_password(
        user_details, master_pass_hash
    ):
        return JsonResponse(data={"data": "Master password is not correct."}, status=400)

    if data != "" and data.get("master_password", "") != "":
        # store the master pass in the memory
        key_manager.set(request.user, master_pass_hash)

        # set the encrypted sample text with the new master pass
        set_masterpass_check_text(user_details, master_pass_hash)

    elif data.get("master_password", "") == "":
        return JsonResponse(data={"data": "Master password cannot be empty."}, status=400)

    # refreshing database session list with provided master password
    session.RefreshDatabaseList()

    # saving new pgmanage_session
    request.session["pgmanage_session"] = session

    return HttpResponse(status=200)

@user_authenticated
def toggle_pin_database(request):
    data = request.data

    database_index: int = data.get("database_index")
    database_name: str = data.get("database_name")
    pinned: bool = data.get("pinned")

    if not database_index or not database_name:
        return JsonResponse(data={"data": "database_index and database_name cannot be empty."}, status=400)

    connection = Connection.objects.filter(id=database_index).first()

    if pinned:
        if database_name not in connection.pinned_databases:
            connection.pinned_databases.append(database_name)
    else:
        connection.pinned_databases = [
            db_name
            for db_name in connection.pinned_databases
            if database_name != db_name
        ]

    connection.save()
    return HttpResponse(status=200)


@user_authenticated
def reset_master_password(request):
    """
    Removes the master password and remove all saved passwords
    This password will be used to encrypt/decrypt saved server passwords
    """

    user_details = UserDetails.objects.get(user=request.user)

    reset_master_pass(user_details)

    return HttpResponse(status=200)


@user_authenticated
def validate_binary_path(request):
    data = request.data

    binary_path = data.get("binary_path")

    utilities = data.get("utilities")

    result = {}

    env = os.environ.copy()

    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        env.pop("LD_LIBRARY_PATH", None)

    for utility in utilities:
        full_path = os.path.join(
            binary_path, utility if os.name != "nt" else (utility + ".exe")
        )

        if not os.path.exists(full_path):
            result[utility] = "not found on the specifed binary path."
            continue

        shell_result = subprocess.run(
            f'"{full_path}" --version',
            shell=True,
            env=env,
            capture_output=True,
            text=True,
        )

        utility_version = shell_result.stdout

        result_utility_version = utility_version.replace(utility, "").strip()

        result[utility] = result_utility_version

    return JsonResponse(data={"data": result})
