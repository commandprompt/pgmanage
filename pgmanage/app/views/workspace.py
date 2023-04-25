import json
import os
import random
import shutil
import string
import subprocess
import sys
from datetime import datetime

import sqlparse
from app.models.main import Group, GroupConnection, Shortcut, Tab, UserDetails
from app.utils.crypto import make_hash
from app.utils.key_manager import key_manager
from app.utils.master_password import (
    reset_master_pass,
    set_masterpass_check_text,
    validate_master_password,
)
from app.views.connections import session_required
from app.views.memory_objects import (
    clear_client_object,
    database_required,
    user_authenticated,
)
from django.contrib.auth import update_session_auth_hash
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist
from django.http import JsonResponse
from django.shortcuts import redirect, render

from pgmanage import settings


@login_required
def index(request):
    try:
        user_details = UserDetails.objects.get(user=request.user)
    except ObjectDoesNotExist:
        user_details = UserDetails(user=request.user)
        user_details.save()

    # Invalid session
    if not request.session.get("pgmanage_session"):
        return redirect(settings.LOGIN_REDIRECT_URL)

    session = request.session.get("pgmanage_session")
    if key_manager.get(request.user):
        session.RefreshDatabaseList()

    theme = "omnidb" if user_details.theme == "light" else "omnidb_dark"

    if user_details.binary_path:
        binary_path = user_details.binary_path
    else:
        binary_path = (
            os.path.dirname(shutil.which("psql")) if shutil.which("psql") else ""
        )

    context = {
        "session": None,
        "editor_theme": theme,
        "theme": user_details.theme,
        "font_size": user_details.font_size,
        "user_id": request.user.id,
        "user_key": request.session.session_key,
        "user_name": request.user.username,
        "super_user": request.user.is_superuser,
        "csv_encoding": user_details.csv_encoding,
        "csv_delimiter": user_details.csv_delimiter,
        "desktop_mode": settings.DESKTOP_MODE,
        "pgmanage_version": settings.PGMANAGE_VERSION,
        "pgmanage_short_version": settings.PGMANAGE_SHORT_VERSION,
        "menu_item": "workspace",
        "tab_token": "".join(
            random.choice(string.ascii_lowercase + string.digits) for i in range(20)
        ),
        "show_terminal_option": False,
        "url_folder": settings.PATH,
        "csrf_cookie_name": settings.CSRF_COOKIE_NAME,
        "master_key": "new"
        if not bool(user_details.masterpass_check)
        else bool(key_manager.get(request.user)),
        "binary_path": binary_path,
    }

    # wiping saved tabs databases list
    session.v_tabs_databases = dict([])
    request.session["pgmanage_session"] = session

    clear_client_object(p_client_id=request.session.session_key)

    return render(request, "app/workspace.html", context)


@user_authenticated
@session_required
def save_config_user(request):
    response_data = {"data": "", "status": "success"}

    session = request.session.get("pgmanage_session")

    request_data = json.loads(request.body) if request.body else {}

    font_size = request_data["font_size"]
    theme = request_data["theme"]
    password = request_data["password"]
    csv_encoding = request_data["csv_encoding"]
    csv_delimiter = request_data["csv_delimiter"]
    binary_path = request_data["binary_path"]

    session.v_theme_id = theme
    session.v_font_size = font_size
    session.v_csv_encoding = csv_encoding
    session.v_csv_delimiter = csv_delimiter

    user_details = UserDetails.objects.get(user=request.user)
    user_details.theme = theme
    user_details.font_size = font_size
    user_details.csv_encoding = csv_encoding
    user_details.csv_delimiter = csv_delimiter
    user_details.binary_path = binary_path
    user_details.save()

    request.session["pgmanage_session"] = session

    if password != "":
        user = User.objects.get(id=request.user.id)
        user.set_password(password)
        user.save()
        update_session_auth_hash(request, user)

    return JsonResponse(response_data)


@user_authenticated
@session_required
def shortcuts(request):
    response_data = {"data": "", "status": "success"}

    if request.method == "POST":
        request_data = json.loads(request.body) if request.body else {}
        shortcut_list = request_data.get("shortcuts")
        current_os = request_data.get("current_os")

        try:
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
            response_data["data"] = str(exc)
            response_data["status"] = "failed"
            return JsonResponse(response_data, status=400)

    if request.method == "GET":

        data = {}

        user_shortcuts = Shortcut.objects.filter(user=request.user)
        for shortcut in user_shortcuts:
            data[shortcut.code] = {
                "ctrl_pressed": shortcut.ctrl_pressed,
                "shift_pressed": shortcut.shift_pressed,
                "alt_pressed": shortcut.alt_pressed,
                "meta_pressed": shortcut.meta_pressed,
                "shortcut_key": shortcut.key,
                "os": shortcut.os,
                "shortcut_code": shortcut.code,
            }
        response_data["data"] = data

    return JsonResponse(response_data)


@user_authenticated
def get_database_list(request):

    response_data = {"v_data": "", "v_error": False, "v_error_id": -1}

    session = request.session.get("pgmanage_session")

    databases = []
    groups = []
    remote_terminals = []

    # Global group
    current_group_data = {
        "v_group_id": 0,
        "v_name": "All connections",
        "conn_list": [],
    }
    groups.append(current_group_data)

    for group in Group.objects.filter(user=request.user):
        current_group_data = {
            "v_group_id": group.id,
            "v_name": group.name,
            "conn_list": [],
        }
        for group_conn in GroupConnection.objects.filter(group=group):
            current_group_data["conn_list"].append(group_conn.connection.id)

        groups.append(current_group_data)

    # Connection list
    for key, database_object in session.v_databases.items():
        if (
            database_object["tunnel"]["enabled"]
            or database_object["technology"] == "terminal"
        ):
            alias = ""
            if database_object["alias"] != "":
                alias = database_object["alias"]
            details = (
                database_object["tunnel"]["user"]
                + "@"
                + database_object["tunnel"]["server"]
                + ":"
                + database_object["tunnel"]["port"]
            )
            terminal_object = {
                "v_conn_id": key,
                "v_alias": alias,
                "v_details": details,
                "v_public": database_object["public"],
            }
            remote_terminals.append(terminal_object)

        if database_object["database"] is not None:
            database = database_object["database"]

            if database.v_alias == "":
                alias = ""
            else:
                alias = f"({database.v_alias}) "
            if not database_object["tunnel"]["enabled"]:
                details = database.PrintDatabaseDetails()
            else:
                details = (
                    database.PrintDatabaseDetails()
                    + " <b>("
                    + database_object["tunnel"]["server"]
                    + ":"
                    + database_object["tunnel"]["port"]
                    + ")</b>"
                )

            database_data = {
                "v_db_type": database.v_db_type,
                "v_alias": database.v_alias,
                "v_conn_id": database.v_conn_id,
                "v_console_help": database.v_console_help,
                "v_database": database.v_active_service,
                "v_conn_string": database.v_conn_string,
                "v_details1": database.PrintDatabaseInfo(),
                "v_details2": details,
                "v_public": database_object["public"],
            }

            databases.append(database_data)

    # retrieving saved tabs
    existing_tabs = []
    for tab in Tab.objects.filter(user=request.user).order_by("connection"):
        if tab.connection.public or tab.connection.user.id == request.user.id:
            existing_tabs.append(
                {
                    "index": tab.connection.id,
                    "snippet": tab.snippet,
                    "title": tab.title,
                    "tab_db_id": tab.id,
                }
            )

    request.session["pgmanage_session"] = session

    response_data["v_data"] = {
        "v_select_html": None,
        "v_select_group_html": None,
        "v_connections": databases,
        "v_groups": groups,
        "v_remote_terminals": remote_terminals,
        "v_id": session.v_database_index,
        "v_existing_tabs": existing_tabs,
    }

    return JsonResponse(response_data)


@user_authenticated
def change_active_database(request):

    response_data = {"v_data": {}, "v_error": False, "v_error_id": -1}

    # Invalid session
    if not request.session.get("pgmanage_session"):
        response_data["v_error"] = True
        response_data["v_error_id"] = 1
        return JsonResponse(response_data)

    session = request.session.get("pgmanage_session")

    json_object = json.loads(request.POST.get("data", None))
    tab_id = json_object["p_tab_id"]
    new_database = json_object["p_database"]

    session.v_tabs_databases[tab_id] = new_database

    request.session["pgmanage_session"] = session

    return JsonResponse(response_data)


@user_authenticated
def renew_password(request):
    response_data = {"v_data": "", "v_error": False, "v_error_id": -1}

    # Invalid session
    if not request.session.get("pgmanage_session"):
        response_data["v_error"] = True
        response_data["v_error_id"] = 1
        return JsonResponse(response_data)

    session = request.session.get("pgmanage_session")

    json_object = json.loads(request.POST.get("data", None))
    database_index = json_object["p_database_index"]
    password = json_object["p_password"]

    database_object = session.v_databases[database_index]
    database_object["database"].v_connection.v_password = password

    test = database_object["database"].TestConnection()

    if test == "Connection successful.":
        database_object["prompt_timeout"] = datetime.now()
    else:
        response_data["v_error"] = True
        response_data["v_data"] = test

    request.session["pgmanage_session"] = session

    return JsonResponse(response_data)


@user_authenticated
@database_required(p_check_timeout=True, p_open_connection=True)
def draw_graph(request, v_database):

    response_data = {"v_data": "", "v_error": False, "v_error_id": -1}

    json_object = json.loads(request.POST.get("data", None))
    complete = json_object["p_complete"]
    schema = json_object["p_schema"]

    nodes = []
    edges = []

    try:
        tables = v_database.QueryTables(False, schema)
        for table in tables.Rows:
            node_data = {
                "id": table["table_name"],
                "label": table["table_name"],
                "group": 1,
            }

            if complete:
                node_data["label"] += "\n"
                columns = v_database.QueryTablesFields(
                    table["table_name"], False, schema
                )
                for column in columns.Rows:
                    node_data["label"] += (
                        column["column_name"] + " : " + column["data_type"] + "\n"
                    )

            nodes.append(node_data)

        data = v_database.QueryTablesForeignKeys(None, False, schema)

        curr_constraint = ""
        curr_from = ""
        curr_to = ""
        curr_to_schema = ""

        for fk in data.Rows:
            if curr_constraint != "" and curr_constraint != fk["constraint_name"]:
                edge = {
                    "from": curr_from,
                    "to": curr_to,
                    "label": "",
                    "arrows": "to",
                }

                # FK referencing other schema, create a new node if it isn't in v_nodes list.
                if v_database.v_schema != curr_to_schema:
                    found = False
                    for k in range(len(nodes) - 1, 0):
                        if nodes[k]["label"] == curr_to:
                            found = True
                            break

                    if not found:
                        node = {"id": curr_to, "label": curr_to, "group": 0}
                        nodes.append(node)

                edges.append(edge)
                curr_constraint = ""

            curr_from = fk["table_name"]
            curr_to = fk["r_table_name"]
            curr_constraint = fk["constraint_name"]
            curr_to_schema = fk["r_table_schema"]

        if curr_constraint != "":
            edge = {"from": curr_from, "to": curr_to, "label": "", "arrows": "to"}

            edges.append(edge)

            # FK referencing other schema, create a new node if it isn't in v_nodes list.
            if v_database.v_schema != curr_to_schema:
                found = False

                for k in range(len(nodes) - 1, 0):
                    if nodes[k]["label"] == curr_to:
                        found = True
                        break

                if not found:
                    node = {"id": curr_to, "label": curr_to, "group": 0}

                    nodes.append(node)

        response_data["v_data"] = {"v_nodes": nodes, "v_edges": edges}

    except Exception as exc:
        response_data["v_data"] = {"password_timeout": True, "message": str(exc)}
        response_data["v_error"] = True
        return JsonResponse(response_data)

    return JsonResponse(response_data)


@user_authenticated
@database_required(p_check_timeout=True, p_open_connection=True)
def start_edit_data(request, v_database):

    response_data = {
        "v_data": {"v_pk": [], "v_cols": [], "v_ini_orderby": ""},
        "v_error": False,
        "v_error_id": -1,
    }

    json_object = json.loads(request.POST.get("data", None))
    table = json_object["p_table"]

    if v_database.v_has_schema:
        schema = json_object["p_schema"]

    try:
        if v_database.v_has_schema:
            pk = v_database.QueryTablesPrimaryKeys(table, False, schema)
            columns = v_database.QueryTablesFields(table, False, schema)
        else:
            pk = v_database.QueryTablesPrimaryKeys(table)
            columns = v_database.QueryTablesFields(table)

        pk_cols = None
        if pk is not None and len(pk.Rows) > 0:
            if v_database.v_has_schema:
                pk_cols = v_database.QueryTablesPrimaryKeysColumns(
                    pk.Rows[0]["constraint_name"], table, False, schema
                )
            else:
                pk_cols = v_database.QueryTablesPrimaryKeysColumns(table)

            response_data["v_data"]["v_ini_orderby"] = "ORDER BY "
            first = True
            for pk_col in pk_cols.Rows:
                if not first:
                    response_data["v_data"]["v_ini_orderby"] = (
                        response_data["v_data"]["v_ini_orderby"] + ", "
                    )
                first = False
                response_data["v_data"]["v_ini_orderby"] = (
                    response_data["v_data"]["v_ini_orderby"]
                    + "t."
                    + pk_col["column_name"]
                )
        index = 0
        for column in columns.Rows:
            col = {
                "v_type": column["data_type"],
                "v_column": column["column_name"],
                "v_is_pk": False,
            }
            # Finding corresponding PK column
            if pk_cols is not None:
                for pk_col in pk_cols.Rows:
                    if pk_col["column_name"].lower() == column["column_name"].lower():
                        col["v_is_pk"] = True
                        pk_info = {
                            "v_column": pk_col["column_name"],
                            "v_index": index,
                            "v_type": column["data_type"],
                        }
                        response_data["v_data"]["v_pk"].append(pk_info)
                        break
            response_data["v_data"]["v_cols"].append(col)
            index += 1

    except Exception as exc:
        response_data["v_data"] = {"password_timeout": True, "message": str(exc)}
        response_data["v_error"] = True

    return JsonResponse(response_data)


@user_authenticated
@database_required(p_check_timeout=True, p_open_connection=True)
def get_completions_table(request, v_database):

    response_data = {"v_data": "", "v_error": False, "v_error_id": -1}

    json_object = json.loads(request.POST.get("data", None))
    table = json_object["p_table"]
    schema = json_object["p_schema"]

    if v_database.v_has_schema:
        table_name = schema + "." + table
    else:
        table_name = table

    fields_list = []

    try:
        data = v_database.v_connection.GetFields(
            "select x.* from " + table_name + " x where 1 = 0"
        )
    except Exception as exc:
        response_data["v_data"] = {"password_timeout": True, "message": str(exc)}
        response_data["v_error"] = True
        return JsonResponse(response_data)

    score = 100

    score -= 100

    for field in data:
        fields_list.append(
            {
                "value": "t." + field.v_truename,
                "score": score,
                "meta": field.v_dbtype,
            }
        )
        v_score -= 100

    response_data["v_data"] = fields_list

    return JsonResponse(response_data)


@user_authenticated
def indent_sql(request):

    response_data = {"v_data": "", "v_error": False, "v_error_id": -1}

    # Invalid session
    if not request.session.get("pgmanage_session"):
        response_data["v_error"] = True
        response_data["v_error_id"] = 1
        return JsonResponse(response_data)

    json_object = json.loads(request.POST.get("data", None))
    sql = json_object["p_sql"]

    response_data["v_data"] = sqlparse.format(sql, reindent=True)

    return JsonResponse(response_data)


@user_authenticated
@database_required(p_check_timeout=True, p_open_connection=True)
def refresh_monitoring(request, v_database):
    response_data = {"v_data": "", "v_error": False, "v_error_id": -1}

    json_object = json.loads(request.POST.get("data", None))
    sql = json_object["p_query"]

    try:
        data = v_database.Query(sql, True, True)
        response_data["v_data"] = {
            "v_col_names": data.Columns,
            "v_data": data.Rows,
            "v_query_info": f"Number of records: {len(data.Rows)}",
        }
    except Exception as exc:
        response_data["v_data"] = {"password_timeout": True, "message": str(exc)}
        response_data["v_error"] = True

    return JsonResponse(response_data)


def get_alias(p_sql, p_pos, p_val):
    try:
        s = sqlparse.parse(p_sql)
        alias = p_val[:-1]
        for stmt in s:
            for item in stmt.tokens:
                if item.ttype is None:
                    try:
                        cur_alias = item.get_alias()
                        if cur_alias is None:
                            if item.value == alias:
                                return item.value
                        elif cur_alias == alias:
                            # check if there is punctuation
                            if str(item.tokens[1].ttype) != "Token.Punctuation":
                                return item.get_real_name()
                            return item.tokens[0].value + "." + item.tokens[2].value
                    except Exception:
                        pass

    except Exception:
        return
    return


@user_authenticated
@database_required(p_check_timeout=True, p_open_connection=True)
def get_autocomplete_results(request, v_database):

    response_data = {"v_data": "", "v_error": False, "v_error_id": -1}

    json_object = json.loads(request.POST.get("data", None))
    sql = json_object["p_sql"]
    value = json_object["p_value"]
    pos = json_object["p_pos"]
    num_dots = value.count(".")

    result = []
    max_result_word = ""
    max_complement_word = ""

    alias = None
    if value != "" and value[-1] == ".":
        alias = get_alias(sql, pos, value)
        if alias:
            try:
                data = v_database.v_connection.GetFields(
                    "select x.* from " + alias + " x where 1 = 0"
                )
                current_group = {"type": "column", "elements": []}
                max_result_length = 0
                max_complement_length = 0
                for v_type in data:
                    curr_result_length = len(value + v_type.v_truename)
                    curr_complement_length = len(v_type.v_dbtype)
                    curr_result_word = value + v_type.v_truename
                    curr_complement_word = v_type.v_dbtype

                    if curr_result_length > max_result_length:
                        max_result_length = curr_result_length
                        max_result_word = curr_result_word
                    if curr_complement_length > max_complement_length:
                        max_complement_length = curr_complement_length
                        max_complement_word = curr_complement_word

                    current_group["elements"].append(
                        {
                            "value": value + v_type.v_truename,
                            "select_value": value + v_type.v_truename,
                            "complement": v_type.v_dbtype,
                        }
                    )
                if len(current_group["elements"]) > 0:
                    result.append(current_group)
            except Exception:
                pass

    if not alias:
        filter_part = f"where search.result like '{value}%' "
        query_columns = "type,sequence,result,select_value,complement"
        if num_dots > 0:
            filter_part = f"where search.result_complete like '{value}%' and search.num_dots <= {num_dots}"
            query_columns = "type,sequence,result_complete as result,select_value,complement_complete as complement"
        elif value == "":
            filter_part = "where search.num_dots = 0 "

        try:
            max_result_length = 0
            max_complement_length = 0

            search = v_database.GetAutocompleteValues(query_columns, filter_part)

            if search is not None:
                current_group = {"type": "", "elements": []}
                if len(search.Rows) > 0:
                    current_group["type"] = search.Rows[0]["type"]
                for search_row in search.Rows:

                    if current_group["type"] != search_row["type"]:
                        result.append(current_group)
                        current_group = {"type": search_row["type"], "elements": []}

                    curr_result_length = len(search_row["result"])
                    curr_complement_length = len(search_row["complement"])
                    curr_result_word = search_row["result"]
                    curr_complement_word = search_row["complement"]
                    current_group["elements"].append(
                        {
                            "value": search_row["result"],
                            "select_value": search_row["select_value"],
                            "complement": search_row["complement"],
                        }
                    )

                    if curr_result_length > max_result_length:
                        max_result_length = curr_result_length
                        max_result_word = curr_result_word
                    if curr_complement_length > max_complement_length:
                        max_complement_length = curr_complement_length
                        max_complement_word = curr_complement_word

                if len(current_group["elements"]) > 0:
                    result.append(current_group)

        except Exception as exc:
            response_data["v_data"] = {"password_timeout": True, "message": str(exc)}
            response_data["v_error"] = True
            return JsonResponse(response_data)

    response_data["v_data"] = {
        "data": result,
        "max_result_word": max_result_word,
        "max_complement_word": max_complement_word,
    }

    return JsonResponse(response_data)


@user_authenticated
def master_password(request):
    """
    Set the master password and store in the memory
    This password will be used to encrypt/decrypt saved server passwords
    """

    response_data = {"v_data": "", "v_error": False, "v_error_id": -1}

    session = request.session.get("pgmanage_session")

    # Invalid session
    if not session:
        response_data["v_error"] = True
        response_data["v_error_id"] = 1
        return JsonResponse(response_data)

    json_object = json.loads(request.POST.get("data", None))
    master_pass = json_object["master_password"]

    master_pass_hash = make_hash(master_pass, request.user)
    user_details = UserDetails.objects.get(user=request.user)

    # if master pass is set previously
    if user_details.masterpass_check and not validate_master_password(
        user_details, master_pass_hash
    ):
        response_data["v_error"] = True
        response_data["v_data"] = "Master password is not correct."
        return JsonResponse(response_data)

    if json_object != "" and json_object.get("master_password", "") != "":

        # store the master pass in the memory
        key_manager.set(request.user, master_pass_hash)

        # set the encrypted sample text with the new master pass
        set_masterpass_check_text(user_details, master_pass_hash)

    elif json_object.get("master_password", "") == "":
        response_data["v_error"] = True
        response_data["v_data"] = "Master password cannot be empty."
        return JsonResponse(response_data)

    # refreshing database session list with provided master password
    session.RefreshDatabaseList()

    # saving new pgmanage_session
    request.session["pgmanage_session"] = session

    return JsonResponse(response_data)


@user_authenticated
def reset_master_password(request):
    """
    Removes the master password and remove all saved passwords
    This password will be used to encrypt/decrypt saved server passwords
    """

    response_data = {"v_data": "", "v_error": False, "v_error_id": -1}

    user_details = UserDetails.objects.get(user=request.user)

    reset_master_pass(user_details)

    return JsonResponse(response_data)


@user_authenticated
def validate_binary_path(request):
    data = json.loads(request.body) if request.body else {}

    binary_path = data.get("binary_path")

    result = {}

    env = os.environ.copy()

    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        env.pop("LD_LIBRARY_PATH", None)

    for utility in ["pg_dump", "pg_dumpall", "pg_restore", "psql"]:
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
