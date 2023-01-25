import ast
import json
import re

from datetime import datetime
from django.core.exceptions import ValidationError
from django.utils.timezone import make_aware
from django.db import DatabaseError
from django.db.models import Q

from app.models.main import ConfigHistory, Connection


def human_to_number(h_value, h_unit=None, h_type=int):
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "YB", "ZB"]
    re_unit = re.compile(r"([0-9.]+)\s*([KMGBTPEYZ]?B)$", re.IGNORECASE)
    m_value = re_unit.match(str(h_value))
    factor = 1
    if h_unit:
        m_unit = re_unit.match(str(h_unit))
        if m_unit:
            factor = int(m_unit.group(1))
            h_unit = str(m_unit.group(2))

    if m_value:
        p_num = m_value.group(1)
        p_unit = m_value.group(2)
        m = 0
        for u in units:
            if h_unit and h_unit.lower() == u.lower():
                m = 0
            if u.lower() == p_unit.lower():
                return (int(p_num) * (1024**m)) / factor
            else:
                m += 1

    # Valid time units are ms (milliseconds), s (seconds), min (minutes),
    # h (hours), and d (days
    re_unit = re.compile(r"([0-9.]+)\s*(us|ms|s|min|h|d)$")
    m_unit = re_unit.match(str(h_value))
    if h_unit == "ms":
        mult = {
            "us": 0.001,
            "ms": 1,
            "s": 1000,
            "min": 60000,
            "h": 3600000,
            "d": 86400000,
        }
    elif h_unit == "s":
        mult = {"ms": -1000, "s": 1, "min": 60, "h": 3600, "d": 86400}
    elif h_unit == "min":
        mult = {"ms": -60000, "s": -60, "min": 1, "h": 60, "d": 1440}
    elif h_unit == "h":
        mult = {"ms": -3600000, "s": -3600, "min": -60, "h": 1, "d": 24}
    elif h_unit == "d":
        mult = {"ms": -86400000, "s": -86400, "min": -1440, "h": -24, "d": 1}
    else:
        mult = {"ms": 1, "s": 1, "min": 1, "h": 1, "d": 1}

    if m_unit:
        p_num = m_unit.group(1)
        p_unit = m_unit.group(2)
        if mult[p_unit] > 0:
            return h_type(p_num) * mult[p_unit]
        else:
            return h_type(p_num) / abs(mult[p_unit])

    return h_value


def get_settings(conn, search=None, grouped=True):
    try:
        tables_json = conn.QueryConfiguration(search).Jsonify()
    except Exception as exc:
        raise DatabaseError(exc)
    tables = json.loads(tables_json)
    ret = {}
    if grouped:
        for row in tables:
            rows = ret.setdefault(row["category"], [])
            enumvals = row["enumvals"]
            if enumvals != "":

                enumvals = list(ast.literal_eval(enumvals))

            rows.append(
                {
                    "name": row["name"],
                    "setting": row["setting"],
                    "setting_raw": row["current_setting"],
                    "category": row["category"],
                    "unit": row["unit"],
                    "vartype": row["vartype"],
                    "min_val": row["min_val"],
                    "max_val": row["max_val"],
                    "boot_val": row["boot_val"],
                    "reset_val": row["reset_val"],
                    "enumvals": enumvals,
                    "context": row["context"],
                    "desc": row["desc"],
                    "pending_restart": row["pending_restart"],
                }
            )

        return [{"category": k, "rows": v} for k, v in ret.items()]
    else:
        return {setting["name"]: setting["setting"] for setting in tables}


def validate_setting(setting_name, setting_val, current_settings):
    do_not_check_names = ["unix_socket_permissions", "log_file_mode"]
    for pg_config_category in current_settings:
        for item in pg_config_category["rows"]:
            if item["name"] == setting_name:
                if (
                    item["name"] in do_not_check_names
                    or item["category"] == "Preset Options"
                ):
                    return True, item
                if item["vartype"] == "integer":
                    setting_val = int(human_to_number(setting_val, item["unit"]))
                    # Integers handling.
                    if item["min_val"] and setting_val < int(item["min_val"]):
                        return False, None
                    if item["max_val"] and setting_val > int(item["max_val"]):
                        return False, None
                    return True, item
                if item["vartype"] == "real":
                    setting_val = float(
                        human_to_number(setting_val, item["unit"], float)
                    )
                    # Real handling.
                    if item["min_val"] and setting_val < float(item["min_val"]):
                        return False, None
                    if item["max_val"] and setting_val > float(item["max_val"]):
                        return False, None
                    return True, item
                if item["vartype"] == "bool":
                    # Boolean handling.
                    if setting_val not in ["on", "off"]:
                        return False, None
                    return True, item
                if item["vartype"] == "enum":
                    # Enum handling.
                    if len(item["enumvals"]) > 0:
                        if setting_val not in item["enumvals"]:
                            return False, None
                        return True, item
                if item["vartype"] == "string":
                    return True, item


def post_settings(request, conn, update, commit_comment=None, new_config=True):
    current_settings = get_settings(conn)
    ret = {"settings": []}
    config_history = ConfigHistory.objects.filter(
        Q(user=request.user) & Q(connection=Connection.objects.get(id=conn.v_conn_id))
    )
    if not config_history:
        config_history = ConfigHistory(
            user=request.user,
            connection=Connection.objects.get(id=conn.v_conn_id),
            config_snapshot=json.dumps(get_settings(conn, grouped=False)),
            start_time=make_aware(datetime.now()),
            commit_comment="Initial Setup",
        )
        config_history.save()

    for setting_name, setting_val in update.items():
        try:
            setting_valid, item = validate_setting(
                setting_name, setting_val, current_settings
            )
        except ValueError as exc:
            raise ValidationError(code=400, message=f"{setting_name}: Invalid setting.")
        if item["category"] == "Preset Options":
            continue
        if setting_valid:
            if (
                (item["vartype"] == "integer" and setting_val != item["setting"])
                or (
                    item["vartype"] == "real"
                    and float(setting_val) != float(item["setting"])
                )
                or (
                    item["vartype"] not in ["integer", "real"]
                    and setting_val != item["setting"]
                )
            ):
                # At this point, all incoming parameters have been checked.
                if setting_val:
                    query = f"ALTER SYSTEM SET {setting_name} TO '{setting_val}';"
                else:
                    query = f"ALTER SYSTEM RESET {setting_name};"

                try:
                    conn.Execute(query)
                except Exception as exc:
                    if ret["settings"]:
                        for setting in ret["settings"]:
                            query = f"ALTER SYSTEM SET {setting['name']} TO '{setting['previous_setting']}';"
                            conn.Execute(query)
                    raise DatabaseError(exc) from exc

                ret["settings"].append(
                    {
                        "name": item["name"],
                        "setting": setting_val,
                        "previous_setting": item["setting_raw"],
                        "restart": True
                        if item["context"] in ["internal", "postmaster"]
                        else False,
                    }
                )

        else:
            if ret["settings"]:
                for setting in ret["settings"]:
                    query = f"ALTER SYSTEM SET {setting['name']} TO '{setting['previous_setting']}';"
                    conn.Execute(query)
            raise ValidationError(code=400, message=f"{setting_name}: Invalid setting.")

    # Reload PG configuration if there are any changes
    if ret["settings"]:
        conn.Execute("SELECT pg_reload_conf()")

        if new_config:
            updated_settings = get_settings(conn, grouped=False)

            config_history = ConfigHistory(
                user=request.user,
                connection=Connection.objects.get(id=conn.v_conn_id),
                config_snapshot=json.dumps(updated_settings),
                start_time=make_aware(datetime.now()),
                commit_comment=commit_comment,
            )
            config_history.save()

    return ret


def get_settings_status(conn):
    settings = get_settings(conn)
    pending_restart_changes = []
    pending_restart = False
    for category in settings:
        for row in category["rows"]:
            if row["pending_restart"] == "True":
                pending_restart = True
                pending_restart_changes.append(row)
    return {
        "restart_pending": pending_restart,
        "restart_changes": pending_restart_changes,
    }
