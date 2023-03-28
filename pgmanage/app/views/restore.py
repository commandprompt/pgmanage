import json
import os
import shutil

from app.bgjob.jobs import BatchJob, IJobDesc
from app.file_manager.file_manager import FileManager
from app.models.main import Connection
from app.views.memory_objects import database_required_new, user_authenticated
from django.http import JsonResponse


class RestoreMessage(IJobDesc):
    def __init__(self, conn_id, backup_file, *args, **kwargs):
        self.conn_id = conn_id
        self.backup_file = backup_file
        self.database = kwargs.get("database")
        self.cmd = ""

        def cmd_arg(x):
            if x:
                x = x.replace("\\", "\\\\")
                x = x.replace('"', '\\"')
                x = x.replace('""', '\\"')
                return ' "' + x + '"'
            return ""

        for arg in args:
            if arg and len(arg) >= 2 and arg[:2] == "--":
                self.cmd += " " + arg
            else:
                self.cmd += cmd_arg(arg)

    def get_connection_name(self):
        conn = Connection.objects.filter(id=self.conn_id).first()

        if conn is None:
            return "Not available"

        host = conn.ssh_server if conn.use_tunnel else conn.server
        port = conn.ssh_port if conn.use_tunnel else conn.port

        return f"{conn.database} ({host}:{port})"

    @property
    def message(self):
        connection_name = self.get_connection_name()
        return f"Restoring backup on the server '{connection_name}'"

    @property
    def type_desc(self):
        return "Restoring backup on the server"

    def details(self, cmd):
        return {
            "message": self.message,
            "cmd": cmd + self.cmd,
            "server": self.get_connection_name(),
            "object": getattr(self, "database", ""),
            "type": "Restore",
        }


def get_args_param_values(data, conn, backup_file, listing_file=None):

    host, port = (conn.v_server, str(conn.v_port))

    args = [
        "--host",
        host,
        "--port",
        port,
        "--username",
        conn.v_user,
        "--no-password",
    ]

    def set_value(key, param, data, args):
        if data.get(key):
            args.append(param)
            args.append(data.get(key))

    def set_param(key, param, data, args):
        if data.get(key):
            args.append(param)
            return True
        return False

    if data.get("type") == "server":
        set_param("quiet", "--quiet", data, args)
        set_param("echo_queries", "--echo-queries", data, args)
        args.extend(["-f", backup_file])
        return args
    set_value("role", "--role", data, args)
    set_value("database", "--dbname", data, args)

    set_param("pre_data", "--section=pre-data", data, args)
    set_param("data", "--section=data", data, args)
    set_param("post_data", "--section=post-data", data, args)

    if not set_param("only_data", "--data-only", data, args):
        set_param("dns_owner", "--no-owner", data, args)
        set_param("dns_privilege", "--no-privileges", data, args)
        set_param("dns_tablespace", "--no-tablespaces", data, args)

    if not set_param("only_schema", "--schema-only", data, args):
        set_param("disable_trigger", "--disable-triggers", data, args)

    set_param("include_create_database", "--create", data, args)
    set_param("clean", "--clean", data, args)
    set_param("single_transaction", "--single-transaction", data, args)
    set_param("no_data_fail_table", "--no-data-for-failed-tables", data, args)
    set_param("use_set_session_auth", "--use-set-session-authorization", data, args)
    set_param("exit_on_error", "--exit-on-error", data, args)

    set_value("number_of_jobs", "--jobs", data, args)
    set_param("verbose", "--verbose", data, args)

    # TODO these values may also include many instances,
    # change it when we can support it in frontend part.
    set_value("schema", "--schema", data, args)
    set_value("table", "--table", data, args)
    set_value("trigger", "--trigger", data, args)
    set_value("function", "--function", data, args)

    args.append(backup_file)

    return args


@database_required_new(check_timeout=True, open_connection=True)
@user_authenticated
def create_restore(request, database):

    data = json.loads(request.body) if request.body else {}

    data = data.get("data", {})

    utility = "psql" if data.get("type") == "server" else "pg_restore"

    ret_val = shutil.which(utility)

    if not ret_val:
        return JsonResponse(data={"data": f"'{utility}' file not found."}, status=400)

    backup_file = data.get("fileName")

    try:
        FileManager(request.user).check_access_permission(backup_file)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=403)

    args = get_args_param_values(data, database, backup_file)

    restore_message = RestoreMessage(
        database.v_conn_id, backup_file, *args, database=data.get("database")
    )
    try:
        job = BatchJob(
            description=restore_message, cmd=utility, args=args, user=request.user
        )

        os.environ[str(job.id)] = database.v_password

        job.start()
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=410)

    return JsonResponse(data={"job_id": job.id,"description": job.description.message, "Success": 1})


@database_required_new(check_timeout=True, open_connection=True)
@user_authenticated
def preview_command(request, database):
    data = json.loads(request.body) if request.body else {}

    data = data.get("data", {})

    backup_file = data.get("fileName")

    try:
        FileManager(request.user).check_access_permission(backup_file)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=403)

    utility = "psql" if data.get("type") == "server" else "pg_restore"

    args = get_args_param_values(data, database, backup_file)

    restore_message = RestoreMessage(
        database.v_conn_id, backup_file, *args, database=data.get("database")
    )

    return JsonResponse(data={"command": restore_message.details(utility)})
