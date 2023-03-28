import functools
import json
import operator
import os
from abc import abstractmethod

from app.bgjob.jobs import BatchJob, IJobDesc, escape_dquotes_process_arg
from app.file_manager.file_manager import FileManager
from app.models.main import Connection
from app.utils.postgresql_utilities import get_utility_path
from app.views.memory_objects import database_required_new, user_authenticated
from django.http import JsonResponse


class Backup:
    """
    A base class representing backup type
    """

    GLOBALS = 1
    SERVER = 2
    OBJECT = 3

    TYPE_MAPPING = {
        "globals": GLOBALS,
        "server": SERVER,
        "objects": OBJECT,
    }

    @classmethod
    def create(cls, backup_type: int):
        if backup_type == cls.GLOBALS:
            return GlobalsBackup()
        if backup_type == cls.SERVER:
            return ServerBackup()
        if backup_type == cls.OBJECT:
            return ObjectBackup()

    @classmethod
    def get_backup_type(cls, backup_type_str):
        backup_type = cls.TYPE_MAPPING.get(backup_type_str)
        if backup_type is None:
            raise ValueError(f"Invalid backup type: {backup_type_str}")
        return backup_type

    @property
    @abstractmethod
    def type_desc(self):
        pass

    @property
    @abstractmethod
    def backup_type(self):
        pass

    @abstractmethod
    def get_message(self, connection_name, database):
        pass


class GlobalsBackup(Backup):
    @property
    def type_desc(self):
        return "Backing up the global objects"

    @property
    def backup_type(self):
        return "Backup Globals"

    def get_message(self, connection_name: str, database=None):
        return f"Backing up the global objects on the server '{connection_name}'"


class ServerBackup(Backup):
    @property
    def type_desc(self):
        return "Backing up the server"

    @property
    def backup_type(self):
        return "Backup Server"

    def get_message(self, connection_name: str, database=None):
        return f"Backing up the server '{connection_name}'"


class ObjectBackup(Backup):
    @property
    def type_desc(self):
        return "Backing up an object on the server"

    @property
    def backup_type(self):
        return "Backup Object"

    def get_message(self, connection_name: str, database=None):
        return f"Backing up an object on the server '{connection_name}' from database '{database}'"


class BackupMessage(IJobDesc):
    """
    BackupMessage(IJobDesc)

    Defines the message shown for the backup operation.
    """

    def __init__(self, backup_type: Backup, conn_id, backup_file, *args, **kwargs):
        self.backup_type = backup_type
        self.conn_id = conn_id
        self.bfile = backup_file
        self.database = kwargs["database"] if "database" in kwargs else None
        self.cmd = ""
        self.args_str = "{0} ({1}:{2})"

        # check to we need this?
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
    def type_desc(self):
        return self.backup_type.type_desc

    @property
    def message(self):
        connection_name = self.get_connection_name()
        return self.backup_type.get_message(connection_name, self.database)

    def details(self, cmd):
        server_name = self.get_connection_name()
        backup_type = self.backup_type.backup_type
        return {
            "message": self.message,
            "cmd": cmd + self.cmd,
            "server": server_name,
            "object": self.database,
            "type": backup_type,
        }


def get_args_params_values(data, conn, backup_obj_type, backup_file):
    """
    Used internally by create_backup_objects_job. This function will create
    the required args and params for the job.
    :param data: input data
    :param conn: connection obj
    :param backup_obj_type: object type
    :param backup_file: file name
    :return: args array
    """

    host, port = (conn.v_server, str(conn.v_port))
    args = [
        "--file",
        backup_file,
        "--host",
        host,
        "--port",
        port,
        "--username",
        conn.v_user,
        "--no-password",
    ]

    def set_param(key, param, assertion=True):
        if not assertion:
            return
        if data.get(key, None):
            args.append(param)

    def set_value(key, param, default_value=None, assertion=True):
        if not assertion:
            return
        val = data.get(key, default_value)
        if isinstance(val, int):
            val = str(val)
        if val:
            args.append(param)
            args.append(val)

    if backup_obj_type != "objects":
        args.append("--database")
        args.append(conn.v_service)

    set_param("verbose", "--verbose")
    set_param("dqoute", "--quote-all-identifiers")
    set_value("role", "--role")

    if backup_obj_type == "objects" and data.get("format", None):
        args.extend(
            [
                "--format={0}".format(
                    {"custom": "c", "tar": "t", "plain": "p", "directory": "d"}[
                        data["format"]
                    ]
                )
            ]
        )

        set_param("blobs", "--blobs", data["format"] in ["custom", "tar"])
        set_value(
            "compression_ratio",
            "--compress",
            None,
            data.get("format") in ["custom", "plain", "directory"],
        )

    set_param("only_data", "--data-only", data.get("only_data", None))
    set_param(
        "disable_trigger",
        "--disable-triggers",
        data.get("only_data", None) and data.get("format", "") == "plain",
    )

    set_param(
        "only_schema",
        "--schema-only",
        data.get("only_schema", None) and not data.get("only_data", None),
    )
    set_param("only_globals", "--globals-only", backup_obj_type != "objects")
    set_param("only_tablespaces", "--tablespaces-only", backup_obj_type != "objects")
    set_param("only_roles", "--roles-only", backup_obj_type != "objects")

    set_param("owner", "--no-owner")
    set_param("include_create_database", "--create")
    set_param("include_drop_commands", "--clean")
    set_param("pre_data", "--section=pre-data")
    set_param("data", "--section=data")
    set_param("post_data", "--section=post-data")
    set_param("privilege", "--no-privileges")
    set_param("tablespace", "--no-tablespaces")
    set_param("unlogged_tbl_data", "--no-unlogged-table-data")
    set_param("use_insert_commands", "--inserts")
    set_param("use_column_inserts", "--column-inserts")
    set_param("disable_quoting", "--disable-dollar-quoting")
    set_param("use_set_session_auth", "--use-set-session-authorization")

    set_param("comments", "--no-comments")
    set_param(
        "load_via_partition_root",
        "--load-via-partition-root",
    )

    set_value("encoding", "--encoding")
    set_value("number_of_jobs", "--jobs", None, data.get("format") == "directory")

    args.extend(
        functools.reduce(
            operator.iconcat,
            map(lambda s: ["--schema", s], data.get("schemas", [])),
            [],
        )
    )

    args.extend(
        functools.reduce(
            operator.iconcat, map(lambda t: ["--table", t], data.get("tables", [])), []
        )
    )

    return args


@database_required_new(check_timeout=True, open_connection=True)
@user_authenticated
def create_backup(request, database):
    """
    Creates a new job for backup task
    (Backup Database(s)/Schema(s)/Table(s))
    """

    data = json.loads(request.body) if request.body else {}

    backup_type_str = data.get("backup_type", "objects")

    data = data.get("data", {})

    backup_file = data.get("fileName")

    try:
        FileManager(request.user).check_access_permission(backup_file)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=403)

    utility = "pg_dump" if backup_type_str == "objects" else "pg_dumpall"

    utility_path = None
    try:
        utility_path = get_utility_path(utility, request.user)
    except FileNotFoundError:
        return JsonResponse(
            data={
                "data": f"'{utility_path if utility_path else utility}' file not found.Correct the Binary Path in Settings dialog."
            },
            status=400,
        )

    args = get_args_params_values(data, database, backup_type_str, backup_file)

    escaped_args = [escape_dquotes_process_arg(arg) for arg in args]

    try:
        backup_type = Backup.get_backup_type(backup_type_str)

        if backup_type_str == "objects":
            args.append(data["database"])
            escaped_args.append(data["database"])
            job = BatchJob(
                description=BackupMessage(
                    Backup.create(backup_type),
                    database.v_conn_id,
                    backup_file,
                    *args,
                    database=data["database"],
                ),
                cmd=utility_path,
                args=escaped_args,
                user=request.user,
            )
        else:
            job = BatchJob(
                description=BackupMessage(
                    Backup.create(backup_type),
                    database.v_conn_id,
                    backup_file,
                    *args,
                ),
                cmd=utility_path,
                args=escaped_args,
                user=request.user,
            )

        os.environ[str(job.id)] = database.v_password

        job.start()
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=410)

    return JsonResponse(
        data={"job_id": job.id, "description": job.description.message, "Success": 1}
    )


@database_required_new(check_timeout=True, open_connection=True)
@user_authenticated
def preview_command(request, database):
    data = json.loads(request.body) if request.body else {}

    backup_type_str = data.get("backup_type", "objects")

    data = data.get("data", {})

    backup_file = data.get("fileName")

    try:
        FileManager(request.user).check_access_permission(backup_file)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=403)

    utility = "pg_dump" if backup_type_str == "objects" else "pg_dumpall"

    utility_path = None
    try:
        utility_path = get_utility_path(utility, request.user)
    except FileNotFoundError:
        return JsonResponse(
            data={
                "data": f"'{utility_path if utility_path else utility}' file not found.Correct the Binary Path in Settings dialog."
            },
            status=400,
        )

    args = get_args_params_values(data, database, backup_type_str, backup_file)

    backup_type = Backup.get_backup_type(backup_type_str)

    backup_message = BackupMessage(
        Backup.create(backup_type),
        database.v_conn_id,
        backup_file,
        *args,
        database=data.get(database),
    )

    return JsonResponse(data={"command": backup_message.details(utility_path)})
