import re

import app.include.Spartacus as Spartacus


class MSSQL:
    def __init__(self, server, port, service, user, password, conn_id, alias):
        self.lock = None
        self.alias = alias
        self.db_type = "mssql"
        self.password = password
        self.conn_id = conn_id

        self.server = server
        self.active_server = server
        self.user = user
        self.active_user = user
        self.schema = service
        self.service = service
        self.active_service = service

        self.port = port

        self.conn_string = ""  # added just to avoid errors, because it is required

        if port is None or port == "":
            self.active_port = "1433"
        else:
            self.active_port = port

        self.connection = Spartacus.Database.MSSQL(
            self.active_server, self.active_port, self.active_service, self.active_user, self.password
        )

        self.console_help = "Console tab. Type the commands in the editor below this box. \? to view command list."

    # Decorator to acquire lock before performing action
    def lock_required(function):
        def wrap(self, *args, **kwargs):
            try:
                if self.lock != None:
                    self.lock.acquire()
            except:
                None
            try:
                r = function(self, *args, **kwargs)
            except:
                try:
                    if self.lock != None:
                        self.lock.release()
                except:
                    None
                raise
            try:
                if self.lock != None:
                    self.lock.release()
            except:
                None
            return r

        wrap.__doc__ = function.__doc__
        wrap.__name__ = function.__name__
        return wrap

    def GetVersion(self):
        return self.ExecuteScalar("SELECT @@VERSION;")

    def PrintDatabaseDetails(self):
        return self.active_server + ":" + self.active_port

    def PrintDatabaseInfo(self):
        return self.active_user + "@" + self.active_service

    def TestConnection(self):
        return_data = ""

        try:
            self.connection.Open()
            self.connection.Close()
            return_data = "Connection successful."
        except Exception as exc:
            return_data = str(exc)
        return return_data

    
    def GetErrorPosition(self, error_message, sql_cmd):
        ret = None
        try:
            err_token = re.search(".*near '(.*)'.*", error_message).group(1)
            if err_token:
                row = sql_cmd.count('\n', 0, sql_cmd.find(err_token)) + 1
                ret = {'row': row, 'col': 0}
        except AttributeError:
            pass

        return ret

    @lock_required
    def Query(self, sql, alltypesstr=False, simple=False):
        return self.connection.Query(sql, alltypesstr, simple)

    @lock_required
    def ExecuteScalar(self, sql):
        return self.connection.ExecuteScalar(sql)

    def QueryDatabases(self):
        return self.Query(
            """SELECT name, database_id
FROM sys.databases; """,
            True,
        )

    def QuerySchemas(self):
        return self.Query(
            """
 SELECT schema_name
      FROM INFORMATION_SCHEMA.SCHEMATA
      ORDER BY schema_name
"""
        )

    def QueryTables(self, all_schemas=False, schema=None):
        query_filter = ""
        if not all_schemas:
            if schema:
                query_filter = "and table_schema = '{0}' ".format(schema)
            else:
                query_filter = "and table_schema = '{0}' ".format(self.schema)
        return self.Query(
            """
            select table_name,
                   table_schema
            from information_schema.tables
            where table_type != 'VIEW'
            {0}
        """.format(
                query_filter
            ),
            True,
        )
