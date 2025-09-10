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
                row = sql_cmd.count("\n", 0, sql_cmd.find(err_token)) + 1
                ret = {"row": row, "col": 0}
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
            query_filter = f"AND table_schema = '{schema or self.schema}'"
        return self.Query(
            """
            SELECT table_name,
                   table_schema
            FROM information_schema.tables
            WHERE table_type != 'VIEW'
            {0}
        """.format(
                query_filter
            ),
            True,
        )

    def QueryTablesFields(self, table=None, all_schemas=False, schema=None):
        query_filter = f"WHERE c.object_id = OBJECT_ID('{schema}.{table}')"
        return self.Query(
            """
SELECT 
    c.name       AS column_name,
    t.name       AS data_type,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
{0}
ORDER BY c.column_id;
""".format(
                query_filter
            ),
            True,
        )

    def QueryViews(self, all_schemas=False, schema=None):
        query_filter = ""
        if not all_schemas:
            query_filter = f"WHERE table_schema = '{schema or self.schema}'"
        return self.Query(
            """
            SELECT table_name
FROM INFORMATION_SCHEMA.VIEWS
{0}
ORDER BY table_name;
        """.format(
                query_filter
            ),
            True,
        )

    def QueryProcedures(self, all_schemas=False, schema=None):
        query_filter = ""
        if not all_schemas:
            query_filter = f"WHERE s.name = '{schema or self.schema}'"
        return self.Query(
            """
SELECT p.name, p.object_id
FROM sys.procedures p
JOIN sys.schemas s ON p.schema_id = s.schema_id
{0}
ORDER BY s.name, p.name;
""".format(
                query_filter
            ),
            True,
        )

    def QueryFunctionFields(self, function, schema):
        query_filter = f"WHERE specific_schema = '{schema or self.schema}' AND specific_name = '{function}'"
        return self.Query(
            """
            SELECT parameter_name,
            data_type,
            CASE parameter_mode
                WHEN 'OUT' THEN 'O'
                WHEN 'INOUT' THEN 'X'
                ELSE 'I'
            END AS param_type
FROM INFORMATION_SCHEMA.PARAMETERS
                          {0}
ORDER BY ORDINAL_POSITION;
""".format(
                query_filter
            ),
            True,
        )

    def QueryFunctions(self, all_schemas=False, schema=None):
        query_filter = ""
        if not all_schemas:
            query_filter = f"AND routine_schema = '{schema or self.schema}'"
        return self.Query(
            """
SELECT routine_name
FROM INFORMATION_SCHEMA.ROUTINES
WHERE routine_type = 'FUNCTION'
{}
ORDER BY routine_name;
""".format(
                query_filter
            )
        )
