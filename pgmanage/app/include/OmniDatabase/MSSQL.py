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
        result = self.ExecuteScalar("SELECT @@VERSION;")
        match = re.search(r"^(.*?)Copyright", result, flags=re.S)
        return match.group(1).strip() if match else result

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

    def QueryTablesStatistics(self, table=None, all_schemas=False, schema=None):
        query_filter = ""
        if not all_schemas:
            table_filter_part = f"AND t.name = '{table}' " if table else " "
            schema_filter_part = f"AND s.name = '{schema or self.schema}' "
            query_filter += table_filter_part + schema_filter_part
        else:
            query_filter = f"AND t.name = '{table}'" if table else ""

        return self.Query(
            """
SELECT 
    s.name AS schema_name,
    t.name AS table_name,
    st.name AS statistic_name
FROM sys.stats st
JOIN sys.tables t 
    ON st.object_id = t.object_id
JOIN sys.schemas s 
    ON t.schema_id = s.schema_id
WHERE 1=1
        {0}
        
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

    def QueryTablesPrimaryKeys(self, table=None, all_schemas=False, schema=None):
        query_filter = ""

        if not all_schemas:
            table_filter_part = f"AND table_name = '{table}' " if table else " "
            schema_filter_part = f"AND table_schema = '{schema or self.schema}'"
            query_filter += table_filter_part + schema_filter_part
        else:
            query_filter = f"AND table_name = '{table}'" if table else ""

        return self.Query(
            """

SELECT table_schema,
       table_name,
       constraint_name
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
WHERE constraint_type = 'PRIMARY KEY'
        {0}
ORDER BY table_schema, table_name;
""".format(
                query_filter
            )
        )

    def QueryTablesPrimaryKeysColumns(self, pkey, table=None, all_schemas=False, schema=None):
        query_filter = ""
        if not all_schemas:
            table_filter_part = f"AND kc.table_name = '{table}' " if table else " "
            schema_filter_part = f"AND kc.table_schema = '{schema or self.schema}' "
            query_filter += table_filter_part + schema_filter_part
        else:
            query_filter = f"AND kc.table_name = '{table}'" if table else ""

        query_filter = query_filter + f"AND kc.constraint_name = '{pkey}' "
        return self.Query(
            """
            SELECT kc.table_schema,
       kc.table_name,
       kc.constraint_name,
       kc.column_name,
       kc.ordinal_position
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
  ON kc.constraint_name = tc.constraint_name
  AND kc.table_schema = tc.table_schema
WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
  {0}
ORDER BY kc.table_schema, kc.table_name, kc.ordinal_position;
        """.format(
                query_filter
            ),
            True,
        )

    def QueryTablesForeignKeys(self, table=None, all_schemas=False, schema=None):
        query_filter = ""

        if not all_schemas:
            table_filter_part = f"AND table_name = '{table}' " if table else " "
            schema_filter_part = f"AND table_schema = '{schema or self.schema}'"
            query_filter += table_filter_part + schema_filter_part
        else:
            query_filter = f"AND table_name = '{table}'" if table else ""

        return self.Query(
            """

    SELECT table_schema,
        table_name,
        constraint_name
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
    WHERE constraint_type = 'FOREIGN KEY'
            {0}
    ORDER BY table_schema, table_name;
    """.format(
                query_filter
            )
        )

    def QueryTablesForeignKeysColumns(self, fkey, table=None, all_schemas=False, schema=None):
        query_filter = ""
        if not all_schemas:
            table_filter_part = f"AND fk.table_name = '{table}' " if table else " "
            schema_filter_part = f"AND fk.table_schema = '{schema or self.schema}' "
            query_filter += table_filter_part + schema_filter_part
        else:
            query_filter = f"AND fk.table_name = '{table}'" if table else ""

        query_filter = query_filter + f"AND fk.constraint_name = '{fkey}' "

        return self.Query(
            """
        SELECT fk.table_schema,
       fk.table_name,
       fk.constraint_name,
       fk.column_name,
       pk.table_schema AS r_schema_name,
       pk.table_name   AS r_table_name,
       pk.column_name  AS r_column_name,
        update_rule,
        delete_rule
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk
  ON rc.constraint_name = fk.constraint_name
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
  ON rc.unique_constraint_name = pk.constraint_name
  AND fk.ordinal_position = pk.ordinal_position
WHERE 1=1
                          {0}
ORDER BY fk.table_schema, fk.table_name, fk.constraint_name, fk.ordinal_position;
""".format(
                query_filter
            ),
            True,
        )

    def QueryTablesUniques(self, table=None, all_schemas=False, schema=None):
        query_filter = ""

        if not all_schemas:
            table_filter_part = f"AND table_name = '{table}' " if table else " "
            schema_filter_part = f"AND table_schema = '{schema or self.schema}'"
            query_filter += table_filter_part + schema_filter_part
        else:
            query_filter = f"AND table_name = '{table}'" if table else ""

        return self.Query(
            """

    SELECT table_schema,
        table_name,
        constraint_name
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
    WHERE constraint_type = 'UNIQUE'
            {0}
    ORDER BY table_schema, table_name;
    """.format(
                query_filter
            )
        )

    def QueryTablesUniquesColumns(self, unique_name, table=None, all_schemas=False, schema=None):
        query_filter = ""
        if not all_schemas:
            table_filter_part = f"AND kc.table_name = '{table}' " if table else " "
            schema_filter_part = f"AND kc.table_schema = '{schema or self.schema}' "
            query_filter += table_filter_part + schema_filter_part
        else:
            query_filter = f"AND kc.table_name = '{table}'" if table else ""

        query_filter = query_filter + f"AND kc.constraint_name = '{unique_name}' "
        return self.Query(
            """
    SELECT kc.table_schema,
        kc.table_name,
        kc.constraint_name,
        kc.column_name,
        kc.ordinal_position
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
    JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    ON kc.constraint_name = tc.constraint_name
    AND kc.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'UNIQUE'
    {0}
    ORDER BY kc.table_schema, kc.table_name, kc.ordinal_position;
    """.format(
                query_filter
            ),
            True,
        )

    def QueryTablesIndexes(self, table=None, all_schemas=False, schema=None):
        query_filter = ""

        if not all_schemas:
            table_filter_part = f"AND t.name = '{table}' " if table else " "
            schema_filter_part = f"AND s.name = '{schema or self.schema}'"
            query_filter += table_filter_part + schema_filter_part
        else:
            query_filter = f"AND t.name = '{table}'" if table else ""
        return self.Query(
            """
SELECT 
i.name AS index_name,
i.is_unique
FROM sys.indexes i
JOIN sys.tables t  ON i.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE i.is_hypothetical = 0 
  AND i.index_id > 0 
        {0}
ORDER BY i.name;
""".format(
                query_filter
            ),
            True,
        )

    def QueryTablesIndexesColumns(self, index_name, table=None, all_schemas=False, schema=None):
        query_filter = ""
        if not all_schemas:
            table_filter_part = f"AND t.name = '{table}' " if table else " "
            schema_filter_part = f"AND s.name = '{schema or self.schema}' "
            query_filter += table_filter_part + schema_filter_part
        else:
            query_filter = f"AND t.name = '{table}'" if table else ""

        query_filter = query_filter + f"AND i.name = '{index_name}' "
        return self.Query(
            """
SELECT c.name AS column_name
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c        ON ic.object_id = c.object_id AND ic.column_id = c.column_id
JOIN sys.tables t         ON i.object_id = t.object_id
JOIN sys.schemas s        ON t.schema_id = s.schema_id
WHERE i.is_hypothetical = 0
  AND i.index_id > 0
{0}
;
""".format(
                query_filter
            )
        )

    def QueryTablesChecks(self, table=None, all_schemas=False, schema=None):
        query_filter = ""

        if not all_schemas:
            table_filter_part = f"AND ctu.table_name = '{table}' " if table else " "
            schema_filter_part = f"AND cc.constraint_schema = '{schema or self.schema}'"
            query_filter += table_filter_part + schema_filter_part
        else:
            query_filter = f"AND ctu.table_name = '{table}'" if table else ""

        return self.Query(
            """
        SELECT cc.constraint_schema,
       ctu.table_name,
       cc.constraint_name,
       cc.check_clause
        FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
        JOIN INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE ctu
        ON cc.constraint_name = ctu.constraint_name
        WHERE 1=1
                                {0}
        ORDER BY cc.constraint_name;
""".format(
                query_filter
            ),
            True,
        )

    def QueryTablesTriggers(self, table=None, all_schemas=False, schema=None):
        query_filter = ""

        if not all_schemas:
            table_filter_part = f"AND t.name = '{table}' " if table else " "
            schema_filter_part = f"AND s.name = '{schema or self.schema}'"
            query_filter += table_filter_part + schema_filter_part
        else:
            query_filter = f"AND t.name = '{table}'" if table else ""

        return self.Query(
            """
SELECT 
    s.name AS schema_name,
    t.name AS table_name,
    tr.name AS trigger_name,
    tr.is_disabled AS is_disabled
FROM sys.triggers tr
JOIN sys.tables t 
    ON tr.parent_id = t.object_id
JOIN sys.schemas s 
    ON t.schema_id = s.schema_id
WHERE 1=1
    {0}
""".format(
                query_filter
            ),
            True,
        )

    def QueryServerRoles(self):
        return self.Query(
            """
    SELECT 
    sp.name AS role_name
FROM sys.server_principals sp
WHERE sp.type = 'R'
ORDER BY sp.name;
"""
        )

    def QueryDatabaseRoles(self):
        return self.Query(
            """
SELECT 
    dp.name       AS role_name
FROM sys.database_principals dp
WHERE dp.type = 'R'
ORDER BY dp.name;
"""
        )

    def QueryLogins(self):
        return self.Query(
            """
    SELECT 
    sp.name AS login_name
FROM sys.server_principals sp
WHERE sp.type IN ('S', 'U', 'G')
ORDER BY sp.name;
"""
        )

    def QueryUsers(self):
        return self.Query(
            """
                          SELECT 
    dp.name  AS user_name
FROM sys.database_principals dp
WHERE dp.type IN ('S', 'U', 'G')  
  AND dp.sid IS NOT NULL 
ORDER BY dp.name;
                          """
        )
