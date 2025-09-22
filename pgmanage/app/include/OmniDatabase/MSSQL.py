import re
from enum import Enum

import app.include.Spartacus as Spartacus
from sqlparse import format


class TemplateType(Enum):
    EXECUTE = 1
    SCRIPT = 2


class Template:
    def __init__(self, text, template_type=TemplateType.EXECUTE):
        self.text = text
        self.type = template_type


class MSSQL:
    def __init__(self, server, port, service, user, password, conn_id, alias, connection_params):
        self.lock = None
        self.connection_params = connection_params if connection_params else {}
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
            self.active_server,
            self.active_port,
            self.active_service,
            self.active_user,
            self.password,
            connection_params=self.connection_params,
        )
        self.has_schema = True

        self.console_help = "Console tab. Type the commands in the editor below this box."

        self.use_server_cursor = False

        self._version = None
        self._major_version = None

    @property
    def version(self):
        if self._version is None:
            self._fetch_version()
        return self._version

    @property
    def major_version(self):
        if self._major_version is None:
            self._fetch_version()
        return self._major_version

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

    def _fetch_version(self):
        try:
            self._version = self.ExecuteScalar("SELECT CAST(SERVERPROPERTY('productversion') AS VARCHAR);")
            self._major_version = self.ExecuteScalar("SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT);")
        except Exception:
            self._version = None
            self._major_version = None

    def GetVersion(self):
        return f"MSSQL {self.version}"

    def PrintDatabaseDetails(self):
        return self.active_server + ":" + self.active_port

    def PrintDatabaseInfo(self):
        return self.active_user + "@" + self.active_service

    @lock_required
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

    def QueryViewFields(self, table=None, all_schemas=False, schema=None):
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
            table_filter_part = f"AND fk.table_name = '{table}' " if table else " "
            schema_filter_part = f"AND fk.table_schema = '{schema or self.schema}'"
            query_filter += table_filter_part + schema_filter_part
        else:
            query_filter = f"AND fk.table_name = '{table}'" if table else ""

        return self.Query(
            """
SELECT 
    fk.table_schema,
    fk.table_name,
    fk.constraint_name,
    pk.table_schema AS r_table_schema,
    pk.TABLE_NAME   AS r_table_name,
    rc.unique_constraint_name AS r_constraint_name
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS fk
    ON rc.constraint_name = fk.constraint_name
    AND rc.constraint_schema = fk.constraint_schema
JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk
    ON rc.unique_constraint_name = pk.constraint_name
    AND rc.unique_constraint_schema = pk.constraint_schema
WHERE fk.constraint_type = 'FOREIGN KEY'
            {0}
    ORDER BY fk.table_schema, fk.table_name;;
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

        fkeys = fkey if isinstance(fkey, list) else [fkey]

        fkeys_list = ", ".join([f"'{str(e)}'" for e in fkeys])

        if fkeys_list:
            query_filter = query_filter + f"AND fk.constraint_name in ({fkeys_list}) "

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

    def TemplateSelect(self, schema, table, object_type):
        if object_type == "t":
            sql = "SELECT "
            fields = self.QueryTablesFields(table, False, schema)
            if fields.Rows:
                sql += ", ".join([f"t.{r['column_name']}" for r in fields.Rows])
            sql += f"\nFROM [{schema}].[{table}] t"
            pk = self.QueryTablesPrimaryKeys(table, False, schema)
            if pk.Rows:
                fields = self.QueryTablesPrimaryKeysColumns(pk.Rows[0]["constraint_name"], table, False, schema)
                if fields.Rows:
                    sql += "\nORDER BY " + ", ".join(f"t.{r['column_name']}" for r in fields.Rows)
        elif object_type == "v":
            sql = "SELECT "
            fields = self.QueryViewFields(table, False, schema)
            if fields.Rows:
                sql += ", ".join([f"t.{r['column_name']}" for r in fields.Rows])
            sql += f"\nFROM [{schema}].[{table}] t"
        else:
            sql = f"SELECT t.*\nFROM [{schema}].[{table}] t"
        formatted_sql = format(sql, keyword_case="upper", reindent=True)
        return Template(formatted_sql)

    def GetPropertiesDatabase(self, database_name):
        return self.Query(
            f"""
SELECT 
    d.name AS "Database",
    suser_sname(d.owner_sid) AS "Owner",
    d.state_desc  AS "State",
    d.is_encrypted AS "Encrypted",
    d.collation_name AS "Collation",
    d.user_access_desc AS "User Access",
    MIN(mf.physical_name) AS "File Path",
    CAST(SUM(mf.size) * 8. / 1024 AS DECIMAL(18, 2)) AS "SizeMB"               
FROM sys.databases d
JOIN sys.master_files mf
    ON d.database_id = mf.database_id        
    WHERE d.name = '{database_name}'
GROUP BY d.name, d.owner_sid, d.state_desc, d.is_encrypted, d.collation_name, d.user_access_desc
"""
        )

    def GetPropertiesLogin(self, login_name):
        return self.Query(
            f"""
SELECT 
    sp.name                  AS "Login Name",
    sp.type_desc             AS "Principal Type",
    sp.default_database_name AS "Default Database",
    sp.default_language_name AS "Default Language",
    sp.create_date           AS "Create Date",
    sp.modify_date           AS "Modify Date",
    sp.is_disabled           AS "Disabled",
    sl.is_policy_checked     AS "Policy Checked",
    sl.is_expiration_checked AS "Expiration Checked",
    sp.credential_id         AS "Credential ID"
FROM sys.server_principals sp
LEFT JOIN sys.sql_logins sl ON sp.principal_id = sl.principal_id
WHERE sp.type IN ('S', 'U', 'G') 
AND sp.name = '{login_name}'
"""
        )

    def GetPropertiesServerRole(self, role_name):
        return self.Query(
            f"""
        SELECT 
    r.name AS "Role Name",
    r.type_desc AS "Role Type",              
    r.create_date AS "Create Date",
    r.modify_date AS "Modify Date",
    r.is_fixed_role AS "Is Fixed Role"
    FROM sys.server_principals r
    WHERE r.type = 'R' AND r.name = '{role_name}'
"""
        )

    def GetPropertiesDatabaseRole(self, role_name):
        return self.Query(
            f"""
SELECT 
    r.name AS "Role Name",
    r.type_desc AS "Role Type",
    r.create_date AS "Create Date",
    r.modify_date AS "Modify Date",
    r.is_fixed_role AS "Is Fixed Role"
FROM sys.database_principals r
WHERE r.type = 'R' AND r.name = '{role_name}' 
"""
        )

    def GetPropertiesUser(self, user_name):
        return self.Query(
            f"""
SELECT 
    dp.name              AS "User Name",
    dp.type_desc         AS "User Type",
    dp.authentication_type_desc AS "Authentication Type",
    dp.default_schema_name AS "Default Schema",
    dp.create_date       AS "Create Date",
    dp.modify_date       AS "Modify Date",
    dp.owning_principal_id AS "Owning Principal Id",
    dp.principal_id      AS "Principal Id",
    dp.is_fixed_role     AS "Is Fixed Role"
FROM sys.database_principals dp
WHERE dp.type IN ('S', 'U', 'G')
  AND dp.sid IS NOT NULL
  AND dp.name = '{user_name}'
"""
        )

    def GetPropertiesSchema(self, schema_name):
        return self.Query(
            f"""
SELECT 
    s.name              AS "Schema Name",
    dp.name             AS "Owner Name",
    dp.type_desc        AS "Owner Type",
    s.schema_id         AS "Schema Id",
    s.principal_id      AS "Principal Id",
    dp.create_date       AS "Create Date",
    dp.modify_date       AS "Modify Date"
FROM sys.schemas s
JOIN sys.database_principals dp 
     ON s.principal_id = dp.principal_id
WHERE s.name = '{schema_name}'
"""
        )

    def GetPropertiesTable(self, schema, table_name):
        return self.Query(
            f"""
SELECT 
    s.name          AS "Schema Name",
    t.name          AS "Table Name",
    t.object_id     AS "Object Id",
    t.create_date   AS "Create Date",
    t.modify_date   AS "Modify Date",
    t.is_ms_shipped AS "Is System Table"
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = '{table_name}' AND s.name = '{schema}'
"""
        )

    def GetPropertiesView(self, schema, view_name):
        return self.Query(
            f"""
SELECT 
    s.name          AS "Schema Name",
    v.name          AS "View Name",
    v.object_id     AS "Object Id",
    v.create_date   AS "Create Date",
    v.modify_date   AS "Modify Date"
FROM sys.views v
JOIN sys.schemas s ON v.schema_id = s.schema_id
WHERE v.name = '{view_name}' AND s.name = '{schema}'
"""
        )

    def GetPropertiesFunction(self, schema, function_name):
        return self.Query(
            f"""
SELECT 
    s.name        AS "Schema Name",
    f.name        AS "Function Name",
    f.object_id   AS "Object Id",
    f.type_desc   AS "Function Type",
    f.create_date AS "Create Date",
    f.modify_date AS "Modify Date"
FROM sys.objects f
JOIN sys.schemas s ON f.schema_id = s.schema_id
WHERE f.type IN ('FN', 'IF', 'TF') AND s.name = '{schema}' AND f.name = '{function_name}'
"""
        )

    def GetPropertiesProcedure(self, schema, procedure_name):
        return self.Query(
            f"""
SELECT 
    s.name        AS "Schema Name",
    p.name        AS "Procedure Name",
    p.object_id   AS "Object Id",
    p.create_date AS "Create Date",
    p.modify_date AS "Modify Date"
FROM sys.procedures p
JOIN sys.schemas s ON p.schema_id = s.schema_id
WHERE s.name = '{schema}' AND p.name = '{procedure_name}'
"""
        )

    def GetPropertiesTableField(self, schema, table, table_field):
        return self.Query(
            f"""
SELECT 
    s.name              AS "Schema Name",
    t.name              AS "Table Name",
    c.name              AS "Column Name",
    c.column_id         AS "Column Id",
    ty.name             AS "Data Type",
    c.max_length        AS "Max Length",
    c.precision         AS "Precision",
    c.scale             AS "Scale",
    c.is_nullable       AS "Is Nullable",
    c.is_identity       AS "Is Identity",
    dc.definition       AS "Default Definition",
    c.collation_name    AS "Collation"
FROM sys.columns c
JOIN sys.tables t         ON c.object_id = t.object_id
JOIN sys.schemas s        ON t.schema_id = s.schema_id
JOIN sys.types ty         ON c.user_type_id = ty.user_type_id
LEFT JOIN sys.default_constraints dc 
       ON c.default_object_id = dc.object_id
WHERE s.name = '{schema}'
  AND t.name = '{table}'
  AND c.name = '{table_field}'
ORDER BY c.column_id;
"""
        )

    def GetPropertiesPK(self, schema, table, constraint_name):
        return self.Query(
            f"""
SELECT 
    kc.table_schema     AS "Schema Name",
    kc.table_name       AS "Table Name",
    kc.constraint_name  AS "Constraint Name",
    kc.COLUMN_NAME      AS "Column Name",
    kc.ORDINAL_POSITION AS "Column Order"
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
    ON kc.constraint_name = tc.constraint_name
   AND kc.table_schema = tc.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND kc.table_schema = '{schema}'
  AND kc.table_name   = '{table}'
  AND kc.constraint_name = '{constraint_name}'
"""
        )

    def GetPropertiesFK(self, schema, table, constraint_name):
        return self.Query(
            f"""
       SELECT fk.table_schema AS "Table Schema",
       fk.table_name AS "TABLE NAME",
       fk.constraint_name AS "Constraint Name",
       fk.column_name AS "Column Name",
       pk.table_schema AS "Referenced Schema",
       pk.table_name   AS "Referenced Table",
       pk.column_name  AS "Referenced Column",
       update_rule AS "Update Rule",
       delete_rule AS "Delete Rule"
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk
  ON rc.constraint_name = fk.constraint_name
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
  ON rc.unique_constraint_name = pk.constraint_name
  AND fk.ordinal_position = pk.ordinal_position
WHERE fk.table_schema = '{schema}'
  AND fk.table_name   = '{table}'
  AND fk.constraint_name = '{constraint_name}'
"""
        )

    def GetPropertiesCheck(self, schema, table, constraint_name):
        return self.Query(
            f"""
SELECT 
    s.name             AS "Schema Name",
    t.name             AS "Table Name",
    cc.name            AS "Constraint Name",
    cc.definition      AS "Check Definition",
    cc.is_disabled     AS "Is Disabled",
    cc.is_not_for_replication AS "Is Not For Replication",
    cc.create_date     AS "Create Date",
    cc.modify_date     AS "Modify Date"
FROM sys.check_constraints cc
JOIN sys.tables t   ON cc.parent_object_id = t.object_id
JOIN sys.schemas s  ON t.schema_id = s.schema_id
WHERE s.name = '{schema}'
  AND t.name = '{table}'
  AND cc.name = '{constraint_name}'
"""
        )

    def GetPropertiesUnique(self, schema, table, constraint_name):
        return self.Query(
            f"""
SELECT 
    kc.table_schema     AS "Schema Name",
    kc.table_name       AS "Table Name",
    kc.constraint_name  AS "Constraint Name",
    kc.column_name      AS "Column Name",
    kc.ordinal_position AS "Column Order"
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
    ON kc.constraint_name = tc.constraint_name
   AND kc.table_schema = tc.table_schema
WHERE tc.constraint_type = 'UNIQUE'
  AND kc.table_schema = '{schema}'
  AND kc.table_name   = '{table}'
  AND kc.constraint_name = '{constraint_name}'
"""
        )

    def GetPropertiesIndex(self, schema, table, index_name):
        return self.Query(
            f""" 
    SELECT 
    s.name          AS "Schema Name",
    t.name          AS "Table Name",
    i.name          AS "Index Name",
    i.index_id      AS "Index Id",
    i.type_desc     AS "Index Type",
    i.is_unique     AS "Is Unique",
    i.is_primary_key AS "Is Primary Key",
    i.is_unique_constraint AS "Is Unique Constraint",
    i.is_disabled   AS "Is Disabled",
    i.fill_factor   AS "Fill Factor",
    i.allow_row_locks AS "Allow Row Locks",
    i.allow_page_locks AS "Allow Page Locks",
    STRING_AGG(
        CASE 
            WHEN ic.is_included_column = 1 
                THEN c.name + ' (INCLUDE)'
            ELSE c.name + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
        END, ', '
    ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS "Columns"
FROM sys.indexes i
JOIN sys.tables t ON i.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE s.name = '{schema}'
  AND t.name = '{table}'
  AND i.name = '{index_name}'
GROUP BY 
    s.name, t.name, i.name, i.index_id, i.type_desc,
    i.is_unique, i.is_primary_key, i.is_unique_constraint,
    i.is_disabled, i.fill_factor, i.allow_row_locks, i.allow_page_locks;
"""
        )

    def GetPropertiesStatistic(self, schema, table, statistic_name):
        return self.Query(
            f"""
SELECT 
    s.name        AS "Schema Name",
    t.name        AS "Table Name",
    st.name       AS "Statistic Name",
    st.stats_id   AS "Stats Id",
    st.auto_created AS "Auto Created",
    st.user_created AS "User Created",
    STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY sc.stats_column_id) AS "Columns"
FROM sys.stats st
JOIN sys.stats_columns sc 
     ON st.object_id = sc.object_id AND st.stats_id = sc.stats_id
JOIN sys.columns c 
     ON sc.object_id = c.object_id AND sc.column_id = c.column_id
JOIN sys.tables t 
     ON st.object_id = t.object_id
JOIN sys.schemas s 
     ON t.schema_id = s.schema_id
WHERE s.name = '{schema}'
  AND t.name = '{table}'
  AND st.name = '{statistic_name}'
  GROUP BY s.name, t.name, st.name, st.stats_id, st.auto_created, st.user_created
"""
        )

    def GetPropertiesViewField(self, schema, view, view_field):
        return self.Query(
            f"""
SELECT 
    c.name       AS "Column Name",
    t.name       AS "Data Type",
    c.max_length AS "Max Length",
    c.precision  AS "Precision",
    c.scale AS "Scale",
    c.is_nullable AS "Is Nullable",
    c.is_identity AS "Is Identity"
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('{schema}.{view}') AND c.name = '{view_field}'
"""
        )

    def GetPropertiesTrigger(self, schema, table, trigger):
        return self.Query(
            f"""
SELECT 
    sch.name        AS "Schema Name",
    tbl.name        AS "Table Name",
    tr.name         AS "Trigger Name",
    tr.object_id    AS "Object Id",
    tr.is_disabled  AS "Is Disabled",
    tr.is_instead_of_trigger AS "Is Instead Of",
    tr.create_date  AS "Create Date",
    tr.modify_date  AS "Modify Date"
FROM sys.triggers tr
JOIN sys.tables tbl
     ON tr.parent_id = tbl.object_id
JOIN sys.schemas sch
     ON tbl.schema_id = sch.schema_id
LEFT JOIN sys.sql_modules m
     ON tr.object_id = m.object_id
WHERE sch.name = '{schema}'
  AND tbl.name = '{table}'
  AND tr.name = '{trigger}';

"""
        )

    def GetProperties(self, schema, table, object_name, object_type):
        if object_type == "database":
            return self.GetPropertiesDatabase(object_name).Transpose("Property", "Value")
        if object_type == "login":
            return self.GetPropertiesLogin(object_name).Transpose("Property", "Value")
        if object_type == "server_role":
            return self.GetPropertiesServerRole(object_name).Transpose("Property", "Value")
        if object_type == "database_role":
            return self.GetPropertiesDatabaseRole(object_name).Transpose("Property", "Value")
        if object_type == "user":
            return self.GetPropertiesUser(object_name).Transpose("Property", "Value")
        if object_type == "schema":
            return self.GetPropertiesSchema(object_name).Transpose("Property", "Value")
        if object_type == "table":
            return self.GetPropertiesTable(schema, object_name).Transpose("Property", "Value")
        if object_type == "table_field":
            return self.GetPropertiesTableField(schema, table, object_name).Transpose("Property", "Value")
        if object_type == "index":
            return self.GetPropertiesIndex(schema, table, object_name).Transpose("Property", "Value")
        if object_type == "primary_key":
            return self.GetPropertiesPK(schema, table, object_name).Transpose("Property", "Value")
        if object_type == "foreign_key":
            return self.GetPropertiesFK(schema, table, object_name).Transpose("Property", "Value")
        if object_type == "unique":
            return self.GetPropertiesUnique(schema, table, object_name).Transpose("Property", "Value")
        if object_type == "check":
            return self.GetPropertiesCheck(schema, table, object_name).Transpose("Property", "Value")
        if object_type == "statistic":
            return self.GetPropertiesStatistic(schema, table, object_name).Transpose("Property", "Value")
        if object_type == "view":
            return self.GetPropertiesView(schema, object_name).Transpose("Property", "Value")
        if object_type == "view_field":
            return self.GetPropertiesViewField(schema, table, object_name).Transpose("Property", "Value")
        if object_type == "function":
            return self.GetPropertiesFunction(schema, object_name).Transpose("Property", "Value")
        if object_type == "procedure":
            return self.GetPropertiesProcedure(schema, object_name).Transpose("Property", "Value")
        if object_type == "trigger":
            return self.GetPropertiesTrigger(schema, table, object_name).Transpose("Property", "Value")

    def GetDDLTable(self, table):
        return self.ExecuteScalar(
            f"""
SELECT
  'CREATE TABLE ' + so.name + ' (' + o.list + ')' + CASE
    WHEN tc.Constraint_Name IS NULL THEN ''
    ELSE '\n\nALTER TABLE ' + so.Name + ' ADD CONSTRAINT ' + tc.Constraint_Name + ' PRIMARY KEY ' + ' (' + LEFT(j.List, Len(j.List) -1) + ')'
  END
FROM
  sysobjects so
  CROSS APPLY (
    SELECT
        '\n\t' + column_name + ' ' + data_type + CASE data_type
        WHEN 'sql_variant' THEN ''
        WHEN 'text' THEN ''
        WHEN 'ntext' THEN ''
        WHEN 'xml' THEN ''
        WHEN 'decimal' THEN '(' + cast(numeric_precision AS varchar) + ', ' + cast(numeric_scale AS varchar) + ')'
        ELSE coalesce(
          '(' + CASE
            WHEN character_maximum_length = -1 THEN 'MAX'
            ELSE cast(character_maximum_length AS varchar)
          END + ')',
          ''
        )
      END + ' ' + CASE
        WHEN EXISTS (
          SELECT
            id
          FROM
            syscolumns
          WHERE
            object_name(id) = so.name
            AND name = column_name
            AND columnproperty(id, name, 'IsIdentity') = 1
        ) THEN 'IDENTITY(' + cast(ident_seed(so.name) AS varchar) + ',' + cast(ident_incr(so.name) AS varchar) + ')'
        ELSE ''
      END + ' ' + (
        CASE
          WHEN UPPER(IS_NULLABLE) = 'NO' THEN 'NOT '
          ELSE ''
        END
      ) + 'NULL' + CASE
        WHEN information_schema.columns.COLUMN_DEFAULT IS NOT NULL THEN ' DEFAULT ' + information_schema.columns.COLUMN_DEFAULT
        ELSE ''
      END + ', '
    FROM
      information_schema.columns
    WHERE
      table_name = so.name
    ORDER BY
      ordinal_position
    FOR XML
      PATH ('')
  ) o (list)
  LEFT JOIN information_schema.table_constraints tc ON tc.Table_name = so.Name
  AND tc.Constraint_Type = 'PRIMARY KEY'
  CROSS APPLY (
    SELECT
      Column_Name + ', '
    FROM
      information_schema.key_column_usage kcu
    WHERE
      kcu.Constraint_Name = tc.Constraint_Name
    ORDER BY
      ORDINAL_POSITION
    FOR XML
      PATH ('')
  ) j (list)
WHERE
  xtype = 'U'
  AND name NOT IN ('dtproperties')
  AND so.name = '{table}'
"""
        )

    def GetDDLProcedure(self, schema, procedure):
        return self.ExecuteScalar(
            f"""
    SELECT OBJECT_DEFINITION(p.object_id) AS [Definition]
FROM sys.procedures p
JOIN sys.schemas s ON p.schema_id = s.schema_id
WHERE s.name = '{schema}'
AND p.name = '{procedure}'
"""
        )

    def GetDDLView(self, schema, view):
        return self.ExecuteScalar(
            f"""
SELECT OBJECT_DEFINITION(v.object_id) AS [Definition]
FROM sys.views v
JOIN sys.schemas s ON v.schema_id = s.schema_id
WHERE s.name = '{schema}'
AND v.name = '{view}'
"""
        )

    def GetDDLFunction(self, schema, function):
        return self.ExecuteScalar(
            f"""
SELECT OBJECT_DEFINITION(f.object_id) AS [Definition]
FROM sys.objects f
JOIN sys.schemas s ON f.schema_id = s.schema_id
WHERE f.type in ('FN', 'IF', 'TF')
AND s.name = '{schema}'
AND f.name = '{function}'
"""
        )

    def GetDDLTrigger(self, schema, trigger):
        return self.ExecuteScalar(
            f"""
SELECT OBJECT_DEFINITION(tr.object_id) AS [Definition]
FROM sys.triggers tr
JOIN sys.tables tbl
     ON tr.parent_id = tbl.object_id
JOIN sys.schemas s ON tbl.schema_id = s.schema_id
WHERE s.name = '{schema}'
AND tr.name = '{trigger}'
"""
        )

    def GetDDL(self, schema, table, object_name, object_type):
        if object_type == "table":
            return self.GetDDLTable(object_name)
        if object_type == "view":
            return self.GetDDLView(schema, object_name)
        if object_type == "function":
            return self.GetDDLFunction(schema, object_name)
        if object_type == "procedure":
            return self.GetDDLProcedure(schema, object_name)
        if object_type == "trigger":
            return self.GetDDLTrigger(schema, object_name)
        return ""
