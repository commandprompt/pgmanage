'''
The MIT License (MIT)

Portions Copyright (c) 2015-2019, The OmniDB Team
Portions Copyright (c) 2017-2019, 2ndQuadrant Limited

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

from enum import Enum
from urllib.parse import urlparse

import app.include.Spartacus as Spartacus

from .sql_templates import get_template

'''
------------------------------------------------------------------------
Template
------------------------------------------------------------------------
'''
class TemplateType(Enum):
    EXECUTE = 1
    SCRIPT = 2

class Template:
    def __init__(self, text, template_type=TemplateType.EXECUTE):
        self.text = text
        self.type = template_type

'''
------------------------------------------------------------------------
Oracle
------------------------------------------------------------------------
'''
class Oracle:
    def __init__(self, server, port, service, user, password, conn_id=0, alias='', conn_string='', parse_conn_string = False, connection_params=None):
        self.lock = None
        self.connection_params = connection_params if connection_params else {}
        self.alias = alias
        self.db_type = 'oracle'
        self.conn_string = conn_string
        self.conn_string_error = ''
        self.password = password
        self.conn_id = conn_id

        self.port = port
        if port is None or port == '':
            self.active_port = '1521'
        else:
            self.active_port = port

        self.service = service.upper()
        if service is None or service == '':
            self.active_service = 'XE'
        else:
            self.active_service = service.upper()

        self.server = server
        self.active_server = server
        self.user = user.upper()
        self.active_user = user.upper()

        #try to get info from connection string
        if conn_string!='' and parse_conn_string:
            try:
                parsed = urlparse(conn_string)
                if parsed.port!=None:
                    self.active_port = str(parsed.port)
                if parsed.hostname!=None:
                    self.active_server = parsed.hostname
                if parsed.username!=None:
                    self.active_user = parsed.username
                if parsed.password!=None and password == '':
                    self.password = parsed.password
                if parsed.query!=None:
                    self.conn_string_query = parsed.query
                parsed_database = parsed.path
                if len(parsed_database)>1:
                    self.active_service = parsed_database[1:]
            except Exception as exc:
                self.conn_string_error = 'Syntax error in the connection string.'

        if self.user.replace(' ', '') != self.user:
            self.schema = '"{0}"'.format(user)
        else:
            self.schema = self.user
        self.connection = Spartacus.Database.Oracle(self.active_server, self.active_port, self.active_service, self.active_user, self.password, conn_string, connection_params=self.connection_params)

        self.has_schema = True
        self.has_functions = True
        self.has_procedures = True
        self.has_packages = True
        self.has_sequences = True
        self.has_primary_keys = True
        self.has_foreign_keys = True
        self.has_uniques = True
        self.has_indexes = True
        self.has_checks = False
        self.has_excludes = False
        self.has_rules = False
        self.has_triggers = False
        self.has_partitions = False
        self.has_statistics = False

        self.has_update_rule = False
        self.can_rename_table = True
        self.rename_table_command = "alter table #p_table_name# rename to #p_new_table_name#"
        self.create_pk_command = "constraint #p_constraint_name# primary key (#p_columns#)"
        self.create_fk_command = "constraint #p_constraint_name# foreign key (#p_columns#) references #p_r_table_name# (#p_r_columns#) #p_delete_update_rules#"
        self.create_unique_command = "constraint #p_constraint_name# unique (#p_columns#)"
        self.can_alter_type = True
        self.alter_type_command = "alter table #p_table_name# modify #p_column_name# #p_new_data_type#"
        self.can_alter_nullable = True
        self.set_nullable_command = "alter table #p_table_name# modify #p_column_name# null"
        self.drop_nullable_command = "alter table #p_table_name# modify #p_column_name# not null"
        self.can_rename_column = True
        self.rename_column_command = "alter table #p_table_name# rename column #p_column_name# to #p_new_column_name#"
        self.can_add_column = True
        self.add_column_command = "alter table #p_table_name# add #p_column_name# #p_data_type# #p_nullable#"
        self.can_drop_column = True
        self.drop_column_command = "alter table #p_table_name# drop column #p_column_name#"
        self.can_add_constraint = True
        self.add_pk_command = "alter table #p_table_name# add constraint #p_constraint_name# primary key (#p_columns#)"
        self.add_fk_command = "alter table #p_table_name# add constraint #p_constraint_name# foreign key (#p_columns#) references #p_r_table_name# (#p_r_columns#) #p_delete_update_rules#"
        self.add_unique_command = "alter table #p_table_name# add constraint #p_constraint_name# unique (#p_columns#)"
        self.can_drop_constraint = True
        self.drop_pk_command = "alter table #p_table_name# drop constraint #p_constraint_name#"
        self.drop_fk_command = "alter table #p_table_name# drop constraint #p_constraint_name#"
        self.drop_unique_command = "alter table #p_table_name# drop constraint #p_constraint_name#"
        self.create_index_command = "create index #p_index_name# on #p_table_name# (#p_columns#)";
        self.create_unique_index_command = "create unique index #p_index_name# on #p_table_name# (#p_columns#)"
        self.drop_index_command = "drop index #p_schema_name#.#p_index_name#"

        self.console_help = "Console tab. Type the commands in the editor below this box. \? to view command list."
        self.use_server_cursor = False

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

    def GetName(self):
        return self.service

    @lock_required
    def GetVersion(self):
        return self.connection.ExecuteScalar('''
            select (case when product like '%Express%'
                         then 'Oracle XE '
                         else 'Oracle '
                    end) || version
            from product_component_version
            where product like 'Oracle%'
        ''')

    def GetUserName(self):
        return self.user

    @lock_required
    def GetUserSuper(self):
        try:
            sessions = self.connection.Query('select * from v$session where rownum <= 1')
            return True
        except Exception as exc:
            return False

    @lock_required
    def GetExpress(self):
        express = self.connection.Query("select * from product_component_version where product like '%Express%'")
        return (len(express.Rows) > 0)

    def PrintDatabaseInfo(self):
        return self.user + '@' + self.service

    def PrintDatabaseDetails(self):
        return self.server + ':' + self.port

    def HandleUpdateDeleteRules(self, update_rule, delete_rule):
        rules = ''
        if delete_rule.strip() != '':
            rules += ' on delete ' + delete_rule + ' '
        return rules

    def TestConnection(self):
        return_data = ''
        if self.conn_string and self.conn_string_error!='':
            return self.conn_string_error

        try:
            self.connection.Open()
            self.connection.Close()
            return_data = 'Connection successful.'
        except Exception as exc:
            return_data = str(exc)
        return return_data

    def GetErrorPosition(self, error_message, sql_cmd):
        return None

    @lock_required
    def Query(self, sql, alltypesstr=False, simple=False):
        return self.connection.Query(sql, alltypesstr, simple)

    @lock_required
    def ExecuteScalar(self, sql):
        return self.connection.ExecuteScalar(sql)

    @lock_required
    def Terminate(self, pid):
        return self.connection.Terminate(pid)

    @lock_required
    def QueryRoles(self):
        return self.connection.Query('''
            select (case when upper(replace(username, ' ', '')) <> username then '"' || username || '"' else username end) as "role_name"
            from all_users
            order by username
        ''', True)

    @lock_required
    def QueryTablespaces(self):
        return self.connection.Query('''
            select (case when upper(replace(tablespace_name, ' ', '')) <> tablespace_name then '"' || tablespace_name || '"' else tablespace_name end) as "tablespace_name"
            from dba_tablespaces
            order by tablespace_name
        ''', True)

    @lock_required
    def QueryTables(self, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if schema:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(self.schema)
        return self.connection.Query('''
            select (case when upper(replace(table_name, ' ', '')) <> table_name then '"' || table_name || '"' else table_name end) as "table_name",
                   (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) as "table_schema"
            from all_tables
            where 1 = 1
            {0}
            order by owner,
                     table_name
        '''.format(query_filter), True)

    @lock_required
    def QueryTablesFields(self, table=None, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if table and schema:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' and (case when upper(replace(table_name, ' ', '')) <> table_name then '"' || table_name || '"' else table_name end) = '{1}' ".format(schema, table)
            elif table:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' and (case when upper(replace(table_name, ' ', '')) <> table_name then '"' || table_name || '"' else table_name end) = '{1}' ".format(self.schema, table)
            elif schema:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(self.schema)
        else:
            if table:
                query_filter = "and table_name = '{0}' ".format(table)
        return self.connection.Query('''
            select (case when upper(replace(table_name, ' ', '')) <> table_name then '"' || table_name || '"' else table_name end) as "table_name",
                   (case when upper(replace(column_name, ' ', '')) <> column_name then '"' || column_name || '"' else column_name end) as "column_name",
                   case when data_type = 'NUMBER' and data_scale = '0' then 'INTEGER' else data_type end as "data_type",
                   case nullable when 'Y' then 'YES' else 'NO' end as "nullable",
                   data_length as "data_length",
                   data_precision as "data_precision",
                   data_scale as "data_scale"
            from all_tab_columns
            where 1 = 1
            {0}
            order by table_name,
                     column_id
        '''.format(query_filter), True)

    @lock_required
    def QueryTablesForeignKeys(self, table=None, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if table and schema:
                query_filter = "and (case when upper(replace(constraint_info.owner, ' ', '')) <> constraint_info.owner then '"' || constraint_info.owner || '"' else constraint_info.owner end) = '{0}' and (case when upper(replace(detail_table.table_name, ' ', '')) <> detail_table.table_name then '"' || detail_table.table_name || '"' else detail_table.table_name end) = '{1}' ".format(schema, table)
            elif table:
                query_filter = "and (case when upper(replace(constraint_info.owner, ' ', '')) <> constraint_info.owner then '"' || constraint_info.owner || '"' else constraint_info.owner end) = '{0}' and (case when upper(replace(detail_table.table_name, ' ', '')) <> detail_table.table_name then '"' || detail_table.table_name || '"' else detail_table.table_name end) = '{1}' ".format(self.schema, table)
            elif schema:
                query_filter = "and (case when upper(replace(constraint_info.owner, ' ', '')) <> constraint_info.owner then '"' || constraint_info.owner || '"' else constraint_info.owner end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(constraint_info.owner, ' ', '')) <> constraint_info.owner then '"' || constraint_info.owner || '"' else constraint_info.owner end) = '{0}' ".format(self.schema)
        else:
            if table:
                query_filter = "and (case when upper(replace(detail_table.table_name, ' ', '')) <> detail_table.table_name then '"' || detail_table.table_name || '"' else detail_table.table_name end) = '{0}' ".format(table)
        return self.connection.Query('''
            select (case when upper(replace(constraint_info.constraint_name, ' ', '')) <> constraint_info.constraint_name then '"' || constraint_info.constraint_name || '"' else constraint_info.constraint_name end) as "constraint_name",
                   (case when upper(replace(detail_table.table_name, ' ', '')) <> detail_table.table_name then '"' || detail_table.table_name || '"' else detail_table.table_name end) as "table_name",
                   (case when upper(replace(constraint_info.r_constraint_name, ' ', '')) <> constraint_info.r_constraint_name then '"' || constraint_info.r_constraint_name || '"' else constraint_info.r_constraint_name end) as "r_constraint_name",
                   (case when upper(replace(master_table.table_name, ' ', '')) <> master_table.table_name then '"' || master_table.table_name || '"' else master_table.table_name end) as "r_table_name",
                   (case when upper(replace(detail_table.owner, ' ', '')) <> detail_table.owner then '"' || detail_table.owner || '"' else detail_table.owner end) as "table_schema",
                   (case when upper(replace(master_table.owner, ' ', '')) <> master_table.owner then '"' || master_table.owner || '"' else master_table.owner end) as "r_table_schema",
                   constraint_info.delete_rule as "delete_rule",
                   'NO ACTION' as "update_rule"
            from user_constraints constraint_info,
                 user_cons_columns detail_table,
                 user_cons_columns master_table
            where constraint_info.constraint_name = detail_table.constraint_name
              and constraint_info.r_constraint_name = master_table.constraint_name
              and detail_table.position = master_table.position
              and constraint_info.constraint_type = 'R'
            {0}
            order by constraint_info.constraint_name,
                     detail_table.table_name
        '''.format(query_filter), True)

    @lock_required
    def QueryTablesForeignKeysColumns(self, fkey, table=None, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if table and schema:
                query_filter = "and (case when upper(replace(constraint_info.owner, ' ', '')) <> constraint_info.owner then '"' || constraint_info.owner || '"' else constraint_info.owner end) = '{0}' and (case when upper(replace(detail_table.table_name, ' ', '')) <> detail_table.table_name then '"' || detail_table.table_name || '"' else detail_table.table_name end) = '{1}' ".format(schema, table)
            elif table:
                query_filter = "and (case when upper(replace(constraint_info.owner, ' ', '')) <> constraint_info.owner then '"' || constraint_info.owner || '"' else constraint_info.owner end) = '{0}' and (case when upper(replace(detail_table.table_name, ' ', '')) <> detail_table.table_name then '"' || detail_table.table_name || '"' else detail_table.table_name end) = '{1}' ".format(self.schema, table)
            elif schema:
                query_filter = "and (case when upper(replace(constraint_info.owner, ' ', '')) <> constraint_info.owner then '"' || constraint_info.owner || '"' else constraint_info.owner end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(constraint_info.owner, ' ', '')) <> constraint_info.owner then '"' || constraint_info.owner || '"' else constraint_info.owner end) = '{0}' ".format(self.schema)
        else:
            if table:
                query_filter = "and (case when upper(replace(detail_table.table_name, ' ', '')) <> detail_table.table_name then '"' || detail_table.table_name || '"' else detail_table.table_name end) = '{0}' ".format(table)

        if type(fkey) == list:
            fkeys = fkey
        else:
            fkeys = [fkey]

        fkey_list = ', '.join(list(f'\'{str(e)}\'' for e in fkeys))

        if fkey_list:
            query_filter = query_filter + "and (case when upper(replace(constraint_info.constraint_name, ' ', '')) <> constraint_info.constraint_name then '"' || constraint_info.constraint_name || '"' else constraint_info.constraint_name end) in ({0}) ".format(fkey_list)
        return self.connection.Query('''
            select (case when upper(replace(constraint_info.constraint_name, ' ', '')) <> constraint_info.constraint_name then '"' || constraint_info.constraint_name || '"' else constraint_info.constraint_name end) as "constraint_name",
                   (case when upper(replace(detail_table.table_name, ' ', '')) <> detail_table.table_name then '"' || detail_table.table_name || '"' else detail_table.table_name end) as "table_name",
                   (case when upper(replace(detail_table.column_name, ' ', '')) <> detail_table.column_name then '"' || detail_table.column_name || '"' else detail_table.column_name end) as "column_name",
                   (case when upper(replace(constraint_info.r_constraint_name, ' ', '')) <> constraint_info.r_constraint_name then '"' || constraint_info.r_constraint_name || '"' else constraint_info.r_constraint_name end) as "r_constraint_name",
                   (case when upper(replace(master_table.table_name, ' ', '')) <> master_table.table_name then '"' || master_table.table_name || '"' else master_table.table_name end) as "r_table_name",
                   (case when upper(replace(master_table.column_name, ' ', '')) <> master_table.column_name then '"' || master_table.column_name || '"' else master_table.column_name end) as "r_column_name",
                   (case when upper(replace(detail_table.owner, ' ', '')) <> detail_table.owner then '"' || detail_table.owner || '"' else detail_table.owner end) as "table_schema",
                   (case when upper(replace(master_table.owner, ' ', '')) <> master_table.owner then '"' || master_table.owner || '"' else master_table.owner end) as "r_table_schema",
                   constraint_info.delete_rule as "delete_rule",
                   'NO ACTION' as "update_rule",
                   detail_table.position as "ordinal_position"
            from user_constraints constraint_info,
                 user_cons_columns detail_table,
                 user_cons_columns master_table
            where constraint_info.constraint_name = detail_table.constraint_name
              and constraint_info.r_constraint_name = master_table.constraint_name
              and detail_table.position = master_table.position
              and constraint_info.constraint_type = 'R'
            {0}
            order by constraint_info.constraint_name,
                     detail_table.table_name,
                     detail_table.position
        '''.format(query_filter), True)

    @lock_required
    def QueryTablesPrimaryKeys(self, table=None, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if table and schema:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' and (case when upper(replace(\"table_name\", ' ', '')) <> \"table_name\" then '"' || \"table_name\" || '"' else \"table_name\" end) = '{1}' ".format(schema, table)
            elif table:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' and (case when upper(replace(\"table_name\", ' ', '')) <> \"table_name\" then '"' || \"table_name\" || '"' else \"table_name\" end) = '{1}' ".format(self.schema, table)
            elif schema:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' ".format(self.schema)
        else:
            if table:
                query_filter = "and (case when upper(replace(\"table_name\", ' ', '')) <> \"table_name\" then '"' || \"table_name\" || '"' else \"table_name\" end) = '{0}' ".format(table)
        return self.connection.Query('''
            select distinct *
            from (
                select (case when upper(replace(cons.constraint_name, ' ', '')) <> cons.constraint_name then '"' || cons.constraint_name || '"' else cons.constraint_name end) as "constraint_name",
                       (case when upper(replace(cols.table_name, ' ', '')) <> cols.table_name then '"' || cols.table_name || '"' else cols.table_name end) as "table_name",
                       (case when upper(replace(cons.owner, ' ', '')) <> cons.owner then '"' || cons.owner || '"' else cons.owner end) as "table_schema"
                from all_constraints cons,
                     all_cons_columns cols,
                     all_tables t
                where cons.constraint_type = 'P'
                  and t.table_name = cols.table_name
                  and cons.constraint_name = cols.constraint_name
                  and cons.owner = cols.owner
                order by cons.owner,
                         cols.table_name,
                         cons.constraint_name
            )
            where 1 = 1
            {0}
        '''.format(query_filter), True)

    @lock_required
    def QueryTablesPrimaryKeysColumns(self, pkey, table=None, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if table and schema:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' and (case when upper(replace(\"table_name\", ' ', '')) <> \"table_name\" then '"' || \"table_name\" || '"' else \"table_name\" end) = '{1}' ".format(schema, table)
            elif table:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' and (case when upper(replace(\"table_name\", ' ', '')) <> \"table_name\" then '"' || \"table_name\" || '"' else \"table_name\" end) = '{1}' ".format(self.schema, table)
            elif schema:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' ".format(self.schema)
        else:
            if table:
                query_filter = "and (case when upper(replace(\"table_name\", ' ', '')) <> \"table_name\" then '"' || \"table_name\" || '"' else \"table_name\" end) = '{0}' ".format(table)
        query_filter = query_filter + "and (case when upper(replace(\"constraint_name\", ' ', '')) <> \"constraint_name\" then '"' || \"constraint_name\" || '"' else \"constraint_name\" end) = '{0}' ".format(pkey)
        return self.connection.Query('''
            select "column_name"
            from (
                select (case when upper(replace(cons.constraint_name, ' ', '')) <> cons.constraint_name then '"' || cons.constraint_name || '"' else cons.constraint_name end) as "constraint_name",
                       (case when upper(replace(cols.table_name, ' ', '')) <> cols.table_name then '"' || cols.table_name || '"' else cols.table_name end) as "table_name",
                       (case when upper(replace(cols.column_name, ' ', '')) <> cols.column_name then '"' || cols.column_name || '"' else cols.column_name end) as "column_name",
                       (case when upper(replace(cons.owner, ' ', '')) <> cons.owner then '"' || cons.owner || '"' else cons.owner end) as "table_schema"
                from all_constraints cons,
                     all_cons_columns cols,
                     all_tables t
                where cons.constraint_type = 'P'
                  and t.table_name = cols.table_name
                  and cons.constraint_name = cols.constraint_name
                  and cons.owner = cols.owner
                order by cons.owner,
                         cols.table_name,
                         cons.constraint_name,
                         cols.position
            )
            where 1 = 1
            {0}
        '''.format(query_filter), True)

    @lock_required
    def QueryTablesUniques(self, table=None, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if table and schema:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' and (case when upper(replace(\"table_name\", ' ', '')) <> \"table_name\" then '"' || \"table_name\" || '"' else \"table_name\" end) = '{1}' ".format(schema, table)
            elif table:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' and (case when upper(replace(\"table_name\", ' ', '')) <> \"table_name\" then '"' || \"table_name\" || '"' else \"table_name\" end) = '{1}' ".format(self.schema, table)
            elif schema:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' ".format(self.schema)
        else:
            if table:
                query_filter = "and (case when upper(replace(\"table_name\", ' ', '')) <> \"table_name\" then '"' || \"table_name\" || '"' else \"table_name\" end) = '{0}' ".format(table)
        return self.connection.Query('''
            select distinct *
            from (
                select (case when upper(replace(cons.constraint_name, ' ', '')) <> cons.constraint_name then '"' || cons.constraint_name || '"' else cons.constraint_name end) as "constraint_name",
                       (case when upper(replace(cols.table_name, ' ', '')) <> cols.table_name then '"' || cols.table_name || '"' else cols.table_name end) as "table_name",
                       (case when upper(replace(cons.owner, ' ', '')) <> cons.owner then '"' || cons.owner || '"' else cons.owner end) as "table_schema"
                from all_constraints cons,
                     all_cons_columns cols,
                     all_tables t
                where cons.constraint_type = 'U'
                  and t.table_name = cols.table_name
                  and cons.constraint_name = cols.constraint_name
                  and cons.owner = cols.owner
                order by cons.owner,
                         cols.table_name,
                         cons.constraint_name
            )
            where 1 = 1
            {0}
        '''.format(query_filter), True)

    @lock_required
    def QueryTablesUniquesColumns(self, unique_name, table=None, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if table and schema:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' and (case when upper(replace(\"table_name\", ' ', '')) <> \"table_name\" then '"' || \"table_name\" || '"' else \"table_name\" end) = '{1}' ".format(schema, table)
            elif table:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' and (case when upper(replace(\"table_name\", ' ', '')) <> \"table_name\" then '"' || \"table_name\" || '"' else \"table_name\" end) = '{1}' ".format(self.schema, table)
            elif schema:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(\"table_schema\", ' ', '')) <> \"table_schema\" then '"' || \"table_schema\" || '"' else \"table_schema\" end) = '{0}' ".format(self.schema)
        else:
            if table:
                query_filter = "and (case when upper(replace(\"table_name\", ' ', '')) <> \"table_name\" then '"' || \"table_name\" || '"' else \"table_name\" end) = '{0}' ".format(table)
        query_filter = query_filter + "and (case when upper(replace(\"constraint_name\", ' ', '')) <> \"constraint_name\" then '"' || \"constraint_name\" || '"' else \"constraint_name\" end) = '{0}' ".format(unique_name)
        return self.connection.Query('''
            select "column_name"
            from (
                select (case when upper(replace(cons.constraint_name, ' ', '')) <> cons.constraint_name then '"' || cons.constraint_name || '"' else cons.constraint_name end) as "constraint_name",
                       (case when upper(replace(cols.table_name, ' ', '')) <> cols.table_name then '"' || cols.table_name || '"' else cols.table_name end) as "table_name",
                       (case when upper(replace(cols.column_name, ' ', '')) <> cols.column_name then '"' || cols.column_name || '"' else cols.column_name end) as "column_name",
                       (case when upper(replace(cons.owner, ' ', '')) <> cons.owner then '"' || cons.owner || '"' else cons.owner end) as "table_schema"
                from all_constraints cons,
                     all_cons_columns cols,
                     all_tables t
                where cons.constraint_type = 'U'
                  and t.table_name = cols.table_name
                  and cons.constraint_name = cols.constraint_name
                  and cons.owner = cols.owner
                order by cons.owner,
                         cols.table_name,
                         cons.constraint_name,
                         cols.position
            )
            where 1 = 1
            {0}
        '''.format(query_filter), True)

    @lock_required
    def QueryTablesIndexes(self, table=None, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if table and schema:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' and (case when upper(replace(table_name, ' ', '')) <> table_name then '"' || table_name || '"' else table_name end) = '{1}' ".format(schema, table)
            elif table:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' and (case when upper(replace(table_name, ' ', '')) <> table_name then '"' || table_name || '"' else table_name end) = '{1}' ".format(self.schema, table)
            elif schema:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(self.schema)
        else:
            if table:
                query_filter = "and (case when upper(replace(table_name, ' ', '')) <> table_name then '"' || table_name || '"' else table_name end) = '{0}' ".format(table)
        return self.connection.Query('''
            select (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) as "schema_name",
                   (case when upper(replace(table_name, ' ', '')) <> table_name then '"' || table_name || '"' else table_name end) as "table_name",
                   (case when upper(replace(index_name, ' ', '')) <> index_name then '"' || index_name || '"' else index_name end) as "index_name",
                   case when uniqueness = 'UNIQUE' then 'Unique' else 'Non Unique' end as "uniqueness"
            from all_indexes
            where 1=1
            {0}
            order by owner,
                     table_name,
                     index_name
        '''.format(query_filter), True)

    @lock_required
    def QueryTablesIndexesColumns(self, index_name, table=None, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if table and schema:
                query_filter = "and (case when upper(replace(t.owner, ' ', '')) <> t.owner then '"' || t.owner || '"' else t.owner end) = '{0}' and (case when upper(replace(t.table_name, ' ', '')) <> t.table_name then '"' || t.table_name || '"' else t.table_name end) = '{1}' ".format(schema, table)
            elif table:
                query_filter = "and (case when upper(replace(t.owner, ' ', '')) <> t.owner then '"' || t.owner || '"' else t.owner end) = '{0}' and (case when upper(replace(t.table_name, ' ', '')) <> t.table_name then '"' || t.table_name || '"' else t.table_name end) = '{1}' ".format(self.schema, table)
            elif schema:
                query_filter = "and (case when upper(replace(t.owner, ' ', '')) <> t.owner then '"' || t.owner || '"' else t.owner end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(t.owner, ' ', '')) <> t.owner then '"' || t.owner || '"' else t.owner end) = '{0}' ".format(self.schema)
        else:
            if table:
                query_filter = "and (case when upper(replace(t.table_name, ' ', '')) <> t.table_name then '"' || t.table_name || '"' else t.table_name end) = '{0}' ".format(table)
        query_filter = query_filter + "and (case when upper(replace(t.index_name, ' ', '')) <> t.index_name then '"' || t.index_name || '"' else t.index_name end) = '{0}' ".format(index_name)
        return self.connection.Query('''
            select (case when upper(replace(c.column_name, ' ', '')) <> c.column_name then '"' || c.column_name || '"' else c.column_name end) as "column_name"
            from all_indexes t,
                 all_ind_columns c
            where t.table_name = c.table_name
              and t.index_name = c.index_name
              and t.owner = c.index_owner
            {0}
            order by c.column_position
        '''.format(query_filter), True)

    @lock_required
    def QueryDataLimited(self, query, count=-1):
        if count != -1:
            try:
                self.connection.Open()
                data = self.connection.QueryBlock('select * from ( {0} ) t where rownum <= {1}'.format(query, count), count, True, True)
                self.connection.Close()
                return data
            except Spartacus.Database.Exception as exc:
                try:
                    self.connection.Cancel()
                except:
                    pass
                raise exc
        else:
            return self.connection.Query(query, True)

    @lock_required
    def QueryTableRecords(self, column_list, table, schema, query_filter, count=-1):
        limit = ''
        if count != -1:
            limit = ' where rownum <= ' + count
        return self.connection.Query('''
            select *
            from (
            select {0}
            from {1} t
            {2}
            )
            {3}
        '''.format(
                column_list,
                table,
                query_filter,
                limit
            ), True
        )

    @lock_required
    def QueryPackages(self, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if schema:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(self.schema)
        return self.connection.Query('''
            select (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) as "schema_name",
                   (case when upper(replace(object_name, ' ', '')) <> object_name then '"' || object_name || '"' else object_name end) as "id",
                   (case when upper(replace(object_name, ' ', '')) <> object_name then '"' || object_name || '"' else object_name end) as "name"
            from all_packages
            where 1 = 1
            {0}
            order by 2
        '''.format(query_filter), True)


    @lock_required
    def QueryFunctions(self, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if schema:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(self.schema)
        return self.connection.Query('''
            select (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) as "schema_name",
                   (case when upper(replace(object_name, ' ', '')) <> object_name then '"' || object_name || '"' else object_name end) as "id",
                   (case when upper(replace(object_name, ' ', '')) <> object_name then '"' || object_name || '"' else object_name end) as "name"
            from all_procedures
            where object_type = 'FUNCTION'
            {0}
            order by 2
        '''.format(query_filter), True)

    @lock_required
    def QueryFunctionFields(self, function_name, schema):
        if schema:
            schema_name = schema
        else:
            schema_name = self.schema
        return self.connection.Query('''
            select (case in_out
                      when 'IN' then 'I'
                      when 'OUT' then 'O'
                      else 'R'
                    end) as "type",
                   (case when position = 0
                         then 'return ' || data_type
                         else argument_name || ' ' || data_type
                    end) as "name",
                   position+1 as "seq"
            from all_arguments
            where (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}'
              and (case when upper(replace(object_name, ' ', '')) <> object_name then '"' || object_name || '"' else object_name end) = '{1}'
            order by 3
        '''.format(schema_name, function_name), True)

    @lock_required
    def GetFunctionDefinition(self, function_name):
        body = '-- DROP FUNCTION {0};\n'.format(function_name)
        body = body + self.connection.ExecuteScalar("select dbms_lob.substr(dbms_metadata.get_ddl('FUNCTION', '{0}'), 4000, 1) from dual".format(function_name))
        return body

    @lock_required
    def QueryProcedures(self, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if schema:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(self.schema)
        return self.connection.Query('''
            select (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) as "schema_name",
                   (case when upper(replace(object_name, ' ', '')) <> object_name then '"' || object_name || '"' else object_name end) as "id",
                   (case when upper(replace(object_name, ' ', '')) <> object_name then '"' || object_name || '"' else object_name end) as "name"
            from all_procedures
            where object_type = 'PROCEDURE'
            {0}
            order by 2
        '''.format(query_filter), True)

    @lock_required
    def QueryProcedureFields(self, procedure, schema):
        if schema:
            schema_name = schema
        else:
            schema_name = self.schema
        return self.connection.Query('''
            select (case in_out
                      when 'IN' then 'I'
                      when 'OUT' then 'O'
                      else 'R'
                    end) as "type",
                   argument_name || ' ' || data_type as "name",
                   position+1 as "seq"
            from all_arguments
            where (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}'
              and (case when upper(replace(object_name, ' ', '')) <> object_name then '"' || object_name || '"' else object_name end) = '{1}'
            order by 3
        '''.format(schema_name, procedure), True)

    @lock_required
    def GetProcedureDefinition(self, procedure):
        body = '-- DROP PROCEDURE {0};\n'.format(procedure)
        body = body + self.connection.ExecuteScalar("select dbms_lob.substr(dbms_metadata.get_ddl('PROCEDURE', '{0}'), 4000, 1) from dual".format(procedure))
        return body

    @lock_required
    def QuerySequences(self, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if schema:
                query_filter = "and (case when upper(replace(sequence_owner, ' ', '')) <> sequence_owner then '"' || sequence_owner || '"' else sequence_owner end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(sequence_owner, ' ', '')) <> sequence_owner then '"' || sequence_owner || '"' else sequence_owner end) = '{0}' ".format(self.schema)
        table = self.connection.Query('''
            select (case when upper(replace(sequence_owner, ' ', '')) <> sequence_owner then '"' || sequence_owner || '"' else sequence_owner end) as "sequence_schema",
                   (case when upper(replace(sequence_name, ' ', '')) <> sequence_name then '"' || sequence_name || '"' else sequence_name end) as "sequence_name"
            from all_sequences
            where 1 = 1
            {0}
            order by sequence_owner,
                     sequence_name
        '''.format(query_filter), True)
        return table

    @lock_required
    def QueryViews(self, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if schema:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(self.schema)
        return self.connection.Query('''
            select (case when upper(replace(view_name, ' ', '')) <> view_name then '"' || view_name || '"' else view_name end) as "table_name",
                   (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) as "table_schema"
            from all_views
            where 1 = 1
            {0}
            order by 2, 1
        '''.format(query_filter), True)

    @lock_required
    def QueryViewFields(self, table=None, all_schemas=False, schema=None):
        query_filter = ''
        if not all_schemas:
            if table and schema:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' and (case when upper(replace(table_name, ' ', '')) <> table_name then '"' || table_name || '"' else table_name end) = '{1}' ".format(schema, table)
            elif table:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' and (case when upper(replace(table_name, ' ', '')) <> table_name then '"' || table_name || '"' else table_name end) = '{1}' ".format(self.schema, table)
            elif schema:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(schema)
            else:
                query_filter = "and (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}' ".format(self.schema)
        else:
            if table:
                query_filter = "and (case when upper(replace(table_name, ' ', '')) <> table_name then '"' || table_name || '"' else table_name end) = '{0}' ".format(table)
        return self.connection.Query('''
            select (case when upper(replace(table_name, ' ', '')) <> table_name then '"' || table_name || '"' else table_name end) as "table_name",
                   (case when upper(replace(column_name, ' ', '')) <> column_name then '"' || column_name || '"' else column_name end) as "column_name",
                   case when data_type = 'NUMBER' and data_scale = '0' then 'INTEGER' else data_type end as "data_type",
                   case nullable when 'Y' then 'YES' else 'NO' end as "nullable",
                   data_length as "data_length",
                   data_precision as "data_precision",
                   data_scale as "data_scale"
            from all_tab_columns
            where 1 = 1
            {0}
            order by table_name, column_id
        '''.format(query_filter), True)

    @lock_required
    def GetViewDefinition(self, view, schema):
        if schema:
            schema_name = schema
        else:
            schema_name = self.schema
        return '''CREATE OR REPLACE VIEW {0}.{1} AS
{2}
'''.format(schema, view,
        self.connection.ExecuteScalar('''
                select text
                from all_views
                where (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}'
                  and (case when upper(replace(view_name, ' ', '')) <> view_name then '"' || view_name || '"' else view_name end) = '{1}'
            '''.format(schema_name, view)
    ))

    def TemplateCreateRole(self):
        template = get_template("oracle", "create_role")
        return template.template

    def TemplateAlterRole(self):
        template = get_template("oracle", "alter_role")
        return template.template

    def TemplateDropRole(self):
        template = get_template("oracle", "drop_role")
        return template.template

    def TemplateCreateTablespace(self):
        template = get_template("oracle", "create_tablespace")
        return template.template

    def TemplateAlterTablespace(self):
        template = get_template("oracle", "alter_tablespace")
        return template.template

    def TemplateDropTablespace(self):
        template = get_template("oracle", "drop_tablespace")
        return template.template

    def TemplateCreateFunction(self):
        template = get_template("oracle", "create_function")
        return template.template

    def TemplateDropFunction(self):
        template = get_template("oracle", "drop_function")
        return template.template

    def TemplateCreateProcedure(self):
        template = get_template("oracle", "create_procedure")
        return template.template

    def TemplateDropProcedure(self):
        template = get_template("oracle", "drop_procedure")
        return template.template

    def TemplateCreateTable(self):
        template = get_template("oracle", "create_table")
        return template.template

    def TemplateAlterTable(self):
        template = get_template("oracle", "alter_table")
        return template.template

    def TemplateDropTable(self):
        template = get_template("oracle", "drop_table")
        return template.template

    def TemplateCreateColumn(self):
        template = get_template("oracle", "create_column")
        return template.template

    def TemplateAlterColumn(self):
        template = get_template("oracle", "alter_column")
        return template.template

    def TemplateDropColumn(self):
        template = get_template("oracle", "drop_column")
        return template.template

    def TemplateCreatePrimaryKey(self):
        template = get_template("oracle", "create_primarykey")
        return template.template

    def TemplateDropPrimaryKey(self):
        template = get_template("oracle", "drop_primarykey")
        return template.template

    def TemplateCreateUnique(self):
        template = get_template("oracle", "create_unique")
        return template.template

    def TemplateDropUnique(self):
        template = get_template("oracle", "drop_unique")
        return template.template

    def TemplateCreateForeignKey(self):
        template = get_template("oracle", "create_foreignkey")
        return template.template

    def TemplateDropForeignKey(self):
        template = get_template("oracle", "drop_foreignkey")
        return template.template

    def TemplateCreateIndex(self):
        template = get_template("oracle", "create_index")
        return template.template

    def TemplateAlterIndex(self):
        template = get_template("oracle", "alter_index")
        return template.template

    def TemplateDropIndex(self):
        template = get_template("oracle", "drop_index")
        return template.template

    def TemplateCreateSequence(self):
        template = get_template("oracle", "create_sequence")
        return template.template

    def TemplateAlterSequence(self):
        template = get_template("oracle", "alter_sequence")
        return template.template

    def TemplateDropSequence(self):
        template = get_template("oracle", "drop_sequence")
        return template.template

    def TemplateCreateView(self):
        template = get_template("oracle", "create_view")
        return template.template

    def TemplateDropView(self):
        template = get_template("oracle", "drop_view")
        return template.template

    def TemplateSelect(self, schema, table):
        sql = 'SELECT t.'
        fields = self.QueryTablesFields(table, False, schema)
        if len(fields.Rows) > 0:
            sql += '\n     , t.'.join([r['column_name'] for r in fields.Rows])
        sql += '\nFROM {0}.{1} t'.format(schema, table)
        pk = self.QueryTablesPrimaryKeys(table, False, schema)
        if len(pk.Rows) > 0:
            fields = self.QueryTablesPrimaryKeysColumns(pk.Rows[0]['constraint_name'], table, False, schema)
            if len(fields.Rows) > 0:
                sql += '\nORDER BY t.'
                sql += '\n       , t.'.join([r['column_name'] for r in fields.Rows])
        return Template(sql)

    def TemplateInsert(self, schema, table):
        fields = self.QueryTablesFields(table, False, schema)
        if len(fields.Rows) > 0:
            sql = 'INSERT INTO {0}.{1} (\n'.format(schema, table)
            pk = self.QueryTablesPrimaryKeys(table, False, schema)
            if len(pk.Rows) > 0:
                table_pk_fields = self.QueryTablesPrimaryKeysColumns(pk.Rows[0]['constraint_name'], table, False, schema)
                pk_fields = [r['column_name'] for r in table_pk_fields.Rows]
                values = []
                first = True
                for r in fields.Rows:
                    if first:
                        sql += '      {0}'.format(r['column_name'])
                        if r['column_name'] in pk_fields:
                            values.append('      ? -- {0} {1} PRIMARY KEY'.format(r['column_name'], r['data_type']))
                        elif r['nullable'] == 'YES':
                            values.append('      ? -- {0} {1} NULLABLE'.format(r['column_name'], r['data_type']))
                        else:
                            values.append('      ? -- {0} {1}'.format(r['column_name'], r['data_type']))
                        first = False
                    else:
                        sql += '\n    , {0}'.format(r['column_name'])
                        if r['column_name'] in pk_fields:
                            values.append('\n    , ? -- {0} {1} PRIMARY KEY'.format(r['column_name'], r['data_type']))
                        elif r['nullable'] == 'YES':
                            values.append('\n    , ? -- {0} {1} NULLABLE'.format(r['column_name'], r['data_type']))
                        else:
                            values.append('\n    , ? -- {0} {1}'.format(r['column_name'], r['data_type']))
            else:
                values = []
                first = True
                for r in fields.Rows:
                    if first:
                        sql += '      {0}'.format(r['column_name'])
                        if r['nullable'] == 'YES':
                            values.append('      ? -- {0} {1} NULLABLE'.format(r['column_name'], r['data_type']))
                        else:
                            values.append('      ? -- {0} {1}'.format(r['column_name'], r['data_type']))
                        first = False
                    else:
                        sql += '\n    , {0}'.format(r['column_name'])
                        if r['nullable'] == 'YES':
                            values.append('\n    , ? -- {0} {1} NULLABLE'.format(r['column_name'], r['data_type']))
                        else:
                            values.append('\n    , ? -- {0} {1}'.format(r['column_name'], r['data_type']))
            sql += '\n) VALUES (\n'
            for v in values:
                sql += v
            sql += '\n)'
        else:
            sql = ''
        return Template(sql)

    def TemplateUpdate(self, schema, table):
        fields = self.QueryTablesFields(table, False, schema)
        if len(fields.Rows) > 0:
            sql = 'UPDATE {0}.{1}\nSET '.format(schema, table)
            pk = self.QueryTablesPrimaryKeys(table, False, schema)
            if len(pk.Rows) > 0:
                table_pk_fields = self.QueryTablesPrimaryKeysColumns(pk.Rows[0]['constraint_name'], table, False, schema)
                pk_fields = [r['column_name'] for r in table_pk_fields.Rows]
                values = []
                first = True
                for r in fields.Rows:
                    if first:
                        if r['column_name'] in pk_fields:
                            sql += '{0} = ? -- {1} PRIMARY KEY'.format(r['column_name'], r['data_type'])
                        elif r['nullable'] == 'YES':
                            sql += '{0} = ? -- {1} NULLABLE'.format(r['column_name'], r['data_type'])
                        else:
                            sql += '{0} = ? -- {1}'.format(r['column_name'], r['data_type'])
                        first = False
                    else:
                        if r['column_name'] in pk_fields:
                            sql += '\n    , {0} = ? -- {1} PRIMARY KEY'.format(r['column_name'], r['data_type'])
                        elif r['nullable'] == 'YES':
                            sql += '\n    , {0} = ? -- {1} NULLABLE'.format(r['column_name'], r['data_type'])
                        else:
                            sql += '\n    , {0} = ? -- {1}'.format(r['column_name'], r['data_type'])
            else:
                values = []
                first = True
                for r in fields.Rows:
                    if first:
                        if r['nullable'] == 'YES':
                            sql += '{0} = ? -- {1} NULLABLE'.format(r['column_name'], r['data_type'])
                        else:
                            sql += '{0} = ? -- {1}'.format(r['column_name'], r['data_type'])
                        first = False
                    else:
                        if r['nullable'] == 'YES':
                            sql += '\n    , {0} = ? -- {1} NULLABLE'.format(r['column_name'], r['data_type'])
                        else:
                            sql += '\n    , {0} = ? -- {1}'.format(r['column_name'], r['data_type'])
            sql += '\nWHERE condition'
        else:
            sql = ''
        return Template(sql)

    def TemplateDelete(self):
        template = get_template("oracle", "delete")
        return template.template

    @lock_required
    def GetProperties(self, schema, object_name, object_type):
        if object_type == 'role':
            table1 = self.connection.Query('''
                select username as "User",
                       user_id as "ID",
                       account_status as "Status",
                       lock_date as "Lock Date",
                       expiry_date as "Expiry Date",
                       default_tablespace as "Default Tablespace",
                       temporary_tablespace as "Temporary Tablespace",
                       created as "Creation Date",
                       initial_rsrc_consumer_group as "Group",
                       authentication_type as "Authentication Type"
                from dba_users
                where (case when upper(replace(username, ' ', '')) <> username then '"' || username || '"' else username end) = '{0}'
            '''.format(object_name), True, True).Transpose('Property', 'Value')
        elif object_type == 'tablespace':
            table1 = self.connection.Query('''
                select tablespace_name as "Tablespace",
                       block_size as "Block Size",
                       initial_extent as "Initial Extent",
                       next_extent as "Next Extent",
                       min_extents as "Min Extents",
                       max_extents as "Max Extents",
                       max_size as "Max Size",
                       pct_increase as "Percent Increase",
                       min_extlen as "Min Extent Length",
                       status as "Status",
                       contents as "Contents",
                       logging as "Logging",
                       force_logging as "Force Logging",
                       extent_management as "Extent Management",
                       allocation_type as "Allocation Type",
                       plugged_in as "Plugged In",
                       segment_space_management as "Segment Space Management",
                       def_tab_compression as "Deferrable Compression",
                       retention as "Retention",
                       bigfile as "Big File",
                       predicate_evaluation as "Predicate Evaluation",
                       encrypted as "Encrypted",
                       compress_for as "Compression Format"
                from dba_tablespaces
                where (case when upper(replace(tablespace_name, ' ', '')) <> tablespace_name then '"' || tablespace_name || '"' else tablespace_name end) = '{0}'
            '''.format(object_name), True, True).Transpose('Property', 'Value')
        else:
            table1 = self.connection.Query('''
                select owner as "Owner",
                       object_name as "Object Name",
                       object_id as "Object ID",
                       object_type as "Object Type",
                       created as "Created",
                       last_ddl_time as "Last DDL Time",
                       timestamp as "Timestamp",
                       status as "Status",
                       temporary as "Temporary",
                       generated as "Generated",
                       secondary as "Secondary"
                from all_objects
                where (case when upper(replace(owner, ' ', '')) <> owner then '"' || owner || '"' else owner end) = '{0}'
                  and (case when upper(replace(object_name, ' ', '')) <> object_name then '"' || object_name || '"' else object_name end) = '{1}'
                  and subobject_name is null
            '''.format(self.schema, object_name), True, True).Transpose('Property', 'Value')
            if object_type == 'sequence':
                table2 = self.connection.Query('''
                    select last_number as "Last Value",
                           min_value as "Min Value",
                           max_value as "Max Value",
                           increment_by as "Increment By",
                           cycle_flag as "Is Cached",
                           order_flag as "Is Ordered",
                           cache_size as "Cache Size"
                    from all_sequences
                    where (case when upper(replace(sequence_owner, ' ', '')) <> sequence_owner then '"' || sequence_owner || '"' else sequence_owner end) = '{0}'
                      and (case when upper(replace(sequence_name, ' ', '')) <> sequence_name then '"' || sequence_name || '"' else sequence_name end) = '{1}'
                '''.format(self.schema, object_name), True, True).Transpose('Property', 'Value')
                table1.Merge(table2)
        return table1

    @lock_required
    def GetDDL(self, schema, table, object_name, object_type):
        if object_type == 'role' or object_type == 'tablespace' or object_type == 'database':
            return ' '
        else:
            return self.connection.ExecuteScalar(
                    '''
select dbms_lob.substr(dbms_metadata.get_ddl(object_type, object_name), 4000, 1) as ddl
from (
select * from all_objects
                where (SHARING is NULL OR SHARING <> 'METADATA LINK') and
                 (case when upper(replace(object_name, ' ', '')) <> object_name then '"' || object_name || '"' else object_name end) = '{0}')'''.format(object_name)
                )

    def GetAutocompleteValues(self, p_columns, p_filter):
        return None
