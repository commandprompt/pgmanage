from string import Template

TEMPLATES = {
    "create_database": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/create-database-transact-sql?view=sql-server-ver$major_version&tabs=sqlpool
CREATE DATABASE database_name
-- [ CONTAINMENT = { NONE | PARTIAL } ]
-- [ ON
--       [ PRIMARY ] <filespec> [ ,...n ]
--       [ , <filegroup> [ ,...n ] ]
--       [ LOG ON <filespec> [ ,...n ] ]
-- ]
-- [ COLLATE collation_name ]
-- [ WITH <option> [,...n ] ]
-- [;]
"""
    ),
    "alter_database": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/alter-database-transact-sql?view=sql-server-ver$major_version&tabs=sqlpool
ALTER DATABASE #database_name#
-- {
--     MODIFY NAME = new_database_name
--   | COLLATE collation_name
--   | <file_and_filegroup_options>
--   | SET <option_spec> [ ,...n ] [ WITH <termination> ]
-- }
-- [;]
"""
    ),
    "drop_database": Template("DROP DATABASE #database_name#"),
    "create_function": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/create-function-transact-sql?view=sql-server-ver$major_version
CREATE [ OR ALTER ] FUNCTION [ schema_name. ] function_name
-- ( [ { @parameter_name [ AS ] [ type_schema_name. ] parameter_data_type [ NULL ]
--  [ = default ] [ READONLY ] }
--     [ , ...n ]
--   ]
-- )
-- RETURNS return_data_type
--     [ WITH <function_option> [ , ...n ] ]
--     [ AS ]
--     BEGIN
--         function_body
--         RETURN scalar_expression
--     END
-- [;]
"""
    ),
    "alter_function": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/alter-function-transact-sql?view=sql-server-ver$major_version
ALTER FUNCTION #schema_name#.#function_name#
-- ( [ { @parameter_name [ AS ][ type_schema_name. ] parameter_data_type   
--     [ = default ] }   
--     [ ,...n ]  
--   ]  
-- )  
-- RETURNS return_data_type  
--     [ WITH <function_option> [ ,...n ] ]  
--     [ AS ]  
--     BEGIN   
--         function_body   
--         RETURN scalar_expression  
--     END  
-- [ ; ]
"""
    ),
    "drop_function": Template("DROP FUNCTION #schema_name#.#function_name#"),
    "create_login": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/create-login-transact-sql?view=sql-server-ver$major_version
CREATE LOGIN login_name
-- { WITH <option_list1> | FROM <sources> }
"""
    ),
    "alter_login": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/alter-login-transact-sql?view=sql-server-ver$major_version
ALTER LOGIN #login_name#
--     {
--     <status_option>
--     | WITH <set_option> [ , ... ]
--     | <cryptographic_credential_option>
--     }
-- [;]
"""
    ),
    "drop_login": Template("DROP LOGIN #login_name#"),
    "create_procedure": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/create-procedure-transact-sql?view=sql-server-ver$major_version
CREATE [ OR ALTER ] { PROC | PROCEDURE }
    [schema_name.] procedure_name [ ; number ]
--  [ { @parameter_name [ type_schema_name. ] data_type }
--         [ VARYING ] [ NULL ] [ = default ] [ OUT | OUTPUT | [READONLY]
--     ] [ ,...n ]
-- [ WITH <procedure_option> [ ,...n ] ]
-- [ FOR REPLICATION ]
-- AS { [ BEGIN ] sql_statement [;] [ ...n ] [ END ] }
-- [;]
"""
    ),
    "alter_procedure": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/alter-procedure-transact-sql?view=sql-server-ver$major_version
ALTER { PROC | PROCEDURE } #schema_name#.#procedure_name# [ ; number ]   
    [ { @parameter_name [ type_schema_name. ] data_type }   
--         [ VARYING ] [ = default ] [ OUT | OUTPUT ] [READONLY]  
--     ] [ ,...n ]   
-- [ WITH <procedure_option> [ ,...n ] ]  
-- [ FOR REPLICATION ]   
-- AS { [ BEGIN ] sql_statement [;] [ ...n ] [ END ] }  
-- [;]   
"""
    ),
    "drop_procedure": Template("DROP PROCEDURE #schema_name#.#procedure_name#"),
    "create_database_role": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/create-role-transact-sql?view=sql-server-ver$major_version
CREATE ROLE role_name 
-- [ AUTHORIZATION owner_name ]  
"""
    ),
    "alter_database_role": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/alter-role-transact-sql?view=sql-server-ver$major_version
ALTER ROLE  #role_name#
{  
       ADD MEMBER database_principal  
    |  DROP MEMBER database_principal  
    |  WITH NAME = new_name  
}  
[;]  
"""
    ),
    "drop_database_role": Template("DROP ROLE #role_name#"),
    "create_server_role": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/create-server-role-transact-sql?view=sql-server-ver$major_version
CREATE SERVER ROLE role_name [ AUTHORIZATION server_principal ]"""
    ),
    "alter_server_role": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/alter-server-role-transact-sql?view=sql-server-ver$major_version
ALTER SERVER ROLE #role_name#
-- {  
--     [ ADD MEMBER server_principal ]  
--   | [ DROP MEMBER server_principal ]  
--   | [ WITH NAME = new_server_role_name ]  
-- } [ ; ]    
"""
    ),
    "drop_server_role": Template("DROP SERVER ROLE #role_name#"),
    "create_schema": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/create-schema-transact-sql?view=sql-server-ver$major_version
CREATE SCHEMA schema_name_clause [ <schema_element> [ ...n ] ]
"""
    ),
    "drop_schema": Template("DROP SCHEMA #schema_name#"),
    "create_statistics": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/create-statistics-transact-sql?view=sql-server-ver$major_version
CREATE STATISTICS statistics_name
ON { table_or_indexed_view_name } ( column [ , ...n ] )
--    [ WITH FULLSCAN ] ;
"""
    ),
    "drop_statistics": Template("DROP STATISTICS #table_name#.#statistic_name#"),
    "drop_table": Template("DROP TABLE #schema_name#.#table_name#"),
    "create_trigger": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/create-trigger-transact-sql?view=sql-server-ver$major_version
CREATE [ OR ALTER ] TRIGGER [ schema_name . ] trigger_name
ON { table | view }
-- [ WITH <dml_trigger_option> [ , ...n ] ]
-- { FOR | AFTER | INSTEAD OF }
-- { [ INSERT ] [ , ] [ UPDATE ] [ , ] [ DELETE ] }
-- [ WITH APPEND ]
-- [ NOT FOR REPLICATION ]
-- AS { sql_statement  [ ; ] [ , ...n ] | EXTERNAL NAME <method_specifier [ ; ] > }
"""
    ),
    "drop_trigger": Template("DROP TRIGGER #schema_name#.#table_name#"),
    "create_user": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/create-user-transact-sql?view=sql-server-ver$major_version
CREATE USER user_name   
--     [   
--         { FOR | FROM } LOGIN login_name   
--     ]  
--     [ WITH <limited_options_list> [ ,... ] ]   
-- [ ; ]  
"""
    ),
    "alter_user": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/alter-user-transact-sql?view=sql-server-ver$major_version
ALTER USER #user_name#
 WITH <set_item> [ ,...n ]
[;]

-- <set_item> ::=
-- NAME = new_user_name
-- | DEFAULT_SCHEMA = { schema_name | NULL }
-- | LOGIN = login_name
-- | PASSWORD = 'password' [ OLD_PASSWORD = 'oldpassword' ]
-- | DEFAULT_LANGUAGE = { NONE | <lcid> | <language name> | <language alias> }
-- | ALLOW_ENCRYPTED_VALUE_MODIFICATIONS = [ ON | OFF ]
"""
    ),
    "drop_user": Template("DROP USER #user_name#"),
    "create_view": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/create-view-transact-sql?view=sql-server-ver$major_version
CREATE [ OR ALTER ] VIEW [ schema_name . ] view_name [ (column [ ,...n ] ) ]
-- [ WITH <view_attribute> [ ,...n ] ]
AS select_statement
-- [ WITH CHECK OPTION ]
[ ; ]  

"""
    ),
    "alter_view": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/alter-view-transact-sql?view=sql-server-ver$major_version
ALTER VIEW #schema_name#.#view_name# [ ( column [ ,...n ] ) ]   
-- [ WITH <view_attribute> [ ,...n ] ]   
AS select_statement   
-- [ WITH CHECK OPTION ] [ ; ]  
"""
    ),
    "drop_view": Template("DROP VIEW #schema_name#.#view_name#"),
    "create_type": Template(
        """-- https://learn.microsoft.com/sql/t-sql/statements/create-type-transact-sql?view=sql-server-ver$major_version
CREATE TYPE [ schema_name. ] type_name
-- {
--       FROM base_type
--       [ ( precision [ , scale ] ) ]
--       [ NULL | NOT NULL ]
--     | EXTERNAL NAME assembly_name [ .class_name ]
--     | AS TABLE ( { <column_definition> | <computed_column_definition> [ , ...n ]
--       [ <table_constraint> ] [ , ...n ]
--       [ <table_index> ] [ , ...n ] } )
-- } [ ; ]
"""
    ),
    "drop_type": Template(
        """
    DROP TYPE #schema_name#.#type_name#
"""
    ),
}


def get_template(key, version=None) -> Template:
    tpl = TEMPLATES.get(key)
    if tpl is None:
        raise KeyError(f"No such template: {key}")
    return tpl
