from string import Template

TEMPLATES = {
    "drop_role": Template("DROP ROLE #role_name#"),
    "create_database": {
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-createdatabase.html
CREATE DATABASE name
--OWNER user_name
--TEMPLATE template
--ENCODING encoding
--STRATEGY strategy
--LOCALE locale
--LC_COLLATE lc_collate
--LC_CTYPE lc_ctype
--ICU_LOCALE icu_locale
--LOCALE_PROVIDER locale_provider
--COLLATION_VERSION collation_version
--TABLESPACE tablespace
--ALLOW_CONNECTIONS allowconn
--CONNECTION LIMIT connlimit
--IS_TEMPLATE istemplate
--OID oid
"""
        ),
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-createdatabase.html
CREATE DATABASE name
--OWNER user_name
--TEMPLATE template
--ENCODING encoding
--LC_COLLATE lc_collate
--LC_CTYPE lc_ctype
--TABLESPACE tablespace
--ALLOW_CONNECTIONS allowconn
--CONNECTION LIMIT connlimit
--IS_TEMPLATE istemplate
"""
        ),
        "13-14": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-createdatabase.html
CREATE DATABASE name
--OWNER user_name
--TEMPLATE template
--ENCODING encoding
--LOCALE locale
--LC_COLLATE lc_collate
--LC_CTYPE lc_ctype
--TABLESPACE tablespace
--ALLOW_CONNECTIONS allowconn
--CONNECTION LIMIT connlimit
--IS_TEMPLATE istemplate
"""
        ),
    },
    "alter_database": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-alterdatabase.html
ALTER DATABASE #database_name#
--ALLOW_CONNECTIONS allowconn
--CONNECTION LIMIT connlimit
--IS_TEMPLATE istemplate
--RENAME TO new_name
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--SET TABLESPACE new_tablespace
--SET configuration_parameter TO { value | DEFAULT }
--SET configuration_parameter FROM CURRENT
--RESET configuration_parameter
--RESET ALL
"""
    ),
    "drop_database": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-dropdatabase.html
DROP DATABASE #database_name#"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-dropdatabase.html
DROP DATABASE #database_name#
--WITH ( FORCE )
"""
        ),
    },
    "create_tablespace": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createtablespace.html
CREATE TABLESPACE name
LOCATION 'directory'
--OWNER new_owner | CURRENT_USER | SESSION_USER
--WITH ( tablespace_option = value [, ... ] )
"""
    ),
    "alter_tablespace": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertablespace.html
ALTER TABLESPACE #tablespace_name#
--RENAME TO new_name
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--SET seq_page_cost = value
--RESET seq_page_cost
--SET random_page_cost = value
--RESET random_page_cost
--SET effective_io_concurrency = value
--RESET effective_io_concurrency
"""
    ),
    "drop_tablespace": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-droptablespace.html
DROP TABLESPACE #tablespace_name#"""
    ),
    "create_schema": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createschema.html
CREATE SCHEMA schema_name
--AUTHORIZATION [ GROUP ] user_name | CURRENT_USER | SESSION_USER
"""
    ),
    "alter_schema": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-alterschema.html
ALTER SCHEMA #schema_name#
--RENAME TO new_name
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
"""
    ),
    "drop_schema": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropschema.html
DROP SCHEMA #schema_name#
--CASCADE
"""
    ),
    "create_sequence": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createsequence.html
CREATE SEQUENCE #schema_name#.name
--INCREMENT BY increment
--MINVALUE minvalue | NO MINVALUE
--MAXVALUE maxvalue | NO MAXVALUE
--START WITH start
--CACHE cache
--CYCLE
--OWNED BY { table_name.column_name | NONE }
"""
    ),
    "alter_sequence": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altersequence.html
ALTER SEQUENCE #sequence_name#
--INCREMENT BY increment
--MINVALUE minvalue | NO MINVALUE
--MAXVALUE maxvalue | NO MAXVALUE
--START WITH start
--RESTART
--RESTART WITH restart
--CACHE cache
--CYCLE
--NO CYCLE
--OWNED BY { table_name.column_name | NONE }
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--RENAME TO new_name
--SET SCHEMA new_schema
"""
    ),
    "drop_sequence": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropsequence.html
DROP SEQUENCE #sequence_name#
--CASCADE
"""
    ),
    "create_function": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createfunction.html
CREATE OR REPLACE FUNCTION #schema_name#.name
--(
--    [ argmode ] [ argname ] argtype [ { DEFAULT | = } default_expr ]
--)
--RETURNS rettype
--RETURNS TABLE ( column_name column_type )
LANGUAGE plpgsql
--IMMUTABLE | STABLE | VOLATILE
--STRICT
--SECURITY DEFINER
--COST execution_cost
--ROWS result_rows
AS
$function$
--DECLARE
-- variables
BEGIN
-- definition
END;
$function$
"""
    ),
    "alter_function": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterfunction.html
ALTER FUNCTION #function_name#
--CALLED ON NULL INPUT
--RETURNS NULL ON NULL INPUT
--STRICT
--IMMUTABLE
--STABLE
--VOLATILE
--NOT LEAKPROOF
--LEAKPROOF
--EXTERNAL SECURITY INVOKER
--SECURITY INVOKER
--EXTERNAL SECURITY DEFINER
--SECURITY DEFINER
--PARALLEL { UNSAFE | RESTRICTED | SAFE }
--COST execution_cost
--ROWS result_rows
--SUPPORT support_function
--SET configuration_parameter { TO | = } { value | DEFAULT }
--SET configuration_parameter FROM CURRENT
--RESET configuration_parameter
--RESET ALL
--RENAME TO new_name
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--SET SCHEMA new_schema
--DEPENDS ON EXTENSION extension_name
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterfunction.html
ALTER FUNCTION #function_name#
--CALLED ON NULL INPUT
--RETURNS NULL ON NULL INPUT
--STRICT
--IMMUTABLE
--STABLE
--VOLATILE
--NOT LEAKPROOF
--LEAKPROOF
--EXTERNAL SECURITY INVOKER
--SECURITY INVOKER
--EXTERNAL SECURITY DEFINER
--SECURITY DEFINER
--PARALLEL { UNSAFE | RESTRICTED | SAFE }
--COST execution_cost
--ROWS result_rows
--SUPPORT support_function
--SET configuration_parameter { TO | = } { value | DEFAULT }
--SET configuration_parameter FROM CURRENT
--RESET configuration_parameter
--RESET ALL
--RENAME TO new_name
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--SET SCHEMA new_schema
--DEPENDS ON EXTENSION extension_name
--NO DEPENDS ON EXTENSION extension_name
"""
        ),
    },
    "drop_function": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropfunction.html
DROP FUNCTION #function_name#
--CASCADE
"""
    ),
    "create_procedure": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createprocedure.html
CREATE OR REPLACE PROCEDURE #schema_name#.name
--(
--    [ argmode ] [ argname ] argtype [ { DEFAULT | = } default_expr ]
--)
LANGUAGE plpgsql
--SECURITY DEFINER
AS
$procedure$
--DECLARE
-- variables
BEGIN
-- definition
END;
$procedure$
"""
    ),
    "alter_procedure": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-alterprocedure.html
ALTER PROCEDURE #procedure_name#
--EXTERNAL SECURITY INVOKER
--SECURITY INVOKER
--EXTERNAL SECURITY DEFINER
--SECURITY DEFINER
--SET configuration_parameter { TO | = } { value | DEFAULT }
--SET configuration_parameter FROM CURRENT
--RESET configuration_parameter
--RESET ALL
--RENAME TO new_name
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--SET SCHEMA new_schema
--DEPENDS ON EXTENSION extension_name
"""
    ),
    "drop_procedure": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropprocedure.html
DROP PROCEDURE #procedure_name#
--CASCADE
"""
    ),
    "create_triggerfunction": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createfunction.html
CREATE OR REPLACE FUNCTION #schema_name#.name()
RETURNS trigger
LANGUAGE plpgsql
--IMMUTABLE | STABLE | VOLATILE
--COST execution_cost
AS
$function$
--DECLARE
-- variables
BEGIN
-- definition
END;
$function$
"""
    ),
    "alter_triggerfunction": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterfunction.html
ALTER FUNCTION #function_name#
--CALLED ON NULL INPUT
--RETURNS NULL ON NULL INPUT
--STRICT
--IMMUTABLE
--STABLE
--VOLATILE
--NOT LEAKPROOF
--LEAKPROOF
--EXTERNAL SECURITY INVOKER
--SECURITY INVOKER
--EXTERNAL SECURITY DEFINER
--SECURITY DEFINER
--PARALLEL { UNSAFE | RESTRICTED | SAFE }
--COST execution_cost
--ROWS result_rows
--SUPPORT support_function
--SET configuration_parameter { TO | = } { value | DEFAULT }
--SET configuration_parameter FROM CURRENT
--RESET configuration_parameter
--RESET ALL
--RENAME TO new_name
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--SET SCHEMA new_schema
--DEPENDS ON EXTENSION extension_name
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterfunction.html
ALTER FUNCTION #function_name#
--CALLED ON NULL INPUT
--RETURNS NULL ON NULL INPUT
--STRICT
--IMMUTABLE
--STABLE
--VOLATILE
--NOT LEAKPROOF
--LEAKPROOF
--EXTERNAL SECURITY INVOKER
--SECURITY INVOKER
--EXTERNAL SECURITY DEFINER
--SECURITY DEFINER
--PARALLEL { UNSAFE | RESTRICTED | SAFE }
--COST execution_cost
--ROWS result_rows
--SUPPORT support_function
--SET configuration_parameter { TO | = } { value | DEFAULT }
--SET configuration_parameter FROM CURRENT
--RESET configuration_parameter
--RESET ALL
--RENAME TO new_name
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--SET SCHEMA new_schema
--DEPENDS ON EXTENSION extension_name
--NO DEPENDS ON EXTENSION extension_name
"""
        ),
    },
    "drop_triggerfunction": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropfunction.html
DROP FUNCTION #function_name#
--CASCADE
"""
    ),
    "create_eventtriggerfunction": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createfunction.html
CREATE OR REPLACE FUNCTION #schema_name#.name()
RETURNS event_trigger
LANGUAGE plpgsql
--IMMUTABLE | STABLE | VOLATILE
--COST execution_cost
AS
$function$
--DECLARE
-- variables
BEGIN
-- definition
END;
$function$
"""
    ),
    "alter_eventtriggerfunction": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterfunction.html
ALTER FUNCTION #function_name#
--CALLED ON NULL INPUT
--RETURNS NULL ON NULL INPUT
--STRICT
--IMMUTABLE
--STABLE
--VOLATILE
--NOT LEAKPROOF
--LEAKPROOF
--EXTERNAL SECURITY INVOKER
--SECURITY INVOKER
--EXTERNAL SECURITY DEFINER
--SECURITY DEFINER
--PARALLEL { UNSAFE | RESTRICTED | SAFE }
--COST execution_cost
--ROWS result_rows
--SUPPORT support_function
--SET configuration_parameter { TO | = } { value | DEFAULT }
--SET configuration_parameter FROM CURRENT
--RESET configuration_parameter
--RESET ALL
--RENAME TO new_name
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--SET SCHEMA new_schema
--DEPENDS ON EXTENSION extension_name
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterfunction.html
ALTER FUNCTION #function_name#
--CALLED ON NULL INPUT
--RETURNS NULL ON NULL INPUT
--STRICT
--IMMUTABLE
--STABLE
--VOLATILE
--NOT LEAKPROOF
--LEAKPROOF
--EXTERNAL SECURITY INVOKER
--SECURITY INVOKER
--EXTERNAL SECURITY DEFINER
--SECURITY DEFINER
--PARALLEL { UNSAFE | RESTRICTED | SAFE }
--COST execution_cost
--ROWS result_rows
--SUPPORT support_function
--SET configuration_parameter { TO | = } { value | DEFAULT }
--SET configuration_parameter FROM CURRENT
--RESET configuration_parameter
--RESET ALL
--RENAME TO new_name
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--SET SCHEMA new_schema
--DEPENDS ON EXTENSION extension_name
--NO DEPENDS ON EXTENSION extension_name
"""
        ),
    },
    "drop_eventtriggerfunction": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropfunction.html
DROP FUNCTION #function_name#
--CASCADE
"""
    ),
    "create_aggregate": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createaggregate.html
CREATE AGGREGATE #schema_name#.name
--([ argmode ] [ argname ] arg_data_type [ , ... ])
--ORDER BY [ argmode ] [ argname ] arg_data_type [ , ... ] )
(
SFUNC = sfunc,
STYPE = state_data_type
--    , SSPACE = state_data_size
--    , FINALFUNC = ffunc
--    , FINALFUNC_EXTRA
--    , FINALFUNC_MODIFY = { READ_ONLY | SHAREABLE | READ_WRITE }
--    , COMBINEFUNC = combinefunc
--    , SERIALFUNC = serialfunc
--    , DESERIALFUNC = deserialfunc
--    , INITCOND = initial_condition
--    , MSFUNC = msfunc
--    , MINVFUNC = minvfunc
--    , MSTYPE = mstate_data_type
--    , MSSPACE = mstate_data_size
--    , MFINALFUNC = mffunc
--    , MFINALFUNC_EXTRA
--    , MFINALFUNC_MODIFY = { READ_ONLY | SHAREABLE | READ_WRITE }
--    , MINITCOND = minitial_condition
--    , SORTOP = sort_operator
--    , PARALLEL = { SAFE | RESTRICTED | UNSAFE }
)
"""
    ),
    "alter_aggregate": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-alteraggregate.html
ALTER AGGREGATE #aggregate_name#
--RENAME TO new_name
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--SET SCHEMA new_schema
"""
    ),
    "drop_aggregate": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropaggregate.html
DROP AGGREGATE #aggregate_name#
--RESTRICT
--CASCADE
"""
    ),
    "create_view": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createview.html
CREATE [ OR REPLACE ] [ TEMP | TEMPORARY ] [ RECURSIVE ] VIEW #schema_name#.name
--WITH ( check_option = local | cascaded )
--WITH ( security_barrier = true | false )
AS
SELECT ...
"""
    ),
    "alter_view": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterview.html
ALTER VIEW #view_name#
--ALTER COLUMN column_name SET DEFAULT expression
--ALTER COLUMN column_name DROP DEFAULT
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--RENAME TO new_name
--SET SCHEMA new_schema
--SET ( check_option = value )
--SET ( security_barrier = { true | false } )
--RESET ( check_option )
--RESET ( security_barrier )

--ALTER TABLE #view_name# RENAME COLUMN column_name TO new_column_name
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterview.html
ALTER VIEW #view_name#
--ALTER COLUMN column_name SET DEFAULT expression
--ALTER COLUMN column_name DROP DEFAULT
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--RENAME COLUMN column_name TO new_column_name
--RENAME TO new_name
--SET SCHEMA new_schema
--SET ( check_option = value )
--SET ( security_barrier = { true | false } )
--RESET ( check_option )
--RESET ( security_barrier )
"""
        ),
    },
    "drop_view": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropview.html
DROP VIEW #view_name#
--CASCADE
"""
    ),
    "create_mview": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-creatematerializedview.html
CREATE MATERIALIZED VIEW #schema_name#.name AS
SELECT ...
--WITH NO DATA
"""
    ),
    "refresh_mview": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-refreshmaterializedview.html
REFRESH MATERIALIZED VIEW
--CONCURRENTLY
#view_name#
--WITH NO DATA
"""
    ),
    "alter_mview": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altermaterializedview.html
ALTER MATERIALIZED VIEW #view_name#
--ALTER COLUMN column_name SET STATISTICS integer
--ALTER COLUMN column_name SET ( attribute_option = value )
--ALTER COLUMN column_name RESET ( attribute_option )
--ALTER COLUMN column_name SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN }
--CLUSTER ON index_name
--SET WITHOUT CLUSTER
--SET ( storage_parameter = value )
--RESET ( storage_parameter )
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--DEPENDS ON EXTENSION extension_name
--RENAME COLUMN column_name TO new_column_name
--RENAME TO new_name
--SET SCHEMA new_schema
--SET TABLESPACE new_tablespace [ NOWAIT ]
"""
    ),
    "drop_mview": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropmaterializedview.html
DROP MATERIALIZED VIEW #view_name#
--CASCADE
"""
    ),
    "drop_table": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-droptable.html
DROP TABLE #table_name#
--CASCADE
"""
    ),
    "create_column": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertable.html
ALTER TABLE #table_name#
ADD COLUMN name data_type
--COLLATE collation
--column_constraint [ ... ] ]
"""
    ),
    "alter_column": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertable.html
ALTER TABLE #table_name#
--ALTER COLUMN #column_name#
--RENAME COLUMN #column_name# TO new_column
--TYPE data_type [ COLLATE collation ] [ USING expression ]
--SET DEFAULT expression
--DROP DEFAULT
--SET NOT NULL
--DROP NOT NULL
--SET STATISTICS integer
--SET ( attribute_option = value [, ... ] )
--RESET ( attribute_option [, ... ] )
--SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN }
"""
    ),
    "drop_column": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertable.html
ALTER TABLE #table_name#
DROP COLUMN #column_name#
--CASCADE
"""
    ),
    "create_primarykey": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertable.html
ALTER TABLE #table_name#
ADD CONSTRAINT name
PRIMARY KEY ( column_name [, ... ] )
--WITH ( storage_parameter [= value] [, ... ] )
--WITH OIDS
--WITHOUT OIDS
--USING INDEX TABLESPACE tablespace_name
"""
    ),
    "drop_primarykey": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertable.html
ALTER TABLE #table_name#
DROP CONSTRAINT #constraint_name#
--CASCADE
"""
    ),
    "create_unique": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertable.html
ALTER TABLE #table_name#
ADD CONSTRAINT name
UNIQUE ( column_name [, ... ] )
--WITH ( storage_parameter [= value] [, ... ] )
--WITH OIDS
--WITHOUT OIDS
--USING INDEX TABLESPACE tablespace_name
"""
    ),
    "drop_unique": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertable.html
ALTER TABLE #table_name#
DROP CONSTRAINT #constraint_name#
--CASCADE
"""
    ),
    "create_foreignkey": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertable.html
ALTER TABLE #table_name#
ADD CONSTRAINT name
FOREIGN KEY ( column_name [, ... ] )
REFERENCES reftable [ ( refcolumn [, ... ] ) ]
--MATCH { FULL | PARTIAL | SIMPLE }
--ON DELETE { NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT }
--ON UPDATE { NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT }
--NOT VALID
"""
    ),
    "drop_foreignkey": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertable.html
ALTER TABLE #table_name#
DROP CONSTRAINT #constraint_name#
--CASCADE
"""
    ),
    "create_index": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-createindex.html
CREATE [ UNIQUE ] INDEX [ CONCURRENTLY ] name
ON [ ONLY ] #table_name#
--USING method
( { column_name | ( expression ) } [ COLLATE collation ] [ opclass ] [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] )
--INCLUDE ( column_name [, ...] )
--WITH ( storage_parameter = value [, ... ] )
--WHERE predicate
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-createindex.html
CREATE [ UNIQUE ] INDEX [ CONCURRENTLY ] name
ON [ ONLY ] #table_name#
--USING method
( { column_name | ( expression ) } [ COLLATE collation ] [ opclass [ ( opclass_parameter = value [, ... ] ) ] ] [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] )
--INCLUDE ( column_name [, ...] )
--WITH ( storage_parameter = value [, ... ] )
--WHERE predicate
"""
        ),
    },
    "alter_index": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterindex.html
ALTER INDEX #index_name#
--RENAME to new_name
--SET TABLESPACE tablespace_name
--ATTACH PARTITION index_name
--DEPENDS ON EXTENSION extension_name
--SET ( storage_parameter = value [, ... ] )
--RESET ( storage_parameter [, ... ] )
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterindex.html
ALTER INDEX #index_name#
--RENAME to new_name
--SET TABLESPACE tablespace_name
--ATTACH PARTITION index_name
--DEPENDS ON EXTENSION extension_name
--NO DEPENDS ON EXTENSION extension_name
--SET ( storage_parameter = value [, ... ] )
--RESET ( storage_parameter [, ... ] )
"""
        ),
    },
    "cluster_index": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-cluster.html
CLUSTER
--VERBOSE
#table_name#
USING #index_name#
"""
    ),
    "reindex": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-reindex.html
REINDEX
--( VERBOSE )
INDEX
--CONCURRENTLY
#index_name#
"""
    ),
    "drop_index": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropindex.html
DROP INDEX
--CONCURRENTLY
#index_name#
--CASCADE
"""
    ),
    "create_check": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertable.html
ALTER TABLE #table_name#
ADD CONSTRAINT name
CHECK ( expression )
"""
    ),
    "drop_check": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertable.html
ALTER TABLE #table_name#
DROP CONSTRAINT #constraint_name#
--CASCADE
"""
    ),
    "create_exclude": Template(
        """-- https://www.postgresql.org/docs/$major_version/ddl-constraints.html#DDL-CONSTRAINTS-EXCLUSION
ALTER TABLE #table_name#
ADD CONSTRAINT name
--USING index_method
EXCLUDE ( exclude_element WITH operator [, ... ] )
--index_parameters
--WHERE ( predicate )
"""
    ),
    "drop_exclude": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altertable.html
ALTER TABLE #table_name#
DROP CONSTRAINT #constraint_name#
--CASCADE
"""
    ),
    "create_rule": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createrule.html
CREATE RULE name
AS ON { SELECT | INSERT | UPDATE | DELETE }
TO #table_name#
--WHERE condition
--DO ALSO { NOTHING | command | ( command ; command ... ) }
--DO INSTEAD { NOTHING | command | ( command ; command ... ) }
"""
    ),
    "alter_rule": Template(
        "-- https://www.postgresql.org/docs/$major_version/sql-alterrule.html \nALTER RULE #rule_name# ON #table_name# RENAME TO new_name"
    ),
    "drop_rule": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-droprule.html \nDROP RULE #rule_name# ON #table_name#
--CASCADE
"""
    ),
    "create_trigger": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createtrigger.html
CREATE TRIGGER name
--BEFORE { INSERT [ OR ] | UPDATE [ OF column_name [, ... ] ] [ OR ] | DELETE [ OR ] | TRUNCATE }
--AFTER { INSERT [ OR ] | UPDATE [ OF column_name [, ... ] ] [ OR ] | DELETE [ OR ] | TRUNCATE }
ON #table_name#
--FROM referenced_table_name
--NOT DEFERRABLE | [ DEFERRABLE ] { INITIALLY IMMEDIATE | INITIALLY DEFERRED }
--FOR EACH ROW
--FOR EACH STATEMENT
--WHEN ( condition )
--EXECUTE PROCEDURE function_name ( arguments )
"""
    ),
    "create_view_trigger": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createtrigger.html
CREATE TRIGGER name
--BEFORE { INSERT [ OR ] | UPDATE [ OF column_name [, ... ] ] [ OR ] | DELETE }
--AFTER { INSERT [ OR ] | UPDATE [ OF column_name [, ... ] ] [ OR ] | DELETE }
--INSTEAD OF { INSERT [ OR ] | UPDATE [ OF column_name [, ... ] ] [ OR ] | DELETE }
ON #table_name#
--FROM referenced_table_name
--NOT DEFERRABLE | [ DEFERRABLE ] { INITIALLY IMMEDIATE | INITIALLY DEFERRED }
--FOR EACH ROW
--FOR EACH STATEMENT
--WHEN ( condition )
--EXECUTE PROCEDURE function_name ( arguments )
"""
    ),
    "alter_trigger": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-altertrigger.html
ALTER TRIGGER #trigger_name# ON #table_name#
--RENAME TO new_name
--DEPENDS ON EXTENSION extension_name
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-altertrigger.html
ALTER TRIGGER #trigger_name# ON #table_name#
--RENAME TO new_name
--DEPENDS ON EXTENSION extension_name
--NO DEPENDS ON EXTENSION extension_name
"""
        ),
    },
    "enable_trigger": Template(
        """ALTER TABLE #table_name# ENABLE
--REPLICA
--ALWAYS
TRIGGER #trigger_name#
"""
    ),
    "disable_trigger": Template(
        "ALTER TABLE #table_name# DISABLE TRIGGER #trigger_name#"
    ),
    "drop_trigger": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-droptrigger.html \nDROP TRIGGER #trigger_name# ON #table_name#
--CASCADE
"""
    ),
    "create_eventtrigger": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createeventtrigger.html
CREATE EVENT TRIGGER name
--ON ddl_command_start
--ON ddl_command_end
--ON table_rewrite
--ON sql_drop
--WHEN TAG IN ( filter_value [, ...] )
EXECUTE PROCEDURE function_name()
"""
    ),
    "alter_eventtrigger": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altereventtrigger.html
ALTER EVENT TRIGGER #trigger_name#
--OWNER TO new_owner
--OWNER TO CURRENT_USER
--OWNER TO SESSION_USER
--RENAME TO new_name
"""
    ),
    "enable_eventtrigger": Template(
        """ALTER EVENT TRIGGER #trigger_name# ENABLE
--REPLICA
--ALWAYS
"""
    ),
    "disable_eventtrigger": Template("ALTER EVENT TRIGGER #trigger_name# DISABLE"),
    "drop_eventtrigger": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropeventtrigger.html \nDROP EVENT TRIGGER #trigger_name#
--CASCADE
"""
    ),
    "create_inherited": Template(
        """CREATE TABLE name (
    CHECK ( condition )
) INHERITS (#table_name#)
"""
    ),
    "noinherit_partition": Template(
        "ALTER TABLE #partition_name# NO INHERIT #table_name#"
    ),
    "create_partition": Template(
        """CREATE TABLE name PARTITION OF #table_name#
--FOR VALUES
--IN ( { numeric_literal | string_literal | NULL } [, ...] )
--FROM ( { numeric_literal | string_literal | MINVALUE | MAXVALUE } [, ...] ) TO ( { numeric_literal | string_literal | MINVALUE | MAXVALUE } [, ...] )
--WITH ( MODULUS numeric_literal, REMAINDER numeric_literal )
--DEFAULT
--PARTITION BY { RANGE | LIST | HASH } ( { column_name | ( expression ) } [ COLLATE collation ] [ opclass ] [, ... ] ) ]
"""
    ),
    "detach_partition": Template(
        "ALTER TABLE #table_name# DETACH PARTITION #partition_name#"
    ),
    "drop_partition": Template("DROP TABLE #partition_name#"),
    "vacuum": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-vacuum.html
VACUUM
--FULL
--FREEZE
--ANALYZE
--DISABLE_PAGE_SKIPPING
--SKIP_LOCKED
--INDEX_CLEANUP
--TRUNCATE
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-vacuum.html
VACUUM
--FULL
--FREEZE
--ANALYZE
--DISABLE_PAGE_SKIPPING
--SKIP_LOCKED
--INDEX_CLEANUP
--TRUNCATE
--PARALLEL number_of_parallel_workers
"""
        ),
    },
    "vacuum_table": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-vacuum.html
VACUUM
--FULL
--FREEZE
--ANALYZE
--DISABLE_PAGE_SKIPPING
--SKIP_LOCKED
--INDEX_CLEANUP
--TRUNCATE
#table_name#
--(column_name, [, ...])
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-vacuum.html
VACUUM
--FULL
--FREEZE
--ANALYZE
--DISABLE_PAGE_SKIPPING
--SKIP_LOCKED
--INDEX_CLEANUP
--TRUNCATE
--PARALLEL number_of_parallel_workers
#table_name#
--(column_name, [, ...])
"""
        ),
    },
    "analyze": Template(
        "-- https://www.postgresql.org/docs/$major_version/sql-analyze.html \nANALYZE"
    ),
    "analyze_table": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-analyze.html
ANALYZE #table_name#
--(column_name, [, ...])
"""
    ),
    "delete": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-delete.html
DELETE FROM
--ONLY
#table_name#
WHERE condition
--WHERE CURRENT OF cursor_name
--RETURNING *
"""
    ),
    "truncate": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-truncate.html
TRUNCATE
--ONLY
#table_name#
--RESTART IDENTITY
--CASCADE
"""
    ),
    "create_physicalreplicationslot": Template(
        """-- https://www.postgresql.org/docs/$major_version/functions-admin.html#FUNCTIONS-REPLICATION-TABLE \nSELECT * FROM pg_create_physical_replication_slot('slot_name')"""
    ),
    "drop_physicalreplicationslot": Template(
        """-- https://www.postgresql.org/docs/$major_version/functions-admin.html#FUNCTIONS-REPLICATION-TABLE \nSELECT pg_drop_replication_slot('#slot_name#')"""
    ),
    "create_logicalreplicationslot": Template(
        """-- https://www.postgresql.org/docs/$major_version/functions-admin.html#FUNCTIONS-REPLICATION-TABLE \nSELECT * FROM pg_create_logical_replication_slot('slot_name', 'pgoutput')"""
    ),
    "drop_logicalreplicationslot": Template(
        """-- https://www.postgresql.org/docs/$major_version/functions-admin.html#FUNCTIONS-REPLICATION-TABLE \nSELECT pg_drop_replication_slot('#slot_name#')"""
    ),
    "create_publication": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-createpublication.html
CREATE PUBLICATION name
--FOR TABLE [ ONLY ] table_name [ * ] [, ...]
--FOR ALL TABLES
--WITH ( publish = 'insert, update, delete, truncate' )
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-createpublication.html
CREATE PUBLICATION name
--FOR TABLE [ ONLY ] table_name [ * ] [, ...]
--FOR ALL TABLES
--WITH ( publish = 'insert, update, delete, truncate' )
--WITH ( publish_via_partition_root = true | false )
"""
        ),
    },
    "alter_publication": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterpublication.html
ALTER PUBLICATION #pub_name#
--ADD TABLE [ ONLY ] table_name [ * ] [, ...]
--SET TABLE [ ONLY ] table_name [ * ] [, ...]
--DROP TABLE [ ONLY ] table_name [ * ] [, ...]
--SET ( publish = 'insert, update, delete, truncate' )
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--RENAME TO new_name
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterpublication.html
ALTER PUBLICATION #pub_name#
--ADD TABLE [ ONLY ] table_name [ * ] [, ...]
--SET TABLE [ ONLY ] table_name [ * ] [, ...]
--DROP TABLE [ ONLY ] table_name [ * ] [, ...]
--SET ( publish = 'insert, update, delete, truncate' )
--SET ( publish_via_partition_root = true | false )
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--RENAME TO new_name
"""
        ),
    },
    "drop_publication": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-droppublication.html \nDROP PUBLICATION #pub_name#
--CASCADE
"""
    ),
    "add_pubtable": Template(
        "-- https://www.postgresql.org/docs/$major_version/sql-alterpublication.html \nALTER PUBLICATION #pub_name# ADD TABLE table_name"
    ),
    "drop_pubtable": Template(
        "-- https://www.postgresql.org/docs/$major_version/sql-alterpublication.html \nALTER PUBLICATION #pub_name# DROP TABLE #table_name#"
    ),
    "create_subscription": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createsubscription.html
CREATE SUBSCRIPTION name
CONNECTION 'conninfo'
PUBLICATION pub_name [, ...]
--WITH (
--copy_data = { true | false }
--, create_slot = { true | false }
--, enabled = { true | false }
--, slot_name = 'name'
--, synchronous_commit = { on | remote_apply | remote_write | local | off }
--, connect = { true | false }
--)
"""
    ),
    "alter_subscription": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-altersubscription.html
ALTER SUBSCRIPTION #sub_name#
--CONNECTION 'conninfo'
--SET PUBLICATION pub_name [, ...] [ WITH ( refresh = { true | false } ) ]
--REFRESH PUBLICATION [ WITH ( copy_data = { true | false } ) ]
--ENABLE
--DISABLE
--SET (
--slot_name = 'name'
--, synchronous_commit = { on | remote_apply | remote_write | local | off }
--)
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--RENAME TO new_name
"""
    ),
    "drop_subscription": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropsubscription.html \nDROP SUBSCRIPTION #sub_name#
--CASCADE
"""
    ),
    "create_fdw": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createforeigndatawrapper.html
CREATE FOREIGN DATA WRAPPER name
--HANDLER handler_function
--NO HANDLER
--VALIDATOR validator_function
--NO VALIDATOR
--OPTIONS ( option 'value' [, ... ] )
"""
    ),
    "alter_fdw": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-alterforeigndatawrapper.html
ALTER FOREIGN DATA WRAPPER #fdwname#
--HANDLER handler_function
--NO HANDLER
--VALIDATOR validator_function
--NO VALIDATOR
--OPTIONS ( [ ADD ] option ['value'] [, ... ] )
--OPTIONS ( SET option ['value'] )
--OPTIONS ( DROP option )
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--RENAME TO new_name
"""
    ),
    "drop_fdw": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropforeigndatawrapper.html \nDROP FOREIGN DATA WRAPPER #fdwname#
--CASCADE
"""
    ),
    "create_foreign_server": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createserver.html
CREATE SERVER server_name
--TYPE 'server_type'
--VERSION 'server_version'
FOREIGN DATA WRAPPER #fdwname#
--OPTIONS ( option 'value' [, ... ] )
"""
    ),
    "alter_foreign_server": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-alterserver.html
ALTER SERVER #srvname#
--VERSION 'new_version'
--OPTIONS ( [ ADD ] option ['value'] [, ... ] )
--OPTIONS ( SET option ['value'] )
--OPTIONS ( DROP option )
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--RENAME TO new_name
"""
    ),
    "import_foreign_schema": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-importforeignschema.html
IMPORT FOREIGN SCHEMA remote_schema
--LIMIT TO ( table_name [, ...] )
--EXCEPT ( table_name [, ...] )
FROM SERVER #srvname#
INTO local_schema
--OPTIONS ( option 'value' [, ... ] )
"""
    ),
    "drop_foreign_server": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropserver.html \nDROP SERVER #srvname#
--CASCADE
"""
    ),
    "create_foreign_table": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createforeigntable.html
CREATE FOREIGN TABLE #schema_name#.table_name
--PARTITION OF parent_table
(
    column_name data_type
    --OPTIONS ( option 'value' [, ... ] )
    --COLLATE collation
    --CONSTRAINT constraint_name
    --NOT NULL
    --CHECK ( expression )
    --NO INHERIT
    --DEFAULT default_expr
    --GENERATED ALWAYS AS ( generation_expr ) STORED
)
--INHERITS ( parent_table [, ... ] )
SERVER server_name
--partition_bound_spec
--OPTIONS ( option 'value' [, ... ] )
"""
    ),
    "alter_foreign_table": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-alterforeigntable.html
ALTER FOREIGN TABLE #table_name#
--ADD COLUMN column_name data_type [ COLLATE collation ] [ column_constraint [ ... ] ]
--DROP COLUMN column_name [ CASCADE ]
--ALTER [ COLUMN column_name [ SET DATA ] TYPE data_type [ COLLATE collation ]
--ALTER COLUMN column_name SET DEFAULT expression
--ALTER COLUMN column_name DROP DEFAULT
--ALTER COLUMN column_name { SET | DROP } NOT NULL
--ALTER COLUMN column_name SET STATISTICS integer
--ALTER COLUMN column_name SET ( attribute_option = value [, ... ] )
--ALTER COLUMN column_name RESET ( attribute_option [, ... ] )
--ALTER COLUMN column_name SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN }
--ALTER COLUMN column_name OPTIONS ( [ ADD | SET | DROP ] option ['value'] [, ... ] )
--ADD table_constraint [ NOT VALID ]
--VALIDATE CONSTRAINT constraint_name
--DROP CONSTRAINT constraint_name [ CASCADE ]
--DISABLE TRIGGER [ trigger_name | ALL | USER ]
--ENABLE TRIGGER [ trigger_name | ALL | USER ]
--ENABLE REPLICA TRIGGER trigger_name
--ENABLE ALWAYS TRIGGER trigger_name
--SET WITH OIDS
--SET WITHOUT OIDS
--INHERIT parent_table
--NO INHERIT parent_table
--OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
--OPTIONS ( [ ADD | SET | DROP ] option ['value'] [, ... ] )
--RENAME COLUMN column_name TO new_column_name
--RENAME TO new_name
--SET SCHEMA new_schema
"""
    ),
    "drop_foreign_table": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropforeigntable.html \nDROP FOREIGN TABLE #table_name#
--CASCADE
"""
    ),
    "create_foreign_column": Template(
        """ALTER FOREIGN TABLE #table_name#
ADD COLUMN name data_type
--COLLATE collation
--column_constraint [ ... ] ]
"""
    ),
    "alter_foreign_column": Template(
        """ALTER FOREIGN TABLE #table_name#
--ALTER COLUMN #column_name#
--RENAME COLUMN #column_name# TO new_column
--TYPE data_type [ COLLATE collation ] [ USING expression ]
--SET DEFAULT expression
--DROP DEFAULT
--SET NOT NULL
--DROP NOT NULL
--SET STATISTICS integer
--SET ( attribute_option = value [, ... ] )
--RESET ( attribute_option [, ... ] )
--SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN }
--OPTIONS ( [ ADD | SET | DROP ] option ['value'] [, ... ] )
"""
    ),
    "drop_foreign_column": Template(
        """ALTER FOREIGN TABLE #table_name#
DROP COLUMN #column_name#
--CASCADE
"""
    ),
    "create_user_mapping": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createusermapping.html
CREATE USER MAPPING
--FOR user_name
--FOR CURRENT_USER
--FOR PUBLIC
SERVER #srvname#
--OPTIONS ( option 'value' [ , ... ] )
"""
    ),
    "alter_user_mapping": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-alterusermapping.html
ALTER USER MAPPING FOR #user_name#
SERVER #srvname#
--OPTIONS ( [ ADD ] option ['value'] [, ... ] )
--OPTIONS ( SET option ['value'] )
--OPTIONS ( DROP option )
"""
    ),
    "drop_user_mapping": Template(
        "-- https://www.postgresql.org/docs/$major_version/sql-dropusermapping.html \nDROP USER MAPPING FOR #user_name# SERVER #srvname#"
    ),
    "create_type": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createtype.html
CREATE TYPE #schema_name#.name

-- AS (
--    attribute_name data_type [ COLLATE collation ] [, ... ]

-- AS ENUM (
--    'label' [, ... ]

-- AS RANGE (
--    SUBTYPE = subtype
--    , SUBTYPE_OPCLASS = subtype_operator_class
--    , COLLATION = collation
--    , CANONICAL = canonical_function
--    , SUBTYPE_DIFF = subtype_diff_function

-- (
--    INPUT = input_function,
--    OUTPUT = output_function
--    , RECEIVE = receive_function
--    , SEND = send_function
--    , TYPMOD_IN = type_modifier_input_function
--    , TYPMOD_OUT = type_modifier_output_function
--    , ANALYZE = analyze_function
--    , INTERNALLENGTH = { internallength | VARIABLE }
--    , PASSEDBYVALUE
--    , ALIGNMENT = alignment
--    , STORAGE = storage
--    , LIKE = like_type
--    , CATEGORY = category
--    , PREFERRED = preferred
--    , DEFAULT = default
--    , ELEMENT = element
--    , DELIMITER = delimiter
--    , COLLATABLE = collatable

-- )
"""
    ),
    "alter_type": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-altertype.html
ALTER TYPE #type_name#
--ADD ATTRIBUTE attribute_name data_type [ COLLATE collation ] [ CASCADE | RESTRICT ]
--DROP ATTRIBUTE [ IF EXISTS ] attribute_name [ CASCADE | RESTRICT ]
--ALTER ATTRIBUTE attribute_name [ SET DATA ] TYPE data_type [ COLLATE collation ] [ CASCADE | RESTRICT ]
--RENAME ATTRIBUTE attribute_name TO new_attribute_name [ CASCADE | RESTRICT ]
--OWNER TO new_owner
--RENAME TO new_name
--SET SCHEMA new_schema
--ADD VALUE [ IF NOT EXISTS ] new_enum_value [ { BEFORE | AFTER } existing_enum_value ]
--RENAME VALUE existing_enum_value TO new_enum_value
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-altertype.html
ALTER TYPE #type_name#
--ADD ATTRIBUTE attribute_name data_type [ COLLATE collation ] [ CASCADE | RESTRICT ]
--DROP ATTRIBUTE [ IF EXISTS ] attribute_name [ CASCADE | RESTRICT ]
--ALTER ATTRIBUTE attribute_name [ SET DATA ] TYPE data_type [ COLLATE collation ] [ CASCADE | RESTRICT ]
--RENAME ATTRIBUTE attribute_name TO new_attribute_name [ CASCADE | RESTRICT ]
--OWNER TO new_owner
--RENAME TO new_name
--SET SCHEMA new_schema
--ADD VALUE [ IF NOT EXISTS ] new_enum_value [ { BEFORE | AFTER } existing_enum_value ]
--RENAME VALUE existing_enum_value TO new_enum_value
--SET ( RECEIVE = value )
--SET ( SEND = value )
--SET ( TYPMOD_IN = value )
--SET ( TYPMOD_OUT = value )
--SET ( ANALYZE = value )
--SET ( STORAGE = plain | extended | external | main )
"""
        ),
    },
    "drop_type": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-droptype.html \nDROP TYPE #type_name#
--CASCADE
"""
    ),
    "create_domain": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createdomain.html
CREATE DOMAIN #schema_name#.name AS data_type
--COLLATE collation
--DEFAULT expression
-- [ CONSTRAINT constraint_name ] NOT NULL
-- [ CONSTRAINT constraint_name ] NULL
-- [ CONSTRAINT constraint_name ] CHECK (expression)
"""
    ),
    "alter_domain": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-alterdomain.html
ALTER DOMAIN #domain_name#
--SET DEFAULT expression
--DROP DEFAULT
--SET NOT NULL
--DROP NOT NULL
--ADD domain_constraint [ NOT VALID ]
--DROP CONSTRAINT constraint_name [ CASCADE ]
--RENAME CONSTRAINT constraint_name TO new_constraint_name
--VALIDATE CONSTRAINT constraint_name
--OWNER TO new_owner
--RENAME TO new_name
--SET SCHEMA new_schema
"""
    ),
    "drop_domain": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-dropdomain.html \nDROP DOMAIN #domain_name#
--CASCADE
"""
    ),
    "create_statistics": Template(
        """-- https://www.postgresql.org/docs/$major_version/sql-createstatistics.html
CREATE STATISTICS #schema_name#.statistics_name
--( ndistinct )
--( dependencies )
--( mcv )
ON column_name, column_name [, ...]
FROM #table_name#
"""
    ),
    "alter_statistics": {
        "<13": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterstatistics.html
ALTER STATISTICS #statistics_name#
--OWNER to { new_owner | CURRENT_USER | SESSION_USER }
--RENAME TO new_name
--SET SCHEMA new_schema
"""
        ),
        "default": Template(
            """-- https://www.postgresql.org/docs/$major_version/sql-alterstatistics.html
ALTER STATISTICS #statistics_name#
--OWNER to { new_owner | CURRENT_USER | SESSION_USER }
--RENAME TO new_name
--SET SCHEMA new_schema
--SET STATISTICS new_target
"""
        ),
    },
    "drop_statistics": Template(
        "-- https://www.postgresql.org/docs/$major_version/sql-dropstatistics.html \nDROP STATISTICS #statistics_name#"
    ),
}


def get_template(key, version=None) -> Template:
    tpl = TEMPLATES.get(key)
    if tpl is None:
        raise KeyError(f"No such template: {key}")

    if isinstance(tpl, dict):
        version = int(version or 0)
        if version < 130000:
            return tpl.get("<13")
        if 140000 > version >= 130000:
            return tpl.get("13-14")
        return tpl.get("default")
    return tpl
