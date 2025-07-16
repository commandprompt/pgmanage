from string import Template

TEMPLATES = {
    "create_role": Template(
        """CREATE { ROLE | USER } name
--NOT IDENTIFIED
--IDENTIFIED BY password
--DEFAULT TABLESPACE tablespace
--TEMPORARY TABLESPACE tablespace
--QUOTA { size | UNLIMITED } ON tablespace
--PASSWORD EXPIRE
--ACCOUNT { LOCK | UNLOCK }
"""
    ),
    "alter_role": Template(
        """ALTER { ROLE | USER } #role_name#
--NOT IDENTIFIED
--IDENTIFIED BY password
--DEFAULT TABLESPACE tablespace
--TEMPORARY TABLESPACE tablespace
--QUOTA { size | UNLIMITED } ON tablespace
--DEFAULT ROLE { role [, role ] ... | ALL [ EXCEPT role [, role ] ... ] | NONE }
--PASSWORD EXPIRE
--ACCOUNT { LOCK | UNLOCK }
"""
    ),
    "drop_role": Template(
        """DROP { ROLE | USER } #role_name#
--CASCADE
"""
    ),
    "create_tablespace": Template(
        """CREATE { SMALLFILE | BIGFILE }
[ TEMPORARY | UNDO ] TABLESPACE name
[ DATAFILE | TEMPFILE ] 'filename' [ SIZE size ] [ REUSE ]
--AUTOEXTEND OFF | AUTOEXTEND ON [ NEXT size ]
--MAXSIZE [ size | UNLIMITED ]
--MINIMUM EXTENT size
--BLOCKSIZE size
--LOGGING | NOLOGGING | FORCE LOGGING
--ENCRYPTION [ USING 'algorithm' ]
--ONLINE | OFFLINE
--EXTENT MANAGEMENT LOCAL { AUTOALLOCATE | UNIFORM [ SIZE size ] }
--SEGMENT SPACE MANAGEMENT { AUTO | MANUAL }
--FLASHBACK { ON | OFF }
--RETENTION { GUARANTEE | NOGUARANTEE }
"""
    ),
    "alter_tablespace": Template(
        """ALTER TABLESPACE #tablespace_name#
--MINIMUM EXTENT size
--RESIZE size
--COALESCE
--SHRINK SPACE [ KEEP size ]
--RENAME TO new_name
--[ BEGIN | END ] BACKUP
--ADD [ DATAFILE | TEMPFILE ] 'filename' [ SIZE size ] [ REUSE AUTOEXTEND OFF | AUTOEXTEND ON [ NEXT size ] ] [ MAXSIZE [ size | UNLIMITED ] ]
--DROP [ DATAFILE | TEMPFILE ] 'filename'
--SHRINK TEMPFILE 'filename' [ KEEP size ]
--RENAME DATAFILE 'filename' TO 'new_filename'
--[ DATAFILE | TEMPFILE ] [ ONLINE | OFFLINE ]
--[ NO ] FORCE LOGGING
--ONLINE
--OFFLINE [ NORMAL | TEMPORARY | IMMEDIATE ]
--READ [ ONLY | WRITE ]
--PERMANENT | TEMPORARY
--AUTOEXTEND OFF | AUTOEXTEND ON [ NEXT size ]
--MAXSIZE [ size | UNLIMITED ]
--FLASHBACK { ON | OFF }
--RETENTION { GUARANTEE | NOGUARANTEE }
"""
    ),
    "drop_tablespace": Template(
        """DROP TABLESPACE #tablespace_name#
--INCLUDING CONTENTS
--[ AND | KEEP ] DATAFILES
--CASCADE CONSTRAINTS
"""
    ),
    "create_sequence": Template(
        """CREATE SEQUENCE #schema_name#.name
--INCREMENT BY increment
--MINVALUE minvalue | NOMINVALUE
--MAXVALUE maxvalue | NOMAXVALUE
--START WITH start
--CACHE cache | NOCACHE
--CYCLE | NOCYCLE
--ORDER | NOORDER
"""
    ),
    "alter_sequence": Template(
        """ALTER SEQUENCE #sequence_name#
--INCREMENT BY increment
--MINVALUE minvalue | NOMINVALUE
--MAXVALUE maxvalue | NOMAXVALUE
--CACHE cache | NOCACHE
--CYCLE | NOCYCLE
--ORDER | NOORDER
"""
    ),
    "drop_sequence": Template("DROP SEQUENCE #sequence_name#"),
    "create_function": Template(
        """CREATE OR REPLACE FUNCTION #schema_name#.name
--(
--    [ argmode ] [ argname ] argtype [ { DEFAULT | = } default_expr ]
--)
--RETURN rettype
--PIPELINED
AS
-- variables
-- pragmas
BEGIN
-- definition
END;
"""
    ),
    "drop_function": Template("DROP FUNCTION #function_name#"),
    "create_procedure": Template(
        """CREATE OR REPLACE PROCEDURE #schema_name#.name
--(
--    [ argmode ] [ argname ] argtype [ { DEFAULT | = } default_expr ]
--)
AS
-- variables
-- pragmas
BEGIN
-- definition
END;
"""
    ),
    "drop_procedure": Template("DROP PROCEDURE #function_name#"),
    "create_view": Template(
        """CREATE OR REPLACE VIEW #schema_name#.name AS
SELECT ...
"""
    ),
    "drop_view": Template(
        """DROP VIEW #view_name#
--CASCADE CONSTRAINTS
"""
    ),
    "create_table": Template(
        """CREATE
--GLOBAL TEMPORARY
TABLE #schema_name#.table_name
--AS query
(
    column_name data_type
    --SORT
    --DEFAULT expr
    --ENCRYPT [ USING 'encrypt_algorithm' ] [ IDENTIFIED BY password ] [ [NO] SALT ]
    --CONSTRAINT constraint_name
    --NOT NULL
    --NULL
    --UNIQUE
    --PRIMARY KEY
    --REFERENCES reftable [ ( refcolumn ) ] [ ON DELETE { CASCADE | SET NULL } ]
    --CHECK ( condition )
    --DEFERRABLE
    --NOT DEFERRABLE
    --INITIALLY IMMEDIATE
    --INITIALLY DEFERRED
    --ENABLE
    --DISABLE
    --VALIDATE
    --NOVALIDATE
    --RELY
    --NORELY
    --USING INDEX index_name
)
--ON COMMIT DELETE ROWS
--ON COMMIT PRESERVE ROWS
--PCTFREE integer
--PCTUSED integer
--INITRANS integer
--STORAGE ( { [ INITIAL size_clause ] | [ NEXT size_clause ] | [ MINEXTENTS integer ] | [ MAXEXTENTS { integer | UNLIMITED } ] } )
--TABLESPACE tablespace
--LOGGING
--NOLOGGING
--COMPRESS
--NOCOMPRESS
--SCOPE IS scope_table
--WITH ROWID
--SCOPE FOR ( { refcol | refattr } ) IS scope_table
--REF ( { refcol | refattr } ) WITH ROWID
--GROUP log_group ( column [ NO LOG ] ) [ ALWAYS ]
--DATA ( { ALL | PRIMARY KEY | UNIQUE | FOREIGN KEY } ) COLUMNS
"""
    ),
    "alter_table": Template(
        """ALTER TABLE #table_name#
--ADD column_name data_type
--MODIFY (column_name [ data_type ] )
--SORT
--DEFAULT expr
--ENCRYPT [ USING 'encrypt_algorithm' ] [ IDENTIFIED BY password ] [ [NO] SALT ]
--CONSTRAINT constraint_name
--NOT NULL
--NULL
--UNIQUE
--PRIMARY KEY
--REFERENCES reftable [ ( refcolumn ) ] [ ON DELETE { CASCADE | SET NULL } ]
--CHECK ( condition )
--DEFERRABLE
--NOT DEFERRABLE
--INITIALLY IMMEDIATE
--INITIALLY DEFERRED
--ENABLE
--DISABLE
--VALIDATE
--NOVALIDATE
--RELY
--NORELY
--USING INDEX index_name
--SET UNUSED COLUMN column [ { CASCADE CONSTRAINTS | INVALIDADE } ]
--DROP COLUMN column [ { CASCADE CONSTRAINTS | INVALIDADE } ] [ CHECKPOINT integer ]
--DROP { UNUSED COLUMNS | COLUMNS CONTINUE } [ CHECKPOINT integer ]
--RENAME COLUMN old_name TO new_name
--ADD CONSTRAINT constraint_name
--NOT NULL
--NULL
--UNIQUE
--PRIMARY KEY
--REFERENCES reftable [ ( refcolumn ) ] [ ON DELETE { CASCADE | SET NULL } ]
--CHECK ( condition )
--MODIFY [ CONSTRAINT constraint_name ] [ PRIMARY KEY ] [ UNIQUE ( column ) ]
--DEFERRABLE
--NOT DEFERRABLE
--INITIALLY IMMEDIATE
--INITIALLY DEFERRED
--ENABLE
--DISABLE
--VALIDATE
--NOVALIDATE
--RELY
--NORELY
--USING INDEX index_name
--RENAME CONSTRAINT old_name TO new_name
--DROP PRIMARY KEY [ CASCADE ] [ { KEEP | DROP } INDEX ]
--DROP UNIQUE ( column ) [ CASCADE ] [ { KEEP | DROP } INDEX ]
--DROP CONSTRAINT constraint_name [ CASCADE ]
--PCTFREE integer
--PCTUSED integer
--INITRANS integer
--STORAGE ( { [ INITIAL size_clause ] | [ NEXT size_clause ] | [ MINEXTENTS integer ] | [ MAXEXTENTS { integer | UNLIMITED } ] } )
--TABLESPACE tablespace
--LOGGING
--NOLOGGING
--COMPRESS
--NOCOMPRESS
--CACHE
--NOCACHE
--READ ONLY
--READ WRITE
--SCOPE IS scope_table
--WITH ROWID
--SCOPE FOR ( { refcol | refattr } ) IS scope_table
--REF ( { refcol | refattr } ) WITH ROWID
--GROUP log_group ( column [ NO LOG ] ) [ ALWAYS ]
--DATA ( { ALL | PRIMARY KEY | UNIQUE | FOREIGN KEY } ) COLUMNS
--NOPARALLEL
--PARALLEL integer
"""
    ),
    "drop_table": Template(
        """DROP TABLE #table_name#
--CASCADE CONSTRAINTS
--PURGE
"""
    ),
    "create_column": Template(
        """ALTER TABLE #table_name#
ADD name data_type
--SORT
--DEFAULT expr
--NOT NULL
"""
    ),
    "alter_column": Template(
        """ALTER TABLE #table_name#
--MODIFY #column_name# { datatype | DEFAULT expr | [ NULL | NOT NULL ]}
--RENAME COLUMN #column_name# TO new_name
"""
    ),
    "drop_column": Template(
        """ALTER TABLE #table_name#
DROP COLUMN #column_name#
--CASCADE CONSTRAINTS
--INVALIDATE
"""
    ),
    "create_primarykey": Template(
        """ALTER TABLE #table_name#
ADD CONSTRAINT name
PRIMARY KEY ( column_name [, ... ] )
--[ NOT ] DEFERRABLE
--INITIALLY { IMMEDIATE | DEFERRED }
--RELY | NORELY
--USING INDEX index_name
--ENABLE
--DISABLE
--VALIDATE
--NOVALIDATE
--EXCEPTIONS INTO table_name
"""
    ),
    "drop_primarykey": Template(
        """ALTER TABLE #table_name#
DROP CONSTRAINT #constraint_name#
--CASCADE
"""
    ),
    "create_unique": Template(
        """ALTER TABLE #table_name#
ADD CONSTRAINT name
UNIQUE ( column_name [, ... ] )
--[ NOT ] DEFERRABLE
--INITIALLY { IMMEDIATE | DEFERRED }
--RELY | NORELY
--USING INDEX index_name
--ENABLE
--DISABLE
--VALIDATE
--NOVALIDATE
--EXCEPTIONS INTO table_name
"""
    ),
    "drop_unique": Template(
        """ALTER TABLE #table_name#
DROP CONSTRAINT #constraint_name#
--CASCADE
"""
    ),
    "create_foreignkey": Template(
        """ALTER TABLE #table_name#
ADD CONSTRAINT name
FOREIGN KEY ( column_name [, ... ] )
REFERENCES reftable [ ( refcolumn [, ... ] ) ]
--[ NOT ] DEFERRABLE
--INITIALLY { IMMEDIATE | DEFERRED }
--RELY | NORELY
--USING INDEX index_name
--ENABLE
--DISABLE
--VALIDATE
--NOVALIDATE
--EXCEPTIONS INTO table_name
"""
    ),
    "drop_foreignkey": Template(
        """ALTER TABLE #table_name#
DROP CONSTRAINT #constraint_name#
--CASCADE
"""
    ),
    "create_index": Template(
        """CREATE [ UNIQUE ] INDEX name
ON #table_name#
( { column_name | ( expression ) } [ ASC | DESC ] )
--ONLINE
--TABLESPACE tablespace
--[ SORT | NOSORT ]
--REVERSE
--[ VISIBLE | INVISIBLE ]
--[ NOPARALLEL | PARALLEL integer ]
"""
    ),
    "alter_index": Template(
        """ALTER INDEX #index_name#
--COMPILE
--[ ENABLE | DISABLE ]
--UNUSABLE
--[ VISIBLE | INVISIBLE ]
--RENAME TO new_name
--COALESCE
--[ MONITORING | NOMONITORING ] USAGE
--UPDATE BLOCK REFERENCES
"""
    ),
    "drop_index": Template(
        """DROP INDEX #index_name#
--FORCE
"""
    ),
    "delete": Template(
        """DELETE FROM #table_name#
WHERE condition
"""
    ),
}


def get_template(key, version=None):
    tpl = TEMPLATES.get(key)
    if tpl is None:
        raise KeyError(f"No such template: {key}")
    return tpl
