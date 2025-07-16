from string import Template

TEMPLATES = {
    "create_view": Template(
        """CREATE
--TEMPORARY
VIEW view_name
--( column_definition, ... )
AS
--SELECT...
"""
    ),
    "drop_view": Template("DROP VIEW #view_name#"),
    "create_table": Template(
        """CREATE
--TEMPORARY
TABLE table_name
(
    column_name data_type
    --CONSTRAINT constraint_name
    --NOT NULL
    --CHECK
    --UNIQUE
    --PRIMARY KEY
    --FOREIGN KEY
)
--WITHOUT ROWID
"""
    ),
    "alter_table": Template(
        """ALTER TABLE #table_name#
--RENAME TO new_table_name
--RENAME COLUMN column_name TO new_column_name
--ADD COLUMN columnd_definition
"""
    ),
    "drop_table": Template("DROP TABLE #table_name#"),
    "create_column": Template(
        """ALTER TABLE #table_name#
ADD COLUMN columnd_definition
"""
    ),
    "create_index": Template(
        """CREATE
--UNIQUE
INDEX index_name ON #table_name# ( column_name, ... )
--WHERE expression
"""
    ),
    "reindex": Template("REINDEX #index_name#"),
    "drop_index": Template("DROP INDEX #index_name#"),
    "delete": Template(
        """DELETE FROM
#table_name#
WHERE condition
"""
    ),
    "create_trigger": Template(
        """CREATE
--TEMPORARY
TRIGGER trigger_name
--BEFORE
--AFTER
--INSTEAD OF
--DELETE
--INSERT
--UPDATE
--OF column_name
ON #table_name#
--FOR EACH ROW
WHEN expression
BEGIN
    statement
;
END
"""
    ),
    "alter_trigger": Template(
        """DROP TRIGGER #trigger_name#
                              CREATE
--TEMPORARY
TRIGGER trigger_name
--BEFORE
--AFTER
--INSTEAD OF
--DELETE
--INSERT
--UPDATE
--OF column_name
ON #table_name#
--FOR EACH ROW
WHEN expression
BEGIN
    statement
;
END
"""
    ),
    "drop_trigger": Template("DROP TRIGGER #trigger_name#"),
}


def get_template(key, version=None):
    tpl = TEMPLATES.get(key)
    if tpl is None:
        raise KeyError(f"No such template: {key}")
    return tpl
