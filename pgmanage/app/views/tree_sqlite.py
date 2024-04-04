from app.utils.decorators import (
    database_required,
    database_required_new,
    user_authenticated,
)
from app.utils.response_helpers import create_response_template, error_response
from django.http import JsonResponse


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_tree_info(request, database):
    try:
        data = {
            "version": database.GetVersion(),
            "create_view": database.TemplateCreateView().v_text,
            "drop_view": database.TemplateDropView().v_text,
            "create_table": database.TemplateCreateTable().v_text,
            "alter_table": database.TemplateAlterTable().v_text,
            "drop_table": database.TemplateDropTable().v_text,
            "create_column": database.TemplateCreateColumn().v_text,
            "create_index": database.TemplateCreateIndex().v_text,
            "reindex": database.TemplateReindex().v_text,
            "drop_index": database.TemplateDropIndex().v_text,
            "delete": database.TemplateDelete().v_text,
            "create_trigger": database.TemplateCreateTrigger().v_text,
            "drop_trigger": database.TemplateDropTrigger().v_text,
        }
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)
    return JsonResponse(data=data)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_tables(request, database):
    tables_list = []
    try:
        tables = database.QueryTables()
        for table in tables.Rows:
            table_data = {
                "name": table["table_name"],
                "name_raw": table["name_raw"],
            }
            tables_list.append(table_data)

    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=tables_list, safe=False)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_columns(request, database):
    table = request.data["table"]

    list_columns = []

    try:
        columns = database.QueryTablesFields(table)

        for column in columns.Rows:
            column_data = {
                "column_name": column["column_name"],
                "data_type": column["data_type"],
                "data_length": column["data_length"],
                "nullable": column["nullable"],
            }

            list_columns.append(column_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_columns, safe=False)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_pk(request, database):
    table = request.data["table"]

    try:
        pks = database.QueryTablesPrimaryKeys(table)
        list_pk = [pk["constraint_name"] for pk in pks.Rows]
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_pk, safe=False)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_pk_columns(request, database):
    table = request.data["table"]

    try:
        pk = database.QueryTablesPrimaryKeysColumns(table)
        list_pk = [row["column_name"] for row in pk.Rows]
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_pk, safe=False)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_fks(request, database):
    table = request.data["table"]

    try:
        fks = database.QueryTablesForeignKeys(table)
        list_fk = [fk["constraint_name"] for fk in fks.Rows]
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_fk, safe=False)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_fks_columns(request, database):
    data = request.data
    fkey = data["fkey"]
    table = data["table"]

    try:
        fks = database.QueryTablesForeignKeysColumns(fkey, table)
        fk = fks.Rows.pop() if fks.Rows else {}
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=fk)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_uniques(request, database):
    table = request.data["table"]

    try:
        uniques = database.QueryTablesUniques(table)
        list_uniques = [unique["constraint_name"] for unique in uniques.Rows]
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_uniques, safe=False)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_uniques_columns(request, database):
    data = request.data
    v_unique = data["unique"]
    v_table = data["table"]

    try:
        uniques = database.QueryTablesUniquesColumns(v_unique, v_table)
        list_uniques = [unique["column_name"] for unique in uniques.Rows]
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_uniques, safe=False)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_indexes(request, database):
    table = request.data["table"]

    list_indexes = []

    try:
        indexes = database.QueryTablesIndexes(table)

        for index in indexes.Rows:
            index_data = {
                "index_name": index["index_name"],
                "uniqueness": index["uniqueness"],
            }
            list_indexes.append(index_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_indexes, safe=False)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_indexes_columns(request, database):
    data = request.data
    index = data["index"]
    table = data["table"]

    try:
        indexes = database.QueryTablesIndexesColumns(index, table)
        list_indexes = [index["column_name"] for index in indexes.Rows]
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_indexes, safe=False)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_views(request, database):
    views_list = []
    try:
        views = database.QueryViews()
        for view in views.Rows:
            view_data = {
                "name": view["table_name"],
                "name_raw": view["name_raw"],
            }
            views_list.append(view_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)
    return JsonResponse(data=views_list, safe=False)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_views_columns(request, database):
    table = request.data["table"]

    list_columns = []

    try:
        columns = database.QueryViewFields(table)

        for v_column in columns.Rows:
            v_column_data = {
                "column_name": v_column["column_name"],
                "data_type": v_column["data_type"],
                "data_length": v_column["data_length"],
            }

            list_columns.append(v_column_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_columns, safe=False)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_triggers(request, database):
    table = request.data["table"]

    try:
        triggers = database.QueryTablesTriggers(table)
        list_triggers = [trigger["trigger_name"] for trigger in triggers.Rows]
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_triggers, safe=False)


@user_authenticated
@database_required(p_check_timeout=False, p_open_connection=True)
def template_select(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data["p_table"]
    v_kind = data["p_kind"]

    try:
        v_template = v_database.TemplateSelect(v_table, v_kind).v_text
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=False, status=400)

    v_return["v_data"] = {"v_template": v_template}

    return JsonResponse(v_return)


@user_authenticated
@database_required(p_check_timeout=False, p_open_connection=True)
def template_insert(request, v_database):
    v_return = create_response_template()

    v_table = request.data["p_table"]

    try:
        v_template = v_database.TemplateInsert(v_table).v_text
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=False, status=400)

    v_return["v_data"] = {"v_template": v_template}

    return JsonResponse(v_return)


@user_authenticated
@database_required(p_check_timeout=False, p_open_connection=True)
def template_update(request, v_database):
    v_return = create_response_template()

    v_table = request.data["p_table"]

    try:
        v_template = v_database.TemplateUpdate(v_table).v_text
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=False, status=400)

    v_return["v_data"] = {"v_template": v_template}

    return JsonResponse(v_return)


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_properties(request, database):
    data = request.data["data"]

    list_properties = []

    try:
        properties = database.GetProperties(data["table"], data["object"], data["type"])

        for property_object in properties.Rows:
            list_properties.append(
                [property_object["Property"], property_object["Value"]]
            )

        ddl = database.GetDDL(data["table"], data["object"], data["type"])
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data={"properties": list_properties, "ddl": ddl})


@user_authenticated
@database_required_new(check_timeout=False, open_connection=True)
def get_table_definition(request, database):
    data = request.data
    table = data["table"]

    columns = []
    try:
        q_primaries = database.QueryTablesPrimaryKeysColumns(table)
        pk_column_names = [x.get("column_name") for x in q_primaries.Rows]
        q_definition = database.QueryTableDefinition(table)
        for col in q_definition.Rows:
            column_data = {
                "name": col["name"],
                "data_type": col["type"],
                "default_value": col["dflt_value"],
                "nullable": col["notnull"] == "0",
                "is_primary": col["name"] in pk_column_names,
            }
            columns.append(column_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data={"data": columns})
