from app.models.main import Connection
from app.utils.decorators import database_required, user_authenticated
from django.http import JsonResponse


@user_authenticated
@database_required(check_timeout=False, open_connection=True)
def get_tree_info(request, database):
    try:
        data = {
            "version": database.GetVersion(),
            "major_version": database.major_version,
        }
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)
    return JsonResponse(data=data)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_databases(request, database):
    list_databases = []

    try:
        conn_object = Connection.objects.get(id=database.conn_id)
        databases = database.QueryDatabases()
        for database_object in databases.Rows:
            database_data = {
                "name": database_object[0],
                "pinned": database_object[0] in conn_object.pinned_databases,
            }
            list_databases.append(database_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_databases, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_schemas(request, database):
    schemas_list = []

    try:
        schemas = database.QuerySchemas()
        for schema in schemas.Rows:
            schema_data = {
                "name": schema["schema_name"],
            }
            schemas_list.append(schema_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=500)
    return JsonResponse(data=schemas_list, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_tables(request, database):
    schema = request.data["schema"]

    try:
        tables = database.QueryTables(False, schema)
        list_tables = [table["table_name"] for table in tables.Rows]
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_tables, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_columns(request, database):
    data = request.data
    table = data["table"]
    schema = data["schema"]

    list_columns = []

    try:
        columns = database.QueryTablesFields(table, False, schema)
        for column in columns.Rows:
            column_data = {
                "column_name": column["column_name"],
                "data_type": column["data_type"],
                "data_length": column["max_length"],
                "nullable": column["is_nullable"],
            }
            list_columns.append(column_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_columns, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_statistics(request, database):
    data = request.data
    table = data["table"]
    schema = data["schema"]

    list_statistics = []

    try:
        statistics = database.QueryTablesStatistics(table, False, schema)
        for statistic in statistics.Rows:
            statistic_data = {
                "statistic_name": statistic["statistic_name"],
                "schema_name": statistic["schema_name"],
            }
            list_statistics.append(statistic_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_statistics, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_views(request, database):
    schema = request.data["schema"]

    list_tables = []

    try:
        tables = database.QueryViews(False, schema)
        for table in tables.Rows:
            table_data = {
                "name": table["table_name"],
            }
            list_tables.append(table_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_tables, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_views_columns(request, database):
    data = request.data
    table = data["table"]
    schema = data["schema"]

    list_columns = []

    try:
        columns = database.QueryViewFields(table, False, schema)
        for column in columns.Rows:
            column_data = {
                "column_name": column["column_name"],
                "data_type": column["data_type"],
            }
            list_columns.append(column_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_columns, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_procedures(request, database):
    schema = request.data["schema"]

    list_functions = []

    try:
        functions = database.QueryProcedures(False, schema)
        for function in functions.Rows:
            function_data = {
                "name": function["name"],
                "oid": function["object_id"],
            }
            list_functions.append(function_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_functions, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_procedure_fields(request, database):
    data = request.data
    function = data["procedure"]
    schema = data["schema"]

    list_fields = []

    try:
        fields = database.QueryFunctionFields(function, schema)
        for field in fields.Rows:
            field_name = f'{field["parameter_name"]} {field["data_type"]}'
            field_data = {"name": field_name, "type": field["param_type"]}
            list_fields.append(field_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_fields, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_functions(request, database):
    schema = request.data["schema"]

    list_functions = []

    try:
        functions = database.QueryFunctions(False, schema)
        for function in functions.Rows:
            function_data = {
                "name": function["routine_name"],
            }
            list_functions.append(function_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_functions, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_function_fields(request, database):
    data = request.data
    function = data["function"]
    schema = data["schema"]

    list_fields = []

    try:
        fields = database.QueryFunctionFields(function, schema)
        for field in fields.Rows:
            field_name = f'{field["parameter_name"]} {field["data_type"]}'
            field_data = {"name": field_name, "type": field["param_type"]}
            list_fields.append(field_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_fields, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_pk(request, database):
    data = request.data
    table = data["table"]
    schema = data["schema"]

    list_pk = []

    try:
        pks = database.QueryTablesPrimaryKeys(table, False, schema)
        for pk in pks.Rows:
            pk_data = {
                "constraint_name": pk["constraint_name"],
            }
            list_pk.append(pk_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_pk, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_pk_columns(request, database):
    data = request.data
    pkey = data["key"]
    table = data["table"]
    schema = data["schema"]

    try:
        pks = database.QueryTablesPrimaryKeysColumns(pkey, table, False, schema)
        list_pk = [pk["column_name"] for pk in pks.Rows]
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_pk, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_fks(request, database):
    data = request.data
    table = data["table"]
    schema = data["schema"]

    list_fk = []

    try:
        fks = database.QueryTablesForeignKeys(table, False, schema)
        for fk in fks.Rows:
            fk_data = {
                "constraint_name": fk["constraint_name"],
            }
            list_fk.append(fk_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_fk, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_fks_columns(request, database):
    data = request.data
    fkey = data["fkey"]
    table = data["table"]
    schema = data["schema"]

    list_fk = []

    try:
        fks = database.QueryTablesForeignKeysColumns(fkey, table, False, schema)
        for fk in fks.Rows:
            fk_data = {
                "r_table_name": fk["r_table_name"],
                "delete_rule": fk["delete_rule"],
                "update_rule": fk["update_rule"],
                "column_name": fk["column_name"],
                "r_column_name": fk["r_column_name"],
            }
            list_fk.append(fk_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_fk, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_uniques(request, database):
    data = request.data
    table = data["table"]
    schema = data["schema"]

    list_uniques = []

    try:
        uniques = database.QueryTablesUniques(table, False, schema)
        for unique in uniques.Rows:
            v_unique_data = {
                "constraint_name": unique["constraint_name"],
            }
            list_uniques.append(v_unique_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_uniques, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_uniques_columns(request, database):
    data = request.data
    unique = data["unique"]
    table = data["table"]
    schema = data["schema"]

    try:
        uniques = database.QueryTablesUniquesColumns(unique, table, False, schema)
        list_uniques = [unique["column_name"] for unique in uniques.Rows]
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_uniques, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_indexes(request, database):
    data = request.data
    table = data["table"]
    schema = data["schema"]

    list_indexes = []

    try:
        indexes = database.QueryTablesIndexes(table, False, schema)
        for index in indexes.Rows:
            index_data = {
                "index_name": index["index_name"],
                "unique": index["is_unique"] == "True",
            }
            list_indexes.append(index_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_indexes, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_indexes_columns(request, database):
    data = request.data
    index = data["index"]
    table = data["table"]
    schema = data["schema"]

    try:
        indexes = database.QueryTablesIndexesColumns(index, table, False, schema)
        list_indexes = [index["column_name"] for index in indexes.Rows]
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_indexes, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_checks(request, database):
    data = request.data
    table = data["table"]
    schema = data["schema"]

    list_checks = []

    try:
        checks = database.QueryTablesChecks(table, False, schema)
        for check in checks.Rows:
            check_data = {
                "constraint_name": check["constraint_name"],
                "check_clause": check["check_clause"],
            }
            list_checks.append(check_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_checks, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_triggers(request, database):
    data = request.data
    table = data["table"]
    schema = data["schema"]

    list_triggers = []

    try:
        triggers = database.QueryTablesTriggers(table, False, schema)
        for trigger in triggers.Rows:
            trigger_data = {
                "trigger_name": trigger["trigger_name"],
                "enabled": trigger["is_disabled"] == "False",
            }
            list_triggers.append(trigger_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data=list_triggers, safe=False)


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_server_roles(request, database):
    list_roles = []
    try:
        roles = database.QueryServerRoles()
        for role in roles.Rows:
            role_data = {
                "name": role["role_name"],
            }
            list_roles.append(role_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)
    return JsonResponse(data={"data": list_roles})


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_database_roles(request, database):
    list_roles = []
    try:
        roles = database.QueryDatabaseRoles()
        for role in roles.Rows:
            role_data = {
                "name": role["role_name"],
            }
            list_roles.append(role_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)
    return JsonResponse(data={"data": list_roles})


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_logins(request, database):
    list_roles = []
    try:
        roles = database.QueryLogins()
        for role in roles.Rows:
            role_data = {
                "name": role["login_name"],
            }
            list_roles.append(role_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)
    return JsonResponse(data={"data": list_roles})


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def get_users(request, database):
    list_roles = []
    try:
        roles = database.QueryUsers()
        for role in roles.Rows:
            role_data = {
                "name": role["user_name"],
            }
            list_roles.append(role_data)
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)
    return JsonResponse(data={"data": list_roles})


@user_authenticated
@database_required(check_timeout=True, open_connection=True)
def template_select(request, database):
    data = request.data
    table = data["table"]
    schema = data["schema"]
    kind = data["kind"]

    try:
        template = database.TemplateSelect(schema, table, kind).text
    except Exception as exc:
        return JsonResponse(data={"data": str(exc)}, status=400)

    return JsonResponse(data={"template": template})
