from app.utils.decorators import (
    database_required,
    database_required_new,
    user_authenticated,
)
from app.utils.response_helpers import create_response_template, error_response
from django.http import JsonResponse


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_tree_info(request, database):
    try:
        data = {
            "database": database.GetName(),
            "version": database.GetVersion(),
            "username": database.GetUserName(),
            "superuser": database.GetUserSuper(),
            "create_role": database.TemplateCreateRole().v_text,
            "alter_role": database.TemplateAlterRole().v_text,
            "drop_role": database.TemplateDropRole().v_text,
            "create_database": database.TemplateCreateDatabase().v_text,
            "alter_database": database.TemplateAlterDatabase().v_text,
            "drop_database": database.TemplateDropDatabase().v_text,
            "create_sequence": database.TemplateCreateSequence().v_text,
            "alter_sequence": database.TemplateAlterSequence().v_text,
            "drop_sequence": database.TemplateDropSequence().v_text,
            "create_function": database.TemplateCreateFunction().v_text,
            "drop_function": database.TemplateDropFunction().v_text,
            "create_procedure": database.TemplateCreateProcedure().v_text,
            "drop_procedure": database.TemplateDropProcedure().v_text,
            #'create_triggerfunction': database.TemplateCreateTriggerFunction().v_text,
            #'drop_triggerfunction': database.TemplateDropTriggerFunction().v_text,
            "create_view": database.TemplateCreateView().v_text,
            "drop_view": database.TemplateDropView().v_text,
            "create_table": database.TemplateCreateTable().v_text,
            "alter_table": database.TemplateAlterTable().v_text,
            "drop_table": database.TemplateDropTable().v_text,
            "create_column": database.TemplateCreateColumn().v_text,
            "alter_column": database.TemplateAlterColumn().v_text,
            "drop_column": database.TemplateDropColumn().v_text,
            "create_primarykey": database.TemplateCreatePrimaryKey().v_text,
            "drop_primarykey": database.TemplateDropPrimaryKey().v_text,
            "create_unique": database.TemplateCreateUnique().v_text,
            "drop_unique": database.TemplateDropUnique().v_text,
            "create_foreignkey": database.TemplateCreateForeignKey().v_text,
            "drop_foreignkey": database.TemplateDropForeignKey().v_text,
            "create_index": database.TemplateCreateIndex().v_text,
            "drop_index": database.TemplateDropIndex().v_text,
            #'create_trigger': database.TemplateCreateTrigger().v_text,
            #'create_view_trigger': database.TemplateCreateViewTrigger().v_text,
            #'alter_trigger': database.TemplateAlterTrigger().v_text,
            #'enable_trigger': database.TemplateEnableTrigger().v_text,
            #'disable_trigger': database.TemplateDisableTrigger().v_text,
            #'drop_trigger': database.TemplateDropTrigger().v_text,
            #'create_partition': database.TemplateCreatePartition().v_text,
            #'noinherit_partition': database.TemplateNoInheritPartition().v_text,
            #'drop_partition': database.TemplateDropPartition().v_text
            "delete": database.TemplateDelete().v_text,
        }
    except Exception as exc:
        data = {"password_timeout": False, "data": str(exc)}
        return JsonResponse(data=data, status=500)
    return JsonResponse(data=data)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_properties(request, database):
    data = request.data["data"]

    list_properties = []
    ddl = ""

    try:
        properties = database.GetProperties(
            data["schema"], data["table"], data["object"], data["type"]
        )
        for property_object in properties.Rows:
            list_properties.append(
                [property_object["Property"], property_object["Value"]]
            )
        ddl = database.GetDDL(
            data["schema"], data["table"], data["object"], data["type"]
        )
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data={"properties": list_properties, "ddl": ddl})


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_tables(request, database):
    schema = request.data["schema"]

    try:
        tables = database.QueryTables(False, schema)
        list_tables = [table["table_name"] for table in tables.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)
    return JsonResponse(data=list_tables, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
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
                "data_length": column["data_length"],
                "nullable": column["nullable"],
            }
            list_columns.append(column_data)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_columns, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_pk(request, database):
    table = request.data["table"]
    schema = request.data["schema"]
    try:
        pks = database.QueryTablesPrimaryKeys(table, False, schema)
        list_pk = [pk["constraint_name"] for pk in pks.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_pk, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_pk_columns(request, database):
    data = request.data
    v_pkey = data["key"]
    v_table = data["table"]
    v_schema = data["schema"]

    try:
        pks = database.QueryTablesPrimaryKeysColumns(v_pkey, v_table, False, v_schema)
        list_pk = [row["column_name"] for row in pks.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_pk, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_fks(request, database):
    data = request.data
    table = data["table"]
    schema = data["schema"]

    try:
        fks = database.QueryTablesForeignKeys(table, False, schema)
        list_fk = [fk["constraint_name"] for fk in fks.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_fk, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_fks_columns(request, database):
    data = request.data
    fkey = data["fkey"]
    table = data["table"]
    schema = data["schema"]

    try:
        fks = database.QueryTablesForeignKeysColumns(fkey, table, False, schema)
        fk = fks.Rows.pop() if fks.Rows else {}
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=fk)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_uniques(request, database):
    data = request.data
    table = data["table"]
    schema = data["schema"]

    try:
        uniques = database.QueryTablesUniques(table, False, schema)
        list_uniques = [unique["constraint_name"] for unique in uniques.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_uniques, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_uniques_columns(request, database):
    data = request.data
    unique = data["unique"]
    table = data["table"]
    schema = data["schema"]

    try:
        uniques = database.QueryTablesUniquesColumns(unique, table, False, schema)
        list_uniques = [unique["column_name"] for unique in uniques.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_uniques, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
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
                "uniqueness": index["uniqueness"],
            }
            list_indexes.append(index_data)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_indexes, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_indexes_columns(request, database):
    data = request.data
    index = data["index"]
    table = data["table"]
    schema = data["schema"]

    try:
        indexes = database.QueryTablesIndexesColumns(index, table, False, schema)
        list_indexes = [index["column_name"] for index in indexes.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_indexes, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_databases(request, database):
    try:
        databases = database.QueryDatabases()
        list_databases = [db[0] for db in databases.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_databases, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_roles(request, database):
    list_roles = []

    try:
        roles = database.QueryRoles()
        for role in roles.Rows:
            role_data = {"name": role["role_name"]}
            list_roles.append(role_data)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_roles, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_functions(request, database):
    schema = request.data["schema"]

    list_functions = []

    try:
        functions = database.QueryFunctions(False, schema)
        for function in functions.Rows:
            function_data = {"name": function["name"], "id": function["id"]}
            list_functions.append(function_data)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_functions, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_function_fields(request, database):
    data = request.data
    function = data["function"]
    schema = data["schema"]

    list_fields = []

    try:
        fields = database.QueryFunctionFields(function, schema)
        for field in fields.Rows:
            field_data = {"name": field["name"], "type": field["type"]}
            list_fields.append(field_data)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_fields, safe=False)


@user_authenticated
@database_required(p_check_timeout=True, p_open_connection=True)
def get_function_definition(request, v_database):
    v_return = create_response_template()

    v_function = request.data["p_function"]

    try:
        v_return["v_data"] = v_database.GetFunctionDefinition(v_function)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_procedures(request, database):
    schema = request.data["schema"]

    list_functions = []

    try:
        functions = database.QueryProcedures(False, schema)
        for function in functions.Rows:
            function_data = {"name": function["name"], "id": function["id"]}
            list_functions.append(function_data)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_functions, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_procedure_fields(request, database):
    data = request.data
    function = data["procedure"]
    schema = data["schema"]

    list_fields = []

    try:
        fields = database.QueryProcedureFields(function, schema)
        for field in fields.Rows:
            field_data = {"name": field["name"], "type": field["type"]}
            list_fields.append(field_data)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_fields, safe=False)


@user_authenticated
@database_required(p_check_timeout=True, p_open_connection=True)
def get_procedure_definition(request, v_database):
    v_return = create_response_template()

    v_function = request.data["p_procedure"]

    try:
        v_return["v_data"] = v_database.GetProcedureDefinition(v_function)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_sequences(request, database):
    schema = request.data.get("schema")

    list_sequences = []

    try:
        sequences = database.QuerySequences(False, schema)
        for sequence in sequences.Rows:
            sequence_data = {"sequence_name": sequence["sequence_name"]}
            list_sequences.append(sequence_data)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_sequences, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
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
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_tables, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
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
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_columns, safe=False)


@user_authenticated
@database_required(p_check_timeout=True, p_open_connection=True)
def get_view_definition(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_view = data["p_view"]
    v_schema = data["p_schema"]

    try:
        v_return["v_data"] = v_database.GetViewDefinition(v_view, v_schema)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)


@user_authenticated
@database_required(p_check_timeout=True, p_open_connection=True)
def kill_backend(request, v_database):
    v_return = create_response_template()

    v_pid = request.data["p_pid"]

    try:
        v_data = v_database.Terminate(v_pid)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)


@user_authenticated
@database_required(p_check_timeout=True, p_open_connection=True)
def template_select(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data["p_table"]
    v_schema = data["p_schema"]

    try:
        v_template = v_database.TemplateSelect(v_schema, v_table).v_text
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return["v_data"] = {"v_template": v_template}

    return JsonResponse(v_return)


@user_authenticated
@database_required(p_check_timeout=True, p_open_connection=True)
def template_insert(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data["p_table"]
    v_schema = data["p_schema"]

    try:
        v_template = v_database.TemplateInsert(v_schema, v_table).v_text
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return["v_data"] = {"v_template": v_template}

    return JsonResponse(v_return)


@user_authenticated
@database_required(p_check_timeout=True, p_open_connection=True)
def template_update(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data["p_table"]
    v_schema = data["p_schema"]

    try:
        v_template = v_database.TemplateUpdate(v_schema, v_table).v_text
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return["v_data"] = {"v_template": v_template}

    return JsonResponse(v_return)
