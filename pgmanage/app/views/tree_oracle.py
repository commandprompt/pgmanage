from django.http import JsonResponse
from app.utils.decorators import (
    user_authenticated,
    database_required,
    database_required_new,
)
from app.utils.response_helpers import create_response_template, error_response


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_tree_info(request, database):
    try:
        data = {
            "database": database.GetName(),
            "version": database.GetVersion(),
            "username": database.GetUserName(),
            "superuser": database.GetUserSuper(),
            "express": database.GetExpress(),
            "create_role": database.TemplateCreateRole().v_text,
            "alter_role": database.TemplateAlterRole().v_text,
            "drop_role": database.TemplateDropRole().v_text,
            "create_tablespace": database.TemplateCreateTablespace().v_text,
            "alter_tablespace": database.TemplateAlterTablespace().v_text,
            "drop_tablespace": database.TemplateDropTablespace().v_text,
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
            #'create_mview': database.TemplateCreateMaterializedView().v_text,
            #'refresh_mview': database.TemplateRefreshMaterializedView().v_text,
            #'drop_mview': database.TemplateDropMaterializedView().v_text,
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
            "alter_index": database.TemplateAlterIndex().v_text,
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
        data = {"password_timeout": True, "data": str(exc)}
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
            data["schema"], data["object"], data["type"]
        )
        for property_object in properties.Rows:
            list_properties.append([property_object["Property"], property_object["Value"]])
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
    try:
        tables = database.QueryTables()
        list_tables = [table["table_name"] for table in tables.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_tables, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
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
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_columns, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_pk(request, database):
    table = request.data["table"]

    try:
        pks = database.QueryTablesPrimaryKeys(table)
        list_pk = [pk["constraint_name"] for pk in pks.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_pk, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_pk_columns(request, database):
    data = request.data
    pkey = data["key"]
    table = data["table"]

    try:
        pks = database.QueryTablesPrimaryKeysColumns(pkey, table)
        list_pk = [row["column_name"] for row in pks.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_pk, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_fks(request, database):
    table = request.data["table"]

    try:
        fks = database.QueryTablesForeignKeys(table)
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

    try:
        fks = database.QueryTablesForeignKeysColumns(fkey, table)
        fk = fks.Rows.pop() if fks.Rows else {}
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=fk)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_uniques(request, database):
    table = request.data["table"]

    try:
        uniques = database.QueryTablesUniques(table)
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

    try:
        uniques = database.QueryTablesUniquesColumns(unique, table)
        list_uniques = [unique["column_name"] for unique in uniques.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_uniques, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
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
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_indexes, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_indexes_columns(request, database):
    data = request.data
    index = data["index"]
    table = data["table"]

    try:
        indexes = database.QueryTablesIndexesColumns(index, table)
        list_indexes = [index["column_name"] for index in indexes.Rows]
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_indexes, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_tablespaces(request, database):
    list_tablespaces = []

    try:
        tablespaces = database.QueryTablespaces()
        for tablespace in tablespaces.Rows:
            tablespace_data = {"name": tablespace["tablespace_name"]}
            list_tablespaces.append(tablespace_data)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_tablespaces, safe=False)


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
@database_required(p_check_timeout=True, p_open_connection=True)
def get_packages(request, v_database):
    v_return = create_response_template()

    v_schema = request.data["p_schema"]

    v_list_packages = []

    try:
        v_packages = v_database.QueryPackages(False, v_schema)
        for v_package in v_packages.Rows:
            v_package_data = {"v_name": v_package["name"], "v_id": v_package["id"]}
            v_package_functions.append(v_package_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return["v_data"] = v_list_packages

    return JsonResponse(v_return)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_functions(request, database):
    list_functions = []

    try:
        functions = database.QueryFunctions()
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
@database_required_new(check_timeout=True, open_connection=True)
def get_function_definition(request, database):
    function = request.data["function"]

    try:
        function_definition = database.GetFunctionDefinition(function)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse({"data": function_definition})


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_procedures(request, database):
    list_functions = []

    try:
        functions = database.QueryProcedures()
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
@database_required_new(check_timeout=True, open_connection=True)
def get_procedure_definition(request, database):
    function = request.data["procedure"]

    try:
        procedure_definition = database.GetProcedureDefinition(function)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse({"data": procedure_definition})


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_sequences(request, database):
    list_sequences = []

    try:
        sequences = database.QuerySequences()
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
    list_tables = []

    try:
        tables = database.QueryViews()
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
    table = request.data["table"]

    list_columns = []

    try:
        columns = database.QueryViewFields(table)
        for column in columns.Rows:
            column_data = {
                "column_name": column["column_name"],
                "data_type": column["data_type"],
                "data_length": column["data_length"],
            }
            list_columns.append(column_data)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_columns, safe=False)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_view_definition(request, database):
    data = request.data
    view = data["view"]
    schema = data["schema"]

    try:
        view_definition = database.GetViewDefinition(view, schema)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse({"data": view_definition})


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
