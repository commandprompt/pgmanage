from django.http import JsonResponse

from app.utils.decorators import database_required, database_required_new, user_authenticated
from app.utils.response_helpers import create_response_template, error_response

@user_authenticated
@database_required_new(check_timeout = True, open_connection = True)
def get_tree_info(request, database):
    try:
        data = {
                'database': database.GetName(),
                'version': database.GetVersion(),
                #'superuser': database.GetUserSuper(),
                'create_role': database.TemplateCreateRole().v_text,
                'alter_role': database.TemplateAlterRole().v_text,
                'drop_role': database.TemplateDropRole().v_text,
                'create_tablespace': database.TemplateCreateTablespace().v_text,
                'alter_tablespace': database.TemplateAlterTablespace().v_text,
                'drop_tablespace': database.TemplateDropTablespace().v_text,
                'create_database': database.TemplateCreateDatabase().v_text,
                'alter_database': database.TemplateAlterDatabase().v_text,
                'drop_database': database.TemplateDropDatabase().v_text,
                'create_extension': database.TemplateCreateExtension().v_text,
                'alter_extension': database.TemplateAlterExtension().v_text,
                'drop_extension': database.TemplateDropExtension().v_text,
                'create_schema': database.TemplateCreateSchema().v_text,
                'alter_schema': database.TemplateAlterSchema().v_text,
                'drop_schema': database.TemplateDropSchema().v_text,
                'create_sequence': database.TemplateCreateSequence().v_text,
                'alter_sequence': database.TemplateAlterSequence().v_text,
                'drop_sequence': database.TemplateDropSequence().v_text,
                'create_function': database.TemplateCreateFunction().v_text,
                'alter_function': database.TemplateAlterFunction().v_text,
                'drop_function': database.TemplateDropFunction().v_text,
                'create_procedure': database.TemplateCreateProcedure().v_text,
                'alter_procedure': database.TemplateAlterProcedure().v_text,
                'drop_procedure': database.TemplateDropProcedure().v_text,
                'create_triggerfunction': database.TemplateCreateTriggerFunction().v_text,
                'alter_triggerfunction': database.TemplateAlterTriggerFunction().v_text,
                'drop_triggerfunction': database.TemplateDropTriggerFunction().v_text,
                'create_eventtriggerfunction': database.TemplateCreateEventTriggerFunction().v_text,
                'alter_eventtriggerfunction': database.TemplateAlterEventTriggerFunction().v_text,
                'drop_eventtriggerfunction': database.TemplateDropEventTriggerFunction().v_text,
                'create_aggregate': database.TemplateCreateAggregate().v_text,
                'alter_aggregate': database.TemplateAlterAggregate().v_text,
                'drop_aggregate': database.TemplateDropAggregate().v_text,
                'create_view': database.TemplateCreateView().v_text,
                'alter_view': database.TemplateAlterView().v_text,
                'drop_view': database.TemplateDropView().v_text,
                'create_mview': database.TemplateCreateMaterializedView().v_text,
                'refresh_mview': database.TemplateRefreshMaterializedView().v_text,
                'alter_mview': database.TemplateAlterMaterializedView().v_text,
                'drop_mview': database.TemplateDropMaterializedView().v_text,
                'create_table': database.TemplateCreateTable().v_text,
                'alter_table': database.TemplateAlterTable().v_text,
                'drop_table': database.TemplateDropTable().v_text,
                'create_column': database.TemplateCreateColumn().v_text,
                'alter_column': database.TemplateAlterColumn().v_text,
                'drop_column': database.TemplateDropColumn().v_text,
                'create_primarykey': database.TemplateCreatePrimaryKey().v_text,
                'drop_primarykey': database.TemplateDropPrimaryKey().v_text,
                'create_unique': database.TemplateCreateUnique().v_text,
                'drop_unique': database.TemplateDropUnique().v_text,
                'create_foreignkey': database.TemplateCreateForeignKey().v_text,
                'drop_foreignkey': database.TemplateDropForeignKey().v_text,
                'create_index': database.TemplateCreateIndex().v_text,
                'alter_index': database.TemplateAlterIndex().v_text,
                'cluster_index': database.TemplateClusterIndex().v_text,
                'reindex': database.TemplateReindex().v_text,
                'drop_index': database.TemplateDropIndex().v_text,
                'create_check': database.TemplateCreateCheck().v_text,
                'drop_check': database.TemplateDropCheck().v_text,
                'create_exclude': database.TemplateCreateExclude().v_text,
                'drop_exclude': database.TemplateDropExclude().v_text,
                'create_rule': database.TemplateCreateRule().v_text,
                'alter_rule': database.TemplateAlterRule().v_text,
                'drop_rule': database.TemplateDropRule().v_text,
                'create_trigger': database.TemplateCreateTrigger().v_text,
                'create_view_trigger': database.TemplateCreateViewTrigger().v_text,
                'alter_trigger': database.TemplateAlterTrigger().v_text,
                'enable_trigger': database.TemplateEnableTrigger().v_text,
                'disable_trigger': database.TemplateDisableTrigger().v_text,
                'drop_trigger': database.TemplateDropTrigger().v_text,
                'create_eventtrigger': database.TemplateCreateEventTrigger().v_text,
                'alter_eventtrigger': database.TemplateAlterEventTrigger().v_text,
                'enable_eventtrigger': database.TemplateEnableEventTrigger().v_text,
                'disable_eventtrigger': database.TemplateDisableEventTrigger().v_text,
                'drop_eventtrigger': database.TemplateDropEventTrigger().v_text,
                'create_inherited': database.TemplateCreateInherited().v_text,
                'noinherit_partition': database.TemplateNoInheritPartition().v_text,
                'create_partition': database.TemplateCreatePartition().v_text,
                'detach_partition': database.TemplateDetachPartition().v_text,
                'drop_partition': database.TemplateDropPartition().v_text,
                'vacuum': database.TemplateVacuum().v_text,
                'vacuum_table': database.TemplateVacuumTable().v_text,
                'analyze': database.TemplateAnalyze().v_text,
                'analyze_table': database.TemplateAnalyzeTable().v_text,
                'delete': database.TemplateDelete().v_text,
                'truncate': database.TemplateTruncate().v_text,
                'create_physicalreplicationslot': database.TemplateCreatePhysicalReplicationSlot().v_text,
                'drop_physicalreplicationslot': database.TemplateDropPhysicalReplicationSlot().v_text,
                'create_logicalreplicationslot': database.TemplateCreateLogicalReplicationSlot().v_text,
                'drop_logicalreplicationslot': database.TemplateDropLogicalReplicationSlot().v_text,
                'create_publication': database.TemplateCreatePublication().v_text,
                'alter_publication': database.TemplateAlterPublication().v_text,
                'drop_publication': database.TemplateDropPublication().v_text,
                'add_pubtable': database.TemplateAddPublicationTable().v_text,
                'drop_pubtable': database.TemplateDropPublicationTable().v_text,
                'create_subscription': database.TemplateCreateSubscription().v_text,
                'alter_subscription': database.TemplateAlterSubscription().v_text,
                'drop_subscription': database.TemplateDropSubscription().v_text,
                'create_fdw': database.TemplateCreateForeignDataWrapper().v_text,
                'alter_fdw': database.TemplateAlterForeignDataWrapper().v_text,
                'drop_fdw': database.TemplateDropForeignDataWrapper().v_text,
                'create_foreign_server': database.TemplateCreateForeignServer().v_text,
                'alter_foreign_server': database.TemplateAlterForeignServer().v_text,
                'import_foreign_schema': database.TemplateImportForeignSchema().v_text,
                'drop_foreign_server': database.TemplateDropForeignServer().v_text,
                'create_foreign_table': database.TemplateCreateForeignTable().v_text,
                'alter_foreign_table': database.TemplateAlterForeignTable().v_text,
                'drop_foreign_table': database.TemplateDropForeignTable().v_text,
                'create_foreign_column': database.TemplateCreateForeignColumn().v_text,
                'alter_foreign_column': database.TemplateAlterForeignColumn().v_text,
                'drop_foreign_column': database.TemplateDropForeignColumn().v_text,
                'create_user_mapping': database.TemplateCreateUserMapping().v_text,
                'alter_user_mapping': database.TemplateAlterUserMapping().v_text,
                'drop_user_mapping': database.TemplateDropUserMapping().v_text,
                'create_type': database.TemplateCreateType().v_text,
                'alter_type': database.TemplateAlterType().v_text,
                'drop_type': database.TemplateDropType().v_text,
                'create_domain': database.TemplateCreateDomain().v_text,
                'alter_domain': database.TemplateAlterDomain().v_text,
                'drop_domain': database.TemplateDropDomain().v_text,
                'create_statistics': database.TemplateCreateStatistics().v_text,
                'alter_statistics': database.TemplateAlterStatistics().v_text,
                'drop_statistics': database.TemplateDropStatistics().v_text,
            }
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=data)


@user_authenticated
@database_required_new(check_timeout = True, open_connection = True)
def get_database_objects(request, database):
    unsupported_versions = ['1.0', '1.1', '1.2', '1.3']
    version_filter = lambda extension: extension[0] == 'pg_cron' and extension[2] not in unsupported_versions

    try:
        extensions = database.QueryExtensions().Rows
        has_pg_cron = len(list(filter(version_filter, extensions))) > 0
        data = {'has_pg_cron': has_pg_cron}
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=data)


@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_properties(request, v_database):
    v_return = create_response_template()

    v_data = request.data['p_data']

    v_list_properties = []
    v_ddl = ''

    try:
        v_properties = v_database.GetProperties(v_data['p_schema'],v_data['p_table'],v_data['p_object'],v_data['p_type'])
        for v_property in v_properties.Rows:
            v_list_properties.append([v_property['Property'],v_property['Value']])
        v_ddl = v_database.GetDDL(v_data['p_schema'],v_data['p_table'],v_data['p_object'],v_data['p_type'])
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = {
        'properties': v_list_properties,
        'ddl': v_ddl
    }

    return JsonResponse(v_return)

@user_authenticated
@database_required_new(check_timeout = True, open_connection = True)
def get_tables(request, database):
    schema = request.data['schema']

    list_tables = []

    try:
        tables = database.QueryTables(False,schema)
        for table in tables.Rows:
            table_data = {
                'name': table['table_name'],
                'oid': table['oid']
            }
            list_tables.append(table_data)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_tables, safe=False)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_columns(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_columns = []

    try:
        v_columns = v_database.QueryTablesFields(v_table,False,v_schema)
        for v_column in v_columns.Rows:
            v_column_data = {
                'v_column_name': v_column['column_name'],
                'v_data_type': v_column['data_type'],
                'v_data_length': v_column['data_length'],
                'v_nullable': v_column['nullable'],
                'v_position': v_column['position']
            }
            v_list_columns.append(v_column_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_columns

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_pk(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_pk = []

    try:
        v_pks = v_database.QueryTablesPrimaryKeys(v_table, False, v_schema)
        for v_pk in v_pks.Rows:
            v_pk_data = []
            v_pk_data.append(v_pk['constraint_name'])
            v_pk_data.append(v_pk['oid'])
            v_list_pk.append(v_pk_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_pk

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_pk_columns(request, v_database):
    v_return = create_response_template()
    
    data = request.data
    v_pkey = data['p_key']
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_pk = []

    try:
        v_pks = v_database.QueryTablesPrimaryKeysColumns(v_pkey, v_table, False, v_schema)
        for v_pk in v_pks.Rows:
            v_pk_data = []
            v_pk_data.append(v_pk['column_name'])
            v_list_pk.append(v_pk_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_pk

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_fks(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_fk = []

    try:
        v_fks = v_database.QueryTablesForeignKeys(v_table, False, v_schema)
        for v_fk in v_fks.Rows:
            v_fk_data = []
            v_fk_data.append(v_fk['constraint_name'])
            v_fk_data.append(v_fk['r_table_name'])
            v_fk_data.append(v_fk['delete_rule'])
            v_fk_data.append(v_fk['update_rule'])
            v_fk_data.append(v_fk['oid'])
            v_list_fk.append(v_fk_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_fk

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_fks_columns(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_fkey = data['p_fkey']
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_fk = []

    try:
        v_fks = v_database.QueryTablesForeignKeysColumns(v_fkey, v_table, False, v_schema)
        for v_fk in v_fks.Rows:
            v_fk_data = []
            v_fk_data.append(v_fk['r_table_name'])
            v_fk_data.append(v_fk['delete_rule'])
            v_fk_data.append(v_fk['update_rule'])
            v_fk_data.append(v_fk['column_name'])
            v_fk_data.append(v_fk['r_column_name'])
            v_list_fk.append(v_fk_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_fk

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_uniques(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_uniques = []

    try:
        v_uniques = v_database.QueryTablesUniques(v_table, False, v_schema)
        for v_unique in v_uniques.Rows:
            v_unique_data = []
            v_unique_data.append(v_unique['constraint_name'])
            v_unique_data.append(v_unique['oid'])
            v_list_uniques.append(v_unique_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_uniques

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_uniques_columns(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_unique = data['p_unique']
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_uniques = []

    try:
        v_uniques = v_database.QueryTablesUniquesColumns(v_unique, v_table, False, v_schema)
        for v_unique in v_uniques.Rows:
            v_unique_data = []
            v_unique_data.append(v_unique['column_name'])
            v_list_uniques.append(v_unique_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_uniques

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_indexes(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_indexes = []

    try:
        v_indexes = v_database.QueryTablesIndexes(v_table, False, v_schema)
        for v_index in v_indexes.Rows:
            v_index_data = []
            v_index_data.append(v_index['index_name'])
            v_index_data.append(v_index['uniqueness'])
            v_index_data.append(v_index['oid'])
            v_list_indexes.append(v_index_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_indexes

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_indexes_columns(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_index = data['p_index']
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_indexes = []

    try:
        v_indexes = v_database.QueryTablesIndexesColumns(v_index, v_table, False, v_schema)
        for v_index in v_indexes.Rows:
            v_index_data = []
            v_index_data.append(v_index['column_name'])
            v_list_indexes.append(v_index_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_indexes

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_checks(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_checks = []

    try:
        v_checks = v_database.QueryTablesChecks(v_table,False,v_schema)
        for v_check in v_checks.Rows:
            v_check_data = []
            v_check_data.append(v_check['constraint_name'])
            v_check_data.append(v_check['constraint_source'])
            v_check_data.append(v_check['oid'])
            v_list_checks.append(v_check_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_checks

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_excludes(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_excludes = []

    try:
        v_excludes = v_database.QueryTablesExcludes(v_table,False,v_schema)
        for v_exclude in v_excludes.Rows:
            v_exclude_data = []
            v_exclude_data.append(v_exclude['constraint_name'])
            v_exclude_data.append(v_exclude['attributes'])
            v_exclude_data.append(v_exclude['operations'])
            v_exclude_data.append(v_exclude['oid'])
            v_list_excludes.append(v_exclude_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_excludes

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_rules(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_rules = []

    try:
        v_rules = v_database.QueryTablesRules(v_table,False,v_schema)
        for v_rule in v_rules.Rows:
            v_rule_data = []
            v_rule_data.append(v_rule['rule_name'])
            v_rule_data.append(v_rule['oid'])
            v_list_rules.append(v_rule_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_rules

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_rule_definition(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_rule = data['p_rule']
    v_table = data['p_table']
    v_schema = data['p_schema']

    try:
        v_return['v_data'] = v_database.GetRuleDefinition(v_rule, v_table, v_schema)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_triggers(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_triggers = []

    try:
        v_triggers = v_database.QueryTablesTriggers(v_table,False,v_schema)
        for v_trigger in v_triggers.Rows:
            v_trigger_data = {
                'v_name': v_trigger['trigger_name'],
                'v_enabled': v_trigger['trigger_enabled'],
                'v_function': v_trigger['trigger_function'],
                'v_id': v_trigger['id'],
                'v_function_oid': v_trigger['function_oid'],
                'v_oid': v_trigger['oid']
            }
            v_list_triggers.append(v_trigger_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_triggers

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_eventtriggers(request, v_database):
    v_return = create_response_template()

    v_list_triggers = []

    try:
        v_triggers = v_database.QueryEventTriggers()
        for v_trigger in v_triggers.Rows:
            v_trigger_data = {
                'v_name': v_trigger['trigger_name'],
                'v_enabled': v_trigger['trigger_enabled'],
                'v_event': v_trigger['event_name'],
                'v_function': v_trigger['trigger_function'],
                'v_id': v_trigger['id'],
                'v_function_oid': v_trigger['function_oid'],
                'v_oid': v_trigger['oid']
            }
            v_list_triggers.append(v_trigger_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_triggers

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_inheriteds(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_partitions = []

    try:
        v_partitions = v_database.QueryTablesInheriteds(v_table,False,v_schema)
        for v_partition in v_partitions.Rows:
            v_partition_data = []
            v_partition_data.append(v_partition['child_schema'] + '.' + v_partition['child_table'])
            v_list_partitions.append(v_partition_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_partitions

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_partitions(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_partitions = []

    try:
        v_partitions = v_database.QueryTablesPartitions(v_table,False,v_schema)
        for v_partition in v_partitions.Rows:
            v_partition_data = []
            v_partition_data.append(v_partition['child_schema'] + '.' + v_partition['child_table'])
            v_list_partitions.append(v_partition_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_partitions

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_statistics(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_statistics = []

    try:
        v_statistics = v_database.QueryTablesStatistics(v_table, False, v_schema)
        for v_statistic in v_statistics.Rows:
            v_statistic_data = []
            v_statistic_data.append(v_statistic['statistic_name'])
            v_statistic_data.append(v_statistic['schema_name'])
            v_statistic_data.append(v_statistic['oid'])
            v_list_statistics.append(v_statistic_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_statistics

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_statistics_columns(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_statistics = data['p_statistics']
    v_schema = data['p_schema']

    v_list_columns = []

    try:
        v_columns = v_database.QueryStatisticsFields(v_statistics,False,v_schema)
        for v_column in v_columns.Rows:
            v_column_data = {
                'v_column_name': v_column['column_name']
            }
            v_list_columns.append(v_column_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_columns

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_views(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_tables = []

    try:
        v_tables = v_database.QueryViews(False,v_schema)
        for v_table in v_tables.Rows:
            v_table_data = {
                'v_name': v_table['table_name'],
                'v_has_rules': v_database.v_has_rules,
                'v_has_triggers': v_database.v_has_triggers,
                'v_oid': v_table['oid']
            }
            v_list_tables.append(v_table_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_tables

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_views_columns(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_columns = []

    try:
        v_columns = v_database.QueryViewFields(v_table,False,v_schema)
        for v_column in v_columns.Rows:
            v_column_data = {
                'v_column_name': v_column['column_name'],
                'v_data_type': v_column['data_type'],
                'v_data_length': v_column['data_length'],
            }
            v_list_columns.append(v_column_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_columns

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_view_definition(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_view = data['p_view']
    v_schema = data['p_schema']

    try:
        v_return['v_data'] = v_database.GetViewDefinition(v_view, v_schema)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_mviews(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_tables = []

    try:
        v_tables = v_database.QueryMaterializedViews(False,v_schema)
        for v_table in v_tables.Rows:
            v_table_data = {
                'v_name': v_table['table_name'],
                'v_has_indexes': v_database.v_has_indexes,
                'v_has_statistics': v_database.v_has_statistics,
                'v_oid': v_table['oid']
            }
            v_list_tables.append(v_table_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_tables

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_mviews_columns(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_columns = []

    try:
        v_columns = v_database.QueryMaterializedViewFields(v_table,False,v_schema)
        for v_column in v_columns.Rows:
            v_column_data = {
                'v_column_name': v_column['column_name'],
                'v_data_type': v_column['data_type'],
                'v_data_length': v_column['data_length'],
            }
            v_list_columns.append(v_column_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_columns

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_mview_definition(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_view = data['p_view']
    v_schema = data['p_schema']

    try:
        v_return['v_data'] = v_database.GetMaterializedViewDefinition(v_view, v_schema)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@database_required_new(check_timeout = True, open_connection = True)
def get_schemas(request, database):
    schemas_list = []

    try:
        schemas = database.QuerySchemas()
        for schema in schemas.Rows:
            schema_data = {
                'name': schema['schema_name'],
                'oid': schema['oid']
            }
            schemas_list.append(schema_data)
    except Exception as exc:
        return JsonResponse(data={'data': str(exc)}, status=500)
    return JsonResponse(data=schemas_list, safe=False)

@user_authenticated
@database_required_new(check_timeout = True, open_connection = True)
def get_databases(request, database):
    list_databases = []

    try:
        databases = database.QueryDatabases()
        for database_object in databases.Rows:
            database_data = {
                'name': database_object['database_name'],
                'oid': database_object['oid']
            }
            list_databases.append(database_data)
    except Exception as exc:
        data = {"password_timeout": True, "data": str(exc)}
        return JsonResponse(data=data, status=500)

    return JsonResponse(data=list_databases, safe=False)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_tablespaces(request, v_database):
    v_return = create_response_template()

    v_list_tablespaces = []

    try:
        v_tablespaces = v_database.QueryTablespaces()
        for v_tablespace in v_tablespaces.Rows:
            v_tablespace_data = {
                'v_name': v_tablespace['tablespace_name'],
                'v_oid': v_tablespace['oid']
            }
            v_list_tablespaces.append(v_tablespace_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_tablespaces

    return JsonResponse(v_return)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_roles(request, database):
    list_roles = []
    try:
        roles = database.QueryRoles()
        for role in roles.Rows:
            role_data = {
                'name': role['role_name'],
                'oid': role['oid']
            }
            list_roles.append(role_data)
    except Exception as exc:
        return JsonResponse(data={'data': str(exc)}, status=400)
    return JsonResponse(data={"data": list_roles})


@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_functions(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_functions = []

    try:
        v_functions = v_database.QueryFunctions(False,v_schema)
        for v_function in v_functions.Rows:
            v_function_data = {
                'v_name': v_function['name'],
                'v_id': v_function['id'],
                'v_function_oid': v_function['function_oid']
            }
            v_list_functions.append(v_function_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_functions

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_function_fields(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_function = data['p_function']
    v_schema = data['p_schema']

    v_list_fields = []

    try:
        v_fields = v_database.QueryFunctionFields(v_function,v_schema)
        for v_field in v_fields.Rows:
            v_field_data = {
                'v_name': v_field['name'],
                'v_type': v_field['type']
            }
            v_list_fields.append(v_field_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_fields

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_function_definition(request, v_database):
    v_return = create_response_template()

    v_function = request.data['p_function']

    try:
        v_return['v_data'] = v_database.GetFunctionDefinition(v_function)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_function_debug(request, v_database):
    v_return = create_response_template()

    v_function = request.data['p_function']

    try:
        v_return['v_data'] = v_database.GetFunctionDebug(v_function)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_procedures(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_functions = []

    try:
        v_functions = v_database.QueryProcedures(False,v_schema)
        for v_function in v_functions.Rows:
            v_function_data = {
                'v_name': v_function['name'],
                'v_id': v_function['id'],
                'v_function_oid': v_function['function_oid']
            }
            v_list_functions.append(v_function_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_functions

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_procedure_fields(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_function = data['p_procedure']
    v_schema = data['p_schema']

    v_list_fields = []

    try:
        v_fields = v_database.QueryProcedureFields(v_function,v_schema)
        for v_field in v_fields.Rows:
            v_field_data = {
                'v_name': v_field['name'],
                'v_type': v_field['type']
            }
            v_list_fields.append(v_field_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_fields

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_procedure_definition(request, v_database):
    v_return = create_response_template()

    v_function = request.data['p_procedure']

    try:
        v_return['v_data'] = v_database.GetProcedureDefinition(v_function)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_procedure_debug(request, v_database):
    v_return = create_response_template()

    v_function = request.data['p_procedure']

    try:
        v_return['v_data'] = v_database.GetProcedureDebug(v_function)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_triggerfunctions(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_functions = []

    try:
        v_functions = v_database.QueryTriggerFunctions(False,v_schema)
        for v_function in v_functions.Rows:
            v_function_data = {
                'v_name': v_function['name'],
                'v_id': v_function['id'],
                'v_function_oid': v_function['function_oid']
            }
            v_list_functions.append(v_function_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_functions

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_triggerfunction_definition(request, v_database):
    v_return = create_response_template()

    v_function = request.data['p_function']

    try:
        v_return['v_data'] = v_database.GetTriggerFunctionDefinition(v_function)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_eventtriggerfunctions(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_functions = []

    try:
        v_functions = v_database.QueryEventTriggerFunctions(False,v_schema)
        for v_function in v_functions.Rows:
            v_function_data = {
                'v_name': v_function['name'],
                'v_id': v_function['id'],
                'v_function_oid': v_function['function_oid']
            }
            v_list_functions.append(v_function_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_functions

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_eventtriggerfunction_definition(request, v_database):
    v_return = create_response_template()

    v_function = request.data['p_function']

    try:
        v_return['v_data'] = v_database.GetEventTriggerFunctionDefinition(v_function)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_aggregates(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_aggregates = []

    try:
        v_aggregates = v_database.QueryAggregates(False,v_schema)
        for v_aggregate in v_aggregates.Rows:
            v_aggregate_data = {
                'v_name': v_aggregate['name'],
                'v_id': v_aggregate['id'],
                'v_oid': v_aggregate['oid']
            }
            v_list_aggregates.append(v_aggregate_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_aggregates

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_sequences(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_sequences = []

    try:
        v_sequences = v_database.QuerySequences(False,v_schema)
        for v_sequence in v_sequences.Rows:
            v_sequence_data = {
                'v_sequence_name': v_sequence['sequence_name'],
                'v_oid': v_sequence['oid']
            }
            v_list_sequences.append(v_sequence_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_sequences

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_extensions(request, v_database):
    v_return = create_response_template()

    v_list_extensions = []

    try:
        v_extensions = v_database.QueryExtensions()
        for v_extension in v_extensions.Rows:
            v_extension_data = {
                'v_name': v_extension['extension_name'],
                'v_oid': v_extension['oid'],
                'v_version': v_extension['extversion']
            }
            v_list_extensions.append(v_extension_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_extensions

    return JsonResponse(v_return)


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_available_extensions(request, database):
    available_extensions = database.QueryAvailableExtensionsVersions()

    list_ext = []

    for extension in available_extensions.Rows:
        extension_data = {
            "name": extension["name"],
            "versions": extension["versions"],
            "comment": extension["comment"],
            "required_schema": extension["required_schema"]
            }
        list_ext.append(extension_data)
    return JsonResponse({"available_extensions": list_ext})


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_extension_details(request, database):
    data = request.data

    extension = database.QueryExtensionByName(data.get("ext_name"))

    if not extension.Rows:
        return JsonResponse({
            "data": f"Extension '{data.get('ext_name')}' does not exist."
            }, status=400)

    [extension_detail] = extension.Rows

    return JsonResponse(data=dict(extension_detail))


@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def save_extension(request, database):
    data = request.data
    try:
        database.Execute(data.get("query"))
    except Exception as exc:
        return JsonResponse({"data": str(exc)}, status=500)
    return JsonResponse({"status": "success"})


@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_physicalreplicationslots(request, v_database):
    v_return = create_response_template()

    v_list_repslots = []

    try:
        v_repslots = v_database.QueryPhysicalReplicationSlots()
        for v_repslot in v_repslots.Rows:
            v_repslot_data = {
                'v_name': v_repslot['slot_name']
            }
            v_list_repslots.append(v_repslot_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_repslots

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_logicalreplicationslots(request, v_database):
    v_return = create_response_template()

    v_list_repslots = []

    try:
        v_repslots = v_database.QueryLogicalReplicationSlots()
        for v_repslot in v_repslots.Rows:
            v_repslot_data = {
                'v_name': v_repslot['slot_name']
            }
            v_list_repslots.append(v_repslot_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_repslots

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_publications(request, v_database):
    v_return = create_response_template()

    v_list_pubs = []

    try:
        v_pubs = v_database.QueryPublications()
        for v_pub in v_pubs.Rows:
            v_pub_data = {
                'v_name': v_pub['pubname'],
                'v_alltables': v_pub['puballtables'],
                'v_insert': v_pub['pubinsert'],
                'v_update': v_pub['pubupdate'],
                'v_delete': v_pub['pubdelete'],
                'v_truncate': v_pub['pubtruncate'],
                'v_oid': v_pub['oid']
            }
            v_list_pubs.append(v_pub_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_pubs

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_publication_tables(request, v_database):
    v_return = create_response_template()

    v_pub = request.data['p_pub']

    v_list_tables = []

    try:
        v_tables = v_database.QueryPublicationTables(v_pub)
        for v_table in v_tables.Rows:
            v_table_data = {
                'v_name': v_table['table_name']
            }
            v_list_tables.append(v_table_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_tables

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_subscriptions(request, v_database):
    v_return = create_response_template()

    v_list_subs = []

    try:
        v_subs = v_database.QuerySubscriptions()
        for v_sub in v_subs.Rows:
            v_sub_data = {
                'v_name': v_sub['subname'],
                'v_enabled': v_sub['subenabled'],
                'v_conninfo': v_sub['subconninfo'],
                'v_publications': v_sub['subpublications'],
                'v_oid': v_sub['oid']
            }
            v_list_subs.append(v_sub_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_subs

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_subscription_tables(request, v_database):
    v_return = create_response_template()

    v_sub = request.data['p_sub']

    v_list_tables = []

    try:
        v_tables = v_database.QuerySubscriptionTables(v_sub)
        for v_table in v_tables.Rows:
            v_table_data = {
                'v_name': v_table['table_name']
            }
            v_list_tables.append(v_table_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_tables

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_foreign_data_wrappers(request, v_database):
    v_return = create_response_template()

    v_list_fdws = []

    try:
        v_fdws = v_database.QueryForeignDataWrappers()
        for v_fdw in v_fdws.Rows:
            v_fdw_data = {
                'v_name': v_fdw['fdwname'],
                'v_oid': v_fdw['oid']
            }
            v_list_fdws.append(v_fdw_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_fdws

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_foreign_servers(request, v_database):
    v_return = create_response_template()

    v_fdw = request.data['p_fdw']

    v_list_servers = []

    try:
        v_servers = v_database.QueryForeignServers(v_fdw)
        for v_server in v_servers.Rows:
            v_server_data = {
                'v_name': v_server['srvname'],
                'v_type': v_server['srvtype'],
                'v_version': v_server['srvversion'],
                'v_options': v_server['srvoptions'],
                'v_oid': v_server['oid']
            }
            v_list_servers.append(v_server_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_servers

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_user_mappings(request, v_database):
    v_return = create_response_template()

    v_foreign_server = request.data['p_foreign_server']

    v_list_mappings = []

    try:
        v_mappings = v_database.QueryUserMappings(v_foreign_server)
        for v_mapping in v_mappings.Rows:
            v_mapping_data = {
                'v_name': v_mapping['rolname'],
                'v_options': v_mapping['umoptions'],
                'v_foreign_server': v_foreign_server
            }
            v_list_mappings.append(v_mapping_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_mappings

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_foreign_tables(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_tables = []

    try:
        v_tables = v_database.QueryForeignTables(False,v_schema)
        for v_table in v_tables.Rows:
            v_table_data = {
                'v_name': v_table['table_name'],
                'v_has_statistics': v_database.v_has_statistics,
                'v_oid': v_table['oid']
            }
            v_list_tables.append(v_table_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_tables

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_foreign_columns(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_columns = []

    try:
        v_columns = v_database.QueryForeignTablesFields(v_table,False,v_schema)
        for v_column in v_columns.Rows:
            v_column_data = {
                'v_column_name': v_column['column_name'],
                'v_data_type': v_column['data_type'],
                'v_data_length': v_column['data_length'],
                'v_nullable': v_column['nullable'],
                'v_options': v_column['attfdwoptions'],
                'v_tableoptions': v_column['ftoptions'],
                'v_server': v_column['srvname'],
                'v_fdw': v_column['fdwname']
            }
            v_list_columns.append(v_column_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_columns

    return JsonResponse(v_return)


@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_types(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_types = []

    try:
        v_types = v_database.QueryTypes(False,v_schema)
        for v_type in v_types.Rows:
            v_type_data = {
                'v_type_name': v_type['type_name'],
                'v_oid': v_type['oid']
            }
            v_list_types.append(v_type_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_types

    return JsonResponse(v_return)


@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_domains(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_domains = []

    try:
        v_domains = v_database.QueryDomains(False,v_schema)
        for v_domain in v_domains.Rows:
            v_domain_data = {
                'v_domain_name': v_domain['domain_name'],
                'v_oid': v_domain['oid']
            }
            v_list_domains.append(v_domain_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_domains

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_inheriteds_parents(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_tables = []

    try:
        v_tables = v_database.QueryTablesInheritedsParents(False,v_schema)
        for v_table in v_tables.Rows:
            v_table_data = {
                'v_name': v_table['table_schema'] + '.' + v_table['table_name']
            }
            v_list_tables.append(v_table_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_tables

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_inheriteds_children(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_tables = []

    try:
        v_tables = v_database.QueryTablesInheritedsChildren(v_table,v_schema)
        for v_table in v_tables.Rows:
            v_table_data = {
                'v_name': v_table['table_name'],
                'v_has_primary_keys': v_database.v_has_primary_keys,
                'v_has_foreign_keys': v_database.v_has_foreign_keys,
                'v_has_uniques': v_database.v_has_uniques,
                'v_has_indexes': v_database.v_has_indexes,
                'v_has_checks': v_database.v_has_checks,
                'v_has_excludes': v_database.v_has_excludes,
                'v_has_rules': v_database.v_has_rules,
                'v_has_triggers': v_database.v_has_triggers,
                'v_has_partitions': v_database.v_has_partitions,
                'v_has_statistics': v_database.v_has_statistics,
                'v_oid': v_table['oid']
            }
            v_list_tables.append(v_table_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_tables

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_partitions_parents(request, v_database):
    v_return = create_response_template()

    v_schema = request.data['p_schema']

    v_list_tables = []

    try:
        v_tables = v_database.QueryTablesPartitionsParents(False,v_schema)
        for v_table in v_tables.Rows:
            v_table_data = {
                'v_name': v_table['table_schema'] + '.' + v_table['table_name']
            }
            v_list_tables.append(v_table_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_tables

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_partitions_children(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    v_list_tables = []

    try:
        v_tables = v_database.QueryTablesPartitionsChildren(v_table,v_schema)
        for v_table in v_tables.Rows:
            v_table_data = {
                'v_name': v_table['table_name'],
                'v_has_primary_keys': v_database.v_has_primary_keys,
                'v_has_foreign_keys': v_database.v_has_foreign_keys,
                'v_has_uniques': v_database.v_has_uniques,
                'v_has_indexes': v_database.v_has_indexes,
                'v_has_checks': v_database.v_has_checks,
                'v_has_excludes': v_database.v_has_excludes,
                'v_has_rules': v_database.v_has_rules,
                'v_has_triggers': v_database.v_has_triggers,
                'v_has_partitions': v_database.v_has_partitions,
                'v_has_statistics': v_database.v_has_statistics,
                'v_oid': v_table['oid']
            }
            v_list_tables.append(v_table_data)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = v_list_tables

    return JsonResponse(v_return)


@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def kill_backend(request, v_database):
    v_return = create_response_template()

    v_pid = request.data['p_pid']

    try:
        v_database.Terminate(v_pid)
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def template_select(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']
    v_kind = data['p_kind']

    try:
        v_template = v_database.TemplateSelect(v_schema, v_table, v_kind).v_text
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = {
        'v_template': v_template
    }

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def template_insert(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    try:
        v_template = v_database.TemplateInsert(v_schema, v_table).v_text
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = {
        'v_template': v_template
    }

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def template_update(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_table = data['p_table']
    v_schema = data['p_schema']

    try:
        v_template = v_database.TemplateUpdate(v_schema, v_table).v_text
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = {
        'v_template': v_template
    }

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def template_select_function(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_function = data['p_function']
    v_functionid = data['p_functionid']
    v_schema = data['p_schema']

    try:
        v_template = v_database.TemplateSelectFunction(v_schema, v_function, v_functionid).v_text
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = {
        'v_template': v_template
    }

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def template_call_procedure(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_procedure = data['p_procedure']
    v_procedureid = data['p_procedureid']
    v_schema = data['p_schema']

    try:
        v_template = v_database.TemplateCallProcedure(v_schema, v_procedure, v_procedureid).v_text
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    v_return['v_data'] = {
        'v_template': v_template
    }

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_version(request, v_database):
    v_return = create_response_template()

    try:
        v_return['v_data'] = {
            'v_version': v_database.GetVersion()
        }
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def change_role_password(request, v_database):
    v_return = create_response_template()

    data = request.data

    try:
        v_database.ChangeRolePassword(data['p_role'], data['p_password'])
    except Exception as exc:
        return error_response(message=str(exc), password_timeout=True)

    return JsonResponse(v_return)

@user_authenticated
@database_required(p_check_timeout = True, p_open_connection = True)
def get_object_description(request, v_database):
    v_return = create_response_template()

    data = request.data
    v_oid = data['p_oid']
    v_type = data['p_type']
    v_position = data['p_position']

    v_data = ''

    try:
        v_data = v_database.GetObjectDescription(v_type, v_oid, v_position)
    except Exception as exc:
        v_return['v_data'] = {'password_timeout': True, 'message': str(exc)}
        v_return['v_error'] = True
        return JsonResponse(v_return)

    v_return['v_data'] = v_data

    return JsonResponse(v_return)
