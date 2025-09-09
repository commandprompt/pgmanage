from app.models.main import Connection
from app.utils.decorators import database_required, user_authenticated
from django.http import JsonResponse


@user_authenticated
@database_required(check_timeout=False, open_connection=True)
def get_tree_info(request, database):
    try:
        data = {
            "version": database.GetVersion(),
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
                "name": database_object["name"],
                "database_id": database_object["database_id"],
                "pinned": database_object["name"] in conn_object.pinned_databases,
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
