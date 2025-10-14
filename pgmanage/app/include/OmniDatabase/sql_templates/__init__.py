from string import Template

from . import mariadb, mysql, oracle, postgres, sqlite, mssql

TEMPLATE_MODULES = {
    "postgres": postgres,
    "mysql": mysql,
    "mariadb": mariadb,
    "sqlite": sqlite,
    "oracle": oracle,
    "mssql": mssql,
}


def get_template(dialect: str, key: str, version=None) -> Template:
    module = TEMPLATE_MODULES.get(dialect)
    if not module:
        raise ValueError(f"No templates for dialect: {dialect}")

    if not hasattr(module, "get_template"):
        raise ValueError(f"{dialect} template module must implement get_template()")

    return module.get_template(key, version)
