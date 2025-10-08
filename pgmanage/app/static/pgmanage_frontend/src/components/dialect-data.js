import TableCompiler_SQLite3 from 'knex/lib/dialects/sqlite3/schema/sqlite-tablecompiler'
import TableCompiler_MySQL from 'knex/lib/dialects/mysql/schema/mysql-tablecompiler'
import TableCompiler_PG from 'knex/lib/dialects/postgres/schema/pg-tablecompiler';
import TableCompiler_MSSQL from 'knex/lib/dialects/mssql/schema/mssql-tablecompiler'
import knex from 'knex';
import identity from 'lodash/identity';
import flatten from 'lodash/flatten';

// TODO: Mysql - add PrimaryKey and Comment handling in alter table

knex.TableBuilder.extend(
  "renameIndex",
  function (oldIndexName, newIndexName) {
    this._statements.push({
      grouping: "alterTable",
      method: "renameIndex",
      args: [oldIndexName, newIndexName],
    });
  }
);

export default Object.freeze({
    'postgres': {
        dataTypes: [
          'serial', 'smallserial', 'bigserial', 'int', 'int2', 'int4', 'int8', 'smallint', 'integer',
          'bigint', 'decimal','numeric', 'real', 'float', 'float4', 'float8', 'double precision', 'money',
          'character varying', 'varchar', 'character', 'char', 'text', 'citext', 'hstore', 'bytea', 'bit',
          'varbit', 'bit varying', 'timetz', 'timestamptz', 'timestamp', 'timestamp without time zone',
          'timestamp with time zone', 'date', 'time', 'time without time zone','time with time zone', 'interval',
          'bool', 'boolean', 'enum', 'point', 'line', 'lseg', 'box', 'path', 'polygon','circle', 'cidr', 'inet',
          'macaddr', 'tsvector', 'tsquery', 'uuid', 'xml', 'json', 'jsonb', 'int4range', 'int8range', 'numrange',
          'tsrange', 'tstzrange', 'daterange', 'geometry', 'geography', 'cube', 'ltree'
        ],
        numericTypes: [
          'smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real', 'double precision', 'serial', 'bigserial'
        ],
        indexMethods: ["btree", "hash", "gist", "spgist", "gin", "brin"],
        operators: ['ilike'],
        hasSchema: true,
        hasComments: true,
        formatterDialect: 'postgresql',
        api_endpoints: {
            schemas_url: "/get_schemas_postgresql/",
            types_url: "/get_types_postgresql/",
            indexes_url: "/get_indexes_postgresql/",
            table_definition_url: "/get_table_definition_postgresql/",
            foreign_keys_url: "/get_fks_postgresql/",
        },
        overrides: [
            () => {
                TableCompiler_PG.prototype.renameIndex = function (from, to) {
                  const sql = `ALTER INDEX ${this.formatter.wrap(
                    from
                  )} RENAME TO ${this.formatter.wrap(to)}`;
                  this.pushQuery({
                    sql: sql,
                  });
                };
            },
        ],
    },
    'sqlite3': {
        dataTypes: [
            'int', 'int2', 'int8', 'integer', 'tinyint', 'smallint', 'mediumint', 'bigint', 'decimal', 'numeric',
            'float', 'double', 'real', 'double precision', 'datetime', 'varying character', 'character','native character',
            'varchar', 'nchar', 'nvarchar2', 'unsigned big int', 'boolean', 'blob', 'text', 'clob', 'date'
        ],
        numericTypes: [
          // none yet, to be added later once we are able to resolve column data types in sqlite3
        ],
        hasSchema: false,
        hasComments: false,
        formatterDialect: 'sqlite',
        api_endpoints: {
            table_definition_url: "/get_table_definition_sqlite/",
            indexes_url: "/get_indexes_sqlite/",
            foreign_keys_url: "/get_fks_sqlite/",
        },
        disabledFeatures: {
            alterColumn: true,
            multiStatement: true,
            multiPrimaryKeys: true,
            renameIndex: true,
            indexMethod: true,
            addForeignKey: true,
            dropForeignKey: true,
        },
        overrides: [
            () => {
                TableCompiler_SQLite3.prototype.dropColumn = function() {
                    const columns = flatten(arguments);
                
                    const columnsWrapped = columns.map((column) =>
                      this.client.customWrapIdentifier(column, identity)
                    );
                    columnsWrapped.forEach((col_name) => {
                      this.pushQuery({
                        sql: `alter table ${this.tableName()} drop column ${this.formatter.wrap(col_name)}`,
                      });
                    })
                }
            },
        ],
    },
    'mysql': {
        dataTypes: [
          'bit', 'int', 'int unsigned', 'integer', 'integer unsigned', 'tinyint', 'tinyint unsigned',
          'smallint', 'smallint unsigned', 'mediumint', 'mediumint unsigned', 'bigint', 'bigint unsigned',
          'float', 'double', 'double precision', 'dec', 'decimal', 'numeric', 'fixed', 'bool', 'boolean',
          'date', 'datetime', 'timestamp', 'time', 'year', 'char', 'nchar', 'national char', 'varchar',
          'nvarchar', 'national varchar', 'text', 'tinytext', 'mediumtext', 'blob', 'longtext', 'tinyblob',
          'mediumblob', 'longblob', 'enum', 'set', 'json', 'binary', 'varbinary', 'geometry', 'point',
          'linestring', 'polygon', 'multipoint', 'multilinestring', 'multipolygon', 'geometrycollection',
        ],
        numericTypes: [
          // these values are not taken from dataTypes, they match with pymysql type mapping
          'decimal', 'tiny', 'short', 'long', 'float', 'double', 'int24', 'longlong', 'newdecimal'
        ],
        indexTypes: ["fulltext", "spatial"],
        hasSchema: false,
        hasComments: true,
        formatterDialect: 'mysql',
        api_endpoints: {
          table_definition_url: "/get_table_definition_mysql/",
          indexes_url: "/get_indexes_mysql/",
          foreign_keys_url: "/get_fks_mysql/",
        },
        disabledFeatures: {
          indexPredicate: true,
          indexMethod: true,
          renameIndex: true,
        },
        overrides: [
          () => {
              TableCompiler_MySQL.prototype._setNullableState = function (
              column,
              isNullable
              ) {
              const nullability = isNullable ? "NULL" : "NOT NULL";
              const columnType = column.dataType;
              const sql = `alter table ${this.tableName()} modify ${this.formatter.wrap(
                  column.name
              )} ${columnType} ${nullability}`;
              return this.pushQuery({
                  sql: sql,
              });
              };
          },
          () => {
              TableCompiler_MySQL.prototype.renameColumn = function (from, to) {
              const table = this.tableName();
              const wrapped =
                  this.formatter.wrap(from) + " TO " + this.formatter.wrap(to);
              const sql = `ALTER TABLE ${table} RENAME COLUMN ${wrapped};`;
              this.pushQuery({
                  sql: sql,
              });
              };
          },
        ],
    },
    'mssql': {
      dataTypes: [
        'int', 'bigint', 'binary', 'bit', 'char', 'date', 'datetime', 'datetime2', 'datetimeoffset', 'decimal',
        'float','ntext', 'numeric', 'nvarchar', 'real', 'smalldatetime', 'smallint', 'smallmoney', 
        'text', 'time', 'timestamp', 'tinyint', 'uniqueidentifier', 'varbinary', 'varchar', 'xml',
        'sql_variant', 'hierarchyid', 'geography', 'geometry', 'rowversion', 'money', 'image', 'nchar'
      ],
      numericTypes: [
        'bigint', 'int', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney'
      ],
      hasSchema: true,
      hasComments: false,
      formatterDialect: 'transactsql',
      api_endpoints: {
          schemas_url: "/get_schemas_mssql/",
          types_url: "/get_types_mssql/",
          indexes_url: "/get_indexes_mssql/",
          table_definition_url: "/get_table_definition_mssql/",
          foreign_keys_url: "/get_fks_mssql/",
      },
      disabledFeatures: {
        indexMethod: true,
        renameIndex: true,
      },
      overrides: [
        () => {
          // tweakes alterColumns that:
          //   includes schema name in drop constraint statement
          //   uses unique names for constrain check variables
          TableCompiler_MSSQL.prototype.alterColumns = function(columns, colBuilder) {
            for (let i = 0, l = colBuilder.length; i < l; i++) {
              const builder = colBuilder[i];
              if (builder.modified.defaultTo) {
                const schema = this.schemaNameRaw || 'dbo';
                const baseQuery = `
                      DECLARE @constraint${i} varchar(100) = (SELECT default_constraints.name
                                                          FROM sys.all_columns
                                                          INNER JOIN sys.tables
                                                            ON all_columns.object_id = tables.object_id
                                                          INNER JOIN sys.schemas
                                                            ON tables.schema_id = schemas.schema_id
                                                          INNER JOIN sys.default_constraints
                                                            ON all_columns.default_object_id = default_constraints.object_id
                                                          WHERE schemas.name = '${schema}'
                                                          AND tables.name = '${
                                                            this.tableNameRaw
                                                          }'
                                                          AND all_columns.name = '${builder.getColumnName()}')

                      IF @constraint${i} IS NOT NULL EXEC('ALTER TABLE ${this.schemaNameRaw}.${
                        this.tableNameRaw
                      } DROP CONSTRAINT ' + @constraint${i})`;
                this.pushQuery(baseQuery);
              }
            }
            // in SQL server only one column can be altered at a time
            columns.sql.forEach((sql) => {
              this.pushQuery({
                sql:
                  (this.lowerCase ? 'alter table ' : 'ALTER TABLE ') +
                  this.tableName() +
                  ' ' +
                  (this.lowerCase
                    ? this.alterColumnPrefix.toLowerCase()
                    : this.alterColumnPrefix) +
                  sql,
                bindings: columns.bindings,
              });
            });
          }
        },
      ],
  },
});