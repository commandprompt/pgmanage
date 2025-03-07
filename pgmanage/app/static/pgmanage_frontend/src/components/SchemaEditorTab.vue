<template>
  <div class="schema-editor-scrollable px-2 pt-3">
    <template v-if="mode === operationModes.UPDATE"> <!-- ALTER SCHEMA -->
      <div class="row">
        <div class="form-group col-2">
            <label class="fw-bold mb-2" :for="`${tabId}_tableNameInput`">Table Name</label>
            <input v-model.trim="localTable.tableName" class="form-control" :id="`${tabId}_tableNameInput`" name="tableName" placeholder="table name..." />
        </div>
        <div class="form-group col d-flex align-items-end">
          <button :disabled="!hasChanges || queryIsRunning" @click='applyChanges' type="button"
            class="btn btn-success mt-4 ms-auto">
            Apply Changes
          </button>
        </div>
      </div>
      <ul class="nav nav-tabs mb-3" role="tablist">
        <li ref="columnsTab" class="nav-item" role="presentation">
          <button class="nav-item nav-link active" :id="`${tabId}-columns-tab`"
            data-bs-toggle="tab" :data-bs-target="`#${tabId}-columns-tab-pane`"
            type="button" role="tab"
            aria-selected="true">
            Columns
          </button>
        </li>
        <li ref="indexesTab" class="nav-item" role="presentation">
          <button class="nav-item nav-link" :id="`${tabId}-indexes-tab`"
            data-bs-toggle="tab"
            :data-bs-target="`#${tabId}-indexes-tab-pane`"
            type="button" role="tab" aria-selected="false">
            Indexes
          </button>
        </li>
      </ul>
      <div class="tab-content">
        <div class="tab-pane fade show active" :id="`${tabId}-columns-tab-pane`" role="tabpanel">
          <ColumnList
            :initialColumns="initialTable.columns"
            :dataTypes="dataTypes"
            :commentable="commentable"
            :mode="getMode"
            :multiPKeys="multiPrimaryKeys"
            @columns:changed="changeColumns" />
        </div>
        <div class="tab-pane fade" :id="`${tabId}-indexes-tab-pane`" role="tabpanel"  >
          <IndexesList
          :initialIndexes="initialIndexes"
          :indexTypes="indexTypes"
          :columns="columnNames"
          :index-methods="indexMethods"
          :disabled-features="disabledFeatures"
          @indexes:changed="changeIndexes"
          />
        </div>
      </div>
    </template>

  <template v-if="mode !== operationModes.UPDATE"> <!-- CREATE SCHEMA -->

    <div class="row">
          <div class="form-group col-2">
              <label class="fw-bold mb-2" :for="`${tabId}_tableNameInput`">Table Name</label>
              <input v-model.trim="localTable.tableName" class="form-control" :id="`${tabId}_tableNameInput`" name="tableName" placeholder="table name..." />
          </div>

          <div v-if="showSchema" class="form-group col-3">
            <label class="fw-bold mb-2" :for="`${tabId}_selectSchema`">Schema</label>
            <select class="form-select text-truncate pe-4" :id="`${tabId}_selectSchema`" v-model="localTable.schema">
              <option v-for="(schema, index) in schemas" :value="schema" :key="index">
                {{ schema }}
              </option>
            </select>
          </div>
          <div class="form-group col d-flex align-items-end">
            <button :disabled="!hasChanges || queryIsRunning" @click='applyChanges' type="button"
              class="btn btn-success mt-4 ms-auto">
              Apply Changes
            </button>
          </div>
        </div>

        <div class="row">
          <div class="col">
            <label class="fw-bold mb-2 me-2">Columns</label>

          <!-- TODO -->
          <!-- <button @click='addColumn' class="btn btn-icon btn-icon-success" title="Add column">
            <i class="fa-solid fa-circle-plus fa-xl"></i>
          </button> -->
          </div>
        </div>

        <ColumnList
          :initialColumns="initialTable.columns"
          :dataTypes="dataTypes"
          :commentable="commentable"
          :mode="getMode"
          :multiPKeys="multiPrimaryKeys"
          @columns:changed="changeColumns" />
  </template>

    <div class="form-group mb-2">
        <PreviewBox :editor-text="generatedSQL" label="Generated SQL" :database-technology="dialect" style="height: 30vh"/>
    </div>
  </div>
</template>

<script>
import Knex from 'knex'
import { format } from 'sql-formatter'
import { emitter } from '../emitter'
import { useVuelidate } from '@vuelidate/core'
import ColumnList from './SchemaEditorColumnList.vue'
import dialects from './dialect-data'
import { createRequest } from '../long_polling'
import { queryRequestCodes, operationModes } from '../constants'
import axios from 'axios'
import { showToast } from '../notification_control'
import { tabsStore } from '../stores/stores_initializer'
import IndexesList from './SchemaEditorIndexesList.vue'
import isEqual from 'lodash/isEqual';
import PreviewBox from './PreviewBox.vue'
import { handleError } from '../logging/utils';


function formatDefaultValue(defaultValue, dataType, table) {
  if (!defaultValue) return null
  if (defaultValue.trim().toLowerCase() == 'null') return null

  let textTypesMap = ['CHAR', 'VARCHAR', 'TINYTEXT', 'MEDIUMTEXT', 'LONGTEXT',
    'TEXT', 'CHARACTER', 'NCHAR', 'NVARCHAR',
    'CHARACTER VARYING',
  ]

  if (textTypesMap.includes(dataType.toUpperCase())) {
    const stringValue = defaultValue.toString()
    return stringValue
  }

  // If no conversion matches, return raw value
  return table.client.raw(defaultValue);
}

export default {
  name: "SchemaEditor",
  props: {
    // pass dbrefs here so that we can make api calls
    mode: operationModes,
    dialect: String,
    schema: String,
    table: String,
    workspaceId: String,
    tabId: String,
    databaseIndex: Number,
    databaseName: String,
    treeNode: Object,
  },
  components: {
    ColumnList,
    IndexesList,
    PreviewBox,
  },
  setup(props) {
    // FIXME: add column nam not-null validations
    return {
      v$: useVuelidate()
    }
  },
  data() {
    return {
        // TODO: improve this
        // 2 - for postgres - store type and its alias as a single item to reduce dropdown length
        dialectData: {},
        knex: null,
        schemas: [],
        customTypes: [],
        localTable: {},
        initialTable: {
          tableName: 'new_table',
          schema: '',
          columns: []
        },
        localIndexes: [],
        initialIndexes: [],
        generatedSQL: '',
        hasChanges: false,
        queryIsRunning: false,
        tabType: "Columns"
    };
  },
  created() {
    // allows for using operationModes in the template
    this.operationModes = operationModes
  },
  mounted() {
    // the "client" parameter is a bit misleading here,
    // we do not connect to any db from Knex, just setting
    // the correct SQL dialect with this option
    this.knex = Knex({ client: this.dialect })
    this.loadDialectData(this.dialect)
    if(this.$props.mode === operationModes.UPDATE) {
      this.loadTableDefinition().then(() => {
        this.loadIndexes();
      });
      // localTable for ALTER case is being set via watcher
    } else {
      this.initialTable.schema = this.$props.schema
      this.localTable = {...this.initialTable}
    }
  },
  methods: {
    loadDialectData(dialect) {
      this.dialectData = dialects[dialect]

      if (this.dialectData.hasOwnProperty('overrides')) {
        this.dialectData.overrides.forEach(func => func())
      }
      this.loadSchemas()
      this.loadTypes()
    },
    loadSchemas() {
      const schemasUrl = this.dialectData?.api_endpoints?.schemas_url ?? null;
      if (!schemasUrl) return;

      axios.post(schemasUrl, {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId
      })
      .then((response) => {
        this.schemas = response.data.map((schema) => {return schema.name})
      })
      .catch((error) => {
        handleError(error);
      })
    },
    loadTypes() {
      const typesUrl = this.dialectData?.api_endpoints?.types_url ?? null;
      if (!typesUrl) return;

      axios.post(typesUrl, {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
        schema: this.schema
      })
      .then((response) => {
        this.customTypes = response.data.map((type) => {return type.type_name})
      })
      .catch((error) => {
        handleError(error);
      })
    },
    async loadTableDefinition() {
      try {
        const response = await axios.post(this.dialectData.api_endpoints.table_definition_url, {
          database_index: this.databaseIndex,
          workspace_id: this.workspaceId,
          table: this.localTable.tableName || this.table,
          schema: this.schema
        })

        let coldefs = response.data.data.map((col) => {
            return {
              dataType: col.data_type,
              name: col.name,
              defaultValue: col.default_value,
              nullable: col.nullable,
              isPK: col.is_primary,
              comment: col.comment ?? null,
              editable: this.editable,
              is_dirty: false,
              deleted: false,
            }
          })
          this.initialTable.columns = coldefs
          this.initialTable.tableName = this.localTable.tableName || this.$props.table
          this.initialTable.schema = this.$props.schema
          this.localTable = JSON.parse(JSON.stringify(this.initialTable));
      } catch (error) {
        handleError(error)
      }
    },
    loadIndexes() {
      const indexesUrl = this.dialectData?.api_endpoints?.indexes_url ?? null;
      if (!indexesUrl) return;

      axios.post(indexesUrl, {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
        schema: this.schema,
        table: this.localTable.tableName || this.table
      })
      .then((resp) => {
        this.initialIndexes = resp.data.map((index) => {
          return {...index, is_dirty: false}
        });
        this.localIndexes = JSON.parse(JSON.stringify(this.initialIndexes));
      })
      .catch((error) => {
        handleError(error);
      })
    },
    generateAlterSQL(knexInstance) {
      let columnChanges = {
          'adds': [],
          'drops': [],
          'typeChanges': [],
          'nullableChanges': [],
          'defaults': [],
          'renames': [],
          'comments': []
        }

        let indexChanges = {
          'adds': [],
          'drops': [],
          'typeChanges': [],
          'renames': [],
        }

        // TODO: add support for altering Primary Keys
        // TODO: add support for composite PKs
        let originalColumns = this.initialTable.columns
        this.localTable.columns?.forEach((column, idx) => {
          if(column.deleted) columnChanges.drops.push(originalColumns[idx].name)
          if(column.new) columnChanges.adds.push(column)
          if(column.deleted || column.new) return //no need to do further steps for new or deleted cols


          if (!isEqual(column, originalColumns[idx])) {
            column.is_dirty = true;
            originalColumns[idx].is_dirty = true;
          } else {
            column.is_dirty = false;
            originalColumns[idx].is_dirty = false;
          }
  
          if(column.dataType !== originalColumns[idx].dataType) columnChanges.typeChanges.push(column)
          if(column.nullable !== originalColumns[idx].nullable) columnChanges.nullableChanges.push(column)
          if(column.defaultValue !== originalColumns[idx].defaultValue) columnChanges.defaults.push(column)
          if(column.name !== originalColumns[idx].name) columnChanges.renames.push({'oldName': originalColumns[idx].name, 'newName': column.name})
          if(column.comment !== originalColumns[idx].comment) columnChanges.comments.push(column)
        })


        let originalIndexes = this.initialIndexes
        this.localIndexes.forEach((index, idx) => {
          if(index.deleted) indexChanges.drops.push(originalIndexes[idx].index_name)
          if(index.new) indexChanges.adds.push(index)
          if(index.deleted || index.new) return

          if (!isEqual(index, originalIndexes[idx])) {
            index.is_dirty = true;
            originalIndexes[idx].is_dirty = true;
          } else {
            index.is_dirty = false;
            originalIndexes[idx].is_dirty = false;
          }
          if(index.index_name !== originalIndexes[idx].index_name) indexChanges.renames.push({'oldName': originalIndexes[idx].index_name, 'newName': index.index_name})
        })

        // we use initial table name here since localTable.tableName may be changed
        // which results in broken SQL
        let knexOperations = knexInstance.alterTable(this.initialTable.tableName, (table) => {
          columnChanges.adds.forEach((coldef) => {
            // use Knex's magic to create a proper auto-incrementing column in database-agnostic way
            let col = coldef.dataType === 'autoincrement' ?
              table.increments(coldef.name) :
              table.specificType(coldef.name, coldef.dataType)

            coldef.nullable ? col.nullable() : col.notNullable()
            if(coldef.defaultValue) {
              let formattedDefault = formatDefaultValue(coldef.defaultValue, coldef.dataType, table);
              col.defaultTo(formattedDefault);
            }
 
            if(coldef.comment) col.comment(coldef.comment)
          })

          if(columnChanges.drops.length) table.dropColumns(columnChanges.drops)

          columnChanges.typeChanges.forEach((coldef) => {
            if (coldef.dataType === 'autoincrement') {
              table.increments(coldef.name).alter()
            } else {
              let formattedDefault = formatDefaultValue(coldef.defaultValue, coldef.dataType, table)
              table.specificType(coldef.name, coldef.dataType).defaultTo(formattedDefault).alter({ alterNullable: false })
              coldef.skipDefaults = true
            }
          })

          columnChanges.defaults.forEach(function (coldef) {
            if (!!coldef?.skipDefaults) return
            let formattedDefault = formatDefaultValue(coldef.defaultValue, coldef.dataType, table)
            table.specificType(coldef.name, coldef.dataType).alter().defaultTo(formattedDefault).alter({ alterNullable: false, alterType: false })

            // FIXME: this does not work, figure out how to do drop default via Knex.
            //  else {
            //   table.specificType(coldef.name, coldef.dataType).defaultTo().alter({alterNullable : false, alterType: false})
            // }
          })

          columnChanges.nullableChanges.forEach((coldef) => {
            if (table.client.dialect === "mysql") {
              coldef.nullable ? table.setNullable(coldef) : table.dropNullable(coldef)
            } else {
              coldef.nullable ? table.setNullable(coldef.name) : table.dropNullable(coldef.name)
            }
          })

          // FIXME: commenting generates drop default - how to avoid this?
          // columnChanges.comments.forEach((coldef) => {
          //   //table.specificType(coldef.name, coldef.dataType).comment('test').alter({alterNullable : false, alterType: false})
          //   // table.raw(`comment on column "${tabledef.schema}"."${table._tableName}"."${coldef.name}" is '${coldef.comment}'`)
          // })

          columnChanges.renames.forEach((rename) => {
            table.renameColumn(rename.oldName, rename.newName)
          })

          indexChanges.adds.forEach((indexDef) => {
            const strippedPredicate = indexDef.predicate.trimStart().replace(/^where/i, "");
            if (indexDef.type === "unique") {
              table.unique(indexDef.columns, {
                indexName: indexDef.index_name,
                useConstraint: false,
                predicate: this.knex.where(this.knex.raw(strippedPredicate)),
              })
            } else {
              table.index(indexDef.columns, indexDef.index_name, {
                indexType: indexDef.type === 'non-unique' ? '' : indexDef.type,
                storageEngineIndexType: indexDef.method,
                predicate: this.knex.where(this.knex.raw(strippedPredicate)),
              }
            )
            }
          })

          indexChanges.drops.forEach((indexName) => {
            table.dropIndex(null, indexName)
          })

          // TODO: add renameIndex support on other database dialects
          indexChanges.renames.forEach((rename) => {
            table.renameIndex(rename.oldName, rename.newName)
          })
        })
        // handle table rename last
        if(this.initialTable.tableName !== this.localTable.tableName) {
          knexOperations.renameTable(this.initialTable.tableName, this.localTable.tableName)
        }

        return knexOperations.toQuery()
    },
    generateSQL() {
      //add knex error handing with notification to the user
      let tabledef = this.localTable
      let k = this.knex.schema.withSchema(tabledef.schema)

      this.hasChanges = false
      if(this.mode === operationModes.UPDATE) {
        this.generatedSQL = this.generateAlterSQL(k);
      } else {
        // mode==create
        this.generatedSQL = k.createTable(tabledef.tableName, function (table) {
          tabledef.columns.forEach((coldef) => {
            // use Knex's magic to create a proper auto-incrementing column in database-agnostic way
            let col = coldef.dataType === 'autoincrement' ?
              table.increments(coldef.name, {primaryKey: false}) :
              table.specificType(coldef.name, coldef.dataType)

            coldef.nullable ? col.nullable() : col.notNullable()

            if(coldef.defaultValue) {
              let formattedDefault = formatDefaultValue(coldef.defaultValue, coldef.dataType, table);
              col.defaultTo(formattedDefault);
            }

            if(coldef.comment) col.comment(coldef.comment)
          })

          // generate PKs
          let pkCols = tabledef.columns.filter((col) => col.isPK)
          if(pkCols.length > 0) {
            table.primary(pkCols.map((col) => col.name))
          }
        }).toQuery()
      }
      if (this.generatedSQL.length > 0) {
        this.generatedSQL = format(
          this.generatedSQL,
            {
              tabWidth: 2,
              keywordCase: 'upper',
              language: this.dialectData.formatterDialect,
              linesBetweenQueries: 1,
            }
        )
      }
      this.hasChanges = this.generatedSQL.length > 0
    },
    changeColumns(columns) {
      this.localTable.columns = [...columns]
    },
    changeIndexes(indexes) {
      this.localIndexes = [...indexes]
    },
    applyChanges() {
      let message_data = {
				sql_cmd : this.generatedSQL,
				db_index: this.databaseIndex,
				workspace_id: this.workspaceId,
				tab_id: this.tabId,
				autocommit: true,
				database_name: this.databaseName,
			}

      let context = {
				callback: this.handleResponse.bind(this),
			}
      this.queryIsRunning = true;
      createRequest(queryRequestCodes.SchemaEditData, message_data, context)
    },
    handleResponse(response) {
      if(response.error == true) {
        showToast("error", response.data.message)
      } else {
        let msg = response.data.status === "CREATE TABLE" ? `Table "${this.localTable.tableName}" created` : `Table "${this.localTable.tableName}" updated`
        showToast("success", msg)

        emitter.emit(`schemaChanged_${this.workspaceId}`, { database_name: this.databaseName, schema_name: this.localTable.schema })
        // ALTER: load table changes into UI
        if(this.mode === operationModes.UPDATE) {
          this.loadTableDefinition().then(() => {
            this.loadIndexes();
          });
        } else {
          // CREATE:reset the editor
          this.initialTable.schema = this.$props.schema
          this.localTable = {...this.initialTable}
        }
      }
      this.queryIsRunning = false
    },
  },
  computed: {
    dataTypes() {
      // our magic datatype to generate db-specific autoincrementing column
      let rawTypes = ['autoincrement']
        .concat(this.dialectData.dataTypes)
        .concat(this.customTypes)
      return rawTypes
    },
    showSchema() {
      return this.mode !== operationModes.UPDATE && this.dialectData.hasSchema
    },
    commentable() {
      return this.dialectData.hasComments
    },
    getMode() {
      return this.mode
    },
    editable() {
      return ((this.mode === operationModes.UPDATE && !this.dialectData?.disabledFeatures?.alterColumn) || this.mode === operationModes.CREATE)
    },
    multiPrimaryKeys() {
      return !this.dialectData?.disabledFeatures?.multiPrimaryKeys
    },
    indexMethods() {
      return this.dialectData.indexMethods
    },
    columnNames() {
      return this.localTable.columns?.map((col) => col.name);
    },
    indexTypes() {
      let defaultTypes = ["non-unique", "unique"]
      return this.dialectData?.indexTypes ? [...defaultTypes, ...this.dialectData.indexTypes] : defaultTypes
    },
    disabledFeatures() {
      return this.dialectData?.disabledFeatures
    }
  },
  watch: {
    // watch our local working copy for changes, generate new SQL when the change occcurs
    localTable: {
      handler(newVal, oldVal) {
        this.generateSQL()
      },
      deep: true
    },
    localIndexes: {
      handler(newVal, oldVal) {
        this.generateSQL()
      },
      deep: true
    },
    hasChanges() {
      const tab = tabsStore.getSecondaryTabById(this.tabId, this.workspaceId);
      if (tab) {
        tab.metaData.hasUnsavedChanges = this.hasChanges;
      }
    },
    mode: {
      handler(newValue) {
        if (newValue === operationModes.CREATE) {
          this.initialTable.columns = [{
            dataType: 'autoincrement',
            name: 'id',
            defaultValue: 0,
            nullable: false,
            isPK: true,
            comment: null,
            editable: true
          }]
        }
      },
      immediate: true,
    }
  }
};
</script>

<style scoped>
  .schema-editor-scrollable {
    height: calc(100vh - 60px);
    overflow: hidden auto;
  }
</style>