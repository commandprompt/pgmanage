<template>
  <div class="schema-editor-scrollable p-1 pr-2">
    <div class="form-row">
      <div class="form-group col-2">
          <label class="font-weight-bold mb-2" for="tableNameInput">Table Name</label>
          <input v-model.trim="localTable.tableName" class="form-control" id="tableNameInput" name="tableName" placeholder="table name..." />
      </div>

      <div v-if="showSchema" class="form-group col-3">
        <label class="font-weight-bold mb-2" for="selectSchema">Schema</label>
        <select class="form-control text-truncate pr-4" id="selectSchema" v-model="localTable.schema">
          <option v-for="(schema, index) in schemas" :value="schema" :key="index">
            {{ schema }}
          </option>
        </select>
      </div>
      <div class="form-group col d-flex align-items-end">
        <button :disabled="!hasChanges || queryIsRunning" @click='applyChanges' type="button"
          class="btn btn-success mt-4 ml-auto">
          Apply Changes
        </button>
      </div>
    </div>

    <div class="form-row">
      <div class="col">
        <label class="font-weight-bold mb-2 mr-2">Columns</label>

      <!-- TODO -->
      <!-- <button @click='addColumn' class="btn btn-icon btn-icon-success" title="Add column">
        <i class="fa-solid fa-circle-plus fa-xl"></i>
      </button> -->
      </div>
    </div>

    <ColumnList
      :initialColumns="localTable.columns"
      :dataTypes="dataTypes"
      :commentable="commentable"
      :mode="getMode"
      @columns:changed="changeColumns" />

    <div class="form-group mb-2">
        <p class="font-weight-bold mb-2">Generated SQL</p>
        <div ref="editor" style="height: 30vh"></div>
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
import { v_queryRequestCodes } from '../query'
import axios from 'axios'
import { showToast } from '../notification_control'

export default {
  name: "SchemaEditor",
  props: {
    // pass dbrefs here so that we can make api calls
    mode: String,
    dialect: String,
    schema: String,
    table: String,
    tab_id: String,
    database_index: Number,
    database_name: String,
    tree_node: Object,
    tree: Object
  },
  components: {
    ColumnList
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
          columns: [{
            dataType: 'autoincrement',
            name: 'id',
            defaultValue: 0,
            nullable: false,
            isPK: true,
            comment: null
          }]
        },
        generatedSQL: '',
        hasChanges: false,
        queryIsRunning: false
    };
  },
  mounted() {
    // the "client" parameter is a bit misleading here,
    // we do not connect to any db from Knex, just setting
    // the correct SQL dialect with this option
    this.knex = Knex({ client: this.dialect })
    this.loadDialectData(this.dialect)
    this.setupEditor()
    if(this.$props.mode==='alter') {
      this.loadTableDefinition()
      // localTable for ALTER case is being set via watcher
    } else {
      this.initialTable.schema = this.$props.schema
      this.localTable = {...this.initialTable}
    }
  },
  methods: {
    loadDialectData(dialect) {
      this.dialectData = dialects[dialect]

      if(dialect === 'postgres') {
        axios.post('/get_schemas_postgresql/', {
          database_index: this.database_index,
          tab_id: this.tab_id
        })
        .then((response) => {
          this.schemas = response.data.map((schema) => {return schema.name})
        })
        .catch((error) => {
          console.log(error)
        })

        axios.post('/get_types_postgresql/', {
          database_index: this.database_index,
          tab_id: this.tab_id,
          schema: this.schema
        })
        .then((response) => {
          this.customTypes = response.data.map((type) => {return type.type_name})
        })
        .catch((error) => {
          console.log(error)
        })
      }
    },
    loadTableDefinition() {
        // FIXME: this should be database-agnostic
        axios.post('/get_table_definition_postgresql/', {
          database_index: this.database_index,
          tab_id: this.tab_id,
          table: this.localTable.tableName || this.table,
          schema: this.schema
        })
        .then((response) => {
          let coldefs = response.data.data.map((col) => {
            return {
              dataType: col.data_type,
              name: col.name,
              defaultValue: col.default_value,
              nullable: col.nullable,
              isPK: col.is_primary,
              comment: col.comment,
            }
          })
          this.initialTable.columns = coldefs
          this.initialTable.tableName = this.localTable.tableName || this.$props.table
          this.initialTable.schema = this.$props.schema
        })
        .catch((error) => {
          console.log(error)
        })
    },
    setupEditor() {
      this.editor = ace.edit(this.$refs.editor);
      this.editor.setTheme("ace/theme/" + window.v_editor_theme);
      this.editor.session.setMode("ace/mode/sql");
      this.editor.setFontSize(Number(window.v_font_size));
      this.editor.$blockScrolling = Infinity;
      this.editor.clearSelection();
      this.editor.setReadOnly(true);
    },
    generateSQL() {
      //add knex error handing with notification to the user
      let tabledef = this.localTable
      let k = this.knex.schema.withSchema(tabledef.schema)
      this.hasChanges = false
      if(this.mode === 'alter') {
        let changes = {
          'adds': [],
          'drops': [],
          'typeChanges': [],
          'nullableChanges': [],
          'defaults': [],
          'renames': [],
          'comments': []
        }

        let originalColumns = this.initialTable.columns
        this.localTable.columns.forEach((column, idx) => {
          if(column.deleted) changes.drops.push(column.name)
          if(column.new) changes.adds.push(column)
          if(column.deleted || column.new) return //no need to do further steps for new or deleted cols
          if(column.dataType !== originalColumns[idx].dataType) changes.typeChanges.push(column)
          if(column.nullable !== originalColumns[idx].nullable) changes.nullableChanges.push(column)
          if(column.defaultValue !== originalColumns[idx].defaultValue) changes.defaults.push(column)
          if(column.name !== originalColumns[idx].name) changes.renames.push({'oldName': originalColumns[idx].name, 'newName': column.name})
          if(column.comment !== originalColumns[idx].comment) changes.comments.push(column)
        })

        // we use initial table name here since localTable.tableName may be changed
        // which results in broken SQL
        let knexOperations = k.alterTable(this.initialTable.tableName, function(table) {
          changes.adds.forEach((coldef) => {
            // use Knex's magic to create a proper auto-incrementing column in database-agnostic way
            let col = coldef.dataType === 'autoincrement' ?
              table.increments(coldef.name) :
              table.specificType(coldef.name, coldef.dataType)

            coldef.nullable ? col.nullable() : col.notNullable()

            if(coldef.defaultValue !== '') col.defaultTo(coldef.defaultValue)
            if(coldef.comment) col.comment(coldef.comment)
          })

          if(changes.drops.length) table.dropColumns(changes.drops)

          changes.typeChanges.forEach((coldef) => {
            if(coldef.dataType === 'autoincrement') {
              table.increments(coldef.name).alter()
            }else {
              table.specificType(coldef.name, coldef.dataType).defaultTo(coldef.defaultValue).alter({alterNullable : false})
            }
          })

          changes.defaults.forEach(function(coldef) {
            if (coldef.defaultValue !== '') {
              table.specificType(coldef.name, coldef.dataType).alter().defaultTo(table.client.raw(coldef.defaultValue)).alter({alterNullable : false, alterType: false})
            }
            // FIXME: this does not work, figure out how to do drop default via Knex.
            //  else {
            //   table.specificType(coldef.name, coldef.dataType).defaultTo().alter({alterNullable : false, alterType: false})
            // }
          })

          changes.nullableChanges.forEach((coldef) => {
            coldef.nullable ? table.setNullable(coldef.name) : table.dropNullable(coldef.name)
          })

          // FIXME: commenting generates drop default - how to avoid this?
          changes.comments.forEach((coldef) => {
            //table.specificType(coldef.name, coldef.dataType).comment('test').alter({alterNullable : false, alterType: false})
            // table.raw(`comment on column "${tabledef.schema}"."${table._tableName}"."${coldef.name}" is '${coldef.comment}'`)
          })

          changes.renames.forEach((rename) => {
            table.renameColumn(rename.oldName, rename.newName)
          })
        })
        // handle table rename last
        if(this.initialTable.tableName !== this.localTable.tableName) {
          knexOperations.renameTable(this.initialTable.tableName, this.localTable.tableName)
        }
        this.generatedSQL = knexOperations.toQuery()

      } else {
        // mode==create
        this.generatedSQL = k.createTable(tabledef.tableName, function (table) {
          tabledef.columns.forEach((coldef) => {
            // use Knex's magic to create a proper auto-incrementing column in database-agnostic way
            let col = coldef.dataType === 'autoincrement' ?
              table.increments(coldef.name) :
              table.specificType(coldef.name, coldef.dataType)

            coldef.nullable ? col.nullable() : col.notNullable()

            if(coldef.defaultValue) col.defaultTo(coldef.defaultValue)
            if(coldef.comment) col.comment(coldef.comment)
          })

          // generate PKs
          let pkCols = tabledef.columns.filter((col) => col.isPK)
          if(pkCols.length > 0) {
            table.primary(pkCols.map((col) => col.name))
          }
        }).toQuery()
      }
      this.hasChanges = this.generatedSQL.length > 0
    },
    changeColumns(columns) {
      this.localTable.columns = columns
    },
    applyChanges() {
      let message_data = {
				v_sql_cmd : this.editor.getValue(), //use formatted SQL from the editor instead of single-line returned by generatedSQL
				v_sql_save : false,
				v_cmd_type: null,
				v_db_index: this.database_index,
				v_conn_tab_id: v_connTabControl.selectedTab.id,
				v_tab_id: this.tab_id,
				v_tab_db_id: this.database_index,
				v_mode: 0,
				v_all_data: false,
				v_log_query: false,
				v_tab_title: 'schema editor',
				v_autocommit: true,
				database_name: this.database_name
			}

      let context = {
				tab_tag: v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag,
				cmd_type: null,
				database_index: this.database_index,
				mode: 0,
				callback: this.handleResponse.bind(this),
				acked: false,
				query: this.editor.getValue(),
				log_query: false,
				save_query: null,
        simple: true //a hacky way to prevent long polling handler from running legacy rentering routines
			}
      this.queryIsRunning = true
      createRequest(v_queryRequestCodes.Query, message_data, context)
    },
    handleResponse(response) {
      if(response.v_error == true) {
        showToast("error", response.v_data.message)
      } else {
        let msg = response.v_data.v_status === "CREATE TABLE" ? `Table "${this.localTable.tableName}" created` : `Table "${this.localTable.tableName}" updated`
        showToast("success", msg)

        emitter.emit(`schemaChanged_${this.tree.id}`, { database_name: this.database_name, schema_name: this.localTable.schema })
        // ALTER: load table changes into UI
        if(this.mode === 'alter') {
          this.loadTableDefinition()
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
      return rawTypes.map((t, idx) => {return {id: idx, name: t}})
    },
    showSchema() {
      return this.mode !== 'alter' && this.dialectData.hasSchema
    },
    commentable() {
      return this.dialectData.hasComments
    },
    getMode() {
      return this.mode
    }
  },
  watch: {
    generatedSQL() {
      this.editor.setValue(
        format(
          this.generatedSQL,
          {
            tabWidth: 2,
            keywordCase: 'upper',
            language: this.dialectData.formatterDialect,
            linesBetweenQueries: 1,
          }
      ))
      this.editor.clearSelection();
    },
    // watch initialTable for changes for cases when it is changed by requesting tabledef from the back-end
    initialTable: {
      handler(newVal, oldVal) {
        this.localTable = JSON.parse(JSON.stringify(newVal))
      },
      deep: true
    },
    // watch our local working copy for changes, generate new SQL when the change occcurs
    localTable: {
      handler(newVal, oldVal) {
        this.generateSQL()
      },
      deep: true
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