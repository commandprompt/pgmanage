<template>
<div class="data-editor">
  <div ref="topToolbar" class="form-row">
    <div class="form-group col-9">
      <form class="form" @submit.prevent>
        <label class="mb-2" for="selectServer">
          <span class="font-weight-bold">Filter</span> <span class='text-info'> select</span> * <span class='text-info'>from</span> {{this.schema ? `${this.schema}.${this.table}` : `${this.table}`}}
        </label>
        <input v-model.trim="queryFilter" class="form-control" name="filter"
           placeholder="extra filter criteria" />
      </form>
    </div>
    <div class="form-group col-2">
      <form class="form" @submit.prevent>
        <label class="font-weight-bold mb-2" for="selectServer">Limit</label>
        <select v-model="rowLimit" class="form-control">
            <option v-for="(option, index) in [10, 100, 1000]"
              :key=index
              :value="option">
                {{option}} rows
            </option>
        </select>
      </form>
    </div>
    <div class="form-group col-1 d-flex align-items-end pl-0">
      <button class="btn btn-primary mr-2" title="Load Data" @click="getTableData()">
        <i class="fa-solid fa-filter"></i>
      </button>
    </div>
  </div>

    <div ref="tabulator" class="tabulator-custom data-grid">
  </div>

  <div ref="bottomToolbar" class="data-editor__footer d-flex align-items-center justify-content-end p-2">
    <p class="text-info mr-2" v-if="!hasPK" ><i class="fa-solid fa-circle-info"></i> The table has no primary key, existing rows can not be updated</p>
    <button type="submit" class="btn btn-success btn-sm mr-5" :disabled="!hasChanges"
      @click.prevent="applyChanges">
      {{this.applyBtnTitle()}}
    </button>
  </div>
</div>
</template>

<script>
import axios from 'axios'
import Knex from 'knex'
import { isEqual, zipObject } from 'lodash';
import { showToast } from "../notification_control";
import { v_queryRequestCodes } from '../query'
import { createRequest } from '../long_polling'
import { TabulatorFull as Tabulator} from 'tabulator-tables'

// TODO: run query in transaction
// TODO: remove old code, update request code numbers
// TODO: redraw the grid on font size change
// TODO: handle font size change - recalculate top/bottom toolbar dimensions


export default {
  name: "DataEditorTab",
  props: {
    dialect: String,
    schema: String,
    table: String,
    tab_id: String,
    database_index: Number,
    database_name: String,
    initial_filter: {
      type: String,
    default: ''
    }
  },
  data() {
    return {
      knex: null,
      tableColumns: [],
      tableData: [],
      tableDataLocal: [],
      queryFilter: '',
      rowLimit: 10,
      dataLoaded: false,
      heightSubtract: 200,
      tabulator: null
    };
  },
  computed: {
    talbleUnquoted() {
      return this.table.replace(/^"(.*)"$/, '$1')
    },
    hasChanges() {
      return this.pendingChanges.length > 0
    },
    pendingChanges() {
      if(this.hasPK)
        return this.tableDataLocal.filter((row) => row[0].is_dirty || row[0].is_new || row[0].is_deleted)
      else
        return this.tableDataLocal.filter((row) => row[0].is_new)
    },
    hasPK() {
      return this.tableColumns.some(c => c.is_primary)
    }
  },
  mounted() {
    this.tabulator = new Tabulator(this.$refs.tabulator, {
      height: `calc(100vh - ${this.heightSubtract}px)`,
      layout: 'fitDataStretch',
      data: [],
      columnDefaults: {
          headerHozAlign: "left",
          headerSort: false,
        },
      selectable: false,
      rowFormatter: this.rowFormatter
    })
    this.tabulator.on("cellEdited", this.cellEdited);

    this.knex = Knex({ client: this.dialect || 'postgres'})
    this.getTableColumns().then(this.getTableData)
    this.handleResize()
  },
  methods: {
    actionsFormatter(cell, formatterParams, onRendered) {
      const div = document.createElement("div");
      div.className = 'btn p-0';


      let cellData = cell.getValue()
      if (!cellData)
        return

      if (cellData.is_deleted || cellData.is_dirty) {
        const revertIcon = document.createElement('i')
        revertIcon.className = 'fas fa-rotate-left text-info'
        revertIcon.title = 'Revert'
        revertIcon.dataset.action = 'revert'
        div.appendChild(revertIcon);

      } else {
        const deleteIcon = document.createElement('i')
        deleteIcon.className = 'fas fa-circle-xmark text-danger'
        deleteIcon.title = 'Remove'
        deleteIcon.dataset.action = 'delete'
        div.appendChild(deleteIcon)
      }

      return div
    },
    rowFormatter(row) {
      let rowMeta = row.getData()[0]

      if (rowMeta) {
        if (rowMeta.is_dirty)
          row.getElement().classList.add('row-dirty')
        if (rowMeta.is_deleted)
          row.getElement().classList.add('row-deleted')
        if (rowMeta.is_new)
          row.getElement().classList.add('row-new')
      }
    },
    getTableColumns() {
      return axios
        .post("/get_table_columns/", {
          database_index: this.database_index,
          tab_id: this.tab_id,
          schema: this.schema,
          table: this.table
        })
        .then((response) => {
          this.tableColumns = response.data.columns
          this.queryFilter = `${this.$props.initial_filter} ${response.data.initial_orderby}`.trim()

          let actionsCol = {
            title: `<div data-action="add" class="btn p-0" title="Add column">
              <i data-action="add" class="fa-solid fa-circle-plus text-success"></i>
              </div>`,
            field: '0',
            frozen: 'true',
            hozAlign: 'center',
            formatter: this.actionsFormatter,
            cellClick: this.handleClick,
            headerClick: this.addRow,
          }
          let columns = this.tableColumns.map((col, idx) => {
            let prepend = col.is_primary ? '<i class="fas fa-key action-key text-secondary mr-1"></i>' : ''
            let title = `${prepend}<span>${col.name}</span><div class='font-weight-light'>${col.data_type}</div>`

            return {
              field: (idx + 1).toString(),
              title: title,
              formatter: this.cellFormatter,
              editor: "input",
              editable: false,
              cellDblClick: function (e, cell) {
                cell.edit(true);
              }
            }
          })
          columns.unshift(actionsCol)
          this.tabulator.setColumns(columns)

          this.dataLoaded = true
        })
        .catch((error) => {
          showToast("error", error.response.data)
        });
    },
    getTableData() {
      var message_data = {
        v_table: this.table,
        v_schema: this.schema,
        v_db_index: this.database_index,
        v_filter : this.queryFilter,
        v_count: this.rowLimit,
        v_conn_tab_id: v_connTabControl.selectedTab.id,
        v_tab_id: this.tab_id
      }

      var context = {
        tab_tag: null,
        callback: this.handleResponse.bind(this),
        start_time: new Date().getTime(),
        database_index: this.database_index
      }

      createRequest(v_queryRequestCodes.QueryEditData, message_data, context)
    },
    handleResize() {
      if(this.$refs === null)
        return

      this.heightSubtract =
        this.$refs.bottomToolbar.getBoundingClientRect().height +
        this.$refs.topToolbar.getBoundingClientRect().bottom
    },
    handleClick(e, cell) {
      let rowNum = cell.getRow().getPosition() - 1
      let action_type = e.target.dataset.action
      if (action_type) {
        let row = cell.getRow()
        let rowMeta = {}
        if (row.getCell(0)) {
          rowMeta = row.getCell(0).getValue()
        }
        if (action_type === 'delete')
          this.deleteRow(rowMeta, rowNum)
        if (action_type === 'revert')
          this.revertRow(rowMeta, rowNum)
      }
    },
    cellEdited(cell) {
      if (!this.hasPK)
        return
      // workaround when empty cell with initial null value is changed to empty string when starting editing
      if (cell.getOldValue() === null && cell.getValue() === '')
        cell.setValue(null)

      let rowData = cell.getRow().getData()
      let rowMeta = rowData[0]
      let originalRow = this.tableData.find((row) => row[0].initial_id == rowMeta.initial_id)

      if (originalRow) {
        debugger
        rowMeta.is_dirty = !isEqual(rowData.slice(1), originalRow.slice(1)) && !rowMeta.is_new
      }

    },
    handleResponse(response) {
      if(response.v_error == true) {
        showToast("error", response.v_data)
      } else {
        //store table data into original var, clone it to the working copy
        let pkIndex = this.tableColumns.findIndex((col) => col.is_primary) || 0
        this.tableData = response.v_data.rows.map((row) => {
          let rowMeta = {
            is_dirty: false,
            is_new: false,
            is_deleted: false,
            initial_id: row[pkIndex]
          }

          return [rowMeta].concat(row)
        })
        this.tableDataLocal = JSON.parse(JSON.stringify(this.tableData))
        this.tabulator.replaceData(this.tableDataLocal)
      }
    },
    handleSaveResponse(response) {
      if(response.v_error == true) {
        showToast("error", response.v_data)
      } else {
        showToast("success", 'data updated')
        this.getTableData()
      }
    },
    addRow() {
      let newRow = Array(this.tableColumns.length + 1).fill(null) //+1 adds an extra actions column
      let rowMeta = {
        is_dirty: false,
        is_new: true,
        is_deleted: false,
        initial_id: -1
      }

      newRow[0] = rowMeta
      this.tableDataLocal.unshift(newRow)
    },
    revertRow(rowMeta,rowNum) {
      let sourceRow = this.tableData.find((row) => row[0].initial_id == rowMeta.initial_id)
      this.tableDataLocal[rowNum] = JSON.parse(JSON.stringify(sourceRow))
    },
    deleteRow(rowMeta,rowNum) {
      if(rowMeta.is_new) {
        this.tableDataLocal.splice(rowNum, 1)
      } else {
        this.tableDataLocal[rowNum][0].is_deleted = true
      }
    },
    generateSQL() {
      let colNames = this.tableColumns.map(c => c.name)
      let deletes = []
      let inserts = []
      let updates = []
      let changes = this.pendingChanges
      let pkColName = 'id'

      if(this.hasPK)
        pkColName = this.tableColumns.find((col) => col.is_primary).name || 'id'

      changes.forEach(function(change) {
        let rowMeta = change[0]
        if(rowMeta.is_new) {
          inserts.push(zipObject(colNames, change.slice(1)))
        }
        if(rowMeta.is_dirty){
          let originalRow = this.tableData.find((row) => row[0].initial_id == rowMeta.initial_id).slice(1)
          let updateArgs = {}

          change.slice(1).forEach((changedCol, idx) => {
            if(changedCol!==originalRow[idx]) {
              updateArgs[colNames[idx]] = changedCol
            }
          })

          updates.push(this.knex(this.talbleUnquoted).where(pkColName, rowMeta.initial_id).update(updateArgs))
        }
      }, this)

      let deletableIds = changes.filter((c) => c[0].is_deleted).map((c) => {return c[0].initial_id})
      if(deletableIds.length > 0)
        deletes.push(this.knex(this.talbleUnquoted).whereIn(pkColName, deletableIds).del())

      let insQ = []
      if(inserts.length)
        insQ = this.knex(this.talbleUnquoted).insert(inserts)

      return [].concat(updates, insQ, deletes).join(';\n')
    },
    applyChanges() {
      let query = this.generateSQL()

      let message_data = {
        v_sql_cmd : query,
        v_sql_save : false,
        v_db_index: this.database_index,
        v_conn_tab_id: v_connTabControl.selectedTab.id,
        v_tab_id: this.tab_id,
        v_tab_db_id: this.database_index,
      }

      let context = {
        callback: this.handleSaveResponse.bind(this),
      }

      createRequest(v_queryRequestCodes.SaveEditData, message_data, context)
    },
    applyBtnTitle() {
      let count = this.pendingChanges.length

      if(count === 0)
        return 'No Changes'
      return `Apply ${count} ${(count > 1 || count == 0) ? 'changes' : 'change'}`
    }
  },
  watch: {
    tableDataLocal: {
      handler(newVal, oldVal) {
        this.tabulator.replaceData(this.tableDataLocal)
      },
      deep: true
    },
  }
};
</script>

<style scoped>
  .grid-scrollable {
    width: 100%;
  }
</style>
