<template>
<div class="data-editor p-2">
  <div ref="topToolbar" class="row">
    <div class="form-group col-9">
      <form class="form" @submit.prevent>
        <label class="mb-2" :for="`${tabId}_queryFilter`">
          <span class="fw-bold">Filter</span> <span class='text-info'> select</span> * <span class='text-info'>from</span> {{this.schema ? `${this.schema}.${this.table}` : `${this.table}`}}
        </label>
        <input :id="`${tabId}_queryFilter`" v-model.trim="queryFilter" class="form-control" name="filter"
           placeholder="extra filter criteria" />
      </form>
    </div>
    <div class="form-group col-2">
      <form class="form" @submit.prevent>
        <label class="fw-bold mb-2" :for="`${tabId}_rowLimit`">Limit</label>
        <select :id="`${tabId}_rowLimit`" v-model="rowLimit" class="form-select">
            <option v-for="(option, index) in [10, 100, 1000]"
              :key=index
              :value="option">
                {{option}} rows
            </option>
        </select>
      </form>
    </div>
    <div class="form-group col-1 d-flex align-items-end ps-0">
      <button class="btn btn-primary me-2" title="Load Data" @click="getTableData()">
        <i class="fa-solid fa-filter"></i>
      </button>
    </div>
  </div>

    <div ref="tabulator" class="tabulator-custom data-grid grid-height">
  </div>

  <div ref="bottomToolbar" class="data-editor__footer d-flex align-items-center justify-content-end p-2">
    <p class="text-info me-2" v-if="!hasPK" ><i class="fa-solid fa-circle-info"></i> The table has no primary key, existing rows can not be updated</p>
    <button type="submit" class="btn btn-success btn-sm me-5" :disabled="!hasChanges"
      @click.prevent="applyChanges">
      {{this.applyBtnTitle()}}
    </button>
  </div>
</div>
</template>

<script>
import axios from 'axios'
import Knex from 'knex'
import isEqual from 'lodash/isEqual';
import zipObject from 'lodash/zipObject';
import { showToast } from "../notification_control";
import { queryRequestCodes } from '../constants'
import { createRequest } from '../long_polling'
import { TabulatorFull as Tabulator} from 'tabulator-tables'
import { emitter } from '../emitter';
import { settingsStore, tabsStore } from '../stores/stores_initializer';

// TODO: run query in transaction

export default {
  name: "DataEditorTab",
  props: {
    dialect: String,
    schema: String,
    table: String,
    tabId: String,
    workspaceId: String,
    databaseIndex: Number,
    databaseName: String,
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
      heightSubtract: 200, //default safe value, recalculated in handleResize
      tabulator: null,
      maxInitialWidth: 200,
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
    },
    gridHeight() {
      return `calc(100vh - ${this.heightSubtract}px)`;
    },
  },
  mounted() {
    this.handleResize()
    let table = new Tabulator(this.$refs.tabulator, {
      layout: 'fitDataStretch',
      data: [],
      autoResize: false,
      columnDefaults: {
          headerHozAlign: "left",
          headerSort: false,
          maxInitialWidth: this.maxInitialWidth,
        },
      selectableRows: false,
      rowFormatter: this.rowFormatter
    })

    table.on("tableBuilt", () => {
      this.tabulator = table;
      this.tabulator.on("cellEdited", this.cellEdited);
    })

    this.knex = Knex({ client: this.dialect || 'postgres'})
    this.getTableColumns().then(this.getTableData)

    emitter.on(`${this.tabId}_query_edit`, () => {
      this.getTableData()
    })

    settingsStore.$onAction((action) => {
      if(action.name === "setFontSize") {
        action.after(() => {
          requestAnimationFrame(() => {
            requestAnimationFrame(() => {
              this.handleResize()
              this.tabulator.redraw();
            })
          })
        })
      }
    })
  },
  unmounted() {
    emitter.all.delete(`${this.tabId}_query_edit`);
  },
  updated() {
    if (tabsStore.selectedPrimaryTab?.metaData?.selectedTab?.id === this.tabId) {
      this.handleResize();
      if (this.tabulator)
        this.tabulator.redraw();
    }
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
          database_index: this.databaseIndex,
          workspace_id: this.workspaceId,
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
            let prepend = col.is_primary ? '<i class="fas fa-key action-key text-secondary me-1"></i>' : ''
            let title = `${prepend}<span>${col.name}</span><div class='fw-light'>${col.data_type}</div>`

            return {
              field: (idx + 1).toString(),
              title: title,
              formatter: this.cellFormatter,
              editor: "input",
              editable: false,
              cellDblClick: function (e, cell) {
                cell.edit(true);
              },
              headerDblClick: (e, column) => {
                if (column.getWidth() > this.maxInitialWidth) {
                  column.setWidth(this.maxInitialWidth);
                } else {
                  column.setWidth(true);
                }
              },
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
        table: this.table,
        schema: this.schema,
        db_index: this.databaseIndex,
        query_filter : this.queryFilter,
        count: this.rowLimit,
        workspace_id: this.workspaceId,
        tab_id: this.tabId
      }

      var context = {
        tab_tag: null,
        callback: this.handleResponse.bind(this),
        start_time: new Date().getTime(),
        database_index: this.databaseIndex
      }

      createRequest(queryRequestCodes.QueryEditData, message_data, context)
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
        rowMeta.is_dirty = !isEqual(rowData.slice(1), originalRow.slice(1)) && !rowMeta.is_new
      }

    },
    handleResponse(response) {
      if(response.error == true) {
        showToast("error", response.data)
      } else {
        //store table data into original var, clone it to the working copy
        let pkIndex = this.tableColumns.findIndex((col) => col.is_primary) || 0
        this.tableData = response.data.rows.map((row) => {
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
      if(response.error == true) {
        showToast("error", response.data)
      } else {
        showToast("success", 'data updated')
        setTimeout(() => {
          this.getTableData()
        }, 100);
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
        sql_cmd : query,
        sql_save : false,
        db_index: this.databaseIndex,
        workspace_id: this.workspaceId,
        tab_id: this.tabId,
        tab_db_id: this.databaseIndex,
      }

      let context = {
        callback: this.handleSaveResponse.bind(this),
      }

      createRequest(queryRequestCodes.SaveEditData, message_data, context)
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
    hasChanges() {
      const tab = tabsStore.getSecondaryTabById(this.tabId, this.workspaceId);
      if (tab) {
        tab.metaData.hasUnsavedChanges = this.hasChanges;
      }
    },
  }
};
</script>

<style scoped>
  .grid-scrollable {
    width: 100%;
  }

  .grid-height {
    height: v-bind(gridHeight);
    
  }
</style>
