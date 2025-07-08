<template>
<div class="data-editor p-2">
  <div ref="topToolbar" class="row">
    <div class="form-group col-10 align-content-center overflow-auto" style="max-height: 170px;">
        <label class="mb-2">
          <span class="fw-bold">Filter</span>
        </label>
      <DataEditorTabFilter :operators="dialectOperators" :updated-raw-query="updatedRawQuery" :columns="columnNames" :filters="queryFilters" @update="handleFilterUpdate"/>
    </div>
    <div class="form-group col-2">
      <div class="form" @submit.prevent>
        <label class="fw-bold mb-2" :for="`${tabId}_rowLimit`">Limit</label>
        <div class="d-flex">
          <select :id="`${tabId}_rowLimit`" v-model="rowLimit" class="form-select">
            <option v-for="(option, index) in [10, 100, 1000]"
              :key=index
              :value="option">
                {{option}} rows
            </option>
        </select>

        <button class="btn btn-primary mx-2" title="Load Data" @click="confirmGetTableData()">
          <i class="fa-solid fa-filter"></i>
        </button>
        </div>
      </div>
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
import isEqualWith from 'lodash/isEqualWith';
import zipObject from 'lodash/zipObject';
import forIn from 'lodash/forIn';
import isArray from 'lodash/isArray'
import escape from 'lodash/escape';
import { showToast } from "../notification_control";
import { queryRequestCodes, requestState } from '../constants'
import { createRequest } from '../long_polling'
import { TabulatorFull as Tabulator} from 'tabulator-tables'
import { emitter } from '../emitter';
import { settingsStore, tabsStore, messageModalStore } from '../stores/stores_initializer';
import DataEditorTabFilterList from './DataEditorTabFilterList.vue'
import { dataEditorFilterModes } from '../constants';
import { handleError } from '../logging/utils';
import dialects from './dialect-data';
import { extractOrderByClause } from '../utils';

// TODO: run query in transaction

function escapeColumnName(name) {
  if (name === "*") return name;
  const nestedMatch = name.match(/(.*?)(\[[0-9]\])/);
  return nestedMatch
    ? `${escapeColumnName(nestedMatch[1])}${nestedMatch[2]}`
    : `"${name.replace(/"/g, '""')}"`;
}



export default {
  name: "DataEditorTab",
  components: {
    DataEditorTabFilter: DataEditorTabFilterList
  },
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
      queryFilters: [{ column: "", operator: "=", value: "" }],
      rowLimit: 10,
      dataLoaded: false,
      heightSubtract: 200, //default safe value, recalculated in handleResize
      tabulator: null,
      maxInitialWidth: 200,
      queryState: requestState.Idle,
      rawQuery: "",
      updatedRawQuery: "",
      dialectData: {},
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
        return this.tableDataLocal.filter((row) => row["rowMeta"].is_dirty || row["rowMeta"].is_new || row["rowMeta"].is_deleted)
      else
        return this.tableDataLocal.filter((row) => row["rowMeta"].is_new)
    },
    hasPK() {
      return this.tableColumns.some(c => c.is_primary)
    },
    gridHeight() {
      return `calc(100vh - ${this.heightSubtract}px)`;
    },
    columnNames() {
      return this.tableColumns.map(col => col.name)
    },
    dialectOperators() {
      return this.dialectData?.operators ?? [];
    }
  },
  mounted() {
    this.dialectData = dialects[this.dialect];
    this.handleResize()
    let table = new Tabulator(this.$refs.tabulator, {
      placeholder: "No Data Available",
      layout: "fitDataFill",
      data: [],
      autoResize: false,
      columnDefaults: {
          headerHozAlign: "left",
          headerSort: false,
          maxInitialWidth: this.maxInitialWidth,
        },
      selectableRows: false,
      rowFormatter: this.rowFormatter,
      sortMode: 'remote',
      headerSortClickElement:"icon",
      ajaxURL: "http://fake",
      ajaxRequestFunc: this.getTableData,
    })

    table.on("tableBuilt", () => {
      this.tabulator = table;
      this.tabulator.on("cellEdited", this.cellEdited);
      this.knex = Knex({ client: this.dialect || 'postgres'})
      this.getTableColumns().then(() => {this.tabulator.setSort("0", "asc")});
    })

    emitter.on(`${this.tabId}_query_edit`, () => {
      this.confirmGetTableData();
    })

    emitter.on(`${this.tabId}_check_query_edit_status`, () => {
        if (this.queryState === requestState.Ready) {
          const tab = tabsStore.getSecondaryTabById(this.tabId, this.workspaceId);
          tab.metaData.isReady = false;
          tab.metaData.isLoading = false;
          this.queryState = requestState.Idle;
        }
      });

    settingsStore.$onAction((action) => {
      if(action.name === "setFontSize") {
        action.after(() => {
          requestAnimationFrame(() => {
            requestAnimationFrame(() => {
              this.handleResize();
            })
          })
        })
      }
    })
  },
  unmounted() {
    emitter.all.delete(`${this.tabId}_query_edit`);
    emitter.all.delete(`${this.tabId}_check_query_edit_status`);
  },
  updated() {
    if (tabsStore.selectedPrimaryTab?.metaData?.selectedTab?.id === this.tabId) {
      this.handleResize();
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
      row.getElement().classList.remove('row-deleted', 'row-dirty', 'row-new');
      let rowMeta = row.getData()["rowMeta"]
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

          let actionsCol = {
            title: `<div data-action="add" class="btn p-0" title="Add column">
              <i data-action="add" class="fa-solid fa-circle-plus text-success"></i>
              </div>`,
            field: 'rowMeta',
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
              field: (idx).toString(),
              title: title,
              editor: "input",
              editable: false,
              headerSort: true,
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
              formatter: this.cellFormatter,
            }
          })
          columns.unshift(actionsCol)
          this.tabulator.setColumns(columns)

          this.dataLoaded = true
        })
        .catch((error) => {
          handleError(error);
        });
    },
    buildQueryFilter() {
      if (this.mode === dataEditorFilterModes.MANUAL) {
        return this.processManualModeQuery();
      } 
      return this.processAutoModeFilters();
    },
    processAutoModeFilters() {
      const preprocessFilters = (rawFilters) => {
        return rawFilters
          .filter((f) => f.operator && f.column && f.value)
          .map((f) => ({
            ...f,
            value:
              f.operator === "in" ? f.value.split(/\s*,\s*/) : String(f.value),
          }));
      };

      const combineFilters = (filterStrings, filterObjects) => {
        return filterStrings.length === 0
          ? ""
          : filterStrings.reduce((acc, filter, idx) => {
              const logic = filterObjects[idx]?.condition || "AND";
              return `${acc} ${logic} ${filter}`;
            });
      };

      let queryFilter = ""
      const filters = preprocessFilters(this.queryFilters);
        if (filters?.length > 0) {
          const filterClauses = filters.map((filter) => {
            if (filter.operator === "in" && isArray(filter.value)) {
              const formattedValues = filter.value.map((val) => {
                return this.knex.raw("?", [val]).toQuery();
              });
              return `${escapeColumnName(
                filter.column
              )} ${filter.operator.toUpperCase()} (${formattedValues.join(",")})`;
            }
            const formattedValue = this.knex.raw("?", [filter.value]).toQuery();
            return `${escapeColumnName(
              filter.column
            )} ${filter.operator.toUpperCase()} ${formattedValue}`;
          });
          queryFilter = "WHERE " + combineFilters(filterClauses, filters);
        }
        return queryFilter
    },
    buildOrderByClause(params) {
      const parseOrderBy = (query) => {
        const orderByRegex = /ORDER\s+BY\s+([\w\s",.]+)$/i;
        const match = query.match(orderByRegex);

        if (!match) return [];

        return match[1].split(",").map((part) => {
          let [column, direction] = part.trim().split(/\s+/);
          direction = direction ? direction.toLowerCase() : "asc";

          if (column.includes(".")) {
            column = column.split(".")[1];
          }

          return { name: column, dir: direction };
        });
      };

      const mapColumnsToIndexes = (orderByColumns, tableColumns) => {
        return orderByColumns
          .map((column) => {
            const columnIndex = tableColumns.findIndex(
              (col) => col.name === column.name
            );
            return columnIndex !== -1
              ? { column: columnIndex, dir: column.dir }
              : null;
          })
          .filter((item) => item !== null);
      };

      const trimmedQuery = this.rawQuery.trim().toLowerCase();
      const { queryFilterCleaned, orderByClause } =
        extractOrderByClause(trimmedQuery);

      const orderByColumns = parseOrderBy(orderByClause);

      if (!params?.sort && orderByClause) {
        const result = mapColumnsToIndexes(orderByColumns, this.tableColumns);
        this.tabulator.setSort(result);
        return null;
      }

      if (params?.sort && params.sort.length === 1) {
        let sortColumnName = this.columnNames[params.sort[0].field];
        const updatedOrderClause = `ORDER BY ${sortColumnName} ${params.sort[0].dir}`;
        this.updatedRawQuery = `${queryFilterCleaned} ${updatedOrderClause}`;
        return updatedOrderClause;
      }

      if (params?.sort) {
        return (
          "ORDER BY " +
          params.sort
            .map((item) => {
              const columnName = this.columnNames[item.field];
              return `${escapeColumnName(
                columnName
              )} ${item.dir.toUpperCase()}`;
            })
            .join(",")
        );
      } else {
        this.tabulator.setSort("0", "asc");
        return null;
      }
    },
    processManualModeQuery() {
      const trimmedQuery = this.rawQuery.trim().toLowerCase();
      const hasWhere = trimmedQuery.startsWith("where ");
      const hasOrderBy = trimmedQuery.includes("order by");
      const { queryFilterCleaned } = extractOrderByClause(trimmedQuery);

      if (!trimmedQuery) return ""
      if (hasOrderBy) {
        if (!!queryFilterCleaned) return hasWhere ? queryFilterCleaned : `WHERE ${queryFilterCleaned}`;
        return ""
      } else {
        return hasWhere ? trimmedQuery : `WHERE ${trimmedQuery}`;
      }
    },
    sendDataRequest(finalFilter) {
      var message_data = {
        table: this.table,
        schema: this.schema,
        db_index: this.databaseIndex,
        query_filter: finalFilter,
        count: this.rowLimit,
        workspace_id: this.workspaceId,
        tab_id: this.tabId,
      };

      var context = {
        tab_tag: null,
        callback: this.handleResponse.bind(this),
        start_time: new Date().getTime(),
        database_index: this.databaseIndex,
      };

      createRequest(queryRequestCodes.QueryEditData, message_data, context);
      const tab = tabsStore.getSecondaryTabById(this.tabId, this.workspaceId);

      this.queryState = requestState.Executing;
      setTimeout(() => {
        if (this.queryState === requestState.Executing) 
          tab.metaData.isLoading = true;
      }, 1000);
      tab.metaData.isReady = false;
    },
    getTableData(_url, _config, params) {
      const queryFilter = this.buildQueryFilter();
      const orderBy = this.buildOrderByClause(params);

      if (orderBy === null) {
        return 
      }

      const finalFilter = `${queryFilter} ${orderBy}`;

      this.sendDataRequest(finalFilter)

      // we need to return promise for tabulator ajaxRequestFunc to work
      return new Promise((resolve, reject) => {
        resolve([])
      })
    },
    handleResize() {
      if(this.$refs === null)
        return

      this.heightSubtract =
        this.$refs.bottomToolbar.getBoundingClientRect().height +
        this.$refs.topToolbar.getBoundingClientRect().bottom
    },
    handleClick(e, cell) {
      let rowNum = cell.getRow().getIndex()
      let action_type = e.target.dataset.action
      if (action_type) {
        let row = cell.getRow()
        let rowMeta = {}
        if (row.getCell('rowMeta')) {
          rowMeta = row.getCell('rowMeta').getValue()
        }
        if (action_type === 'delete')
          this.deleteRow(rowMeta, rowNum)
        if (action_type === 'revert') {
          this.revertRow(rowMeta, rowNum)

        }
      }
    },
    cellEdited(cell) {
      if (!this.hasPK)
        return
      // workaround when empty cell with initial null value is changed to empty string when starting editing
      if (cell.getOldValue() === null && cell.getValue() === '')
        cell.setValue(null)

      let rowData = cell.getRow().getData()
      let rowMeta = rowData["rowMeta"]
      let originalRow = this.tableData.find((row) => row["rowMeta"].initial_id == rowMeta.initial_id)

      if (originalRow) {
        rowMeta.is_dirty = !isEqualWith(rowData, originalRow, (val1, val2, key) => {
          if (key === 'rowMeta') {
            return true
          }
        }) && !rowMeta.is_new
        cell.getRow().reformat()
      }

    },
    handleResponse(response) {
      const tab = tabsStore.getSecondaryTabById(this.tabId, this.workspaceId);
      tab.metaData.isLoading = false;
      if (
          this.workspaceId === tabsStore.selectedPrimaryTab.id &&
          this.tabId === tabsStore.selectedPrimaryTab.metaData.selectedTab.id
        ) {
          this.queryState = requestState.Idle;

          tab.metaData.isReady = false;
        } else {
          this.queryState = requestState.Ready;

          tab.metaData.isReady = true;
        }

      if(response.error == true) {
        showToast("error", response.data)
      } else {
        //store table data into original var, clone it to the working copy
        let pkIndex = this.tableColumns.findIndex((col) => col.is_primary) || 0
        this.tableData = response.data.rows.map((row, index) => {
          let rowMeta = {
            is_dirty: false,
            is_new: false,
            is_deleted: false,
            initial_id: row[pkIndex]
          }
          return {id: index, rowMeta: rowMeta, ...row}
        })
        this.tableDataLocal = JSON.parse(JSON.stringify(this.tableData))
        this.tabulator.replaceData(this.tableDataLocal)
      }
    },
    handleSaveResponse(response) {
      const tab = tabsStore.getSecondaryTabById(this.tabId, this.workspaceId);
      tab.metaData.isReady = false;
      tab.metaData.isLoading = false;
      this.queryState = requestState.Idle;
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
      let newRow = Array(this.tableColumns.length + 1).fill(null); //+1 adds an extra actions column
      let rowMeta = {
        is_dirty: false,
        is_new: true,
        is_deleted: false,
        initial_id: -1,
      };

      let newRowId = this.tabulator.getRows().length;
      const newRowObject = { rowMeta: rowMeta, id: newRowId, ...newRow };
      this.tabulator.addData(newRowObject, true);
      this.tableDataLocal.unshift(this.tabulator.getRow(newRowId).getData());
    },
    revertRow(rowMeta, rowNum) {
      let sourceRow = this.tableData.find(
        (row) => row["rowMeta"].initial_id == rowMeta.initial_id
      );
      let copyRow = JSON.parse(JSON.stringify(sourceRow));
      this.tabulator.updateData([{ id: rowNum, ...copyRow }]).then(() => {
        this.tabulator.getRow(rowNum).reformat();
      });
    },
    deleteRow(rowMeta, rowNum) {
      if (rowMeta.is_new) {
        this.tabulator.deleteRow(rowNum).then(() => {
          this.tableDataLocal = this.tableDataLocal.filter(
            (row) => row.id !== rowNum
          );
        });
      } else {
        rowMeta.is_deleted = true;
        this.tabulator
          .updateData([{ id: rowNum, rowMeta: rowMeta }])
          .then(() => {
            this.tabulator.getRow(rowNum).reformat();
          });
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
        let rowMeta = change["rowMeta"]
        let {rowMeta:_, ...changeWitNoRowmeta} = change
        if(rowMeta.is_new) {
          inserts.push(zipObject(colNames, Object.values(change)))
        }
        if(rowMeta.is_dirty){
          
          let originalRow = this.tableData.find((row) => row["rowMeta"].initial_id == rowMeta.initial_id)
          let updateArgs = {}

          forIn(changeWitNoRowmeta, (value, key) => {
            if (value !== originalRow[key]) {
              updateArgs[colNames[key]] = value
            }
          })

          updates.push(this.knex(this.talbleUnquoted).withSchema(this.schema).where(pkColName, rowMeta.initial_id).update(updateArgs))
        }
      }, this)
      let deletableIds = changes.filter((c) => c["rowMeta"].is_deleted).map((c) => {return c["rowMeta"].initial_id})
      if(deletableIds.length > 0)
        deletes.push(this.knex(this.talbleUnquoted).withSchema(this.schema).whereIn(pkColName, deletableIds).del())

      let insQ = []
      if(inserts.length)
        insQ = this.knex(this.talbleUnquoted).withSchema(this.schema).insert(inserts)

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

      const tab = tabsStore.getSecondaryTabById(this.tabId, this.workspaceId);
  
      this.queryState = requestState.Executing;
      setTimeout(() => {
        if (this.queryState === requestState.Executing) 
          tab.metaData.isLoading = true;
      }, 1000);

      tab.metaData.isReady = false;
    },
    applyBtnTitle() {
      let count = this.pendingChanges.length

      if(count === 0)
        return 'No Changes'
      return `Apply ${count} ${(count > 1 || count == 0) ? 'changes' : 'change'}`
    },
    handleFilterUpdate({ mode, filters, rawQuery }) {
      this.$nextTick(() => {
        this.handleResize();
      })
      this.mode = mode;
      if (filters) this.filters = filters;
      if (rawQuery !== undefined) {
        this.rawQuery = rawQuery;
      } 
    },
    confirmGetTableData(url, config, params) {
      if (this.hasChanges) {
        messageModalStore.showModal(
          "Are you sure you wish to discard the current changes?",
          () => {
            this.getTableData(url, config, params);
          },
          null
        );
      } else {
        this.getTableData(url, config, params);
      }

      return new Promise((resolve, reject) => {
        resolve(this.tableDataLocal);
      });
    },
    cellFormatter(cell, params, onRendered) {
      let cellVal = cell.getValue()
      if (!!cellVal && cellVal?.length > 1000) {
          let filtered = escape(cellVal.slice(0, 1000).toString().replace(/\n/g, ' ↲ '))
          return `${filtered}...`;
        }
      return cellVal
    }
  },
  watch: {
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
