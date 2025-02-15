<template>
  <div ref="resultDiv" :id="`query_result_tabs_container_${tabId}`" class="omnidb__query-result-tabs pe-2">
    <button :id="`bt_fullscreen_${tabId}`" style="position: absolute; top: 0.25rem; right: 0.5rem" type="button"
      class="btn btn-sm btn-icon btn-icon-secondary" @click="toggleFullScreen()">
      <i class="fas fa-expand"></i>
    </button>

    <!-- DATA, MESSAGE, EXPLAIN tabs-->
    <div :id="`query_result_tabs_${tabId}`" class="h-100">
      <div class="omnidb__tab-menu ps-2">
        <div class="nav nav-tabs justi" role="tablist">
          <a ref="dataTab" class="omnidb__tab-menu__link nav-item nav-link active" :id="`nav_data_tab_${tabId}`"
          data-bs-toggle="tab" :data-bs-target="`#nav_data_${tabId}`" type="button" role="tab"
            :aria-controls="`nav_data_${tabId}`" aria-selected="true">
            <span class="omnidb__tab-menu__link-name">Data</span>
          </a>
          <template v-if="postgresqlDialect">
            <a ref="messagesTab" class="omnidb__tab-menu__link nav-item nav-link" :id="`nav_messages_tab_${tabId}`"
            data-bs-toggle="tab" :data-bs-target="`#nav_messages_${tabId}`" type="button" role="tab"
              :aria-controls="`nav_messages_${tabId}`" aria-selected="true">
              <span class="omnidb__tab-menu__link-name">
                Messages
                <span v-if="noticesCount" class="badge badge-pill badge-primary">{{ noticesCount }}</span>
              </span>
            </a>
            <a ref="explainTab" class="nav-item nav-link omnidb__tab-menu__link" :id="`nav_explain_tab_${tabId}`"
            data-bs-toggle="tab" :data-bs-target="`#nav_explain_${tabId}`" type="button" role="tab"
              :aria-controls="`nav_explain_${tabId}`" aria-selected="false">
              <span class="omnidb__tab-menu__link-name"> Explain </span>
            </a>
          </template>
        </div>
      </div>

      <div ref="tabContent" class="tab-content pb-3">
        <div class="tab-pane active pt-2" :id="`nav_data_${tabId}`" role="tabpanel"
          :aria-labelledby="`nav_data_tab_${tabId}`">
          <div class="result-div">
            <template v-if="exportFileName && exportDownloadName">
              The file is ready.
              <a class="link_text" :href="exportFileName" :download="exportDownloadName">Save</a>
            </template>
            <template v-else-if="errorMessage" class="error_text" style="white-space: pre">
              {{ errorMessage }}
            </template>
            <template v-else-if="queryInfoText">
              <div class="query_info">
                {{ queryInfoText }}
              </div>
            </template>
            <div v-show="showTable" ref="tabulator" class="tabulator-custom"></div>
          </div>
        </div>
        <template v-if="postgresqlDialect">
          <div class="tab-pane" :id="`nav_messages_${tabId}`" role="tabpanel"
            :aria-labelledby="`nav_messages_tab_${tabId}`">
            <div class="messages__wrap p-2">
              <div class="result-div">
                <p v-for="notice in notices">{{ notice }}</p>
              </div>
            </div>
          </div>
          <ExplainTabContent :tab-id="tabId" :query="query" :plan="plan" />
        </template>
      </div>
    </div>
  </div>
</template>

<script>
import ExplainTabContent from "./ExplainTabContent.vue";
import { queryModes, tabStatusMap } from "../constants";
import { emitter } from "../emitter";
import { TabulatorFull as Tabulator } from "tabulator-tables";
import { settingsStore, tabsStore, cellDataModalStore } from "../stores/stores_initializer";
import { showToast } from "../notification_control";
import escape from 'lodash/escape';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import mean from 'lodash/mean';
import { Tab } from "bootstrap";

export default {
  components: {
    ExplainTabContent,
  },
  props: {
    tabId: String,
    workspaceId: String,
    editorContent: String,
    dialect: String,
    tabStatus: Number,
    resizeDiv: Boolean,
    blockSize: Number,
  },
  watch: {
    resizeDiv(newValue, oldValue) {
      if (newValue) {
        this.handleResize();
        if (this.table) this.table.redraw();
        this.$emit("resized");
      }
    },
  },
  emits: ["enableExplainButtons", "runExplain", "showFetchButtons", "resized"],
  data() {
    return {
      errorMessage: "",
      exportFileName: "",
      exportDownloadName: "",
      notices: [],
      queryInfoText: "",
      tableSettings: {
        data: [],
        placeholderHeaderFilter: "No Matching Data",
        autoResize: false,
        selectableRows: true,
        height: "100%",
        layout: "fitDataStretch",
        columnDefaults: {
          headerHozAlign: "left",
          headerSort: false,
          maxInitialWidth: 200,
        },
        clipboard: "copy",
        clipboardCopyRowRange: "selected",
        clipboardCopyConfig: {
          columnHeaders: false, //do not include column headers in clipboard output
        },
      },
      query: "",
      plan: "",
      table: null,
      heightSubtract: 200,
      colWidthArray: [],
    };
  },
  computed: {
    queryModes() {
      return queryModes;
    },
    postgresqlDialect() {
      return this.dialect === "postgresql";
    },
    noticesCount() {
      return this.notices.length;
    },
    showTable() {
      return !(
        (!!this.exportFileName && !!this.exportDownloadName) || !!this.errorMessage 
      );
    },
    resultTabHeight() {
      return `calc(100vh - ${this.heightSubtract}px)`;
    },
  },
  mounted() {
    this.handleResize();
    if (this.dialect === "postgresql") {
      
      this.$refs.explainTab.addEventListener("shown.bs.tab", () => {
        this.$emit("enableExplainButtons");
        if (!(this.tabStatus === tabStatusMap.RUNNING))
          this.$emit("runExplain");
      });

      this.$refs.explainTab.addEventListener("hidden.bs.tab", () => {
        this.$emit("enableExplainButtons");
      });
    }

    window.addEventListener("resize", () => {
      if (
        tabsStore.selectedPrimaryTab?.metaData?.selectedTab?.id !== this.tabId
      )
        return;
      this.handleResize();
    });

    let table = new Tabulator(this.$refs.tabulator, this.tableSettings);
    table.on("tableBuilt", () => {
      this.table = table;
    });
    settingsStore.$onAction((action) => {
      if (action.name === "setFontSize") {
        if (
          tabsStore.selectedPrimaryTab?.metaData?.selectedTab?.id !== this.tabId
        )
          return;
        action.after(() => {
          requestAnimationFrame(() => {
            requestAnimationFrame(() => {
              this.handleResize();
              this.table.redraw();
            });
          });
        });
      }
    });

    this.$refs.dataTab.addEventListener("shown.bs.tab", (event) => {
      this.table.redraw();
    });
  },
  updated() {
    this.handleResize();
  },
  methods: {
    cellFormatter(cell, params, onRendered) {
      let cellVal = cell.getValue()
      if (isNil(cellVal)) {
        return '<span class="text-muted">[null]</span>'
      }

      if(isEmpty(cellVal)) {
        return '<span class="text-muted">[empty]</span>'
      }

      let filtered = escape(cellVal.toString().replace(/\n/g, ' ↲ '))
      return filtered
    },
    renderResult(data, context) {
      this.clearData();
      if (data.error && context.cmd_type !== "explain") {
        this.errorMessage = data.data.message;
      } else {
        if (context.cmd_type === "explain") {
          this.showExplainTab(data);
        } else if (!!context.cmd_type && context.cmd_type.includes("export")) {
          this.showExport(data.data);
        } else {
          this.showDataTab(data.data, context);

          let mode = ["CREATE", "DROP", "ALTER"];
          if (!!data.data.status && isNaN(data.data.status)) {
            let status = data.data.status?.split(" ");
            if (mode.includes(status[0])) {
              let node_type = status[1]
                ? `${status[1].toLowerCase()}_list`
                : null;

              if (!!node_type)
                emitter.emit(`refreshTreeRecursive_${this.workspaceId}`, node_type);
            }
          }
        }
      }
    },
    showExplainTab(data) {
      Tab.getOrCreateInstance(this.$refs.explainTab).show()
      if (data?.error) {
        this.query = this.editorContent;
        this.plan = data.data.message;
        return;
      }

      // Adjusting data.
      let explain_text = data.data.data.join("\n");

      if (explain_text.length > 0) {
        this.query = this.editorContent;
        this.plan = explain_text;
      }
    },
    showExport(data) {
      Tab.getOrCreateInstance(this.$refs.dataTab).show()

      this.exportFileName = data.file_name;
      this.exportDownloadName = data.download_name;
    },
    showDataTab(data, context) {
      Tab.getOrCreateInstance(this.$refs.dataTab).show()

      if (data.notices.length) {
        this.notices = data.notices;
      }

      if (
        data.data.length >= 50 && context.mode === this.queryModes.DATA_OPERATION ||
        data.data.length >= this.blockSize && context.mode === this.queryModes.FETCH_MORE
      ) {
        this.$emit("showFetchButtons", true);
      } else {
        this.$emit("showFetchButtons", false);
      }

      if (context.mode === this.queryModes.DATA_OPERATION) {
        this.showDataOperationResult(data);
      } else if (
        context.mode === this.queryModes.FETCH_MORE ||
        context.mode === this.queryModes.FETCH_ALL
      ) {
        this.fetchData(data);
      } else {
        this.queryInfoText = data.status;
      }
    },
    showDataOperationResult(data) {
      if (data.data.length === 0) {
        if (data.col_names.length === 0) {
          this.queryInfoText = data.status ? data.status : "Done";
        }
      }
      this.updateTableData(data);
    },
    prepareColumns(colNames, colTypes) {
      let cellContextMenu = [
        {
          label:
            '<div style="position: absolute;"><i class="fas fa-copy cm-all" style="vertical-align: middle;"></i></div><div style="padding-left: 30px;">Copy</div>',
          action: function (e, cell) {
            cell.getTable().copyToClipboard("selected");
          },
        },
        {
          label:
            '<div style="position: absolute;"><i class="fas fa-edit cm-all" style="vertical-align: middle;"></i></div><div style="padding-left: 30px;">View Content</div>',
          action: (e, cell) => {
            cellDataModalStore.showModal(cell.getValue())
          },
        },
      ];
      let columns = colNames.map((col, idx) => {
        let formatTitle = function(col, idx) {
          if(colTypes?.length === 0 )
            return col
          return `${col}<br><span class='subscript'>${colTypes[idx]}</span>`
        }

        return {
          title: formatTitle(col, idx),
          field: idx.toString(),
          resizable: "header",
          formatter: this.cellFormatter,
          contextMenu: cellContextMenu,
          headerDblClick: (e, column) => {
            if (
              column.getWidth() >
              this.tableSettings.columnDefaults.maxInitialWidth
            ) {
              column.setWidth(
                this.tableSettings.columnDefaults.maxInitialWidth
              );
            } else {
              column.setWidth(true);
            }
          },
          headerTooltip: 'double-click to maximize/minimize',
        };
      });

      let headerMenu = [
        {
          label:"Adaptive",
          action: () => {
            this.customLayout = 'adaptive'
            this.applyLayout()
          }
        },
        {
          label:"Compact",
          action: () => {
            this.customLayout = 'compact'
            this.applyLayout()
          }
        },{
          label:"Fit Content",
          action:() => {
            this.customLayout = 'fitcontent'
            this.applyLayout()
          }
        },{
          label:"Reset Layout",
          action:() => {
            this.customLayout = undefined
            this.table.blockRedraw();
            this.table.setColumns(columns);
            this.table.restoreRedraw();
          }
        },
      ]

      columns.unshift({
        formatter: "rownum",
        hozAlign: "center",
        minWidth: 55,
        frozen: true,
        headerMenu: headerMenu,
        headerMenuIcon:'<i class="actions-menu fa-solid fa-ellipsis-vertical p-2"></i>',
        headerTooltip: 'Layout'
      });
      return columns
    },
    updateTableData(data) {
      const columns = this.prepareColumns(data.col_names, data.col_types)
      this.table.destroy()
      this.tableSettings.data = data.data
      this.tableSettings.columns = columns
      let table = new Tabulator(this.$refs.tabulator, this.tableSettings);
      table.on("renderStarted", () => {
        if (this.customLayout === undefined || this.colWidthArray.length === 0) return
        this.table.getColumns().forEach((col, idx) => {
          if(idx > 0) {
            col.setWidth(this.colWidthArray[idx-1])
          }
        });
      })
      table.on("tableBuilt", () => {
        this.table = table;
        if (this.customLayout !== undefined && this.colWidthArray.length !== 0) {
          
          this.table.getColumns().forEach((col, idx) => {
            if(idx > 0) {
              col.setWidth(this.colWidthArray[idx-1])
            }
          });
        }
      });

      table.on(
        "cellDblClick",
        function (e, cell) {
          if (cell.getValue()) cellDataModalStore.showModal(cell.getValue())
        }
      );
    },
     applyLayout() {
      this.colWidthArray = []
      if(this.customLayout === undefined)
        return

      this.table.blockRedraw();

      this.table.getColumns().forEach((col, idx) => {
        if(idx > 0) {
          if(this.customLayout == 'adaptive') {
            let widths = col.getCells().map((cell) => {return cell.getElement().scrollWidth}).filter((el) => el > 0)
            col.setWidth(mean(widths))
            this.colWidthArray.push(mean(widths))
          }

          if(this.customLayout == 'compact') {
            col.setWidth(100);
            this.colWidthArray.push(100)
          }

          if(this.customLayout == 'fitcontent') {
            col.setWidth(true);
            this.colWidthArray.push(true)
          }
        }
      });

      this.table.restoreRedraw();
    },
    fetchData(data) {
      let initialData = this.table.getData();
      const allData = [...initialData, ...data.data]
      let rowsToAppend = data.data.length

      // destroy and recreate table instance if there is a lot of data to append
      if(rowsToAppend > 1000) {
        const scrollTop = this.table.rowManager.scrollTop
        const scrollLeft = this.table.rowManager.scrollLeft
        this.tableSettings.data = allData;
        this.tableSettings.columns = this.prepareColumns(data.col_names, data.col_types)

        this.table.destroy()
        let table = new Tabulator(this.$refs.tabulator, this.tableSettings);
        table.on("tableBuilt", () => {
          this.table = table;
          this.applyLayout();
          this.table.rowManager.element.scrollTop = scrollTop;
          this.table.rowManager.scrollLeft = scrollLeft;
        });

        table.on(
          "cellDblClick",
          function (e, cell) {
            if (cell.getValue()) cellDataModalStore.showModal(cell.getValue())
          }
        );
      } else {
        this.table.addData(data.data);
      }


    },
    clearData() {
      this.notices = [];
      this.queryInfoText = "";
      this.exportDownloadName = "";
      this.exportFileName = "";
      this.errorMessage = "";
    },
    toggleFullScreen() {
      this.$refs.resultDiv.classList.toggle("omnidb__panel-view--full");
      this.handleResize();
    },
    handleResize() {
      if (this.$refs?.tabContent === null) return;

      this.heightSubtract = this.$refs.tabContent.getBoundingClientRect().top;
    },
  },
};
</script>

<style scoped>
.tab-content,
.messages__wrap {
  height: v-bind(resultTabHeight);
}

.tab-pane,
.result-div {
  height: 100%;
}

.result-div {
  overflow: auto;
}
</style>
