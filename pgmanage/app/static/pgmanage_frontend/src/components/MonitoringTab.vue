<template>
  <div class="p-2">
    <div ref="topToolbar">
      <button
        class="btn btn-primary btn-sm my-2 me-1"
        title="Refresh"
        @click="refreshMonitoring"
      >
        <i class="fas fa-sync-alt me-2"></i>Refresh
      </button>
      <span class="query_info"> Number of records: {{ dataLength }} </span>
    </div>
    <div ref="tabulator" class="tabulator-custom grid-height pb-3"></div>
  </div>
</template>

<script>
import { TabulatorFull as Tabulator } from "tabulator-tables";
import axios from "axios";
import { showToast } from "../notification_control";
import { emitter } from "../emitter";
import { messageModalStore, settingsStore, cellDataModalStore } from "../stores/stores_initializer";

export default {
  name: "MonitoringTab",
  props: {
    query: String,
    databaseIndex: Number,
    workspaceId: String,
    dialect: String,
  },
  data() {
    return {
      table: null,
      dataLength: 0,
      heightSubtract: 150,
    };
  },
  computed: {
    gridHeight() {
      return `calc(100vh - ${this.heightSubtract}px)`;
    },
  },
  mounted() {
    this.handleResize();
    this.setupTable();

    settingsStore.$onAction((action) => {
      if (action.name === "setFontSize") {
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
  },
  updated() {
    this.handleResize();
    this.table.redraw();
  },
  methods: {
    setupTable() {
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

      this.table = new Tabulator(this.$refs.tabulator, {
        autoColumns: true,
        layout: "fitDataStretch",
        autoResize: false,
        columnDefaults: {
          headerHozAlign: "left",
          headerSort: false,
        },
        autoColumnsDefinitions: (definitions) => {
          //definitions - array of column definition objects

          definitions.forEach((column) => {
            column.contextMenu = cellContextMenu;
          });

          let updatedDefinitions = definitions.filter(
            (column) => column.title != "actions"
          );

          updatedDefinitions.unshift({
            title: "actions",
            field: "actions",
            formatter: this.actionsFormatter,
            hozAlign: "center",
            frozen: true,
            clipboard: false,
          });
          updatedDefinitions.unshift({
            formatter: "rownum",
            hozAlign: "center",
            width: 40,
            frozen: true,
          });

          return updatedDefinitions;
        },
        selectableRows: true,
        clipboard: "copy",
        clipboardCopyConfig: {
          columnHeaders: false, //do not include column headers in clipboard output
        },
        clipboardCopyRowRange: "selected",
      });
      this.refreshMonitoring();
    },
    actionsFormatter(cell, formatterParams, onRendered) {
      let sourceDataRow = cell.getRow().getData();
      let actionsWrapper = document.createElement("div");

      cell.getValue().forEach((actionItem) => {
        let iconClassName;
        if (actionItem.icon.includes("fa-times")) {
          iconClassName = `${actionItem.icon} text-danger`;
        } else {
          iconClassName = `${actionItem.icon} omnidb__theme-icon--primary`;
        }

        const actionWrapper = document.createElement("div");
        actionWrapper.className = "text-center";
        const actionIcon = document.createElement("i");
        actionIcon.className = `actionable_icon ${iconClassName}`;

        actionIcon.onclick = () => {
          actionItem.action(sourceDataRow);
        };

        actionWrapper.appendChild(actionIcon);
        actionsWrapper.appendChild(actionWrapper);
      });
      return actionsWrapper;
    },
    refreshMonitoring() {
      axios
        .post("/refresh_monitoring/", {
          database_index: this.databaseIndex,
          workspace_id: this.workspaceId,
          query: this.query,
        })
        .then((resp) => {
          let data = resp.data.data;
          this.dataLength = data.length;

          data.forEach((col, idx) => {
            col.actions = [
              {
                icon: "fas fa-times action-grid action-close",
                title: "Terminate",
                action: this.terminateBackend,
              },
            ];
          });
          this.table
            .setData(data)
            .then(() => {
              this.table.redraw(true);
            })
            .catch((error) => {
              showToast("error", error);
            });
        })
        .catch((error) => {
          if (error.response.data?.password_timeout) {
            emitter.emit("show_password_prompt", {
              databaseIndex: this.databaseIndex,
              successCallback: () => {
                this.refreshMonitoring();
              },
              message: error.response.data.data,
            });
          } else {
            showToast("error", error.response.data.data);
          }
        });
    },
    terminateBackend(row) {
      let pid;
      switch (this.dialect) {
        case "postgresql":
          pid = row.pid;
          break;
        case "mysql":
          pid = row.ID;
          break;
        case "mariadb":
          pid = row.ID;
          break;
        case "oracle":
          pid = `${row.SID},${row["SERIAL#"]}`;
          break;
        default:
          break;
      }
      if (!!pid) {
        messageModalStore.showModal(
          `Are you sure you want to terminate backend ${pid}?`,
          () => {
            this.terminateBackendConfirm(pid);
          }
        );
      }
    },
    terminateBackendConfirm(pid) {
      axios
        .post(`/kill_backend_${this.dialect}/`, {
          database_index: this.databaseIndex,
          workspace_id: this.workspaceId,
          pid: pid,
        })
        .then((resp) => {
          this.refreshMonitoring();
        })
        .catch((error) => {
          if (error.response.data?.password_timeout) {
            emitter.emit("show_password_prompt", {
              databaseIndex: this.databaseIndex,
              successCallback: () => {
                this.terminateBackendConfirm(pid);
              },
              message: error.response.data.data,
            });
          } else {
            showToast("error", error.response.data.data);
          }
        });
    },
    handleResize() {
      if (this.$refs === null) return;

      this.heightSubtract =
        this.$refs.topToolbar.getBoundingClientRect().bottom;
    },
  },
};
</script>

<style scoped>
.grid-height {
  height: v-bind(gridHeight);
}
</style>
