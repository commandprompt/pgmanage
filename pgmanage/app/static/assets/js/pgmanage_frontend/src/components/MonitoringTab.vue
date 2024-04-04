<template>
  <div class="p-2">
    <button
      class="btn btn-primary btn-sm my-2 mr-1"
      title="Refresh"
      @click="refreshMonitoring"
    >
      <i class="fas fa-sync-alt mr-2"></i>Refresh
    </button>
    <span class="query_info"> Number of records: {{ dataLength }} </span>
    <div
      ref="tabulator"
      class="omnidb__query-result-tabs__content tabulator-custom"
    ></div>
  </div>
</template>

<script>
import { TabulatorFull as Tabulator } from "tabulator-tables";
import { cellDataModal } from "../header_actions";
import axios from "axios";
import { createMessageModal, showToast } from "../notification_control";
import { emitter } from "../emitter";

export default {
  name: "MonitoringTab",
  props: {
    query: String,
    databaseIndex: Number,
    connId: String,
    dialect: String,
  },
  data() {
    return {
      table: null,
      dataLength: 0,
    };
  },
  mounted() {
    this.setupTable();
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
            cellDataModal(null, null, null, cell.getValue(), false);
          },
        },
      ];

      this.table = new Tabulator(this.$refs.tabulator, {
        height: "90vh",
        width: "100%",
        autoColumns: true,
        layout: "fitDataStretch",
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
          tab_id: this.connId,
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
        createMessageModal(
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
          tab_id: this.connId,
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
  },
};
</script>
