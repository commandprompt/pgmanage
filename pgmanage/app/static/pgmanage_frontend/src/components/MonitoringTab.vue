<template>
  <!-- schema-editor-scrollable class copy pasted for nav tabs to look like other nav tabs, maybe we should consider using more common name -->
  <div class="schema-editor-scrollable p-2">
    <ul v-if="postgresqlDialect" class="nav nav-tabs" role="tablist">
      <li class="nav-item">
        <button
          ref="BackendsTabButton"
          data-testid="backends-tab-button"
          class="nav-item nav-link active omnidb__tab-menu__link"
          :id="`${tabId}-backends-tab`"
          data-bs-toggle="tab"
          :data-bs-target="`#${tabId}-backends-tab-pane`"
          type="button"
          role="tab"
          aria-selected="true"
        >
          Processes
        </button>
      </li>
      <li class="nav-item">
        <button
          class="nav-item nav-link omnidb__tab-menu__link"
          :id="`${tabId}-logs-tab`"
          data-testid="logs-tab-button"
          data-bs-toggle="tab"
          :data-bs-target="`#${tabId}-logs-tab-pane`"
          type="button"
          role="tab"
          aria-selected="false"
        >
          Logs
        </button>
      </li>
    </ul>

    <div class="tab-content">
      <div
        class="tab-pane fade show active"
        role="tabpanel"
        :id="`${tabId}-backends-tab-pane`"
      >
        <div ref="topToolbar">
          <button
            data-testid="monitoring-refresh-button"
            class="btn btn-primary btn-sm my-2 me-1"
            title="Refresh"
            @click="refreshMonitoring"
          >
            <i class="fas fa-sync-alt me-2"></i>Refresh
          </button>
          <button
            v-if="!isActive"
            data-testid="monitoring-play-button"
            class="btn btn-secondary btn-sm me-1"
            title="Play"
            @click="playMonitoring"
          >
            <i class="fas fa-play-circle fa-light"></i>
          </button>

          <button
            v-else
            data-testid="monitoring-pause-button"
            class="btn btn-secondary btn-sm me-1"
            title="Pause"
            @click="pauseMonitoring"
          >
            <i class="fas fa-pause-circle fa-light"></i>
          </button>
          <div
            class="d-inline-flex align-items-center refresh-menu ms-1"
            data-bs-toggle="dropdown"
          >
            <a class="refresh-menu__link" href="">{{
              humanizeDuration(monitoringInterval)
            }}</a>
            <div class="dropdown-menu dropdown-menu-width-auto">
              <a
                v-for="(option, index) in refreshIntervalOptions"
                :key="index"
                :data-testid="`refresh-option-${option}`"
                @click="monitoringInterval = option"
                class="dropdown-item"
                href="#"
              >
                {{ humanizeDuration(option) }}
              </a>
            </div>
          </div>
          <span class="float-end"> Total processes: {{ dataLength }} </span>
        </div>
        <div class="card border-0">
          <Transition :duration="100">
            <div
              v-if="showLoading"
              class="div_loading d-block"
              style="z-index: 10"
            >
              <div class="div_loading_cover"></div>
              <div class="div_loading_content">
                <div
                  class="spinner-border spinner-size text-primary"
                  role="status"
                >
                  <span class="sr-only">Loading...</span>
                </div>
              </div>
            </div>
          </Transition>

          <div ref="tabulator" class="tabulator-custom grid-height pb-3"></div>
        </div>
      </div>
      <div
        v-if="postgresqlDialect"
        class="tab-pane fade"
        role="tabpanel"
        :id="`${tabId}-logs-tab-pane`"
      >
        <MonitoringTabLogs
          :database-index="databaseIndex"
          :workspace-id="workspaceId"
          :tab-id="tabId"
        />
      </div>
    </div>
  </div>
</template>

<script>
import { TabulatorFull as Tabulator } from "tabulator-tables";
import axios from "axios";
import { emitter } from "../emitter";
import {
  messageModalStore,
  settingsStore,
  cellDataModalStore,
} from "../stores/stores_initializer";
import { useVuelidate } from "@vuelidate/core";
import { minValue, required } from "@vuelidate/validators";
import { handleError } from "../logging/utils";
import HumanizeDurationMixin from "../mixins/humanize_duration_mixin";
import MonitoringTabLogs from "./MonitoringTabLogs.vue";

export default {
  name: "MonitoringTab",
  components: {
    MonitoringTabLogs,
  },
  setup() {
    return {
      v$: useVuelidate({ $lazy: true }),
    };
  },
  mixins: [HumanizeDurationMixin],
  props: {
    tabId: String,
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
      timeoutObject: null,
      isActive: true,
      monitoringInterval: 10,
      showLoading: true,
      refreshIntervalOptions: [5, 10, 30, 60, 120, 300],
    };
  },
  computed: {
    gridHeight() {
      return `calc(100vh - ${this.heightSubtract}px)`;
    },
    postgresqlDialect() {
      return this.dialect === "postgresql";
    },
  },
  validations() {
    return {
      monitoringInterval: {
        required,
        minValue: minValue(5),
      },
    };
  },
  mounted() {
    this.handleResize();
    this.setupTable();

    settingsStore.$onAction((action) => {
      if (action.name === "setFontSize") {
        action.after(() => {
          requestAnimationFrame(() => {
            requestAnimationFrame(() => {
              this.refreshInterface();
            });
          });
        });
      }
    });

    emitter.on(`${this.tabId}_redraw_monitoring_tab`, () => {
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          this.refreshInterface();
        });
      });
    });

    if (this.postgresqlDialect) {
      this.$refs.BackendsTabButton.addEventListener("shown.bs.tab", (event) => {
        this.refreshInterface();
      });
    }
  },
  unmounted() {
    clearTimeout(this.timeoutObject);
    emitter.all.delete(`${this.tabId}_redraw_monitoring_tab`);
  },
  updated() {
    this.refreshInterface();
  },
  methods: {
    setupTable() {
      let cellContextMenu = [
        {
          label: '<i class="fas fa-copy"></i><span>Copy</span>',
          action: function (e, cell) {
            cell.getTable().copyToClipboard("selected");
          },
        },
        {
          label: '<i class="fas fa-edit"></i><span>View Content</span>',
          action: (e, cell) => {
            cellDataModalStore.showModal(cell.getValue(), "sql", true);
          },
        },
      ];

      this.table = new Tabulator(this.$refs.tabulator, {
        autoColumns: true,
        layout: "fitDataStretch",
        autoResize: false,
        columnDefaults: {
          headerHozAlign: "left",
          headerSort: true,
          maxWidth: "500px",
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
            title: "Actions",
            field: "actions",
            formatter: this.actionsFormatter,
            hozAlign: "center",
            headerSort: false,
            frozen: true,
            clipboard: false,
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
        const actionWrapper = document.createElement("div");
        actionWrapper.className = "text-center";
        const actionIcon = document.createElement("i");
        actionIcon.className = actionItem.icon;
        actionIcon.title = "Terminate";
        actionIcon.onclick = () => {
          actionItem.action(sourceDataRow);
        };

        actionWrapper.appendChild(actionIcon);
        actionsWrapper.appendChild(actionWrapper);
      });
      return actionsWrapper;
    },
    refreshMonitoring(showLoading = true) {
      clearTimeout(this.timeoutObject);
      if (showLoading) this.showLoading = true;
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
                icon: "fas fa-times text-danger",
                title: "Terminate",
                action: this.terminate,
              },
            ];
          });
          if (this.timeoutObject === null) {
            this.table.setData(data);
          } else {
            this.table.replaceData(data);
          }
          if (this.isActive) {
            this.timeoutObject = setTimeout(() => {
              this.refreshMonitoring(false);
            }, this.monitoringInterval * 1000);
          }
          this.showLoading = false;
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
            handleError(error);
          }
          this.showLoading = false;
        });
    },
    terminate(row) {
      let pid;
      let entityName = "backend";
      switch (this.dialect) {
        case "postgresql":
          pid = row.Pid;
          break;
        case "mysql":
        case "mariadb":
          entityName = "process";
          pid = row.ID;
          break;
        case "oracle":
          entityName = "session";
          pid = `${row.SID},${row["SERIAL#"]}`;
          break;
        default:
          break;
      }
      if (!!pid) {
        messageModalStore.showModal(
          `Are you sure you want to terminate ${entityName} ${pid}?`,
          () => {
            this.terminateConfirm(pid);
          }
        );
      }
    },
    terminateConfirm(pid) {
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
                this.terminateConfirm(pid);
              },
              message: error.response.data.data,
            });
          } else {
            handleError(error);
          }
        });
    },
    handleResize() {
      if (this.$refs === null) return;

      this.heightSubtract =
        this.$refs.topToolbar.getBoundingClientRect().bottom;
    },
    pauseMonitoring() {
      clearTimeout(this.timeoutObject);
      this.isActive = false;
    },
    playMonitoring() {
      this.isActive = true;
      this.refreshMonitoring();
    },
    refreshInterface() {
      this.handleResize();
      this.table.redraw();
    },
  },
};
</script>

<style scoped>
.grid-height {
  height: v-bind(gridHeight);
}
</style>
