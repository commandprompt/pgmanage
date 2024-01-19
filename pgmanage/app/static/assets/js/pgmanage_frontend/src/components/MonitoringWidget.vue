<template>
  <div class="col-md-6 my-2">
    <div class="card">
      <div class="card-body">
        <button class="close" @click="closeMonitorWidget">
          <span aria-hidden="true">&times;</span>
        </button>

        <Transition :duration="100">
          <div
            v-if="showLoading"
            class="div_loading d-block"
            style="z-index: 10"
          >
            <div class="div_loading_cover"></div>
            <div class="div_loading_content">
              <div
                class="spinner-border text-primary"
                style="width: 4rem; height: 4rem"
                role="status"
              >
                <span class="sr-only">Loading...</span>
              </div>
            </div>
          </div>
        </Transition>

        <div class="form-inline mb-1">
          <span class="mr-1">
            {{ monitoringWidget.title }}
          </span>
          <button
            class="btn btn-secondary btn-sm mr-1"
            title="Refresh"
            @click="refreshMonitorWidget"
          >
            <i class="fas fa-sync-alt fa-light"></i>
          </button>

          <button
            v-if="!isActive"
            class="btn btn-secondary btn-sm my-2 mr-1"
            title="Play"
            @click="playMonitorWidget"
          >
            <i class="fas fa-play-circle fa-light"></i>
          </button>

          <button
            v-else
            class="btn btn-secondary btn-sm my-2 mr-1"
            title="Pause"
            @click="pauseMonitorWidget"
          >
            <i class="fas fa-pause-circle fa-light"></i>
          </button>

          <input
            v-model.number="widgetInterval"
            @change="updateInterval"
            class="form-control form-control-sm mr-2"
            style="width: 60px"
          />
          <span class="unit_header_element">seconds</span>
          <span v-if="isGrid" class="unit_header_element ml-2">
            {{ gridRows }} rows
          </span>
        </div>

        <div class="dashboard_unit_content_group">
          <div v-if="errorText" class="error_text">
            {{ this.errorText }}
          </div>

          <div v-else-if="isGrid" ref="widgetContent"></div>
          <div v-else-if="isChart">
            <canvas ref="canvas" class="w-100" style="height: 250px"></canvas>
          </div>
          <div ref="legend_box" class="dashboard_unit_legend_box"></div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from "axios";
import { cellDataModal } from "../header_actions";
import { TabulatorFull as Tabulator } from "tabulator-tables";
import { Transition } from "vue";
import { emitter } from "../emitter";
import { showToast } from "../notification_control";

export default {
  name: "MonitoringWidget",
  components: {
    Transition,
  },
  props: {
    monitoringWidget: {
      type: Object,
      required: true,
    },
    connId: String,
    tabId: String,
    databaseIndex: Number,
    refreshWidget: Boolean,
  },
  emits: ["widgetRefreshed", "widgetClose", "intervalUpdated"],
  data() {
    return {
      showLoading: true,
      isActive: true,
      visualizationObject: null,
      errorText: "",
      gridRows: "",
      timeoutObject: null,
      widgetInterval: this.monitoringWidget.interval,
    };
  },
  computed: {
    isGrid() {
      return this.monitoringWidget.type === "grid";
    },
    isChart() {
      return ["timeseries", "chart", "chart_append"].includes(
        this.monitoringWidget.type
      );
    },
  },
  mounted() {
    this.refreshMonitorWidget();

    emitter.on(`${this.tabId}_redraw_widget_grid`, () => {
      if (this.isGrid) {
        this.visualizationObject.redraw(true);
      }
    });
  },
  unmounted() {
    emitter.all.delete(`${this.tabId}_redraw_widget_grid`);
  },
  watch: {
    refreshWidget(newValue, oldValue) {
      if (!!newValue) {
        this.refreshMonitorWidget();
        this.$emit("widgetRefreshed");
      }
    },
  },
  methods: {
    refreshMonitorWidget(showLoading = true) {
      clearTimeout(this.timeoutObject);
      if (showLoading) this.showLoading = true;
      this.errorText = "";
      axios
        .post("/refresh_monitor_widget/", {
          database_index: this.databaseIndex,
          tab_id: this.connId,
          widget: this.monitoringWidget,
        })
        .then((resp) => {
          this.buildMonitorWidget(resp.data);
          this.showLoading = false;
        })
        .catch((error) => {
          this.errorText = error.response.data.message;
          this.showLoading = false;
        });

      this.timeoutObject = setTimeout(() => {
        this.refreshMonitorWidget(false);
      }, this.monitoringWidget.interval * 1000);
    },
    buildGrid(data) {
      this.gridRows = data.data.length;
      if (!this.visualizationObject) {
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
              // can we use vue component here?
              cellDataModal(null, null, null, cell.getValue(), false);
            },
          },
        ];
        this.$refs.widgetContent.classList.add("unit_grid");
        this.$refs.widgetContent.classList.add("tabulator-custom");
        let tabulator = new Tabulator(this.$refs.widgetContent, {
          data: data.data,
          height: "100%",
          layout: "fitDataStretch",
          selectable: true,
          clipboard: "copy",
          clipboardCopyConfig: {
            columnHeaders: false, //do not include column headers in clipboard output
          },
          clipboardCopyRowRange: "selected",
          columnDefaults: {
            headerHozAlign: "center",
            headerSort: false,
          },
          autoColumns: true,
          autoColumnsDefinitions: function (definitions) {
            //definitions - array of column definition objects
            definitions.unshift({
              formatter: "rownum",
              hozAlign: "center",
              width: 40,
              frozen: true,
            });

            definitions.forEach((column) => {
              column.contextMenu = cellContextMenu;
            });
            return definitions;
          },
        });
        this.visualizationObject = tabulator;
      } else {
        this.visualizationObject.setData(data.data);
      }
    },
    buildChart(data) {
      console.log("Not implemented");
    },
    buildGraph(data) {
      console.log("Not implemented");
    },
    buildMonitorWidget(data, showLoading = true) {
      switch (this.monitoringWidget.type) {
        case "grid":
          this.buildGrid(data);
          break;
        case "chart":
        case "timeseries":
        case "chart_append":
          this.buildChart(data);
          break;
        case "graph":
          this.buildGraph(data);
          break;
        default:
          break;
      }
    },
    closeMonitorWidget() {
      clearTimeout(this.timeoutObject);
      this.$emit("widgetClose", this.monitoringWidget.saved_id);
    },
    pauseMonitorWidget() {
      clearTimeout(this.timeoutObject);
      this.isActive = false;
    },
    playMonitorWidget() {
      this.isActive = true;
      this.refreshMonitorWidget();
    },
    updateInterval() {
      axios
        .post("/update_saved_monitor_widget_interval/", {
          saved_id: this.monitoringWidget.saved_id,
          interval: this.widgetInterval,
        })
        .then((resp) => {
          this.$emit("intervalUpdated", {
            saved_id: this.monitoringWidget.saved_id,
            interval: this.widgetInterval,
          });
        })
        .catch((error) => {
          showToast("error", error);
        });
    },
  },
};
</script>
