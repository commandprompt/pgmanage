<template>
  <div class="col-md-6 my-2">
    <div class="card">
      <div class="card-body">
        <button
          v-if="!isTestWidget"
          data-testid="widget-close-button"
          class="btn-close"
          @click="closeMonitoringWidget"
        ></button>

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

        <div v-if="!isTestWidget" class="form-inline mb-1">
          <span data-testid="widget-title" class="me-1">
            {{ monitoringWidget.title }}
          </span>
          <button
            data-testid="widget-refresh-button"
            class="btn btn-secondary btn-sm me-1"
            title="Refresh"
            @click="refreshMonitoringWidget"
          >
            <i class="fas fa-sync-alt fa-light"></i>
          </button>

          <button
            v-if="!isActive"
            data-testid="widget-play-button"
            class="btn btn-secondary btn-sm me-1"
            title="Play"
            @click="playMonitoringWidget"
          >
            <i class="fas fa-play-circle fa-light"></i>
          </button>

          <button
            v-else
            data-testid="widget-pause-button"
            class="btn btn-secondary btn-sm me-1"
            title="Pause"
            @click="pauseMonitoringWidget"
          >
            <i class="fas fa-pause-circle fa-light"></i>
          </button>
          <div class="d-inline-flex align-items-center">
            <input
              data-testid="widget-interval-input"
              v-model.number="v$.widgetInterval.$model"
              @change="updateInterval"
              :class="[
                'form-control',
                'form-control-sm',
                'me-2',
                { 'is-invalid': v$.widgetInterval.$invalid },
              ]"
              style="width: 60px"
            />
            <span>seconds</span>
            <div class="invalid-feedback">
              <a v-for="error of v$.widgetInterval.$errors" :key="error.$uid">
                {{ error.$message }}
                <br />
              </a>
            </div>
          </div>

          <span v-if="isGrid" class="ms-2"> {{ gridRows }} rows </span>
        </div>
        <template v-else>
          <h2 class="text-center pb-1">Monitoring test widget</h2>
        </template>

        <div class="widget-content">
          <div v-if="errorText" class="error_text">
            {{ this.errorText }}
          </div>

          <div v-else-if="isGrid" ref="gridContent"></div>

          <div
            v-else-if="isChart"
            class="w-100 position-relative"
            style="height: 300px"
          >
            <canvas ref="canvas"></canvas>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from "axios";
import { TabulatorFull as Tabulator } from "tabulator-tables";
import { emitter } from "../emitter";
import { showToast } from "../notification_control";
import Chart from "chart.js/auto";
import { useVuelidate } from "@vuelidate/core";
import { minValue, required } from "@vuelidate/validators";
import { settingsStore, cellDataModalStore } from "../stores/stores_initializer";
import { markRaw } from "vue";

export default {
  name: "MonitoringWidget",
  setup() {
    return {
      v$: useVuelidate({ $lazy: true }),
    };
  },
  props: {
    monitoringWidget: {
      type: Object,
      required: true,
    },
    workspaceId: String,
    tabId: String,
    databaseIndex: Number,
    refreshWidget: Boolean,
    isTestWidget: {
      type: Boolean,
      default: false,
    },
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
      widgetData: null,
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
  validations() {
    return {
      widgetInterval: {
        required,
        minValue: minValue(5),
      },
    };
  },
  mounted() {
    if (!this.isTestWidget) {
      this.refreshMonitoringWidget();
    } else {
      this.testMonitoringWidget();
    }

    emitter.on(`${this.tabId}_redraw_widget_grid`, () => {
      if (this.isGrid) {
        this.visualizationObject.redraw(true);
      }
    });

    if (this.isChart) {
      settingsStore.$onAction((action) => {
        if (action.name === "setTheme") {
          action.after(() => {
            this.changeChartTheme();
          });
        }
      });
    }
  },
  unmounted() {
    this.clearEventsAndTimeout();
  },
  watch: {
    refreshWidget(newValue, oldValue) {
      if (!!newValue) {
        this.refreshMonitoringWidget();
        this.$emit("widgetRefreshed");
      }
    },
  },
  methods: {
    refreshMonitoringWidget(showLoading = true) {
      clearTimeout(this.timeoutObject);
      if (showLoading) this.showLoading = true;
      this.errorText = "";
      axios
        .post(`/monitoring-widgets/${this.monitoringWidget.saved_id}/refresh`, {
          database_index: this.databaseIndex,
          workspace_id: this.workspaceId,
          widget: {
            ...this.monitoringWidget,
            initial: !this.visualizationObject,
            widget_data: this.widgetData,
          },
        })
        .then((resp) => {
          this.buildMonitoringWidget(resp.data);
          this.showLoading = false;
        })
        .catch((error) => {
          this.errorText = error.response.data.data;
          this.showLoading = false;
        });

      this.timeoutObject = setTimeout(() => {
        this.refreshMonitoringWidget(false);
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
              cellDataModalStore.showModal(cell.getValue())
            },
          },
        ];
        this.$refs.gridContent.classList.add("tabulator-custom");
        let tabulator = new Tabulator(this.$refs.gridContent, {
          data: data.data,
          height: "100%",
          autoResize: false,
          layout: "fitDataStretch",
          selectableRows: true,
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
      let chartData = { ...data.object };
      this.widgetData = JSON.parse(
        JSON.stringify(chartData?.data ?? chartData)
      );

      if (!this.visualizationObject) {
        let ctx = this.$refs.canvas.getContext("2d");

        if (chartData?.options?.maintainAspectRatio) {
          chartData.options.maintainAspectRatio = false;
        }

        const chartObj = new Chart(ctx, chartData);

        this.visualizationObject = markRaw(chartObj);
        this.changeChartTheme();
      } else {
        //TODO this part of code still needs refactoring
        if (this.monitoringWidget.type === "chart") {
          //foreach dataset in returning data, find corresponding dataset in existing chart
          for (let i = 0; i < chartData.datasets.length; i++) {
            let return_dataset = chartData.datasets[i];

            // checking datasets

            let found = false;

            for (
              let j = 0;
              j < this.visualizationObject.data.datasets.length;
              j++
            ) {
              let dataset = this.visualizationObject.data.datasets[j];
              // Dataset exists, update data and adjust colors
              if (return_dataset.label == dataset.label) {
                let new_dataset = dataset;

                // rebuild color list if it exists

                if (
                  return_dataset.backgroundColor &&
                  return_dataset.backgroundColor.length
                ) {
                  let color_list = [];
                  for (let k = 0; k < chartData.labels.length; k++) {
                    let found_label = false;
                    for (
                      let m = 0;
                      m < this.visualizationObject.data.labels.length;
                      m++
                    ) {
                      if (
                        JSON.stringify(chartData.labels[k]) ==
                        JSON.stringify(this.visualizationObject.data.labels[m])
                      ) {
                        color_list.push(dataset.backgroundColor[m]);
                        found_label = true;
                        break;
                      }
                    }

                    if (!found_label) {
                      color_list.push(return_dataset.backgroundColor[k]);
                    }
                  }
                  new_dataset.backgroundColor = color_list;
                }
                new_dataset.data = return_dataset.data;

                dataset = new_dataset;

                found = true;
                break;
              }
            }
            //dataset doesn't exist, create it
            if (!found) {
              this.visualizationObject.data.datasets.push(return_dataset);
            }
          }

          this.visualizationObject.data.labels = chartData.labels;

          // update title

          if (
            chartData.title &&
            chartData.options &&
            chartData.options?.plugins?.title
          ) {
            this.visualizationObject.options.plugins.title.text =
              chartData.title;
          }

          try {
            this.visualizationObject.update();
          } catch (err) {
            console.log(err);
          }
        } else if (this.monitoringWidget.type === "timeseries") {
          // timeseries
          // adding new label in X axis
          this.visualizationObject.data.labels.push(chartData.labels[0]);

          let shift = false;
          if (chartData.labels.length > 100) {
            chartData.labels.shift();
            shift = true;
          }

          //foreach dataset in existing chart, find corresponding dataset in returning data
          this.visualizationObject.data.datasets.forEach((dataset) => {
            dataset.data.push(null);
            if (shift) {
              dataset.data.shift();
            }
          });

          //foreach dataset in returning data, find corresponding dataset in existing chart
          for (let i = 0; i < chartData.datasets.length; i++) {
            let return_dataset = chartData.datasets[i];

            let found = false;
            for (
              let j = 0;
              j < this.visualizationObject.data.datasets.length;
              j++
            ) {
              let dataset = this.visualizationObject.data.datasets[j];
              //Dataset exists, update data
              if (return_dataset.label == dataset.label) {
                let new_dataset = dataset;
                new_dataset.data[new_dataset.data.length - 1] =
                  return_dataset.data[0];
                dataset = new_dataset;

                found = true;
                break;
              }
            }

            //dataset doesn't exist, create it
            if (!found) {
              // populate dataset with empty data prior to newest value
              for (
                let k = 0;
                k < this.visualizationObject.data.labels.length - 1;
                k++
              ) {
                return_dataset.data.unshift(null);
              }
              this.visualizationObject.data.datasets.push(return_dataset);
            }
          }

          //update title
          if (
            chartData.title &&
            chartData.options &&
            chartData.options?.plugins?.title
          ) {
            this.visualizationObject.options.plugins.title.text =
              chartData.title;
          }

          try {
            this.visualizationObject.update();
          } catch (err) {
            console.log(err);
          }
        }
      }
    },
    buildMonitoringWidget(data) {
      switch (this.monitoringWidget.type) {
        case "grid":
          this.buildGrid(data);
          break;
        case "chart":
        case "timeseries":
        case "chart_append":
          this.buildChart(data);
          break;
        default:
          break;
      }
    },
    closeMonitoringWidget() {
      clearTimeout(this.timeoutObject);
      this.$emit("widgetClose", this.monitoringWidget.saved_id);
    },
    pauseMonitoringWidget() {
      clearTimeout(this.timeoutObject);
      this.isActive = false;
    },
    playMonitoringWidget() {
      this.isActive = true;
      this.refreshMonitoringWidget();
    },
    updateInterval() {
      this.v$.$validate();
      if (this.v$.$invalid) return;
      axios
        .patch(`/monitoring-widgets/${this.monitoringWidget.saved_id}`, {
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
    testMonitoringWidget() {
      axios
        .post("/monitoring-widgets/test", {
          workspace_id: this.workspaceId,
          database_index: this.databaseIndex,
          widget: this.monitoringWidget,
        })
        .then((resp) => {
          this.buildMonitoringWidget(resp.data);
          this.showLoading = false;
        })
        .catch((error) => {
          this.errorText = error.response.data.data;
          this.showLoading = false;
        });
    },
    changeChartTheme() {
      let chartFontColor, chartGridColor;

      if (settingsStore.theme == "light") {
        chartFontColor = "#666666";
        chartGridColor = "rgba(0, 0, 0, 0.1)";
      } else {
        chartFontColor = "#DCDDDE";
        chartGridColor = "rgba(100, 100, 100, 0.3)";
      }

      try {
        if (this.monitoringWidget.type === "chart") {
          this.visualizationObject.options.plugins.legend.labels.color =
            chartFontColor;
          this.visualizationObject.options.plugins.title.color = chartFontColor;
        } else {
          this.visualizationObject.scales.y.options.grid.border.color =
            chartGridColor;
          this.visualizationObject.scales.x.options.grid.border.color =
            chartGridColor;
          this.visualizationObject.scales.x.options.ticks.minor.fontColor =
            chartFontColor;
          this.visualizationObject.scales.y.options.ticks.minor.fontColor =
            chartFontColor;
          this.visualizationObject.scales.x.options.title.color =
            chartFontColor;
          this.visualizationObject.scales.y.options.title.color =
            chartFontColor;
        }
      } catch (err) {
      }
      this.visualizationObject.update();
    },
    clearEventsAndTimeout() {
      emitter.all.delete(`${this.tabId}_redraw_widget_grid`);
      clearTimeout(this.timeoutObject);
    },
  },
};
</script>

<style scoped>
.spinner-size {
  width: 4rem;
  height: 4rem;
}

.widget-content {
  overflow: auto;
  height: 300px;
}
</style>
