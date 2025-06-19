<template>
  <div class="col-md-6 my-2">
    <div class="card">
      <div class="d-flex justify-content-between p-3 pb-0">
        <h3 data-testid="widget-title" class="me-1" v-if="!isTestWidget">
          {{ monitoringWidget.title }}
        </h3>
        <template v-else>
          <h3 class="text-center pb-1">Monitoring test widget</h3>
        </template>

        <div v-if="!isTestWidget" class="">
          <div
            class="d-inline-flex align-items-center refresh-menu ms-1"
            data-bs-toggle="dropdown">
            <a class="refresh-menu__link" href="">{{ humanizeDuration(monitoringWidget.interval) }}</a>
            <div class="dropdown-menu dropdown-menu-width-auto">
              <a
                v-for="(option, index) in refreshIntervalOptions" :key=index
                @click="updateInterval(option)"
                class="dropdown-item"
                href="#"
              >
                {{ humanizeDuration(option) }}
              </a>
            </div>
          </div>
          <!-- <span v-if="isGrid" class="ms-2"> {{ gridRows }} rows </span>  -->
          <button
            data-testid="widget-refresh-button"
            class="btn btn-icon btn-icon-secondary ms-2"
            title="Refresh"
            @click="refreshMonitoringWidget"
          >
            <i class="fas fa-sync-alt fa-light"></i>
          </button>

          <button
            v-if="!isActive"
            data-testid="widget-play-button"
            class="btn btn-icon btn-icon-secondary ms-2"
            title="Play"
            @click="playMonitoringWidget"
          >
            <i class="fas fa-play-circle fa-light"></i>
          </button>

          <button
            v-else
            data-testid="widget-pause-button"
            class="btn btn-icon btn-icon-secondary ms-2"
            title="Pause"
            @click="pauseMonitoringWidget"
          >
            <i class="fas fa-pause-circle fa-light"></i>
          </button>

          <button
            data-testid="widget-close-button"
            class="btn btn-icon btn-icon-secondary ms-4"
            @click="closeMonitoringWidget"
            >
            <i class="fa fa-times"></i>
          </button>
        </div>
      </div>

      <div class="card-body p-3">
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
import Chart from "chart.js/auto";
import "chartjs-adapter-moment";
import moment from "moment";
import { useVuelidate } from "@vuelidate/core";
import { minValue, required } from "@vuelidate/validators";
import { settingsStore, cellDataModalStore } from "../stores/stores_initializer";
import { markRaw } from "vue";
import { handleError } from "../logging/utils";
import HumanizeDurationMixin from '../mixins/humanize_duration_mixin'
import { interpolateRainbow } from  "d3-scale-chromatic";

const colorScale = interpolateRainbow;

export default {
  name: "MonitoringWidget",
  setup() {
    return {
      v$: useVuelidate({ $lazy: true }),
    };
  },
  mixins: [HumanizeDurationMixin],
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
  emits: ["widgetRefreshed", "toggleWidget", "intervalUpdated"],
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
      refreshIntervalOptions: [5, 10, 30, 60, 120, 300]
    };
  },
  computed: {
    isGrid() {
      return this.monitoringWidget.type === "grid";
    },
    isChart() {
      return ["timeseries", "chart"].includes(
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
      // add random delay to widet timer start,
      // prevents that many widgets from bashing back-end with simulteneous requests
      let randomWait = Math.floor(Math.random() * 150)
      this.timeoutObject = setTimeout(this.refreshMonitoringWidget, randomWait)
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
        if (action.name === "setTheme" || action.name === 'setFontSize') {
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
          this.errorText = error.response?.data.data;
          this.showLoading = false;
        });
      
      if (this.isActive) {
        this.timeoutObject = setTimeout(() => {
          this.refreshMonitoringWidget(false);
        }, this.monitoringWidget.interval * 1000);
      }
    },
    buildGrid(data) {
      this.gridRows = data.data.length;
      if (!this.visualizationObject) {
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
              cellDataModalStore.showModal(String(cell.getValue()))
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
            headerHozAlign: "left",
            headerSort: false,
          },
          autoColumns: true,
          autoColumnsDefinitions: function (definitions) {
            definitions.forEach((column) => {
              column.contextMenu = cellContextMenu;
            });
            return definitions;
          },
        });
        this.visualizationObject = tabulator;
      } else {
        this.visualizationObject.replaceData(data.data);
      }
    },
    generatePalette(colorsNum) {
      // generate palette with colorsNum from d3 color scale
      const colorStart = 0, colorEnd = 1;
      const intervalSize = (colorEnd - colorStart) / colorsNum;
      let i, colorPoint;
      let colorArray = [];

      for (i = 0; i < colorsNum; i++) {
        colorPoint = colorStart + (i * intervalSize);
        colorArray.push(colorScale(colorPoint));
      }
      return colorArray;
    },
    buildChart(data) {
      let chartData = { ...data.object };
      this.widgetData = JSON.parse(
        JSON.stringify(chartData?.data ?? chartData)
      );

      if (!this.visualizationObject && this.$refs.canvas) {
        let ctx = this.$refs.canvas.getContext("2d");

        if (chartData?.options?.maintainAspectRatio) {
          chartData.options.maintainAspectRatio = false;
        }
        chartData.options.plugins['colors'] = { 'enabled': false}

        if(['doughnut', 'pie'].includes(chartData?.type)) {
          // generate palette if dataset does not define it
          if(!chartData?.data?.datasets[0].backgroundColor?.length)
            chartData.data.datasets[0].backgroundColor = this.generatePalette(chartData.data.labels.length)
        }
        const chartObj = new Chart(ctx, chartData);
        this.visualizationObject = markRaw(chartObj);
        this.changeChartTheme();
      } else {
        // yes, this is actually possible when widget is quickly toggled on/off
        if (!this.visualizationObject)
          return;
        //TODO this part of code still needs refactoring
        if (this.monitoringWidget.type === "chart") {
          //foreach dataset in returning data, find corresponding dataset in existing chart
          for (let i = 0; i < chartData?.datasets?.length; i++) {
            let return_dataset = chartData.datasets[i];

            if(['doughnut', 'pie'].includes(chartData.type)) {
              // generate palette if dataset does not define it
              if(!return_dataset.backgroundColor?.length)
                return_dataset.backgroundColor = this.generatePalette(chartData.data.labels.length)
            }

            // checking datasets
            let found = false;
            let chart_datasets = this.visualizationObject.data.datasets
            for (let j = 0; j < chart_datasets.length; j++) {
              let dataset = chart_datasets[j];
              dataset.backgroundColor = this.generatePalette(dataset.data.length)
              // Dataset exists, update data and adjust colors
              if (return_dataset.label == dataset.label) {
                found = true;
                dataset.data = return_dataset.data;
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
          // maximum time span to fit on timeseries graph
          const timespanSeconds = 3600;
          // number of datapoints to keep in each dataset
          // must be > timespan / 5 ( 5 is minimum possible widget refresh interval)
          const maxDatapoints = 1000;
          this.visualizationObject.data.datasets.forEach((ds, idx) => {
            ds.data.push(chartData.datasets[idx].data[0])
            // remove datapoints which are potentially outside of timespan
            ds.data.splice(0, ds.data.length - maxDatapoints)
          })

          // dynamically readjust min and max values of the time axis
          // to draw only datpoints fitting into timespan beteen now and now - timespan
          // where now is the last datapoint timestamp
          // the graph is being compressed initially, then starts to
          // scroll if dataset occupies full timespan
          if (this.visualizationObject.options.scales.x) {
            let ds = this.visualizationObject.data.datasets[0].data
            // timestamp of the very first datapoint
            let firstTS = ds[0].x
            let lastTS = ds[ds.length - 1].x
            if(moment(lastTS).diff(moment(firstTS),'seconds') > timespanSeconds) {
              this.visualizationObject.options.scales.x.min = moment(lastTS).subtract(timespanSeconds, 'seconds').toISOString();
            } else {
              this.visualizationObject.options.scales.x.min = moment(firstTS).toISOString();
            }
            this.visualizationObject.options.scales.x.max = moment(lastTS).toISOString();
          }
          
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
          this.buildChart(data);
          break;
        default:
          break;
      }
    },
    closeMonitoringWidget() {
      clearTimeout(this.timeoutObject);
      this.$emit("toggleWidget", this.monitoringWidget, false);
    },
    pauseMonitoringWidget() {
      clearTimeout(this.timeoutObject);
      this.isActive = false;
    },
    playMonitoringWidget() {
      this.isActive = true;
      this.refreshMonitoringWidget();
    },
    updateInterval(value) {
      this.widgetInterval = value
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
          handleError(error);
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
          this.errorText = error.response?.data.data;
          this.showLoading = false;
        });
    },
    changeChartTheme() {
      let chartFontColor, chartGridColor, chartLineBorderColor, chartLineBackgroundColor;
      let chartFont = {
        size: Math.round(settingsStore.fontSize * 0.8),
        family: "'Poppins', sans-serif"
      };
      // TODO: add chart tooltip font and color changing
      if (settingsStore.theme == "light") {
        chartFontColor = "#666666";
        chartGridColor = "rgba(0, 0, 0, 0.1)";
        chartLineBorderColor = "#1560AD";
        chartLineBackgroundColor = "#1560AD20";
      } else {
        chartFontColor = "#DCDDDE";
        chartGridColor = "#314264";
        chartLineBorderColor = "#2190ff";
        chartLineBackgroundColor = "#1560AD60";
      }

      try {
        this.visualizationObject.options.plugins.title.color = chartFontColor;
        this.visualizationObject.options.plugins.title.font = chartFont;
        this.visualizationObject.options.plugins.legend.labels.color = chartFontColor;
        this.visualizationObject.options.plugins.legend.labels.font = chartFont;

        if (this.monitoringWidget.type === "timeseries") {
          // axis x and y borders
          this.visualizationObject.options.elements.line.backgroundColor = chartLineBackgroundColor;
          this.visualizationObject.options.elements.line.borderColor = chartLineBorderColor;
          this.visualizationObject.scales.x.options.border.color = chartGridColor;
          this.visualizationObject.scales.y.options.border.color = chartGridColor;
          // grid lines
          this.visualizationObject.options.scales.x.grid.color = chartGridColor;
          this.visualizationObject.options.scales.y.grid.color = chartGridColor;
          // axis tick labels
          this.visualizationObject.options.scales.x.ticks.color = chartFontColor;
          this.visualizationObject.options.scales.y.ticks.color = chartFontColor;
          // axis title
          this.visualizationObject.options.scales.x.title.color = chartFontColor;
          this.visualizationObject.options.scales.y.title.color = chartFontColor;
          // chart font sizes
          this.visualizationObject.scales.x.options.ticks.font = chartFont;
          this.visualizationObject.scales.y.options.ticks.font = chartFont;
          this.visualizationObject.scales.x.options.title.font = chartFont;
          this.visualizationObject.scales.y.options.title.font = chartFont;
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
