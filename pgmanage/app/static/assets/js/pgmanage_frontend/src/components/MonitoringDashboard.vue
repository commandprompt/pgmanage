<template>
  <div>
    <div class="container-fluid">
      <button class="btn btn-primary btn-sm my-2 mr-2" @click="refreshWidgets">
        <i class="fas fa-sync-alt mr-2"></i>
        Refresh All
      </button>
      <button class="btn btn-primary btn-sm my-2" @click="showMonitorUnitList">
        Manage Units
      </button>

      <div class="monitoring-widgets row">
        <MonitoringWidget
          v-for="widget in widgets"
          :key="widget.saved_id"
          :monitoring-widget="widget"
          :conn-id="connId"
          :tab-id="tabId"
          :database-index="databaseIndex"
          :refresh-widget="refreshWidget"
          @widget-refreshed="waitForAllAndRefreshCounter"
          @widget-close="closeWidget"
          @interval-updated="updateWidgetInterval"
        ></MonitoringWidget>
      </div>
    </div>
  </div>
</template>

<script>
import { showMonitorUnitList } from "../monitoring";
import axios from "axios";
import MonitoringWidget from "./MonitoringWidget.vue";
import { showToast } from "../notification_control";

export default {
  name: "MonitoringDashboard",
  components: {
    MonitoringWidget,
  },
  props: {
    connId: String,
    tabId: String,
    databaseIndex: Number,
  },
  data() {
    return {
      widgets: [],
      refreshWidget: false,
      counter: 0,
    };
  },
  mounted() {
    this.getMonitorUnits();
  },
  methods: {
    getMonitorUnits() {
      axios
        .post("/get_monitor_widgets/", {
          tab_id: this.connId,
          database_index: this.databaseIndex,
        })
        .then((resp) => {
          resp.data.widgets.forEach((widget) => {
            this.widgets.push(widget);
          });
        });
    },
    refreshWidgets() {
      this.refreshWidget = true;
    },
    waitForAllAndRefreshCounter() {
      this.counter++;
      if (this.counter === this.widgets.length) {
        this.refreshWidget = false;
        this.counter = 0;
      }
    },
    closeWidget(widget_saved_id) {
      let widget_id = this.widgets.findIndex(
        (widget) => widget.saved_id === widget_saved_id
      );
      axios
        .post("/remove_saved_monitor_widget/", { saved_id: widget_saved_id })
        .then(() => {
          this.widgets.splice(widget_id, 1);
        })
        .catch((error) => {
          showToast("error", error);
        });
    },
    updateWidgetInterval({ saved_id, interval }) {
      let widget = this.widgets.find((widget) => widget.saved_id === saved_id);
      widget.interval = interval;
    },
  },
};
</script>

<style scoped>
.monitoring-widgets {
  overflow: auto;
  height: 90vh;
}
</style>
