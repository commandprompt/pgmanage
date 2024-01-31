<template>
  <div>
    <div class="container-fluid">
      <button
        data-testid="refresh-all-widgets-button"
        class="btn btn-primary btn-sm my-2 mr-2"
        @click="refreshWidgets"
      >
        <i class="fas fa-sync-alt mr-2"></i>
        Refresh All
      </button>
      <button
        class="btn btn-primary btn-sm my-2"
        @click="showMonitoringWidgetsList"
      >
        Manage Widgets
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
        />
      </div>
    </div>
  </div>

  <Teleport to="body">
    <MonitoringWidgetsModal
      :widgets="widgets"
      :conn-id="connId"
      :tab-id="tabId"
      :database-index="databaseIndex"
      :widgets-modal-visible="monitoringModalVisible"
      @modal-hide="monitoringModalVisible = false"
      @toggle-widget="toggleWidget"
    />
  </Teleport>
</template>

<script>
import axios from "axios";
import MonitoringWidget from "./MonitoringWidget.vue";
import { showToast } from "../notification_control";
import MonitoringWidgetsModal from "./MonitoringWidgetsModal.vue";

export default {
  name: "MonitoringDashboard",
  components: {
    MonitoringWidget,
    MonitoringWidgetsModal,
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
      monitoringModalVisible: false,
    };
  },
  mounted() {
    this.getMonitoringWidges();
  },
  methods: {
    getMonitoringWidges() {
      axios
        .post("/monitoring-widgets", {
          tab_id: this.connId,
          database_index: this.databaseIndex,
        })
        .then((resp) => {
          this.widgets = resp.data.widgets;
        })
        .catch((error) => {
          showToast("error", error);
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
    closeWidget(widgetSavedId) {
      let widgetIdx = this.widgets.findIndex(
        (widget) => widget.saved_id === widgetSavedId
      );
      axios
        .delete(`/monitoring-widgets/${widgetSavedId}`)
        .then(() => {
          this.widgets.splice(widgetIdx, 1);
        })
        .catch((error) => {
          showToast("error", error);
        });
    },
    updateWidgetInterval({ saved_id, interval }) {
      let widget = this.widgets.find((widget) => widget.saved_id === saved_id);
      widget.interval = interval;
    },
    showMonitoringWidgetsList() {
      this.monitoringModalVisible = true;
    },
    toggleWidget(widgetData) {
      let widgetIdx = this.widgets.findIndex(
        (widget) => widget.id === widgetData.id
      );

      if (widgetIdx === -1) {
        let newWidget = {
          id: widgetData.id,
          title: widgetData.title,
          interval: widgetData.interval,
          plugin_name: widgetData.plugin_name ?? "",
          type: widgetData.type,
        };
        this.saveWidgetToDatabaseAndShow(newWidget);
      } else {
        let widget = this.widgets[widgetIdx];
        this.closeWidget(widget.saved_id);
      }
    },
    saveWidgetToDatabaseAndShow(newWidget) {
      axios
        .post("/monitoring-widgets/create", {
          tab_id: this.connId,
          database_index: this.databaseIndex,
          widget_data: newWidget,
        })
        .then((resp) => {
          newWidget.saved_id = resp.data.user_widget.id;

          this.widgets.unshift(newWidget);
        })
        .catch((error) => {
          showToast("error", error);
        });
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
