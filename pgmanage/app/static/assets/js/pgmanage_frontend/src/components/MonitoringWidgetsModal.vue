<template>
  <div
    ref="monitoringWidgetsModal"
    class="modal fade"
    tabindex="-1"
    role="dialog"
    aria-hidden="true"
  >
    <div class="modal-dialog modal-lg" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 class="modal-title font-weight-bold">Monitoring Widgets</h2>
          <button
            type="button"
            class="close"
            data-dismiss="modal"
            aria-label="Close"
          >
            <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
          </button>
        </div>
        <div class="modal-body">
          <div
            ref="tabulator"
            class="tabulator-custom"
            style="width: 100%; height: 300px; overflow: hidden"
          ></div>
        </div>
        <div class="modal-footer">
          <button
            @click="editMonitoringWidget(null)"
            class="btn btn-primary btn-sm mr-3"
          >
            New Widget
          </button>
        </div>
      </div>
    </div>
  </div>
  <MonitoringWidgetEditModal
    :conn-id="connId"
    :tab-id="tabId"
    :database-index="databaseIndex"
    :modal-visible="editModalVisible"
    :widget-id="editWidgetId"
    @modal-hide="onEditHide"
  />
</template>

<script>
import axios from "axios";
import { TabulatorFull as Tabulator } from "tabulator-tables";
import { createMessageModal, showToast } from "../notification_control";
import MonitoringWidgetEditModal from "./MonitoringWidgetEditModal.vue";

export default {
  name: "MonitoringWidgetsModal",
  components: {
    MonitoringWidgetEditModal,
  },
  props: {
    widgetsModalVisible: Boolean,
    connId: String,
    tabId: String,
    databaseIndex: Number,
    widgets: Array,
  },
  emits: ["modalHide", "toggleWidget"],
  data() {
    return {
      table: null,
      editModalVisible: false,
      editWidgetId: null,
    };
  },
  mounted() {
    $(this.$refs.monitoringWidgetsModal).on("hide.bs.modal", () => {
      this.$emit("modalHide");
    });
  },
  watch: {
    widgetsModalVisible(newVal, oldVal) {
      if (newVal) {
        $(this.$refs.monitoringWidgetsModal).modal("show");
        this.getMonitoringWidgetList();
      } else {
        if (!!this.table) this.table.destroy();
      }
    },
  },
  methods: {
    getMonitoringWidgetList() {
      axios
        .post("/monitoring-widgets/list", {
          database_index: this.databaseIndex,
          tab_id: this.connId,
        })
        .then((resp) => {
          this.table = new Tabulator(this.$refs.tabulator, {
            layout: "fitDataStretch",
            data: resp.data.data,
            columnDefaults: {
              headerHozAlign: "center",
              headerSort: false,
            },
            columns: [
              {
                title: "Show",
                field: "editable",
                hozAlign: "center",
                formatter: this.actionsFormatter,
              },
              { title: "Title", field: "title" },
              { title: "Type", field: "type" },
              { title: "Refr.(s)", field: "interval", hozAlign: "center" },
            ],
          });
        })
        .catch((error) => {
          console.log(error);
        });
    },
    actionsFormatter(cell, formatterParams, onRendered) {
      let sourceDataRow = cell.getRow().getData();
      let checked = this.widgets.some(
        (widget) => widget.id === sourceDataRow.id
      )
        ? "checked"
        : "";

      const input = document.createElement("input");
      input.type = "checkbox";
      input.checked = checked;
      input.onclick = () => {
        this.$emit("toggleWidget", sourceDataRow);
      };

      if (!!cell.getValue()) {
        const cellWrapper = document.createElement("div");
        cellWrapper.className =
          "d-flex justify-content-between align-items-center";

        cellWrapper.appendChild(input);

        const editIcon = document.createElement("i");
        editIcon.title = "Edit";
        editIcon.className = "fas fa-edit action-grid action-edit-monitor";
        editIcon.onclick = () => {
          this.editMonitoringWidget(sourceDataRow.id);
        };

        const deleteIcon = document.createElement("i");
        deleteIcon.title = "Delete";
        deleteIcon.className =
          "fas fa-times action-grid action-close text-danger";
        deleteIcon.onclick = () => {
          this.deleteMonitorWidget(sourceDataRow.id);
        };

        cellWrapper.appendChild(editIcon);
        cellWrapper.appendChild(deleteIcon);

        return cellWrapper;
      }
      return input;
    },
    deleteMonitorWidget(widgetId) {
      createMessageModal(
        "Are you sure you want to delete this monitor widget?",
        () => {
          let widget = this.widgets.find((widget) => widget.id === widgetId);
          if (!!widget) {
            this.$emit("toggleWidget", widget);
          }
          axios
            .delete(`/monitoring-widgets/user-created/${widgetId}`)
            .then((resp) => {
              this.getMonitoringWidgetList();
            })
            .catch((error) => {
              showToast("error", error);
            });
        }
      );
    },
    editMonitoringWidget(widgetId = null) {
      $(this.$refs.monitoringWidgetsModal).modal("hide");
      this.editModalVisible = true;
      this.editWidgetId = widgetId;
    },
    onEditHide() {
      this.editModalVisible = false;
      this.editWidgetId = null;
    },
  },
};
</script>
