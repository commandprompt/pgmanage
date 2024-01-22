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
          <h2 class="modal-title font-weight-bold">Monitoring Units</h2>
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
            @click="editMonitorWidget"
            class="btn btn-primary btn-sm mr-3"
          >
            New Unit
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from "axios";
import { TabulatorFull as Tabulator } from "tabulator-tables";

export default {
  name: "MonitoringWidgetsModal",
  props: {
    widgetsModalVisible: Boolean,
    connId: String,
    databaseIndex: Number,
    widgets: Array,
  },
  emits: ["modalHide", "toggleWidget"],
  data() {
    return {
      table: null,
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
        this.getMonitorWidgetList();
      } else {
        if (!!this.table) this.table.destroy();
      }
    },
  },
  methods: {
    getMonitorWidgetList() {
      axios
        .post("/get_monitor_widget_list/", {
          database_index: this.databaseIndex,
          tab_id: this.connId,
        })
        .then((resp) => {
          console.log(resp);
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
                field: "actions",
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
        editIcon.onclick = function () {
          //   editMonitorUnit(sourceDataRow.id);
        };

        const deleteIcon = document.createElement("i");
        deleteIcon.title = "Delete";
        deleteIcon.className =
          "fas fa-times action-grid action-close text-danger";
        deleteIcon.onclick = function () {
          //   deleteMonitorUnit(sourceDataRow.id);
        };

        cellWrapper.appendChild(editIcon);
        cellWrapper.appendChild(deleteIcon);

        return cellWrapper;
      }
      return input;
    },
  },
};
</script>
