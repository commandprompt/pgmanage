<template>
  <div class="modal fade" ref="historyModal" tabindex="-1" role="dialog" aria-hidden="true">
    <div class="modal-dialog modal-xl" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 class="modal-title font-weight-bold">
            {{ tabType }} commands history
          </h2>
          <button type="button" class="close" data-dismiss="modal" aria-label="Close">
            <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
          </button>
        </div>
        <div class="modal-body">
          <div class="mb-3">
            <div class="form-row">
              <div class="form-group col-5">
                <p class="font-weight-bold mb-2">Select a daterange:</p>
                <input v-model="startedFrom" type="text" class="form-control form-control-sm d-none"
                  placeholder="Start Time" />
                <input v-model="startedTo" type="text" class="form-control form-control-sm d-none"
                  placeholder="End Time" />
                <button ref="timeRange" type="button" class="btn btn-outline-primary">
                  <i class="far fa-calendar-alt"></i>
                  <span class="mx-1">{{ timeRangeLabel }}</span><i class="fa fa-caret-down"></i>
                </button>
              </div>

              <div class="form-group col-7 d-flex justify-content-end align-items-end">
                <div>
                  <label class="font-weight-bold mb-2">Command contains:</label>
                  <input v-model="commandContains" @change="getCommandsHistory()" type="text" class="form-control" />
                </div>

                <button class="bt_execute btn btn-primary ml-1" title="Refresh" @click="getCommandsHistory()">
                  <i class="fas fa-sync-alt mr-1"></i>
                  Refresh
                </button>
                <ConfirmableButton :confirm-text="`Confirm Clear?`" :callbackFunc="clearCommandsHistory"
                  class="btn btn-danger ml-1">
                  <i class="fas fa-broom mr-1"></i>
                  Clear List
                </ConfirmableButton>
              </div>
            </div>
          </div>

          <div ref="daterangePicker" class="position-relative"></div>

          <div class="pagination d-flex align-items-center mb-3">
            <button class="pagination__btn mr-2" @click="getFirstPage()">
              First
            </button>
            <button class="pagination__btn mx-2" @click="getPreviousPage()">
              <i class="fa-solid fa-arrow-left"></i>
              Previous
            </button>
            <div class="pagination__pages mx-3">
              <span>{{ currentPage }}</span>
              /
              <span>{{ pages }}</span>
            </div>

            <button class="pagination__btn mx-2" @click="getNextPage()">
              Next
              <i class="fa-solid fa-arrow-right"></i>
            </button>

            <button class="pagination__btn ml-2" @click="getLastPage()">
              Last
            </button>
          </div>

          <div :id="`${tabId}_commands_history_table`" class="tabulator-custom" style="height: calc(100vh - 20rem)"></div>
        </div>
      </div>
    </div>
  </div>

  <CellDataModal :cell-content="cellContent" :show-modal="cellModalVisible" @modal-hide="cellModalVisible = false" />
</template>

<script>
import axios from "axios";
import moment from "moment";
import { TabulatorFull as Tabulator } from "tabulator-tables";
import { emitter } from "../emitter";
import ConfirmableButton from "./ConfirmableButton.vue";
import { showToast } from "../notification_control";
import CellDataModal from "./CellDataModal.vue";

export default {
  name: "CommandsHistoryModal",
  components: {
    ConfirmableButton,
    CellDataModal,
  },
  props: {
    tabId: String,
    databaseIndex: Number,
    tabType: {
      type: String,
      validator(value) {
        return ["Query", "Console"].includes(value);
      },
    },
    commandsModalVisible: Boolean,
  },
  emits: ["modalHide"],
  data() {
    return {
      currentPage: 1,
      pages: 1,
      startedFrom: moment().subtract(6, "hour").toISOString(),
      startedTo: moment().toISOString(),
      commandContains: "",
      timeRangeLabel: "Last 6 Hours",
      cellContent: "",
      cellModalVisible: false,
    };
  },
  computed: {
    defaultColumns() {
      if (this.tabType === "Query") {
        return [
          {
            title: "Start",
            field: "start_time",
          },
          {
            title: "End",
            field: "end_time",
          },
          {
            title: "Duration",
            field: "duration",
          },
          {
            title: "Status",
            field: "status",
            hozAlign: "center",
            formatter: function (cell, formatterParams, onRendered) {
              if (cell.getValue() === "success") {
                return "<i title='Success' class='fas fa-check text-success'></i>";
              } else {
                return "<i title='Error' class='fas fa-exclamation-circle text-danger'></i>";
              }
            },
          },
          {
            title: "Database",
            field: "database",
          },
          {
            title: "Command",
            field: "snippet",
            contextMenu: [
              {
                label: "Copy Content To Query Tab",
                action: (e, cell) => {
                  emitter.emit(`${this.tabId}_copy_to_editor`, cell.getValue());
                  $(this.$refs.historyModal).modal("hide");
                },
              },
            ],
          },
        ];
      }
      return [
        {
          title: "Date",
          field: "start_time",
        },
        {
          title: "Database",
          field: "database",
        },
        {
          title: "Command",
          field: "snippet",
          contextMenu: [
            {
              label:
                '<div style="position: absolute;"><i class="fas fa-copy cm-all" style="vertical-align: middle;"></i></div><div style="padding-left: 30px;">Copy</div>',
              action: (e, cell) => {
                this.table.selectRow(cell.getRow());
                this.table.copyToClipboard("selected");
              },
            },
            {
              label:
                '<div style="position: absolute;"><i class="fas fa-bolt cm-all" style="vertical-align: middle;"></i></div><div style="padding-left: 30px;">Copy Content To Console Tab</div>',
              action: (e, cell) => {
                emitter.emit(`${this.tabId}_copy_to_editor`, cell.getValue());
                $(this.$refs.historyModal).modal("hide");
              },
            },
            {
              label:
                '<div style="position: absolute;"><i class="fas fa-edit cm-all" style="vertical-align: middle;"></i></div><div style="padding-left: 30px;">View Content</div>',
              action: (e, cell) => {
                this.cellContent = cell.getValue();
                this.cellModalVisible = true;
              },
            },
          ],
        },
      ];
    },
  },
  watch: {
    commandsModalVisible: function () {
      if (this.commandsModalVisible) {
        this.resetToDefault();
        this.showCommandsModal();
        setTimeout(() => {
          this.getCommandsHistory();
        }, 200);
      }
    },
  },
  mounted() {
    this.setupTabulator();

    $(this.$refs.historyModal).on("shown.bs.modal", () => {
      this.setupDateRangePicker();
    });

    $(this.$refs.historyModal).on("hide.bs.modal", () => {
      this.$emit("modalHide");
      $(this.$refs.timeRange).data('daterangepicker').remove()
    });
  },
  methods: {
    setupDateRangePicker() {
      $(this.$refs.timeRange).daterangepicker(
        {
          timePicker: true,
          startDate: moment(this.startedFrom).format(),
          endDate: moment(this.startedTo).format(),
          parentEl: this.$refs.daterangePicker,
          previewUTC: true,
          locale: {
            format: moment.defaultFormat,
          },
          ranges: {
            "Last 6 Hours": [
              moment().subtract(6, "hour").format(),
              moment().format(),
            ],
            "Last 12 Hours": [
              moment().subtract(12, "hour").format(),
              moment().format(),
            ],
            "Last 24 Hours": [
              moment().subtract(24, "hour").format(),
              moment().format(),
            ],
            "Last 7 Days": [
              moment().subtract(7, "days").startOf("day").format(),
              moment().format(),
            ],
            "Last 30 Days": [
              moment().subtract(30, "days").startOf("day").format(),
              moment().format(),
            ],
            Yesterday: [
              moment().subtract(1, "days").startOf("day").format(),
              moment().subtract(1, "days").endOf("day").format(),
            ],
            "This Month": [
              moment().startOf("month").format(),
              moment().format(),
            ],
            "Last Month": [
              moment()
                .subtract(1, "month")
                .startOf("month")
                .format(),
              moment().subtract(1, "month").endOf("month").format(),
            ],
          },
        },
        (start, end, label) => {
          this.startedFrom = moment(start).toISOString();

          // Update Button Labels
          if (label === "Custom Range") {
            this.timeRangeLabel = `${start.format(
              "MMMM D, YYYY hh:mm A"
            )}-${end.format("MMMM D, YYYY hh:mm A")}`;
          } else {
            this.timeRangeLabel = label;
          }

          if (
            label === "Custom Range" ||
            label === "Yesterday" ||
            label === "Last Month"
          ) {
            this.startedTo = moment(end).toISOString();
          } else {
            this.startedTo = null;
          }
          this.getCommandsHistory();
        }
      );
    },
    setupTabulator() {
      this.table = new Tabulator(`#${this.tabId}_commands_history_table`, {
        placeholder: "No Data Available",
        selectable: true,
        layout: "fitDataStretch",
        width: '100%',
        clipboard: "copy",
        clipboardCopyConfig: {
          columnHeaders: false, //do not include column headers in clipboard output
        },
        clipboardCopyFormatter: function (type, output) {
          if (type == "plain") {
            return output.split("\t").pop();
          }
          return output;
        },
        columnDefaults: {
          headerHozAlign: "left",
          headerSort: false,
        },
        columns: this.defaultColumns,
      });
    },
    getCommandsHistory() {
      axios
        .post("/get_commands_history/", {
          command_from: this.startedFrom,
          command_to: this.startedTo,
          command_contains: this.commandContains,
          command_type: this.tabType,
          current_page: this.currentPage,
          database_index: this.databaseIndex,
        })
        .then((resp) => {
          this.pages = resp.data.pages;
          if (this.currentPage > resp.data.pages) this.currentPage = 1;

          resp.data.command_list.forEach((el) => {
            el.start_time = moment(el.start_time).format();
            if (el.end_time) el.end_time = moment(el.end_time).format();
          });
          this.table.setData(resp.data.command_list);
          this.table.redraw();
        })
        .catch((error) => {
          showToast("error", error.response.data.data);
        });
    },
    clearCommandsHistory() {
      axios
        .post("/clear_commands_history/", {
          command_from: this.startedFrom,
          command_to: this.startedTo,
          command_contains: this.commandContains,
          database_index: this.databaseIndex,
          command_type: this.tabType,
        })
        .then((resp) => {
          this.currentPage = 1;
          this.getCommandsHistory();
        })
        .catch((error) => {
          showToast("error", error.response.data.data);
        });
    },
    getNextPage() {
      if (this.currentPage < this.pages) {
        this.currentPage += 1;
        this.getCommandsHistory();
      }
    },
    getPreviousPage() {
      if (this.currentPage > 1) {
        this.currentPage -= 1;
        this.getCommandsHistory();
      }
    },
    getFirstPage() {
      if (this.currentPage !== 1) {
        this.currentPage = 1;
        this.getCommandsHistory();
      }
    },
    getLastPage() {
      if (this.currentPage !== this.pages) {
        this.currentPage = this.pages;
        this.getCommandsHistory();
      }
    },
    showCommandsModal() {
      $(this.$refs.historyModal).modal("show");
    },
    resetToDefault() {
      this.startedFrom = moment().subtract(6, "hour").toISOString();
      this.startedTo = moment().toISOString();
      this.timeRangeLabel = "Last 6 Hours";
      this.commandContains = "";
    },
  },
};
</script>
