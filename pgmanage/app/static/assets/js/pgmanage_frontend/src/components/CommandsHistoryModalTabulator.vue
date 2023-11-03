<template>
  <div class="modal fade" ref="historyModal" tabindex="-1" role="dialog" aria-hidden="true">
    <div class="modal-dialog modal-xl" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 class="modal-title font-weight-bold">
            Command history Tabulator
          </h2>
          <button type="button" class="close" data-dismiss="modal" aria-label="Close">
            <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
          </button>
        </div>
        <div class="modal-body">
          <div :id="`command_history_div_${tabId}_tabulator`" class="query_command_history">
            <div :id="`command_history_header_${tabId}_tabulator`" class="query_command_history_header">
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
                      <input v-model="commandContains"  @change="getCommandList()" type="text" class="form-control" />
                    </div>

                    <button class="bt_execute btn btn-primary ml-1" title="Refresh" @click="getCommandList()">
                      <i class="fas fa-sync-alt mr-1"></i>
                      Refresh
                    </button>

                    <button class="bt_execute btn btn-danger ml-1" title="Clear List">
                      <i class="fas fa-broom mr-1"></i>
                      Clear List
                    </button>
                  </div>
                </div>
              </div>

              <div ref="daterangePicker" class="position-relative"></div>

              <div class="pagination d-flex align-items-center mb-3">
                <button class="pagination__btn mr-2">First</button>
                <button class="pagination__btn mx-2">
                  <i class="fa-solid fa-arrow-left"></i>
                  Previous
                </button>
                <div class="pagination__pages mx-3">
                  <span>{{ currentPage }}</span>
                  /
                  <span>{{ pages }}</span>
                </div>

                <button class="pagination__btn mx-2">
                  Next
                  <i class="fa-solid fa-arrow-right"></i>
                </button>

                <button class="pagination__btn ml-2">Last</button>
              </div>
            </div>
            <!--DIV FOR TABULATOR-->
            <div id="tabulator-example" class="query_command_history_grid"
              style="width: 100%; height: calc(100vh - 20rem); overflow: hidden"></div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from "axios";
import moment from "moment";
import { TabulatorFull as Tabulator } from "tabulator-tables";
import { emitter } from "../emitter";

export default {
  setup() { },
  name: "CommandsHistoryModal",
  props: {
    tabId: String,
    databaseIndex: Number,
  },
  data() {
    return {
      currentPage: 1,
      pages: 1,
      startedFrom: moment().subtract(6, "hour").toISOString(),
      startedTo: moment().toISOString(),
      commandContains: "",
      timeRangeLabel: "Last 6 Hours",
    };
  },
  mounted() {
    this.setupDateRangePicker();
    this.setupTabulator();
  },
  methods: {
    show() {
      $(this.$refs.historyModal).modal("show");
      setTimeout(() => {
        this.getCommandList();
      }, 200);
    },
    setupDateRangePicker() {
      $(this.$refs.timeRange).daterangepicker(
        {
          timePicker: true,
          startDate: moment(this.startedFrom).format("Y-MM-DD H"),
          endDate: moment(this.startedTo).format("Y-MM-DD H"),
          parentEl: this.$refs.daterangePicker,
          previewUTC: true,
          locale: {
            format: "Y-MM-DD H",
          },
          ranges: {
            "Last 6 Hours": [
              moment().subtract(6, "hour").format("Y-MM-DD H"),
              moment().format("Y-MM-DD H"),
            ],
            "Last 12 Hours": [
              moment().subtract(12, "hour").format("Y-MM-DD H"),
              moment().format("Y-MM-DD H"),
            ],
            "Last 24 Hours": [
              moment().subtract(24, "hour").format("Y-MM-DD H"),
              moment().format("Y-MM-DD H"),
            ],
            "Last 7 Days": [
              moment().subtract(7, "days").startOf("day").format("Y-MM-DD H"),
              moment().format("Y-MM-DD H"),
            ],
            "Last 30 Days": [
              moment().subtract(30, "days").startOf("day").format("Y-MM-DD H"),
              moment().format("Y-MM-DD H"),
            ],
            Yesterday: [
              moment().subtract(1, "days").startOf("day").format("Y-MM-DD H"),
              moment().subtract(1, "days").endOf("day").format("Y-MM-DD H"),
            ],
            "This Month": [
              moment().startOf("month").format("Y-MM-DD H"),
              moment().format("Y-MM-DD H"),
            ],
            "Last Month": [
              moment()
                .subtract(1, "month")
                .startOf("month")
                .format("Y-MM-DD H"),
              moment().subtract(1, "month").endOf("month").format("Y-MM-DD H"),
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
          this.getCommandList();
        }
      );
    },
    setupTabulator() {
      this.table = new Tabulator("#tabulator-example", {
        placeholder: "No Data Available",
        selectable: true,
        layout:"fitDataTable",
        columns: [
          {
            title: "Start",
            field: "start_time",
            headerSort: false,
            headerHozAlign: "center",
          },
          {
            title: "End",
            field: "end_time",
            headerSort: false,
            headerHozAlign: "center",
          },
          {
            title: "Duration",
            field: "duration",
            headerSort: false,
            headerHozAlign: "center",
          },
          {
            title: "Status",
            field: "status",
            hozAlign: "center",
            headerSort: false,
            headerHozAlign: "center",
            formatter: function (cell, formatterParams, onRendered) {
              if (cell.getValue() === "success") {
                return "<i title='Success' class='fas fa-check text-success'></i>";
              } else {
                return "<i title='Error' class='fas fa-exclamation-circle text-danger'></i>";
              }
            },
          },
          {
            title: "Command",
            field: "snippet",
            headerSort: false,
            headerHozAlign: "center",
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
        ],
      });
    },
    getCommandList() {
      axios
        .post("/get_command_list_tabulator/", {
          command_from: this.startedFrom,
          command_to: this.startedTo,
          command_contains: this.commandContains,
          current_page: this.currentPage,
          database_index: this.databaseIndex,
        })
        .then((resp) => {
          this.pages = resp.data.pages;

          resp.data.command_list.forEach((el) => {
            el.start_time = moment(el.start_time).format();
            el.end_time = moment(el.end_time).format();
          });
          this.table.setData(resp.data.command_list);  
        })
        .catch((error) => {
          console.log(error);
        });
    },
    clearCommandList() {
      
    }
  },
};
</script>
