<template>
  <div class="modal fade" id="pgCronModal" tabindex="-1" role="dialog" aria-hidden="true">
    <div class="modal-dialog modal-xl" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 class="modal-title font-weight-bold">{{ modalTitle }}</h2>
          <button type="button" class="close" data-dismiss="modal" aria-label="Close">
            <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
          </button>
        </div>

        <div class="modal-body">

          <ul class="nav nav-tabs" role="tablist">
            <li class="nav-item">
              <a class="nav-link active" id="job_schedule-tab" data-toggle="tab" href="#job_schedule"
                role="tab" aria-controls="job_schedule" aria-selected="true">Schedule</a>
            </li>
            <li v-if="mode==='Edit'" class="nav-item">
              <a class="nav-link" id="job_statistics-tab" data-toggle="tab" href="#job_statistics" role="tab"
                aria-controls="job_statistics" aria-selected="false">Job Statistics</a>
            </li>
          </ul>
          <div class="tab-content p-3  flex-grow-1">
            <!-- Job main tab -->
            <div class="tab-pane fade show active" id="job_schedule" role="tabpanel"
                aria-labelledby="job_schedule-tab">
              <div class="form-row">
                <div class="form-group col-6 mb-2">
                  <label for="job_name" class="font-weight-bold mb-2">Job Name</label>
                  <input
                    v-model="jobName" id="job_name" type="text" :disabled="jobId"
                    :class="['form-control', { 'is-invalid': v$.jobName.$invalid }]">
                  <div class="invalid-feedback">
                    <span v-for="error of v$.jobName.$errors" :key="error.$uid">
                      {{ error.$message }}
                    </span>
                  </div>
                </div>

                <div class="form-group col-6 mb-2">
                  <label for="in_database" class="font-weight-bold mb-2">Run In Database</label>
                  <select v-model="inDatabase" :disabled="jobId" id="in_database" class="form-control">
                      <option value=""></option>
                      <option v-for="(database, index) in databases"
                        :key=index
                        :value="database">
                          {{database}}
                      </option>
                  </select>
                </div>
              </div>

              <div class="form-group mb-2">
                <label class="font-weight-bold mb-2">Run At</label>
                <div
                  @click.capture="clickProxy"
                  :class="[{ 'vcron-disabled': manualInput }]">
                  <cron-light v-model="schedule" @error="scheduleError=$event"></cron-light>
                </div>
              </div>

              <div class="form-row">
                <div class="form-group col-4">
                  <label for="schedule_override" class="font-weight-bold mb-2">Cron Expression</label>
                  <input
                    v-model="scheduleOverride" id="schedule_override" type="text" :disabled="!manualInput"
                    :class="['form-control', { 'is-invalid': scheduleError }]">
                  <div class="invalid-feedback">
                    <span>
                      {{ scheduleError }}
                    </span>
                  </div>
                </div>
                <div class="form-group col-4 d-flex align-items-end">
                  <div class="custom-control custom-switch mb-2">
                    <input v-model="manualInput" id="manual_switch" type="checkbox" class="custom-control-input" >
                    <label class="custom-control-label font-weight-bold" for="manual_switch">Define Manually</label>
                  </div>
                </div>
              </div>

              <div class="form-group mb-2">
                <p class="font-weight-bold mb-2">Command to Run</p>
                <div id="job_command" style="height: 20vh">
                </div>
                <div :class="[{ 'is-invalid': v$.command.$invalid }]"></div>
                <div :class="[{ 'is-invalid': v$.command.$invalid }]" class="invalid-feedback">
                  <span v-for="error of v$.command.$errors" :key="error.$uid">
                    {{ error.$message }}
                  </span>
                </div>
              </div>
            </div>

            <!-- Job stats tab -->
            <div v-if="mode==='Edit'" class="tab-pane fade show" id="job_statistics" role="tabpanel"
                aria-labelledby="job_statistics-tab" style="height: 50vh">
                <div v-if="jobLogs.length">
                  <div class='job-statistics-tab__header d-flex justify-content-between align-items-center pb-3'>
                    <h3 class="mb-0">{{jobStatsHeader}}</h3>
                    <div>
                      <button @click="clearJobStats" class="btn btn-outline-primary">Clear Statistics</button>
                    </div>
                  </div>
                </div>
                <div id="job_statistics_grid"></div>
                <h4 v-if="jobLogs.length" class="mb-0 mt-2">Showing last {{jobLogs.length}} records</h4>
            </div>
          </div>
        </div>

        <div class="modal-footer mt-auto justify-content-between">
          <ConfirmableButton v-if="mode==='Edit'" :callbackFunc="deleteJob" class="btn btn-danger m-0" />
          <button type="button" class="btn btn-primary m-0 ml-auto"
            @click="saveJob">
            Save
          </button>
        </div>
      </div>
    </div>
  </div>

</template>

<style scoped>
  .modal-content {
    min-height: calc(100vh - 200px);
  }

  .modal-body {
    display: flex;
    flex-direction: column;
  }

  .modal-footer {
    z-index: unset;
  }
</style>

<script>

import ConfirmableButton from './ConfirmableButton.vue'
import { required, maxLength } from '@vuelidate/validators'
import { useVuelidate } from '@vuelidate/core'
import { emitter } from '../emitter'
import ace from 'ace-builds'
import axios from 'axios'
import { showToast } from '../notification_control'
import moment from 'moment'
import { settingsStore } from '../stores/settings'
import { TabulatorFull as Tabulator } from "tabulator-tables";

export default {
  name: 'PgCronModal',
  components: {
      ConfirmableButton
  },
  props: {
    mode: String,
    treeNode: Object
  },
  data() {
    return {
      tree: window.v_connTabControl.selectedTab.tag.tree,
      tabId: window.v_connTabControl.selectedTab.id,
      databaseIndex: window.v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
      error: '',
      manualInput: false,
      jobName: '',
      jobId: null,
      command: '',
      databases: [],
      inDatabase: null,
      schedule: '* * * * *',
      scheduleOverride: '* * * * *',
      scheduleError: null,
      jobLogs: [],
      jobStats: null
    }
  },

  validations() {
    let baseRules = {
        jobName: {
          required: required,
          maxLength: maxLength(20),
        },
        command: {
          required,
        },
        schedule: {
          required,
        },
    }
    return baseRules;
  },

  setup() {
    return { v$: useVuelidate({ $lazy: true }) }
  },

  computed: {
    modalTitle() {
      if (this.mode === 'Edit') return 'Edit Job'
      return 'Create Job'
    },
    jobStatsHeader() {
      if(this.jobStats) {
        let total = parseInt(this.jobStats.succeeded) + parseInt(this.jobStats.failed)
        return `Total Runs: ${total} (${this.jobStats.failed} failed)`
      }
      return ''
    }
  },

  watch: {
    command() {
      this.editor.setValue(this.command)
      this.editor.clearSelection();
    },
    // the following two watchers sync schedule inputs in both directions
    // this is done instead of using model binding on both inputs to avoid
    // vue-js-cron from hindering typing in manual schedule input box
    schedule() {
      // sync input box unless manual schedule input is ON
      if(!this.manualInput) {
        this.scheduleOverride = this.schedule
      }
    },
    scheduleOverride() {
      // sync cron-input widget if manual schedule input is ON
      if(this.manualInput) {
        this.schedule = this.scheduleOverride
      }
    },
    manualInput() {
      if(!this.manualInput) {
        this.schedule = this.scheduleOverride
      }
    }
  },

  mounted() {
    this.getDatabases()
    if (this.mode === 'Edit') {
        this.getJobDetails()
        $('#job_statistics-tab').on('shown.bs.tab', this.setupJobStatisticsTab)
    }
    this.setupEditor()
    $('#pgCronModal').modal('show')
  },

  methods: {
    clickProxy(e) {
      //block click events if vue-cron component is "disabled"
      if(this.manualInput) {
        e.stopPropagation()
      }
    },

    getDatabases() {
      axios.post('/get_databases_postgresql/', {
        database_index: this.databaseIndex,
        tab_id: this.tabId,
      })
        .then((resp) => {
          this.databases = resp.data.map((x) => x.name)
        })
        .catch((error) => {
            console.log(error)
        })
    },

    getJobDetails() {
      axios.post('/get_pgcron_job_details/', {
        database_index: this.databaseIndex,
        tab_id: this.tabId,
        job_meta: this.treeNode.data.job_meta
      })
        .then((resp) => {
          this.jobId = resp.data.jobid
          this.jobName = resp.data.jobname
          this.schedule = resp.data.schedule
          this.command = resp.data.command
          this.inDatabase = resp.data.database
        })
        .catch((error) => {
            console.log(error)
        })
    },

    setupJobStatisticsTab() {
      const grid_columns = [
      {'title': 'Run ID', field: "runid"},
        {'title': 'Job PID', field: "job_pid"},
        {'title': 'Database', field: "database"},
        {'title': 'Username', field: "username"},
        {'title': 'Status', field: "status", formatter: function (cell, formatterParams, onRendered) {
              if (cell.getValue() === "succeeded") {
                return "<div class='text-center'><i title='Success' class='fas fa-check text-success action-grid action-status-ok'></i></div>";
              } else {
                return "<div class='text-center'><i title='Error' class='fas fa-exclamation-circle text-danger action-grid action-status-error'></i></div>";
              }
            },},
        {'title': 'Start', field: "start_time",},
        {'title': 'End', field: "end_time",},
        {'title': 'Return Message', field: "return_message"},
        {'title': 'Command', field: "command", layout: "fitData"},
      ]
      axios.post('/get_pgcron_job_logs/', {
        database_index: this.databaseIndex,
        tab_id: this.tabId,
        job_meta: this.treeNode.data.job_meta
      })
        .then((resp) => {
          this.jobLogs = resp.data.logs

          this.jobLogs.forEach(log => {
            log.job_pid = !!log.job_pid ? log.job_pid : "N/A"
            log.start_time = moment(log.start_time).isValid() ? moment(log.start_time).format() : "N/A"
            log.end_time = moment(log.end_time).isValid() ? moment(log.end_time).format() : "N/A"
          })

          this.jobStats = resp.data.stats
          let table = new Tabulator('#job_statistics_grid', {
            placeholder: "No Logs",
            height:"41vh",
            width: "100%",
            layout: "fitDataStretch", 
            columnDefaults: {
            headerHozAlign: "center",
            headerSort: false,
          },
          columns: grid_columns,
          data: this.jobLogs
          })
        })
        .catch((error) => {
          showToast("error", error.response.data.data)
        })
    },

    clearJobStats() {
      axios.post('/delete_pgcron_job_logs/', {
        database_index: this.databaseIndex,
        tab_id: this.tabId,
        job_meta: this.treeNode.data.job_meta
      })
        .then((resp) => {
          this.setupJobStatisticsTab()
        })
        .catch((error) => {
          showToast("error", error.response.data.data)
        })
    },

    saveJob() {
      this.v$.$validate()
      if(!this.v$.$invalid) {
          axios.post('/save_pgcron_job/', {
            database_index: this.databaseIndex,
            tab_id: this.tabId,
            jobId: this.jobId,
            jobName: this.jobName,
            schedule: this.schedule,
            command: this.command,
            inDatabase: this.inDatabase
          })
            .then((resp) => {
              emitter.emit(`refreshNode_${this.tree.id}`, {"node": this.treeNode})
              $('#pgCronModal').modal('hide')
            })
            .catch((error) => {
              showToast("error", error.response.data.data)
            })
        }
    },

    setupEditor() {
      this.editor = ace.edit('job_command');
      this.editor.setTheme("ace/theme/" + settingsStore.editorTheme);
      this.editor.session.setMode("ace/mode/sql");
      this.editor.setFontSize(Number(settingsStore.fontSize));
      this.editor.$blockScrolling = Infinity;

      this.editor.setValue(this.command)
      this.editor.clearSelection();
      this.editor.on('change', () => {
    	  this.command = this.editor.getValue()
      })
    },

    deleteJob() {
      axios.post('/delete_pgcron_job/', {
        database_index: this.databaseIndex,
        tab_id: this.tabId,
        job_meta: this.treeNode.data.job_meta
      })
        .then((resp) => {
          emitter.emit(`removeNode_${this.tree.id}`, {"node": this.treeNode})
          $('#pgCronModal').modal('hide')
        })
        .catch((error) => {
          showToast("error", error.response.data.data)
        })
    }
  },
}
</script>