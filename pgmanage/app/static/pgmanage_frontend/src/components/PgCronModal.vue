<template>
  <div class="modal fade" id="pgCronModal" tabindex="-1" role="dialog" aria-hidden="true">
    <div class="modal-dialog modal-xl" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 class="modal-title fw-bold">{{ modalTitle }}</h2>
          <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
        </div>

        <div class="modal-body">

          <ul class="nav nav-tabs" role="tablist">
            <li class="nav-item">
              <a class="nav-link active" id="job_schedule-tab" data-bs-toggle="tab" href="#job_schedule"
                role="tab" aria-controls="job_schedule" aria-selected="true">Schedule</a>
            </li>
            <li v-if="mode===operationModes.UPDATE" class="nav-item">
              <a ref="jobStatistics" class="nav-link" id="job_statistics-tab" data-bs-toggle="tab" href="#job_statistics" role="tab"
                aria-controls="job_statistics" aria-selected="false">Job Statistics</a>
            </li>
          </ul>
          <div class="tab-content p-3  flex-grow-1">
            <!-- Job main tab -->
            <div class="tab-pane fade show active" id="job_schedule" role="tabpanel"
                aria-labelledby="job_schedule-tab">
              <div class="row">
                <div class="form-group col-6 mb-2">
                  <label for="job_name" class="fw-bold mb-2">Job Name</label>
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
                  <label for="in_database" class="fw-bold mb-2">Run In Database</label>
                  <select v-model="inDatabase" :disabled="jobId" id="in_database" class="form-select">
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
                <label class="fw-bold mb-2">Run At</label>
                <div
                  @click.capture="clickProxy"
                  :class="[{ 'vcron-disabled': manualInput }]">
                  <cron-light v-model="schedule" @error="scheduleError=$event"></cron-light>
                </div>
              </div>

              <div class="row">
                <div class="form-group col-4">
                  <label for="schedule_override" class="fw-bold mb-2">Cron Expression</label>
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
                  <div class="form-check form-switch mb-2">
                    <input v-model="manualInput" id="manual_switch" type="checkbox" class="form-check-input" >
                    <label class="form-check-label fw-bold" for="manual_switch">Define Manually</label>
                  </div>
                </div>
              </div>

              <div class="form-group mb-2">
                <p class="fw-bold mb-2">Command to Run</p>
                <div ref="editor" id="job_command" style="height: 20vh">
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
            <div v-if="mode===operationModes.UPDATE" class="tab-pane fade show" id="job_statistics" role="tabpanel"
                aria-labelledby="job_statistics-tab" style="height: 50vh">
                <div v-if="jobLogs.length">
                  <div class='job-statistics-tab__header d-flex justify-content-between align-items-center pb-3'>
                    <h3 class="mb-0">{{jobStatsHeader}}</h3>
                    <div>
                      <button @click="clearJobStats" class="btn btn-outline-primary">Clear Statistics</button>
                    </div>
                  </div>
                </div>
                <div id="job_statistics_grid" class="tabulator-custom"></div>
                <h4 v-if="jobLogs.length" class="mb-0 mt-2">Showing last {{jobLogs.length}} records</h4>
            </div>
          </div>
        </div>

        <div class="modal-footer mt-auto justify-content-between">
          <ConfirmableButton v-if="mode===operationModes.UPDATE" :callbackFunc="deleteJob" class="btn btn-danger m-0" />
          <button type="button" class="btn btn-primary m-0 ms-auto"
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
import axios from 'axios'
import moment from 'moment'
import { settingsStore } from '../stores/stores_initializer'
import { TabulatorFull as Tabulator } from 'tabulator-tables'
import { operationModes } from '../constants'
import { Modal } from 'bootstrap'
import { CronLight } from '@vue-js-cron/light'
import { handleError } from '../logging/utils';

export default {
  name: 'PgCronModal',
  components: {
      ConfirmableButton,
      CronLight,
  },
  props: {
    mode: operationModes,
    treeNode: Object,
    workspaceId: String,
    databaseIndex: Number,
  },
  data() {
    return {
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
      if (this.mode === operationModes.UPDATE) return 'Edit Job'
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
  created() {
    // allows for using operationModes in the template
    this.operationModes = operationModes
  },
  mounted() {
    this.getDatabases()
    if (this.mode === operationModes.UPDATE) {
        this.getJobDetails()
        this.$refs.jobStatistics.addEventListener('shown.bs.tab', this.setupJobStatisticsTab)
    }
    this.setupEditor()
    Modal.getOrCreateInstance('#pgCronModal').show()
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
        workspace_id: this.workspaceId,
      })
        .then((resp) => {
          this.databases = resp.data.map((x) => x.name)
        })
        .catch((error) => {
          handleError(error);
        })
    },

    getJobDetails() {
      axios.post('/get_pgcron_job_details/', {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
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
          handleError(error);
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
        {'title': 'Command', field: "command"},
      ]
      axios.post('/get_pgcron_job_logs/', {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
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
          handleError(error);
        })
    },

    clearJobStats() {
      axios.post('/delete_pgcron_job_logs/', {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
        job_meta: this.treeNode.data.job_meta
      })
        .then((resp) => {
          this.setupJobStatisticsTab()
        })
        .catch((error) => {
          handleError(error);
        })
    },

    saveJob() {
      this.v$.$validate()
      if(!this.v$.$invalid) {
          axios.post('/save_pgcron_job/', {
            database_index: this.databaseIndex,
            workspace_id: this.workspaceId,
            jobId: this.jobId,
            jobName: this.jobName,
            schedule: this.schedule,
            command: this.command,
            inDatabase: this.inDatabase
          })
            .then((resp) => {
              emitter.emit(`refreshNode_${this.workspaceId}`, {"node": this.treeNode})
              Modal.getOrCreateInstance('#pgCronModal').hide()
            })
            .catch((error) => {
              handleError(error);
            })
        }
    },

    setupEditor() {
      this.editor = ace.edit(this.$refs.editor);
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
        workspace_id: this.workspaceId,
        job_meta: this.treeNode.data.job_meta
      })
        .then((resp) => {
          emitter.emit(`removeNode_${this.workspaceId}`, {"node": this.treeNode})
          Modal.getOrCreateInstance('#pgCronModal').hide()
        })
        .catch((error) => {
          handleError(error);
        })
    }
  },
}
</script>