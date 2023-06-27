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
          <div class="tab-content p-3">
            <!-- Job main tab -->
            <div class="tab-pane fade show active" id="job_schedule" role="tabpanel"
                aria-labelledby="job_schedule-tab">
              <div class="form-row">
                <div class="form-group col-6 mb-2">
                  <label for="job_name" class="font-weight-bold mb-1">Job Name</label>
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
                  <label for="in_database" class="font-weight-bold mb-1">Run In Database</label>
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
                <label class="font-weight-bold mb-1">Run At</label>
                <div @click.capture="clickProxy">
                  <cron-light v-model="schedule" @error="scheduleError=$event"></cron-light>
                </div>
              </div>

              <div class="form-row">
                <div class="form-group col-4">
                  <label for="schedule_override" class="font-weight-bold mb-1">Cron Expression</label>
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
                  <div class="custom-control custom-switch mb-1">
                    <input v-model="manualInput" id="manual_switch" type="checkbox" class="custom-control-input" >
                    <label class="custom-control-label font-weight-bold" for="manual_switch">Define Manually</label>
                  </div>
                </div>
              </div>

              <div class="form-group mb-2">
                <p class="font-weight-bold mb-1">Command to Run</p>
                <div id="job_command" style="height: 10vh">
                </div>
                <div :class="[{ 'is-invalid': v$.command.$invalid }]"></div>
                <div class="[{ 'is-invalid': v$.command.$invalid }]" class="invalid-feedback">
                  <span v-for="error of v$.command.$errors" :key="error.$uid">
                    {{ error.$message }}
                  </span>
                </div>
              </div>
            </div>

            <!-- Job stats tab -->
            <div v-if="mode==='Edit'" class="tab-pane fade show" id="job_statistics" role="tabpanel"
                aria-labelledby="job_statistics-tab">
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
                <div v-if="!jobLogs.length">No Logs</div>
            </div>
          </div>
        </div>

        <div class="modal-footer mt-auto justify-content-between">
          <ConfirmableButton v-if="mode==='Edit'" :callbackFunc="deleteJob" class="btn btn-danger mr-2" />
          <button type="button" class="btn btn-primary mr-2 ml-auto"
            @click="saveJob">
            Save
          </button>
        </div>
      </div>
    </div>
  </div>

</template>


<script>
const { required, maxLength } = window.VuelidateValidators
import ConfirmableButton from './ConfirmableButton.vue'

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
    return { v$: Vuelidate.useVuelidate({ $lazy: true }) }
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
        p_database_index: this.databaseIndex,
        p_tab_id: this.tabId,
      })
        .then((resp) => {
          this.databases = resp.data.v_data.map((x) => x.v_name)
        })
        .catch((error) => {
            console.log(error)
        })
    },

    getJobDetails() {
      axios.post('/get_pgcron_job_details/', {
        database_index: this.databaseIndex,
        tab_id: this.tabId,
        job_meta: this.treeNode.tag.job_meta
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
      let container = document.querySelector('#job_statistics_grid')
      // reset grid container state if it already has hansontable stuff
      // this is needed to properly hide the grid when stats are cleared
      container.innerHTML = ''
      container.removeAttribute('class')
      container.removeAttribute('style')

      const grid_columns = [
        {'title': 'Run ID', readOnly: true},
        {'title': 'Job PID', readOnly: true},
        {'title': 'Database', readOnly: true},
        {'title': 'Username', readOnly: true},
        {'title': 'Status', readOnly: true, renderer: 'html'},
        {'title': 'Start', readOnly: true, },
        {'title': 'End', readOnly: true},
        {'title': 'Return Message', readOnly: true},
        {'title': 'Command', readOnly: true},
      ]
      axios.post('/get_pgcron_job_logs/', {
        database_index: this.databaseIndex,
        tab_id: this.tabId,
        job_meta: this.treeNode.tag.job_meta
      })
        .then((resp) => {
          this.jobLogs = resp.data.logs
          this.jobLogs.forEach(log => {
            log[5] = moment(log[5]).format()
            log[6] = moment(log[6]).format()
            if(log[4] === 'succeeded') {
              log[4] = "<div class='text-center'><i title='Success' class='fas fa-check text-success action-grid action-status-ok'></i></div>"
            } else {
              log[4] = "<div class='text-center'><i title='Error' class='fas fa-exclamation-circle text-danger action-grid action-status-error'></i></div>"
            }
          })

          this.jobStats = resp.data.stats
          // no sense in rendering the table if there is no logs
          if(this.jobLogs.length) {
            let hot = new Handsontable(container, {
              data: this.jobLogs,
              className: 'simple',
              columns : grid_columns,
              colHeaders : true,
              rowHeaders : false,
              height: 260,
              width: '100%',
              stretchH: 'last',
              licenseKey: 'non-commercial-and-evaluation' // for non-commercial use only
            });
          }
        })
        .catch((error) => {
          // FIXME: send a proper error response so that this code works
          showError(error.response.data.data);
        })
    },

    clearJobStats() {
      axios.post('/delete_pgcron_job_logs/', {
        database_index: this.databaseIndex,
        tab_id: this.tabId,
        job_meta: this.treeNode.tag.job_meta
      })
        .then((resp) => {
          this.setupJobStatisticsTab()
        })
        .catch((error) => {
          // FIXME: send a proper error response so that this code works
          showError(error.response.data.data);
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
              refreshTreePostgresql(this.treeNode)
              $('#pgCronModal').modal('hide')
            })
            .catch((error) => {
              // FIXME: send a proper error response so that this code works
              showError(error.response.data.data);
            })
        }
    },

    setupEditor() {
      this.editor = ace.edit('job_command');
      this.editor.setTheme("ace/theme/" + window.v_editor_theme);
      this.editor.session.setMode("ace/mode/sql");
      this.editor.setFontSize(Number(window.v_font_size));
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
        job_meta: this.treeNode.tag.job_meta
      })
        .then((resp) => {
          this.treeNode.removeNode()
          $('#pgCronModal').modal('hide')
        })
        .catch((error) => {
          showError(error.response.data.data);
        })
    }
  },
}
</script>