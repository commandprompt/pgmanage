<template>
  <div class="backup-tab-scrollable p-2">
  <form @submit.prevent>
    <div class="row">
      <div :class="(isNotServer) ? 'col-4':'col-12'" class="d-flex">
          <div class="card flex-grow-1">
            <h4 class="card-header fw-bold px-3 py-2">General</h4>
            <div class="card-body d-flex flex-column px-3 py-2">
              <div class="form-group mb-1">
                <label :for="`${restoreTabId}_restoreFileName`" class="fw-bold mb-1">File name</label>
                <div class="input-group">
                    <div class="btn btn-secondary" @click="openFileManagerModal">Select</div>
                  <input :id="`${restoreTabId}_restoreFileName`" type="text" class="form-control" :value="truncateText(restoreOptions.fileName, 20)"
                    placeholder="backup file" disabled>
                </div>
              </div>

              <div  class="form-group mb-1">
                <label for="restoreFormat" class="fw-bold mb-1">Format</label>
                <select id="restoreFormat" class="form-select" v-model="restoreOptions.format">
                  <option value="custom/tar">Custom or tar</option>
                  <option value="directory">Directory</option>
                </select>
              </div>

              <div v-if="!isNotServer" class="form-group mb-1 mt-2">
                <div class="form-check form-switch">
                  <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsEchoQueries`"
                    v-model="restoreOptions.echo_queries">
                  <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsEchoQueries`">
                    Echo all queries
                  </label>
                </div>

                <div class="form-check form-switch">
                  <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsQuiet`" v-model="restoreOptions.quiet">
                  <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsQuiet`">Quiet mode</label>
                </div>
              </div>

              <div v-if="isNotServer" class="form-group mb-1">
                <label for="restoreNumberOfJobs" class="fw-bold mb-1">Number of jobs</label>
                <select id="restoreNumberOfJobs" class="form-select" v-model="restoreOptions.number_of_jobs">
                  <option value="" disabled>Select an item...</option>
                  <option v-for="number_of_jobs in numberOfJobs" :value="number_of_jobs" :key="number_of_jobs">{{ number_of_jobs }}</option>
                </select>
              </div>

              <div v-if="isNotServer" class="form-group mb-1">
                <label for="restoreRoleName" class="fw-bold mb-1">Restore as:</label>
                <select id="restoreRoleName" class="form-select" v-model="restoreOptions.role">
                  <option value="" disabled>Select an item...</option>
                  <option v-for="name in roleNames" :value="name" :key="name">{{ name }}</option>
                </select>
              </div>

              <div v-if="!isWindowsOS" class="form-group mb-1">
                <div class="form-check form-switch">
                  <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsPigz`" v-model="restoreOptions.pigz" :disabled="isDirectoryFormat">
                  <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsPigz`">
                    Decompress with Pigz
                  </label>
                </div>
              </div>
              
                <div class="form-group" :class="(restoreOptions.pigz) ? 'collapse show':'collapse'">
                  <label for="restorePigzNumberOfJobs" class="fw-bold mb-1">Number of jobs</label>
                  <select id="restorePigzNumberOfJobs" class="form-select" v-model="restoreOptions.pigz_number_of_jobs">
                    <option v-for="number_of_jobs in pigzNumberOfJobs" :value="number_of_jobs" :key="number_of_jobs">{{ number_of_jobs }}</option>
                  </select>
                </div>

            </div>
          </div>
      </div>

      <div v-if="isNotServer" class="d-flex col-4">
        <div class="card flex-grow-1">
          <h4 class="card-header fw-bold px-3 py-2">Data/Objects</h4>
          <div class="card-body d-flex flex-column px-3 py-2">
            <div class="form-group mb-1">
              <p class="fw-bold mb-1">Sections</p>
              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsPreData`"
                  v-model="restoreOptions.pre_data" :disabled="restoreOptions.only_data || restoreOptions.only_schema">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsPreData`">
                  Pre-data
                </label>
              </div>

              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsData`" v-model="restoreOptions.data"
                  :disabled="restoreOptions.only_data || restoreOptions.only_schema">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsData`">Data</label>
              </div>

              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsPostData`"
                  v-model="restoreOptions.post_data" :disabled="restoreOptions.only_data || restoreOptions.only_schema">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsPostData`">
                  Post-data
                </label>
              </div>
            </div>

            <div class="form-group mb-1">
              <p class="fw-bold mb-1">Type of objects</p>
              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsOnlyData`"
                  v-model="restoreOptions.only_data"
                  :disabled="restoreOptions.pre_data || restoreOptions.data || restoreOptions.post_data">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsOnlyData`">
                  Only data
                </label>
              </div>

              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsOnlySchema`"
                  v-model="restoreOptions.only_schema"
                  :disabled="restoreOptions.pre_data || restoreOptions.data || restoreOptions.post_data">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsOnlySchema`">
                  Only schema
                </label>
              </div>
            </div>
    
            <div class="form-group mb-1">
              <p class="fw-bold mb-1">Do not save</p>
              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsOwner`" v-model="restoreOptions.dns_owner">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsOwner`">
                  Owner
                </label>
              </div>

              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsPrivilege`"
                  v-model="restoreOptions.dns_privilege">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsPrivilege`">
                  Privilege
                </label>
              </div>

              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsTablespace`"
                  v-model="restoreOptions.dns_tablespace">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsTablespace`">
                  Tablespace
                </label>
              </div>

              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsComments`"
                  v-model="restoreOptions.no_comments">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsComments`">
                  Comments
                </label>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div v-if="isNotServer" class="d-flex col-4">
        <div class="card flex-grow-1">
          <h4 class="card-header fw-bold px-3 py-2">Options</h4>
          <div class="card-body d-flex flex-column px-3 py-2">
            <div class="form-group mb-1">
              <p class="fw-bold mb-1">Queries</p>
              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsIncludeCreateDatabase`"
                  v-model="restoreOptions.include_create_database">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsIncludeCreateDatabase`">
                  Include 'Create Database' statement
                </label>
              </div>

              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsClean`" v-model="restoreOptions.clean">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsClean`">
                  Clean before restore
                </label>
              </div>

              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsSingleTransaction`"
                  v-model="restoreOptions.single_transaction">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsSingleTransaction`">
                  Single transaction
                </label>
              </div>

            </div>

            <div class="form-group mb-1">
              <p class="fw-bold mb-1">Disable</p>
              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsTrigger`"
                  v-model="restoreOptions.disable_trigger">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsTrigger`">
                  Trigger
                </label>
              </div>

              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsNoDataTableFail`"
                  v-model="restoreOptions.no_data_fail_table">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsNoDataTableFail`">
                  No data for failed tables
                </label>
              </div>
            </div>

            <div class="form-group mb-1">
              <p class="fw-bold mb-1">Miscellaneous</p>
              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsVerboseMessages`"
                  v-model="restoreOptions.verbose">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsVerboseMessages`">
                  Verbose messages
                </label>
              </div>

              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsSetSeessionAuthorization`"
                  v-model="restoreOptions.use_set_session_auth">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsSetSeessionAuthorization`">
                  Use SET SESSION AUTHORIZATION
                </label>
              </div>

              <div class="form-check form-switch">
                <input class="form-check-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsExitOnError`"
                  v-model="restoreOptions.exit_on_error">
                <label class="form-check-label" :for="`${restoreTabId}_restoreOptionsExitOnError`">
                  Exit on error
                </label>
              </div>
            </div>
          </div>
      </div>
      </div>    
    </div>  

    <div class="d-flex justify-content-between mt-3">
      <a data-testid="revert-settings-button" :class="['btn', 'btn-outline-secondary', 'mb-2', { 'disabled': !isOptionsChanged }]" @click="resetToDefault">Revert settings</a>
      <div class="btn-group" role="group">
        <a data-testid="preview-button" :class="['btn', 'btn-outline-primary', 'mb-2', { 'disabled': !restoreOptions.fileName }]"
          @click="previewCommand">Preview</a>
          <a data-testid="restore-button" :class="['btn', 'btn-success', 'mb-2', { 'disabled': !restoreOptions.fileName || restoreLocked }]"
          @click.prevent="createRestore">Restore</a>
      </div>
    </div>
  </form>
  <UtilityJobs @jobExit="handleJobExit" ref="jobs" />
</div>
</template>

<script>
import UtilityJobs from './UtilityJobs.vue';
import axios from 'axios'
import { showAlert } from '../notification_control';
import { settingsStore, fileManagerStore, tabsStore } from '../stores/stores_initializer';
import { truncateText } from "../utils";
import { handleError } from '../logging/utils';

export default {
  name: "RestoreTab",
  components: {
    UtilityJobs,
  },
  props: {
    workspaceId: String,
    tabId: String,
    databaseIndex: Number,
    treeNode: Object,
    restoreType: String
  },
  data() {
    return {
      roleNames: [],
      restoreOptionsDefault: {
        database: this.treeNode.data.database,
        type: this.restoreType,
        table: "",
        schema: "",
        function: "",
        trigger: "",
        role: "",
        fileName: "",
        pre_data: false,
        data: false,
        post_data: false,
        only_data: false,
        only_schema: false,
        dns_owner: false,
        dns_privilege: false,
        dns_tablespace: false,
        no_comments: false,
        include_create_database: false,
        clean: false,
        single_transaction: false,
        disable_trigger: false,
        no_data_fail_table: false,
        verbose: false,
        use_set_session_auth: false,
        exit_on_error: false,
        number_of_jobs: "",
        quiet: false,
        echo_queries: false,
        format: 'custom/tar',
        pigz: false,
        pigz_number_of_jobs: 'auto',
      },
      restoreOptions: {},
      restoreTabId: this.tabId,
      restoreLocked: false,
      lastJobId: 0
    }
  },
  computed: {
    isOptionsChanged() {
      return JSON.stringify(this.restoreOptionsDefault) !== JSON.stringify(this.restoreOptions)
    },
    isNotServer() {
      return this.restoreType !== 'server'
    },
    dialogType() {
      return this.restoreOptions.format === 'custom/tar' ? 'select_file' : 'select_folder'
    },
    pigzNumberOfJobs() {
      return ['auto', ...Array.from({length: 8}, (_, index) => index + 1)]
    },
    isDirectoryFormat() {
      return this.restoreOptions.format === 'directory'
    },
    numberOfJobs() {
      return Array.from({length: 8}, (_, index) => index + 1)
    },
    isWindowsOS() {
      return navigator.userAgent.indexOf("Win") != -1
    }
  },
  watch: {
    'restoreOptions.format'(newValue){
      if (newValue === 'directory') {
        this.restoreOptions.pigz = false
      }
    },
    // --create and --single-transaction are mutually exclusive
    'restoreOptions.single_transaction'(newValue){
      if (newValue === true) {
        this.restoreOptions.include_create_database = false
      }
    },
    'restoreOptions.include_create_database'(newValue){
      if (newValue === true) {
        this.restoreOptions.single_transaction = false
      }
    },
    restoreOptions: {
      handler() {
        this.restoreLocked = false;
      },
      deep: true
    },
  },
  mounted() {
    this.$nextTick(() => {
      if (this.treeNode.data.type === 'schema') {
        this.restoreOptionsDefault.schema = this.treeNode.title
      } else if (this.treeNode.data.type === 'table') {
        this.restoreOptionsDefault.table = `${this.treeNode.data.schema}.${this.treeNode.title}`
      } else if (this.treeNode.data.type === 'trigger') {
        this.restoreOptionsDefault.trigger = `${this.treeNode.data.schema}.${this.treeNode.title}`
      } else if (this.treeNode.data.type === 'function') {
        this.restoreOptionsDefault.function = `${this.treeNode.data.id}`
      }
      this.restoreOptions = { ...this.restoreOptionsDefault }
      this.getRoleNames()
    })

    fileManagerStore.$onAction(({name, store, after}) => {
      if (name === "changeFile" && this.tabId === tabsStore.selectedPrimaryTab.metaData.selectedTab.id) {
        after(() => {
          this.changeFilePath(store.file.path)
        })
      }
    })
  },
  methods: {
    getRoleNames() {
      axios.post("/get_roles_postgresql/", {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
      })
        .then((resp) => {
          resp.data.data.forEach(element => this.roleNames.push(element.name))
        })
        .catch((error) => {
          handleError(error);
        })
    },
    createRestore() {
      this.restoreLocked = true;
      axios.post("/restore/", {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
        data: this.restoreOptions
      })
        .then((resp) => {
          this.$refs.jobs.startJob(resp.data.job_id, resp.data.description)
          this.lastJobId = resp.data.job_id;
        })
        .catch((error) => {
          handleError(error);
          this.restoreLocked = false;
        })
    },
    onFile(e) {
      const [file] = e.target.files
      this.restoreOptions.fileName = file?.path
    },
    changeFilePath(filePath) {
      this.restoreOptions.fileName = filePath;
    },
    openFileManagerModal() {
      fileManagerStore.showModal(settingsStore.desktopMode, this.onFile, this.dialogType);
    },
    resetToDefault() {
      this.restoreOptions = { ...this.restoreOptionsDefault }
    },
    previewCommand() {
      axios.post("/restore/preview_command/", {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
        data: this.restoreOptions,
      })
        .then((resp) => {
          showAlert(resp.data.command.cmd)
        })
        .catch((error) => {
          handleError(error);
        })
    },
    handleJobExit(jobId) {
      if(jobId === this.lastJobId)
        this.restoreLocked = false;
    },
    truncateText
  }
}

</script>