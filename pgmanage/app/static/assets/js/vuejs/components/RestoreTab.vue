<template>
  <form>
    <div class="row">
      <div :class="(isNotServer) ? 'col-4':'col-12'" class="d-flex">
          <div class="card flex-grow-1">
            <h4 class="card-header font-weight-bold px-3 py-2">General</h4>
            <div class="card-body d-flex flex-column px-3 py-2">
              <div class="form-group mb-1">
                <label for="restoreFileName" class="font-weight-bold mb-1">FileName</label>
                <div class="input-group">
                  <div class="input-group-prepend">
                    <div class="input-group-text btn btn-secondary" @click="openFileManagerModal">Select
                      a file</div>
                  </div>
                  <input type="text" class="form-control" :value="restoreOptions.fileName"
                    placeholder="Select file or folder" disabled>
                </div>
              </div>

              <div  class="form-group mb-1">
                <label for="restoreFormat" class="font-weight-bold mb-1">Format</label>
                <select id="restoreFormat" class="form-control" v-model="restoreOptions.format">
                  <option value="custom/tar">Custom or tar</option>
                  <option value="directory">Directory</option>
                </select>
              </div>

              <div v-if="!isNotServer" class="form-group mb-1 mt-2">
                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" id="restoreOptionsEchoQueries"
                    v-model="restoreOptions.echo_queries">
                  <label class="custom-control-label" for="restoreOptionsEchoQueries">
                    Echo all queries
                  </label>
                </div>

                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" id="restoreOptionsQuiet" v-model="restoreOptions.quiet">
                  <label class="custom-control-label" for="restoreOptionsQuiet">Quiet mode</label>
                </div>
              </div>

              <div v-if="isNotServer" class="form-group mb-1">
                <label for="restoreNumberOfJobs" class="font-weight-bold mb-1">Number of jobs</label>
                <select id="restoreNumberOfJobs" class="form-control" v-model="restoreOptions.number_of_jobs">
                  <option value="" disabled>Select an item...</option>
                  <option v-for="number_of_jobs in numberOfJobs" :value="number_of_jobs" :key="number_of_jobs">{{ number_of_jobs }}</option>
                </select>
              </div>

              <div v-if="isNotServer" class="form-group mb-1">
                <label for="restoreRoleName" class="font-weight-bold mb-1">Restore as:</label>
                <select id="restoreRoleName" class="form-control" v-model="restoreOptions.role">
                  <option value="" disabled>Select an item...</option>
                  <option v-for="name in roleNames" :value="name" :key="name">{{ name }}</option>
                </select>
              </div>

              <div v-if="!isWindowsOS" class="form-group mb-1">
                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${restoreTabId}_restoreOptionsPigz`" v-model="restoreOptions.pigz" :disabled="isDirectoryFormat">
                  <label class="custom-control-label" :for="`${restoreTabId}_restoreOptionsPigz`">
                    Decompress with Pigz
                  </label>
                </div>
              </div>
              
                <div class="form-group" :class="(restoreOptions.pigz) ? 'collapse show':'collapse'">
                  <label for="restorePigzNumberOfJobs" class="font-weight-bold mb-1">Number of jobs</label>
                  <select id="restorePigzNumberOfJobs" class="form-control" v-model="restoreOptions.pigz_number_of_jobs">
                    <option v-for="number_of_jobs in pigzNumberOfJobs" :value="number_of_jobs" :key="number_of_jobs">{{ number_of_jobs }}</option>
                  </select>
                </div>

            </div>
          </div>
      </div>

      <div v-if="isNotServer" class="d-flex col-4">
        <div class="card flex-grow-1">
          <h4 class="card-header font-weight-bold px-3 py-2">Data/Objects</h4>
          <div class="card-body d-flex flex-column px-3 py-2">
            <div class="form-group mb-1">
              <p class="font-weight-bold mb-1">Sections</p>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsPreData"
                  v-model="restoreOptions.pre_data" :disabled="restoreOptions.only_data || restoreOptions.only_schema">
                <label class="custom-control-label" for="restoreOptionsPreData">
                  Pre-data
                </label>
              </div>

              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsData" v-model="restoreOptions.data"
                  :disabled="restoreOptions.only_data || restoreOptions.only_schema">
                <label class="custom-control-label" for="restoreOptionsData">Data</label>
              </div>

              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsPostData"
                  v-model="restoreOptions.post_data" :disabled="restoreOptions.only_data || restoreOptions.only_schema">
                <label class="custom-control-label" for="restoreOptionsPostData">
                  Post-data
                </label>
              </div>
            </div>

            <div class="form-group mb-1">
              <p class="font-weight-bold mb-1">Type of objects</p>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsOnlyData"
                  v-model="restoreOptions.only_data"
                  :disabled="restoreOptions.pre_data || restoreOptions.data || restoreOptions.post_data">
                <label class="custom-control-label" for="restoreOptionsOnlyData">
                  Only data
                </label>
              </div>

              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsOnlySchema"
                  v-model="restoreOptions.only_schema"
                  :disabled="restoreOptions.pre_data || restoreOptions.data || restoreOptions.post_data">
                <label class="custom-control-label" for="restoreOptionsOnlySchema">
                  Only schema
                </label>
              </div>
            </div>
    
            <div class="form-group mb-1">
              <p class="font-weight-bold mb-1">Do not save</p>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsOwner" v-model="restoreOptions.dns_owner">
                <label class="custom-control-label" for="restoreOptionsOwner">
                  Owner
                </label>
              </div>

              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsPrivilege"
                  v-model="restoreOptions.dns_privilege">
                <label class="custom-control-label" for="restoreOptionsPrivilege">
                  Privilege
                </label>
              </div>

              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsTablespace"
                  v-model="restoreOptions.dns_tablespace">
                <label class="custom-control-label" for="restoreOptionsTablespace">
                  Tablespace
                </label>
              </div>

              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsComments"
                  v-model="restoreOptions.no_comments">
                <label class="custom-control-label" for="restoreOptionsComments">
                  Comments
                </label>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div v-if="isNotServer" class="d-flex col-4">
        <div class="card flex-grow-1">
          <h4 class="card-header font-weight-bold px-3 py-2">Options</h4>
          <div class="card-body d-flex flex-column px-3 py-2">
            <div class="form-group mb-1">
              <p class="font-weight-bold mb-1">Queries</p>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsIncludeCreateDatabase"
                  v-model="restoreOptions.include_create_database">
                <label class="custom-control-label" for="restoreOptionsIncludeCreateDatabase">
                  Include 'Create Database' statement
                </label>
              </div>

              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsClean" v-model="restoreOptions.clean">
                <label class="custom-control-label" for="restoreOptionsClean">
                  Clean before restore
                </label>
              </div>

              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsSingleTransaction"
                  v-model="restoreOptions.single_transaction">
                <label class="custom-control-label" for="restoreOptionsSingleTransaction">
                  Single transaction
                </label>
              </div>

            </div>

            <div class="form-group mb-1">
              <p class="font-weight-bold mb-1">Disable</p>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsTrigger"
                  v-model="restoreOptions.disable_trigger">
                <label class="custom-control-label" for="restoreOptionsTrigger">
                  Trigger
                </label>
              </div>

              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsNoDataTableFail"
                  v-model="restoreOptions.no_data_fail_table">
                <label class="custom-control-label" for="restoreOptionsNoDataTableFail">
                  No data for failed tables
                </label>
              </div>
            </div>

            <div class="form-group mb-1">
              <p class="font-weight-bold mb-1">Miscellaneous</p>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsVerboseMessages"
                  v-model="restoreOptions.verbose">
                <label class="custom-control-label" for="restoreOptionsVerboseMessages">
                  Verbose messages
                </label>
              </div>

              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsSetSeessionAuthorization"
                  v-model="restoreOptions.use_set_session_auth">
                <label class="custom-control-label" for="restoreOptionsSetSeessionAuthorization">
                  Use SET SESSION AUTHORIZATION
                </label>
              </div>

              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" id="restoreOptionsExitOnError"
                  v-model="restoreOptions.exit_on_error">
                <label class="custom-control-label" for="restoreOptionsExitOnError">
                  Exit on error
                </label>
              </div>
            </div>
          </div>
      </div>
      </div>    
    </div>  

    <div class="d-flex justify-content-between mt-3">
      <a :class="['btn', 'btn-outline-secondary', 'mb-2', { 'disabled': !isOptionsChanged }]" @click="resetToDefault">Revert settings</a>
      <div class="btn-group" role="group">
        <a :class="['btn', 'btn-outline-primary', 'mb-2', { 'disabled': !restoreOptions.fileName }]"
          @click="previewCommand">Preview</a>
          <a :class="['btn', 'btn-success', 'mb-2', { 'disabled': !restoreOptions.fileName }]"
          @click.prevent="createRestore">Restore</a>
      </div>
    </div>
  </form>
  <UtilityJobs ref="jobs" />
  <FileManager ref="fileManager" @change-file="changeFilePath" />
</template>

<script>
import UtilityJobs from './UtilityJobs.vue';
import FileManager from './FileManager.vue';

export default {
  name: "RestoreTab",
  components: {
    FileManager,
    UtilityJobs
  },
  props: {
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
      desktopMode: window.gv_desktopMode,
      restoreTabId: window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.id
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
    }
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
  },
  methods: {
    getRoleNames() {
      axios.post("/get_roles_postgresql/", {
        database_index: window.v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
        tab_id: window.v_connTabControl.selectedTab.id,
      })
        .then((resp) => {
          resp.data.data.forEach(element => this.roleNames.push(element.name))
        })
        .catch((error) => {
          console.log(error)
        })
    },
    createRestore() {
      axios.post("/restore/", {
        database_index: window.v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
        tab_id: window.v_connTabControl.selectedTab.id,
        data: this.restoreOptions
      })
        .then((resp) => {
          console.log(resp)
          this.$refs.jobs.startJob(resp.data.job_id, resp.data.description)
        })
        .catch((error) => {
          console.log(error)
          showError(error.response.data.data)
        })
    },
    onFile(e) {
      const [file] = e.target.files
      this.restoreOptions.fileName = file?.path
    },
    changeFilePath(event) {
      this.restoreOptions.fileName = event.filePath
    },
    openFileManagerModal() {
      this.$refs.fileManager.show(this.desktopMode, this.onFile, this.dialogType)
    },
    resetToDefault() {
      this.restoreOptions = { ...this.restoreOptionsDefault }
    },
    previewCommand() {
      axios.post("/restore/preview_command/", {
        database_index: window.v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
        tab_id: window.v_connTabControl.selectedTab.id,
        data: this.restoreOptions,
      })
        .then((resp) => {
          console.log(resp)
          showAlert(resp.data.command.cmd)
        })
        .catch((error) => {
          console.log(error)
          showError(error.response.data.data);

        })
    }
  }
}

</script>