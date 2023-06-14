<template>
  <form>
      <div class="row">
        <div :class="'col-4'" class="d-flex">
          <div class="card flex-grow-1">
            <h4 class="card-header font-weight-bold px-3 py-2">General</h4>
            <div class="card-body d-flex flex-column px-3 py-2">
              <div class="form-group mb-1">
                <label for="backupFileName" class="font-weight-bold mb-1">FileName</label>
                  <div class="input-group">
                    <div class="input-group-prepend">
                      <div class="input-group-text btn btn-secondary" @click="openFileManagerModal">Select
                        a file</div>
                    </div>
                    <input type="text" class="form-control" :value="backupOptions.fileName"
                      placeholder="Select file or folder" disabled>
                  </div>
              </div>
              <div v-if="isObjectsType" class="form-group mb-1">
                <label for="backupFormat" class="font-weight-bold mb-1">Format</label>
                <select id="backupFormat" class="form-control" v-model="backupOptions.format">
                  <option v-for="(value, key) in formats" :value="key" :key="key">{{ value }}</option>
                </select>
              </div>
              <div v-if="isObjectsType" class="form-group mb-1">
                <label for="backupCompressionRatio" class="font-weight-bold mb-1">Compression ratio</label>
                <select id="backupCompressionRatio" class="form-control" v-model="backupOptions.compression_ratio" :disabled="isTarFormat">
                  <option value="" disabled>Select an item...</option>
                  <option v-for="compress_ratio in compressionRatioValues" :value="compress_ratio" :key="compress_ratio">{{ compress_ratio }}</option>
                </select>
              </div>
              <div class="form-group mb-1">
                <label for="backupEncoding" class="font-weight-bold mb-1">Encoding</label>
                <select id="backupEncoding" class="form-control" v-model="backupOptions.encoding">
                  <option value="">Use database encoding</option>
                  <option v-for="encoding in encodingList" :key="encoding" :value="encoding">{{ encoding }}</option>
                </select>
              </div>
              <div v-if="isObjectsType" class="form-group mb-1">
                <label for="backupNumberOfJobs" class="font-weight-bold mb-1">Number of jobs</label>
                <input type="text" class="form-control" id="backupNumberOfJobs"
                  v-model.number="backupOptions.number_of_jobs" :disabled="backupOptions.format != 'directory'">
              </div>
              <div class="form-group mb-1">
                <label for="backupRoleName" class="font-weight-bold mb-1">Backup as:</label>
                <select id="backupRoleName" class="form-control" v-model="backupOptions.role">
                  <option value="" disabled>Select an item...</option>
                  <option v-for="name in roleNames" :value="name" :key="name">{{ name }}</option>
                </select>
              </div>
              <div v-if="isServerType" class="d-flex font-italic mt-auto muted-text">
                <i class="fa-solid fa-circle-info mr-1"></i>
                <p>The backup will be in PLAIN format.</p>
              </div>
            </div>
          </div>
        </div>

        <div class="col-4 d-flex">
          <div class="card flex-grow-1">
            <h4 class="card-header font-weight-bold px-3 py-2">Data/Objects</h4>
            <div class="card-body px-3 py-2">
              <div v-if="isObjectsType" class="form-group mb-1">
                <p class="font-weight-bold mb-2">Sections</p>
                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsPreData`"
                    v-model="backupOptions.pre_data" :disabled="isOnlyDataOrSchemaSelected">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsPreData`">
                    Pre-data
                  </label>
                </div>
                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsData`" v-model="backupOptions.data"
                    :disabled="isOnlyDataOrSchemaSelected">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsData`">
                    Data
                  </label>
                </div>
                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsPostData`"
                    v-model="backupOptions.post_data" :disabled="isOnlyDataOrSchemaSelected">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsPostData`">
                    Post-data
                  </label>
                </div>
              </div>

              <div class="form-group mb-1">
                <p class="font-weight-bold mb-1">Type of objects</p>
                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsOnlyData`"
                    v-model="backupOptions.only_data"
                    :disabled="backupOptions.pre_data || backupOptions.data || backupOptions.post_data || backupOptions.include_drop_commands">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsOnlyData`">
                    Only data
                  </label>
                </div>
                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsOnlySchema`"
                    v-model="backupOptions.only_schema"
                    :disabled="backupOptions.pre_data || backupOptions.data || backupOptions.post_data">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsOnlySchema`">
                    Only schema
                  </label>
                </div>
                <div v-if="isObjectsType" class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsBlobs`" v-model="backupOptions.blobs">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsBlobs`">
                    Blobs
                  </label>
                </div>
                <div v-if="isServerType" class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsOnlyRoles`"
                    v-model="backupOptions.only_roles" 
                    :disabled="backupOptions.only_tablespaces || backupOptions.only_globals">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsOnlyRoles`">
                    Only roles
                  </label>
                </div>
                <div v-if="isServerType" class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsOnlyTablespaces`"
                    v-model="backupOptions.only_tablespaces"
                    :disabled="backupOptions.only_roles || backupOptions.only_globals">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsOnlyTablespaces`">
                    Only tablespaces
                  </label>
                </div>
                <div v-if="isServerType" class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsOnlyGlobals`"
                  v-model="backupOptions.only_globals"
                  :disabled="backupOptions.only_roles || backupOptions.only_tablespaces">
                <label class="custom-control-label" :for="`${backupTabId}_backupOptionsOnlyGlobals`">
                  Only global objects
                </label>
              </div>
              </div>

              <div class="form-group mb-0">
                <p class="font-weight-bold mb-2">Do not save</p>
                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsOwner`" v-model="backupOptions.owner">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsOwner`">
                    Owner
                  </label>
                </div>
                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsPrivilege`"
                    v-model="backupOptions.privilege">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsPrivilege`">
                    Privilege
                  </label>
                </div>
                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsTablespace`"
                    v-model="backupOptions.tablespace">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsTablespace`">
                    Tablespace
                  </label>
                </div>
                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsUnloggedTableData`"
                    v-model="backupOptions.unlogged_tbl_data">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsUnloggedTableData`">
                    Unlogged table data
                  </label>
                </div>
                <div class="custom-control custom-switch">
                  <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsComments`"
                    v-model="backupOptions.comments">
                  <label class="custom-control-label" :for="`${backupTabId}_backupOptionsComments`">
                    Comments
                  </label>
                </div>
              </div>
            </div>
          </div>
        </div>
        
        <div class="col-4 d-flex">
          <div class="card flex-grow-1">
            <h4 class="card-header font-weight-bold px-3 py-2">Options</h4>
            <div class="card-body px-2 px-3 py-2">
            <div class="form-group mb-1">
              <p class="font-weight-bold mb-1">Queries</p>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsColumnInserts`"
                  v-model="backupOptions.use_column_inserts">
                <label class="custom-control-label" :for="`${backupTabId}_backupOptionsColumnInserts`">
                  Use Column Inserts
                </label>
              </div>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsInsertCommands`"
                  v-model="backupOptions.use_insert_commands">
                <label class="custom-control-label" :for="`${backupTabId}_backupOptionsInsertCommands`">
                  Use Insert Commands
                </label>
              </div>
              <div v-if="isObjectsType" class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsCreateDbStatement`"
                  v-model="backupOptions.include_create_database">
                <label class="custom-control-label" :for="`${backupTabId}_backupOptionsCreateDbStatement`">
                  Include CREATE DATABASE statement
                </label>
              </div>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsDropCommands`"
                  v-model="backupOptions.include_drop_commands" :disabled="backupOptions.only_data">
                <label class="custom-control-label" :for="`${backupTabId}_backupOptionsDropCommands`">
                  Include DROP commands
                </label>
              </div>
              <div v-if="isObjectsType" class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsPartitionRoot`"
                  v-model="backupOptions.load_via_partition_root">
                <label class="custom-control-label" :for="`${backupTabId}_backupOptionsPartitionRoot`">
                  Load Via Partition Root
                </label>
              </div>
            </div>

            <div class="form-group mb-1">
              <p class="font-weight-bold mb-1">Disable</p>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsTrigger`"
                  title="disabled on object backup" v-model="backupOptions.disable_trigger"
                  :disabled="!backupOptions.only_data">
                <label class="custom-control-label" :for="`${backupTabId}_backupOptionsTrigger`">
                  Trigger
                </label>
              </div>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsQuoting`"
                  v-model="backupOptions.disable_quoting">
                <label class="custom-control-label" :for="`${backupTabId}_backupOptionsQuoting`">
                  $ quoting
                </label>
              </div>
            </div>
            
            <div class="form-group mb-0">
              <p class="font-weight-bold mb-1">Miscellaneous</p>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsVerboseMessages`"
                  v-model="backupOptions.verbose">
                <label class="custom-control-label" :for="`${backupTabId}_backupOptionsVerboseMessages`">
                  Verbose messages
                </label>
              </div>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsDoubleQuote`"
                  v-model="backupOptions.dqoute">
                <label class="custom-control-label" :for="`${backupTabId}_backupOptionsDoubleQuote`">
                  Force double quote on identifiers
                </label>
              </div>
              <div class="custom-control custom-switch">
                <input class="custom-control-input" type="checkbox" :id="`${backupTabId}_backupOptionsSetSeessionAuthorization`"
                  v-model="backupOptions.use_set_session_auth">
                <label class="custom-control-label" :for="`${backupTabId}_backupOptionsSetSeessionAuthorization`">
                  Use SET SESSION AUTHORIZATION
                </label>
              </div>
            </div>
            </div>
          </div>
        </div>
      </div>

      <div class="d-flex justify-content-between mt-3">
        <a :class="['btn', 'btn-outline-secondary', 'mb-2', { 'disabled': !isOptionsChanged }]" 
            @click="resetToDefault">Revert settings</a>
          <div class="btn-group">
            <a :class="['btn', 'btn-outline-primary', 'mb-2', { 'disabled': !backupOptions.fileName}]"
                @click="previewCommand">Preview</a>
            <a :class="['btn', 'btn-success', 'mb-2', 'ml-0', { 'disabled': !backupOptions.fileName }]"
                @click.prevent="saveBackup">Backup</a>
          </div>
      </div>
  </form>
  <UtilityJobs ref="jobs" />

  <FileManager ref="fileManager" @change-file="changeFilePath" />
</template>
<script>
import UtilityJobs from "./UtilityJobs.vue";
import FileManager from "./FileManager.vue";

export default {
  name: "BackupTab",
  components: {
    UtilityJobs,
    FileManager
  },
  props: {
    backupType: String,
    treeNode: Object
  },
  data() {
    return {
      roleNames: [],
      formats: {
        custom: 'Custom',
        tar: 'Tar',
        plain: 'Plain',
        directory: 'Directory'
      },
      encodingList: [
        'BIG5', 'EUC_CN', 'EUC_JIS_2004', 'EUC_JP', 'EUC_KR', 'EUC_TW', 'GB18030',
        'GBK', 'ISO_8859_5', 'ISO_8859_6', 'ISO_8859_7', 'ISO_8859_8', 'JOHAB',
        'KOI18R', 'KOI8U', 'LATIN1', 'LATIN10', 'LATIN2', 'LATIN3', 'LATIN4', 'LATIN5',
        'LATIN6', 'LATIN7', 'LATIN8', 'LATIN9', 'MULE_INTERNAL', 'SHIFT_JIS_2004', 'SJIS', 'SQL_ASCII',
        'UHC', 'UTF8', 'WIN1250', 'WIN1251', 'WIN1252', 'WIN1253', 'WIN1254', 'WIN1255', 'WIN1256',
        'WIN1257', 'WIN1258', 'WIN866', 'WIN874'
      ],
      backupOptionsDefault: {
        database: this.treeNode.tag.database,
        tables: [],
        schemas: [],
        role: "",
        format: this.backupType === 'objects' ? 'custom' : 'plain',
        encoding: "",
        fileName: "",
        pre_data: false,
        data: false,
        post_data: false,
        only_data: false,
        only_schema: false,
        only_roles: false,
        only_tablespaces: false,
        only_globals: false,
        blobs: this.backupType === 'objects' ? true : false,
        owner: false,
        privilege: false,
        tablespace: false,
        unlogged_tbl_data: false,
        comments: false,
        use_column_inserts: false,
        use_insert_commands: false,
        include_create_database: false,
        include_drop_commands: false,
        load_via_partition_root: false,
        disable_trigger: false,
        disable_quoting: false,
        verbose: true,
        dqoute: false,
        use_set_session_auth: false,
        number_of_jobs: "",
        compression_ratio: "",
      },
      backupOptions: {},
      desktopMode: window.gv_desktopMode,
      backupTabId: window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.id
    }
  },
  computed: {
    isOptionsChanged() {
      return JSON.stringify(this.backupOptionsDefault) !== JSON.stringify(this.backupOptions)
    },
    compressionRatioValues() {
      return [...Array(10).keys()]
    },
    isTarFormat() {
      return this.backupOptions.format === 'tar'
    },
    type() {
      return this.backupOptions.only_globals ? 'globals' : this.backupType
    },
    isServerType() {
      return this.backupType === 'server'
    },
    isObjectsType() {
      return this.backupType === 'objects'
    },
    isOnlyDataOrSchemaSelected() {
      return this.backupOptions.only_data || this.backupOptions.only_schema
    },
    dialogType() {
      return this.backupOptions.format === 'directory' ? 'select_folder' : 'create_file'
    }
  },
  mounted() {
    this.$nextTick(() => {
      if (this.treeNode.tag.type === 'schema') {
        this.backupOptionsDefault.schemas.push(this.treeNode.text)
      } else if (this.treeNode.tag.type === 'table') {
        this.backupOptionsDefault.tables.push(`${this.treeNode.tag.schema}.${this.treeNode.text}`)
      }
      this.backupOptions = { ...this.backupOptionsDefault }
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
    saveBackup() {
      axios.post("/backup/", {
        database_index: window.v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
        tab_id: window.v_connTabControl.selectedTab.id,
        data: this.backupOptions,
        backup_type: this.type
      })
        .then((resp) => {
          console.log(resp)
          this.$refs.jobs.startJob(resp.data.job_id, resp.data.description)
        })
        .catch((error) => {
          console.log(error)
          showError(error.response.data.data);

        })
    },
    onFile(e) {
      const [file] = e.target.files
      this.backupOptions.fileName = file?.path
    },
    changeFilePath(event) {
      this.backupOptions.fileName = event.filePath
    },
    openFileManagerModal() {
      this.$refs.fileManager.show(this.desktopMode, this.onFile, this.dialogType)
    },
    resetToDefault() {
      this.backupOptions = { ...this.backupOptionsDefault }
    },
    previewCommand() {
      axios.post("/backup/preview_command/", {
        database_index: window.v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
        tab_id: window.v_connTabControl.selectedTab.id,
        data: this.backupOptions,
        backup_type: this.type
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