<template>
  <div>
  <splitpanes class="default-theme query-body" horizontal>
    <pane size="30">
      <QueryEditor ref="editor" class="h-100 mr-2"
        :read-only="readOnlyEditor"
        :tab-id="tabId"
        :database-index="databaseIndex"
        :database-name="databaseName"
        tab-mode="query"
        :dialect="dialect" @editor-change="updateEditorContent" @run-selection="queryRunOrExplain(false)" :autocomplete="autocomplete"/>
    </pane>

    <pane size="70" class="px-2 border-top">
      <!-- ACTION BUTTONS-->
      <div class="py-2 pr-1 d-flex align-items-center">
        <div class="tab-actions d-flex w-100">
          <button :id="`bt_start_${tabId}`" class="btn btn-sm btn-primary btn-run" title="Run" @click="queryRunOrExplain()" :disabled="executingState">
            <i class="fas fa-play fa-light"></i>
          </button>

          <button :id="`bt_start_${tabId}`" class="btn btn-sm btn-primary btn-run" title="Run Selection" @click="queryRunOrExplain(false)" :disabled="executingState">
            [
            <i class="fas fa-play fa-light"></i>
            ]
          </button>

          <button :id="`bt_indent_${tabId}`" class="btn btn-sm btn-secondary" title="Indent SQL" @click="indentSQL()">
            <i class="fas fa-indent fa-light"></i>
          </button>

          <button :id="`bt_indent_${tabId}`" class="btn btn-sm btn-secondary" title="Find/Replace" @click="showFindReplace()">
            <i class="fas fa-magnifying-glass fa-light"></i>
          </button>

          <button :class="`bt_history_${tabId}`" class="btn btn-sm btn-secondary" title="Command History"
            @click="showCommandsHistory()">
            <i class="fas fa-clock-rotate-left fa-light"></i>
          </button>

          <button :id="`bt_open_file_${tabId}`" class="btn btn-sm btn-secondary ml-2" title="Load from File" @click="openFileManagerModal">
            <i class="fas fa-folder-open fa-light"></i>
          </button>

          <button :disabled="fileSaveDisabled" :id="`bt_save_file_${tabId}`" class="btn btn-sm btn-secondary mr-2 " title="Save to File" @click="saveFile">
            <i class="fas fa-download fa-light"></i>
          </button>

          <template v-if="postgresqlDialect">
            <!-- EXPLAIN ANALYZE BUTTONS-->
            <div class="btn-group ml-2 mr-2">
              <button :id="`bt_explain_${tabId}`" class="btn btn-sm btn-secondary" title="Explain" @click="runExplain(0)"
                :disabled="!enableExplainButtons">
                <i class="fas fa-chart-simple fa-light"></i>
              </button>

              <button :id="`bt_analyze_${tabId}`" class="btn btn-sm btn-secondary" title="Explain Analyze"
                @click="runExplain(1)" :disabled="!enableExplainButtons">
                <i class="fas fa-magnifying-glass-chart fa-light"></i>
              </button>
            </div>

            <!-- AUTOCOMMIt-->
            <div class="omnidb__form-check form-check form-check-inline">
              <input :id="`check_autocommit_${tabId}`" class="form-check-input" type="checkbox" v-model="autocommit" />
              <label class="form-check-label custom-checkbox query_info"
                :for="`check_autocommit_${tabId}`">Autocommit</label>
            </div>

            <TabStatusIndicator :tab-status="tabStatus" />
          </template>

          <!-- Query ACTIONS BUTTONS-->
          <button :id="`bt_fetch_more_${tabId}`" class="btn btn-sm btn-secondary" title="Run" v-if="showFetchButtons"
            @click="querySQL(queryModes.FETCH_MORE)">
            Fetch More
          </button>

          <button :id="`bt_fetch_all_${tabId}`" class="btn btn-sm btn-secondary" title="Run" v-if="showFetchButtons"
            @click="querySQL(queryModes.FETCH_ALL)">
            Fetch all
          </button>

          <template v-if="activeTransaction">
            <button :id="`bt_commit_${tabId}`" class="btn btn-sm btn-primary" title="Run"
              @click="querySQL(queryModes.COMMIT)">
              Commit
            </button>

            <button :id="`bt_rollback_${tabId}`" class="btn btn-sm btn-secondary" title="Run"
              @click="querySQL(queryModes.ROLLBACK)">
              Rollback
            </button>
          </template>

          <CancelButton v-if="executingState && longQuery" :tab-id="tabId" :conn-id="connId"
            @cancelled="cancelSQLTab()" />

          <!-- QUERY INFO DIV-->
          <div :id="`div_query_info_${tabId}`" class="">
            <p class="m-0 h6" v-if="cancelled">
              <b>Cancelled</b>
            </p>
            <p v-else-if="queryStartTime && queryDuration" class="h6 m-0  mr-2">
              <b>Start time:</b> {{ queryStartTime.format() }}<br/>
              <b>Duration:</b> {{ queryDuration }}
            </p>
            <p v-else-if="queryStartTime" class=" m-0 h6">
              <b>Start time:</b> {{ queryStartTime.format() }}
            </p>
          </div>

          <!-- EXPORT BUTTON with SELECT OPTIONS -->
          <button class="btn btn-sm btn-primary ml-auto" title="Export Data" @click="exportData()">
            <i class="fas fa-download fa-light"></i>
          </button>

          <div class="form-group mb-0">
            <select v-model="exportType" :id="`sel_export_type${tabId}`" class="form-control" style="width: 80px;">
              <option v-for="(name, value) in exportTypes" :value="value">
                {{ name }}
              </option>
            </select>
          </div>
        </div>
      </div>
      <QueryResultTabs ref="queryResults" :conn-id="connId" :tab-id="tabId" :editor-content="editorContent"
        :dialect="dialect" :tab-status="tabStatus" @enable-explain-buttons="toggleExplainButtons"
        @run-explain="runExplain(0)" @show-fetch-buttons="toggleFetchButtons" />
    </pane>
  </splitpanes>

  <CommandsHistoryModal ref="commandsHistory" :tab-id="tabId" :database-index="databaseIndex" tab-type="Query" :commands-modal-visible="commandsModalVisible" @modal-hide="commandsModalVisible=false"/>
  <FileManager ref="fileManager"/>
</div>
</template>

<script>
import { Splitpanes, Pane } from "splitpanes";
import { createMessageModal, showToast } from "../notification_control";
import moment from "moment";
import { createRequest } from "../long_polling";
import { queryModes, requestState, tabStatusMap, queryRequestCodes } from "../constants";
import CancelButton from "./CancelSQLButton.vue";
import QueryEditor from "./QueryEditor.vue";
import { emitter } from "../emitter";
import CommandsHistoryModal from "./CommandsHistoryModal.vue";
import TabStatusIndicator from "./TabStatusIndicator.vue";
import QueryResultTabs from "./QueryResultTabs.vue";
import FileManager from "./FileManager.vue";
import FileInputChangeMixin from '../mixins/file_input_mixin'
import { tabsStore, connectionsStore } from "../stores/stores_initializer";

export default {
  name: "QueryTab",
  components: {
    Splitpanes,
    Pane,
    CancelButton,
    QueryEditor,
    CommandsHistoryModal,
    TabStatusIndicator,
    QueryResultTabs,
    FileManager
  },
  mixins: [FileInputChangeMixin],
  props: {
    connId: String,
    tabId: String,
    databaseIndex: Number,
    databaseName: String,
    dialect: String,
    initTabDatabaseId: Number,
    initialQuery: String,
  },
  data() {
    return {
      queryState: requestState.Idle,
      tabStatus: tabStatusMap.NOT_CONNECTED,
      autocommit: true,
      queryStartTime: "",
      queryDuration: "",
      data: "",
      context: "",
      tempData: [],
      tabDatabaseId: this.initTabDatabaseId,
      cancelled: false,
      enableExplainButtons: false,
      exportTypes: {
        csv: "CSV",
        "csv-no_headers": "CSV(no headers)",
        xlsx: "XLSX",
        "xlsx-no_headers": "XLSX(no headers)",
      },
      exportType: "csv",
      showFetchButtons: false,
      readOnlyEditor: false,
      editorContent: "",
      longQuery: false,
      commandsModalVisible: false,
      lastQuery: null,
      queryInterval: null,
    };
  },
  computed: {
    postgresqlDialect() {
      return this.dialect === "postgresql";
    },
    idleState() {
      return this.queryState === requestState.Idle;
    },
    executingState() {
      return this.queryState === requestState.Executing;
    },
    activeTransaction() {
      return [
        tabStatusMap.IDLE_IN_TRANSACTION,
        tabStatusMap.IDLE_IN_TRANSACTION_ABORTED,
      ].includes(this.tabStatus);
    },
    queryModes() {
      return queryModes;
    },
    autocomplete() {
      return connectionsStore.getConnection(this.databaseIndex).autocomplete
    },
    fileSaveDisabled() {
      return !this.editorContent;
    }
  },
  mounted() {
    this.setupEvents();

    if (!!this.initialQuery) {
      emitter.emit(`${this.tabId}_copy_to_editor`, this.initialQuery);
    }
  },
  unmounted() {
    this.clearEvents();
  },
  methods: {
    getQueryEditorValue(raw_query) {
      return this.$refs.editor.getQueryEditorValue(raw_query);
    },
    querySQL(
      mode,
      cmd_type = null,
      all_data = false,
      query = this.getQueryEditorValue(true),
      log_query = true,
      save_query = this.editorContent,
      clear_data = false
    ) {
      if (!this.idleState) {
        showToast("info", "Tab with activity in progress.");
      } else {
        if (!query) {
          showToast("info", "Please provide a string.");
        } else {
          let tab = tabsStore.getSelectedSecondaryTab(this.connId);
          this.queryDuration = "";
          this.cancelled = false;
          this.showFetchButtons = false;
          this.longQuery = false;
          this.tempData = [];
          this.lastQuery = query

          let message_data = {
            sql_cmd: query,
            mode: mode,
            autocommit: this.autocommit,
            v_db_index: this.databaseIndex,
            v_conn_tab_id: this.connId,
            v_tab_id: this.tabId,
            tab_db_id: this.tabDatabaseId,
            sql_save: save_query,
            database_name: this.databaseName,
            cmd_type: cmd_type,
            all_data: all_data,
            log_query: log_query,
            tab_title: tab.name,
          };

          this.readOnlyEditor = true;
          this.queryStartTime = moment();

          let context = {
            tab: tab,
            database_index: this.databaseIndex,
            acked: false,
            clear_data: clear_data,
            cmd_type: cmd_type,
            mode: mode,
            callback: this.querySQLReturn.bind(this),
            passwordSuccessCallback: this.passwordSuccessCallback.bind(this),
            passwordFailCalback: () => {
              emitter.emit(`${this.tabId}_cancel_query`);
            },
          };

          context.tab.metaData.context = context;

          createRequest(queryRequestCodes.Query, message_data, context);

          this.queryState = requestState.Executing;

          setTimeout(() => {
            this.longQuery = true;
          }, 1000);

          this.queryInterval = setInterval((function(){
            let diff = moment().diff(this.queryStartTime)
            this.queryDuration = moment.utc(diff).format('HH:mm:ss')
          }).bind(this), 1000)

          tab.metaData.isLoading = true;
          tab.metaData.isReady = false;

          this.tabStatus = tabStatusMap.RUNNING;
        }
      }
    },
    querySQLReturn(data, context) {
      clearInterval(this.queryInterval)
      this.queryInterval = null;
      if (!data.v_error) {
        this.tempData = this.tempData.concat(data.v_data.data)
      }

      //Update tab_db_id if not null in response
      if (data.v_data.inserted_id) {
        this.tabDatabaseId = data.v_data.inserted_id;
      }

      //If query wasn't canceled already

      if (!this.idleState && (data.v_data.last_block || data.v_data.file_name || data.v_error )) {
        data.v_data.data = this.tempData;
        this.readOnlyEditor = false;
        this.tabStatus = data.v_data.con_status;

        if (
          this.connId === tabsStore.selectedPrimaryTab.id &&
          this.tabId === tabsStore.selectedPrimaryTab.metaData.selectedTab.id
        ) {
          this.context = "";
          this.data = "";

          this.queryState = requestState.Idle;
          this.$refs.queryResults.renderResult(data, context);

          this.queryDuration = data.v_data.duration;

          context.tab.metaData.isReady = false;
          context.tab.metaData.isLoading = false;
        } else {
          this.queryState = requestState.Ready;
          this.data = data;
          this.context = context;

          context.tab.metaData.isReady = true
          context.tab.metaData.isLoading = false
        }
      }
    },
    runExplain(explainMode) {
      let command = this.getQueryEditorValue(true);

      if (command.trim() === "") {
        showToast("info", "Please provide a string.");
      } else {
        let should_prepend =
          command.trim().split(" ")[0].toUpperCase() !== "EXPLAIN";
        if (should_prepend) {
          if (explainMode === 0) {
            command = "explain " + command;
          } else if (explainMode === 1) {
            command = "explain (analyze, buffers) " + command;
          }
        }

        this.querySQL(
          this.queryModes.DATA_OPERATION,
          "explain",
          true,
          command,
          false
        );
      }
    },
    queryRunOrExplain(use_raw_query=true) {
      let query = this.getQueryEditorValue(use_raw_query)
      if (this.dialect === "postgresql") {
        let should_explain =
          query.trim().split(" ")[0].toUpperCase() === "EXPLAIN";
        if (should_explain) {
          return this.querySQL(
            this.queryModes.DATA_OPERATION,
            "explain",
            true,
            query,
            false
          );
        }
      }

      this.querySQL(this.queryModes.DATA_OPERATION, null, false, query=query)
    },
    exportData() {
      let cmd_type = `export_${this.exportType}`;
      let query = this.lastQuery || this.getQueryEditorValue(true)
      this.querySQL(
        this.queryModes.DATA_OPERATION,
        cmd_type,
        true,
        query,
        true,
        query,
        true
      );
    },
    cancelSQLTab() {
      this.readOnlyEditor = false;

      this.queryState = requestState.Idle;
      this.tabStatus = tabStatusMap.NOT_CONNECTED;

      this.cancelled = true;
    },
    indentSQL() {
      emitter.emit(`${this.tabId}_indent_sql`);
    },
    showFindReplace() {
      emitter.emit(`${this.tabId}_find_replace`);
    },
    updateEditorContent(newContent) {
      this.editorContent = newContent;
    },
    toggleFetchButtons(newValue) {
      this.showFetchButtons = newValue;
    },
    toggleExplainButtons() {
      this.enableExplainButtons = !this.enableExplainButtons;
    },
    passwordSuccessCallback(context) {
      emitter.emit(`${this.tabId}_cancel_query`);

      this.querySQL(
        context.mode,
        context.cmd_type,
        context.all_data,
        context.query,
        context.log_query,
        context.save_query,
        context.clear_data
      );
    },
    setupEvents() {
      emitter.on(`${this.tabId}_check_query_status`, () => {
        this.$refs.editor.focus();
        if (this.queryState === requestState.Ready) {
          this.context.tab.metaData.isReady = false;
          this.context.tab.metaData.isLoading = false;
          this.queryState = requestState.Idle;

          this.$refs.queryResults.renderResult(this.data, this.context);
        }
      });

      emitter.on(`${this.tabId}_run_query`, (sql_command) => {
        if (sql_command) {
          emitter.emit(`${this.tabId}_copy_to_editor`, sql_command);
        }
        this.queryRunOrExplain();
      });

      emitter.on(`${this.tabId}_run_selection`, (sql_command) => {
        this.queryRunOrExplain(false)
      });

      emitter.on(`${this.tabId}_run_explain`, () => {
        this.runExplain(0);
      });

      emitter.on(`${this.tabId}_run_explain_analyze`, () => {
        this.runExplain(1);
      });
    },
    clearEvents() {
      emitter.all.delete(`${this.tabId}_check_query_status`);
      emitter.all.delete(`${this.tabId}_run_explain`);
      emitter.all.delete(`${this.tabId}_run_explain_analyze`);
      emitter.all.delete(`${this.tabId}_run_query`);
    },
    showCommandsHistory() {
      this.commandsModalVisible = true
    },
    openFileManagerModal() {
      if (!!this.editorContent) {
        createMessageModal(
          "Are you sure you wish to discard the current changes?",
          () => {
            this.$refs.fileManager.show(true, this.handleFileInputChange);
          },
          null
        );
      } else {
        this.$refs.fileManager.show(true, this.handleFileInputChange);
      }
    },
    async saveFile() {
      const today = new Date()
      const nameSuffix = `${today.getHours()}${today.getMinutes()}`
      let tab = tabsStore.getSelectedSecondaryTab(this.connId);
      const fileName = tab.metaData?.editingFile ? tab.name : `pgmanage-query-${nameSuffix}.sql`

      const file = new File([this.editorContent], fileName, {
        type: "application/sql",
      })

      if(window.showSaveFilePicker) {
        try {
          const handle = await showSaveFilePicker(
            { suggestedName: file.name,
              types: [{
                description: 'SQL Script',
                accept: {
                  'application/sql': ['.sql'],
                }
              }],
            }
          )

          const writable = await handle.createWritable()
          await writable.write(file)
          writable.close()
        } catch(e) {
          console.log(e)
        }

      } else {
        const downloadLink = document.createElement("a")
        downloadLink.href = URL.createObjectURL(file)
        downloadLink.download = file.name
        downloadLink.click();
        setTimeout(() => URL.revokeObjectURL(downloadLink.href), 60000 )
      }
    },
  },
};
</script>

<style scoped>
.query-body {
  height: calc(100vh - 60px);
  padding-top: 16px;
}

.tab-actions {
  align-items: center;
  display: flex;
  justify-content: flex-start;
  min-height: 35px;
}

.tab-actions>button {
  margin-right: 5px;
}

.splitpanes .splitpanes__pane {
  transition: none;
}

.btn-run {
  padding-left: 2px;
  padding-right: 2px;
  min-width: 2rem;
}

.btn-run i {
    margin-left: -3px;
    margin-right: -3px;
}
</style>
