<template>
  <splitpanes class="default-theme console-body" horizontal @resized="onResize">
    <pane size="80">
      <div ref="console" :id="`txt_console_${tabId}`" class="omnidb__txt-console mr-2 h-100"></div>
    </pane>

    <pane size="20" class="pl-2 border-top">
      <div class="tab-actions py-2 d-flex align-items-center">
        <button class="btn btn-sm btn-primary" title="Run" @click="consoleSQL(false)">
          <i class="fas fa-play fa-light"></i>
        </button>
        
        <button class="btn btn-sm btn-secondary" title="Open File" @click="openFileManagerModal">
            <i class="fas fa-folder-open fa-light"></i>
        </button>

        <button class="btn btn-sm btn-secondary" title="Indent SQL" @click="indentSQL()">
          <i class="fas fa-indent fa-ligth"></i>
        </button>

        <button class="btn btn-sm btn-secondary" title="Clear Console" @click="clearConsole()">
          <i class="fas fa-broom fa-ligth"></i>
        </button>

        <button class="btn btn-sm btn-secondary" title="Command History" @click="showCommandsHistory()">
          <i class="fas fa-clock-rotate-left fa-light"></i>
        </button>

        <template v-if="postgresqlDialect">
          <div class="omnidb__form-check form-check form-check-inline">
            <input :id="`check_autocommit_${tabId}`" class="form-check-input" type="checkbox" v-model="autocommit" />
            <label class="form-check-label" :for="`check_autocommit_${tabId}`">Autocommit</label>
          </div>

          <TabStatusIndicator :tab-status="tabStatus" />
        </template>

        <button v-if="fetchMoreData && idleState" class="btn btn-sm btn-secondary" title="Fetch More"
          @click="consoleSQL(false, 1)">
          Fetch more
        </button>

        <button v-if="fetchMoreData && idleState" class="btn btn-sm btn-secondary" title="Fetch All"
          @click="consoleSQL(false, 2)">
          Fetch all
        </button>

        <button v-if="fetchMoreData && idleState" class="btn btn-sm btn-secondary" title="Skip Fetch"
          @click="consoleSQL(false, 3)">
          Skip Fetch
        </button>

        <button v-if="openedTransaction && !executingState" class="btn btn-sm btn-primary" title="Run">
          Commit
        </button>

        <button v-if="openedTransaction && !executingState" class="btn btn-sm btn-secondary" title="Run">
          Rollback
        </button>

        <CancelButton v-if="executingState && longQuery" :tab-id="tabId" :conn-id="connId"
          @cancelled="cancelConsoleTab()" />

        <div :id="`div_query_info_${tabId}`">
          <span v-if="cancelled">
            <b>Cancelled</b>
          </span>
          <span v-else-if="queryStartTime && queryDuration" class="mr-2">
            <b>Start time:</b> {{ queryStartTime }} <b>Duration:</b>
            {{ queryDuration }}
          </span>
          <span v-else-if="queryStartTime">
            <b>Start time:</b> {{ queryStartTime }}
          </span>
        </div>
      </div>
      <!--FIXME: add proper editor height recalculation-->
        <QueryEditor ref="editor" class="custom-editor mr-2" :read-only="readOnlyEditor" :tab-id="tabId" tab-mode="console"
          :dialect="dialect" @editor-change="updateEditorContent" :autocomplete="autocomplete"/>
    </pane>
  </splitpanes>

  <CommandsHistoryModal ref="commandsHistory" :tab-id="tabId" :database-index="databaseIndex" tab-type="Console" :commands-modal-visible="commandsModalVisible" @modal-hide="commandsModalVisible=false"/>
  <FileManager ref="fileManager"/>
</template>

<script>
import { Terminal } from "xterm";
import { FitAddon } from "xterm-addon-fit";
import { Splitpanes, Pane } from "splitpanes";
import { emitter } from "../emitter";
import { showToast } from "../notification_control";
import CommandsHistoryModal from "./CommandsHistoryModal.vue";
import moment from "moment";
import { createRequest } from "../long_polling";
import { settingsStore } from "../stores/stores_initializer";
import { connectionsStore } from "../stores/connections";
import TabStatusIndicator from "./TabStatusIndicator.vue";
import QueryEditor from "./QueryEditor.vue";
import CancelButton from "./CancelSQLButton.vue";
import { tabStatusMap, requestState, queryRequestCodes } from "../constants";
import FileManager from "./FileManager.vue";
import FileInputChangeMixin from '../mixins/file_input_mixin'

export default {
  name: "ConsoleTab",
  components: {
    Splitpanes,
    Pane,
    CommandsHistoryModal,
    TabStatusIndicator,
    QueryEditor,
    CancelButton,
    FileManager
  },
  mixins: [FileInputChangeMixin],
  props: {
    connId: String,
    tabId: String,
    consoleHelp: String,
    databaseIndex: Number,
    dialect: String,
  },
  data() {
    return {
      consoleState: requestState.Idle,
      lastCommand: "",
      autocommit: true,
      fetchMoreData: false,
      openedTransaction: false, //TODO: implement commit/rollback functionality
      data: "",
      context: "",
      tabStatus: tabStatusMap.NOT_CONNECTED,
      queryDuration: "",
      queryStartTime: "",
      cancelled: false,
      readOnlyEditor: false,
      editorContent: "",
      longQuery: false,
      commandsModalVisible: false
    };
  },
  computed: {
    executingState() {
      return this.consoleState === requestState.Executing;
    },
    idleState() {
      return this.consoleState === requestState.Idle;
    },
    postgresqlDialect() {
      return this.dialect === "postgresql";
    },
    autocomplete() {
      return connectionsStore.getConnection(this.databaseIndex).autocomplete
    }
  },
  mounted() {
    this.setupTerminal();
    this.setupEvents();

    settingsStore.$subscribe((mutation, state) => {
      this.terminal.options.theme = state.terminalTheme;
      this.terminal.options.fontSize = state.fontSize;
    });

    setTimeout(() => {
      this.onResize();
    }, 200);
  },
  unmounted() {
    this.clearEvents();
  },
  methods: {
    setupTerminal() {
      this.terminal = new Terminal({
        fontSize: settingsStore.fontSize,
        theme: settingsStore.terminalTheme,
        fontFamily: "'Ubuntu Mono', monospace",
        rendererType: "dom", //FIXME: investigate in detail, for no use dom renderer because in nwjs we had some text rendering bugs on light theme
      });

      this.terminal.open(this.$refs.console);
      this.terminal.write(this.consoleHelp);

      this.fitAddon = new FitAddon();

      this.terminal.loadAddon(this.fitAddon);
      this.fitAddon.fit();
    },
    setupEvents() {
      emitter.on(`${this.tabId}_resize`, () => {
        this.onResize();
      });

      emitter.on(`${this.tabId}_check_console_status`, () => {
        if (this.consoleState === requestState.Ready) {
          this.consoleReturnRender(this.data, this.context);
        }
      });

      emitter.on(`${this.tabId}_run_console`, (check_command) => {
        this.consoleSQL(check_command);
      });
    },
    clearEvents() {
      emitter.all.delete(`${this.tabId}_resize`);
      emitter.all.delete(`${this.tabId}_check_console_status`);
      emitter.all.delete(`${this.tabId}_run_console`);
    },
    onResize() {
      this.fitAddon.fit();
    },
    consoleSQL(check_command = true, mode = 0) {
      const command = this.editorContent.trim();
      let tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
      this.queryDuration = "";
      this.cancelled = false;
      this.longQuery = false;

      if (!check_command || command[0] === "\\") {
        if (!this.idleState) {
          showToast("info", "Tab with activity in progres.");
        } else {
          // FIXME: add enum to mode values
          if (command === "" && mode === 0) {
            showToast("info", "Please provide a string.");
          } else {
            emitter.emit(`${this.tabId}_copy_to_editor`, "");
            this.lastCommand = command;

            let message_data = {
              v_sql_cmd: command,
              v_mode: mode,
              v_db_index: this.databaseIndex,
              v_conn_tab_id: this.connId,
              v_tab_id: this.tabId,
              v_autocommit: this.autocommit,
            };

            this.readOnlyEditor = true;

            this.queryStartTime = moment().format();

            let context = {
              tab_tag: tab_tag,
              database_index: this.databaseIndex,
              start_datetime: this.queryStartTime,
              acked: false,
              last_command: this.lastCommand,
              check_command: check_command,
              mode: mode,
              new: true,
              callback: this.consoleReturn.bind(this),
              passwordSuccessCallback: this.passwordSuccessCallback.bind(this),
              passwordFailCalback: () => {
                emitter.emit(`${this.tabId}_cancel_query`);
              },
            };

            context.tab_tag.context = context;

            createRequest(queryRequestCodes.Console, message_data, context);

            this.consoleState = requestState.Executing;

            setTimeout(() => {
              this.longQuery = true;
            }, 1000);

            //FIXME: change into event emitting later
            tab_tag.tab_loading_span.style.visibility = "visible";
            tab_tag.tab_check_span.style.display = "none";

            this.tabStatus = tabStatusMap.RUNNING;
          }
        }
      }
    },
    consoleReturn(data, context) {
      if (!this.idleState) {
        //FIXME: get rid of it when we will have own vue tab wrapper
        if (
          this.tabId === context.tab_tag.tabControl.selectedTab.id &&
          this.connId ===
          context.tab_tag.connTab.tag.connTabControl.selectedTab.id
        ) {
          this.consoleReturnRender(data, context);
        } else {
          this.consoleState = requestState.Ready;
          this.data = data;
          this.context = context;

          //FIXME: change into event emitting later
          context.tab_tag.tab_loading_span.style.visibility = "hidden";
          context.tab_tag.tab_check_span.style.display = "";
        }
      }
    },
    consoleReturnRender(data, context) {
      this.consoleState = requestState.Idle;

      this.tabStatus = data.v_data.v_con_status;
      this.readOnlyEditor = false;

      this.terminal.write(data.v_data.v_data);

      //FIXME: change into event emitting later
      context.tab_tag.tab_loading_span.style.visibility = "hidden";
      context.tab_tag.tab_check_span.style.display = "none";

      this.fetchMoreData = data.v_data.v_show_fetch_button;
      this.queryDuration = data.v_data.v_duration;

      if (!data.v_error) {
        let mode = ["CREATE", "DROP", "ALTER"];
        let status = data.v_data.v_status.split(" ");

        if (mode.includes(status[0])) {
          let node_type = status[1] ? `${status[1].toLowerCase()}_list` : null;

          if (!!node_type)
            emitter.emit(`refreshTreeRecursive_${this.connId}`, node_type);
        }
      }
    },
    clearConsole() {
      this.terminal.write("\x1b[H\x1b[2J");
      this.terminal.write(this.consoleHelp);
    },
    indentSQL() {
      emitter.emit(`${this.tabId}_indent_sql`);
    },
    cancelConsoleTab() {
      this.readOnlyEditor = false;

      this.consoleState = requestState.Idle;
      this.tabStatus = tabStatusMap.NOT_CONNECTED;

      this.cancelled = true;
    },
    passwordSuccessCallback(context) {
      emitter.emit(`${this.tabId}_cancel_query`);

      emitter.emit(`${this.tabId}_copy_to_editor`, this.lastCommand);

      this.consoleSQL(context.check_command, context.mode);
    },
    updateEditorContent(newContent) {
      this.editorContent = newContent;
    },
    showCommandsHistory() {
      this.commandsModalVisible = true
    },
    openFileManagerModal() {
      this.$refs.fileManager.show(true, this.handleFileInputChange);
    },
  },
};
</script>

<style scoped>
.custom-editor {
  height: calc(100% - 50px);
}
.console-body {
  height: calc(100vh - 60px);
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
</style>
