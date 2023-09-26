<template>
  <splitpanes class="default-theme console-body" horizontal @resized="onResize">
    <pane size="80">
      <div ref="console" :id="`txt_console_${tabId}`" class="omnidb__txt-console h-100"></div>
    </pane>

    <pane size="20">
      <div class="row mb-1">
        <div class="tab-actions col-12">
          <button class="btn btn-sm btn-primary" title="Run" @click="consoleSQL(false)">
            <i class="fas fa-play fa-light"></i>
          </button>

          <button class="btn btn-sm btn-secondary" title="Indent SQL" @click="indentSQL()">
            <i class="fas fa-indent fa-ligth"></i>
          </button>

          <button class="btn btn-sm btn-secondary" title="Clear Console" @click="clearConsole()">
            <i class="fas fa-broom fa-ligth"></i>
          </button>

          <button class="btn btn-sm btn-secondary" title="Command History" @click="showConsoleHistory()">
            <i class="fas fa-clock-rotate-left fa-light"></i>
          </button>

          <template v-if="postgresqlDialect">
            <div class="omnidb__form-check form-check form-check-inline">
              <input :id="`check_autocommit_${tabId}`" class="form-check-input" type="checkbox" v-model="autocommit" />
              <label class="form-check-label" :for="`check_autocommit_${tabId}`">Autocommit</label>
            </div>

            <div class="mr-2">
              <i :id="`query_tab_status_${tabId}`" :title="statusText"
                :class="['fas fa-dot-circle tab-status', statusClass]">
                <div v-if="tabStatus === 1 || tabStatus === 2" class="tab-status-indicator">
                  <span :class="circleWavesClass">
                    <span v-for="n in 4" :key="n"></span>
                  </span>
                </div>
              </i>
              <span :id="`query_tab_status_text_${tabId}`" :title="statusText" class="ml-1">
                {{ statusText }}
              </span>
            </div>
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

          <button v-if="openedTransaction && !executingState" class="btn btn-sm btn-primary" title="Run"
            @click="querySQL(3)">
            Commit
          </button>

          <button v-if="openedTransaction && !executingState" class="btn btn-sm btn-secondary" title="Run"
            @click="querySQL(4)">
            Rollback
          </button>

          <button v-if="executingState" class="btn btn-sm btn-danger" title="Cancel" @click="cancelConsole()">
            Cancel
          </button>

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
      </div>
      <div ref="editor" :id="`txt_input_${tabId}`" class="h-100" @keyup="autocompleteStart" @keydown="autocompleteKeyDown"
        @contextmenu.stop.prevent="contextMenu"></div>
    </pane>
  </splitpanes>

  <ConsoleHistoryModal :tab-id="tabId" />
</template>

<script>
import { showConsoleHistory } from "../console";
import { uiCopyTextToClipboard } from "../workspace";
import { querySQL } from "../query";
import ace from "ace-builds";
import { Terminal } from "xterm";
import { FitAddon } from "xterm-addon-fit";
import {
  autocomplete_start,
  autocomplete_keydown,
  autocomplete_update_editor_cursor,
} from "../autocomplete";
import { buildSnippetContextMenuObjects } from "../tree_context_functions/tree_snippets";
import ContextMenu from "@imengyu/vue3-context-menu";
import { Splitpanes, Pane } from "splitpanes";
import { emitter } from "../emitter";
import { snippetsStore } from "../stores/snippets";
import { showToast } from "../notification_control";
import ConsoleHistoryModal from "./ConsoleHistoryModal.vue";
import moment from "moment";
import { v_queryRequestCodes, refreshTreeNode } from "../query";
import { createRequest, removeContext, SetAcked } from "../long_polling";
import { format } from "sql-formatter";
import { settingsStore } from "../stores/settings";

const consoleState = {
  Idle: 0,
  Executing: 1,
  Ready: 2,
};

const tabStatusMap = {
  NOT_CONNECTED: 0,
  IDLE: 1,
  RUNNING: 2,
  IDLE_IN_TRANSACTION: 3,
  IDLE_IN_TRANSACTION_ABORTED: 4,
};

export default {
  name: "ConsoleTab",
  components: {
    Splitpanes,
    Pane,
    ConsoleHistoryModal,
  },
  props: {
    connId: String,
    tabId: String,
    consoleHelp: String,
    databaseIndex: Number,
    dialect: String,
  },
  data() {
    return {
      autocomplete: true,
      consoleState: consoleState.Idle,
      lastCommand: "",
      autocommit: true,
      fetchMoreData: false,
      openedTransaction: false,
      data: "",
      context: "",
      tabStatus: tabStatusMap.NOT_CONNECTED,
      queryDuration: "",
      queryStartTime: "",
      formatOptions: {
        tabWidth: 2,
        keywordCase: "upper",
        //sql-formatter uses 'plsql' for oracle sql flavor
        // otherwise - our db technology names match perfectly
        language: this.dialect === "oracle" ? "plsql" : this.dialect,
        linesBetweenQueries: 1,
      },
      cancelled: false,
    };
  },
  computed: {
    executingState() {
      return this.consoleState === consoleState.Executing;
    },
    idleState() {
      return this.consoleState === consoleState.Idle;
    },
    statusText() {
      const statusMap = {
        0: "Not Connected",
        1: "Idle",
        2: "Running",
        3: "Idle in transaction",
        4: "Idle in transaction (aborted)",
      };
      return statusMap[this.tabStatus] || "";
    },
    statusClass() {
      const statusClassMap = {
        0: "tab-status-closed",
        1: "tab-status-idle position-relative",
        2: "tab-status-running position-relative",
        3: "tab-status-idle_in_transaction",
        4: "tab-status-idle_in_transaction_aborted",
      };

      return `${statusClassMap[this.tabStatus] || ""}`;
    },
    circleWavesClass() {
      return {
        "omnis__circle-waves":
          this.tabStatus === tabStatusMap.IDLE ||
          this.tabStatus === tabStatusMap.RUNNING,
        "omnis__circle-waves--idle": this.tabStatus === tabStatusMap.IDLE,
        "omnis__circle-waves--running": this.tabStatus === tabStatusMap.RUNNING,
      };
    },
    postgresqlDialect() {
      return this.dialect === "postgresql";
    },
  },
  mounted() {
    this.setupEditor();
    this.setupTerminal();
    this.setupEvents();

    settingsStore.$subscribe((mutation, state) => {
      this.editor.setTheme(`ace/theme/${state.editorTheme}`);
      this.editor.setFontSize(state.fontSize);
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
    setupEditor() {
      //TODO: move into mixin
      this.editor = ace.edit(this.$refs.editor);
      this.editor.$blockScrolling = Infinity;
      this.editor.setTheme(`ace/theme/${settingsStore.editorTheme}`);
      this.editor.session.setMode("ace/mode/sql");
      this.editor.setFontSize(settingsStore.fontSize);

      // Remove shortcuts from ace in order to avoid conflict with pgmanage shortcuts
      this.editor.commands.bindKey("ctrl-space", null);
      this.editor.commands.bindKey("Cmd-,", null);
      this.editor.commands.bindKey("Cmd-Delete", null);
      this.editor.commands.bindKey("Ctrl-Delete", null);
      this.editor.commands.bindKey("Ctrl-Up", null);
      this.editor.commands.bindKey("Ctrl-Down", null);
      this.editor.commands.bindKey("Ctrl-,", null);
      this.editor.commands.bindKey("Up", null);
      this.editor.commands.bindKey("Down", null);
      this.editor.commands.bindKey("Tab", null);

      this.editor.focus();
      this.editor.resize();
    },
    setupTerminal() {
      this.terminal = new Terminal({
        fontSize: settingsStore.fontSize,
        theme: settingsStore.terminalTheme,
        fontFamily: "Monospace",
        rendererType: "dom", //FIXME: investigate in detail, for no use dom renderer because in nwjs we had some text rendering bugs on light theme
      });

      this.terminal.open(this.$refs.console);
      this.terminal.write(this.consoleHelp);

      this.fitAddon = new FitAddon();

      this.terminal.loadAddon(this.fitAddon);
      this.fitAddon.fit();
    },
    setupEvents() {
      emitter.on(`${this.tabId}_toggle_autocomplete`, (checked) => {
        this.autocomplete = checked;
      });

      emitter.on(`${this.tabId}_resize`, () => {
        this.onResize();
      });

      emitter.on(`${this.tabId}_copy_to_editor`, (command) => {
        this.editor.setValue(command);
        this.editor.clearSelection();
        this.editor.gotoLine(0, 0, true);
      });

      emitter.on(`${this.tabId}_check_console_status`, () => {
        if (this.consoleState === consoleState.Ready) {
          this.consoleReturnRender(this.data, this.context);
        }
      });

      emitter.on(`${this.tabId}_run_console`, (check_command) => {
        this.consoleSQL(check_command);
      });

      emitter.on(`${this.tabId}_cancel_query`, () => {
        this.cancelConsole();
      });

      emitter.on(`${this.tabId}_indent_sql`, () => {
        this.indentSQL();
      });

      emitter.on(`${this.tabId}_show_autocomplete_results`, (event) => {
        this.autocompleteStart(event, true);
      });
    },
    clearEvents() {
      emitter.all.delete(`${this.tabId}_autocomplete`);
      emitter.all.delete(`${this.tabId}_resize`);
      emitter.all.delete(`${this.tabId}_copy_to_editor`);
      emitter.all.delete(`${this.tabId}_check_console_status`);
      emitter.all.delete(`${this.tabId}_run_console`);
      emitter.all.delete(`${this.tabId}_cancel_query`);
      emitter.all.delete(`${this.tabId}_indent_sql`);
      emitter.all.delete(`${this.tabId}_show_autocomplete_results`);
    },
    autocompleteKeyDown(event) {
      if (this.autocomplete) {
        autocomplete_keydown(this.editor, event);
      } else {
        autocomplete_update_editor_cursor(this.editor, event);
      }
    },
    autocompleteStart(event, force = null) {
      if (this.autocomplete) {
        autocomplete_start(this.editor, 1, event, force);
      }
    },
    contextMenu(event) {
      let option_list = [
        {
          label: "Copy",
          icon: "fas cm-all fa-terminal",
          onClick: () => {
            let copy_text = this.editor.getValue();

            uiCopyTextToClipboard(copy_text);
          },
        },
        {
          label: "Save as snippet",
          icon: "fas cm-all fa-save",
          children: buildSnippetContextMenuObjects(
            "save",
            snippetsStore,
            this.editor
          ),
        },
      ];

      if (snippetsStore.files.length != 0 || snippetsStore.folders.length != 0)
        option_list.push({
          label: "Use snippet",
          icon: "fas cm-all fa-file-code",
          children: buildSnippetContextMenuObjects(
            "load",
            snippetsStore,
            this.editor
          ),
        });
      ContextMenu.showContextMenu({
        theme: "pgmanage",
        x: event.x,
        y: event.y,
        zIndex: 1000,
        minWidth: 230,
        items: option_list,
      });
    },
    onResize() {
      this.fitAddon.fit();
      this.editor.resize();
    },
    consoleSQL(check_command = true, mode = 0) {
      const command = this.editor.getValue().trim();
      let tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
      this.queryDuration = "";
      this.cancelled = false;

      if (!check_command || command[0] === "\\") {
        if (!this.idleState) {
          showToast("info", "Tab with activity in progres.");
        } else {
          // FIXME: add enum to mode values
          if (command === "" && mode === 0) {
            showToast("info", "Please provide a string.");
          } else {
            this.editor.setValue("");
            this.editor.clearSelection();
            this.lastCommand = command;

            let message_data = {
              v_sql_cmd: command,
              v_mode: mode,
              v_db_index: this.databaseIndex,
              v_conn_tab_id: this.connId,
              v_tab_id: this.tabId,
              v_autocommit: this.autocommit,
            };

            this.editor.setReadOnly(true);

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
              passwordFailCalback: this.cancelConsoleTab.bind(this),
            };

            context.tab_tag.context = context;

            createRequest(v_queryRequestCodes.Console, message_data, context);

            this.consoleState = consoleState.Executing;

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
          this.consoleState = consoleState.Ready;
          this.data = data;
          this.context = context;

          //FIXME: change into event emitting later
          context.tab_tag.tab_loading_span.style.visibility = "hidden";
          context.tab_tag.tab_check_span.style.display = "";
        }
      }
    },
    consoleReturnRender(data, context) {
      this.consoleState = consoleState.Idle;

      this.tabStatus = data.v_data.v_con_status;
      this.editor.setReadOnly(false);

      this.terminal.write(data.v_data.v_data);

      //FIXME: change into event emitting later
      context.tab_tag.tab_loading_span.style.visibility = "hidden";
      context.tab_tag.tab_check_span.style.display = "none";

      this.fetchMoreData = data.v_data.v_show_fetch_button;
      this.queryDuration = data.v_data.v_duration;

      if (!data.v_error) {
        let mode = ["CREATE", "DROP", "ALTER"];
        let status = data.v_data.v_status.split(" ");
        let status_name = status[1];

        if (mode.includes(status[0])) {
          //FIXME: replace this with event emitting on tree instance
          let root_node = v_connTabControl.selectedTab.tag.tree.getRootNode();
          if (!!status_name) refreshTreeNode(root_node, status_name);
        }
      }
    },
    clearConsole() {
      this.terminal.write("\x1b[H\x1b[2J");
      this.terminal.write(this.consoleHelp);
    },
    indentSQL() {
      let editor_value = this.editor.getValue();
      let formatted = format(editor_value, this.formatOptions);
      if (formatted.length) {
        this.editor.setValue(formatted);
        this.editor.clearSelection();
        this.editor.gotoLine(0, 0, true);
      }
    },
    cancelConsole() {
      let tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
      let message_data = { tab_id: this.tabId, conn_tab_id: this.connId };

      createRequest(v_queryRequestCodes.CancelThread, message_data, null);

      this.cancelConsoleTab(tab_tag);
    },
    cancelConsoleTab(tab_tag) {
      this.editor.setReadOnly(false);

      this.consoleState = consoleState.Idle;
      this.tabStatus = tabStatusMap.NOT_CONNECTED;

      this.cancelled = true;

      removeContext(tab_tag.context.v_context_code);
      SetAcked(tab_tag.context);
    },
    passwordSuccessCallback(context) {
      this.cancelConsoleTab(context.tab_tag);

      this.editor.setValue(this.lastCommand);
      this.editor.clearSelection();

      this.consoleSQL(context.check_command, context.mode);
    },
    showConsoleHistory,
    querySQL,
  },
};
</script>

<style scoped>
.tab-status-indicator {
  position: absolute;
  width: 15px;
  height: 15px;
  overflow: visible;
  left: 0px;
  top: 0px;
  display: block;
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
