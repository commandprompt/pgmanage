<template>
  <splitpanes class="default-theme" horizontal style="height: calc(100vh - 60px)" @resized="onResize">
    <pane>
      <div ref="console" :id="`txt_console_${tabId}`" class="omnidb__txt-console" style="height: 100%"></div>
    </pane>

    <pane>
      <div class="row mb-1">
        <div class="tab_actions omnidb__tab-actions col-12">
          <button class="btn btn-sm btn-primary omnidb__tab-actions__btn" title="Run" @click="sendConsoleSQL(false)">
            <i class="fas fa-play fa-light"></i>
          </button>

          <button class="btn btn-sm btn-secondary omnidb__tab-actions__btn" title="Indent SQL" @click="indentSQL()">
            <i class="fas fa-indent fa-ligth"></i>
          </button>

          <button class="btn btn-sm btn-secondary omnidb__tab-actions__btn" title="Clear Console" @click="clearConsole()">
            <i class="fas fa-broom fa-ligth"></i>
          </button>

          <button class="btn btn-sm btn-secondary omnidb__tab-actions__btn" title="Command History"
            @click="showConsoleHistory()">
            <i class="fas fa-clock-rotate-left fa-light"></i>
          </button>

          <div class="dbms_object postgresql_object omnidb__form-check form-check form-check-inline">
            <input :id="`check_autocommit_${tabId}`" class="form-check-input" type="checkbox" v-model="autocommit" />
            <label class="form-check-label dbms_object postgresql_object custom_checkbox query_info"
              :for="`check_autocommit_${tabId}`">Autocommit</label>
          </div>

          <div class="dbms_object postgresql_object omnidb__tab-status">
            <i :id="`query_tab_status_${tabId}`" title="Not connected"
              class="fas fa-dot-circle tab-status tab-status-closed dbms_object postgresql_object omnidb__tab-status__icon">
            </i>
            <span :id="`query_tab_status_text_${tabId}`" title="Not connected"
              class="tab-status-text query_info dbms_object postgresql_object ml-1">
              Not connected
            </span>
          </div>

          <button v-if="fetchMoreData && idleState" class="btn btn-sm btn-secondary omnidb__tab-actions__btn"
            title="Fetch More" @click="consoleSQL(false, 1)">
            Fetch more
          </button>

          <button v-if="fetchMoreData && idleState" class="btn btn-sm btn-secondary omnidb__tab-actions__btn"
            title="Fetch All" style="margin-left: 5px" @click="consoleSQL(false, 2)">
            Fetch all
          </button>

          <button v-if="fetchMoreData && idleState" class="btn btn-sm btn-secondary omnidb__tab-actions__btn"
            title="Skip Fetch" style="margin-left: 5px" @click="consoleSQL(false, 3)">
            Skip Fetch
          </button>

          <button v-if="openedTransaction && !executingState"
            class="dbms_object dbms_object_hidden postgresql_object btn btn-sm btn-primary omnidb__tab-actions__btn"
            title="Run" style="margin-left: 5px" @click="querySQL(3)">
            Commit
          </button>

          <button v-if="openedTransaction && !executingState"
            class="dbms_object dbms_object_hidden postgresql_object btn btn-sm btn-secondary omnidb__tab-actions__btn"
            title="Run" style="margin-left: 5px" @click="querySQL(4)">
            Rollback
          </button>

          <button v-if="executingState" class="btn btn-sm btn-danger omnidb__tab-actions__btn" title="Cancel"
            @click="cancelConsole()">
            Cancel
          </button>

          <div :id="`div_query_info_${tabId}`" class="omnidb__query-info"></div>
        </div>
      </div>
      <div ref="editor" :id="`txt_input_${tabId}`" class="omnidb__console__text-input" style="height: 100%"
        @keyup="autocompleteStart" @keydown="autocompleteKeyDown" @contextmenu.stop.prevent="contextMenu"></div>
    </pane>
  </splitpanes>

  <ConsoleHistoryModal :tab-id="tabId" />
</template>

<script>
import {
  consoleSQL,
  showConsoleHistory,
  cancelConsole
} from "../console";
import { indentSQL, uiCopyTextToClipboard } from "../workspace";
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
import { createRequest } from "../long_polling";

const consoleState = {
  Idle: 0,
  Executing: 1,
  Ready: 2,
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
    editorTheme: String,
    fontSize: Number,
    terminalTheme: Object,
    consoleHelp: String,
    databaseIndex: Number,
  },
  data() {
    return {
      autocomplete: true,
      consoleState: consoleState.Idle,
      lastCommand: "",
      autocommit: true,
      fetchMoreData: false,
      openedTransaction: false,
      data: '',
      context: '',
    };
  },
  computed: {
    executingState() {
      return this.consoleState === consoleState.Executing;
    },
    idleState() {
      return this.consoleState === consoleState.Idle;
    },
  },
  mounted() {
    this.setupEditor();
    this.setupTerminal();
    emitter.on(`${this.tabId}_autocomplete`, (checked) => {
      this.autocomplete = checked;
    });

    emitter.on(`${this.tabId}_console_return`, ({data, context}) => {
      if (!this.idleState) {
        //TODO: move current connection tab and current tab to global state
        if (this.tabId === context.tab_tag.tabControl.selectedTab.id && this.connId === context.tab_tag.connTab.tag.connTabControl.selectedTab.id) {
          this.consoleReturnRender(data, context)
        } else {
          this.consoleState = consoleState.Ready
          this.data = data
          this.context = context

          //FIXME: change into event emitting later
          context.tab_tag.tab_loading_span.style.visibility = 'hidden';
			    context.tab_tag.tab_check_span.style.display = '';
        }
      }
    })
  },
  unmounted() {
    emitter.all.delete(`${this.tabId}_autocomplete`);
  },
  methods: {
    setupEditor() {
      //TODO: move into mixin
      this.editor = ace.edit(this.$refs.editor);
      this.editor.$blockScrolling = Infinity;
      this.editor.setTheme(`ace/theme/${this.editorTheme}`);
      this.editor.session.setMode("ace/mode/sql");
      this.editor.setFontSize(this.fontSize);

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
        fontSize: this.fontSize,
        theme: this.terminalTheme,
        fontFamily: "Monospace",
        rendererType: "dom", //FIXME: investigate in detail, for no use dom renderer because in nwjs we had some text rendering bugs on light theme
      });

      this.terminal.open(this.$refs.console);
      this.terminal.write(this.consoleHelp);

      this.fitAddon = new FitAddon();

      this.terminal.loadAddon(this.fitAddon);
      this.fitAddon.fit();
    },
    autocompleteKeyDown(event) {
      if (this.autocomplete) {
        autocomplete_keydown(this.editor, event);
      } else {
        autocomplete_update_editor_cursor(this.editor, event);
      }
    },
    autocompleteStart(event) {
      if (this.autocomplete) {
        autocomplete_start(this.editor, 1, event);
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
    sendConsoleSQL(check_command = true, mode = 0) {
      const command = this.editor.getValue().trim();
      let tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag

      if (!check_command || command === "\\") {

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

            let start_datetime = moment().format();

            let context = {
              tab_tag: tab_tag,
              database_index: this.databaseIndex,
              start_datetime: start_datetime,
              acked: false,
              last_command: this.lastCommand,
              check_command: check_command,
              mode: mode,
              new: true
            };

            createRequest(v_queryRequestCodes.Console, message_data, context);

            this.consoleState = consoleState.Executing;

            //FIXME: change into event emitting later
            tab_tag.tab_loading_span.style.visibility = 'visible';
            tab_tag.tab_check_span.style.display = 'none';
            //
            // check this as well
            // probably move this inside this component
            // setTabStatus(tab_tag, 2);
          }
        }
      }
    },
    consoleReturnRender(data, context) {
      this.consoleState = consoleState.Idle

      // setTabStatus(p_context.tab_tag,p_message.v_data.v_con_status);
      this.editor.setReadOnly(false)

      this.terminal.write(context.tab_tag.tempData)

      //FIXME: change into event emitting later
      context.tab_tag.tab_loading_span.style.visibility = 'hidden'
      context.tab_tag.tab_check_span.style.display = 'none'

      if (data.v_data.v_show_fetch_button) {
        this.fetchMoreData = true
      }

      if (!data.v_error) {
        let mode = ['CREATE', 'DROP', 'ALTER'];
        let status = data.v_data.v_status.split(" ");
        let status_name = status[1];

        if (mode.includes(status[0])) {
          //FIXME: replace this with event emitting on tree instance
          let root_node = v_connTabControl.selectedTab.tag.tree.getRootNode();
          if (!!status_name)
            refreshTreeNode(root_node, status_name);
        }
      }
    },
    setTabStatus(code) {

    },
    clearConsole() {
      this.terminal.write('\x1b[H\x1b[2J');
      this.terminal.write(this.consoleHelp)
    },
    showConsoleHistory,
    cancelConsole,
    indentSQL,
    querySQL,
    autocomplete_start,
  },
};
</script>
