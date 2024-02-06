<template>
  <div
    ref="editor"
    @contextmenu.stop.prevent="contextMenu"
    @keyup="autocompleteStart"
    @keydown="autocompleteKeyDown"
  ></div>
</template>

<script>
import ContextMenu from "@imengyu/vue3-context-menu";
import { snippetsStore, settingsStore } from "../stores/stores_initializer";
import { buildSnippetContextMenuObjects } from "../tree_context_functions/tree_snippets";
import { uiCopyTextToClipboard } from "../workspace";
import {
  autocomplete_start,
  autocomplete_keydown,
  autocomplete_update_editor_cursor,
  close_autocomplete,
} from "../autocomplete";
import { emitter } from "../emitter";
import { format } from "sql-formatter";
import { setupAceDragDrop } from "../file_drop";
import { maxLinesForIndentSQL } from "../constants";
import { showToast } from "../notification_control";

export default {
  props: {
    readOnly: {
      type: Boolean,
      default: false,
    },
    autocomplete: {
      type: Boolean,
      default: true,
    },
    tabId: String,
    tabMode: String,
    dialect: String,
  },
  emits: ["editorChange"],
  data() {
    return {
      editor: "",
      formatOptions: {
        tabWidth: 2,
        keywordCase: "upper",
        //sql-formatter uses 'plsql' for oracle sql flavor
        // otherwise - our db technology names match perfectly
        language: this.dialect === "oracle" ? "plsql" : this.dialect,
        linesBetweenQueries: 1,
      },
    };
  },
  computed: {
    autocompleteMode() {
      return this.tabMode === "query" ? 0 : 1;
    },
  },
  watch: {
    readOnly(newValue, oldValue) {
      this.editor.setReadOnly(newValue);
    },
  },
  mounted() {
    this.setupEditor();
    this.setupEvents();
    this.editor.on("change", () => {
      this.$emit("editorChange", this.editor.getValue().trim());
    });

    this.editor.on("blur", () => {
      setTimeout(() => {
        close_autocomplete();
      }, 200);
    });

    settingsStore.$subscribe((mutation, state) => {
      this.editor.setTheme(`ace/theme/${state.editorTheme}`);
      this.editor.setFontSize(state.fontSize);
    });
  },
  unmounted() {
    this.clearEvents();
  },
  methods: {
    setupEditor() {
      this.editor = ace.edit(this.$refs.editor);
      this.editor.$blockScrolling = Infinity;
      this.editor.setTheme(`ace/theme/${settingsStore.editorTheme}`);
      this.editor.session.setMode("ace/mode/sql");
      this.editor.setFontSize(settingsStore.fontSize);
      this.editor.setShowPrintMargin(false);

      // Remove shortcuts from ace in order to avoid conflict with pgmanage shortcuts
      this.editor.commands.bindKey("ctrl-space", null);
      this.editor.commands.bindKey("alt-e", null);
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

      setupAceDragDrop(this.editor);
    },
    getQueryEditorValue(raw_query) {
      if (raw_query) return this.editor.getValue().trim();
      let selectedText = this.editor.getSelectedText();
      let lineAtCursor = this.editor.session.getLine(
        this.editor.getCursorPosition().row
      );
      return !!selectedText ? selectedText : lineAtCursor;
    },
    contextMenu(event) {
      let option_list = [
        {
          label: "Run selection/line at cursor",
          icon: "fas cm-all fa-play fa-light",
          onClick: () => {
            this.$emit("run-selection");
          },
        },
        {
          label: "Copy",
          icon: "fas cm-all fa-terminal",
          onClick: () => {
            let copy_text = this.getQueryEditorValue(true);

            uiCopyTextToClipboard(copy_text);
          },
        },
        {
          label: "Save as snippet",
          icon: "fas cm-all fa-save",
          children: buildSnippetContextMenuObjects(
            "save",
            snippetsStore,
            this.editor.getValue()
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
            this.editor.getValue()
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
    autocompleteKeyDown(event) {
      if (this.autocomplete) {
        autocomplete_keydown(this.editor, event);
      } else {
        autocomplete_update_editor_cursor(this.editor, event);
      }
    },
    autocompleteStart(event, force = null) {
      if (this.autocomplete) {
        autocomplete_start(this.editor, this.autocompleteMode, event, force);
      }
    },
    indentSQL() {
      let editor_value = this.editor.getValue();

      if (this.editor.session.getLength() > maxLinesForIndentSQL) {
        showToast(
          "error",
          `Max lines(${maxLinesForIndentSQL}) for indentSQL exceeded.`
        );
        return;
      }

      let formatted = format(editor_value, this.formatOptions);
      if (formatted.length) {
        this.editor.setValue(formatted);
        this.editor.clearSelection();
        this.editor.gotoLine(0, 0, true);
      }
    },
    focus() {
      this.editor.focus();
    },
    setupEvents() {
      emitter.on(`${this.tabId}_show_autocomplete_results`, (event) => {
        this.autocompleteStart(event, true);
      });

      emitter.on(`${this.tabId}_copy_to_editor`, (command) => {
        this.editor.setValue(command);
        this.editor.clearSelection();
        this.editor.gotoLine(0, 0, true);
      });

      emitter.on(`${this.tabId}_insert_to_editor`, (command) => {
        this.editor.insert(command);
        this.editor.clearSelection();
      });

      emitter.on(`${this.tabId}_indent_sql`, () => {
        this.indentSQL();
      });
    },
    clearEvents() {
      emitter.all.delete(`${this.tabId}_show_autocomplete_results`);
      emitter.all.delete(`${this.tabId}_copy_to_editor`);
      emitter.all.delete(`${this.tabId}_indent_sql`);
    },
  },
};
</script>
