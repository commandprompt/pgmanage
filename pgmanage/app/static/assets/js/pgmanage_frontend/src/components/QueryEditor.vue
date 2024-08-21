<template>
  <div ref="editor" @contextmenu.stop.prevent="contextMenu">
  </div>
</template>
<script>
import ContextMenu from "@imengyu/vue3-context-menu";
import { snippetsStore, settingsStore, dbMetadataStore  } from "../stores/stores_initializer";
import { buildSnippetContextMenuObjects } from "../tree_context_functions/tree_snippets";
import { emitter } from "../emitter";
import { format } from "sql-formatter";
import { setupAceDragDrop, setupAceSelectionHighlight } from "../ace_plugins";
import { maxLinesForIndentSQL } from "../constants";
import { showToast } from "../notification_control";
import { SQLAutocomplete, SQLDialect } from 'sql-autocomplete';

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
    databaseIndex: Number,
    databaseName: String,
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
      completer: null
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
    autocomplete(newVal, oldVal) {
      this.editor.setOptions({
        enableLiveAutocompletion: newVal,
        liveAutocompletionDelay: 100,
      })
    }
  },
  mounted() {
    this.setupEditor();
    this.setupEvents();
    this.editor.on("change", () => {
      this.$emit("editorChange", this.editor.getValue().trim());
    });

    settingsStore.$subscribe((mutation, state) => {
      this.editor.setTheme(`ace/theme/${state.editorTheme}`);
      this.editor.setFontSize(state.fontSize);
    });

    if(this.databaseIndex && this.databaseName) {
      dbMetadataStore.$onAction((action) => {
        if (action.name === "fetchDbMeta" && action.args[0] == this.databaseIndex) {
          action.after((result) => {
            this.setupCompleter();
          });
        }
      });

      dbMetadataStore.fetchDbMeta(this.databaseIndex, this.tabId, this.databaseName)
    }
    if(this.autocomplete) {
      this.editor.setOptions({
        enableLiveAutocompletion: true,
        liveAutocompletionDelay: 100,
      })
    }
  },
  unmounted() {
    this.clearEvents();
  },
  methods: {
    refetchMetaHandler(e) {
      if(e.databaseIndex == this.databaseIndex)
        dbMetadataStore.fetchDbMeta(this.databaseIndex, this.tabId, this.databaseName)
    },
    setupEditor() {
      const EDITOR_MODEMAP = {
        'postgresql': 'pgsql',
        'mysql': 'mysql',
        'mariadb': 'mysql',
        'oracle': 'plsql'
      }

      let editor_mode = EDITOR_MODEMAP[this.dialect] || 'sql'

      this.editor = ace.edit(this.$refs.editor);
      this.editor.$blockScrolling = Infinity;
      this.editor.setTheme(`ace/theme/${settingsStore.editorTheme}`);
      this.editor.session.setMode(`ace/mode/${editor_mode}`);
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
      this.editor.commands.bindKey("Ctrl-F", null);
      this.editor.commands.bindKey("Ctrl-,", null);

      this.editor.setOptions({
        enableBasicAutocompletion: [
          {
            getCompletions: (function (editor, session, pos, prefix, callback) {
              if(!this.completer)
                return
              const options = this.completer.autocomplete(
                editor.getValue(),
                editor.session.doc.positionToIndex(editor.selection.getCursor())
              );

              let ret = [];
              options.forEach(function(opt) {
                ret.push({
                    caption: opt.value,
                    value: opt.value,
                    meta: opt.optionType.toLowerCase(),
                    score: 100
                });
              })
              callback(null, ret)
            }).bind(this)
          }
        ],
      });

      this.editor.focus();
      this.editor.resize();

      setupAceDragDrop(this.editor);
      setupAceSelectionHighlight(this.editor);
    },
    setupCompleter() {
      // TODO:
      // figure out how to do completion for console tab - suggest keywords only?
      // reuse completer instance for the same dbindex/database combo if possible
      // use fuzzy matching for completions
      // fix cursor overlapping with text letters in the editor
      // oracle support
      // multiple get meta requests ?

      const dbMeta = dbMetadataStore.getDbMeta(this.databaseIndex, this.databaseName)

      if(!dbMeta)
        return

      let tableNames = []
      let columnNames = []
      dbMeta.forEach((schema) => {
        if(['information_schema', 'pg_catalog'].includes(schema.name))
          return

        tableNames = tableNames.concat(schema.tables.map((t) => t.name))
        schema.tables.forEach((t) => {
          columnNames = columnNames.concat(t.columns)
        })
      })

      columnNames = [...new Set(columnNames)]
      const DIALECT_MAP = {
        'postgresql': SQLDialect.PLpgSQL,
        'mysql': SQLDialect.MYSQL,
        'mariadb': SQLDialect.MYSQL,
        'oracle': SQLDialect.PLpgSQL,
        'sqlite': SQLDialect.SQLITE,
      }

      this.completer = new SQLAutocomplete(DIALECT_MAP[this.dialect] || SQLDialect.PLpgSQL, tableNames, columnNames);
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
          disabled: !this.editor.getSelectedText(),
          onClick: () => {
            document.execCommand("copy");
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
        this.editor.execCommand("startAutocomplete")
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

      emitter.on(`${this.tabId}_find_replace`, () => {
        this.editor.execCommand("find")
      });

      // by using a scoped function we can then unsubscribe with mitt.off
      emitter.on("refetchMeta", this.refetchMetaHandler)
    },
    clearEvents() {
      emitter.all.delete(`${this.tabId}_show_autocomplete_results`);
      emitter.all.delete(`${this.tabId}_copy_to_editor`);
      emitter.all.delete(`${this.tabId}_insert_to_editor`);
      emitter.all.delete(`${this.tabId}_indent_sql`);
      emitter.all.delete(`${this.tabId}_find_replace`);

      emitter.off("refetchMeta", this.refetchMetaHandler)
    },
  }
};
</script>
