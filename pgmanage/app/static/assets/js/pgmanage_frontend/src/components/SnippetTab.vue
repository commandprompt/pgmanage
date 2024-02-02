<template>
  <div ref="editor" class="snippet-editor"></div>

  <div ref="bottomToolbar" class="row px-2">
    <div class="tab_actions tab-actions col-12">
      <button
        data-testid="snippet-tab-indent-button"
        class="btn btn-secondary"
        title="Indent SQL"
        @click="indentSQL"
      >
        <i class="fas fa-indent mr-2"></i>Indent
      </button>
      <button
        data-testid="snippet-tab-save-button"
        class="btn btn-primary"
        title="Save"
        @click="saveSnippetText"
      >
        <i class="fas fa-save mr-2"></i>Save
      </button>
      <button
        data-testid="snippet-tab-save-file-button"
        class="btn btn-primary"
        title="Open file"
        @click="openFileManagerModal"
      >
        <i class="fas fa-folder-open mr-2"></i>Open file
      </button>
    </div>
  </div>
  <FileManager ref="fileManager" />
</template>

<script>
import { format } from "sql-formatter";
import { emitter } from "../emitter";
import ContextMenu from "@imengyu/vue3-context-menu";
import {
  buildSnippetContextMenuObjects,
  saveSnippetTextConfirm,
} from "../tree_context_functions/tree_snippets";
import { snippetsStore, settingsStore } from "../stores/stores_initializer";
import FileManager from "./FileManager.vue";
import { setupAceDragDrop } from '../file_drop'
import FileInputChangeMixin from '../mixins/file_input_mixin'

export default {
  name: "SnippetTab",
  components: {
    FileManager
  },
  mixins: [FileInputChangeMixin],
  props: {
    tabId: String,
    snippet: {
      type: Object,
      default: {
        id: null,
        name: null,
        parent: null,
        type: "snippet",
      },
    },
  },
  data() {
    return {
      editor: null,
      formatOptions: {
        tabWidth: 2,
        keywordCase: "upper",
        linesBetweenQueries: 1,
        language: "sql",
      },
      heightSubtract: 100 + settingsStore.fontSize,
    };
  },
  computed: {
    editorSize() {
      return `calc(100vh - ${this.heightSubtract}px)`;
    },
  },
  mounted() {
    this.setupEditor();
    this.handleResize();
    this.setupEvents();
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

      this.editor.commands.bindKey("ctrl-space", null);

      //Remove shortcuts from ace in order to avoid conflict with omnidb shortcuts
      this.editor.commands.bindKey("Cmd-,", null);
      this.editor.commands.bindKey("Ctrl-,", null);
      this.editor.commands.bindKey("Cmd-Delete", null);
      this.editor.commands.bindKey("Ctrl-Delete", null);
      this.editor.commands.bindKey("Ctrl-Up", null);
      this.editor.commands.bindKey("Ctrl-Down", null);

      this.editor.focus();

      setupAceDragDrop(this.editor)
    },
    setupEvents() {
      emitter.on(`${this.tabId}_editor_focus`, () => {
        this.editor.focus();
      });

      emitter.on(`${this.tabId}_copy_to_editor`, (snippet) => {
        this.editor.setValue(snippet);
        this.editor.clearSelection();
        this.editor.gotoLine(0, 0, true);
      });

      emitter.on(`${this.tabId}_resize`, () => {
        this.handleResize();
      });

      settingsStore.$onAction((action) => {
        if (action.name === "setFontSize") {
          action.after(() => {
            this.editor.setFontSize(settingsStore.fontSize);
            this.handleResize();
          });
        }
      });
    },
    clearEvents() {
      emitter.all.delete(`${this.tabId}_resize`);
      emitter.all.delete(`${this.tabId}_editor_focus`);
      emitter.all.delete(`${this.tabId}_copy_to_editor`);
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
    saveSnippetText(event) {
      // TODO: this old callback is needed here to track whether the tab is opened or not from outside
      // Change this when snippets tab management is implemented
      let callback = function (return_object) {
        v_connTabControl.snippet_tag.tabControl.selectedTab.tag.snippetObject =
          return_object;
        v_connTabControl.snippet_tag.tabControl.selectedTab.tag.tab_title_span.innerHTML =
          return_object.name;
      };

      if (this.snippet.id !== null) {
        saveSnippetTextConfirm(this.snippet, this.editor.getValue(), callback);
      } else {
        ContextMenu.showContextMenu({
          theme: "pgmanage",
          x: event.x,
          y: event.y,
          zIndex: 1000,
          minWidth: 230,
          direction: "tr",
          items: buildSnippetContextMenuObjects(
            "save",
            snippetsStore,
            this.editor.getValue(),
            callback
          ),
        });
      }
    },
    handleResize() {
      // handle case when snippets panel is not visible
      const top =
        this.$refs.editor.getBoundingClientRect().top > window.innerHeight
          ? 55
          : this.$refs.editor.getBoundingClientRect().top;
      this.heightSubtract = top + 30 * (settingsStore.fontSize / 10);
    },
    openFileManagerModal() {
      this.$refs.fileManager.show(true, this.handleFileInputChange);
    },
  },
};
</script>

<style scoped>
.snippet-editor {
  height: v-bind(editorSize);
}

.tab-actions {
  align-items: center;
  display: flex;
  justify-content: flex-start;
  min-height: 35px;
}

.tab-actions > button {
  margin-right: 5px;
}
</style>
