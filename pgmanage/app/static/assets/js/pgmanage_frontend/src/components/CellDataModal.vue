<template>
  <div
    id="cell_data_modal"
    ref="cellDataModal"
    class="modal fade"
    aria-hidden="true"
    role="dialog"
    tabindex="-1"
  >
    <div class="modal-dialog" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 class="modal-title font-weight-bold">Show Data</h2>
          <button
            type="button"
            class="btn-close"
            data-dismiss="modal"
            aria-label="Close"
            @click="store.hideModal()"
          >
          </button>
        </div>
        <div class="modal-body">
          <div ref="editor" class="ace-editor"></div>
        </div>
        <div class="modal-footer">
          <button
            type="button"
            class="btn btn-secondary"
            data-dismiss="modal"
            @click="store.hideModal()"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import {
  settingsStore,
  cellDataModalStore,
} from "../stores/stores_initializer";
import { Modal } from "bootstrap";

export default {
  name: "CellDataModal",
  data() {
    return {
      editor: null,
      modalInstance: null,
    };
  },
  computed: {
    store() {
      return cellDataModalStore;
    },
  },
  mounted() {
    cellDataModalStore.$onAction((action) => {
      if (action.name === "showModal") {
        action.after(() => {
          this.setupEdidor();
          this.setEditorContent();
          this.modalInstance = Modal.getOrCreateInstance(this.$refs.cellDataModal, {
            backdrop: "static",
            keyboard: false,
          });
          this.modalInstance.show();
        });
      }

      if (action.name === "hideModal") {
        this.editor.destroy();
        this.modalInstance.hide();
        // erase leftover css classes left after editor destruction
        this.$refs.editor.classList.remove('ace-omnidb', 'ace-omnidb_dark')
      }
    });
  },
  methods: {
    setupEdidor() {
      this.editor = ace.edit(this.$refs.editor);
      this.editor.$blockScrolling = Infinity;
      this.editor.setTheme(`ace/theme/${settingsStore.editorTheme}`);
      this.editor.session.setMode("ace/mode/sql");
      this.editor.setFontSize(settingsStore.fontSize);
      this.editor.setShowPrintMargin(false);
      this.editor.setReadOnly(true);

      this.editor.commands.bindKey("Cmd-,", null);
      this.editor.commands.bindKey("Ctrl-,", null);
      this.editor.commands.bindKey("Cmd-Delete", null);
      this.editor.commands.bindKey("Ctrl-Delete", null);
    },
    setEditorContent() {
      let cellContent = this.store.cellContent;
      if (cellContent) cellContent = this.store.cellContent.toString();
      this.editor.setValue(cellContent);
      this.editor.clearSelection();
    },
  },
};
</script>

<style scoped>
.modal-dialog {
  width: 1200px;
  max-width: 90vw;
}

.ace-editor {
  height: 70vh;
}

.modal-body {
  white-space: pre-line;
}
</style>
