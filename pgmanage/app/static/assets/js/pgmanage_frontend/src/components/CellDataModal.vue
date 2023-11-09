<template>
  <div ref="cellDataModal" class="modal fade" aria-hidden="true" role="dialog" tabindex="-1">
    <div class="modal-dialog" role="document" style="width: 1200px; max-width: 90vw">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 class="modal-title font-weight-bold">Show Data</h2>
          <button type="button" class="close" data-dismiss="modal" aria-label="Close" @click="hideCellDataModal()">
            <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
          </button>
        </div>
        <div class="modal-body" style="white-space: pre-line">
          <div ref="editor" style="height: 70vh; border: 1px solid rgb(195, 195, 195)"></div>
        </div>
        <div class="modal-footer">
          <button type="button" class="btn btn-secondary" data-dismiss="modal" @click="hideCellDataModal()">
            Close
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import ace from "ace-builds";
import { settingsStore } from "../stores/settings";

export default {
  name: "CellDataModal",
  props: {
    cellContent: String,
    showModal: Boolean,
  },
  emits: ["modalHide"],
  watch: {
    showModal: function () {
      this.showCellDataModal();
    },
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
    showCellDataModal() {
      this.setupEdidor();
      this.editor.setValue(this.cellContent);
      this.editor.clearSelection();
      $(this.$refs.cellDataModal).modal({
        backdrop: "static",
        keyboard: false,
      });
    },
    hideCellDataModal() {
      this.editor.destroy();
      $(this.$refs.cellDataModal).modal("hide");
      this.$emit("modalHide");
    },
  },
};
</script>
