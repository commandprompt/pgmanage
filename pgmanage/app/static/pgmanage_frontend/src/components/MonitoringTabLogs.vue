<template>
  <div>
    <div class="row">
      <div class="align-items-center d-flex col-2 offset-10">
        <span class="me-1"> Format: </span>
        <select class="form-select" v-model="formatMode">
          <template v-for="(modeObj, modeName, index) in formatModes">
            <option v-if="modeObj.available" :value="modeName" :key="index">
              {{ modeObj.text }}
            </option>
          </template>
        </select>
      </div>
    </div>
    <div ref="editor" class="ace-editor"></div>
  </div>
</template>

<script>
import { settingsStore } from "../stores/stores_initializer";
import { handleError } from "../logging/utils";

import axios from "axios";
export default {
  name: "MonitoringTabLogs",
  props: {
    databaseIndex: Number,
    workspaceId: String,
  },
  data() {
    return {
      editor: null,
      formatModes: {},
      formatMode: "stderr",
    };
  },
  watch: {
    formatMode(newValue) {
      const aceMode =
        this.formatModes[newValue]?.ace_mode ?? "ace/mode/pgsql_extended";
      this.editor.session.setMode(aceMode);
    },
  },
  mounted() {
    this.setupEditor();
    // this.editor.setValue(testText)
    // this.editor.clearSelection();

    // this.editor.session.setMode("ace/mode/pgsql_extended");
    // beautify(this.editor.getSession());
    this.getLogFormat();
    this.getLog();
  },
  methods: {
    setupEditor() {
      this.editor = ace.edit(this.$refs.editor);
      this.editor.$blockScrolling = Infinity;
      this.editor.setTheme(`ace/theme/${settingsStore.editorTheme}`);
      this.editor.setFontSize(settingsStore.fontSize);
      this.editor.setShowPrintMargin(false);
      this.editor.setReadOnly(true);

      this.editor.commands.bindKey("Cmd-,", null);
      this.editor.commands.bindKey("Ctrl-,", null);
      this.editor.commands.bindKey("Cmd-Delete", null);
      this.editor.commands.bindKey("Ctrl-Delete", null);
      //   this.editor.getSession().on("changeScrollTop", this.onEditorScroll);
    },
    getLog() {
      axios
        .post("/get_postgres_server_log/", {
          database_index: this.databaseIndex,
          workspace_id: this.workspaceId,
        })
        .then((response) => {
          this.data = response.data;
          this.editor.setValue(response.data.data[0][0]);
          this.editor.clearSelection();
          this.editor.session.setMode(this.contentMode);
        })
        .catch((error) => {
          console.log(error);
        });
    },
    getLogFormat() {
      axios
        .post("/get_postgres_server_log_formats/", {
          database_index: this.databaseIndex,
          workspace_id: this.workspaceId,
        })
        .then((resp) => {
          this.formatModes = {
            stderr: {
              ace_mode: "ace/mode/pgsql_extended",
              text: "TEXT",
              available: !!resp.data.formats[0]?.includes("stderr"),
            },
            csvlog: {
              ace_mode: "ace/mode/xml",
              text: "CSV",
              available: !!resp.data.formats[0]?.includes("csvlog"),
            },
            jsonlog: {
              ace_mode: "ace/mode/json",
              text: "JSON",
              available: !!resp.data.formats[0]?.includes("jsonlog"),
            },
          };
        })
        .catch((error) => {
          console.log(error);
          // handleError(error);
        });
    },
  },
};
</script>

<style scoped>
.ace-editor {
  height: 70vh;
}
</style>
