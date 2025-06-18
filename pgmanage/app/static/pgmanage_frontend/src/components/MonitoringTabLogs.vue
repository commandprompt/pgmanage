<template>
  <div
    v-if="loggingDisabled"
    class="alert alert-warning d-flex align-items-center justify-content-center"
    role="alert"
  >
    <i class="fas fa-exclamation-triangle me-2"></i>
    <div>Please enable logging to view the server logs.</div>
  </div>

  <div v-else>
    <div ref="topToolbar" class="row p-1">
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
    <div class="card border-0">
      <Transition :duration="100">
        <div v-if="showLoading" class="div_loading d-block" style="z-index: 10">
          <div class="div_loading_cover"></div>
          <div class="div_loading_content">
            <div class="spinner-border spinner-size text-primary" role="status">
              <span class="sr-only">Loading...</span>
            </div>
          </div>
        </div>
      </Transition>
      <div ref="editor" class="ace-editor"></div>
    </div>
  </div>
</template>

<script>
import { settingsStore, tabsStore } from "../stores/stores_initializer";
import { handleError } from "../logging/utils";

import axios from "axios";
export default {
  name: "MonitoringTabLogs",
  props: {
    databaseIndex: Number,
    workspaceId: String,
    tabId: String,
    height: String,
  },
  data() {
    return {
      editor: null,
      formatModes: {},
      formatMode: "stderr",
      heightSubtract: 150,
      showLoading: true,
      loggingDisabled: false,
    };
  },
  computed: {
    editorHeight() {
      return `calc(100vh - ${this.heightSubtract}px)`;
    },
  },
  watch: {
    formatMode(newValue) {
      const aceMode =
        this.formatModes[newValue]?.ace_mode ?? "ace/mode/pgsql_extended";
      this.editor.session.setMode(aceMode);
      this.getLog();
    },
  },
  mounted() {
    this.setupEditor();
    this.getLogFormat();

    window.addEventListener("resize", () => {
      if (
        tabsStore.selectedPrimaryTab?.metaData?.selectedTab?.id !== this.tabId
      )
        return;
      this.handleResize();
    });

    settingsStore.$onAction((action) => {
      if (action.name === "setFontSize") {
        if (
          tabsStore.selectedPrimaryTab?.metaData?.selectedTab?.id !== this.tabId
        )
          return;
        action.after(() => {
          requestAnimationFrame(() => {
            requestAnimationFrame(() => {
              this.handleResize();
            });
          });
        });
      }
    });

    settingsStore.$subscribe((mutation, state) => {
      this.editor.setTheme(`ace/theme/${state.editorTheme}`);
      this.editor.setFontSize(state.fontSize);
    });

    const tabEl = document.getElementById(`${this.tabId}-logs-tab`);
    tabEl.addEventListener("shown.bs.tab", (event) => {
      this.handleResize();
      this.getLog();
    });
  },
  methods: {
    setupEditor() {
      this.editor = ace.edit(this.$refs.editor);
      this.editor.$blockScrolling = Infinity;
      this.editor.session.setMode("ace/mode/pgsql_extended");
      this.editor.setTheme(`ace/theme/${settingsStore.editorTheme}`);
      this.editor.setFontSize(settingsStore.fontSize);
      this.editor.setShowPrintMargin(false);
      this.editor.setReadOnly(true);

      this.editor.commands.bindKey("Cmd-,", null);
      this.editor.commands.bindKey("Ctrl-,", null);
      this.editor.commands.bindKey("Cmd-Delete", null);
      this.editor.commands.bindKey("Ctrl-Delete", null);
    },
    getLog() {
      this.showLoading = true;
      axios
        .post("/get_postgres_server_log/", {
          database_index: this.databaseIndex,
          workspace_id: this.workspaceId,
          log_format: this.formatMode,
        })
        .then((response) => {
          if (response.data.logs === null) {
            this.loggingDisabled = true;
          }

          this.editor.setValue(response.data.logs);
          this.editor.clearSelection();
          this.showLoading = false;
        })
        .catch((error) => {
          handleError(error);
          this.showLoading = false;
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
              available: !!resp.data.formats?.includes("stderr"),
            },
            csvlog: {
              ace_mode: "ace/mode/xml",
              text: "CSV",
              available: !!resp.data.formats?.includes("csvlog"),
            },
            jsonlog: {
              ace_mode: "ace/mode/json",
              text: "JSON",
              available: !!resp.data.formats?.includes("jsonlog"),
            },
          };
        })
        .catch((error) => {
          handleError(error);
        });
    },
    handleResize() {
      if (this.$refs === null) return;

      this.heightSubtract =
        this.$refs.topToolbar.getBoundingClientRect().bottom;
    },
  },
};
</script>

<style scoped>
.ace-editor {
  height: v-bind(editorHeight);
}
</style>
