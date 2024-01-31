<template>
  <Teleport to="body">
    <div
      data-testid="widget-edit-modal"
      ref="editWidgetModal"
      class="modal"
      tabindex="-1"
      role="dialog"
    >
      <div class="modal-dialog modal-xl">
        <div class="modal-content">
          <div class="modal-header align-items-center">
            <h2
              data-testid="widget-edit-header-title"
              class="modal-title font-weight-bold"
            >
              Monitoring Widget
            </h2>
            <button
              type="button"
              class="close"
              data-dismiss="modal"
              aria-label="Close"
              @click="closeModal"
            >
              <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
            </button>
          </div>

          <div class="modal-body">
            <div class="form-row mt-3">
              <div class="form-group col-3">
                <label for="widgetName" class="font-weight-bold mb-2"
                  >Name</label
                >
                <input
                  type="text"
                  :class="[
                    'form-control',
                    { 'is-invalid': v$.widgetName.$invalid },
                  ]"
                  id="widgetName"
                  data-testid="widget-edit-name"
                  placeholder="Widget name"
                  v-model="v$.widgetName.$model"
                  :disabled="showTestWidget"
                />
                <div class="invalid-feedback">
                  <a v-for="error of v$.widgetName.$errors" :key="error.$uid">
                    {{ error.$message }}
                    <br />
                  </a>
                </div>
              </div>

              <div class="form-group col-2">
                <label for="widgetType" class="font-weight-bold mb-2"
                  >Type</label
                >
                <select
                  id="widgetType"
                  class="form-control"
                  placeholder="Widget type"
                  v-model="selectedType"
                  :disabled="showTestWidget"
                >
                  <option
                    v-for="(widgetType, index) in widgetTypes"
                    :key="index"
                    :value="widgetType"
                  >
                    {{ widgetType }}
                  </option>
                </select>
              </div>

              <div class="form-group col-2">
                <label for="refreshInterval" class="font-weight-bold mb-2"
                  >Refresh Interval</label
                >
                <input
                  type="text"
                  :class="[
                    'form-control',
                    { 'is-invalid': v$.widgetInterval.$invalid },
                  ]"
                  id="refreshInterval"
                  data-testid="widget-edit-refresh-interval"
                  placeholder="Widget Interval"
                  v-model.number="v$.widgetInterval.$model"
                  :disabled="showTestWidget"
                />
                <div class="invalid-feedback">
                  <a
                    v-for="error of v$.widgetInterval.$errors"
                    :key="error.$uid"
                  >
                    {{ error.$message }}
                    <br />
                  </a>
                </div>
              </div>

              <div class="form-group col-5">
                <label for="widgetTemplates" class="font-weight-bold mb-2"
                  >Template</label
                >
                <select
                  id="widgetTemplates"
                  data-testid="widget-edit-template-select"
                  class="form-control"
                  v-model="selectedWidget"
                  @change="changeTemplate"
                  :disabled="showTestWidget"
                >
                  <option value="" disabled>Select Template</option>
                  <option
                    v-for="(widget, index) in widgets"
                    :key="index"
                    :value="widget"
                  >
                    ({{ widget.type }}) {{ widget.title }}
                  </option>
                </select>
              </div>
            </div>

            <Transition>
              <div class="form-row">
                <div
                  v-if="showTestWidget"
                  data-testid="widget-edit-test-wrapper"
                  class="col d-flex justify-content-center"
                >
                  <MonitoringWidget
                    :conn-id="connId"
                    :tab-id="tabId"
                    :database-index="databaseIndex"
                    :monitoring-widget="testWidgetData"
                    :is-test-widget="true"
                  />
                </div>

                <div v-show="!showTestWidget" class="col-6">
                  <div ref="dataEditor" class="custom-editor"></div>
                </div>

                <div v-show="!showTestWidget" class="col-6">
                  <div ref="scriptEditor" class="custom-editor"></div>
                </div>
              </div>
            </Transition>
          </div>

          <div class="modal-footer">
            <button
              v-if="!showTestWidget"
              data-testid="widget-edit-test-button"
              class="btn btn-secondary"
              @click="showTestWidget = true"
            >
              Test
            </button>
            <button
              v-else
              class="btn btn-secondary"
              @click="showTestWidget = false"
            >
              Done
            </button>
            <button
              data-testid="widget-edit-save-button"
              class="btn btn-primary"
              @click="saveMonitoringWidget"
              :disabled="v$.$invalid"
            >
              Save
            </button>
          </div>
        </div>
      </div>
    </div>
  </Teleport>
</template>

<script>
import ace from "ace-builds";
import { settingsStore } from "../stores/stores_initializer";
import axios from "axios";
import { showToast } from "../notification_control";
import MonitoringWidget from "./MonitoringWidget.vue";
import { useVuelidate } from "@vuelidate/core";
import { required, minValue, minLength } from "@vuelidate/validators";

export default {
  name: "MonitoringWidgetEditModal",
  components: {
    MonitoringWidget,
  },
  setup() {
    return {
      v$: useVuelidate({ $lazy: true }),
    };
  },
  props: {
    connId: String,
    databaseIndex: Number,
    modalVisible: Boolean,
    widgetId: Number,
    tabId: String,
  },
  emits: ["modalHide"],
  data() {
    return {
      widgetTypes: ["timeseries", "chart", "grid"],
      widgets: [],
      dataEditor: null,
      scriptEditor: null,
      selectedWidget: "",
      widgetTemplate: null,
      selectedType: "timeseries",
      widgetName: "",
      widgetInterval: "",
      showTestWidget: false,
      testWidgetData: {},
    };
  },
  validations() {
    return {
      widgetName: {
        required,
        minLength: minLength(1),
      },
      widgetInterval: {
        required,
        minValue: minValue(5),
      },
    };
  },
  watch: {
    modalVisible(newValue, oldValue) {
      if (newValue) {
        $(this.$refs.editWidgetModal).modal({
          backdrop: "static",
          keyboard: false,
        });
        this.setupModal();
      }
    },
    showTestWidget(newValue, oldValue) {
      if (newValue) {
        this.testWidgetData = {
          script_chart: this.scriptEditor.getValue(),
          script_data: this.dataEditor.getValue(),
          type: this.selectedType,
        };
      }
    },
  },
  methods: {
    getMonitoringWidgetList() {
      axios
        .post("/monitoring-widgets/list", {
          tab_id: this.connId,
          database_index: this.databaseIndex,
        })
        .then((resp) => {
          this.widgets = resp.data.data;
        })
        .catch((error) => {
          showToast("error", error);
        });
    },
    getMonitoringWidgetDetails() {
      axios
        .get(`/monitoring-widgets/user-created/${this.widgetId}`)
        .then((resp) => {
          this.widgetName = resp.data.title;
          this.widgetInterval = resp.data.interval;
          this.selectedType = resp.data.type;
          this.setDataEditorValue(resp.data.script_data);
          this.setScriptEditorValue(resp.data.script_chart);
        })
        .catch((error) => {
          showToast("error", error);
        });
    },
    setupEditor(editorDiv) {
      const editor = ace.edit(editorDiv);
      editor.setShowPrintMargin(false);
      editor.$blockScrolling = Infinity;
      editor.setTheme(`ace/theme/${settingsStore.editorTheme}`);
      editor.session.setMode("ace/mode/python");
      editor.setFontSize(settingsStore.fontSize);
      editor.commands.bindKey("ctrl-space", null);
      editor.commands.bindKey("Cmd-,", null);
      editor.commands.bindKey("Ctrl-,", null);
      editor.commands.bindKey("Cmd-Delete", null);
      editor.commands.bindKey("Ctrl-Delete", null);
      editor.commands.bindKey("Ctrl-Up", null);
      editor.commands.bindKey("Ctrl-Down", null);
      return editor;
    },
    setScriptEditorValue(value) {
      this.scriptEditor.setValue(value);
      this.scriptEditor.clearSelection();
      this.scriptEditor.gotoLine(0, 0, true);
    },
    setDataEditorValue(value) {
      this.dataEditor.setValue(value);
      this.dataEditor.clearSelection();
      this.dataEditor.gotoLine(0, 0, true);
    },
    changeTemplate() {
      axios
        .post(`/monitoring-widgets/${this.selectedWidget.id}/template`, {
          plugin_name: this.selectedWidget.plugin_name,
        })
        .then((resp) => {
          this.widgetInterval = resp.data.interval;
          this.selectedType = resp.data.type;
          this.setDataEditorValue(resp.data.script_data);
          this.setScriptEditorValue(resp.data.script_chart);
        })
        .catch((error) => {
          showToast("error", error);
        });
    },
    resetToDefault() {
      this.v$.$reset();
      this.scriptEditor.destroy();
      this.dataEditor.destroy();
      this.selectedWidget = "";
      this.widgetTemplate = null;
      this.widgetName = "";
      this.widgetInterval = "";
      this.selectedType = "timeseries";
      this.showTestWidget = false;
    },
    saveMonitoringWidget() {
      this.v$.$validate();
      if (this.v$.$invalid) return;

      if (this.widgetId) {
        this.updateMonitoringWidget();
      } else {
        this.createMonitoringWidget();
      }
    },
    createMonitoringWidget() {
      axios
        .post("/monitoring-widgets/user-created", {
          tab_id: this.connId,
          database_index: this.databaseIndex,
          widget_name: this.widgetName,
          widget_type: this.selectedType,
          widget_interval: this.widgetInterval,
          widget_script_data: this.dataEditor.getValue(),
          widget_script_chart: this.scriptEditor.getValue(),
        })
        .then((resp) => {
          $(this.$refs.editWidgetModal).modal("hide");
          showToast("success", "Monitoring widget created.");
        })
        .catch((error) => {
          showToast("error", error);
        });
    },
    updateMonitoringWidget() {
      axios
        .put(`/monitoring-widgets/user-created/${this.widgetId}`, {
          widget_name: this.widgetName,
          widget_type: this.selectedType,
          widget_interval: this.widgetInterval,
          widget_script_data: this.dataEditor.getValue(),
          widget_script_chart: this.scriptEditor.getValue(),
        })
        .then((resp) => {
          $(this.$refs.editWidgetModal).modal("hide");
          showToast("success", "Monitoring widget updated.");
        })
        .catch((error) => {
          showToast("error", error);
        });
    },
    closeModal() {
      this.resetToDefault();
      this.$emit("modalHide");
    },
    setupModal() {
      this.dataEditor = this.setupEditor(this.$refs.dataEditor);
      this.scriptEditor = this.setupEditor(this.$refs.scriptEditor);
      this.getMonitoringWidgetList();
      if (this.widgetId) {
        this.getMonitoringWidgetDetails();
      }
    },
  },
};
</script>

<style scoped>
.modal-content {
  min-height: calc(100vh - 100px);
}

.custom-editor {
  width: 100%;
  height: calc(100vh - 400px);
}
</style>
