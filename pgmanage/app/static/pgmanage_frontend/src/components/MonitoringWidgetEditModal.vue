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
              class="modal-title fw-bold"
            >
              Monitoring Widget
            </h2>
            <button
              type="button"
              class="btn-close"
              data-bs-dismiss="modal"
              aria-label="Close"
              @click="closeModal"
            >
            </button>
          </div>

          <div class="modal-body">
            <div ref="topToolbar" class="row mt-3">
              <div class="form-group col-3">
                <label for="widgetName" class="fw-bold mb-2"
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

              <div class="form-group col-3">
                <label for="widgetType" class="fw-bold mb-2"
                  >Type</label
                >
                <select
                  id="widgetType"
                  class="form-select"
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

              <div class="form-group col-3">
                <label for="refreshInterval" class="fw-bold mb-2"
                  >Refresh Interval</label
                >
                <select
                  id="widgetInterval"
                  class="form-select"
                  v-model="widgetInterval"
                  :disabled="showTestWidget"
                >
                  <option
                    v-for="(option, index) in refreshIntervalOptions"
                    :key="index"
                    :value="option"
                  >
                    {{ humanizeDuration(option) }}
                  </option>
                </select>
              </div>

              <div class="form-group col-3">
                <label for="widgetTemplates" class="fw-bold mb-2"
                  >Template</label
                >
                <select
                  id="widgetTemplates"
                  data-testid="widget-edit-template-select"
                  :class="[
                    'form-select',
                  ]"
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
              <div class="row">
                <div
                  v-if="showTestWidget"
                  data-testid="widget-edit-test-wrapper"
                  class="col d-flex justify-content-center"
                >
                  <MonitoringWidget
                    :workspace-id="workspaceId"
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

          <div ref="bottomToolbar" class="modal-footer">
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
              v-if="!showTestWidget"
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
import { settingsStore } from "../stores/stores_initializer";
import axios from "axios";
import { showToast } from "../notification_control";
import MonitoringWidget from "./MonitoringWidget.vue";
import { useVuelidate } from "@vuelidate/core";
import { required, minValue, minLength } from "@vuelidate/validators";
import { Modal } from "bootstrap";
import { handleError } from "../logging/utils";
import HumanizeDurationMixin from '../mixins/humanize_duration_mixin';

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
  mixins: [HumanizeDurationMixin],
  props: {
    workspaceId: String,
    databaseIndex: Number,
    modalVisible: Boolean,
    widgetId: Number,
    tabId: String,
  },
  emits: ["modalHide", "widgetCreated"],
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
      widgetInterval: 5,
      showTestWidget: false,
      testWidgetData: {},
      heightSubtract: 150,
      modalInstance: null,
      refreshIntervalOptions: [5, 10, 30, 60, 120, 300]
    };
  },
  computed: {
    gridHeight() {
      return `calc(100vh - ${this.heightSubtract}px)`;
    },
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
        this.modalInstance = Modal.getOrCreateInstance(this.$refs.editWidgetModal, {
          backdrop: "static",
          keyboard: false,
        })
        this.modalInstance.show();
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
          workspace_id: this.workspaceId,
          database_index: this.databaseIndex,
        })
        .then((resp) => {
          this.widgets = resp.data.data;
        })
        .catch((error) => {
          handleError(error);
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
          handleError(error);
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
          handleError(error);
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
          workspace_id: this.workspaceId,
          database_index: this.databaseIndex,
          widget_name: this.widgetName,
          widget_type: this.selectedType,
          widget_interval: this.widgetInterval,
          widget_script_data: this.dataEditor.getValue(),
          widget_script_chart: this.scriptEditor.getValue(),
        })
        .then((resp) => {
          this.resetToDefault();
          this.modalInstance.hide();
          this.$emit("modalHide");
          this.$emit("widgetCreated", resp.data)
          showToast("success", "Monitoring widget created.");
        })
        .catch((error) => {
          handleError(error);
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
          this.resetToDefault();
          this.modalInstance.hide();
          this.$emit("modalHide");
          showToast("success", "Monitoring widget updated.");
        })
        .catch((error) => {
          handleError(error);
        });
    },
    closeModal() {
      this.resetToDefault();
      this.$emit("modalHide");
    },
    setupModal() {
      this.handleResize();
      this.dataEditor = this.setupEditor(this.$refs.dataEditor);
      this.scriptEditor = this.setupEditor(this.$refs.scriptEditor);
      settingsStore.$subscribe((mutation, state) => {
        this.dataEditor.setTheme(`ace/theme/${state.editorTheme}`);
        this.dataEditor.setFontSize(state.fontSize);
        this.scriptEditor.setTheme(`ace/theme/${state.editorTheme}`);
        this.scriptEditor.setFontSize(state.fontSize);
      });
      this.getMonitoringWidgetList();
      if (this.widgetId) {
        this.getMonitoringWidgetDetails();
      }
    },
    handleResize() {
      if (this.$refs === null) return;

      this.heightSubtract =
        this.$refs.bottomToolbar.getBoundingClientRect().height +
        this.$refs.topToolbar.getBoundingClientRect().bottom +
        50;
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
  height: v-bind(gridHeight);
}
</style>
