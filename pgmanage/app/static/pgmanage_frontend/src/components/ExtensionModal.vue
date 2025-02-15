<template>
  <div class="modal fade" id="postgresqlExtensionModal" tabindex="-1" role="dialog" aria-hidden="true">
    <div class="modal-dialog" role="document">
      <div class="modal-content">

        <div class="modal-header align-items-center">
          <h2 class="modal-title fw-bold">{{ modalTitle }}</h2>
          <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
        </div>

        <div class="modal-body">
          <div class="form-group mb-2">
            <label for="extensionName" class="fw-bold mb-1">Name</label>
            <select id="extensionName" class="form-select" v-model="selectedExtension"
              :disabled="mode === operationModes.UPDATE">
              <option value="" disabled>Select an item...</option>
              <option v-for="(extension, index) in availableExtensions" :value="extension" :key="index">{{ extension.name
              }}</option>
            </select>
          </div>

          <div class="form-group mb-2">
            <label for="extensionComment" class="fw-bold mb-1">Comment</label>
            <textarea class="form-control" id="extensionComment" disabled
              :value="selectedExtension?.comment"></textarea>
          </div>

          <div class="form-group mb-2">
            <label for="extensionSchema" class="fw-bold mb-1">Schema</label>
            <select id="extensionSchema" class="form-select" v-model="selectedSchema"
              :disabled="!!requiredSchema || !isRelocatable">
              <option value="" disabled="">Select an item...</option>
              <option v-for="(schema, index) in schemaList" :value="schema.name_raw" :key="index">{{ schema.name }}</option>
            </select>
          </div>

          <div class="form-group mb-2">
            <label for="extensionVersions" class="fw-bold mb-1">Version</label>
            <select id="extensionVersions" class="form-select" v-model="selectedVersion">
              <option value="" disabled>Select an item...</option>
              <option v-for="(version, index) in selectedExtension?.versions" :value="version" :key="index">{{ version }}
              </option>
            </select>
          </div>

          <div class="form-group mb-2">
            <p class="fw-bold mb-1">Preview</p>
            <div id="generated_sql_div" style="height: 10vh">
            </div>
          </div>
        </div>

        <div class="modal-footer">
          <button type="button" class="btn btn-primary me-2" :disabled="!selectedExtension || noUpdates"
            @click="saveExtension">
            Save
          </button>
        </div>
      </div>
    </div>
  </div>
</template>


<script>
import { emitter } from '../emitter'
import axios from 'axios'
import { showToast } from '../notification_control'
import { settingsStore, messageModalStore } from '../stores/stores_initializer';
import { operationModes } from '../constants';
import { Modal } from 'bootstrap';

export default {
  name: 'ExtensionModal',
  props: {
    mode: operationModes,
    treeNode: Object,
    workspaceId: String,
    databaseIndex: Number
  },
  data() {
    return {
      availableExtensions: [],
      schemaList: '',
      selectedExtension: '',
      selectedSchema: '',
      selectedVersion: '',
      editor: '',
      modalInstance: null
    }
  },
  computed: {
    generatedSQL() {
      if (!!this.selectedExtension && this.mode === operationModes.CREATE) {
        const schema = !!this.selectedSchema ? `\n    SCHEMA ${this.selectedSchema}` : ''
        const version = !!this.selectedVersion ? `\n    VERSION "${this.selectedVersion}"` : ''
        const name = this.selectedExtension.name.includes('-') ? `"${this.selectedExtension.name}"` : this.selectedExtension.name
        return `CREATE EXTENSION ${name}${schema}${version};`
      } else if (this.mode === operationModes.UPDATE && !!this.selectedExtension) {
        const name = this.selectedExtension.name.includes('-') ? `"${this.selectedExtension.name}"` : this.selectedExtension.name
        const clauses = [];
        if (this.selectedSchema !== this.selectedExtension?.schema) {
          clauses.push(`ALTER EXTENSION ${name}\n    SET SCHEMA ${this.selectedSchema};`)
        }
        if (this.selectedVersion !== this.selectedExtension?.version) {
          clauses.push(`ALTER EXTENSION ${name}\n    UPDATE TO "${this.selectedVersion}";`)
        }
        if (clauses.length > 0) {
          return clauses.join('\n')
        } else {
          return '-- No updates.'
        }
      } else {
        return '-- No updates.'
      }
    },
    requiredSchema() {
      if (!!this.selectedExtension?.required_schema) {
        return this.selectedExtension?.required_schema
      }
    },
    isRelocatable() {
      return this.mode === operationModes.CREATE || (this.mode === operationModes.UPDATE && this.selectedExtension.relocatable);
    },
    modalTitle() {
      if (this.mode === operationModes.UPDATE) return this.selectedExtension.name
      return 'Create Extension'
    },
    noUpdates() {
      return this.generatedSQL === '-- No updates.'
    }
  },
  watch: {
    requiredSchema(newValue) {
      this.selectedSchema = newValue ? newValue : ''
    },
    selectedExtension() {
      if (this.mode === operationModes.CREATE) this.selectedVersion = ''
    },
    generatedSQL() {
      this.editor.setValue(this.generatedSQL)
      this.editor.clearSelection();
    }
  },
  created() {
    // allows for using operationModes in the template
    this.operationModes = operationModes
  },
  mounted() {
    this.getAvailableExtensions()
    this.getSchemas()
    if (this.mode === operationModes.UPDATE) {
      this.getExtensionDetails()
    }
    this.setupEditor()
    if (this.mode !== operationModes.DELETE) {
      this.modalInstance = new Modal('#postgresqlExtensionModal')
      this.modalInstance.show()
    } else {
      messageModalStore.showModal(`Are you sure you want to drop extension '${this.treeNode.title}'?`, this.dropExtension, null, true, [{ 'label': 'CASCADE', 'checked': false }])
    }
  },
  methods: {
    getAvailableExtensions() {
      axios.post('/get_available_extensions_postgresql/', {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
      })
        .then((resp) => {
          this.availableExtensions.push(...resp.data.available_extensions)
        })
        .catch((error) => {
          console.log(error)
        })
    },
    getSchemas() {
      axios.post('/get_schemas_postgresql/', {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
      })
        .then((resp) => {
          this.schemaList = resp.data
        })
        .catch((error) => {
          showToast("error", error.response.data.data)
        })
    },
    saveExtension() {
      axios.post('/execute_query_postgresql/', {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
        query: this.generatedSQL
      })
        .then((resp) => {
          emitter.emit(`refreshTreeRecursive_${this.workspaceId}`, "extension_list");
          this.modalInstance.hide()
        })
        .catch((error) => {
          showToast("error", error.response.data.data)
        })
    },
    setupEditor() {
      this.editor = ace.edit('generated_sql_div');
      this.editor.setTheme("ace/theme/" + settingsStore.editorTheme);
      this.editor.session.setMode("ace/mode/sql");
      this.editor.setFontSize(Number(settingsStore.fontSize));
      this.editor.setReadOnly(true);
      this.editor.$blockScrolling = Infinity;

      this.editor.setValue(this.generatedSQL)
      this.editor.clearSelection();
    },
    getExtensionDetails() {
      axios.post('/get_extension_details/', {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
        ext_name: this.treeNode.title
      })
        .then((resp) => {
          this.availableExtensions.push(resp.data)
          this.selectedExtension = resp.data
          this.selectedSchema = resp.data.schema
          this.selectedVersion = resp.data.version
        })
        .catch((error) => {
          showToast("error", error.response.data.data)
        })
    },
    dropExtension() {
      const checkedValues = messageModalStore.checkboxes
      const cascade = checkedValues[0].checked ? 'CASCADE' : ''
      const query = `DROP EXTENSION IF EXISTS "${this.treeNode.title}" ${cascade};`
      axios.post('/execute_query_postgresql/', {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
        query: query
      })
        .then((resp) => {
          emitter.emit(`refreshTreeRecursive_${this.workspaceId}`, "extension_list");
        })
        .catch((error) => {
          showToast("error", error.response.data.data)
        })
    }

  },
}
</script>