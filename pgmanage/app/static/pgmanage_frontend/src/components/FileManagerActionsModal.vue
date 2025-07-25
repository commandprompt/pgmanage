<template>
  <div id="file_manager_actions_modal" ref="fileManagerActionsModal" class="modal fade" tabindex="-1" role="dialog" aria-hidden="true">
    <div class="modal-dialog modal-dialog-centered" role="document">
      <div class="modal-content">
        <div class="modal-header text-center pt-2">
          <h2 class="modal-title w-100">{{ modalTitle }}</h2>
        </div>
        <div class="modal-body form-group">
          <p v-if="action === 'rename'">
            <i :class="['fas', 'fa-2xl', { 'fa-folder': file.is_directory, 'fa-file': !file.is_directory }]"
              :style="{ 'color': file.is_directory ? '#0ea5e9' : 'rgb(105 114 118)', }"></i>
            <span class="ms-1">{{ file.file_name }}</span>
          </p>
          <input v-if="action !== 'delete'" type="text" class="form-control" v-model="name">
          <div v-if="action === 'delete'">
            <div class="row align-items-center">
              <div class="col-9">
                <p>Are you sure you want to delete this {{ deleteFileType }}?</p>
              </div>
              <div class="pt-3 text-break text-center col">
                <div class="position-relative">
                  <i :class="['fas', 'fa-2xl', { 'fa-folder': file.is_directory, 'fa-file': !file.is_directory }]"
                    :style="{ 'color': file.is_directory ? '#0ea5e9' : 'rgb(105 114 118)', }"></i>
                </div>
                <span class="text-dark">{{ file.file_name }}</span>
              </div>

            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
          <button v-if="action === 'rename'" type="button" class="btn btn-primary" data-bs-dismiss="modal"
            @click="rename">Rename</button>
          <button v-else-if="action === 'delete'" type="button" class="btn btn-danger" data-bs-dismiss="modal"
            @click="deleteFile">Delete</button>
          <button v-else type="button" class="btn btn-primary" data-bs-dismiss="modal"
            @click="create($event, createdFileType)">Create</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from 'axios'
import { handleError } from '../logging/utils';

export default {
  name: 'ActionsModal',
  props: {
    action: {
      type: String,
      required: true
    },
    file: {
      type: Object,
      required: true
    },
    currentPath: {
      type: String,
      default: null
    }
  },
  data() {
    return {
      name: '',
    }
  },
  emits: ['actionDone'],
  computed: {
    modalTitle() {
      if (this.action === 'addFolder') {
        return 'New folder'
      } else if (this.action == 'addFile') {
        return 'New file'
      } else if (this.action == 'delete') {
        return `Delete ${this.deleteFileType}`
      } else {
        return 'Rename'
      }
    },
    createdFileType() {
      if (this.action === 'addFolder') {
        return 'dir'
      } else {
        return 'file'
      }
    },
    deleteFileType() {
      if (this.file.is_directory) {
        return 'Directory'
      } else {
        return 'File'
      }
    }
  },
  mounted() {
    this.$refs.fileManagerActionsModal.addEventListener('hidden.bs.modal', () => {
      this.name = ''
    })
  },
  methods: {
    rename(event) {
      axios.post('/file_manager/rename/', {
        path: this.file?.path,
        name: this.name
      })
        .then((resp) => {
          this.$emit('actionDone', event, this.name)
        })
        .catch((error) => {
          handleError(error);
        })
    },
    create(event, type) {
      axios.post('/file_manager/create/', {
        path: this.currentPath,
        name: this.name,
        type: type
      })
        .then((resp) => {
          this.$emit('actionDone', event, this.name)
        })
        .catch((error) => {
          handleError(error);
        })
    },
    deleteFile() {
      axios.post('/file_manager/delete/', {
        path: this.file?.path
      })
        .then((resp) => {
          this.$emit('actionDone')
        })
        .catch((error) => {
          handleError(error);
        })
    },

  },

}

</script>