<template>
  <div class="modal" :id="modalId" tabindex="-1">
    <div class="modal-dialog modal-xl">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 class="modal-title">File manager</h2>
          <button type="button" class="close" data-dismiss="modal" aria-label="Close">
            <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
          </button>
        </div>
        <div class="modal-body p-0" style="height: 60vh">
          <div id="actions-tab" class="d-flex justify-content-between border-bottom px-4 py-3">
            <div class="btn-group">
              <a class="btn btn-outline-secondary btn-sm" title="Add File" @click="openActionsModal('addFile')"><i
                  class="fas fa-file-circle-plus fa-xl"></i></a>
              <a class="btn btn-outline-secondary btn-sm" title="Add Folder" @click="openActionsModal('addFolder')"><i
                  class="fas fa-folder-plus fa-xl"></i></a>
              <a :class="['btn', 'btn-outline-secondary', 'btn-sm', { 'disabled': !Object.keys(selectedFile).length }]"
                title="Rename Folder/File" @click="openActionsModal('rename')"><i
                  class="fas fa-thin fa-file-pen fa-xl"></i></a>
            </div>
            <div>
              <a :class="['btn', 'btn-outline-secondary', 'btn-sm', { 'disabled': !Object.keys(selectedFile).length }]"
              title="Delete" @click="openActionsModal('delete')"><i class="fas fa-trash fa-xl"></i></a>
            </div>
          </div>

          <div class="d-flex justify-content-between align-items-center border-bottom px-4 py-3">
            <div class="btn-group">
              <a :class="['btn', 'btn-outline-secondary', 'btn-sm', { 'disabled': !isChild }]" title="Click to go back"
                @click="stepBackDir"><i class="fas fa-left-long fa-xl"></i></a>
              <a class="btn btn-outline-secondary btn-sm" title="Refresh" @click="refreshManager"><i
                  class="fas fa-refresh fa-xl"></i></a>
              <a :class="['btn', 'btn-outline-secondary', 'btn-sm', { 'disabled': !isChild }]"
                title="Go back to root directory" @click="stepHomeDir"><i class="fas fa-house fa-xl"></i></a>
            </div>
            <input class="w-75 form-control" type="text" :value="currentPath" disabled>
            <a v-if="currentView === 'table'" class="btn btn-outline-secondary btn-sm" @click="changeView"
                title="Change View"><i class="fas fa-list-ul fa-xl"></i></a>
            <a v-else class="btn btn-outline-secondary btn-sm" @click="changeView" title="Change View"><i
                  class="fas fa-grip-horizontal fa-xl"></i></a>
          </div>

          <!-- Box format for files and folders -->
          <div v-if="isGrid" class="d-flex p-2 flex-wrap">
            <div v-for="file in files" :key="file.file_name"
              :class="['text-center', 'btn', 'btn-outline-light', 'border-0', 'pt-3', { 'active': file === selectedFile }]"
              style="height: 55px;width: 80px;" @click="selectFileOrDir(file.file_name)"
              @dblclick="file.file_type === 'dir' ? getDirContent(file.file_path) : confirmSelection()">
              <div class="position-relative">
                <i :class="['fas', 'fa-2xl', 'mr-2', { 'fa-folder': file.file_type === 'dir', 'fa-file': file.file_type === 'file' }]"
                  :style="{ 'color': file.file_type === 'dir' ? '#0ea5e9' : 'rgb(105 114 118)', }"></i>
              </div>
              <p class="text-dark clipped-text">{{ file.file_name }}</p>
            </div>
          </div>

          <!-- Table format for files and folders-->
          <div v-else class="card file-table">
            <div class="card-body p-0">
              <ul class="list-group list-group-flush form-group rounded-0">
                <li class="list-group-item d-flex row no-gutters font-weight-bold">
                  <div class="col-7">Name</div>
                  <div class="col-2">Size</div>
                  <div class="col-3">Modified</div>
                </li>
                <li class="list-group-item d-flex row no-gutters"
                    v-for="file in files" :key="file.file_name" @click="selectFileOrDir(file.file_name)"
                    @dblclick="file.file_type === 'dir' ? getDirContent(file.file_path) : confirmSelection()">
                    <div class="col-7">
                      <i :class="['fas', 'fa-2xl', { 'fa-folder': file.file_type === 'dir', 'fa-file': file.file_type === 'file' }]"
                        :style="{ 'color': file.file_type === 'dir' ? '#0ea5e9' : 'rgb(105 114 118)', }"></i>
                        {{ file.file_name }}
                    </div>
                    <div class="col-2" v-if="file.file_type === 'file'">{{ file.file_size }}</div>
                    <div class="col-2" v-if="file.file_type === 'dir'">{{ file.dir_size }} {{ file.dir_size == 1 ? 'item' : 'items' }}</div>
                    <div class="col-3">{{ file.modified }}</div>
                </li>
              </ul>
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <a :class="['btn', 'btn-secondary', 'btn-sm', 'm-0', { 'disabled': !Object.keys(selectedFile).length }]"
            @click="confirmSelection">
            Select</a>
        </div>
      </div>
    </div>
  </div>

  <ActionsModal :action="action" :file="selectedFile" :current-path="currentPath" ref="actionsModal"
    @action-done="refreshManager" />
</template>

<script>
import FileManagerActionsModal from './FileManagerActionsModal.vue'

export default {
  name: 'FileManager',
  components: {
    ActionsModal: FileManagerActionsModal
  },
  data() {
    return {
      currentPath: null,
      parent: false,
      files: [],
      selectedFile: {},
      action: '',
      currentView: 'grid',
      modalId: `${v_connTabControl.selectedTab.tag.tabControl.selectedTab.id}_filemanager`
    }
  },
  emits: ['changeFile'],
  computed: {
    isChild() {
      return this.parent
    },
    isGrid() {
      return this.currentView === 'grid'
    },
  },
  mounted() {
    if (!window.gv_desktopMode)
      this.getDirContent()
  },
  methods: {
    refreshManager(event, created_file_name = null) {
      if (!!created_file_name) {
        this.getDirContent(this.currentPath, null, created_file_name)
      }
      this.getDirContent(this.currentPath)
    },
    selectFileOrDir(file_name) {
      this.selectedFile = this.files.find((file) => file.file_name === file_name)
    },
    changeView() {
      if (this.currentView === 'grid') {
        this.currentView = 'table'
      } else {
        this.currentView = 'grid'
      }

    },
    stepBackDir() {
      this.getDirContent(this.currentPath, this.parent)
    },
    stepHomeDir() {
      this.getDirContent()
    },
    getDirContent(path = null, parent_dir = null, created_file_name = null) {
      axios.post('/file_manager/get_directory/', {
        current_path: path,
        parent_dir: parent_dir
      })
        .then((resp) => {
          this.files = [...resp.data.files]
          this.currentPath = resp.data.current_path
          this.parent = resp.data.parent
          this.selectedFile = {}
          if (!!created_file_name) {
            this.selectFileOrDir(created_file_name)
          }
        })
        .catch((error) => {
          console.log(error)
        })
    },
    openActionsModal(action) {
      this.action = action
      $(this.$refs.actionsModal.$el).modal('show')
    },
    confirmSelection() {
      this.$emit('changeFile', {
        filePath: this.selectedFile.file_path
      })
      $(`#${this.modalId}`).modal('hide')
    },
    show(desktop_mode, onChange, file_format) {
      if (desktop_mode) {
        this.showNative(onChange, file_format)
      } else {
        $(`#${this.modalId}`).modal('show')
      }
    },
    showNative(onChange, file_format) {
      let inputEl = document.createElement('input');
      inputEl.setAttribute('type', 'file')
      inputEl.onchange = onChange
      if (file_format === 'directory') {
        inputEl.setAttribute('nwdirectory', '')
      } else {
        inputEl.setAttribute('nwsaveas', '')
      }

      inputEl.dispatchEvent(new MouseEvent('click'))
    }
  }
}
</script>

<style scoped>
.file-table {
  overflow-y: auto;
    height: 100%;
    max-height: calc(100% - 72px);
    position: relative;
}
</style>