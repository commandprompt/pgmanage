<template>
  <Teleport to="body">
    <div
      id="file_manager"
      ref="fileManagerModal"
      class="modal fade"
      tabindex="-1"
      aria-hidden="true"
    >
      <div class="modal-dialog modal-xl modal-file-manager">
        <div class="modal-content">
          <div class="modal-header align-items-center">
            <h2 class="modal-title">File manager</h2>
            <button
              type="button"
              class="btn-close"
              data-bs-dismiss="modal"
              aria-label="Close"
            ></button>
          </div>
          <div class="modal-body p-0" style="height: 60vh">
            <div
              id="actions-tab"
              class="d-flex justify-content-between border-bottom px-4 py-3"
            >
              <div class="btn-group">
                <a
                  data-testid="add-file-button"
                  class="btn btn-outline-secondary btn-sm"
                  title="Add File"
                  @click="openActionsModal('addFile')"
                  ><i class="fas fa-file-circle-plus fa-xl"></i
                ></a>
                <a
                  data-testid="add-folder-button"
                  class="btn btn-outline-secondary btn-sm"
                  title="Add Folder"
                  @click="openActionsModal('addFolder')"
                  ><i class="fas fa-folder-plus fa-xl"></i
                ></a>
                <a
                  data-testid="rename-button"
                  :class="[
                    'btn',
                    'btn-outline-secondary',
                    'btn-sm',
                    { disabled: !Object.keys(selectedFile).length },
                  ]"
                  title="Rename Folder/File"
                  @click="openActionsModal('rename')"
                  ><i class="fas fa-thin fa-file-pen fa-xl"></i
                ></a>
              </div>
              <div>
                <div class="btn-group me-2">
                  <a
                    data-testid="download-button"
                    class="btn btn-outline-secondary btn-sm"
                    :class="{
                      disabled:
                        !Object.keys(selectedFile).length ||
                        selectedFile.is_directory,
                    }"
                    title="Download"
                    @click="onDownload"
                  >
                    <i class="fas fa-download"></i
                  ></a>
                  <a
                    data-testid="upload-button"
                    class="btn btn-outline-secondary btn-sm"
                    :class="{ disabled: !!uploadingFile }"
                    title="Upload"
                    @click="onUpload"
                  >
                    <i class="fas fa-upload"></i
                  ></a>
                </div>
                <div class="btn-group">
                  <a
                    data-testid="delete-file-button"
                    :class="[
                      'btn',
                      'btn-outline-secondary',
                      'btn-sm',
                      { disabled: !Object.keys(selectedFile).length },
                    ]"
                    title="Delete"
                    @click="openActionsModal('delete')"
                    ><i class="fas fa-trash fa-xl"></i
                  ></a>
                </div>
              </div>
            </div>

            <div
              class="d-flex justify-content-between align-items-center border-bottom px-4 py-3"
            >
              <div class="btn-group">
                <a
                  data-testid="step-back-dir-button"
                  :class="[
                    'btn',
                    'btn-outline-secondary',
                    'btn-sm',
                    { disabled: !isChild },
                  ]"
                  title="Click to go back"
                  @click="stepBackDir"
                  ><i class="fas fa-left-long fa-xl"></i
                ></a>
                <a
                  data-testid="refresh-manager-button"
                  class="btn btn-outline-secondary btn-sm"
                  title="Refresh"
                  @click="refreshManager"
                  ><i class="fas fa-refresh fa-xl"></i
                ></a>
                <a
                  data-testid="step-home-dir-button"
                  :class="[
                    'btn',
                    'btn-outline-secondary',
                    'btn-sm',
                    { disabled: !isChild },
                  ]"
                  title="Go back to root directory"
                  @click="stepHomeDir"
                  ><i class="fas fa-house fa-xl"></i
                ></a>
              </div>
              <div class="form-group w-75 mb-0">
                <input
                class="form-control"
                type="text"
                :value="currentPath"
                disabled
              />
              </div>

              <a
                v-if="currentView === 'table'"
                class="btn btn-outline-secondary btn-sm"
                @click="changeView"
                title="Change View"
                ><i class="fas fa-list-ul fa-xl"></i
              ></a>
              <a
                v-else
                class="btn btn-outline-secondary btn-sm"
                @click="changeView"
                title="Change View"
                ><i class="fas fa-grip-horizontal fa-xl"></i
              ></a>
            </div>

            <div v-if="isDirEmpty" class="align-items-center d-flex flex-column h-75 justify-content-center">
              
              <i class="fas fa-regular fa-folder fs-1 text-secondary"></i>
              <span class="mt-2 fw-medium user-select-none">Folder is Empty</span>
            </div>
            <template v-else>
              <!-- Box format for files and folders -->
              <div v-if="isGrid" class="d-flex p-2 flex-wrap files-grid">
                <div
                  v-for="file in files"
                  :key="file.file_name"
                  :class="[
                    'files-grid__item',
                    'text-center',
                    'border-0',
                    'pt-3',
                    'me-2',
                    { active: file === selectedFile },
                  ]"
                  @click="selectFileOrDir(file.file_name)"
                  @dblclick="
                    file.is_directory
                      ? getDirContent(file.path)
                      : confirmSelection()
                  "
                >
                  <div class="position-relative">
                    <i
                      :class="[
                        'fas',
                        'fa-2xl',
                        'me-2',
                        {
                          'fa-folder': file.is_directory,
                          'fa-file': !file.is_directory,
                        },
                      ]"
                      :style="{
                        color: file.is_directory ? '#0ea5e9' : 'rgb(105 114 118)',
                      }"
                    ></i>
                  </div>
                  <p class="clipped-text mt-1">{{ file.file_name }}</p>
                </div>
              </div>
  
              <!-- Table format for files and folders-->
              <div v-else class="file-table">
                <div class="p-0">
                  <ul class="list-group list-group-flush form-group">
                    <li
                      class="list-group-item d-flex row g-0 fw-bold mb-1 border-0 file-table-header bg-transparent"
                    >
                      <div class="col-7">Name</div>
                      <div class="col-2">Size</div>
                      <div class="col-3">Modified</div>
                    </li>
                    <li
                      class="list-group-item d-flex row g-0 mb-1 border-0"
                      :class="{ active: file === selectedFile }"
                      v-for="file in files"
                      :key="file.file_name"
                      @click="selectFileOrDir(file.file_name)"
                      @dblclick="
                        file.is_directory
                          ? getDirContent(file.path)
                          : confirmSelection()
                      "
                    >
                      <div class="col-7">
                        <i
                          :class="[
                            'fas',
                            'fa-2xl',
                            {
                              'fa-folder': file.is_directory,
                              'fa-file': !file.is_directory,
                            },
                          ]"
                          :style="{
                            color: file.is_directory
                              ? '#0ea5e9'
                              : 'rgb(105 114 118)',
                          }"
                        ></i>
                        {{ file.file_name }}
                      </div>
                      <div class="col-2" v-if="!file.is_directory">
                        {{ file.file_size }}
                      </div>
                      <div class="col-2" v-if="file.is_directory">
                        {{ file.dir_size }}
                        {{ file.dir_size == 1 ? "item" : "items" }}
                      </div>
                      <div class="col-3">{{ file.modified }}</div>
                    </li>
                  </ul>
                </div>
              </div>

            </template>
          </div>
          <div class="modal-footer justify-content-between">
            <div class="w-75">
              <div v-if="showLoading" class="d-flex align-items-center">
                <div class="progress w-25">
                  <div
                    class="progress-bar"
                    role="progressbar"
                    :style="{ width: `${uploadProgress}%` }"
                    aria-valuenow="25"
                    aria-valuemin="0"
                    aria-valuemax="100"
                  ></div>
                </div>
                <button
                  type="button"
                  class="btn-close"
                  aria-label="Close"
                  title="Cancel Upload"
                  @click="cancelUpload"
                ></button>
                <span class="fw-bold"
                  >Uploading {{ uploadingFile }}...{{ uploadProgress }}%</span
                >
              </div>
            </div>
            <a
              :class="[
                'btn',
                'btn-secondary',
                'btn-sm',
                'm-0',
                { disabled: !Object.keys(selectedFile).length },
              ]"
              @click="confirmSelection"
            >
              Select</a
            >
          </div>
        </div>
      </div>
    </div>

    <ActionsModal
      :action="action"
      :file="selectedFile"
      :current-path="currentPath"
      ref="actionsModal"
      @action-done="refreshManager"
    />
  </Teleport>
</template>

<script>
import FileManagerActionsModal from "./FileManagerActionsModal.vue";
import axios from "axios";
import { showToast } from "../notification_control";
import { fileManagerStore, settingsStore } from "../stores/stores_initializer";
import { Modal } from "bootstrap";
import { handleError } from "../logging/utils";

export default {
  name: "FileManager",
  components: {
    ActionsModal: FileManagerActionsModal,
  },
  data() {
    return {
      currentPath: null,
      parent: false,
      files: [],
      selectedFile: {},
      action: "",
      currentView: "grid",
      showLoading: false,
      uploadProgress: null,
      uploadingFile: "",
      controller: null,
    };
  },
  computed: {
    isChild() {
      return this.parent;
    },
    isGrid() {
      return this.currentView === "grid";
    },
    isDirEmpty() {
      return this.files.length === 0;
    },
  },
  mounted() {
    fileManagerStore.$onAction(({ name, store, after }) => {
      if (name === "showModal") {
        after(() => {
          this.show(store.desktopMode, store.onChange, store.dialogType);
        });
      }
    });
    this.$refs.fileManagerModal.addEventListener("hide.bs.modal", () => {
      fileManagerStore.hideModal();
    });

    window.addEventListener("beforeunload", this.preventNavigation);
  },
  beforeUnmount() {
    window.removeEventListener("beforeunload", this.preventNavigation);
  },
  methods: {
    preventNavigation(event) {
      if (this.uploadingFile) {
        event.preventDefault();
        event.returnValue =
          "You have an ongoing upload. Are you sure you want to leave?";
        return event.returnValue;
      }
    },
    refreshManager(event, created_file_name = null) {
      if (!!created_file_name) {
        this.getDirContent(this.currentPath, null, created_file_name);
      }
      this.getDirContent(this.currentPath);
    },
    selectFileOrDir(file_name) {
      this.selectedFile = this.files.find(
        (file) => file.file_name === file_name
      );
    },
    changeView() {
      if (this.currentView === "grid") {
        this.currentView = "table";
      } else {
        this.currentView = "grid";
      }
    },
    stepBackDir() {
      this.getDirContent(this.currentPath, this.parent);
    },
    stepHomeDir() {
      this.getDirContent();
    },
    getDirContent(path = null, parent_dir = null, created_file_name = null) {
      axios
        .post("/file_manager/get_directory/", {
          current_path: path,
          parent_dir: parent_dir,
        })
        .then((resp) => {
          this.files = [...resp.data.files];
          this.currentPath = resp.data.current_path;
          this.parent = resp.data.parent;
          this.selectedFile = {};
          if (!!created_file_name) {
            this.selectFileOrDir(created_file_name);
          }
        })
        .catch((error) => {
          handleError(error);
        });
    },
    openActionsModal(action) {
      this.action = action;
      Modal.getOrCreateInstance(this.$refs.actionsModal.$el).show();
    },
    confirmSelection() {
      fileManagerStore.changeFile(this.selectedFile);
      Modal.getOrCreateInstance(this.$refs.fileManagerModal).hide();
    },
    show(desktopMode, onChange, dialog_type) {
      if (desktopMode) {
        this.showNative(onChange, dialog_type);
      } else {
        this.getDirContent();
        Modal.getOrCreateInstance(this.$refs.fileManagerModal).show();
      }
    },
    showNative(onChange, dialog_type) {
      let inputEl = document.createElement("input");
      inputEl.setAttribute("type", "file");
      inputEl.onchange = onChange;
      if (dialog_type === "select_folder") {
        inputEl.setAttribute("nwdirectory", "");
      } else if (dialog_type === "create_file") {
        inputEl.setAttribute("nwsaveas", "");
      }

      inputEl.dispatchEvent(new MouseEvent("click"));
    },
    onDownload() {
      axios
        .post(
          "/file_manager/download/",
          { path: this.selectedFile.path },
          { responseType: "blob" }
        )
        .then((resp) => {
          const url = window.URL.createObjectURL(new Blob([resp.data]));
          const link = document.createElement("a");
          link.href = url;
          link.setAttribute(
            "download",
            this.selectedFile.path.split("/").pop()
          );
          document.body.appendChild(link);
          link.click();
          link.remove();
          window.URL.revokeObjectURL(url);
        })
        .catch((error) => {
          handleError(error);
        });
    },
    async onUploadProgress(event) {
      const file = event.target.files[0];

      if (file.size > settingsStore.max_upload_size) {
        showToast(
          "error",
          `Please select a file that is ${
            settingsStore.max_upload_size / 1024 ** 2
          }MB or less.`
        );
        return;
      }

      const formData = new FormData();
      formData.append("file", file);
      formData.append("path", this.currentPath);

      try {
        this.showLoading = true;
        this.uploadingFile = file.name;
        this.controller = new AbortController();
        const response = await axios.post("/file_manager/upload/", formData, {
          headers: {
            "Content-Type": "multipart/form-data",
          },
          onUploadProgress: (progressEvent) => {
            this.uploadProgress = Math.round(progressEvent.progress * 100);
          },
          signal: this.controller.signal,
        });
        this.showLoading = false;
        this.uploadProgress = null;
        this.uploadingFile = null;
        this.controller = null;
        this.sendNotifyUploadFinished(`File ${file.name} is uploaded.`, () => {
          Modal.getOrCreateInstance(this.$refs.fileManagerModal).show();
        });
        this.getDirContent(this.currentPath, null, file.name);
      } catch (error) {
        this.showLoading = false;
        this.uploadProgress = null;
        this.uploadingFile = null;
        this.controller = null;
        if (axios.isCancel(error)) {
          showToast("info", `Upload of file "${file.name}"" was cancelled.`);
        } else {
          handleError(error);
        }
      }
    },
    onUpload() {
      let inputEl = document.createElement("input");
      inputEl.setAttribute("type", "file");
      inputEl.onchange = this.onUploadProgress;
      inputEl.dispatchEvent(new MouseEvent("click"));
    },
    cancelUpload() {
      if (!!this.controller) this.controller.abort();
    },
    createNotifyMessage(title, desc) {
      return `<div class="v-toast__body p-0">
                  <h3 class="fw-bold">${title}</h3>
                  <p>${desc}</p>
                  <div class="text-end">
                    <button class="btn v-toast__details fw-bold">Open File Manager</button>
                  </div>
              </div>`;
    },
    sendNotifyUploadFinished(desc, onClickProcess) {
      let message = this.createNotifyMessage("File Manager", desc);

      this.$toast.success(message, {
        onClick: onClickProcess,
      });
    },
  },
};
</script>

<style scoped>
.file-table {
  overflow-y: auto;
  height: 100%;
  max-height: calc(100% - 72px);
  position: relative;
}
</style>
