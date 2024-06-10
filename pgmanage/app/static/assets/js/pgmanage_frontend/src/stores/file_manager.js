import { defineStore } from "pinia";

const useFileManagerStore = defineStore("fileManager", {
  state: () => ({
    onChange: () => {},
    dialogType: null,
    filePath: null,
    visible: false,
    desktopMode: null,
  }),
  actions: {
    showModal(desktopMode, onChange, dialogType) {
      this.visible = true;
      this.desktopMode = desktopMode;
      this.onChange = onChange;
      this.dialogType = dialogType;
    },
    changeFile(filePath) {
      this.filePath = filePath;
    },
    hideModal() {
      this.visible = false;
      this.reset();
    },
    reset() {
      this.onChange = () => {};
      this.dialogType = null;
      this.filePath = null;
      this.desktopMode = null;
    },
  },
});

export { useFileManagerStore };
