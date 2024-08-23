// stores/modal.js
import { defineStore } from "pinia";

const useMessageModalStore = defineStore("messageModal", {
  state: () => ({
    visible: false,
    message: "",
    successFunc: () => {},
    cancelFunc: () => {},
    closable: true,
    checkboxes: [],
  }),
  actions: {
    showModal(
      message,
      successFunc,
      cancelFunc,
      closable = true,
      checkboxes = []
    ) {
      this.message = message;
      this.successFunc = successFunc;
      this.cancelFunc = cancelFunc;
      this.closable = closable;
      this.checkboxes = checkboxes;
      this.visible = true;
    },
    hideModal() {
      this.visible = false;
      this.resetModal();
    },
    executeSuccess() {
      if (!!this.successFunc && typeof this.successFunc === "function") {
        this.successFunc();
      }
      this.hideModal();
    },
    executeCancel() {
      if (!!this.cancelFunc && typeof this.cancelFunc === "function") {
        this.cancelFunc();
      }
      this.hideModal();
    },
    resetModal() {
      this.message = "";
      this.successFunc = () => {};
      this.cancelFunc = () => {};
      this.closable = true;
      this.checkboxes = [];
    },
  },
});

export { useMessageModalStore };
