import { defineStore } from "pinia";

const useCellDataModalStore = defineStore("cellDataModal", {
  state: () => ({
    visible: false,
    cellContent: null,
  }),
  actions: {
    showModal(cellContent) {
      this.cellContent = cellContent;
      this.visible = true;
    },
    hideModal() {
      this.visible = false;
      this.cellContent = null;
    },
  },
});

export { useCellDataModalStore };
