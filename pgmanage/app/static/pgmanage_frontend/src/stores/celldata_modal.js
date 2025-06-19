import { defineStore } from "pinia";

const useCellDataModalStore = defineStore("cellDataModal", {
  state: () => ({
    visible: false,
    cellContent: null,
    cellType: null,
    showControls: false,
  }),
  actions: {
    showModal(cellContent, cellType, showControls) {
      this.cellContent = cellContent;
      this.cellType = cellType;
      this.visible = true;
      this.showControls = showControls ?? false;
    },
    hideModal() {
      this.visible = false;
      this.cellContent = null;
      this.cellType = null;
    },
  },
});

export { useCellDataModalStore };
