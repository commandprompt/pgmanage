import { defineStore } from "pinia";

const useCommandsHistoryStore = defineStore("commandsHistoryModal", {
  state: () => ({
    visible: false,
    tabId: null,
    databaseIndex: null,
    tabType: null,
  }),
  actions: {
    showModal(tabId, databaseIndex, tabType) {
      this.tabId = tabId;
      this.databaseIndex = databaseIndex;
      this.tabType = tabType;
      this.visible = true;
    },
    reset() {
      this.tabId = null;
      this.databaseIndex = null;
      this.tabType = null;
      this.visible = false;
    },
  },
});

export { useCommandsHistoryStore };
