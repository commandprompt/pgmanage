<template>
  <button class="btn btn-sm btn-danger" title="Cancel" @click="cancelSQL()">
    Cancel
  </button>
</template>

<script>
import { emitter } from "../emitter";
import { createRequest, removeContext } from "../long_polling";
import { queryRequestCodes } from "../constants";
import { tabsStore } from "../stores/stores_initializer";
export default {
  props: {
    tabId: String,
    connId: String,
  },
  emits: ['cancelled'],
  methods: {
    cancelSQL() {
      let tab = tabsStore.getSelectedSecondaryTab(this.connId);
      let message_data = { tab_id: this.tabId, conn_tab_id: this.connId };
      let context = {
        tab: tab,
        database_index: this.databaseIndex,
        callback: this.cancelSQLReturn.bind(this),
      }
      removeContext(tab.metaData.context.code);
      createRequest(queryRequestCodes.CancelThread, message_data, context);
    },
    cancelSQLReturn() {
      let tab = tabsStore.getSelectedSecondaryTab(this.connId);
      tab.metaData.isLoading = false;
      tab.metaData.isReady = false;
      this.$emit("cancelled");
    }
  },
  mounted() {
    emitter.on(`${this.tabId}_cancel_query`, () => {
      this.cancelSQL();
    });
  },
  unmounted() {
    emitter.all.delete(`${this.tabId}_cancel_query`);
  },
};
</script>
