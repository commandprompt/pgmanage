<template>
  <button class="btn btn-sm btn-danger" title="Cancel" @click="cancelSQL()">
    Cancel
  </button>
</template>

<script>
import { emitter } from "../emitter";
import { createRequest, removeContext, SetAcked } from "../long_polling";
import { queryRequestCodes } from "../constants";
export default {
  props: {
    tabId: String,
    connId: String,
  },
  methods: {
    cancelSQL() {
      let tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
      let message_data = { tab_id: this.tabId, conn_tab_id: this.connId };

      createRequest(queryRequestCodes.CancelThread, message_data);

      removeContext(tab_tag.context.v_context_code);

      SetAcked(tab_tag.context);

      //FIXME: change into event emitting later
      tab_tag.tab_loading_span.style.visibility = "hidden";
      tab_tag.tab_check_span.style.display = "none";

      this.$emit("cancelled");
    },
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
