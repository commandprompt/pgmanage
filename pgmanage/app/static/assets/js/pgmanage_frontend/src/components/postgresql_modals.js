import { createApp } from "vue";
import UtilityJobsJobDetail from "./UtilityJobsJobDetail.vue";
import cronLight from "@vue-js-cron/light";
import ExtensionModal from "../components/ExtensionModal.vue";
import PgCronModal from "../components/PgCronModal.vue";

const jobDetailModal = createApp({
  components: {
    "job-detail": UtilityJobsJobDetail,
  },
});

jobDetailModal.mount("#utility-job-detail-wrap");

function createExtensionModal(node, mode) {
  const wrap_div = document.getElementById("extension-modal-wrap");

  wrap_div.innerHTML = `<extension-modal :mode=mode :tree-node=treeNode :tab-id=tabId :database-index=databaseIndex></extension-modal>`;

  const app = createApp({
    components: {
      "extension-modal": ExtensionModal,
    },
    data() {
      return {
        mode: mode,
        treeNode: node,
        tabId: window.v_connTabControl.selectedTab.id,
        databaseIndex: window.v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
      };
    },
    mounted() {
      setTimeout(() => {
        $("#postgresqlExtensionModal").on("hidden.bs.modal", () => {
          app.unmount();
        });
        $("#generic_modal_message").on("hidden.bs.modal", () => {
          app.unmount();
        });
      }, 500);
    },
  });
  app.mount(`#extension-modal-wrap`);
}

function createPgCronModal(node, mode) {
  // vuejs keep track of installed plugins, but since we reinstantiate app each time
  // we need to reset this flag in order to make the component work on next modal show
  cronLight.install.installed = false;
  const wrap_div = document.getElementById("pgcron-modal-wrap");

  wrap_div.innerHTML = `<pgcron-modal :mode=mode :tree-node=treeNode></pgcron-modal>`;

  const app = createApp({
    components: {
      "pgcron-modal": PgCronModal,
    },
    data() {
      return {
        mode: mode,
        treeNode: node,
      };
    },
    mounted() {
      setTimeout(() => {
        $("#pgCronModal").on("hidden.bs.modal", () => {
          app.unmount();
        });
      }, 500);
    },
  });
  app.use(cronLight);
  app.mount(`#pgcron-modal-wrap`);
}

export { createExtensionModal, createPgCronModal };
