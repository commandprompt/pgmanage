const jobDetailModal = createApp({
  components: {
    "job-detail": Vue.defineAsyncComponent(() =>
      loadModule(
        "/static/assets/js/vuejs/components/UtilityJobsJobDetail.vue",
        options
      )
    ),
  },
});

jobDetailModal.mount("#utility-job-detail-wrap");

function createExtensionModal(node, mode) {
  const wrap_div = document.getElementById("extension-modal-wrap");

  wrap_div.innerHTML = `<extension-modal :mode=mode :tree-node=treeNode></extension-modal>`;

  const app = createApp({
    components: {
      "extension-modal": Vue.defineAsyncComponent(() =>
        loadModule(
          "../static/assets/js/vuejs/components/ExtensionModal.vue",
          options
        )
      ),
    },
    data() {
      return {
        mode: mode,
        treeNode: node,
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
  let cronLight = light //exported from light.min.js
  // FIXME: imprort cronlight properly
  // vuejs keep track of installed plugins, but since we reinstantiate app each time
  // we need to reset this flag in order to make the component work on next modal show
  light.install.installed = false

  const wrap_div = document.getElementById("pgcron-modal-wrap");

  wrap_div.innerHTML = `<pgcron-modal :mode=mode :tree-node=treeNode></pgcron-modal>`;

  const app = createApp({
    components: {
      "pgcron-modal": Vue.defineAsyncComponent(() =>
        loadModule(
          "../static/assets/js/vuejs/components/PgCronModal.vue",
          options
        )
      ),
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