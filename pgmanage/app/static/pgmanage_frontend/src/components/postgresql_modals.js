import { createApp, defineAsyncComponent } from "vue";
import { tabsStore } from "../stores/stores_initializer";


function createExtensionModal(node, mode) {
  const wrap_div = document.getElementById("extension-modal-wrap");

  wrap_div.innerHTML = `<extension-modal :mode=mode :tree-node=treeNode :workspace-id=workspaceId :database-index=databaseIndex></extension-modal>`;

  const app = createApp({
    components: {
      "extension-modal": defineAsyncComponent(() => import("@src/components/ExtensionModal.vue")),
    },
    data() {
      return {
        mode: mode,
        treeNode: node,
        workspaceId: tabsStore.selectedPrimaryTab.id,
        databaseIndex: tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex
      };
    },
    mounted() {
      setTimeout(() => {
        let extensionModalEl = document.getElementById("postgresqlExtensionModal");
        let messageModalEl = document.getElementById("generic_modal_message");
        extensionModalEl.addEventListener("hidden.bs.modal", () => {
          app.unmount();
        });
        messageModalEl.addEventListener("hidden.bs.modal", () => {
          app.unmount();
        });
      }, 500);
    },
  });
  app.mount(`#extension-modal-wrap`);
}

function createPgCronModal(node, mode) {
  const wrap_div = document.getElementById("pgcron-modal-wrap");

  wrap_div.innerHTML = `<pgcron-modal :mode=mode :tree-node=treeNode :database-index="databaseIndex" :workspace-id="workspaceId"></pgcron-modal>`;

  const app = createApp({
    components: {
      "pgcron-modal": defineAsyncComponent(() => import("@src/components/PgCronModal.vue")),
    },
    data() {
      return {
        mode: mode,
        treeNode: node,
        databaseIndex:
          tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
        workspaceId: tabsStore.selectedPrimaryTab.id,
      };
    },
    mounted() {
      setTimeout(() => {
        let pgCronModalEl = document.getElementById("pgCronModal")
        pgCronModalEl.addEventListener("hidden.bs.modal", () => {
          app.unmount();
        });
      }, 500);
    },
  });
  app.mount(`#pgcron-modal-wrap`);
}

function createRoleModal(node, mode, version) {
  const wrap_div = document.getElementById("role-modal-wrap");

  wrap_div.innerHTML = `<role-modal :mode=mode :tree-node=treeNode :database-index="databaseIndex" :workspace-id="workspaceId" :version="version"></role-modal>`;

  const app = createApp({
    components: {
      "role-modal": defineAsyncComponent(() => import("@src/components/RoleModal.vue")),
    },
    data() {
      return {
        mode: mode,
        treeNode: node,
        databaseIndex:
        tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
        workspaceId: tabsStore.selectedPrimaryTab.id,
        version: version
      };
    },
    mounted() {
      setTimeout(() => {
        let roleModalEl = document.getElementById("roleModal")
        roleModalEl.addEventListener("hidden.bs.modal", () => {
          app.unmount();
        });
      }, 500);
    },
  });
  app.mount(`#role-modal-wrap`);
}

export { createExtensionModal, createPgCronModal, createRoleModal };
