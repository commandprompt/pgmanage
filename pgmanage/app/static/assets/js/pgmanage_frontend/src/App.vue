<template>
  <SettingsModal />
  <SideBarTabs />
  <PasswordModal />
  <MasterPasswordModal />
</template>

<script>
import SettingsModal from "./components/SettingsModal.vue";
import SideBarTabs from "./components/SideBarTabs.vue";
import PasswordModal from "./components/PasswordModal.vue";
import MasterPasswordModal from './components/MasterPasswordModal.vue'
import { emitter } from "./emitter";
import { conn_app, connectionsModalInit } from "./connections_modal";

export default {
  name: "PgManage",
  components: {
    SettingsModal,
    SideBarTabs,
    PasswordModal,
    MasterPasswordModal
  },
  mounted() {
    // Ask for master password
    if (master_key === 'new') {
      emitter.emit("show_master_pass_prompt", true)
    } else if (master_key == 'False') {
      emitter.emit("show_master_pass_prompt", false)
    } else {
      conn_app.mount("#connections-modal-wrap");
      setTimeout(() => {
        v_omnis.div.style.opacity = 1
      }, 100)
    }

    connectionsModalInit()
  }
};
</script>
