<template>
  <SettingsModal />
  <SideBarTabs />
  <PasswordModal />
  <MasterPasswordModal @check-completed="initialSetup" />
  <UtilityJobsJobDetail />
  <template v-if="initialized">
    <ConnectionsModal />
  </template>
</template>

<script>
import SettingsModal from "./components/SettingsModal.vue";
import SideBarTabs from "./components/SideBarTabs.vue";
import PasswordModal from "./components/PasswordModal.vue";
import MasterPasswordModal from './components/MasterPasswordModal.vue'
import UtilityJobsJobDetail from "./components/UtilityJobsJobDetail.vue";
import ConnectionsModal from './components/ConnectionsModal.vue'
import { emitter } from "./emitter";

export default {
  name: "PgManage",
  components: {
    SettingsModal,
    SideBarTabs,
    PasswordModal,
    MasterPasswordModal,
    UtilityJobsJobDetail,
    ConnectionsModal
  },
  data() {
    return {
      initialized: false
    }
  },
  mounted() {
    // Ask for master password
    if (master_key === 'new') {
      emitter.emit("show_master_pass_prompt", true)
    } else if (master_key == 'False') {
      emitter.emit("show_master_pass_prompt", false)
    } else {
      this.initialized = true
      setTimeout(() => {
        v_omnis.div.style.opacity = 1
      }, 100)
    }
  },
  methods: {
    initialSetup() {
      this.initialized = true
      v_omnis.div.style.opacity = 1
    }
  }
};
</script>
