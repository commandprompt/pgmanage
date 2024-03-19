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

export default {
  name: "PgManage",
  components: {
    SettingsModal,
    SideBarTabs,
    PasswordModal,
    MasterPasswordModal
  },
};
</script>
