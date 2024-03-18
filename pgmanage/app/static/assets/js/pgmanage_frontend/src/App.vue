<template>
  <SettingsModal />
  <SideBarTabs />
  <PasswordModal />
</template>

<script>
import SettingsModal from "./components/SettingsModal.vue";
import SideBarTabs from "./components/SideBarTabs.vue";
import { passwordModalsInit } from "./passwords.js";
import PasswordModal from "./components/PasswordModal.vue";
export default {
  name: "PgManage",
  components: {
    SettingsModal,
    SideBarTabs,
    PasswordModal,
  },
  mounted() {
    passwordModalsInit();
  },
};
</script>
