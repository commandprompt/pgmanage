<template>
  <SettingsModal />
  <SideBarTabs />
  <PasswordModal />
  <MasterPasswordModal @check-completed="initialSetup" />
  <UtilityJobsJobDetail />
  <template v-if="initialized">
    <ConnectionsModal />
  </template>
  <GenericMessageModal />
  <CellDataModal />
  <FileManager />
  <UtilitiesMenu />
  <AboutModal />
  <CommandsHistoryModal />
  <template v-for="extraComp in enterpriseComps">
    <component :is="extraComp">
    </component>
  </template>
</template>

<script>
import SettingsModal from "./components/SettingsModal.vue";
import SideBarTabs from "./components/SideBarTabs.vue";
import PasswordModal from "./components/PasswordModal.vue";
import MasterPasswordModal from "./components/MasterPasswordModal.vue";
import UtilityJobsJobDetail from "./components/UtilityJobsJobDetail.vue";
import ConnectionsModal from "./components/ConnectionsModal.vue";
import GenericMessageModal from "./components/GenericMessageModal.vue";
import CellDataModal from "./components/CellDataModal.vue";
import FileManager from "./components/FileManager.vue";
import UtilitiesMenu from "./components/UtilitiesMenu.vue";
import AboutModal from './components/AboutModal.vue';
import CommandsHistoryModal from "./components/CommandsHistoryModal.vue";
import { emitter } from "./emitter";
import { startTutorial } from "./tutorial";
import { createOmnis } from "./omnis-control";

export default {
  name: "PgManage",
  components: {
    SettingsModal,
    SideBarTabs,
    PasswordModal,
    MasterPasswordModal,
    UtilityJobsJobDetail,
    ConnectionsModal,
    GenericMessageModal,
    CellDataModal,
    FileManager,
    UtilitiesMenu,
    AboutModal,
    CommandsHistoryModal,
  },
  data() {
    return {
      initialized: false,
      enterpriseComps: []
    };
  },
  mounted() {
    this.createOmnisAssistant();
    // Ask for master password
    if (master_key === "new") {
      emitter.emit("show_master_pass_prompt", true);
    } else if (master_key == "False") {
      emitter.emit("show_master_pass_prompt", false);
    } else {
      this.initialSetup();
    }
  },
  methods: {
    initialSetup() {
      this.initialized = true;
      v_omnis.div.style.opacity = 1;

      this.enterpriseComps = this?.enterpriseComponents ?? []
    },
    createOmnisAssistant() {
      v_omnis = createOmnis();
      v_omnis.root = document.getElementById("app");
      v_omnis.div = document.createElement("div");
      v_omnis.div.setAttribute("id", "omnis");
      v_omnis.div.classList.add("omnis");
      v_omnis.div.style.top =
        v_omnis.root.getBoundingClientRect().height - 45 + "px";
      v_omnis.div.style.left =
        v_omnis.root.getBoundingClientRect().width - 60 + "px";
      v_omnis.div.style["z-index"] = "99999999";
      v_omnis.div.style.opacity = 0;
      v_omnis.div.innerHTML = v_omnis.template;
      document.body.appendChild(v_omnis.div);
      v_omnis.div.addEventListener("click", function () {
        startTutorial("getting_started");
      });
    }
  }
};
</script>
