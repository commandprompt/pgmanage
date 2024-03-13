<template>
  <div
    class="omnidb__tab-menu--container omnidb__tab-menu--container--primary omnidb__tab-menu--container--menu-shown"
  >
    <div
      class="omnidb__tab-menu border-bottom omnidb__tab-menu--primary omnidb__theme-bg--menu-primary"
    >
      <nav>
        <div class="nav nav-tabs">
          <a
            :id="tab.id"
            data-toggle="tab"
            :class="[
              'omnidb__tab-menu__link',
              'nav-item',
              'nav-link',
              { disabled: tab.disabled, active: tab.id == selectedTab.id },
            ]"
            role="tab"
            aria-selected="false"
            :aria-controls="`${tab.id}_content`"
            :href="`#${tab.id}_content`"
            :draggable="tab.isDraggable"
            @dragend="tab.dragEndFunction($event, tab)"
            @click.prevent.stop="clickHandler($event, tab)"
            @dblclick="tab.dblClickFunction && tab.dblClickFunction(tab)"
            @contextmenu="contextMenuHandler($event, tab)"
            v-for="tab in tabs"
          >
            <span class="omnidb__tab-menu__link-content">
              <span
                v-if="tab.icon"
                class="omnidb__menu__btn omnidb__tab-menu__link-icon"
                v-html="tab.icon"
              >
              </span>
              <span class="omnidb__tab-menu__link-name">
                <span>{{ tab.name }}</span>
              </span>
            </span>

            <i
              v-if="tab.closable"
              class="fas fa-times tab-icon omnidb__tab-menu__link-close"
              @click.stop.prevent="tab.closeFunction($event, tab)"
            ></i>
          </a>
        </div>
      </nav>
    </div>

    <div class="tab-content omnidb__tab-content omnidb__tab-content--primary">
      <component
        v-for="tab in tabs"
        :key="tab.id"
        :is="tab.component"
        :id="`${tab.id}_content`"
        v-show="tab.id === selectedTab.id || tab.name === 'Snippets'"
        v-bind="getCurrentProps(tab)"
        role="tabpanel"
      ></component>
    </div>
  </div>
</template>

<script>
import { defineAsyncComponent } from "vue";
import { tabsStore } from "../stores/stores_initializer";
import WelcomeScreen from "./WelcomeScreen.vue";
import SnippetPanel from "./SnippetPanel.vue";
import TabsUtils from "../mixins/tabs_utils_mixin.js";

export default {
  name: "SideBarTabs",
  mixins: [TabsUtils],
  components: {
    WelcomeScreen,
    SnippetPanel,
    ConnectionTab: defineAsyncComponent(() => import("./ConnectionTab.vue")),
    TerminalTab: defineAsyncComponent(() => import("./TerminalTab.vue")),
  },
  computed: {
    tabs() {
      return tabsStore.tabs;
    },
    selectedTab() {
      return tabsStore.selectedPrimaryTab;
    },
  },
  mounted() {
    tabsStore.createConnectionsTab();
    tabsStore.createWelcomeTab();
    tabsStore.createSnippetPanel();
  },
  methods: {
    getCurrentProps(tab) {
      const componentsProps = {
        ConnectionTab: {
          "conn-tab-id": tab.id,
        },
        SnippetPanel: {
          "tab-id": tab.id,
        },
        TerminalTab: {
          tabId: tab.id,
          databaseIndex: tab?.metaData?.selectedDatabaseIndex,
        },
      };
      return componentsProps[tab.component];
    },
  },
};
</script>
