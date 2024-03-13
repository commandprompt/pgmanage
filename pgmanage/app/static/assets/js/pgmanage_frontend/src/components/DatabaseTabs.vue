<template>
  <div
    class="omnidb__tab-menu--container omnidb__tab-menu--container--secondary omnidb__tab-menu--container--menu-shown"
  >
    <div
      class="omnidb__tab-menu border-bottom omnidb__tab-menu--secondary omnidb__theme-bg--menu-secondary"
    >
      <nav class="d-flex align-items-center justify-content-between">
        <div class="d-flex flex-nowrap scrollable-outer">
          <div
            ref="navTabs"
            class="nav nav-tabs text-nowrap flex-nowrap scrollable-inner"
            @wheel="handleNavWheel"
            @scrollend="updateScrollOptions"
          >
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
                  <span
                    v-if="tab.metaData.mode !== 'add'"
                    :class="{ invisible: !tab.metaData.isLoading }"
                  >
                    <i class="tab-icon node-spin"></i>
                  </span>
                  <i
                    :class="[
                      'fas',
                      'fa-check-circle',
                      'tab-icon',
                      'icon-check',
                      { 'd-none': !tab.metaData.isReady },
                    ]"
                  ></i>
                </span>
              </span>

              <i
                v-if="tab.closable"
                class="fas fa-times tab-icon omnidb__tab-menu__link-close"
                @click.stop.prevent="tab.closeFunction($event, tab)"
              ></i>
            </a>
          </div>
          <button
            data-testid="add-tab-button"
            style="width: 40px"
            class="flex-shrink-0 btn btn-icon btn-icon-primary navigation-add"
            @click="addTab"
          >
            <i class="fas fa-lg fa-plus"></i>
          </button>
        </div>

        <div class="navigation-actions d-flex flex-nowrap">
          <button
            class="btn btn-icon btn-icon-secondary mr-3"
            :disabled="!canScrollLeft"
            @click="handleLeftScrollClick"
          >
            <i class="fas fa-lg fa-chevron-left"></i>
          </button>
          <button
            class="btn btn-icon btn-icon-secondary mr-2"
            :disabled="!canScrollRight"
            @click="handleRightScrollClick"
          >
            <i class="fas fa-lg fa-chevron-right"></i>
          </button>
        </div>
      </nav>
    </div>

    <div
      class="`tab-content omnidb__tab-content omnidb__tab-content--secondary"
    >
      <component
        v-for="tab in tabs"
        :key="tab.id"
        :is="tab.component"
        :id="`${tab.id}_content`"
        v-show="tab.id === selectedTab.id"
        v-bind="getCurrentProps(tab)"
        role="tabpanel"
      ></component>
    </div>
  </div>
</template>

<script>
import { defineAsyncComponent } from "vue";
import { tabsStore } from "../stores/stores_initializer";
import ContextMenu from "@imengyu/vue3-context-menu";
import SnippetTab from "./SnippetTab.vue";
import TabsUtils from "../mixins/tabs_utils_mixin";

export default {
  name: "DatabaseTabs",
  mixins: [TabsUtils],
  components: {
    ConsoleTab: defineAsyncComponent(() => import("./ConsoleTab.vue")),
    QueryTab: defineAsyncComponent(() => import("./QueryTab.vue")),
    MonitoringDashboard: defineAsyncComponent(() =>
      import("./MonitoringDashboard.vue")
    ),
    ConfigTab: defineAsyncComponent(() => import("./ConfigTab.vue")),
    BackupTab: defineAsyncComponent(() => import("./BackupTab.vue")),
    RestoreTab: defineAsyncComponent(() => import("./RestoreTab.vue")),
    ERDTab: defineAsyncComponent(() => import("./ERDTab.vue")),
    DataEditorTab: defineAsyncComponent(() => import("./DataEditorTab.vue")),
    SchemaEditorTab: defineAsyncComponent(() =>
      import("./SchemaEditorTab.vue")
    ),
    MonitoringTab: defineAsyncComponent(() => import("./MonitoringTab.vue")),
    SnippetTab,
  },
  props: {
    tabId: {
      type: String,
      required: true,
    },
  },
  data() {
    return {
      canScrollLeft: false,
      canScrollRight: false,
    };
  },
  computed: {
    tabs() {
      return tabsStore.getSecondaryTabs(this.tabId);
    },
    selectedTab() {
      return tabsStore.getSelectedSecondaryTab(this.tabId);
    },
    isSnippetsPanel() {
      let primaryTab = tabsStore.getPrimaryTabById(this.tabId);
      return primaryTab?.name === "Snippets";
    },
  },
  updated() {
    this.$nextTick(() => {
      this.updateScrollOptions();
    });
  },
  mounted() {
    if (this.isSnippetsPanel) {
      tabsStore.createSnippetTab(this.tabId);
    }

    tabsStore.$onAction((action) => {
      if (action.name === "addTab") {
        action.after(() => {
          if (
            (tabsStore.selectedPrimaryTab.id === this.tabId &&
              this.tabs.includes(this.selectedTab)) ||
            this.isSnippetsPanel
          ) {
            this.$nextTick(() => {
              let lastTab = document.getElementById(this.selectedTab.id);
              if (!!lastTab) {
                setTimeout(() => {
                  lastTab.scrollIntoView({
                    inline: "end",
                    behavior: "smooth",
                  });
                }, 100);
              }
            });
          }
        });
      }
    });
  },
  methods: {
    getCurrentProps(tab) {
      const componentsProps = {
        SnippetTab: {
          tabId: tab.id,
          snippet: tab.metaData.snippetObject,
        },
        ConsoleTab: {
          connId: tab.parentId,
          tabId: tab.id,
          consoleHelp: tab?.metaData?.consoleHelp,
          databaseIndex: tab?.metaData?.databaseIndex,
          dialect: tab?.metaData?.dialect,
        },
        QueryTab: {
          connId: tab.parentId,
          tabId: tab.id,
          databaseIndex: tab.metaData?.databaseIndex,
          dialect: tab.metaData?.dialect,
          databaseName: tab.metaData.databaseName,
          initTabDatabaseId: tab.metaData.initTabDatabaseId,
          initialQuery: tab.metaData.initialQuery,
        },
        MonitoringDashboard: {
          connId: tab.parentId,
          tabId: tab.id,
          databaseIndex: tab?.metaData?.databaseIndex,
        },
        BackupTab: {
          connId: tab.parentId,
          tabId: tab.id,
          databaseIndex: tab?.metaData?.databaseIndex,
          backupType: tab.metaData.backupType,
          treeNode: tab.metaData.treeNode,
        },
        RestoreTab: {
          connId: tab.parentId,
          tabId: tab.id,
          databaseIndex: tab?.metaData?.databaseIndex,
          restoreType: tab.metaData.backupType,
          treeNode: tab.metaData.treeNode,
        },
        ERDTab: {
          tabId: tab.parentId,
          databaseIndex: tab?.metaData?.databaseIndex,
          databaseName: tab?.metaData?.databaseName,
          schema: tab.metaData?.schema,
        },
        DataEditorTab: {
          connId: tab.parentId,
          tabId: tab.id,
          databaseIndex: tab?.metaData?.databaseIndex,
          databaseName: tab?.metaData?.databaseName,
          table: tab.metaData.table,
          schema: tab.metaData.schema,
          dialect: tab.metaData.dialect,
          query_filter: tab.metaData.dialect,
        },
        SchemaEditorTab: {
          connId: tab.parentId,
          tabId: tab.id,
          databaseIndex: tab?.metaData?.databaseIndex,
          databaseName: tab?.metaData?.databaseName,
          mode: tab.metaData.editMode,
          schema: tab.metaData.schema,
          table: tab.metaData.table,
          treeNode: tab.metaData.treeNode,
          dialect: tab.metaData.dialect,
        },
        MonitoringTab: {
          connId: tab.parentId,
          databaseIndex: tab?.metaData?.databaseIndex,
          dialect: tab?.metaData?.dialect,
          query: tab.metaData.query,
        },
        ConfigTab: {
          databaseIndex: tab?.metaData?.databaseIndex,
          connId: tab.parentId,
        },
      };
      return componentsProps[tab.component];
    },
    addTab(event) {
      if (tabsStore.getPrimaryTabById(this.tabId).name === "Snippets") {
        tabsStore.createSnippetTab(this.tabId);
      } else {
        this.showMenuNewTab(event);
      }
    },
    showMenuNewTab(e) {
      let optionList = [
        {
          label: "Query Tab",
          icon: "fas cm-all fa-search",
          onClick: () => {
            let name = tabsStore.selectedPrimaryTab.metaData.selectedDatabase
              .replace("\\", "/")
              .split("/")
              .pop();
            tabsStore.createQueryTab(name);
          },
        },
        {
          label: "Console Tab",
          icon: "fas cm-all fa-terminal",
          onClick: () => {
            tabsStore.createConsoleTab();
          },
        },
      ];

      if (
        ["postgresql", "mysql", "mariadb"].includes(
          tabsStore.selectedPrimaryTab.metaData.selectedDBMS
        )
      ) {
        optionList.push({
          label: "Monitoring Dashboard",
          icon: "fas cm-all fa-chart-line",
          onClick: () => {
            tabsStore.createMonitoringDashboardTab();
          },
        });
      }

      if (tabsStore.selectedPrimaryTab.metaData.selectedDBMS === "postgresql") {
        optionList.push({
          label: "Backends",
          icon: "fas cm-all fa-tasks",
          onClick: () => {
            tabsStore.createMonitoringTab(
              "Backends",
              "select * from pg_stat_activity"
            );
          },
        });
      } else if (
        ["mysql", "mariadb"].includes(
          tabsStore.selectedPrimaryTab.metaData.selectedDBMS
        )
      ) {
        optionList.push({
          label: "Process List",
          icon: "fas cm-all fa-tasks",
          onClick: () => {
            tabsStore.createMonitoringTab(
              "Process List",
              "select * from information_schema.processlist"
            );
          },
        });
      }

      ContextMenu.showContextMenu({
        theme: "pgmanage",
        x: e.x,
        y: e.y,
        zIndex: 1000,
        minWidth: 230,
        items: optionList,
      });
    },
    handleNavWheel(event) {
      event.deltaY > 0
        ? this.$refs.navTabs.scrollTo({
            left: this.$refs.navTabs.scrollLeft + 150,
            behavior: "smooth",
          })
        : this.$refs.navTabs.scrollTo({
            left: this.$refs.navTabs.scrollLeft - 150,
            behavior: "smooth",
          });
    },
    handleLeftScrollClick() {
      this.$refs.navTabs.scrollTo({
        left: this.$refs.navTabs.scrollLeft - 150,
        behavior: "smooth",
      });
    },
    handleRightScrollClick() {
      this.$refs.navTabs.scrollTo({
        left: this.$refs.navTabs.scrollLeft + 150,
        behavior: "smooth",
      });
    },
    updateScrollOptions() {
      if (this.$refs.navTabs.scrollWidth > this.$refs.navTabs.clientWidth) {
        this.canScrollLeft = this.$refs.navTabs.scrollLeft === 0 ? false : true;
        if (
          this.$refs.navTabs.scrollLeft + this.$refs.navTabs.clientWidth <
          this.$refs.navTabs.scrollWidth
        ) {
          this.canScrollRight = true;
        } else {
          this.canScrollRight = false;
        }
      } else {
        this.canScrollRight = false;
        this.canScrollLeft = false;
      }
    },
  },
};
</script>

<style scoped>
.scrollable-outer {
  overflow-y: hidden;
  max-width: calc(100% - 55px);
}

.scrollable-inner {
  overflow: scroll;
  overflow-y: hidden;
  scrollbar-width: none;
}

.scrollable-inner::-webkit-scrollbar {
  display: none;
}
</style>
