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
import { emitter } from "../emitter";
import { renameTab } from "../workspace.js";
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
    primaryTab() {
      return tabsStore.getPrimaryTabById(this.tabId);
    },
  },
  updated() {
    this.$nextTick(() => {
      this.updateScrollOptions();
    });
  },
  mounted() {
    this.setupEvents();

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
  unmounted() {
    this.clearEvents();
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
    setupEvents() {
      if (this.isSnippetsPanel) {
        emitter.on("create_snippet_tab", (snippet) => {
          this.createSnippetTab(snippet);
        });
        this.createSnippetTab();
      }

      emitter.on(`${this.tabId}_create_console_tab`, () => {
        this.createConsoleTab();
      });

      emitter.on(`${this.tabId}_create_query_tab`, (data) => {
        this.createQueryTab(
          data?.name,
          data?.tabDbId,
          data?.tabDbName,
          data?.initialQuery
        );
      });

      emitter.on(`${this.tabId}_create_conf_tab`, () => {
        this.createConfigurationTab();
      });

      emitter.on(
        `${this.tabId}_create_utility_tab`,
        ({ node, utility, backupType = "objects" }) => {
          this.createUtilityTab(node, utility, backupType);
        }
      );

      emitter.on(`${this.tabId}_create_erd_tab`, (schema) => {
        this.createERDTab(schema);
      });

      emitter.on(
        `${this.tabId}_create_data_editor_tab`,
        ({ table, schema = "" }) => {
          this.createDataEditorTab(table, schema);
        }
      );

      emitter.on(
        `${this.tabId}_create_schema_editor_tab`,
        ({ node, mode, dialect }) => {
          this.createSchemaEditorTab(node, mode, dialect);
        }
      );

      emitter.on(`${this.tabId}_create_monitoring_tab`, ({ name, query }) => {
        this.createMonitoringTab(name, query);
      });

      emitter.on(`${this.tabId}_create_monitoring_dashboard_tab`, () => {
        this.createMonitoringDashboardTab();
      });
    },
    clearEvents() {
      emitter.all.delete(`${this.tabId}_create_console_tab`);
      emitter.all.delete(`${this.tabId}_create_query_tab`);
      emitter.all.delete(`${this.tabId}_create_utility_tab`);
      emitter.all.delete(`${this.tabId}_create_erd_tab`);
      emitter.all.delete(`${this.tabId}_create_data_editor_tab`);
      emitter.all.delete(`${this.tabId}_create_schema_editor_tab`);
      emitter.all.delete(`${this.tabId}_create_monitoring_tab`);
      emitter.all.delete(`${this.tabId}_create_monitoring_dashboard_tab`);
      emitter.all.delete("create_snippet_tab");
    },
    addTab(event) {
      if (tabsStore.getPrimaryTabById(this.tabId).name === "Snippets") {
        this.createSnippetTab();
      } else {
        this.showMenuNewTab(event);
      }
    },
    createSnippetTab(snippet) {
      let snippetName = "New Snippet";

      let snippetDetails = {
        id: null,
        name: null,
        parent: null,
        type: "snippet",
      };

      if (snippet) {
        snippetName = snippet.name;
        snippetDetails = {
          id: snippet.id,
          name: snippetName,
          parent: snippet.id_parent,
          type: "snippet",
        };
      }

      let tab = tabsStore.addTab({
        name: snippetName,
        parentId: this.tabId,
        component: "SnippetTab",
        mode: "snippet",
        selectFunction: function () {
          emitter.emit(`${this.id}_editor_focus`);
          emitter.emit(`${this.id}_resize`);
        },
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, function () {
            tabsStore.removeTab(tab);
          });
        },
      });

      tab.metaData.snippetObject = snippetDetails;

      tabsStore.selectTab(tab);
    },
    createConsoleTab() {
      const tab = tabsStore.addTab({
        parentId: this.tabId,
        name: "Console",
        component: "ConsoleTab",
        icon: "<i class='fas fa-terminal icon-tab-title'></i>",
        mode: "console",
        selectFunction: function () {
          emitter.emit(`${this.id}_resize`);
          emitter.emit(`${this.id}_check_console_status`);
        },
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, function () {
            tabsStore.removeTab(tab);
          });
        },
      });

      tab.metaData.consoleHelp = this.primaryTab?.metaData?.consoleHelp;
      tab.metaData.databaseIndex =
        this.primaryTab?.metaData?.selectedDatabaseIndex;
      tab.metaData.dialect = this.primaryTab?.metaData?.selectedDBMS;

      tabsStore.selectTab(tab);
    },
    createMonitoringDashboardTab() {
      const tab = tabsStore.addTab({
        parentId: this.tabId,
        name: "Monitoring",
        icon: '<i class="fas fa-chart-bar icon-tab-title"></i>',
        component: "MonitoringDashboard",
        mode: "monitoring_dashboard",
        selectFunction: function () {
          emitter.emit(`${this.id}_redraw_widget_grid`);
        },
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.removeTab(tab);
          });
        },
        dblClickFunction: renameTab,
      });
      tab.metaData.databaseIndex =
        this.primaryTab?.metaData?.selectedDatabaseIndex;
      tabsStore.selectTab(tab);
    },
    createQueryTab(
      name = "Query",
      tabDbId = null,
      tabDbName = null,
      initialQuery = null
    ) {
      const tab = tabsStore.addTab({
        parentId: this.tabId,
        name: name,
        component: "QueryTab",
        mode: "query",
        selectFunction: function () {
          emitter.emit(`${this.id}_check_query_status`);
        },
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.removeTab(tab);
          });
        },
        dblClickFunction: renameTab,
      });

      tab.metaData.databaseName =
        tabDbName ?? this.primaryTab?.metaData?.selectedDatabase;
      tab.metaData.initTabDatabaseId = tabDbId;
      tab.metaData.initialQuery = initialQuery;
      tab.metaData.databaseIndex =
        this.primaryTab?.metaData?.selectedDatabaseIndex;
      tab.metaData.dialect = this.primaryTab?.metaData?.selectedDBMS;

      tabsStore.selectTab(tab);
    },
    createConfigurationTab() {
      const tab = tabsStore.addTab({
        parentId: this.tabId,
        name: "Configuration",
        component: "ConfigTab",
        mode: "configuration",
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.removeTab(tab);
          });
        },
      });

      tab.metaData.databaseIndex =
        this.primaryTab?.metaData?.selectedDatabaseIndex;
      tabsStore.selectTab(tab);
    },
    createUtilityTab(node, utility, backupType = "objects") {
      let utilityTitle =
        backupType === "objects"
          ? `(${node.data.type}:${node.title})`
          : backupType;
      let tabName = `${utility} ${utilityTitle}`;
      let mode = utility.toLowerCase();

      const tab = tabsStore.addTab({
        parentId: this.tabId,
        name: tabName,
        mode: mode,
        component: `${utility}Tab`,
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.removeTab(tab);
          });
        },
      });

      tab.metaData.treeNode = node;
      tab.metaData.backupType = backupType;
      tab.metaData.databaseIndex =
        this.primaryTab?.metaData?.selectedDatabaseIndex;

      tabsStore.selectTab(tab);
    },
    createERDTab(schema = "") {
      let tabName = schema ? `ERD: ${schema}` : "ERD";

      const tab = tabsStore.addTab({
        parentId: this.tabId,
        name: tabName,
        component: "ERDTab",
        icon: '<i class="fab fa-hubspot icon-tab-title"></i>',
        selectFunction: function () {
          document.title = "PgManage";
        },
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.removeTab(tab);
          });
        },
      });

      tab.metaData.schema = schema;
      tab.metaData.databaseIndex =
        this.primaryTab?.metaData?.selectedDatabaseIndex;
      tab.metaData.databaseName = this.primaryTab?.metaData?.selectedDatabase;

      tabsStore.selectTab(tab);
    },
    createDataEditorTab(table, schema = "") {
      let tabName = schema
        ? `Edit data: ${schema}.${table}`
        : `Edit data: ${table}`;

      const tab = tabsStore.addTab({
        icon: '<i class="fas fa-table icon-tab-title"></i>',
        parentId: this.tabId,
        name: tabName,
        component: "DataEditorTab",
        mode: "edit",
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.removeTab(tab);
          });
        },
      });

      const DIALECT_MAP = {
        oracle: "oracledb",
        mariadb: "mysql",
      };
      let dialect = tabsStore.selectedPrimaryTab.metaData.selectedDBMS;
      let mappedDialect = DIALECT_MAP[dialect] || dialect;

      tab.metaData.dialect = mappedDialect;
      tab.metaData.table = table;
      tab.metaData.schema = schema;
      tab.metaData.query_filter = ""; //to be used in the future for passing extra filters when tab is opened
      tab.metaData.databaseIndex =
        this.primaryTab?.metaData?.selectedDatabaseIndex;
      tab.metaData.databaseName = this.primaryTab?.metaData?.selectedDatabase;

      tabsStore.selectTab(tab);
    },
    createSchemaEditorTab(node, mode, dialect) {
      let tableName = node.title.replace(/^"(.*)"$/, "$1");
      let tabTitle = mode === "alter" ? `Alter: ${tableName}` : "New Table";

      const tab = tabsStore.addTab({
        parentId: this.tabId,
        name: tabTitle,
        component: "SchemaEditorTab",
        mode: "alter",
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.removeTab(tab);
          });
        },
      });

      tab.metaData.dialect = dialect || "postgres";
      tab.metaData.editMode = mode;
      tab.metaData.schema = node.data.schema;
      tab.metaData.table = mode === "alter" ? tableName : null;
      tab.metaData.treeNode = node;
      tab.metaData.databaseIndex =
        this.primaryTab?.metaData?.selectedDatabaseIndex;
      tab.metaData.databaseName = this.primaryTab?.metaData?.selectedDatabase;

      tabsStore.selectTab(tab);
    },
    createMonitoringTab(name = "Backends", query) {
      const tab = tabsStore.addTab({
        parentId: this.tabId,
        name: name,
        component: "MonitoringTab",
        icon: `<i class="fas fa-desktop icon-tab-title"></i>`,
        mode: "monitor_grid",
        selectFunction: () => {
          document.title = "PgManage";
        },
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.removeTab(tab);
          });
        },
        dblClickFunction: renameTab,
      });

      tab.metaData.query = query;
      tab.metaData.databaseIndex =
        this.primaryTab?.metaData?.selectedDatabaseIndex;
      tab.metaData.dialect = this.primaryTab?.metaData?.selectedDBMS;

      tabsStore.selectTab(tab);
    },
    showMenuNewTab(e) {
      let optionList = [
        {
          label: "Query Tab",
          icon: "fas cm-all fa-search",
          onClick: () => {
            this.createQueryTab();
          },
        },
        {
          label: "Console Tab",
          icon: "fas cm-all fa-terminal",
          onClick: () => {
            this.createConsoleTab();
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
            this.createMonitoringDashboardTab();
          },
        });
      }

      if (tabsStore.selectedPrimaryTab.metaData.selectedDBMS === "postgresql") {
        optionList.push({
          label: "Backends",
          icon: "fas cm-all fa-tasks",
          onClick: () => {
            this.createMonitoringTab(
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
            this.createMonitoringTab(
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
            left: this.$refs.navTabs.scrollLeft + 100,
            behavior: "smooth",
          })
        : this.$refs.navTabs.scrollTo({
            left: this.$refs.navTabs.scrollLeft - 100,
            behavior: "smooth",
          });
      this.updateScrollOptions();
    },
    handleLeftScrollClick(event) {
      this.$refs.navTabs.scrollTo({
        left: this.$refs.navTabs.scrollLeft - 100,
        behavior: "smooth",
      });
      this.updateScrollOptions();
    },
    handleRightScrollClick(event) {
      this.$refs.navTabs.scrollTo({
        left: this.$refs.navTabs.scrollLeft + 100,
        behavior: "smooth",
      });
      this.updateScrollOptions();
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
  overflow-x: scroll;
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
