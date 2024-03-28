import { defineStore } from "pinia";
import ShortUniqueId from "short-unique-id";
import { connectionsStore } from "./stores_initializer";
import { showToast, showConfirm } from "../notification_control";
import ContextMenu from "@imengyu/vue3-context-menu";
import { createRequest } from "../long_polling";
import moment from "moment";
import { emitter } from "../emitter";
import { queryRequestCodes } from "../constants";
import { showMenuNewTabOuter, renameTab } from "../workspace";
import { h } from "vue";

const useTabsStore = defineStore("tabs", {
  state: () => ({
    id: new ShortUniqueId({
      dictionary: "alpha_lower",
      length: 8,
    }).randomUUID(),
    tabs: [],
    selectedPrimaryTab: "",
  }),
  getters: {
    selectablePrimaryTabs() {
      return this.tabs.filter((tab) => tab.selectable);
    },
  },
  actions: {
    addTab({
      parentId = null,
      name = "",
      component = null,
      icon = false,
      tooltip = false,
      disabled = false,
      isDraggable = true,
      selectable = true,
      clickFunction = null,
      closable = true,
      closeFunction = null,
      dblClickFunction = null,
      rightClickFunction = false,
      selectFunction = null,
      mode = null,
    }) {
      let tabId = new ShortUniqueId({
        dictionary: "alpha_lower",
        length: 8,
      }).randomUUID();

      let tab = {
        id: tabId,
        parentId: parentId,
        name: name,
        component: component,
        icon: icon,
        tooltip: tooltip,
        disabled: disabled,
        isDraggable: isDraggable,
        selectable: selectable,
        closable: closable,
        clickFunction: clickFunction,
        selectFunction: selectFunction,
        dblClickFunction: dblClickFunction,
        closeFunction: closeFunction,
        rightClickFunction: rightClickFunction,
        metaData: {
          secondaryTabs: [],
          selectedTab: "",
          isLoading: false,
          isReady: false,
          mode: mode,
        },
        dragEndFunction: this.dragEndFunction,
      };
      if (parentId) {
        let primaryTab = this.tabs.find((tab) => tab.id === parentId);
        //TODO: handle case if tab with specified Id not exist
        primaryTab.metaData.secondaryTabs.push(tab);
      } else {
        this.tabs.push(tab);
      }
      return tab;
    },
    selectTab(tab) {
      if (tab.parentId) {
        let primaryTab = this.tabs.find(
          (primaryTab) => primaryTab.id === tab.parentId
        );
        primaryTab.metaData.selectedTab = tab;
      } else {
        this.selectedPrimaryTab = tab;
      }

      if (tab.selectFunction != null) {
        tab.selectFunction();
      }
    },
    removeTab(tabToRemove) {
      const isPrimaryTab = !tabToRemove.parentId;
      if (isPrimaryTab) {
        this.removePrimaryTab(tabToRemove);
      } else {
        this.removeSecondaryTab(tabToRemove);
      }
    },
    removePrimaryTab(tabToRemove) {
      const tabIndex = this.tabs.indexOf(tabToRemove);
      if (this.selectedPrimaryTab === tabToRemove) {
        const selectableTabs = this.selectablePrimaryTabs;
        const nextTabIndex = selectableTabs.indexOf(tabToRemove) + 1;
        const newSelectedTab =
          nextTabIndex < selectableTabs.length
            ? selectableTabs[nextTabIndex]
            : selectableTabs[nextTabIndex - 2];

        this.selectedPrimaryTab = newSelectedTab;
      }
      this.tabs.splice(tabIndex, 1);
    },
    removeSecondaryTab(tabToRemove) {
      const primaryTab = this.tabs.find(
        (tab) => tab.id === tabToRemove.parentId
      );

      const secondaryTabs = primaryTab.metaData.secondaryTabs;
      const tabIndex = secondaryTabs.indexOf(tabToRemove);
      if (primaryTab.metaData.selectedTab === tabToRemove) {
        const nextTabIndex = tabIndex + 1;
        if (secondaryTabs.length > 1) {
          const newSelectedTab = secondaryTabs[nextTabIndex]?.selectable
            ? secondaryTabs[nextTabIndex]
            : secondaryTabs[nextTabIndex - 2];
          primaryTab.metaData.selectedTab = newSelectedTab;
        }
      }
      secondaryTabs.splice(tabIndex, 1);
    },
    getSecondaryTabs(parentId) {
      const primaryTabIdx = this.tabs.findIndex((tab) => tab.id === parentId);
      return this.tabs[primaryTabIdx]?.metaData?.secondaryTabs || [];
    },
    getSelectedSecondaryTab(parentId) {
      const primaryTabIdx = this.tabs.findIndex((tab) => tab.id === parentId);
      return this.tabs[primaryTabIdx]?.metaData?.selectedTab;
    },
    dragEndFunction(e, tab) {
      // Get the dragged element and drop position
      let el = e.target;
      let drop_pos_x = e.clientX;
      let drop_pos_y = e.clientY;
      let allNodes = Array.from(el.parentNode.children);
      let oldIndex = allNodes.indexOf(el);

      // Filter out non-draggable siblings
      let siblings = allNodes.filter((node) => node !== el && !!node.draggable);

      // Find the new index based on drop position
      let newIndex = siblings.findIndex((sibling) => {
        let rect = sibling.getBoundingClientRect();
        return (
          drop_pos_x >= rect.left &&
          drop_pos_x <= rect.right &&
          drop_pos_y >= rect.top &&
          drop_pos_y <= rect.bottom
        );
      });

      // Handle case where newIndex is -1 (drop position not found among siblings)
      if (newIndex === -1) {
        newIndex = siblings.length;
      } else if (newIndex === oldIndex) {
        newIndex++;
      }

      // Reorder the tabs based on the new index
      if (oldIndex !== -1 && oldIndex !== newIndex) {
        if (!tab.parentId) {
          let removedEl = this.tabs.splice(oldIndex, 1)[0];
          this.tabs.splice(newIndex, 0, removedEl);
        } else {
          let primaryTab = this.tabs.find(
            (primaryTab) => primaryTab.id === tab.parentId
          );
          let removedEl = primaryTab.metaData.secondaryTabs.splice(
            oldIndex,
            1
          )[0];
          primaryTab.metaData.secondaryTabs.splice(newIndex, 0, removedEl);
        }
      }
    },
    getPrimaryTabById(tabId) {
      return this.tabs.find((tab) => tab.id === tabId);
    },
    beforeCloseTab(e, confirmFunction) {
      if (e) {
        if (e.clientX == 0 && e.clientY == 0) {
          showConfirm("Are you sure you want to remove this tab?", function () {
            confirmFunction();
          });
        } else {
          ContextMenu.showContextMenu({
            theme: "pgmanage",
            x: e.x,
            y: e.y,
            zIndex: 1000,
            minWidth: 230,
            items: [
              {
                label: "Confirm",
                icon: "fas cm-all fa-check",
                onClick: function () {
                  confirmFunction();
                },
              },
              {
                label: "Cancel",
                icon: "fas cm-all fa-times",
              },
            ],
          });
        }
      } else {
        confirmFunction();
      }
    },
    createSnippetPanel() {
      this.addTab({
        name: "Snippets",
        component: "SnippetPanel",
        icon: '<i class="fas fa-file-code"></i>',
        tooltip: "Snippets Panel",
        closable: false,
        selectable: false,
        clickFunction: function () {
          emitter.emit("toggle_snippet_panel");
        },
      });
    },
    createConnectionsTab() {
      this.addTab({
        name: "Connections",
        icon: '<i class="fas fa-bolt"></i>',
        tooltip: "Connections",
        closable: false,
        selectable: false,
        clickFunction: function (e) {
          showMenuNewTabOuter(e);
        },
      });
    },
    createWelcomeTab() {
      const tab = this.addTab({
        name: "Welcome",
        component: "WelcomeScreen",
        icon: '<i class="fas fa-hand-spock"></i>',
        tooltip: "Welcome to PgManage",
        closable: false,
        selectFunction: function () {
          document.title = "Welcome to PgManage";
        },
      });

      this.selectTab(tab);
    },
    createConnectionTab(
      index,
      createInitialTabs = true,
      name = false,
      tooltipName = false
    ) {
      return new Promise((resolve, reject) => {
        if (connectionsStore.connections.length == 0) {
          showToast("error", "Create connections first.");
          reject("No connections available.");
        } else {
          let v_conn = connectionsStore.connections[0];
          for (let i = 0; i < connectionsStore.connections.length; i++) {
            if (connectionsStore.connections[i].id === index) {
              // patch the connection last used date when connecting
              // to refresh last-used labels on the welcome screen
              connectionsStore.connections[i].last_access_date = moment.now();
              v_conn = connectionsStore.connections[i];
            }
          }

          let connName = "";
          if (name) {
            connName = name;
          }
          if (connName === "" && v_conn.alias && v_conn.alias !== "") {
            connName = v_conn.alias;
          }

          if (!tooltipName) {
            tooltipName = "";

            if (v_conn.conn_string && v_conn.conn_string !== "") {
              if (v_conn.alias) {
                tooltipName += `<h5 class="my-1">${v_conn.alias}</h5>`;
              }
              tooltipName += `<div class="mb-1">${v_conn.conn_string}</div>`;
            } else {
              if (v_conn.alias) {
                tooltipName += `<h5 class="my-1">${v_conn.alias}</h5>`;
              }
              if (v_conn.details1) {
                tooltipName += `<div class="mb-1">${v_conn.details1}</div>`;
              }
              if (v_conn.details2) {
                tooltipName += `<div class="mb-1">${v_conn.details2}</div>`;
              }
            }
          }

          const imgPath =
            import.meta.env.MODE === "development"
              ? `${import.meta.env.BASE_URL}src/assets/images/`
              : `${import.meta.env.BASE_URL}assets/`;

          let imgName;
          if (
            import.meta.env.MODE === "development" ||
            v_conn.technology === "sqlite"
          ) {
            imgName = v_conn.technology;
          } else {
            imgName = `${v_conn.technology}2`;
          }

          let icon = `<img src="${v_url_folder}${imgPath}${imgName}.svg"/>`;

          const connTab = this.addTab({
            name: connName,
            component: "ConnectionTab",
            icon: icon,
            tooltip: tooltipName,
            mode: "connection",
            selectFunction: () => {
              document.title = "PgManage";
              this.checkTabStatus();
            },
            closeFunction: (e, primaryTab) => {
              $('[data-toggle="tab"]').tooltip("hide");
              this.beforeCloseTab(e, () => {
                var v_tabs_to_remove = [];

                let tabs = this.getSecondaryTabs(primaryTab.id);

                tabs.forEach((tab) => {
                  if (
                    tab.metaData.mode == "query" ||
                    tab.metaData.mode == "edit" ||
                    tab.metaData.mode == "debug" ||
                    tab.metaData.mode == "console"
                  ) {
                    var v_message_data = {
                      tab_id: tab.id,
                      tab_db_id: null,
                      conn_tab_id: primaryTab.id,
                    };
                    if (tab.metaData.mode == "query")
                      v_message_data.tab_db_id = tab.metaData.initTabDatabaseId;
                    v_tabs_to_remove.push(v_message_data);
                  }

                  if (tab.closeFunction) tab.closeFunction(e, tab);
                });

                var v_message_data = {
                  conn_tab_id: primaryTab.id,
                  tab_db_id: null,
                  tab_id: null,
                };
                v_tabs_to_remove.push(v_message_data);

                if (v_tabs_to_remove.length > 0) {
                  createRequest(queryRequestCodes.CloseTab, v_tabs_to_remove);
                }
                this.removeTab(primaryTab);
              });
            },
          });
          connTab.metaData.selectedDBMS = v_conn.technology;
          connTab.metaData.consoleHelp = v_conn.console_help;
          connTab.metaData.selectedDatabaseIndex = v_conn.id;
          connTab.metaData.selectedDatabase =
            v_conn.last_used_database || v_conn.service;
          connTab.metaData.createInitialTabs = createInitialTabs;

          this.selectTab(connTab);
          resolve(connTab);
        }
      });
    },
    createTerminalTab(index, alias, details) {
      let tooltipName = "";

      if (alias) {
        tooltipName += `<h5 class="my-1">${alias}</h5>`;
      }
      if (details) {
        tooltipName += `<div class="mb-1">${details}</div>`;
      }

      const tab = this.addTab({
        name: alias,
        component: "TerminalTab",
        icon: '<i class="fas fa-terminal"></i>',
        tooltip: tooltipName,
        closable: false,
        mode: "outer_terminal",
        selectFunction: function () {
          emitter.emit(`${this.id}_resize`);
        },
        rightClickFunction: (e, tab) => {
          this.terminalContextMenu(e, tab);
        },
      });
      tab.metaData.selectedDatabaseIndex = index;

      this.selectTab(tab);
    },
    createConsoleTab(parentId) {
      const tab = this.addTab({
        parentId: parentId ?? this.selectedPrimaryTab.id,
        name: "Console",
        component: "ConsoleTab",
        icon: "<i class='fas fa-terminal icon-tab-title'></i>",
        mode: "console",
        selectFunction: function () {
          emitter.emit(`${this.id}_resize`);
          emitter.emit(`${this.id}_check_console_status`);
        },
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.closeTab(tab);
          });
        },
      });
      const primaryTab = !!parentId
        ? this.getPrimaryTabById(parentId)
        : this.selectedPrimaryTab;
      tab.metaData.consoleHelp = primaryTab.metaData?.consoleHelp;
      tab.metaData.databaseIndex = primaryTab.metaData?.selectedDatabaseIndex;
      tab.metaData.dialect = primaryTab.metaData?.selectedDBMS;

      this.selectTab(tab);
    },
    createQueryTab(
      name = "Query",
      tabDbId = null,
      tabDbName = null,
      initialQuery = null,
      parentId = null
    ) {
      const tab = this.addTab({
        parentId: parentId ?? this.selectedPrimaryTab.id,
        name: name,
        icon: '<i class="fas fa-database icon-tab-title"></i>',
        component: "QueryTab",
        mode: "query",
        selectFunction: function () {
          emitter.emit(`${this.id}_check_query_status`);
        },
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.closeTab(tab);
          });
        },
        dblClickFunction: renameTab,
      });
      const primaryTab = !!parentId
        ? this.getPrimaryTabById(parentId)
        : this.selectedPrimaryTab;
      tab.metaData.databaseName =
        tabDbName ?? primaryTab.metaData?.selectedDatabase;
      tab.metaData.initTabDatabaseId = tabDbId;
      tab.metaData.initialQuery = initialQuery;
      tab.metaData.databaseIndex = primaryTab.metaData?.selectedDatabaseIndex;
      tab.metaData.dialect = primaryTab.metaData?.selectedDBMS;

      this.selectTab(tab);
    },
    createSnippetTab(tabId, snippet) {
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

      let tab = this.addTab({
        name: snippetName,
        parentId: tabId,
        component: "SnippetTab",
        mode: "snippet",
        selectFunction: function () {
          emitter.emit(`${this.id}_editor_focus`);
          emitter.emit(`${this.id}_resize`);
        },
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.removeTab(tab);
          });
        },
      });

      tab.metaData.snippetObject = snippetDetails;

      this.selectTab(tab);
    },
    createMonitoringDashboardTab() {
      const tab = this.addTab({
        parentId: this.selectedPrimaryTab.id,
        name: "Monitoring",
        icon: '<i class="fas fa-chart-line icon-tab-title"></i>',
        component: "MonitoringDashboard",
        mode: "monitoring_dashboard",
        selectFunction: function () {
          emitter.emit(`${this.id}_redraw_widget_grid`);
        },
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.closeTab(tab);
          });
        },
        dblClickFunction: renameTab,
      });
      tab.metaData.databaseIndex =
        this.selectedPrimaryTab?.metaData?.selectedDatabaseIndex;
      this.selectTab(tab);
    },
    createConfigurationTab() {
      const tab = this.addTab({
        parentId: this.selectedPrimaryTab.id,
        name: "Configuration",
        component: "ConfigTab",
        mode: "configuration",
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.closeTab(tab);
          });
        },
      });

      tab.metaData.databaseIndex =
        this.selectedPrimaryTab?.metaData?.selectedDatabaseIndex;
      this.selectTab(tab);
    },
    createUtilityTab(node, utility, backupType = "objects") {
      let utilityTitle =
        backupType === "objects"
          ? `(${node.data.type}:${node.title})`
          : backupType;
      let tabName = `${utility} ${utilityTitle}`;
      let mode = utility.toLowerCase();

      const tab = this.addTab({
        parentId: this.selectedPrimaryTab.id,
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
        this.selectedPrimaryTab?.metaData?.selectedDatabaseIndex;

      this.selectTab(tab);
    },
    createERDTab(schema = "") {
      let tabName = schema ? `ERD: ${schema}` : "ERD";

      const tab = this.addTab({
        parentId: this.selectedPrimaryTab.id,
        name: tabName,
        component: "ERDTab",
        icon: '<i class="fab fa-hubspot icon-tab-title"></i>',
        selectFunction: function () {
          document.title = "PgManage";
        },
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.closeTab(tab);
          });
        },
      });

      tab.metaData.schema = schema;
      tab.metaData.databaseIndex =
        this.selectedPrimaryTab?.metaData?.selectedDatabaseIndex;
      tab.metaData.databaseName =
        this.selectedPrimaryTab?.metaData?.selectedDatabase;

      this.selectTab(tab);
    },
    createDataEditorTab(table, schema = "") {
      let tabName = schema
        ? `Edit data: ${schema}.${table}`
        : `Edit data: ${table}`;

      const tab = this.addTab({
        icon: '<i class="fas fa-table icon-tab-title"></i>',
        parentId: this.selectedPrimaryTab.id,
        name: tabName,
        component: "DataEditorTab",
        mode: "edit",
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.closeTab(tab);
          });
        },
      });

      const DIALECT_MAP = {
        oracle: "oracledb",
        mariadb: "mysql",
      };
      let dialect = this.selectedPrimaryTab.metaData.selectedDBMS;
      let mappedDialect = DIALECT_MAP[dialect] || dialect;

      tab.metaData.dialect = mappedDialect;
      tab.metaData.table = table;
      tab.metaData.schema = schema;
      tab.metaData.query_filter = ""; //to be used in the future for passing extra filters when tab is opened
      tab.metaData.databaseIndex =
        this.selectedPrimaryTab?.metaData?.selectedDatabaseIndex;
      tab.metaData.databaseName =
        this.selectedPrimaryTab?.metaData?.selectedDatabase;

      this.selectTab(tab);
    },
    createSchemaEditorTab(node, mode, dialect) {
      let tableName =
        dialect === "mysql"
          ? `${node.data.database}.${node.title}`
          : node.title.replace(/^"(.*)"$/, "$1");

      let tabTitle = mode === "alter" ? `Alter: ${tableName}` : "New Table";

      const tab = this.addTab({
        parentId: this.selectedPrimaryTab.id,
        name: tabTitle,
        component: "SchemaEditorTab",
        mode: "alter",
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.closeTab(tab);
          });
        },
      });

      tab.metaData.dialect = dialect || "postgres";
      tab.metaData.editMode = mode;
      tab.metaData.schema = node.data.schema;
      tab.metaData.table = mode === "alter" ? tableName : null;
      tab.metaData.treeNode = node;
      tab.metaData.databaseIndex =
        this.selectedPrimaryTab?.metaData?.selectedDatabaseIndex;

      tab.metaData.databaseName =
        dialect === "mysql"
          ? node.data.database
          : this.selectedPrimaryTab?.metaData?.selectedDatabase;

      this.selectTab(tab);
    },
    createMonitoringTab(name = "Backends", query) {
      const tab = this.addTab({
        parentId: this.selectedPrimaryTab.id,
        name: name,
        component: "MonitoringTab",
        icon: `<i class="fas fa-tasks icon-tab-title"></i>`,
        mode: "monitor_grid",
        selectFunction: () => {
          document.title = "PgManage";
        },
        closeFunction: (e, tab) => {
          this.beforeCloseTab(e, () => {
            this.closeTab(tab);
          });
        },
        dblClickFunction: renameTab,
      });

      tab.metaData.query = query;
      tab.metaData.databaseIndex =
        this.selectedPrimaryTab?.metaData?.selectedDatabaseIndex;
      tab.metaData.dialect = this.selectedPrimaryTab?.metaData?.selectedDBMS;

      this.selectTab(tab);
    },
    checkTabStatus() {
      const tab = this.selectedPrimaryTab.metaData.selectedTab;
      switch (tab?.metaData?.mode) {
        case "query":
          emitter.emit(`${tab.id}_check_query_status`);
          break;
        case "console":
          emitter.emit(`${tab.id}_check_console_status`);
          break;
        case "edit":
          console.log("Not implemented"); // TODO: implement check tab status functionality for edit tab
          break;
        default:
          break;
      }
    },
    terminalContextMenu(e, tab) {
      let optionList = [
        {
          label: "Adjust Terminal Dimensions",
          icon: "fas cm-all fa-window-maximize",
          onClick: function () {
            emitter.emit(`${tab.id}_adjust_terminal_dimensions`);
          },
        },
        {
          label: h("p", {
            class: "mb-0 text-danger",
            innerHTML: "Close Terminal",
          }),
          onClick: () => {
            ContextMenu.closeContextMenu();
            ContextMenu.showContextMenu({
              theme: "pgmanage",
              x: e.x,
              y: e.y,
              zIndex: 1000,
              minWidth: 230,
              items: [
                {
                  label: "Confirm",
                  icon: "fas cm-all fa-check",
                  onClick: () => {
                    this.closeTab(tab);
                  },
                },
                {
                  label: "Cancel",
                  icon: "fas cm-all fa-times",
                },
              ],
            });
          },
        },
      ];

      ContextMenu.showContextMenu({
        theme: "pgmanage",
        x: e.x,
        y: e.y,
        zIndex: 1000,
        minWidth: 230,
        items: optionList,
      });
    },
    closeTab(tab) {
      if (
        ["query", "edit", "console", "outer_terminal"].includes(
          tab.metaData.mode
        )
      ) {
        let messageData = {
          tab_id: tab.id,
          tab_db_id: null,
          conn_tab_id: this.selectedPrimaryTab.id,
        };
        if (tab.metaData.mode === "query") {
          messageData.tab_db_id = tab.metaData.initTabDatabaseId;
        }

        if (tab.metaData.mode === "outer_terminal") {
          messageData.tab_id = null;
        }

        createRequest(queryRequestCodes.CloseTab, [messageData]);
      }

      this.removeTab(tab);
    },
  },
});

export { useTabsStore };
