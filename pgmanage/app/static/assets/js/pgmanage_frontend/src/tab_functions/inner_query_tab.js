import { beforeCloseTab } from "../create_tab_functions";
import { removeTab, showMenuNewTab, renameTab } from "../workspace";
import QueryTab from "../components/QueryTab.vue";
import { createApp } from "vue";
import { emitter } from "../emitter";

export let createQueryTabFunction = function (
  name = "Query",
  tab_db_id = null,
  tab_db_name
) {
  // Removing last tab of the inner tab list.
  v_connTabControl.selectedTab.tag.tabControl.removeLastTab();
  let name_html = `
    <span id="tab_title">
      ${name}
    </span>
    <span id="tab_loading" style="visibility:hidden;">
      <i class="tab-icon node-spin"></i>
    </span>
    <i title="" id="tab_check" style="display: none;" class="fas fa-check-circle tab-icon icon-check"></i>
    `;

  // Creating query tab in the inner tab list

  let tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
    p_name: name_html,
    p_selectFunction: function () {
      if (this.tag != null) {
        emitter.emit(`${this.id}_check_query_status`);
      }
    },
    p_closeFunction: function (e, tab) {
      let current_tab = tab;
      beforeCloseTab(e, function () {
        current_tab.app.unmount();
        removeTab(current_tab);
      });
    },
    p_dblClickFunction: renameTab,
  });

  // Selecting newly created tab.
  v_connTabControl.selectedTab.tag.tabControl.selectTab(tab);

  // Adding unique names to spans.

  let tab_title_span = document.getElementById("tab_title");
  tab_title_span.id = `tab_title_${tab.id}`;
  let tab_loading_span = document.getElementById("tab_loading");
  tab_loading_span.id = `tab_loading_${tab.id}`;
  let tab_check_span = document.getElementById("tab_check");
  tab_check_span.id = `tab_check_${tab.id}`;

  tab.elementDiv.innerHTML = `<query-tab 
                                  :conn-id="connId"
                                  :tab-id="tabId" 
                                  :database-index="databaseIndex"
                                  :dialect="dialect"
                                  :database-name="databaseName"
                                  :init-tab-database-id="initTabDatabaseId"
                                  >
                                </query-tab>`;

  const app = createApp({
    components: {
      QueryTab,
    },
    data() {
      return {
        connId: v_connTabControl.selectedTab.id,
        tabId: tab.id,
        databaseIndex: v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
        dialect: v_connTabControl.selectedTab.tag.selectedDBMS,
        databaseName: !!tab_db_name
          ? tab_db_name
          : v_connTabControl.selectedTab.tag.selectedDatabase,
        initTabDatabaseId: tab_db_id,
      };
    },
  });

  tab.app = app;

  app.mount(`#${tab.elementDiv.id}`);

  let tag = {
    tab_id: tab.id,
    mode: "query",
    tab_loading_span: tab_loading_span,
    tab_check_span: tab_check_span,
    tab_title_span: tab_title_span,
    tabControl: v_connTabControl.selectedTab.tag.tabControl,
    connTab: v_connTabControl.selectedTab,
    tab_db_id: tab_db_id,
  };

  tab.tag = tag;

  // Creating + tab in the outer tab list
  let add_tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
    p_name: "+",
    p_close: false,
    p_selectable: false,
    p_isDraggable: false,
    p_clickFunction: function (e) {
      showMenuNewTab(e);
    },
  });

  add_tab.tag = {
    mode: "add",
  };

};
