import { renameTab, showMenuNewTab, removeTab } from "../workspace";
import MonitoringDashboard from "../components/MonitoringDashboard.vue";
import { createApp } from "vue";
import { beforeCloseTab } from "../create_tab_functions";
import { emitter } from "../emitter";

export let createMonitoringDashboardTabFunction = function () {
  // Removing last tab of the inner tab list
  v_connTabControl.selectedTab.tag.tabControl.removeLastTab();

  let name_html = `
  <span id="tab_title">
  Monitoring
  </span>
  <span id="tab_loading" style="visibility:hidden;">
    <i class="tab-icon node-spin"></i>
  </span>
  <i title="" id="tab_check" style="display: none;" class="fas fa-check-circle tab-icon icon-check"></i>
  `;

  let tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
    p_icon: '<i class="fas fa-chart-bar icon-tab-title"></i>',
    p_name: name_html,
    p_selectFunction: function () {
      emitter.emit(`${this.id}_redraw_widget_grid`);
    },
    p_closeFunction: function (e, tab) {
      let current_tab = tab;
      beforeCloseTab(e, function () {
        removeTab(current_tab);
        current_tab.app.unmount();
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

  tab.elementDiv.innerHTML = `<monitoring-dashboard 
   :tab-id="tabId" 
   :database-index="databaseIndex"
   :conn-id="connId"
   >
   </monitoring-dashboard>`;

  const app = createApp({
    components: {
      MonitoringDashboard,
    },
    data() {
      return {
        connId: v_connTabControl.selectedTab.id,
        tabId: tab.id,
        databaseIndex: v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
      };
    },
  });

  app.mount(`#${tab.elementDiv.id}`);

  tab.app = app;

  let tag = {
    tab_id: tab.id,
    mode: "monitoring_dashboard",
    tab_title_span: tab_title_span,
    tab_loading_span: tab_loading_span,
    tab_check_span: tab_check_span,
  };

  tab.tag = tag;

  let add_tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
    p_name: "+",
    p_isDraggable: false,
    p_close: false,
    p_selectable: false,
    p_clickFunction: function (e) {
      showMenuNewTab(e);
    },
  });
  add_tab.tag = {
    mode: "add",
  };
};
