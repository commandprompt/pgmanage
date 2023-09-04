import { removeTab, showMenuNewTab } from "../workspace";
import { beforeCloseTab } from "../create_tab_functions";
import { v_current_terminal_theme } from "../header_actions";
import { createApp } from "vue";
import ConsoleTab from "../components/ConsoleTab.vue";

let createConsoleTabFunctionNew = function() {

   // Removing last tab of the inner tab list
  v_connTabControl.selectedTab.tag.tabControl.removeLastTab();

  // Creating console tab in the inner tab list

  let tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
    p_icon: '<i class="fas fa-terminal icon-tab-title"></i>',
    p_name: '<span> Console</span><span id="tab_loading" style="visibility:hidden;"><i class="tab-icon node-spin"></i></span><i title="" id="tab_check" style="display: none;" class="fas fa-check-circle tab-icon icon-check"></i></span>',
    p_selectFunction: function() {

    },
    p_closeFunction: function(e, tab) {
      let current_tab = tab;
      beforeCloseTab(e, function() {
        current_tab.app.unmount();
        removeTab(current_tab)
      })
    }
  })

   // Selecting the newly created tab
  v_connTabControl.selectedTab.tag.tabControl.selectTab(tab);

  //Adding unique names to spans
  let tab_loading_span = document.getElementById('tab_loading');
  tab_loading_span.id = `tab_loading_${tab.id}`;
  let tab_check_span = document.getElementById('tab_check');
  tab_check_span.id = `tab_check_${tab.id}`;

  tab.elementDiv.innerHTML = `<console-tab 
                                  :conn-id="connId"
                                  :tab-id="tabId" 
                                  :editor-theme="editorTheme" 
                                  :font-size="fontSize"
                                  :terminal-theme="terminalTheme"
                                  :console-help="consoleHelp"
                                  :database-index="databaseIndex"
                                  >
                                </console-tab>`;
  const app = createApp({
    components: {
      ConsoleTab
    },
    data() {
      return {
        connId: v_connTabControl.selectedTab.id,
        tabId: tab.id,
        editorTheme: window.v_editor_theme,
        fontSize: window.v_font_size,
        terminalTheme: v_current_terminal_theme,
        consoleHelp: v_connTabControl.selectedTab.tag.consoleHelp,
        databaseIndex: v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
      }
    }
  })

  tab.app = app

  app.mount(`#${tab.elementDiv.id}`);

  let tag = {
    tab_id: tab.id,
    mode: 'console',
    tab_loading_span : tab_loading_span,
    tab_check_span : tab_check_span,
    tabControl: v_connTabControl.selectedTab.tag.tabControl,
    connTab: v_connTabControl.selectedTab,
    vueComponent: true,
    consoleHistory: {
      modal: document.getElementById('modal_console_history_' + tab.id),
      div: document.getElementById('console_history_div_' + tab.id),
      headerDiv: document.getElementById('console_history_header_' + tab.id),
      gridDiv: document.getElementById('console_history_grid_' + tab.id),
      grid: null,
      currentPage: 1,
      pages: 1,
      spanNumPages: null,
      spanCurrPage: null,
      inputStartedFrom: null,
      inputStartedFromLastValue: null,
      inputStartedTo: null,
      inputStartedToLastValue: null,
      inputCommandContains: null,
      inputCommandContainsLastValue: null
    },
  }

  tab.tag = tag

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

  // Sets a render refresh for the grid on the consoleHistory.modal after the modal is fully loaded
  $(v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.modal).on('shown.bs.modal', function () {
    v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.grid.render();
  });
}

export { createConsoleTabFunctionNew }