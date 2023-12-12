import { monitoringAction, renameTab, removeTab, showMenuNewTab } from '../workspace'
import { beforeCloseTab } from "../create_tab_functions"
import { cellDataModal } from '../header_actions';
import { showPasswordPrompt } from '../passwords';
import { execAjax } from '../ajax_control';
import { showToast } from '../notification_control';
import { TabulatorFull as Tabulator } from 'tabulator-tables';

var v_createMonitoringTabFunction = function(p_name, p_query, p_actions) {

  var v_name = 'Backends';
  if (p_name)
    v_name = p_name;

  // Removing last tab of the inner tab list
  v_connTabControl.selectedTab.tag.tabControl.removeLastTab();

  // Creating console tab in the inner tab list
  var v_tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
    p_icon: `<i class="fas fa-desktop icon-tab-title"></i>`,
    p_name: '<span id="tab_title">' + v_name + '</span><span id="tab_loading" style="visibility:hidden;"><i class="tab-icon node-spin"></i></span><i title="" id="tab_check" style="display: none;" class="fas fa-check-circle tab-icon icon-check"></i>',
    p_selectFunction: function() {
      document.title = 'PgManage'
      if(this.tag != null) {
        this.tag.resize();
      }
    },
    p_closeFunction: function(e,p_tab) {
      var v_current_tab = p_tab;
      beforeCloseTab(e,
        function() {
          removeTab(v_current_tab);
        });
    },
    p_dblClickFunction: renameTab
  });

  // Selecting newly created tab
  v_connTabControl.selectedTab.tag.tabControl.selectTab(v_tab);

  // Adding unique names to spans
  var v_tab_title_span = document.getElementById('tab_title');
  v_tab_title_span.id = 'tab_title_' + v_tab.id;
  var v_tab_loading_span = document.getElementById('tab_loading');
  v_tab_loading_span.id = 'tab_loading_' + v_tab.id;
  var v_tab_check_span = document.getElementById('tab_check');
  v_tab_check_span.id = 'tab_check_' + v_tab.id;
  // v_tab_close_span.id = 'tab_close_' + v_tab.id;
  // v_tab_close_span.onclick = function(e) {
  //   var v_current_tab = v_tab;
  //   beforeCloseTab(e,
  //     function() {
  //       removeTab(v_current_tab);
  //     });
  // };

  var v_html =
  "<div class='p-2'>" +
    "<button id='bt_refresh_" + v_tab.id + "' class='btn btn-primary btn-sm my-2 mr-1' title='Refresh'><i class='fas fa-sync-alt mr-2'></i>Refresh</button>" +
    "<span id='div_query_info_" + v_tab.id + "' class='query_info'></span>" +
    "<div id='div_result_" + v_tab.id + "' class='omnidb__query-result-tabs__content tabulator-custom' style='width: 100%; overflow: auto;'></div>"
  "</div>";

  // var v_div = document.getElementById('div_' + v_tab.id);
  // v_div.innerHTML = v_html;
  v_tab.elementDiv.innerHTML = v_html;

  // var v_currTabControl = createTabControl({
  //   p_div: v_tab.id + '_tabs',
  //   p_hierarchy: 'secondary'
  // });

  var v_bt_refresh = document.getElementById('bt_refresh_' + v_tab.id);

  var v_resizeFunction = function () {
    var v_tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
    if (v_tab_tag.div_result) {
      v_tab_tag.div_result.style.height = window.innerHeight - $(v_tab_tag.div_result).offset().top - (1.25)*v_font_size + 'px';
      setTimeout(function(){
        if (v_tab_tag.ht!=null) {
          v_tab_tag.ht.render();
        }
      },400);
    }
  }

  var v_tag = {
    tab_id: v_tab.id,
    tabTitle: 'teste',
    divTree: document.getElementById(v_tab.id + '_tree'),
    divLeft: document.getElementById(v_tab.id + '_div_left'),
    divRight: document.getElementById(v_tab.id + '_div_right'),
    // tab_title_span : v_tab_title_span,
    // tab_close_span : v_tab_close_span,
    query_info: document.getElementById('div_query_info_' + v_tab.id),
    div_result: document.getElementById('div_result_' + v_tab.id),
    bt_refresh: v_bt_refresh,
    tabControl: v_connTabControl.selectedTab.tag.tabControl,
    ht: null,
    query: p_query,
    actions: p_actions,
    mode: 'monitor_grid',
    resize: v_resizeFunction
  };

  //Adding action to button
  v_bt_refresh.onclick = function() {
    refreshMonitoring(v_tag);
  };

  v_tab.tag = v_tag;

  // Creating + tab in the outer tab list
  var v_add_tab = v_connTabControl.selectedTab.tag.tabControl.createTab(
    {
      p_name: '+',
      p_isDraggable: false,
      p_close: false,
      p_selectable: false,
      p_clickFunction: function(e) {
        showMenuNewTab(e);
      }
    });
  v_add_tab.tag = {
    mode: 'add'
  }

  setTimeout(function() {
    v_resizeFunction();
    refreshMonitoring(v_tag);
  },10);

};

/// <summary>
/// Refreshes monitoring tab.
/// </summary>
function refreshMonitoring(p_tab_tag) {

  if (!p_tab_tag)
    var p_tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;

  execAjax('/refresh_monitoring/',
      JSON.stringify({"p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
                      "p_tab_id": v_connTabControl.selectedTab.id,
                      "p_query": p_tab_tag.query}),
      function (p_return) {
        let data = p_return.v_data;
  
        if (p_tab_tag.tabulator != null) {
          p_tab_tag.tabulator.destroy();
          p_tab_tag.tabulator = null;
        }
  
        let cellContextMenu = [
          {
            label:
              '<div style="position: absolute;"><i class="fas fa-copy cm-all" style="vertical-align: middle;"></i></div><div style="padding-left: 30px;">Copy</div>',
            action: function (e, cell) {
              cell.getTable().copyToClipboard("selected");
            },
          },
          {
            label:
              '<div style="position: absolute;"><i class="fas fa-edit cm-all" style="vertical-align: middle;"></i></div><div style="padding-left: 30px;">View Content</div>',
            action: (e, cell) => {
              cellDataModal(null, null, null, cell.getValue(), false);
            },
          },
        ];
  
        p_tab_tag.query_info.innerHTML = `Number of records: ${data.data.length}`;
  
        data.data.forEach((col, idx) => {
          col.actions = [...p_tab_tag.actions];
        });
  
        function actionsFormatter(cell, formatterParams, onRendered) {
          let sourceDataRow = cell.getRow().getData();
          let actionsWrapper = document.createElement("div");

          cell.getValue().forEach((actionItem) => {
            let iconClassName;
            if (actionItem.icon.includes("fa-times")) {
              iconClassName = `${actionItem.icon} text-danger`;
            } else {
              iconClassName = `${actionItem.icon} omnidb__theme-icon--primary`;
            }

            const actionWrapper = document.createElement("div");
            actionWrapper.className = "text-center";
            const actionIcon = document.createElement("i");
            actionIcon.className = `actionable_icon ${iconClassName}`;

            actionIcon.onclick = () => {
              monitoringAction(sourceDataRow, actionItem.action);
            };

            actionWrapper.appendChild(actionIcon);
            actionsWrapper.appendChild(actionWrapper);
          });
          return actionsWrapper;
        }
  
        p_tab_tag.tabulator = new Tabulator(p_tab_tag.div_result, {
          data: data.data,
          height: "90vh",
          width: "100%",
          autoColumns: true,
          layout: "fitDataStretch",
          columnDefaults: {
            headerHozAlign: "left",
            headerSort: false,
          },
          autoColumnsDefinitions: function (definitions) {
            //definitions - array of column definition objects
  
            definitions.forEach((column) => {
              column.contextMenu = cellContextMenu;
            });
  
            let updatedDefinitions = definitions.filter(
              (column) => column.title != "actions"
            );
  
            updatedDefinitions.unshift({
              title: "actions",
              field: "actions",
              formatter: actionsFormatter,
              hozAlign: "center",
              frozen: true,
              clipboard: false,
            });
            updatedDefinitions.unshift({
              formatter: "rownum",
              hozAlign: "center",
              width: 40,
              frozen: true,
            });
  
            return updatedDefinitions;
          },
          selectable: true,
          clipboard: "copy",
          clipboardCopyConfig: {
            columnHeaders: false, //do not include column headers in clipboard output
          },
          clipboardCopyRowRange: "selected",
        });
      },
      function(p_return) {
        if (p_return.v_data.password_timeout) {
          showPasswordPrompt(
            v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            function() {
              refreshMonitoring(p_tab_tag);
            },
            null,
            p_return.v_data.message
          );
        }
        else {
          showToast("error", p_return.v_data.message)
        }
      },
      'box',
      true);

}

export { v_createMonitoringTabFunction, refreshMonitoring }