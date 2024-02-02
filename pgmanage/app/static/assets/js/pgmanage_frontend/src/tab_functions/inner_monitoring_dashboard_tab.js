import { renameTab, removeTab, showMenuNewTab } from '../workspace'
import { beforeCloseTab } from "../create_tab_functions"
import {
  refreshMonitorUnitsObjects,
  showMonitorUnitList,
  refreshMonitorDashboard,
  closeMonitorDashboardTab,
  saveMonitorScript,
  testMonitorScript,
  selectUnitTemplate
} from "../monitoring";
import { settingsStore } from '../stores/stores_initializer';

var v_createMonitorDashboardTabFunction = function() {

  // Removing last tab of the inner tab list
  v_connTabControl.selectedTab.tag.tabControl.removeLastTab();

  // Creating tab name pattern.
  let v_name_html =
  '<span id="tab_title">' +
    ' Monitoring' +
  '</span>' +
  '<span id="tab_loading" style="visibility:hidden;">' +
    '<i class="tab-icon node-spin"></i>' +
  '</span>' +
  '<i title="" id="tab_check" style="display: none;" class="fas fa-check-circle tab-icon icon-check"></i>';

  // Creating monitoring tab in the inner tab list
  var v_tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
    p_icon: '<i class="fas fa-chart-bar icon-tab-title"></i>',
    p_name: v_name_html,
    p_selectFunction: function() {
      if(this.tag != null) {
        this.tag.resize();
        refreshMonitorUnitsObjects();
      }
    },
    p_closeFunction: function(e,p_tab) {
      var v_current_tab = p_tab;
      beforeCloseTab(e,
        function() {
          closeMonitorDashboardTab(v_tab);
          if (v_tab.tag.tabCloseFunction)
            v_tab.tag.tabCloseFunction(v_tab.tag);
        });
    },
    p_dblClickFunction: renameTab
  });

  // Selecting newly created tab.
  v_connTabControl.selectedTab.tag.tabControl.selectTab(v_tab);

  // Adding unique names to spans.
  var v_tab_title_span = document.getElementById('tab_title');
  v_tab_title_span.id = 'tab_title_' + v_tab.id;
  var v_tab_loading_span = document.getElementById('tab_loading');
  v_tab_loading_span.id = 'tab_loading_' + v_tab.id;
  var v_tab_check_span = document.getElementById('tab_check');
  v_tab_check_span.id = 'tab_check_' + v_tab.id;

  var v_html =
    `<div class='omnidb__monitoring-result-tabs'>
      <div class='container-fluid'>
        <button id='${v_tab.id}_refresh_dashboard' class='btn btn-primary btn-sm my-2 mr-2'>
        <i class='fas fa-sync-alt mr-2'></i>Refresh All</button>
        <button id='${v_tab.id}_manage_units' class='btn btn-primary btn-sm my-2'>Manage Units</button>
        <div id='dashboard_${v_tab.id}' class='dashboard_all row'></div>
      </div>
    </div>`;

  // Updating the html.
  v_tab.elementDiv.innerHTML = v_html;

  let refresh_btn = document.getElementById(`${v_tab.id}_refresh_dashboard`)
  refresh_btn.onclick = function() { refreshMonitorDashboard(true) }

  let manage_units_btn = document.getElementById(`${v_tab.id}_manage_units`)
  manage_units_btn.onclick = function() { showMonitorUnitList() }

  var v_resizeFunction = function () {
    var v_tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
    if (v_tab_tag.dashboard_div) {
      v_tab_tag.dashboard_div.style.height = window.innerHeight - $(v_tab_tag.dashboard_div).offset().top - $(v_tab_tag.dashboard_div.parentElement).scrollTop() - (0.833)*v_font_size + "px";
    }
  }

  var v_tag = {
    tab_id: v_tab.id,
    mode: 'monitor_dashboard',
    dashboard_div: document.getElementById('dashboard_' + v_tab.id),
    unit_list_div: document.getElementById('unit_list_div_' + v_tab.id),
    unit_list_grid_div: document.getElementById('unit_list_grid_' + v_tab.id),
    unit_list_grid: null,
    unit_list_id_list: [],
    tab_title_span : v_tab_title_span,
    tab_loading_span : v_tab_loading_span,
    tab_check_span : v_tab_check_span,
    tabControl: v_connTabControl.selectedTab.tag.tabControl,
    units: [],
    unit_sequence: 0,
    tab_active: true,
    connTabTag: v_connTabControl.selectedTab.tag,
    resize: v_resizeFunction,
    tabCloseFunction: function(p_tag) {
      for (var i=0; i<p_tag.units.length; i++) {
        try {
          p_tag.units[i].object.destroy();
        }
        catch(err) {
        }
      }
    }
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
  },10);

};

var v_createNewMonitorUnitTabFunction = function() {

  // Removing last tab of the inner tab list
  v_connTabControl.selectedTab.tag.tabControl.removeLastTab();

  let v_name_html =
  '<span id="tab_title">Monitor Unit</span>' +
  '<span id="tab_loading" style="visibility:hidden;">' +
    '<i class="tab-icon node-spin"></i>' +
  '</span>' +
  '<i title="" id="tab_check" style="display: none;" class="fas fa-check-circle tab-icon icon-check"></i>';

  // Creating console tab in the inner tab list
  var v_tab = v_connTabControl.selectedTab.tag.tabControl.createTab({
    p_icon: '<i class="fas fa-align-left icon-tab-title"></i>',
    p_name: v_name_html,
    p_selectFunction: function() {
      if(this.tag != null) {
        this.tag.resize();
      }
    },
    p_closeFunction: function(e,p_tab) {
      var v_current_tab = p_tab;
      beforeCloseTab(e,
        function() {
          removeTab(v_current_tab);
          if (v_tab.tag.tabCloseFunction)
            v_tab.tag.tabCloseFunction(v_tab.tag);
        });
    },
    p_dblClickFunction: renameTab
  });

  // Adding unique names to spans.
  var v_tab_title_span = document.getElementById('tab_title');
  v_tab_title_span.id = 'tab_title_' + v_tab.id;
  var v_tab_loading_span = document.getElementById('tab_loading');
  v_tab_loading_span.id = 'tab_loading_' + v_tab.id;
  var v_tab_check_span = document.getElementById('tab_check');
  v_tab_check_span.id = 'tab_check_' + v_tab.id;

  v_connTabControl.selectedTab.tag.tabControl.selectTab(v_tab);

  var v_html =
  '<button id="test_monitor_script_' + v_tab.id + '" class="btn btn-secondary btn-sm my-1 mr-1">Test</button>' +
  '<button id="save_monitor_script_' + v_tab.id + '" class="btn btn-secondary btn-sm my-1">Save</button>' +
  '<div class="form-row">' +
  '  <div class="col-md-3 mb-3">' +
  '    <label for="conn_form_title">Name</label>' +
  '    <input type="text" class="form-control" id="txt_unit_name_' + v_tab.id + '" placeholder="Name">' +
  '  </div>' +
  '  <div class="col-md-3 mb-3">' +
  '    <label for="conn_form_type">Type</label>' +
  '    <select id="select_type_' + v_tab.id + '" class="form-control">' +
  '      <option value="timeseries">Timeseries</option>' +
  '      <option value="chart">Chart (No Append)</option>' +
  '      <option value="grid">Grid</option>' +
  '      <option value="graph">Graph</option>' +
  '    </select>' +
  '  </div>' +
  '  <div class="col-md-3 mb-3">' +
  '    <label for="conn_form_title">Refresh Interval</label>' +
  '    <input type="text" class="form-control" id="txt_interval_' + v_tab.id + '" placeholder="Title">' +
  '  </div>' +
  '  <div class="col-md-3 mb-3">' +
  '    <label for="conn_form_type">Template</label>' +
  '    <select id="select_template_' + v_tab.id + '" class="form-control">' +
  '      <option value=-1>Select Template</option>' +
  '    </select>' +
  '  </div>' +
  '</div>' +
  '<div class="form-row">' +
  '  <div class="col-md-6">' +
  '    <div id="txt_data_' + v_tab.id + '" style=" width: 100%; height: 250px;"></div>' +
  '  </div>' +
  '  <div class="col-md-6">' +
  '    <div id="txt_script_' + v_tab.id + '" style=" width: 100%; height: 250px;"></div>' +
  '  </div>' +
  "</div>";

  var v_div = document.getElementById('div_' + v_tab.id);
  v_div.innerHTML = v_html;

  let save_monitor_script = document.getElementById(`save_monitor_script_${v_tab.id}`)
  save_monitor_script.onclick = function() { saveMonitorScript() }

  let test_monitor_script = document.getElementById(`test_monitor_script_${v_tab.id}`)
  test_monitor_script.onclick = function() { testMonitorScript() }

  let select_template = document.getElementById(`select_template_${v_tab.id}`)
  select_template.onchange = (event) => { selectUnitTemplate(event.target.value)}

  var langTools = ace.require("ace/ext/language_tools");

  var v_txt_script = document.getElementById('txt_script_' + v_tab.id);
  var v_editor = ace.edit('txt_script_' + v_tab.id);
  v_editor.$blockScrolling = Infinity;
  v_editor.setTheme("ace/theme/" + settingsStore.editorTheme);
  v_editor.session.setMode("ace/mode/python");
  v_editor.setFontSize(Number(settingsStore.fontSize));
  v_editor.commands.bindKey("ctrl-space", null);
  v_editor.commands.bindKey("Cmd-,", null)
  v_editor.commands.bindKey("Ctrl-,", null)
  v_editor.commands.bindKey("Cmd-Delete", null)
  v_editor.commands.bindKey("Ctrl-Delete", null)
  v_editor.commands.bindKey("Ctrl-Up", null)
  v_editor.commands.bindKey("Ctrl-Down", null)

  var v_txt_data = document.getElementById('txt_data_' + v_tab.id);
  var v_editor_data = ace.edit('txt_data_' + v_tab.id);
  v_editor_data.$blockScrolling = Infinity;
  v_editor_data.setTheme("ace/theme/" + settingsStore.editorTheme);
  v_editor_data.session.setMode("ace/mode/python");
  v_editor_data.setFontSize(Number(settingsStore.fontSize));
  v_editor_data.commands.bindKey("ctrl-space", null);
  v_editor_data.commands.bindKey("Cmd-,", null)
  v_editor_data.commands.bindKey("Ctrl-,", null)
  v_editor_data.commands.bindKey("Cmd-Delete", null)
  v_editor_data.commands.bindKey("Ctrl-Delete", null)
  v_editor_data.commands.bindKey("Ctrl-Up", null)
  v_editor_data.commands.bindKey("Ctrl-Down", null)

  v_txt_script.onclick = function() {

    v_editor.focus();

  };

  var v_resizeFunction = function () {
    var v_tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
    if (v_tab_tag.editorDataDiv) {
      var v_new_height = window.innerHeight - $(v_tab_tag.editorDataDiv).offset().top - v_font_size + 'px';
      v_tab_tag.editorDiv.style.height = v_new_height;
      v_tab_tag.editorDataDiv.style.height = v_new_height;
      v_tab_tag.editor.resize();
      v_tab_tag.editor_data.resize();
    }
  }

  var v_tag = {
    tab_id: v_tab.id,
    mode: 'monitor_unit',
    editor: v_editor,
    editor_data: v_editor_data,
    editorDiv: v_txt_script,
    editorDataDiv: v_txt_data,
    editorDivId: 'txt_script_' + v_tab.id,
    tab_title_span : v_tab_title_span,
    tab_loading_span : v_tab_loading_span,
    tab_check_span : v_tab_check_span,
    select_type: document.getElementById('select_type_' + v_tab.id),
    select_template: document.getElementById('select_template_' + v_tab.id),
    input_unit_name: document.getElementById('txt_unit_name_' + v_tab.id),
    input_interval: document.getElementById('txt_interval_' + v_tab.id),
    div_result: document.getElementById('monitoring_unit_test_result'),
    div_result_label: document.getElementById('monitoring_unit_test_legend'),
    bt_test: document.getElementById('bt_test_' + v_tab.id),
    tabControl: v_connTabControl.selectedTab.tag.tabControl,
    unit_id: null,
    object: null,
    resize: v_resizeFunction,
    tabCloseFunction: function(p_tag) {
      try {
        p_tag.object.destroy();
      }
      catch(err) {
      }
    }
  };

  v_tab.tag = v_tag;

  // Creating + tab in the outer tab list
  var v_add_tab = v_connTabControl.selectedTab.tag.tabControl.createTab(
    {
      p_name: '+',
      p_close: false,
      p_isDraggable: false,
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
  },10);

};

export { v_createMonitorDashboardTabFunction, v_createNewMonitorUnitTabFunction }