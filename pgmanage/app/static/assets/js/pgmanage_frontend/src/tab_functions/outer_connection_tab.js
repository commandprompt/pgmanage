/*
This file is part of OmniDB.
OmniDB is open-source software, distributed "AS IS" under the MIT license in the hope that it will be useful.

The MIT License (MIT)

Portions Copyright (c) 2015-2020, The OmniDB Team
Portions Copyright (c) 2017-2020, 2ndQuadrant Limited

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

import {
  changeDatabase,
  refreshTreeHeight,
  toggleTreeTabsContainer,
  checkTabStatus,
  refreshHeights,
  resizeConnectionHorizontal,
  resizeTreeVertical,
  toggleTreeContainer,
  toggleConnectionAutocomplete
} from "../workspace";
import { beforeCloseTab } from "../create_tab_functions";
import { connectionsStore } from "../stores/connections.js";
import { createRequest } from "../long_polling";
import { queryRequestCodes } from "../constants";
import { cancelMonitorUnits } from "../monitoring";
import { createTabControl } from "../tabs";
import moment from "moment";
import { endLoading } from "../ajax_control";
import { showToast } from "../notification_control";
import { settingsStore } from "../stores/stores_initializer";
import { TabulatorFull as Tabulator } from "tabulator-tables";

var v_createConnTabFunction = function(p_index,p_create_query_tab = true, p_name = false, p_tooltip_name = false) {
  // Creating the first outer tab without any connections created.
  if (connectionsStore.connections.length==0) {
    v_connTabControl.selectTabIndex(v_connTabControl.tabList.length-2);
    showToast("error", "Create connections first.")
  }
  else {
    // v_connTabControl.removeLastTab();

    let v_conn = connectionsStore.connections[0];
    for (let i = 0; i < connectionsStore.connections.length; i++) {
      if (connectionsStore.connections[i].id === p_index) {
        // patch the connection last used date when connecting
        // to refresh last-used labels on the welcome screen
        connectionsStore.connections[i].last_access_date = moment.now()
        v_conn = connectionsStore.connections[i];
      }
    }
    var v_conn_name = '';
    if (p_name) {
      v_conn_name = p_name;
    }
    if (v_conn_name === '' && v_conn.alias && v_conn.alias !== '') {
      v_conn_name = v_conn.alias;
    }
    if (!p_tooltip_name) {
      p_tooltip_name = '';

      if (v_conn.conn_string && v_conn.conn_string !== '') {
        if (v_conn.alias) {
          p_tooltip_name += '<h5 class="my-1">' + v_conn.alias + '</h5>';
        }
        p_tooltip_name += '<div class="mb-1">' + v_conn.conn_string + '</div>';
      }
      else {
        if (v_conn.alias) {
          p_tooltip_name += '<h5 class="my-1">' + v_conn.alias + '</h5>';
        }
        if (v_conn.details1) {
          p_tooltip_name += '<div class="mb-1">' + v_conn.details1 + '</div>';
        }
        if (v_conn.details2) {
          p_tooltip_name += '<div class="mb-1">' + v_conn.details2 + '</div>';
        }
      }
    }
    const imgPath = import.meta.env.MODE === 'development' ? `${import.meta.env.BASE_URL}src/assets/images/` : `${import.meta.env.BASE_URL}assets/`
    // vite generates two svg files for each technology icon, in current case we take second one
    let imgName
    if (import.meta.env.MODE === 'development' || v_conn.technology === 'sqlite') {
      imgName = v_conn.technology;
    } else {
      imgName = `${v_conn.technology}2`;
    }

    let v_icon = `<img src="${v_url_folder}${imgPath}${imgName}.svg"/>`

    var v_tab = v_connTabControl.createTab({
      p_icon: v_icon,
      p_name: v_conn_name,
      p_selectFunction: function() {
        document.title = 'PgManage'
        if(this.tag != null) {
          checkTabStatus(this);
          refreshHeights(true);
        }
        if(this.tag != null && this.tag.tabControl != null && this.tag.tabControl.selectedTab.tag.editor != null) {
            this.tag.tabControl.selectedTab.tag.editor.focus();
        }
        $('[data-toggle="tooltip"]').tooltip({animation:true});// Loads or Updates all tooltips
      },
      p_close: true,
      p_closeFunction: function(e,p_tab) {
        var v_this_tab = p_tab;
        beforeCloseTab(e,
          function() {
            var v_tabs_to_remove = [];

            for (var i=0; i < p_tab.tag.tabControl.tabList.length; i++) {

              var v_tab = p_tab.tag.tabControl.tabList[i];
              if (v_tab.tag!=null) {
                if (v_tab.tag.mode=='query' || v_tab.tag.mode=='edit' || v_tab.tag.mode=='debug' || v_tab.tag.mode=='console') {
                  var v_message_data = { tab_id: v_tab.tag.tab_id, tab_db_id: null , conn_tab_id: p_tab.id};
                  if (v_tab.tag.mode=='query')
                    v_message_data.tab_db_id = v_tab.tag.tab_db_id;

                  if (v_tab.tag.mode === 'console' && !!v_tab.app) {
                    v_tab.app.unmount()
                  }
                  v_tabs_to_remove.push(v_message_data);
                }
                else if (v_tab.tag.mode=='monitor_dashboard') {
                  v_tab.tag.tab_active = false;
                  cancelMonitorUnits(v_tab.tag);
                }
              }

              if (v_tab.tag.tabCloseFunction)
                v_tab.tag.tabCloseFunction(v_tab.tag);
            }

            var v_message_data = { conn_tab_id: p_tab.tag.tab_id, tab_db_id: null, tab_id: null };
            v_tabs_to_remove.push(v_message_data);

            if (v_tabs_to_remove.length>0) {
              createRequest(queryRequestCodes.CloseTab, v_tabs_to_remove);
            }
            if (!!v_this_tab.tree) {
              v_this_tab.tree.unmount()
            }
            v_this_tab.removeTab();
          });
      },
      p_tooltip: p_tooltip_name
    });

    v_connTabControl.selectTab(v_tab);

    var v_width = Math.ceil((300/window.innerWidth)*100);
    var v_complement_width = 100 - v_width;

    var v_html =
    '<div style="position: relative;">' +
      '<div style="display: grid; grid-template-areas: \'left right\'; grid-template-columns: auto minmax(0, 1fr);">' +
        '<div id="' + v_tab.id + '_div_left" class="omnidb__workspace__div-left col" style="max-width: 300px; width: 300px;">' +
          "<div class='row'>" +

            // "<div onmousedown='resizeHorizontal(event)' style='width: 10px; height: 100%; cursor: ew-resize; position: absolute; top: 0px; right: 0px;'><div class='resize_line_vertical' style='width: 5px; height: 100%; border-right: 1px dashed #acc4e8;'></div><div style='width:5px;'></div></div>" +


            '<div class="omnidb__workspace__content-left border-right">' +
              '<div id="' + v_tab.id + '_details" class="omnidb__workspace__connection-details"></div>' +
              '<div id="' + v_tab.id + '_tree" style="overflow: auto; flex-grow: 1; transition: scroll 0.3s;"></div>' +
              '<div id="' + v_tab.id + '_left_resize_line_horizontal" class="omnidb__resize-line__container--horizontal"><div class="resize_line_horizontal"></div><div style="height:5px;"></div></div>' +
              '<div id="tree_tabs_parent_' + v_tab.id + '" class="omnidb__tree-tabs--not-in-view omnidb__tree-tabs" style="position: relative;flex-shrink: 0;">' +
                '<div id="' + v_tab.id + '_loading" class="div_loading" style="z-index: 1000;">' +
                  '<div class="div_loading_cover"></div>' +
                  '<div class="div_loading_content">' +
                  '  <div class="spinner-border text-primary" style="width: 4rem; height: 4rem;" role="status">' +
                  '    <span class="sr-only ">Loading...</span>' +
                  '  </div>' +
                  '</div>' +
                '</div>' +
                '<button type="button" class="btn btn-icon btn-icon-secondary omnidb__tree-tabs__toggler mr-2"><i class="fas fa-arrows-alt-v"></i></button>' +
                '<div id="tree_tabs_' + v_tab.id + '" class="omnidb__tree-tabs__container" style="position: relative;"></div>' +
              '</div>' +
            '</div>' +
          '</div>' +
          '<div id="' + v_tab.id + '_left_resize_line_vertical" class="omnidb__resize-line__container--vertical"><div class="resize_line_vertical"></div></div>' +
        '</div>' +//.div_left
        '<div id="' + v_tab.id + '_div_right" class="omnidb__workspace__div-right" style="position: relative;">' +
          // "<div class='row'>" +
             '<button id="' + v_tab.id + '_tree_toggler" type="button" class="py-4 px-0 btn btn-secondary omnidb__tree__toggler"><i class="fas fa-arrows-alt-h"></i></button>' +
            '<div id="' + v_tab.id + '_tabs" class="w-100"></div>' +
          // "</div>" +
        '</div>' +//.div_right
      '</div>' +//.row

    '</div>';
    // var v_tab_title_span = document.getElementById('tab_title');
    // v_tab_title_span.id = 'tab_title_' + v_tab.id;
    var v_tab_title_span = $(v_tab.elementA).find('.omnidb__tab-menu__link-name');
    if (v_tab_title_span) {
      v_tab_title_span.attr('id', 'tab_title_' + v_tab.id);
    }


    v_tab.elementDiv.innerHTML = v_html;

    let toggle_button = document.querySelector(`#tree_tabs_parent_${v_tab.id} button`)
    toggle_button.onclick = function() {toggleTreeTabsContainer(`tree_tabs_parent_${v_tab.id}`, `${v_tab.id}_left_resize_line_horizontal`)}

    let resize_vertical = document.getElementById(`${v_tab.id}_left_resize_line_vertical`)
    resize_vertical.onmousedown = (event) => { resizeConnectionHorizontal(event) }

    let resize_horizontal = document.getElementById(`${v_tab.id}_left_resize_line_horizontal`)
    resize_horizontal.onmousedown = (event) => { resizeTreeVertical(event) }

    let tree_toggler = document.getElementById(`${v_tab.id}_tree_toggler`)
    tree_toggler.onclick = function() { toggleTreeContainer() }

    // Tab control under the tree
    var v_treeTabs = createTabControl({ p_div: 'tree_tabs_' + v_tab.id });

    // Functions called when Properties and DDL tabs are clicked on
    var v_selectPropertiesTabFunc = function() {
      v_treeTabs.selectTabIndex(0);
      v_connTabControl.selectedTab.tag.currTreeTab = 'properties';
      refreshTreeHeight();
    }

    var v_selectDDLTabFunc = function() {
      v_treeTabs.selectTabIndex(1);
      v_connTabControl.selectedTab.tag.currTreeTab = 'ddl';
      refreshTreeHeight();
    }

    var v_properties_tab = v_treeTabs.createTab(
      {
        p_name: 'Properties',
        p_close: false,
        p_clickFunction: function(e) {
          v_selectPropertiesTabFunc();
        }
      });
    var v_ddl_tab = v_treeTabs.createTab(
      {
        p_name: 'DDL',
        p_close: false,
        p_clickFunction: function(e) {
          v_selectDDLTabFunc();
        }
      });
    v_treeTabs.selectTabIndex(0);

    // Tab control on the right (for queries, consoles, etc)
    // var v_currTabControl = createTabControl({ p_div: v_tab.id + '_tabs' });
    var v_currTabControl = createTabControl({
      p_div: v_tab.id + '_tabs',
      p_hierarchy: 'secondary'
    });
    v_currTabControl.createTab(
      {
        p_name: '+',
        p_isDraggable: false,
        p_close: false,
        p_selectable: false,
        p_clickFunction: function(e) {
          showMenuNewTab(e);
        }
      });

    //DDL editor
    var v_ddl_div = v_ddl_tab.elementDiv;

    var langTools = ace.require('ace/ext/language_tools');
    var v_editor = ace.edit(v_ddl_tab.elementDiv);
    v_editor.$blockScrolling = Infinity;
    v_editor.setTheme('ace/theme/' + settingsStore.editorTheme);
    v_editor.session.setMode('ace/mode/sql');

    v_editor.setFontSize(Number(settingsStore.fontSize));

    v_editor.commands.bindKey('ctrl-space', null);

    //Remove shortcuts from ace in order to avoid conflict with omnidb shortcuts
    v_editor.commands.bindKey('Cmd-,', null)
    v_editor.commands.bindKey('Ctrl-,', null)
    v_editor.commands.bindKey('Cmd-Delete', null)
    v_editor.commands.bindKey('Ctrl-Delete', null)
    v_editor.commands.bindKey('Ctrl-Up', null)
    v_editor.commands.bindKey('Ctrl-Down', null)
    v_editor.setReadOnly(true);

    v_ddl_div.onclick = function() {
      v_editor.focus();
    };

    //Properties Grid
    v_properties_tab.elementDiv.innerHTML =
    "<div class='p-2'>" +
      "<div id='div_properties_result_" + v_tab.id + "' class='tabulator-custom simple' style='width: 100%; overflow: hidden;'></div>" +
    "</div>";
    var v_divProperties = document.getElementById('div_properties_result_' + v_tab.id);
    // v_divProperties.classList.add('omnidb__theme-border--primary');
    // v_divProperties.style.overflow = 'hidden';
    var v_ddlProperties = v_ddl_tab.elementDiv;


    let tabulator = new Tabulator(v_divProperties, {
      columnDefaults: {
        headerHozAlign: "left",
        headerSort: false,
      },
      data: [],
      columns: [
        {
          title: "property",
          field: "0",
          resizable: false,
        },
        {
          title: "value",
          field: "1",
          resizable: false,
        },
      ],
      layout: "fitDataStretch",
      selectable: false,
    });

    var v_tag = {
      tab_id: v_tab.id,
      tabControl: v_currTabControl,
      tabTitle: v_tab_title_span,
      divDetails: document.getElementById(v_tab.id + '_details'),
      divTree: document.getElementById(v_tab.id + '_tree'),
      divTreeTabs: document.getElementById('tree_tabs_parent_' + v_tab.id),
      divProperties: v_divProperties,
      gridProperties: tabulator,
      gridPropertiesCleared: true,
      divDDL: v_ddlProperties,
      divLoading: document.getElementById(v_tab.id + '_loading'),
      divLeft: document.getElementById(v_tab.id + '_div_left'),
      divRight: document.getElementById(v_tab.id + '_div_right'),
      selectedDatabaseIndex: 0,
      connTabControl: v_connTabControl,
      mode: 'connection',
      firstTimeOpen: true,
      TreeTabControl: v_treeTabs,
      treeTabsVisible: false,
      currTreeTab: null,
      ddlEditor: v_editor,
      consoleHistoryFecthed: false,
      consoleHistoryList: null
    };

    v_tab.tag = v_tag;

    v_tag.selectPropertiesTabFunc = v_selectPropertiesTabFunc;
    v_tag.selectDDLTabFunc = v_selectDDLTabFunc;

    var v_index = connectionsStore.connections[0].id;
    if (p_index) {
      v_index = p_index;
    }

    changeDatabase(v_index);

    if (p_create_query_tab) {
       v_connTabControl.tag.createConsoleTab();
       v_connTabControl.tag.createQueryTab();
    }

    // Creating `Add` tab in the outer tab list
    // v_connTabControl.createAddTab();

    $('[data-toggle="tooltip"]').tooltip({animation:true});// Loads or Updates all tooltips

    setTimeout(function() {
      v_selectPropertiesTabFunc();
    },10);

  }

  endLoading();

}

function refreshOuterConnectionHeights() {
  var v_tab_tag = v_connTabControl.selectedTab.tag;
  if (v_tab_tag.divLeft) {
    // Checking if the element is shrunk before resizing children elements.
    var v_is_shrunk = $(v_tab_tag.divLeft).hasClass('omnidb__workspace__div-left--shrink');
    // if (!v_is_shrunk) {
      var v_div_left = v_connTabControl.selectedTab.tag.divLeft;
      var v_div_right = v_connTabControl.selectedTab.tag.divRight;
      var v_totalWidth = v_connTabControl.selectedDiv.getBoundingClientRect().width;
      var v_div_left_width_value = v_connTabControl.selectedTab.tag.divLeft.getBoundingClientRect().width;
      var v_right_width_value = v_totalWidth - v_div_left_width_value;
      // v_div_left.style['max-width'] = v_div_left_width_value + 'px';
      // v_div_left.style['width'] = v_div_left_width_value + 'px';
      v_div_right.style['max-width'] = v_right_width_value + 'px';
      v_div_right.style['width'] = v_right_width_value + 'px';
      // v_div_left.style['flex'] = '0 0 ' + v_div_left_width_value + 'px';
      // var v_right_width_value = v_totalWidth - v_div_left_width_value;
      // v_div_right.style['max-width'] = v_right_width_value + 'px';
      // v_div_right.style['flex'] = '0 0 ' + v_right_width_value + 'px';
    // }
  }
}

function addDbTreeHeader(header_div, tab_id, database_name, conn_id) {
  let autocomplete_btn_id = `autocomplete_toggler_${tab_id}`
  let connection = connectionsStore.getConnection(conn_id)

  let autocomplete_switch_status =
    connection.autocomplete !== false
      ? " checked "
      : "";

  header_div.innerHTML = `<i class="fas fa-server mr-1"></i>selected DB:
      <b>${database_name}</b>
      <div class="omnidb__switch omnidb__switch--sm float-right" data-toggle="tooltip" data-placement="bottom" data-html="true" title="" data-original-title="<h5>Toggle autocomplete.</h5><div>Switch OFF <b>disables the autocomplete</b> on the inner tabs for this connection.</div>">
        <input type="checkbox" ${autocomplete_switch_status} id="${autocomplete_btn_id}" class="omnidb__switch--input">
        <label for="${autocomplete_btn_id}" class="omnidb__switch--label">
              <span>
                  <i class="fas fa-spell-check"></i>
              </span>
          </label>
  </div>`;

  let autocomplete_btn = document.getElementById(`${autocomplete_btn_id}`)
  autocomplete_btn.onchange = function() { toggleConnectionAutocomplete(autocomplete_btn_id, connection.id) }
}

export { v_createConnTabFunction, refreshOuterConnectionHeights, addDbTreeHeader }