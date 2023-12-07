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
import { initCreateTabFunctions } from './create_tab_functions'
import { getTreeSqlite } from './tree_context_functions/tree_sqlite'
import { refreshOuterConnectionHeights } from './tab_functions/outer_connection_tab'
import { getAllSnippets } from './tree_context_functions/tree_snippets'
import { getTreePostgresql, postgresqlTerminateBackend } from './tree_context_functions/tree_postgresql'
import { getTreeMysql, mysqlTerminateBackend } from './tree_context_functions/tree_mysql'
import { getTreeMariadb, mariadbTerminateBackend } from './tree_context_functions/tree_mariadb'
import { getTreeOracle, oracleTerminateBackend } from './tree_context_functions/tree_oracle'
import { connectionsModalInit, conn_app} from './connections_modal.js'
import { connectionsStore } from './stores/connections.js'
import { passwordModalsInit, showNewMasterPassPrompt, showMasterPassPrompt } from './passwords.js'
import { settingsModalInit } from './settings_modal.js'
import { format } from 'sql-formatter'
import ContextMenu from '@imengyu/vue3-context-menu'
import { createRequest } from './long_polling'
import { v_queryRequestCodes } from './query'
import { checkDebugStatus } from './debug'
import { startMonitorDashboard } from './monitoring'
import { createTabControl } from './tabs'
import { startLoading } from './ajax_control'
import axios from 'axios'
import { showAlert, showConfirm } from './notification_control'
import { emitter } from './emitter'
import { settingsStore } from './stores/settings'
import { startTutorial } from './tutorial'
import { welcomeScreenInit } from './tab_functions/welcome_screen'

let v_start_height;
/// <summary>
/// Startup function.
/// </summary>
$(function () {
  settingsModalInit()

  // Instantiating outer tab component.
  v_connTabControl = createTabControl({
    p_div: 'omnidb_main_tablist',
    p_hierarchy: 'primary'
  });

  // Objects to control sequential change of active database tabs.
  v_connTabControl.tag.change_active_database_call_list = [];
  v_connTabControl.tag.change_active_database_call_running = false;

  // Create the branding item for omnidb.
  if (v_connTabControl.tabList.length === 0) {
    // Creating the toggling button.
    // v_connTabControl.createTab(
    //   {
    //     p_icon: '<i class="fas fa-bars collapse-menu"></i>',
    //     p_name: 'Collapse menu',
    //     p_close: false,
    //     p_selectable: false,
    //     p_clickFunction: function(e) {
    //       v_connTabControl.toggleTabMenu();
    //       refreshHeights();
    //     },
    //   }
    // );

    v_connTabControl.createTab({
      p_icon: '<i class="fas fa-bolt"></i>',
      p_name: 'Connections',
      p_close: false,
      p_selectable: false,
      p_tooltip: 'Connections',
      p_clickFunction: function(e) {
        showMenuNewTabOuter(e);
      }
    });
  }

  // Instantiating functions responsible for creating all the different types of tabs.
  initCreateTabFunctions();

  // Creating the welcome tab.
  welcomeScreenInit()
  // Creating the snippets panel.
  v_connTabControl.tag.createSnippetPanel();

  passwordModalsInit()
  // Ask for master password
  if (master_key === 'new') {
    showNewMasterPassPrompt(`Please set your master password. It will be used to secure your connection credentials.`);
  } else if (master_key == 'False'){
    showMasterPassPrompt(`Please provide your master password to unlock your connection credentials for this session.`);
  } else {
    conn_app.mount("#connections-modal-wrap");
  }

  // Updating explain component choice.
  updateExplainComponent();

  // Retrieving global snippets
  getAllSnippets();

  // Creating omnis.
  v_omnis.root = document.getElementById('omnidb__main');
  v_omnis.div = document.createElement('div');
  v_omnis.div.setAttribute('id', 'omnis');
  v_omnis.div.classList.add('omnis');
  v_omnis.div.style.top = v_omnis.root.getBoundingClientRect().height - 45 + 'px';
  v_omnis.div.style.left = v_omnis.root.getBoundingClientRect().width - 45 + 'px';
  v_omnis.div.style['z-index'] = '99999999';
  v_omnis.div.innerHTML = v_omnis.template;
  document.body.appendChild(v_omnis.div);
  v_omnis.div.addEventListener('click',function(){
    startTutorial('getting_started');
  });

  // Loads or Updates all tooltips.
  $('[data-toggle="tooltip"]').tooltip({animation:true});

  connectionsModalInit()
});

function queueChangeActiveDatabaseThreadSafe(p_data) {
	v_connTabControl.tag.change_active_database_call_list.push(p_data);
	if (!v_connTabControl.tag.change_active_database_call_running) {
		changeActiveDatabaseThreadSafe(v_connTabControl.tag.change_active_database_call_list.pop());
	}
}

function changeActiveDatabaseThreadSafe(data) {
	v_connTabControl.tag.change_active_database_call_running = true;
  axios.post('/change_active_database/', data)
  .then((resp) => {
    v_connTabControl.tag.change_active_database_call_running = false;
    if (v_connTabControl.tag.change_active_database_call_list.length>0)
      changeActiveDatabaseThreadSafe(v_connTabControl.tag.change_active_database_call_list.pop());
  })
  .catch((error) => {
    console.log(error)
  })
}

/// <summary>
/// Changing database in the current connection tab.
/// </summary>
function changeDatabase(p_value) {
  // Emptying the details of the connected db.
  v_connTabControl.selectedTab.tag.divDetails.innerHTML = "";

  // Finding the connection object.
  var conn_object = null;
  for (var i = 0; i < connectionsStore.connections.length; i++) {
    if (p_value == connectionsStore.connections[i].id) {
      conn_object = connectionsStore.connections[i];
      break;
    }
  }
  // Selecting the first connection when none is found.
  if (!conn_object) {
    conn_object = connectionsStore.connections[0];
  }

  v_connTabControl.selectedTab.tag.selectedDatabaseIndex = parseInt(p_value);
  v_connTabControl.selectedTab.tag.selectedDBMS = conn_object.technology;
  v_connTabControl.selectedTab.tag.consoleHelp = conn_object.console_help;
  v_connTabControl.selectedTab.tag.selectedDatabase = conn_object.last_used_database || conn_object.service;
  v_connTabControl.selectedTab.tag.selectedTitle = conn_object.alias;

  queueChangeActiveDatabaseThreadSafe({
    database_index: v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
    tab_id: v_connTabControl.selectedTab.id,
    database: v_connTabControl.selectedTab.tag.selectedDatabase,
  });

  if (conn_object.technology == "postgresql") {
    getTreePostgresql(v_connTabControl.selectedTab.tag.divTree.id);
  } else if (conn_object.technology == "oracle") {
    getTreeOracle(v_connTabControl.selectedTab.tag.divTree.id);
  } else if (conn_object.technology == "mysql") {
    getTreeMysql(v_connTabControl.selectedTab.tag.divTree.id);
  } else if (conn_object.technology == "mariadb") {
    getTreeMariadb(v_connTabControl.selectedTab.tag.divTree.id);
  } else if (conn_object.technology == "sqlite") {
    getTreeSqlite(v_connTabControl.selectedTab.tag.divTree.id);
  }
}

/// <summary>
/// Check if there are troublesome tabs
/// </summary>
/// <param name="p_cancel_function">Ok function.</param>
/// <param name="p_ok_function">Cancel function.</param>
function checkBeforeChangeDatabase(p_cancel_function, p_ok_function) {
	for (var i=0; i < v_connTabControl.selectedTab.tag.tabControl.tabList.length; i++) {

		var v_tab = v_connTabControl.selectedTab.tag.tabControl.tabList[i];
		if (v_tab.tag!=null) {
      if (v_tab.tag.mode=='edit' || v_tab.tag.mode=='alter' || v_tab.tag.mode=='debug' || v_tab.tag.mode=='monitor_dashboard' || v_tab.tag.mode=='data_mining') {
        showAlert('Before changing connection please close any tab that belongs to the following types: <br/><br/><b>Edit Data<br/><br/>Alter Table<br/><br/>Function Debugging<br/><br/>Monitoring Dashboard<br/><br/>Advanced Object Search');
        //v_connTabControl.selectedTab.tag.dd_object.set("selectedIndex",v_connTabControl.selectedTab.tag.dd_selected_index);
        if (p_cancel_function!=null) {
          p_cancel_function();
        }
        return false;
      }
    }
	}
	if (p_ok_function!=null) {
    p_ok_function();
  }
  return true;
}

function adjustQueryTabObjects(p_all_tabs) {
	var v_dbms = v_connTabControl.selectedTab.tag.selectedDBMS;

	var v_target_div = null;
	if (!p_all_tabs) {
    v_target_div = v_connTabControl.selectedTab.tag.tabControl.selectedTab.elementDiv;
  }
	else {
    v_target_div = v_connTabControl.selectedTab.elementDiv;
  }

	var v_objects = $(v_target_div).find(".dbms_object").each(function() {
	  $( this ).css('display','none');
	});

	var v_objects = $(v_target_div).find("." + v_dbms + "_object").each(function() {
		if (!$( this ).hasClass('dbms_object_hidden')) {
      $( this ).css('display','inline-block');
      if ($(this).hasClass('form-check-inline'))
        $(this).removeAttr("style");
    }
	});

}

/// <summary>
/// Rename tab.
/// </summary>
function renameTab(p_tab) {

	showConfirm('<input id="tab_name"/ class="form-control" value="' + p_tab.tag.tab_title_span.innerHTML + '" style="width: 100%;">',
    function() {
			renameTabConfirm(p_tab,document.getElementById('tab_name').value);
    },
    null,
    function() {
      var v_input = document.getElementById('tab_name');
      v_input.focus();
      v_input.selectionStart = 0;
      v_input.selectionEnd = 10000;
    }
  );
	var v_input = document.getElementById('tab_name');
	v_input.onkeydown = function() {
		if (event.keyCode == 13) {
      document.getElementById('modal_message_ok').click();
    }
		else if (event.keyCode == 27) {
      document.getElementById('modal_message_cancel').click();
    }
	}

}

/// <summary>
/// Renames tab.
/// </summary>
function renameTabConfirm(p_tab, p_name) {

	p_tab.tag.tab_title_span.innerHTML = p_name;

}

/// <summary>
/// Removes tab.
/// </summary>
function removeTab(p_tab) {

	if (p_tab.tag.ht!=null) {
		p_tab.tag.ht.destroy();
		p_tab.tag.div_result.innerHTML = '';
	}

	if (p_tab.tag.editor!=null)
		p_tab.tag.editor.destroy();

	if (p_tab.tag.mode=='query' || p_tab.tag.mode=='edit' || p_tab.tag.mode=='console' || p_tab.tag.mode=='outer_terminal') {
		var v_message_data = { tab_id: p_tab.tag.tab_id, tab_db_id: null , conn_tab_id: v_connTabControl.selectedTab.id};
		if (p_tab.tag.mode=='query') {
      v_message_data.tab_db_id = p_tab.tag.tab_db_id;
    }

    createRequest(v_queryRequestCodes.CloseTab, [v_message_data]);
	}
	p_tab.removeTab();

}

/// <summary>
/// Resize snippet panel and transforms position when its visible.
/// </summary>
var resizeSnippetPanel = async function(p_left_pos_x = false) {
  if (v_connTabControl.snippet_tag !== undefined) {
    var v_element = $(v_connTabControl.snippet_tag.divPanel);
    var v_snippet_tag = v_connTabControl.snippet_tag;
    if (v_element.hasClass('omnidb__panel--slide-in')) {
      // Getting the selected tab
      var v_selected_tab = v_connTabControl.selectedTab;
      // Setting a default max top position for the toggling event.
      var v_target_tag_div_result_top = 30;
      // Updating the max top position considering if a tab is selected.
      if (v_connTabControl.selectedTab && v_connTabControl.selectedTab !== null) {
        if (v_connTabControl.selectedTab.tag.tabControl) {
          var v_target_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
          let current_query_tab = v_target_tag.currQueryTab;
          if (current_query_tab === 'data') {
            v_target_tag_div_result_top = v_target_tag.div_result.getBoundingClientRect().height - 25;
          }
          else if (current_query_tab === 'explain') {
            v_target_tag_div_result_top = v_target_tag.div_explain.getBoundingClientRect().height - 25;
          }
          else if (current_query_tab === 'message') {
            v_target_tag_div_result_top = v_target_tag.div_notices.getBoundingClientRect().height - 25;
          }
          else {
            v_target_tag_div_result_top = document.getElementsByClassName('omnidb__main')[0].getBoundingClientRect().height - 25;
          }
        }
        else {
          v_target_tag_div_result_top = document.getElementsByClassName('omnidb__main')[0].getBoundingClientRect().height - 25;
        }
      }
      else {
        v_target_tag_div_result_top = document.getElementsByClassName('omnidb__main')[0].getBoundingClientRect().height - 25;
      }


      v_snippet_tag.isVisible = true;
      v_element.css('transform', 'translateY(-' + v_target_tag_div_result_top + 'px)');
    }
    else {
      v_snippet_tag.isVisible = false;
      v_element.css('transform','translateY(0px)');
    }

    var v_snippet_tag = v_connTabControl.snippet_tag;
    var v_inner_snippet_tag = v_snippet_tag.tabControl.selectedTab.tag;

    var updateOuterSnippetLayout = new Promise(resolve => {
      setTimeout(function(){

        var v_totalWidth = v_snippet_tag.divLayoutGrid.getBoundingClientRect().width;
        var v_max_allowed_left_width = v_totalWidth - 50;
        var v_div_left = v_snippet_tag.divLeft;

        let v_left_pos_x = v_div_left.getBoundingClientRect().width;
        if (p_left_pos_x) {
          var v_div_left_offset = v_div_left.getBoundingClientRect().left;
          v_left_pos_x = p_left_pos_x - v_div_left_offset;
        }

        var v_pixel_value = (v_left_pos_x > 50 && v_left_pos_x < v_max_allowed_left_width)
        ? v_left_pos_x
        : 120;

        var v_left_width_value = v_pixel_value + 'px';

        v_div_left.style['max-width'] = v_left_width_value;
        v_div_left.style['flex'] = '0 0 ' + v_left_width_value;

        var v_div_left_width = v_snippet_tag.divLeft.getBoundingClientRect().width;

      	var v_div_right = v_snippet_tag.divRight;
        var v_right_width_value = (v_totalWidth - v_div_left_width) + 'px';

        v_div_right.style['max-width'] = v_right_width_value;
        v_div_right.style['flex'] = '0 0 ' + v_right_width_value;

        let v_target_tag_div_result_top = 100;

        // Updating the max top position considering if a tab is selected.
        if (v_connTabControl.selectedTab && v_connTabControl.selectedTab !== null) {
          if (v_connTabControl.selectedTab.tag.tabControl) {
            var v_target_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
            let current_query_tab = v_target_tag.currQueryTab;
            if (current_query_tab === 'data') {
              v_target_tag_div_result_top = v_target_tag.div_result.getBoundingClientRect().height - 25;
            }
            else if (current_query_tab === 'explain') {
              v_target_tag_div_result_top = v_target_tag.div_explain.getBoundingClientRect().height - 25;
            }
            else if (current_query_tab === 'message') {
              v_target_tag_div_result_top = v_target_tag.div_notices.getBoundingClientRect().height - 25;
            }
            else {
              v_target_tag_div_result_top = document.getElementsByClassName('omnidb__main')[0].getBoundingClientRect().height - 25;
            }
          }
          else {
            v_target_tag_div_result_top = document.getElementsByClassName('omnidb__main')[0].getBoundingClientRect().height - 25;
          }
        }
        else {
          v_target_tag_div_result_top = document.getElementsByClassName('omnidb__main')[0].getBoundingClientRect().height - 25;
        }

        // Wait for containers animations.
        setTimeout(function(){
          resolve(v_target_tag_div_result_top);
        },10);

      },400);
    });
    await updateOuterSnippetLayout.then(function(v_target_tag_div_result_top){
      // Updating the inner_sinnpet_tag divs size.
      if (v_inner_snippet_tag.editor !== undefined) {
        if (v_snippet_tag.isVisible) {
          v_snippet_tag.divPanel.style.transform = 'translateY(-' + v_target_tag_div_result_top + 'px)';
        }
        v_snippet_tag.divPanel.style.height = v_target_tag_div_result_top + 'px';
        v_snippet_tag.divTree.style.height = v_target_tag_div_result_top + 'px';
        v_inner_snippet_tag.editorDiv.style.height = v_target_tag_div_result_top - (7)*v_font_size + 'px';
        v_inner_snippet_tag.editor.resize();
      }
    });

  }
}

/// <summary>
/// Resize SQL editor and result div.
/// </summary>
function resizeTreeVertical(event) {
	let v_verticalLine = document.createElement('div');
	v_verticalLine.id = 'vertical-resize-line';
  v_connTabControl.selectedTab.tag.divLeft.appendChild(v_verticalLine);

	document.body.addEventListener(
		'mousemove',
		getVerticalLinePosition
	)
  document.body.addEventListener("mouseup", resizeTreeVerticalEnd);

  let node = event.target.parentElement.nextElementSibling
  if(!node)
    return

  if ($(`#${node.id}`).hasClass('omnidb__tree-tabs--not-in-view')) {
    if ($(`#${node.id}`).css('flex-basis') !== 'auto') {
      $(`#${node.id}`).css('flex-basis', 'auto');
    }
    $(`#${node.id}`).removeClass('omnidb__tree-tabs--not-in-view');
    v_connTabControl.selectedTab.tag.treeTabsVisible = true;
  }

  v_start_height = event.screenY;
	document.body.addEventListener("mouseup", resizeTreeVerticalEnd);

}

/// <summary>
/// Resize SQL editor and result div.
/// </summary>
function resizeTreeVerticalEnd(event) {
	document.body.removeEventListener("mouseup", resizeTreeVerticalEnd);
	document.getElementById('vertical-resize-line').remove();

	document.body.removeEventListener(
		'mousemove',
		getVerticalLinePosition
	)
	var v_height_diff = event.screenY - v_start_height;

	var v_tag = v_connTabControl.selectedTab.tag;

	var v_tree_div = v_tag.divTree;
	var v_result_div = null;

  var v_tree_tabs_div = v_tag.divTreeTabs;
  var v_tree_tabs_height = v_tag.divLeft.clientHeight - 14 - event.pageY;

  v_tree_tabs_height = (v_tree_tabs_height < 26) ? 26 : v_tree_tabs_height

  v_tree_tabs_div.style.flexBasis = `${v_tree_tabs_height}px`;

  var v_inner_height = `${v_tree_tabs_height - 49}px`;

  if (v_tag.currTreeTab=='properties') {
    v_result_div = v_tag.divProperties;
  }
	else if (v_tag.currTreeTab=='ddl') {
    v_result_div = v_tag.divDDL;
  }

	v_tree_div.style.height = parseInt(v_tree_div.clientHeight, 10) + v_height_diff + 'px';
  v_result_div.style.height = v_inner_height;

	if (v_tag.currTreeTab=='properties') {
    v_tag.gridProperties.redraw();
  }
	else if (v_tag.currTreeTab=='ddl') {
    v_tag.ddlEditor.resize();
  }

}

/// <summary>
/// Redefines horizontal resize line position.
/// </summary>
function horizontalLinePosition(p_event) {
	document.getElementById('horizontal-resize-line').style.left = p_event.pageX + 'px';
}

/// <summary>
/// Resize Snippet panel editor horizontally.
/// </summary>
function resizeConnectionHorizontal(event) {
  event.preventDefault();
	var v_horizontalLine = document.createElement('div');
	v_horizontalLine.id = 'horizontal-resize-line';
	v_connTabControl.selectedDiv.appendChild(v_horizontalLine);

	document.body.addEventListener(
		'mousemove',
		horizontalLinePosition
	)

	document.body.addEventListener("mouseup", resizeConnectionHorizontalEnd);

}

/// <summary>
/// Resize Connection tab horizontally.
/// </summary>
function resizeConnectionHorizontalEnd(event) {

	document.body.removeEventListener("mouseup", resizeConnectionHorizontalEnd);
  var v_horizontal_line = document.getElementById('horizontal-resize-line');
  if (v_horizontal_line) {
    document.getElementById('horizontal-resize-line').remove();
  }

	document.body.removeEventListener(
		'mousemove',
		horizontalLinePosition
	)

	var v_div_left = v_connTabControl.selectedTab.tag.divLeft;
  var v_div_right = v_connTabControl.selectedTab.tag.divRight;
  var v_totalWidth = v_connTabControl.selectedDiv.getBoundingClientRect().width;

  var v_paddingCompensation = 8;
  var v_offsetLeft = v_div_left.getBoundingClientRect().left;
  var v_mousePosX = event.x;

  // if (event) {
  //   v_mousePosX = event.x;
  // }
  // else {
  //   v_mousePosX = v_offsetLeft + v_div_left.getBoundingClientRect().width - v_paddingCompensation;
  // }

  var v_pixel_value = (v_mousePosX > v_offsetLeft)
  ? (v_paddingCompensation + v_mousePosX - v_offsetLeft)
  : 0;

  var v_left_width_value = v_pixel_value + 'px';

  v_div_left.style['max-width'] = v_left_width_value;
  v_div_left.style['width'] = v_left_width_value;

  // var v_right_width_value = (v_totalWidth - v_pixel_value) + 'px';
  //
  // v_div_right.style['max-width'] = v_right_width_value;
  // v_div_right.style['flex'] = '0 0 ' + v_right_width_value;

	var v_tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;

  refreshHeights();

}

/// <summary>
/// Resize Snippet panel editor horizontally.
/// </summary>
function resizeSnippetHorizontal(event) {
  event.preventDefault();
	let v_horizontalLine = document.createElement('div');
	v_horizontalLine.id = 'horizontal-resize-line';
	v_connTabControl.snippet_tag.divPanel.appendChild(v_horizontalLine);

	document.body.addEventListener(
		'mousemove',
		horizontalLinePosition
	)

	document.body.addEventListener("mouseup", resizeSnippetHorizontalEnd);
}

/// <summary>
/// Resize Snippet panel editor horizontally.
/// </summary>
function resizeSnippetHorizontalEnd(event) {

	document.body.removeEventListener("mouseup", resizeSnippetHorizontalEnd);
	document.getElementById('horizontal-resize-line').remove();

	document.body.removeEventListener(
		'mousemove',
		horizontalLinePosition
	)

  var v_mousePosX = event.x;

  resizeSnippetPanel(v_mousePosX);

}

/// <summary>
/// Resize SQL editor and result div.
/// </summary>
function resizeVertical(event) {
  event.preventDefault();
	let v_verticalLine = document.createElement('div');
	v_verticalLine.id = 'vertical-resize-line';
	v_connTabControl.selectedTab.tag.divRight.appendChild(v_verticalLine);

	document.body.addEventListener(
		'mousemove',
		getVerticalLinePosition
	)

	v_start_height = event.screenY;
	document.body.addEventListener("mouseup", resizeVerticalEnd);

}

/// <summary>
/// Resize SQL editor and result div.
/// </summary>
function resizeVerticalEnd(event) {

	document.body.removeEventListener("mouseup", resizeVerticalEnd);
	document.getElementById('vertical-resize-line').remove();

	document.body.removeEventListener(
		'mousemove',
		getVerticalLinePosition
	)

	var v_height_diff = event.screenY - v_start_height;

	var v_editor_div = document.getElementById(v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editorDivId);
	var v_result_div = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.div_result;

	if (v_height_diff < 0) {
		if (Math.abs(v_height_diff) > parseInt(v_editor_div.style.height, 10))
		 v_height_diff = parseInt(v_editor_div.style.height, 10)*-1 + 10;
	}
	else {
		if (Math.abs(v_height_diff) > parseInt(v_result_div.style.height, 10))
		 v_height_diff = parseInt(v_result_div.style.height, 10) - 10;
	}
	v_editor_div.style.height = parseInt(v_editor_div.style.height, 10) + v_height_diff + 'px';
	v_result_div.style.height = parseInt(v_result_div.style.height, 10) - v_height_diff + 'px';

  refreshHeights();

}

function resizeWindow(){
	refreshHeights(true);
}

var resizeTimeout;
$(window).resize(function() {
	clearTimeout(resizeTimeout);
	resizeTimeout = setTimeout(resizeWindow, 200);
});

/// <summary>
/// Refresh divs sizes and components of the currently selected tab
/// </summary>
function refreshHeights(p_all) {

  setTimeout(function(){
    //Adjusting tree height
    // if (p_all) {
    //   refreshTreeHeight();
    // }

    if (v_connections_data && v_connections_data.v_active) {
      v_connections_data.ht.render();
    }

    if (v_connTabControl.selectedTab.tag.mode=='monitor_all') {
      v_connTabControl.selectedTab.tag.tabControlDiv.style.height = window.innerHeight - $(v_connTabControl.selectedTab.tag.tabControlDiv).offset().top - (1.5)*v_font_size + 'px';
    }
    if (v_connTabControl.selectedTab.tag.mode=='connection') {
      refreshOuterConnectionHeights();
    }
    else if (v_connTabControl.selectedTab.tag.mode=='outer_terminal') {
      v_connTabControl.selectedTab.tag.div_console.style.height = window.innerHeight - $(v_connTabControl.selectedTab.tag.div_console).offset().top - (1.25)*v_font_size + 'px';
      v_connTabControl.selectedTab.tag.fitAddon.fit();
    }

    //If inner tab exists
    if (v_connTabControl.selectedTab.tag.tabControl != null && v_connTabControl.selectedTab.tag.tabControl.selectedTab) {
      var v_tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;

      if (v_tab_tag.mode=='debug' || v_tab_tag.mode=='edit' || v_tab_tag.mode=='graph' || v_tab_tag.mode=='monitor_dashboard' || v_tab_tag.mode=='monitor_grid' || v_tab_tag.mode=='monitor_unit' || v_tab_tag.mode=='website' || v_tab_tag.mode=='website_outer') {
          v_tab_tag.resize();
      }
      else if (v_tab_tag.mode === 'console') {
        emitter.emit(`${v_tab_tag.tab_id}_resize`)
      }
      // else if (v_tab_tag.mode=='query_history') {
      //   v_tab_tag.div_result.style.height = window.innerHeight - $(v_tab_tag.div_result).offset().top - (1.75)*v_font_size + 'px';
      //   if (v_tab_tag.ht!=null)
      //   v_tab_tag.ht.render();
      // }
      else if (v_tab_tag.mode=='alter') {
        if (v_tab_tag.alterTableObject.window=='columns') {
          var v_height = window.innerHeight - $(v_tab_tag.htDivColumns).offset().top - 45;
          v_tab_tag.htDivColumns.style.height = v_height + 'px';
          if (v_tab_tag.alterTableObject.htColumns!=null) {
            v_tab_tag.alterTableObject.htColumns.render();
          }
        }
        else if (v_tab_tag.alterTableObject.window=='constraints') {
          var v_height = window.innerHeight - $(v_tab_tag.htDivConstraints).offset().top - 45;
          v_tab_tag.htDivConstraints.style.height = v_height + 'px';
          if (v_tab_tag.alterTableObject.htConstraints!=null) {
            v_tab_tag.alterTableObject.htConstraints.render();
          }
        }
        else {
          var v_height = window.innerHeight - $(v_tab_tag.htDivIndexes).offset().top - 45;
          v_tab_tag.htDivIndexes.style.height = v_height + 'px';
          if (v_tab_tag.alterTableObject.htIndexes!=null) {
            v_tab_tag.alterTableObject.htIndexes.render();
          }
        }
      }
      else if(v_tab_tag.mode == 'data_mining') {
        if(v_tab_tag.currQueryTab == 'data') {
          v_tab_tag.div_result.style.height = window.innerHeight - $(v_tab_tag.div_result).offset().top - (1.25)*v_font_size + 'px';
        }
      }
    }

    // Updating tree sizes
    refreshTreeHeight();

    // Snippet panel
    resizeSnippetPanel();

    // Updating position of omnis.
    if (v_omnis) {
      if (v_omnis.omnis_ui_assistant) {
        v_omnis.omnis_ui_assistant.goToStep(v_omnis.omnis_ui_assistant.stepSelected);
      }
      else if (v_omnis.div) {
        v_omnis.div.style.top = v_omnis.root.getBoundingClientRect().height - 45 + 'px';
        v_omnis.div.style.left = v_omnis.root.getBoundingClientRect().width - 45 + 'px';
      }
    }

  },351);
}

function refreshTreeHeight() {
  var v_tag = v_connTabControl.selectedTab.tag;

	if (v_tag.currTreeTab=='properties') {
		var v_height  = window.innerHeight - $(v_tag.divProperties).offset().top - 15;
		v_tag.divProperties.style.height = v_height + "px";
		v_tag.gridProperties.redraw(true);
	}
	else if (v_tag.currTreeTab=='ddl') {
		var v_height  = window.innerHeight - $(v_tag.divDDL).offset().top - 15;
		v_tag.divDDL.style.height = v_height + "px";
		v_tag.ddlEditor.resize();
	}
}

function checkTabStatus(v_tab) {
	if (v_tab.tag.tabControl.selectedTab.tag.mode=='query')
    emitter.emit(`${v_tab.tag.tabControl.selectedTab.id}_check_query_status`);
	else if (v_tab.tag.tabControl.selectedTab.tag.mode=='edit')
    console.log('Not implemented') // TODO: implement check tab status functionality for edit tab
	else if (v_tab.tag.tabControl.selectedTab.tag.mode=='debug')
		checkDebugStatus(v_tab.tag.tabControl.selectedTab);
	else if (v_tab.tag.tabControl.selectedTab.tag.mode=='console')
    emitter.emit(`${v_tab.tag.tabControl.selectedTab.id}_check_console_status`);
}

/// <summary>
/// Indent SQL.
/// </summary>
function indentSQL(mode = false) {
  var tab_tag = null;
	let editor = null;

  let dialect = v_connTabControl.selectedTab.tag.selectedDBMS
  let format_options = {
    tabWidth: 2,
    keywordCase: 'upper',
    //sql-formatter uses 'plsql' for oracle sql flavor
    // otherwise - our db technology names match perfectly
    language: dialect === 'oracle' ? 'plsql' : dialect,
    linesBetweenQueries: 1,
  }

  if (mode=='snippet') {
    tab_tag = v_connTabControl.snippet_tag.tabControl.selectedTab.tag;
    editor = tab_tag.editor;
  }
  else {
    if (v_connTabControl.selectedTab.tag.tabControl) {
      tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
      mode = tab_tag.mode;

      if (mode=='query') {
        editor = tab_tag.editor;
      }
    	else if (mode=='console') {
        editor = tab_tag.editor_input;
      }
    }
  }

  if (mode) {
    let editor_value = editor.getValue();
    let formatted = format(editor_value, format_options)
    if(formatted.length) {
      editor.setValue(formatted)
      editor.clearSelection();
      editor.gotoLine(0, 0, true);
    }

  }
}

function showMenuNewTabOuter(e) {
  function getConnectionInfo(conn) {
    let conn_name = "";
    let tooltip_name = "";
    let name = "";
    if (conn.public) {
      conn_name += '<i class="fas fa-users mr-3" style="color:#c57dd2;"></i>';
    }
    if (conn.alias) {
      name = conn.alias;
      conn_name += `(${conn.alias})`;
      tooltip_name += `<h5 class="my-1">${conn.alias}</h5>`;
    }
    if (conn.conn_string) {
      conn_name += ` ${conn.conn_string}`;
      tooltip_name += `<div class="mb-1">${conn.conn_string}</div>`;
    } else {
      if (conn.details1) {
        conn_name += conn.details1;
        tooltip_name += `<div class="mb-1">${conn.details1}</div>`;
      }
      if (conn.details2) {
        conn_name += ` - ${conn.details2}`;
        tooltip_name += `<div class="mb-1">${conn.details2}</div>`;
      }
    }
    return [conn, conn_name, tooltip_name, name];
  }

  function createConnectionGroup(group) {
    const group_connections = connectionsStore.connections
      .filter((conn) => group.conn_list.includes(conn.id))
      .map((conn) => {
        const [_, conn_name, tooltip_name, name] = getConnectionInfo(conn);
        let icon =
          conn.technology == "terminal"
            ? "fa-terminal"
            : `node node-${conn.technology}`;
        let onClick;
        if (conn.technology == "terminal") {
          onClick = () => {
            v_connTabControl.tag.createOuterTerminalTab(
              conn.id,
              conn.alias,
              conn.details1
            );
          };
        } else {
          onClick = () => {
            v_connTabControl.tag.createConnTab(
              conn.id,
              true,
              name,
              tooltip_name
            );
          };
        }
        return {
          label: conn_name,
          icon: `fas cm-all ${icon}`,
          onClick: onClick,
        };
      });

    return {
      label: group.name,
      icon: "fas cm-all fa-plug",
      children: group_connections,
    };
  }
  // Opening connections management when there are no configured connections.
  if (
    !connectionsStore.connections ||
    connectionsStore.connections.length === 0
  ) {
    $("#connections-modal").modal({
      show: true,
    });
  } else {
    let items = [];

    // Building connection list
    if (connectionsStore.connections.length > 0) {
      // No custom groups, render all connections in the same list
      if (!connectionsStore.groups.length) {
        const connectionsList = createConnectionGroup({
          name: "Connections",
          conn_list: connectionsStore.connections.map((conn) => conn.id),
        });

        items.push(connectionsList);
      }
      //Render connections split in groups
      else {
        const group_list = [
          createConnectionGroup({
            name: "All Connections",
            conn_list: connectionsStore.connections.map((conn) => conn.id),
          }),
          ...connectionsStore.groups.map(createConnectionGroup),
        ];

        items.push({
          label: "Connections",
          icon: "fas cm-all fa-plug",
          children: group_list,
        });
      }
    }

    if (connectionsStore.remote_terminals.length > 0) {
      const submenu_terminal_list = connectionsStore.remote_terminals.map(
        (term) => {
          const { id, alias, details1 } = term;
          let term_name = alias ? `(${alias})` : "";
          if (details1) {
            term_name += details1;
          }
          return {
            label: term_name,
            icon: "fas cm-all fa-terminal",
            onClick: () => {
              v_connTabControl.tag.createOuterTerminalTab(id, alias, details1);
            },
          };
        }
      );

      items.push({
        label: "SSH Consoles",
        icon: "fas cm-all fa-terminal",
        children: submenu_terminal_list,
      });
    }

    if (items.length > 0) {
      items.unshift({
        label: "Manage Connections",
        icon: "fas cm-all fa-gears",
        onClick: () => {
          $("#connections-modal").modal({
            show: true,
          });
        },
      });

      ContextMenu.showContextMenu({
        theme: "pgmanage",
        x: e.x,
        y: e.y,
        zIndex: 1000,
        minWidth: 230,
        items: items,
      });
    } else {
      startLoading();
      setTimeout(function () {
        v_connTabControl.tag.createConnTab();
      }, 0);
    }
  }
}

function showMenuNewTab(e) {
	var v_option_list = [
    {
			label: 'Query Tab',
			icon: 'fas cm-all fa-search',
			onClick: function() {
				v_connTabControl.tag.createQueryTab();
			}
		},
		{
			label: 'Console Tab',
			icon: 'fas cm-all fa-terminal',
			onClick: function() {
				v_connTabControl.tag.createConsoleTab();
			}
		},
	];

	if (v_connTabControl.selectedTab.tag.selectedDBMS=='postgresql' ||
			v_connTabControl.selectedTab.tag.selectedDBMS=='mysql' ||
			v_connTabControl.selectedTab.tag.selectedDBMS=='mariadb') {
		v_option_list.push(
			{
				label: 'Monitoring Dashboard',
				icon: 'fas cm-all fa-chart-line',
				onClick: function() {
					v_connTabControl.tag.createMonitorDashboardTab();
					startMonitorDashboard();
				}
			}
		);
	}

  if (v_connTabControl.selectedTab.tag.selectedDBMS=='postgresql') {
		v_option_list.push(
			{
				label: 'Backends',
				icon: 'fas cm-all fa-tasks',
				onClick: function() {
					v_connTabControl.tag.createMonitoringTab(
							'Backends',
							'select * from pg_stat_activity', [{
									icon: 'fas fa-times action-grid action-close',
									title: 'Terminate',
									action: 'postgresqlTerminateBackend'
							}]);
				}
			}
		);
	}
	else if (v_connTabControl.selectedTab.tag.selectedDBMS=='mysql' || v_connTabControl.selectedTab.tag.selectedDBMS=='mariadb') {
		v_option_list.push(
			{
				label: 'Process List',
				icon: 'fas cm-all fa-tasks',
				onClick: function() {
					v_connTabControl.tag.createMonitoringTab(
							'Process List',
							'select * from information_schema.processlist', [{
									icon: 'fas fa-times action-grid action-close',
									title: 'Terminate',
									action: 'mysqlTerminateBackend'
							}]);
				}
			}
		);
	}

  ContextMenu.showContextMenu({
    theme: "pgmanage",
    x: e.x,
    y: e.y,
    zIndex: 1000,
    minWidth: 230,
    items: v_option_list,
  });

}

function toggleTreeContainer() {
  var v_tab_tag = v_connTabControl.selectedTab.tag;
  if (v_tab_tag.divLeft) {
    $(v_tab_tag.divLeft).toggleClass('omnidb__workspace__div-left--shrink');
    refreshHeights();
  }
}

function toggleTreeTabsContainer(p_target_id,p_horizonta_line_id) {
  var v_tab_tag = v_connTabControl.selectedTab.tag;
  var v_target_element = $('#' + p_target_id);
  if (v_target_element.hasClass('omnidb__tree-tabs--not-in-view')) {
    $(`#${p_target_id}`).removeClass('omnidb__tree-tabs--not-in-view');
    if ($(`#${p_target_id}`).css('flex-basis') === 'auto') {
      $(`#${p_target_id}`).css('flex-basis', '280px');
    }
    v_tab_tag.treeTabsVisible = true;
    setTimeout(function(){refreshTreeHeight();},360);
  }
  else {
    $(`#${p_target_id}`).addClass('omnidb__tree-tabs--not-in-view');
    v_tab_tag.treeTabsVisible = false;
  }
  // $('#' + p_target_id).toggleClass('omnidb__tree-tabs--not-in-view');
  // $('#' + p_horizonta_line_id).toggleClass('d-none');
}

function dragStart(event, gridContainer) {
  try {
    event.dataTransfer.setData("Text", event.target.id);
    event.dataTransfer.effectAllowed = "move";
    gridContainer.classList.add('omnidb__workspace-resize-grid--active');
    event.srcElement.classList.add('omnidb__workspace-resize-grid__draggable--is-dragging');
  }
  catch (e) {

  }
}

function dragEnd(event, grid_container) {
  grid_container.classList.remove('omnidb__workspace-resize-grid--active');
  event.target.classList.remove('omnidb__workspace-resize-grid__draggable--is-dragging');
}

function dragEnter(event) {
  event.target.classList.add('omnidb__workspace-resize-grid__column--enter');
}

function dragLeave(event) {
  event.target.classList.remove('omnidb__workspace-resize-grid__column--enter');
}

function allowDrop(event) {
  event.preventDefault();
}

function drop(event, grid_container, div_left, div_right) {
  event.preventDefault();
  try {
    var data = event.dataTransfer.getData("Text");
    event.target.appendChild(document.getElementById(data));

  	let pos = parseInt( event.srcElement.getBoundingClientRect().left );
  	let space = parseInt( window.innerWidth );
  	let cells = Math.round( pos*12 / space );

    div_left.classList = [' omnidb__workspace__div-left col-md-' + cells];
    div_right.classList = [' omnidb__workspace__div-right col-md-' + (12 - cells)];

    let cols = document.getElementsByClassName('omnidb__workspace-resize-grid__column');
    for (let i = 0; i < cols.length; i++) {
      document.getElementsByClassName('omnidb__workspace-resize-grid__column')[i].classList.remove('omnidb__workspace-resize-grid__column--enter');
    }
    v_connTabControl.selectedTab.tag.gridProperties.redraw();
  }
  catch (e) {

  }

}

/**
 * ## getVerticalLinePosition
 * @desc Gets the Y position of the pointer event.
 *
 * @param  {Object} p_event UI action pointer event.
 * @return {String}         The Y position of the pointer in pixels.
 */
function getVerticalLinePosition(p_event) {
	document.getElementById('vertical-resize-line').style.top = p_event.pageY + 'px';
}

function toggleExpandToPanelView(p_target_id) {
  let v_target = document.getElementById(p_target_id);
  if (v_target) {
    v_target.classList.toggle('omnidb__panel-view--full');
    setTimeout(function(){
      refreshHeights();
    },350);
  }
}

function updateExplainComponent() {
  if (v_explain_control.context === 'default') {
    $('#omnidb__main').addClass('omnidb__explain--default');
  }
  else {
    $('#omnidb__main').removeClass('omnidb__explain--default');
  }
}


function monitoringAction(row_data, p_function) {
  const handlerFnMap = {
    postgresqlTerminateBackend: postgresqlTerminateBackend,
    mysqlTerminateBackend: mysqlTerminateBackend,
    oracleTerminateBackend: oracleTerminateBackend,
    mariadbTerminateBackend: mariadbTerminateBackend,
  };

  let handlerFn = handlerFnMap[p_function];

  if (handlerFn && typeof handlerFn === "function") {
    handlerFn(row_data);
  }
}

function uiCopyTextToClipboard(p_value) {
  // Create temporary invisible textarea.
  var v_text_area = document.createElement('textarea');
  v_text_area.styleList = {'height':'0px','overflow':'hidden'}
  document.body.appendChild(v_text_area);
  // Updating the temporary textarea and selecting the values.
  v_text_area.value = p_value;
  v_text_area.select();
  v_text_area.setSelectionRange(0, 9999999);
  // Copying the text inside the temporary textarea.
  document.execCommand("copy");
  // Remove and delete the temporary textarea.
  document.body.removeChild(v_text_area);
  v_text_area = null
  // Prompting an alert.
  showAlert('<b>Text copied:</b> \n<div class="mt-2 p-2 border-1 omnidb__theme-bg--light"><code>' + p_value + '</code></div>');
}

function toggleConnectionAutocomplete(p_toggler_id) {
  let checked = document.getElementById(p_toggler_id).checked;
  settingsStore.setAutocomplete(checked)
}

export {
  refreshHeights,
  changeDatabase,
  adjustQueryTabObjects,
  renameTab,
  refreshTreeHeight,
  removeTab,
  showMenuNewTab,
  toggleTreeTabsContainer,
  resizeSnippetPanel,
  indentSQL,
  checkTabStatus,
  checkBeforeChangeDatabase,
  renameTabConfirm,
  monitoringAction,
  toggleExpandToPanelView,
  resizeConnectionHorizontal,
  resizeTreeVertical,
  resizeVertical,
  resizeSnippetHorizontal,
  toggleConnectionAutocomplete,
  toggleTreeContainer,
  uiCopyTextToClipboard
};