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
import { cellDataModal } from "./header_actions";
import { blueHtmlRenderer, whiteHtmlRenderer } from "./renderers";
import { execAjax } from "./ajax_control";
import { showConfirm } from "./notification_control";
import moment from "moment";
import { emitter } from "./emitter";

/// <summary>
/// Console state
/// </summary>
var v_consoleState = {
	Idle: 0,
	Executing: 1,
	Ready: 2
}

/// <summary>
/// Wipes command history.
/// </summary>
function deleteConsoleHistoryList() {
	showConfirm(
		'Are you sure you want to clear console history corresponding to applied filters?',
		function() {
			execAjax(
				'/clear_console_list/',
				JSON.stringify({
					'p_database_index': v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
					'p_console_from': v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputStartedFrom.value,
					'p_console_to': v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputStartedTo.value,
					'p_console_contains': v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputCommandContains.value
				}),
				function(p_return) {
					v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage = 1;
					refreshConsoleHistoryList();
				}
			);
		}
	);
}

function showConsoleHistory() {
  // var input = JSON.stringify({
	// 	"p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
	// 	"p_tab_id": v_connTabControl.selectedTab.id
	// });
	var v_conn_tag = v_connTabControl.selectedTab.tag;
  var v_tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;

	v_tab_tag.consoleHistory.headerDiv.innerHTML =
	"<div class='mb-2 form-inline justify-content-center'>" +
		"<div class='input-group w-auto mr-2'>" +
			"<span class='my-auto'>Select a daterange:</span>&nbsp;" +
			"<input type='text' class='form-control form-control-sm d-none' placeholder='Start Time' id='cl_input_from_" + v_tab_tag.tab_id + "'>" +
			"<input type='text' class='form-control form-control-sm d-none' placeholder='End Time' id='cl_input_to_" + v_tab_tag.tab_id + "'>" +
			"<button type='button' class='btn btn-sm btn-primary' id='cl_time_range_" + v_tab_tag.tab_id + "'>" +
				"<i class='far fa-calendar-alt'></i>&nbsp;" +
				"<span>Last 6 Hours</span> <i class='fa fa-caret-down'></i>" +
			"</button>" +
		"</div>" +
		"<label class='mr-1'>Command contains:</label>" +
		"<input type='text' id='cl_input_contains_" + v_tab_tag.tab_id + "' class='mr-2 form-control' />" +
	"</div>" +
	"<div id='console_history_daterangepicker_container_" + v_tab_tag.id  + "' style='position:relative;'></div>" +
	"<div class='mb-2 d-flex justify-content-center align-items-center'>" +
		"<button id='bt_first_" + v_tab_tag.tab_id + "' class='bt_execute btn btn-sm btn-secondary mx-1' title='First'>First</button>" +
		"<button id='bt_previous_" + v_tab_tag.tab_id + "' class='bt_execute btn btn-sm btn-secondary mx-1' title='Previous'>Previous</button>" +
		"<span id='cl_curr_page_" + v_tab_tag.tab_id + "'></span> / <span id='cl_num_pages_" + v_tab_tag.tab_id + "'></span>" +
		"<button id='bt_next_" + v_tab_tag.tab_id + "' class='bt_execute btn btn-sm btn-secondary mx-1' title='Next'>Next</button>" +
		"<button id='bt_last_" + v_tab_tag.tab_id + "' class='bt_execute btn btn-sm btn-secondary mx-1' title='Last'>Last</button>" +
		"<button id='bt_refresh_" + v_tab_tag.tab_id + "' class='bt_execute btn btn-sm btn-primary mx-1' title='Refresh'><i class='fas fa-sync-alt mr-1'></i>Refresh</button>" +
		"<button id='bt_clear_" + v_tab_tag.tab_id + "' class='bt_execute btn btn-sm btn-danger mx-1' title='Clear List'><i class='fas fa-broom mr-1'></i>Clear List</button>" +
	"</div>";

	let btn_first = document.getElementById(`bt_first_${v_tab_tag.tab_id}`)
	btn_first.onclick = function() { consoleHistoryFirstPage() }

	let btn_previous = document.getElementById(`bt_previous_${v_tab_tag.tab_id}`)
	btn_previous.onclick = function() { consoleHistoryPreviousPage() }

	let btn_next = document.getElementById(`bt_next_${v_tab_tag.tab_id}`)
	btn_next.onclick = function() { consoleHistoryNextPage() }

	let btn_last = document.getElementById(`bt_last_${v_tab_tag.tab_id}`)
	btn_last.onclick = function() { consoleHistoryLastPage() }

	let btn_refresh = document.getElementById(`bt_refresh_${v_tab_tag.tab_id}`)
	btn_refresh.onclick = function() { refreshConsoleHistoryList() }

	let btn_clear = document.getElementById(`bt_clear_${v_tab_tag.tab_id}`)
	btn_clear.onclick = function() { deleteConsoleHistoryList() }

	let cl_input = document.getElementById(`cl_input_contains_${v_tab_tag.tab_id}`)
	cl_input.onchange = function() { refreshConsoleHistoryList() }

  var v_grid_div = v_tab_tag.consoleHistory.gridDiv;
  v_grid_div.innerHTML = '';

	if(v_tab_tag.consoleHistory.grid != null) {
		v_tab_tag.consoleHistory.grid.destroy();
	}

	var columnProperties = [];

	// var col = new Object();
	// col.readOnly = true;
	// col.title =  ' ';
	// col.width = '26px';
	// columnProperties.push(col);

	var col = new Object();
	col.readOnly = true;
	col.title =  'Date';
	col.width = '160px';
	columnProperties.push(col);

	var col = new Object();
	col.readOnly = true;
	col.title =  'Command';
	col.width = '415px';
	columnProperties.push(col);

	v_tab_tag.consoleHistory.grid = new Handsontable(v_grid_div,
	{
		licenseKey: 'non-commercial-and-evaluation',
		data: [],
		className: 'simple',
		columns : columnProperties,
		colHeaders : true,
		rowHeaders : false,
		stretchH: 'last',
		//copyRowsLimit : 1000000000,
		//copyColsLimit : 1000000000,
		copyPaste: {pasteMode: '', rowsLimit: 1000000000, columnsLimit: 1000000000},
		beforeCopy: (data, coords) => {
			const tempData = [...data]
			tempData.map((row) => {
				const tempRow = row.map(el => el.replace(/\n/g, ' '))
				return [...tempRow]
			})
			data.splice(coords[0].startRow,data.length, tempData)
			return data
		},
		manualColumnResize: true,
		fillHandle:false,
		contextMenu: {
			callback: function (key, options) {
				if (key === 'view_data') {
						cellDataModal(this,options[0].start.row,options[0].start.col,this.getDataAtCell(options[0].start.row,options[0].start.col),false);
				}
				else if (key === 'copy') {
					this.selectCell(options[0].start.row,options[0].start.col,options[0].end.row,options[0].end.col);
					document.execCommand('copy');
				}
				else if (key === 'copy_to_console') {
					consoleHistoryOpenCmd(options[0].start.row);
				}
			},
			items: {
				"copy": {name: '<div style=\"position: absolute;\"><i class=\"fas fa-copy cm-all\" style=\"vertical-align: middle;\"></i></div><div style=\"padding-left: 30px;\">Copy</div>'},
				'copy_to_console': {name: '<div style="position: absolute;"><i class=\"fas fa-bolt cm-all\" style=\"vertical-align: middle;\"></i></div><div style="padding-left: 30px;">Copy Content To Console Tab</div>'},
				"view_data": {name: '<div style=\"position: absolute;\"><i class=\"fas fa-edit cm-all\" style=\"vertical-align: middle;\"></i></div><div style=\"padding-left: 30px;\">View Content</div>'}
			}
			},
				cells: function (row, col, prop) {
				var cellProperties = {};
				if (row % 2 == 0)
				cellProperties.renderer = blueHtmlRenderer;
			else
				cellProperties.renderer = whiteHtmlRenderer;
				return cellProperties;
		}
	});

	$(v_tab_tag.consoleHistory.modal).modal('show');
	v_tab_tag.consoleHistory.div.style.display = 'block';

	v_tab_tag.consoleHistory.currentPage = 1;
	v_tab_tag.consoleHistory.pages = 1;
	v_tab_tag.consoleHistory.spanNumPages = document.getElementById('cl_num_pages_' + v_tab_tag.tab_id);
	v_tab_tag.consoleHistory.spanNumPages.innerHTML = 1;
	v_tab_tag.consoleHistory.spanCurrPage = document.getElementById('cl_curr_page_' + v_tab_tag.tab_id);
	v_tab_tag.consoleHistory.spanCurrPage.innerHTML = 1;
	v_tab_tag.consoleHistory.inputStartedFrom = document.getElementById('cl_input_from_' + v_tab_tag.tab_id);
	v_tab_tag.consoleHistory.inputStartedFrom.value = moment().subtract(6, 'hour').toISOString();
	v_tab_tag.consoleHistory.inputStartedTo = document.getElementById('cl_input_to_' + v_tab_tag.tab_id);
	v_tab_tag.consoleHistory.inputStartedTo.value = moment().toISOString();
	v_tab_tag.consoleHistory.inputCommandContains = document.getElementById('cl_input_contains_' + v_tab_tag.tab_id);
	v_tab_tag.consoleHistory.inputCommandContains.value = v_tab_tag.consoleHistory.inputCommandContainsLastValue;

	// Setting daterangepicker
	var cl_time_range = document.getElementById('cl_time_range_' + v_tab_tag.tab_id);

	$(cl_time_range).daterangepicker({
		timePicker: true,
		startDate: moment(v_tab_tag.consoleHistory.inputStartedFrom.value).format('Y-MM-DD H'),
		endDate: moment(v_tab_tag.consoleHistory.inputStartedTo.value).format('Y-MM-DD H'),
		parentEl: document.getElementById('console_history_daterangepicker_container_' + v_tab_tag.tab_id),
		previewUTC: true,
		locale: {
			format: 'Y-MM-DD H'
		},
		ranges: {
			'Last 6 Hours': [moment().subtract(6, 'hour').format('Y-MM-DD H'), moment().format('Y-MM-DD H')],
			'Last 12 Hours': [moment().subtract(12, 'hour').format('Y-MM-DD H'), moment().format('Y-MM-DD H')],
			'Last 24 Hours': [moment().subtract(24, 'hour').format('Y-MM-DD H'), moment().format('Y-MM-DD H')],
			'Last 7 Days': [moment().subtract(7, 'days').startOf('day').format('Y-MM-DD H'), moment().format('Y-MM-DD H')],
			'Last 30 Days': [moment().subtract(30, 'days').startOf('day').format('Y-MM-DD H'), moment().format('Y-MM-DD H')],
			'Yesterday': [moment().subtract(1, 'days').startOf('day').format('Y-MM-DD H'), moment().subtract(1, 'days').endOf('day').format('Y-MM-DD H')],
			'This Month': [moment().startOf('month').format('Y-MM-DD H'), moment().format('Y-MM-DD H')],
			'Last Month': [moment().subtract(1, 'month').startOf('month').format('Y-MM-DD H'), moment().subtract(1, 'month').endOf('month').format('Y-MM-DD H')]
		}
	}, function(start, end, label) {

		v_tab_tag.consoleHistory.inputStartedFrom.value = moment(start).toISOString();

		// Update Button Labels
		if (label === "Custom Range") {
			$('#cl_time_range_' + v_tab_tag.tab_id + ' span').html(start.format('MMMM D, YYYY hh:mm A') + ' - ' + end.format('MMMM D, YYYY hh:mm A'));
		}
		else {
			$('#cl_time_range_' + v_tab_tag.tab_id + ' span').html(label);
		}

		if (label === "Custom Range" || label === "Yesterday" || label === "Last Month") {
			v_tab_tag.consoleHistory.inputStartedTo.value = moment(end).toISOString();
		}
		else
			v_tab_tag.consoleHistory.inputStartedTo.value = null;

		refreshConsoleHistoryList();
	});

	refreshConsoleHistoryList();
}

function consoleHistoryNextPage() {
	if(v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage < v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.pages) {
		v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage += 1;
		refreshConsoleHistoryList();
	}
}

function consoleHistoryPreviousPage() {
	if(v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage > 1) {
		v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage -= 1;
		refreshConsoleHistoryList();
	}
}

function consoleHistoryFirstPage() {
	if(v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage != 1) {
		v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage = 1;
		refreshConsoleHistoryList();
	}
}

function consoleHistoryLastPage() {
	if(v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage != v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.pages) {
		v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.pages;
		refreshConsoleHistoryList();
	}
}

function consoleHistoryOpenCmd(p_index) {
	let command = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.grid.getDataAtRow(p_index)[1];
	let selected_tab = v_connTabControl.selectedTab.tag.tabControl.selectedTab
	emitter.emit(`${selected_tab.id}_copy_to_editor`, command)
	closeConsoleHistory();
}

/// <summary>
/// Retrieves and displays console history.
/// </summary>
function refreshConsoleHistoryList() {
	var v_conn_tag = v_connTabControl.selectedTab.tag;
  var v_tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputStartedFromLastValue = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputStartedFrom.value;
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputStartedToLastValue = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputStartedTo.value;
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputCommandContainsLastValue = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputCommandContains.value;

	execAjax(
		'/get_console_history/',
		JSON.stringify({
			'p_command_from': v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputStartedFrom.value,
			'p_command_to': v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputStartedTo.value,
			'p_command_contains': v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputCommandContains.value,
			'p_current_page': v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage,
			'p_database_index': v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
			"p_tab_id": v_connTabControl.selectedTab.id
		}),
		function(p_return) {
			v_conn_tag.consoleHistoryFecthed = true;
			v_conn_tag.consoleHistoryList = p_return.v_data.commandList;

			if(v_conn_tag.consoleHistoryList.length == 0) {
				v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage = 1;
			}

			v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.pages = p_return.v_data.pages;
			v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.spanNumPages.innerHTML = p_return.v_data.pages;
			v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.spanCurrPage.innerHTML = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage;

			for (let i = 0; i < v_conn_tag.consoleHistoryList.length; i++) {
				v_conn_tag.consoleHistoryList[i][0] = moment(v_conn_tag.consoleHistoryList[i][0]).format();
			};

			v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.grid.loadData(v_conn_tag.consoleHistoryList);

		},
		null,
		'box'
	);
}

function closeConsoleHistory() {
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.grid.destroy();
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.grid = null;
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.div.style.display = 'none';
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.headerDiv.innerHTML = '';
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.gridDiv.innerHTML = '';
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.currentPage = 1;
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.pages = 1;
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.spanNumPages = null;
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.spanCurrPages = null;
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputStartedFrom = null;
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputStartedTo = null;
	v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.inputCommandContains = null;
	$(v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.consoleHistory.modal).modal('hide');
}

function consoleHistorySelectCommand() {
  var v_tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
  var v_grid = v_tab_tag.consoleHistory.grid;

  var v_command = v_grid.getDataAtRow(v_grid.getSelected()[0][0])[2];
  closeConsoleHistory();
  v_tab_tag.editor_input.setValue(v_command);
  v_tab_tag.editor_input.clearSelection();
  v_tab_tag.editor_input.focus();
}

export {
  showConsoleHistory,
  closeConsoleHistory,
  v_consoleState,
};