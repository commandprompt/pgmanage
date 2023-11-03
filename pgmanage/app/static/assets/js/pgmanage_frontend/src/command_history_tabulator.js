function showCommandListTabulator() {
	var v_tabTag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;

	let btn_first = document.getElementById(`bt_first_${v_tabTag.tab_id}`)
	btn_first.onclick = function() { commandHistoryFirstPage() }

	let btn_previous = document.getElementById(`bt_previous_${v_tabTag.tab_id}`)
	btn_previous.onclick = function() { commandHistoryPreviousPage() }

	let btn_next = document.getElementById(`bt_next_${v_tabTag.tab_id}`)
	btn_next.onclick = function() { commandHistoryNextPage() }

	let btn_last = document.getElementById(`bt_last_${v_tabTag.tab_id}`)
	btn_last.onclick = function() { commandHistoryLastPage() }

	let btn_refresh = document.getElementById(`bt_refresh_${v_tabTag.tab_id}`)
	btn_refresh.onclick = function() { refreshCommandList() }

	let btn_clear = document.getElementById(`bt_clear_${v_tabTag.tab_id}`)
	btn_clear.onclick = function() { deleteCommandList() }

	let cl_input = document.getElementById(`cl_input_contains_${v_tabTag.tab_id}`)
	cl_input.onchange = function() { refreshCommandList() }

	var v_gridDiv = v_tabTag.commandHistory.gridDiv;
	v_gridDiv.innerHTML = '';

	if(v_tabTag.commandHistory.grid != null) {
		v_tabTag.commandHistory.grid.destroy();
	}



	$(v_tabTag.commandHistory.modal).modal('show');
	v_tabTag.commandHistory.div.style.display = 'block';

	v_tabTag.commandHistory.currentPage = 1;
	v_tabTag.commandHistory.pages = 1;
	v_tabTag.commandHistory.spanNumPages = document.getElementById('cl_num_pages_' + v_tabTag.tab_id);
	v_tabTag.commandHistory.spanNumPages.innerHTML = 1;
	v_tabTag.commandHistory.spanCurrPage = document.getElementById('cl_curr_page_' + v_tabTag.tab_id);
	v_tabTag.commandHistory.spanCurrPage.innerHTML = 1;
	v_tabTag.commandHistory.inputStartedFrom = document.getElementById('cl_input_from_' + v_tabTag.tab_id);
	v_tabTag.commandHistory.inputStartedFrom.value = moment().subtract(6, 'hour').toISOString();
	v_tabTag.commandHistory.inputStartedTo = document.getElementById('cl_input_to_' + v_tabTag.tab_id);
	v_tabTag.commandHistory.inputStartedTo.value = moment().toISOString();
	v_tabTag.commandHistory.inputCommandContains = document.getElementById('cl_input_contains_' + v_tabTag.tab_id);
	v_tabTag.commandHistory.inputCommandContains.value = v_tabTag.commandHistory.inputCommandContainsLastValue;

	// Setting daterangepicker
	var cl_time_range = document.getElementById('cl_time_range_' + v_tabTag.tab_id);

	$(cl_time_range).daterangepicker({
		timePicker: true,
		startDate: moment(v_tabTag.commandHistory.inputStartedFrom.value).format('Y-MM-DD H'),
		endDate: moment(v_tabTag.commandHistory.inputStartedTo.value).format('Y-MM-DD H'),
		parentEl: document.getElementById('command_history_daterangepicker_container_' + v_tabTag.tab_id),
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

		v_tabTag.commandHistory.inputStartedFrom.value = moment(start).toISOString();

		// Update Button Labels
		if (label === "Custom Range") {
			$('#cl_time_range_' + v_tabTag.tab_id + ' span').html(start.format('MMMM D, YYYY hh:mm A') + ' - ' + end.format('MMMM D, YYYY hh:mm A'));
		}
		else {
			$('#cl_time_range_' + v_tabTag.tab_id + ' span').html(label);
		}

		if (label === "Custom Range" || label === "Yesterday" || label === "Last Month") {
			v_tabTag.commandHistory.inputStartedTo.value = moment(end).toISOString();
		}
		else
			v_tabTag.commandHistory.inputStartedTo.value = null;

		refreshCommandList();
	});

	refreshCommandList();
}

function refreshCommandList() {


}

export {showCommandListTabulator}