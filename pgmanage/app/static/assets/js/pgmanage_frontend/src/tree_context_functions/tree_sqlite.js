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

import { createApp } from "vue";
import TreeSqlite from "../components/TreeSqlite.vue";
import { tabSQLTemplate } from "./tree_postgresql";
import { execAjax } from "../ajax_control";
import { showToast} from "../notification_control";
import { emitter } from "../emitter";
import { tabsStore } from "../stores/stores_initializer";


/// <summary>
/// Retrieving SELECT SQL template.
/// </summary>
function TemplateSelectSqlite(p_table, p_kind) {
    execAjax(
        '/template_select_sqlite/',
        JSON.stringify({
            'p_database_index': tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
            'p_tab_id': tabsStore.selectedPrimaryTab.id,
            'p_table': p_table,
            'p_kind': p_kind
        }),
        function(p_return) {
            let tab_name = p_table;
            tabsStore.createQueryTab(tab_name, null, null, p_return.v_data.v_template)

            setTimeout(() => {
              emitter.emit(`${tabsStore.selectedPrimaryTab.metaData.selectedTab.id}_run_query`)
          }, 200)
        },
        function(p_return) {
            showToast("error", p_return.v_data)
            return '';
        },
        'box',
        true
    );
}

/// <summary>
/// Retrieving INSERT SQL template.
/// </summary>
function TemplateInsertSqlite(p_table) {
    execAjax(
        '/template_insert_sqlite/',
        JSON.stringify({
            'p_database_index': tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
            'p_tab_id': tabsStore.selectedPrimaryTab.id,
            'p_table': p_table
        }),
        function(p_return) {
          tabSQLTemplate(
              'Insert ' + p_table,
              p_return.v_data.v_template
          );
        },
        function(p_return) {
            showToast("error", p_return.v_data)
            return '';
        },
        'box',
        true
    );
}

/// <summary>
/// Retrieving UPDATE SQL template.
/// </summary>
function TemplateUpdateSqlite(p_table) {
    execAjax(
        '/template_update_sqlite/',
        JSON.stringify({
            'p_database_index': tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
            'p_tab_id': tabsStore.selectedPrimaryTab.id,
            'p_table': p_table
        }),
        function(p_return) {
          tabSQLTemplate(
              'Update ' + p_table,
              p_return.v_data.v_template
          );
        },
        function(p_return) {
            showToast("error", p_return.v_data)
            return '';
        },
        'box',
        true
    );
}

export {
  TemplateSelectSqlite,
  TemplateInsertSqlite,
  TemplateUpdateSqlite,
};