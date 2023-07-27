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

function tabSQLTemplate(p_tab_name, p_template, p_showTip=true) {
    v_connTabControl.tag.createQueryTab(p_tab_name);
    v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
        p_template);
    v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
    v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.gotoLine(
        0, 0, true);

    // if(p_showTip) {
    //   var v_instance = new Tooltip($(v_connTabControl.selectedTab.tag.tabControl.selectedLi),{
    //     title: 'Adjust command and run!',
    //     placement: "bottom",
    //     container: 'body',
    //     offset: 100
    //   });
    //   v_instance.show();
    //   window.setTimeout(function() {
    //       v_instance.dispose();
    //   }, 4000);
    // }
}

function tabAdvancedObjectSearch(node) {
    var v_name = 'Advanced Object Search';

    v_connTabControl.selectedTab.tag.tabControl.removeTabIndex(v_connTabControl
        .selectedTab.tag.tabControl.tabList.length - 1);

    var v_tab = v_connTabControl.selectedTab.tag.tabControl.createTab(
        '<span id="tab_title">' + v_name +
        '</span><span id="tab_loading" style="visibility:hidden;"><i class="tab-icon node-spin"></i></span><i title="" id="tab_check" style="display: none;" class="fas fa-check-circle tab-icon icon-check"></i><i title="Close" id="tab_close" class="fas fa-times tab-icon icon-close"></i>',
        false,
        null,
        renameTab,
        null,
        null,
        true,
        function() {
            if (this.tag != null) {
                refreshHeights();
            }

            if (this.tag != null) {
                checkAdvancedObjectSearchStatus(this);
            }
        }
    );
    v_connTabControl.selectedTab.tag.tabControl.selectTab(v_tab);

    //Adding unique names to spans
    var v_tab_title_span = document.getElementById('tab_title');
    v_tab_title_span.id = 'tab_title_' + v_tab.id;

    var v_tab_loading_span = document.getElementById('tab_loading');
    v_tab_loading_span.id = 'tab_loading_' + v_tab.id;

    var v_tab_close_span = document.getElementById('tab_close');
    v_tab_close_span.id = 'tab_close_' + v_tab.id;

    v_tab_close_span.onclick = function(e) {
        var v_current_tab = v_tab;
        customMenu({
                x: e.clientX + 5,
                y: e.clientY + 5
            }, [{
                text: 'Confirm',
                icon: 'fas cm-all fa-check',
                action: function() {
                    removeTab(v_current_tab);
                }
            }, {
                text: 'Cancel',
                icon: 'fas cm-all fa-times',
                action: function() {}
            }],
            null);
    };

    var v_tab_check_span = document.getElementById('tab_check');
    v_tab_check_span.id = 'tab_check_' + v_tab.id;

    var v_html =
        "<div id='txt_query_" + v_tab.id +
        "' style=' width: 100%; height: 400px; border: 1px solid #c3c3c3;'></div>" +
        "<div class='omnidb__resize-line__container--horizontal' onmousedown='resizeVertical(event)'><div class='resize_line_horizontal'></div><div style='height:5px;'></div></div>" +
        "<button id='bt_start_" + v_tab.id +
        "' class='bt_execute bt_icon_only' title='Run' style='margin-bottom: 5px; margin-right: 5px; display: inline-block; vertical-align: middle;'><i class='fas fa-play fa-light'></i></button>" +
        "<button id='bt_cancel_" + v_tab.id +
        "' class='bt_red' title='Cancel' style='margin-bottom: 5px; margin-left: 5px; display: none; vertical-align: middle;' onclick='cancelSQL();'>Cancel</button>" +
        "<div id='div_query_info_" + v_tab.id +
        "' class='query_info' style='display: inline-block; margin-left: 5px; vertical-align: middle;'></div>" +

        "        <div id='query_result_tabs_" + v_tab.id + "'>" +
        "            <ul>" +
        "            <li id='query_result_tabs_" + v_tab.id +
        "_tab1'>Data</li>" +
        "			</ul>" +
        "			<div id='div_query_result_tabs_" + v_tab.id + "_tab1'>" +
        "<div id='div_result_" + v_tab.id +
        "' class='query_result' style='width: 100%; overflow: auto;'></div>" +
        "			</div>";

    var v_div = document.getElementById('div_' + v_tab.id);
    v_div.innerHTML = v_html;

    var v_ = document.createElement('div');

    var v_containerDiv = document.getElementById('txt_query_' + v_tab.id);
    v_containerDiv.style.display = 'flex';
    v_containerDiv.className = 'query_info';
    v_containerDiv.style.flexDirection = 'column';
    v_containerDiv.style.overflow = 'auto';

    var v_filterHeader = document.createElement('h3');
    v_filterHeader.innerHTML = 'Text Filter';
    v_filterHeader.style.marginLeft = '10px';
    v_filterHeader.className = 'query_info';
    v_filterHeader.style.marginBottom = '0px';
    v_filterHeader.style.flex = '0 0 auto';
    v_containerDiv.appendChild(v_filterHeader);

    var v_filterContainerDiv = document.createElement('div');
    v_filterContainerDiv.style.display = 'flex';
    v_filterContainerDiv.style.flex = '0 0 auto';
    v_containerDiv.appendChild(v_filterContainerDiv);

    var v_inputFilter = document.createElement('input');
    v_inputFilter.type = 'text';
    v_inputFilter.placeholder = 'Type the pattern to be searched...';
    v_inputFilter.style.margin = '10px';
    v_inputFilter.style.flex = '1 0 auto';
    v_inputFilter.classList.add('advanced-object-search-input-text');
    v_filterContainerDiv.appendChild(v_inputFilter);

    var v_divCase = document.createElement('div');
    v_divCase.style.margin = '10px';
    v_divCase.style.flex = '0 0 auto';
    v_filterContainerDiv.appendChild(v_divCase);

    var v_inputCase = document.createElement('input');
    v_inputCase.type = 'checkbox';
    v_inputCase.style.margin = '10px';
    v_inputCase.classList.add('advanced-object-search-input-case');
    v_divCase.appendChild(v_inputCase);

    var v_spanCase = document.createElement('span');
    v_spanCase.innerHTML = 'Case-sensitive';
    v_spanCase.className = 'query_info';
    v_divCase.appendChild(v_spanCase);

    var v_divRegex = document.createElement('div');
    v_divRegex.style.margin = '10px';
    v_divRegex.style.flex = '0 0 auto';
    v_filterContainerDiv.appendChild(v_divRegex);

    var v_inputRegex = document.createElement('input');
    v_inputRegex.type = 'checkbox';
    v_inputRegex.style.margin = '10px';
    v_inputRegex.classList.add('advanced-object-search-input-regex');
    v_divRegex.appendChild(v_inputRegex);

    /*v_inputRegex.addEventListener(
        'click',
        function(p_inputCase, p_spanCase, p_event) {
            p_inputCase.disabled = this.checked;

            if (this.checked) {
                p_spanCase.style.opacity = '0.5';
            } else {
                p_spanCase.style.opacity = '';
            }
        }.bind(v_inputRegex, v_inputCase, v_spanCase)
    );*/

    var v_spanRegex = document.createElement('span');
    v_spanRegex.innerHTML = 'Regular Expression';
    v_divRegex.appendChild(v_spanRegex);

    var v_optionsHeader = document.createElement('h3');
    v_optionsHeader.innerHTML = 'Categories Filter';
    v_optionsHeader.style.marginLeft = '10px';
    v_optionsHeader.style.marginBottom = '0px';
    v_optionsHeader.style.flex = '0 0 auto';
    v_containerDiv.appendChild(v_optionsHeader);

    var v_optionsContainerDiv = document.createElement('div');
    v_optionsContainerDiv.style.display = 'grid';
    v_optionsContainerDiv.style.gridTemplateColumns = '1fr 1fr 1fr 1fr';
    v_optionsContainerDiv.style.gridRowGap = '10px';
    v_optionsContainerDiv.style.gridColumnGap = '10px';
    v_optionsContainerDiv.style.justifyItems = 'start';
    v_optionsContainerDiv.style.boxSizing = 'border-box';
    v_optionsContainerDiv.style.padding = '10px';
    v_containerDiv.appendChild(v_optionsContainerDiv);

    if (parseInt(getMajorVersionPostgresql(node.tree.tag.version)) >= 10) {
      var v_optionList = [{
              'text': 'Data',
              'value': 1
          }, {
              'text': 'FK Name',
              'value': 2
          }, {
              'text': 'Function Definition',
              'value': 3
          }, {
              'text': 'Function Name',
              'value': 4
          }, {
              'text': 'Index Name',
              'value': 5
          }, {
              'text': 'Materialized View Column Name',
              'value': 6
          }, {
              'text': 'Materialized View Name',
              'value': 7
          }, {
              'text': 'PK Name',
              'value': 8
          }, {
              'text': 'Schema Name',
              'value': 9
          }, {
              'text': 'Sequence Name',
              'value': 10
          }, {
              'text': 'Table Column Name',
              'value': 11
          }, {
              'text': 'Table Name',
              'value': 12
          }, {
              'text': 'Trigger Name',
              'value': 13
          }, {
              'text': 'Trigger Source',
              'value': 14
          }, {
              'text': 'Unique Name',
              'value': 15
          }, {
              'text': 'View Column Name',
              'value': 16
          }, {
              'text': 'View Name',
              'value': 17
          }, {
              'text': 'Check Name',
              'value': 18
          }, {
              'text': 'Rule Name',
              'value': 19
          }, {
              'text': 'Rule Definition',
              'value': 20
          }, {
              'text': 'Inherited Table Name',
              'value': 21
          }, {
              'text': 'Partition Name',
              'value': 22
          }, {
              'text': 'Role Name',
              'value': 23
          }, {
              'text': 'Tablespace Name',
              'value': 24
          }, {
              'text': 'Extension Name',
              'value': 25
          }, {
              'text': 'FK Column Name',
              'value': 26
          }, {
              'text': 'PK Column Name',
              'value': 27
          }, {
              'text': 'Unique Column Name',
              'value': 28
          }, {
              'text': 'Index Column Name',
              'value': 29
          }, {
              'text': 'Check Definition',
              'value': 30
          }, {
              'text': 'Table Trigger Name',
              'value': 31
          }, {
              'text': 'Materialized View Definition',
              'value': 32
          }, {
              'text': 'View Definition',
              'value': 33
          }, {
              'text': 'Type Name',
              'value': 34
          }, {
              'text': 'Domain Name',
              'value': 35
          }, {
              'text': 'Event Trigger Name',
              'value': 36
          }, {
              'text': 'Event Trigger Function Name',
              'value': 37
          }, {
              'text': 'Event Trigger Function Definition',
              'value': 38
          }, {
              'text': 'Procedure Name',
              'value': 39
          }, {
              'text': 'Procedure Definition',
              'value': 40
          }
      ];
    } else if (parseInt(getMajorVersionPostgresql(node.tree.tag.version)) >= 10) {
      var v_optionList = [{
              'text': 'Data',
              'value': 1
          }, {
              'text': 'FK Name',
              'value': 2
          }, {
              'text': 'Function Definition',
              'value': 3
          }, {
              'text': 'Function Name',
              'value': 4
          }, {
              'text': 'Index Name',
              'value': 5
          }, {
              'text': 'Materialized View Column Name',
              'value': 6
          }, {
              'text': 'Materialized View Name',
              'value': 7
          }, {
              'text': 'PK Name',
              'value': 8
          }, {
              'text': 'Schema Name',
              'value': 9
          }, {
              'text': 'Sequence Name',
              'value': 10
          }, {
              'text': 'Table Column Name',
              'value': 11
          }, {
              'text': 'Table Name',
              'value': 12
          }, {
              'text': 'Trigger Name',
              'value': 13
          }, {
              'text': 'Trigger Source',
              'value': 14
          }, {
              'text': 'Unique Name',
              'value': 15
          }, {
              'text': 'View Column Name',
              'value': 16
          }, {
              'text': 'View Name',
              'value': 17
          }, {
              'text': 'Check Name',
              'value': 18
          }, {
              'text': 'Rule Name',
              'value': 19
          }, {
              'text': 'Rule Definition',
              'value': 20
          }, {
              'text': 'Inherited Table Name',
              'value': 21
          }, {
              'text': 'Partition Name',
              'value': 22
          }, {
              'text': 'Role Name',
              'value': 23
          }, {
              'text': 'Tablespace Name',
              'value': 24
          }, {
              'text': 'Extension Name',
              'value': 25
          }, {
              'text': 'FK Column Name',
              'value': 26
          }, {
              'text': 'PK Column Name',
              'value': 27
          }, {
              'text': 'Unique Column Name',
              'value': 28
          }, {
              'text': 'Index Column Name',
              'value': 29
          }, {
              'text': 'Check Definition',
              'value': 30
          }, {
              'text': 'Table Trigger Name',
              'value': 31
          }, {
              'text': 'Materialized View Definition',
              'value': 32
          }, {
              'text': 'View Definition',
              'value': 33
          }, {
              'text': 'Type Name',
              'value': 34
          }, {
              'text': 'Domain Name',
              'value': 35
          }, {
              'text': 'Event Trigger Name',
              'value': 36
          }, {
              'text': 'Event Trigger Function Name',
              'value': 37
          }, {
              'text': 'Event Trigger Function Definition',
              'value': 38
          }
      ];
    } else {
      var v_optionList = [{
              'text': 'Data',
              'value': 1
          }, {
              'text': 'FK Name',
              'value': 2
          }, {
              'text': 'Function Definition',
              'value': 3
          }, {
              'text': 'Function Name',
              'value': 4
          }, {
              'text': 'Index Name',
              'value': 5
          }, {
              'text': 'Materialized View Column Name',
              'value': 6
          }, {
              'text': 'Materialized View Name',
              'value': 7
          }, {
              'text': 'PK Name',
              'value': 8
          }, {
              'text': 'Schema Name',
              'value': 9
          }, {
              'text': 'Sequence Name',
              'value': 10
          }, {
              'text': 'Table Column Name',
              'value': 11
          }, {
              'text': 'Table Name',
              'value': 12
          }, {
              'text': 'Trigger Name',
              'value': 13
          }, {
              'text': 'Trigger Source',
              'value': 14
          }, {
              'text': 'Unique Name',
              'value': 15
          }, {
              'text': 'View Column Name',
              'value': 16
          }, {
              'text': 'View Name',
              'value': 17
          }, {
              'text': 'Check Name',
              'value': 18
          }, {
              'text': 'Rule Name',
              'value': 19
          }, {
              'text': 'Rule Definition',
              'value': 20
          }, {
              'text': 'Inherited Table Name',
              'value': 21
          }, {
              'text': 'Role Name',
              'value': 22
          }, {
              'text': 'Tablespace Name',
              'value': 23
          }, {
              'text': 'Extension Name',
              'value': 24
          }, {
              'text': 'FK Column Name',
              'value': 25
          }, {
              'text': 'PK Column Name',
              'value': 26
          }, {
              'text': 'Unique Column Name',
              'value': 27
          }, {
              'text': 'Index Column Name',
              'value': 28
          }, {
              'text': 'Check Definition',
              'value': 29
          }, {
              'text': 'Table Trigger Name',
              'value': 30
          }, {
              'text': 'Materialized View Definition',
              'value': 31
          }, {
              'text': 'View Definition',
              'value': 32
          }, {
              'text': 'Type Name',
              'value': 33
          }, {
              'text': 'Domain Name',
              'value': 34
          }, {
              'text': 'Event Trigger Name',
              'value': 35
          }, {
              'text': 'Event Trigger Function Name',
              'value': 36
          }, {
              'text': 'Event Trigger Function Definition',
              'value': 37
          }
      ];
    }

    var v_compare = function(a, b) {
        if (a.text < b.text) {
            return -1;
        } else if (a.text > b.text) {
            return 1;
        } else {
            return 0;
        }
    }

    v_optionList.sort(v_compare);

    var v_inputDataFilter = document.createElement('input');
    var v_dataFilterHeader = document.createElement('h3');

    for (var i = 0; i < v_optionList.length; i++) {
        var v_divOption = document.createElement('div');
        v_optionsContainerDiv.appendChild(v_divOption);

        var v_inputOption = document.createElement('input');
        v_inputOption.type = 'checkbox';
        v_inputOption.value = v_optionList[i].text;
        v_inputOption.classList.add('advanced-object-search-input-option');
        v_divOption.appendChild(v_inputOption);

        if(v_optionList[i].text == 'Data') {
            v_inputOption.addEventListener(
                'click',
                function(p_inputDataFilter, p_dataFilterHeader, p_event) {
                    p_inputDataFilter.disabled = !this.checked;

                    if(!this.checked) {
                        p_dataFilterHeader.style.opacity = '0.5';
                    }
                    else {
                        p_dataFilterHeader.style.opacity = '';
                    }
                }.bind(v_inputOption, v_inputDataFilter, v_dataFilterHeader)
            );
        }

        var v_spanOption = document.createElement('span');
        v_spanOption.innerHTML = v_optionList[i].text;
        v_divOption.appendChild(v_spanOption);
    }

    var v_categoriesButtonsContainer = document.createElement('div');
    v_categoriesButtonsContainer.style.display = 'flex';
    v_categoriesButtonsContainer.style.flex = '0 0 auto';
    v_containerDiv.appendChild(v_categoriesButtonsContainer);

    var v_buttonSelectAllCategories = document.createElement('button');
    v_buttonSelectAllCategories.style.margin = '10px';
    v_buttonSelectAllCategories.innerHTML = 'Select All';

    v_buttonSelectAllCategories.addEventListener(
        'click',
        function(p_event) {
            var v_grandParent = this.parentElement.parentElement;

            var v_categoryList = v_grandParent.querySelectorAll(
                '.advanced-object-search-input-option');

            for (var i = 0; i < v_categoryList.length; i++) {
                if (!v_categoryList[i].checked) {
                    v_categoryList[i].click();
                }
            }
        }
    );

    v_categoriesButtonsContainer.appendChild(v_buttonSelectAllCategories);

    var v_buttonUnselectAllCategories = document.createElement('button');
    v_buttonUnselectAllCategories.style.margin = '10px';
    v_buttonUnselectAllCategories.innerHTML = 'Unselect All';

    v_buttonUnselectAllCategories.addEventListener(
        'click',
        function(p_event) {
            var v_grandParent = this.parentElement.parentElement;

            var v_categoryList = v_grandParent.querySelectorAll(
                '.advanced-object-search-input-option');

            for (var i = 0; i < v_categoryList.length; i++) {
                if (v_categoryList[i].checked) {
                    v_categoryList[i].click();
                }
            }
        }
    );

    v_categoriesButtonsContainer.appendChild(v_buttonUnselectAllCategories);

    var v_schemasHeader = document.createElement('h3');
    v_schemasHeader.innerHTML = 'Schemas Filter';
    v_schemasHeader.style.marginLeft = '10px';
    v_schemasHeader.style.marginBottom = '0px';
    v_schemasHeader.style.flex = '0 0 auto';
    v_containerDiv.appendChild(v_schemasHeader);

    var v_schemasContainerDiv = document.createElement('div');
    v_schemasContainerDiv.style.display = 'grid';
    v_schemasContainerDiv.style.gridTemplateColumns = '1fr 1fr 1fr 1fr 1fr';
    v_schemasContainerDiv.style.gridRowGap = '10px';
    v_schemasContainerDiv.style.gridColumnGap = '10px';
    v_schemasContainerDiv.style.justifyItems = 'start';
    v_schemasContainerDiv.style.boxSizing = 'border-box';
    v_schemasContainerDiv.style.padding = '10px';
    v_containerDiv.appendChild(v_schemasContainerDiv);

    execAjax('/get_schemas_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id
        }),
        function(p_schemasContainerDiv, p_return) {
            var v_schemaList = p_return.v_data;

            var v_compare = function(a, b) {
                if (a.v_name < b.v_name) {
                    return -1;
                } else if (a.v_name > b.v_name) {
                    return 1;
                } else {
                    return 0;
                }
            }

            v_schemaList.sort(v_compare);

            v_disconsiderSchemas = {
                'information_schema': 1,
                'omnidb': 1,
                'pg_catalog': 1,
                'pg_toast': 1
            }

            for (var i = 0; i < v_schemaList.length; i++) {
                if (!(v_schemaList[i].v_name in v_disconsiderSchemas) && (
                        v_schemaList[i].v_name.search(/pg.*temp.*/) == -1)) {
                    var v_divSchema = document.createElement('div');
                    p_schemasContainerDiv.appendChild(v_divSchema);

                    var v_inputSchema = document.createElement('input');
                    v_inputSchema.type = 'checkbox';
                    v_inputSchema.value = v_schemaList[i].v_name;
                    v_inputSchema.classList.add('advanced-object-search-input-schema');
                    v_divSchema.appendChild(v_inputSchema);

                    var v_spanSchema = document.createElement('span');
                    v_spanSchema.innerHTML = v_schemaList[i].v_name;
                    v_divSchema.appendChild(v_spanSchema);
                }
            }
        }.bind(null, v_schemasContainerDiv),
        function(p_return) {
            showAlert(p_return.v_data);
        },
        'box',
        false
    );

    var v_schemasButtonsContainer = document.createElement('div');
    v_schemasButtonsContainer.style.display = 'flex';
    v_schemasButtonsContainer.style.flex = '0 0 auto';
    v_containerDiv.appendChild(v_schemasButtonsContainer);

    var v_buttonSelectAllSchemas = document.createElement('button');
    v_buttonSelectAllSchemas.style.margin = '10px';
    v_buttonSelectAllSchemas.innerHTML = 'Select All';

    v_buttonSelectAllSchemas.addEventListener(
        'click',
        function(p_event) {
            var v_grandParent = this.parentElement.parentElement;

            var v_schemaList = v_grandParent.querySelectorAll(
                '.advanced-object-search-input-schema');

            for (var i = 0; i < v_schemaList.length; i++) {
                if (!v_schemaList[i].checked) {
                    v_schemaList[i].click();
                }
            }
        }
    );

    v_schemasButtonsContainer.appendChild(v_buttonSelectAllSchemas);

    var v_buttonUnselectAllSchemas = document.createElement('button');
    v_buttonUnselectAllSchemas.style.margin = '10px';
    v_buttonUnselectAllSchemas.innerHTML = 'Unselect All';

    v_buttonUnselectAllSchemas.addEventListener(
        'click',
        function(p_event) {
            var v_grandParent = this.parentElement.parentElement;

            var v_schemaList = v_grandParent.querySelectorAll(
                '.advanced-object-search-input-schema');

            for (var i = 0; i < v_schemaList.length; i++) {
                if (v_schemaList[i].checked) {
                    v_schemaList[i].click();
                }
            }
        }
    );

    v_schemasButtonsContainer.appendChild(v_buttonUnselectAllSchemas);

    v_dataFilterHeader.innerHTML = 'Data Category Filter';
    v_dataFilterHeader.style.opacity = '0.5'
    v_dataFilterHeader.style.marginLeft = '10px';
    v_dataFilterHeader.style.marginBottom = '0px';
    v_dataFilterHeader.style.flex = '0 0 auto';
    v_containerDiv.appendChild(v_dataFilterHeader);

    var v_dataFilterContainerDiv = document.createElement('div');
    v_dataFilterContainerDiv.style.display = 'flex';
    v_dataFilterContainerDiv.style.flex = '0 0 auto';
    v_containerDiv.appendChild(v_dataFilterContainerDiv);

    v_inputDataFilter.type = 'text';
    v_inputDataFilter.disabled = true;
    v_inputDataFilter.placeholder = 'Type the filter to be applied to data category...';
    v_inputDataFilter.style.margin = '10px';
    v_inputDataFilter.style.flex = '1 0 auto';
    v_inputDataFilter.classList.add('advanced-object-search-data-input-text');
    v_dataFilterContainerDiv.appendChild(v_inputDataFilter);

    var v_buttonStart = document.getElementById('bt_start_' + v_tab.id);

    v_buttonStart.addEventListener(
        'click',
        function(p_event) {
            if (v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.state != v_queryState.Idle) {
        		showAlert('Tab with activity in progress.');
        	}
            else {
                var v_parent = this.parentElement;

                var v_data = {
                    text: '',
                    regex: false,
                    caseSensitive: false,
                    categoryList: [],
                    schemaList: [],
                    dataCategoryFilter: ''
                };

                var v_inputFilter = v_parent.querySelector('.advanced-object-search-input-text');

                if (v_inputFilter != null) {
                    v_data.text = v_inputFilter.value;
                }

                if (v_data.text.trim() == '') {
                    showAlert('Please, provide a string in order to search.');
                    return;
                }

                var v_inputCase = v_parent.querySelector('.advanced-object-search-input-case');

                if (v_inputCase != null) {
                    v_data.caseSensitive = v_inputCase.checked;
                }

                var v_inputRegex = v_parent.querySelector('.advanced-object-search-input-regex');

                if (v_inputRegex != null) {
                    v_data.regex = v_inputRegex.checked;
                }

                var v_categoryList = v_parent.querySelectorAll('.advanced-object-search-input-option');

                for (var i = 0; i < v_categoryList.length; i++) {
                    if (v_categoryList[i].checked) {
                        v_data.categoryList.push(v_categoryList[i].value);

                        if (v_categoryList[i].value == 'Data') {
                            var v_dataInputFilter = v_parent.querySelector('.advanced-object-search-data-input-text');

                            if (v_dataInputFilter != null) {
                                v_data.dataCategoryFilter = v_dataInputFilter.value;
                            }
                        }
                    }
                }

                if (v_data.categoryList.length == 0) {
                    showAlert('Please, select at least one category to search.');
                    return;
                }

                var v_schemaList = v_parent.querySelectorAll('.advanced-object-search-input-schema');

                for (var i = 0; i < v_schemaList.length; i++) {
                    if (v_schemaList[i].checked) {
                        v_data.schemaList.push(v_schemaList[i].value);
                    }
                }

                if (v_data.schemaList.length == 0) {
                    showAlert('Please, select at least one schema to search.');
                    return;
                }

                if (v_data.categoryList.indexOf('Data') != -1) {
                    showConfirm(
                        'You have selected the category "Data". Please, be aware that it can consume a considerable amount of time, depending on selected schemas size. Do you want to proceed?',
                        function(p_data) {
                            queryAdvancedObjectSearch(p_data);
                        }.bind(null, v_data)
                    );
                } else {
                    queryAdvancedObjectSearch(v_data);
                }
            }
        }
    );

    var v_curr_tabs = createTabControl('query_result_tabs_' + v_tab.id, 0, null);

    var v_tab_db_id = null;

    var v_tag = {
        tab_id: v_tab.id,
        mode: 'data_mining',
        editorDivId: 'txt_query_' + v_tab.id,
        query_info: document.getElementById('div_query_info_' + v_tab.id),
        div_result: document.getElementById('div_result_' + v_tab.id),
        div_notices: document.getElementById('div_notices_' + v_tab.id),
        div_count_notices: document.getElementById(
            'query_result_tabs_count_notices_' + v_tab.id),
        sel_filtered_data: document.getElementById('sel_filtered_data_' +
            v_tab.id),
        sel_export_type: document.getElementById('sel_export_type_' + v_tab
            .id),
        tab_title_span: v_tab_title_span,
        tab_loading_span: v_tab_loading_span,
        tab_close_span: v_tab_close_span,
        tab_check_span: v_tab_check_span,
        bt_start: document.getElementById('bt_start_' + v_tab.id),
        bt_cancel: document.getElementById('bt_cancel_' + v_tab.id),
        state: 0,
        context: null,
        tabControl: v_connTabControl.selectedTab.tag.tabControl,
        queryTabControl: v_curr_tabs,
        currQueryTab: null,
        connTab: v_connTabControl.selectedTab,
        currDatabaseIndex: null,
        tab_db_id: v_tab_db_id
    };

    v_tab.tag = v_tag;

    var v_selectDataTabFunc = function() {
        v_curr_tabs.selectTabIndex(0);
        v_tag.currQueryTab = 'data';
        refreshHeights();
    }

    var v_selectMessageTabFunc = function() {
        v_curr_tabs.selectTabIndex(1);
        v_tag.currQueryTab = 'message';
        v_tag.div_count_notices.style.display = 'none';
        refreshHeights();
    }

    v_tag.selectDataTabFunc = v_selectDataTabFunc;
    v_curr_tabs.tabList[0].elementLi.onclick = v_selectDataTabFunc;

    v_selectDataTabFunc();

    var v_add_tab = v_connTabControl.selectedTab.tag.tabControl.createTab('+',false,function(e) {showMenuNewTab(e); },null,null,null,null,null,false);

    v_add_tab.tag = {
        mode: 'add'
    }

    setTimeout(
        function() {
            refreshHeights();
        },
        10
    );

    var v_instance1 = new Tooltip($(v_connTabControl.selectedTab.tag.tabControl.selectedLi),{
      title: 'Adjust parameters and run!',
      placement: 'top',
      container: 'body'
    });
    v_instance1.show();
    window.setTimeout(function() {
        v_instance1.dispose();
    }, 4000);

    var v_instance2 = new Tooltip($('.advanced-object-search-input-text'),{
      title: '<div style="text-align: left;">If Regular Expression is not selected, the pattern will work as follows:<br /><br />' +
      '- if it does not contain sql % wildcard, it will put your pattern between two % <br /><br />' +
      '- else it will consider your pattern as it is.</div>',
      placement: "bottom",
      container: 'body',
      html: true
    });

    var v_instance3 = new Tooltip($('.advanced-object-search-data-input-text'),{
      title: '<div style="text-align: left;">If Data category is selected you can use it to filter search space and get a faster response.<br /><br />' +
      'If you want to filter you must fill it with a | separeted list of patterns that may use % wildcard.<br /><br />' +
      'For example: public.%mytable%|mysch%ema.% will search for data just in tables that match given patterns.</div>',
      placement: "top",
      container: 'body',
      html: true
    });

}

/// <summary>
/// Retrieving tree.
/// </summary>
function getTreePostgresql(p_div) {

    var context_menu = {
        'cm_database': {
            elements: [
            /*, {
                text: 'Advanced Object Search',
                icon: 'fas cm-all fa-search',
                action: function(node) {
                  checkCurrentDatabase(node, true, function() {
                      tabAdvancedObjectSearch(node);
                  }, function() {
                      node.collapseNode();
                  })
                }
            }*/
          ]
        },
        'cm_function': {
            elements: [
            // {
            //     text: 'Debug Function',
            //     icon: 'fas cm-all fa-bug',
            //     action: function(node) {
            //         v_connTabControl.tag.createDebuggerTab(
            //             node.text);
            //         setupDebug(node, 'f');
            //     }
            // }
                ]
        },
        
        'cm_procedure': {
            elements: [
            //  {
            //     text: 'Debug Procedure',
            //     icon: 'fas cm-all fa-bug',
            //     action: function(node) {
            //         v_connTabControl.tag.createDebuggerTab(
            //             node.text);
            //         setupDebug(node, 'p');
            //     }
            // }
            ]
        },
    };

    const div_tree = document.getElementById(p_div);
    div_tree.innerHTML =
      '<tree-postgresql :database-index="databaseIndex" :tab-id="tabId"></tree-postgresql>';
    const app = createApp({
      components: {
        "tree-postgresql": Vue.defineAsyncComponent(() =>
          loadModule(
            "../static/assets/js/vuejs/components/TreePostgresql.vue",
            options
          )
        ),
      },
      data() {
        return {
          databaseIndex:
            window.v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
          tabId: window.v_connTabControl.selectedTab.id,
        };
      },
    });
    app.mount(`#${p_div}`);

    // save tree referece in the tab, it will be later used to destroy tree instance on tab close
    v_connTabControl.selectedTab.tree = app

    let autocomplete_switch_status =
      v_connTabControl.selectedTab.tag.enable_autocomplete !== false
        ? " checked "
        : "";
    v_connTabControl.selectedTab.tag.divDetails.innerHTML = `<i class="fas fa-server mr-1"></i>selected DB: 
        <b>${v_connTabControl.selectedTab.tag.selectedDatabase}</b>
        <div class="omnidb__switch omnidb__switch--sm float-right" data-toggle="tooltip" data-placement="bottom" data-html="true" title="" data-original-title="<h5>Toggle autocomplete.</h5><div>Switch OFF <b>disables the autocomplete</b> on the inner tabs for this connection.</div>">
    	    <input type="checkbox" ${autocomplete_switch_status} id="autocomplete_toggler_${v_connTabControl.selectedTab.tag.tab_id}" class="omnidb__switch--input" onchange="toggleConnectionAutocomplete(\'autocomplete_toggler_${v_connTabControl.selectedTab.tag.tab_id}\')">
    	    <label for="autocomplete_toggler_${v_connTabControl.selectedTab.tag.tab_id}" class="omnidb__switch--label">
                <span>
                    <i class="fas fa-spell-check"></i>
                </span>
            </label>
		</div>`;

    // tree.clickNodeEvent = function(node) {
    //   if (v_connTabControl.selectedTab.tag.treeTabsVisible) {
    //     getPropertiesPostgresql(node);
    //   }
    //   else {
    //     // Do nothing
    //   }
    // }

    // tree.beforeContextMenuEvent = function(node, callback) {

    //     var v_elements = [];
    //     //Hooks
    //     if (v_connTabControl.tag.hooks.postgresqlTreeContextMenu.length>0) {
    //       for (var i=0; i<v_connTabControl.tag.hooks.postgresqlTreeContextMenu.length; i++)
    //         v_elements = v_elements.concat(v_connTabControl.tag.hooks.postgresqlTreeContextMenu[i](node));
    //     }

    //     var v_customCallback = function() {
    //       callback(v_elements);
    //     }
    //     checkCurrentDatabase(node, false, v_customCallback);
    // }
}

function checkCurrentDatabase(p_node, p_complete_check, p_callback_continue,
    p_callback_stop) {
    if ((p_node.tag != null && p_node.tag.database != null && p_node.tag.database !=
            v_connTabControl.selectedTab.tag.selectedDatabase) && (
            p_complete_check || (!p_complete_check && p_node.tag.type !=
                'database'))) {
        let isAllowed  = checkBeforeChangeDatabase(p_callback_stop)
        if (isAllowed){
            execAjax('/change_active_database/',
                JSON.stringify({
                    "p_database_index": v_connTabControl
                        .selectedTab.tag.selectedDatabaseIndex,
                    "p_tab_id": v_connTabControl.selectedTab
                        .id,
                    "p_database": p_node.tag.database
                }),
                function(p_return) {

                        v_connTabControl.selectedTab.tag.divDetails
                            .innerHTML =
                            '<i class="fas fa-server mr-1"></i>selected DB: <b>' + p_node
                            .tag.database + '</b>';

                        v_connTabControl.selectedTab.tag.selectedDatabaseNode
                            .clearNodeBold();
                        //searching new selected database node
                        var v_list_database_nodes = p_node.tree
                            .childNodes[0].childNodes[0].childNodes;
                        for (var i = 0; i <
                            v_list_database_nodes.length; i++
                        ) {
                            if (p_node.tag.database ==
                                v_list_database_nodes[i].text
                                .replace(/"/g, '')) {
                                v_list_database_nodes[i].setNodeBold();
                                v_connTabControl.selectedTab
                                    .tag.selectedDatabase =
                                    p_node.tag.database;
                                v_connTabControl.selectedTab
                                    .tag.selectedDatabaseNode =
                                    v_list_database_nodes[i];

                                if (v_connTabControl.selectedTab.tag.selectedTitle!='')
                                        v_connTabControl.selectedTab.tag.tabTitle.innerHTML = '<img src="' + v_url_folder + '/static/assets/images/' + v_connTabControl.selectedTab.tag.selectedDBMS + '_medium.png"/> ' + v_connTabControl.selectedTab.tag.selectedTitle + ' - ' + v_connTabControl.selectedTab.tag.selectedDatabase;
                                    else
                                        v_connTabControl.selectedTab.tag.tabTitle.innerHTML = '<img src="' + v_url_folder + '/static/assets/images/' + v_connTabControl.selectedTab.tag.selectedDBMS + '_medium.png"/> ' + v_connTabControl.selectedTab.tag.selectedDatabase;
                            }

                        }
                        if (p_callback_continue)
                            p_callback_continue();

                },
                function(p_return) {
                    nodeOpenErrorPostgresql(p_return, node);
                },
                'box');
        }
    } else
        p_callback_continue();
}


/// <summary>
/// Refreshing tree node.
/// </summary>
/// <param name="node">Node object.</param>
function refreshTreePostgresql(p_node) {
    checkCurrentDatabase(p_node, true, function() {
        refreshTreePostgresqlConfirm(p_node);
    }, function() {
        p_node.collapseNode();
    });
}

/// <summary>
/// Refreshing tree node.
/// </summary>
/// <param name="node">Node object.</param>
function getPropertiesPostgresql(p_node) {
    checkCurrentDatabase(p_node, false, function() {
        getPropertiesPostgresqlConfirm(p_node);
    })
}

/// <summary>
/// Retrieving properties.
/// </summary>
/// <param name="node">Node object.</param>
function getPropertiesPostgresqlConfirm(node) {
    if (node.tag != undefined) {
        if (node.tag.type == 'role') {
            getProperties('/get_properties_postgresql/', {
                p_schema: null,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'tablespace') {
            getProperties('/get_properties_postgresql/', {
                p_schema: null,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'database') {
            getProperties('/get_properties_postgresql/', {
                p_schema: null,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'extension') {
            getProperties('/get_properties_postgresql/', {
                p_schema: null,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'schema') {
            getProperties('/get_properties_postgresql/', {
                p_schema: null,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'table') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'table_field') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: node.parent.parent.text,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'sequence') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'view') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'mview') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'function') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.tag.id,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'procedure') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.tag.id,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'trigger') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: node.parent.parent.text,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'eventtrigger') {
            getProperties('/get_properties_postgresql/', {
                p_schema: null,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'triggerfunction') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.tag.id,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'direct_triggerfunction') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.tag.id,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'eventtriggerfunction') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.tag.id,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'direct_eventtriggerfunction') {
            getProperties('/get_properties_postgresql/', {
                p_schema: null,
                p_table: null,
                p_object: node.tag.id,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'index') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: node.parent.parent.text,
                p_object: node.text.replace(' (Non Unique)', '').replace(
                    ' (Unique)', ''),
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'pk') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: node.parent.parent.text,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'foreign_key') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: node.parent.parent.text,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'unique') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: node.parent.parent.text,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'check') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: node.parent.parent.text,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'exclude') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: node.parent.parent.text,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'rule') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: node.parent.parent.text,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'foreign_table') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'user_mapping') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.foreign_server,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'foreign_server') {
            getProperties('/get_properties_postgresql/', {
                p_schema: null,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'fdw') {
            getProperties('/get_properties_postgresql/', {
                p_schema: null,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'type') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'domain') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'publication') {
            getProperties('/get_properties_postgresql/', {
                p_schema: null,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'subscription') {
            getProperties('/get_properties_postgresql/', {
                p_schema: null,
                p_table: null,
                p_object: node.text,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'statistic') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.tag.statistics,
                p_type: node.tag.type
            });
        } else if (node.tag.type == 'aggregate') {
            getProperties('/get_properties_postgresql/', {
                p_schema: node.tag.schema,
                p_table: null,
                p_object: node.tag.id,
                p_type: node.tag.type
            });
        } else {
            clearProperties();
        }
    }

    //Hooks
    if (v_connTabControl.tag.hooks.postgresqlTreeNodeClick.length>0) {
      for (var i=0; i<v_connTabControl.tag.hooks.postgresqlTreeNodeClick.length; i++)
        v_connTabControl.tag.hooks.postgresqlTreeNodeClick[i](node);
    }
}


function afterNodeOpenedCallbackPostgreSQL(node) {
  //Hooks
  if (v_connTabControl.tag.hooks.postgresqlTreeNodeOpen.length>0) {
    for (var i=0; i<v_connTabControl.tag.hooks.postgresqlTreeNodeOpen.length; i++)
      v_connTabControl.tag.hooks.postgresqlTreeNodeOpen[i](node);
  }
}

/// <summary>
/// Retrieving function definition.
/// </summary>
/// <param name="node">Node object.</param>
function getDebugFunctionDefinitionPostgresql(node) {

    execAjax('/get_function_debug_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_function": node.tag.id
        }),
        function(p_return) {

            v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor
                .setValue(p_return.v_data);
            v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor
                .clearSelection();
            v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor
                .gotoLine(0, 0, true);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        true);

}

/// <summary>
/// Retrieving procedure definition.
/// </summary>
/// <param name="node">Node object.</param>
function getDebugProcedureDefinitionPostgresql(node) {

    execAjax('/get_procedure_debug_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_procedure": node.tag.id
        }),
        function(p_return) {

            v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor
                .setValue(p_return.v_data);
            v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor
                .clearSelection();
            v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor
                .gotoLine(0, 0, true);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        true);

}

/// <summary>
/// Retrieving SELECT SQL template.
/// </summary>
function TemplateSelectPostgresql(p_schema, p_table, p_kind) {

    execAjax('/template_select_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_table": p_table,
            "p_schema": p_schema,
            "p_kind": p_kind
        }),
        function(p_return) {
            let v_tab_name = `${v_connTabControl.selectedTab.tag.selectedDatabase}@${p_schema}.${p_table}`
            v_connTabControl.tag.createQueryTab(v_tab_name);

            var v_tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;
            v_tab_tag.editor.setValue(p_return.v_data.v_template);
            v_tab_tag.editor.clearSelection();

            querySQL(0);
        },
        function(p_return) {
            showError(p_return.v_data);
            return '';
        },
        'box',
        true);
}

/// <summary>
/// Retrieving INSERT SQL template.
/// </summary>
function TemplateInsertPostgresql(p_schema, p_table) {

    execAjax('/template_insert_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_table": p_table,
            "p_schema": p_schema
        }),
        function(p_return) {
          tabSQLTemplate(
              'Insert ' + p_schema + '.' + p_table,
              p_return.v_data.v_template);
        },
        function(p_return) {
            showError(p_return.v_data);
            return '';
        },
        'box',
        true);
}

/// <summary>
/// Retrieving UPDATE SQL template.
/// </summary>
function TemplateUpdatePostgresql(p_schema, p_table) {

    execAjax('/template_update_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_table": p_table,
            "p_schema": p_schema
        }),
        function(p_return) {
          tabSQLTemplate(
              'Update ' + p_schema + '.' + p_table,
              p_return.v_data.v_template);
        },
        function(p_return) {
            showError(p_return.v_data);
            return '';
        },
        'box',
        true);
}

/// <summary>
/// Retrieving SELECT FUNCTION SQL template.
/// </summary>
function TemplateSelectFunctionPostgresql(p_schema, p_function, p_functionid) {

    execAjax('/template_select_function_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_function": p_function,
            "p_functionid": p_functionid,
            "p_schema": p_schema
        }),
        function(p_return) {
          tabSQLTemplate(
              'Select ' + p_schema + '.' + p_function,
              p_return.v_data.v_template);
        },
        function(p_return) {
            showError(p_return.v_data);
            return '';
        },
        'box',
        true);
}

/// <summary>
/// Retrieving CALL PROCEDURE SQL template.
/// </summary>
function TemplateCallProcedurePostgresql(p_schema, p_procedure, p_procedureid) {

    execAjax('/template_call_procedure_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_procedure": p_procedure,
            "p_procedureid": p_procedureid,
            "p_schema": p_schema
        }),
        function(p_return) {
          tabSQLTemplate(
              'Call ' + p_schema + '.' + p_procedure,
              p_return.v_data.v_template);
        },
        function(p_return) {
            showError(p_return.v_data);
            return '';
        },
        'box',
        true);
}

function nodeOpenErrorPostgresql(p_return, p_node) {
    if (p_return.v_data.password_timeout) {
        p_node.collapseNode();
        showPasswordPrompt(
            v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            function() {
                p_node.expandNode();
            },
            null,
            p_return.v_data.message
        );
    } else {

        if (p_node.childNodes.length > 0)
            p_node.removeChildNodes();

        v_node = p_node.createChildNode(
            "Error - <a class='a_link' onclick='showError(&quot;" +
            p_return.v_data.message.replace(/\n/g, "<br/>").replace(/'/g, '') +
            "&quot;)'>View Detail</a>", false,
            'fas fa-times node-error', {
                type: 'error',
                message: p_return.v_data
            }, null);
    }

}

function nodeOpenErrorPostgresqlNew(error_response, node) {
    if (error_response.response.data?.password_timeout) {
        node.collapseNode();
        showPasswordPrompt(
            v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            function() {
                node.expandNode();
            },
            null,
            error_response.response.data.data
        );
    } else {

        if (node.childNodes.length > 0)
            node.removeChildNodes();

        node.createChildNode(
            "Error - <a class='a_link' onclick='showError(&quot;" +
            error_response.response.data.data.replace(/\n/g, "<br/>").replace(/"/g, '') +
            "&quot;)'>View Detail</a>", false,
            'fas fa-times node-error', {
                type: 'error',
                message: error_response.response.data.data
            }, null);
    }

}

function getMajorVersionPostgresql(p_version) {
    var v_version = p_version.split(' (')[0]
    var tmp = v_version.replace('PostgreSQL ', '').replace('beta', '.')
                .replace('rc', '.').split('.')
    tmp.pop()
    return tmp.join('.')
}

function postgresqlTerminateBackendConfirm(pid) {
  axios
    .post("/kill_backend_postgresql/", {
      database_index: v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
      tab_id: v_connTabControl.selectedTab.id,
      pid: pid,
    })
    .then((resp) => {
      refreshMonitoring();
    })
    .catch((error) => {
      if (error.response.data?.password_timeout) {
        showPasswordPrompt(
          v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
          function () {
            postgresqlTerminateBackendConfirm(pid);
          },
          null,
          error.response.data.data
        );
      } else {
        showError(error.response.data.data);
      }
    });
}

function postgresqlTerminateBackend(row) {
  createMessageModal(
    `Are you sure you want to terminate backend ${row[2]}?`,
    function () {
      postgresqlTerminateBackendConfirm(row[2]);
    }
  );
}

function getExplain(p_mode) {

    var v_tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;

    var v_query;
    var v_selected_text = v_connTabControl.selectedTab.tag.tabControl.selectedTab
        .tag.editor.getSelectedText();

    if (v_selected_text != '')
        v_query = v_selected_text;
    else
        v_query = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor
        .getValue();

    if (v_query.trim() == '') {
        showAlert('Please provide a string.');
    }
    else {
        let should_prepend = v_query.trim().split(' ')[0].toUpperCase() !== 'EXPLAIN'
        // do not prepend the query with explain stuff if it already have it
        if(should_prepend) {
            if (p_mode == 0) {
                v_query = 'explain ' + v_query;
            }
            else if (p_mode == 1) {
                v_query = 'explain (analyze, buffers) ' + v_query;
            }

        }

        querySQL(0, true, v_query, getExplainReturn, true);
    }
}

function getExplainReturn(p_data) {

    var v_tab_tag = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag;

    v_tab_tag.selectExplainTabFunc();
    if (p_data.v_error) {
        v_tab_tag.div_explain.innerHTML = `<div class="error_text">${p_data.v_data.message}</div>`;
    } else {

        // Adjusting data.
        var v_explain_text = p_data.v_data.v_data.join('\n');

        if (v_explain_control.context === 'default') {
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.div_explain.style.display = 'block';
          if (v_explain_text.length > 0) {
            let v_query = getQueryEditorValue();
            v_tab_tag.div_explain.innerHTML= `<pev2 class="h-100" :plan-source="plan" :plan-query="query" />`;
            vueExplain(v_tab_tag.div_explain.id, v_query, v_explain_text);
            }
        }
    }

    refreshHeights();
}
