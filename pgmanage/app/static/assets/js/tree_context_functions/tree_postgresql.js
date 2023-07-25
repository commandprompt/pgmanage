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
        'cm_tablespaces': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Tablespace',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create Tablespace', node.tree
                        .tag.create_tablespace);
                }
            }, {
                text: 'Doc: Tablespaces',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Tablespaces',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/manage-ag-tablespaces.html'
                    );
                }
            }]
        },
        'cm_tablespace': {
            elements: [{
                text: 'Alter Tablespace',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Alter Tablespace', node.tree
                        .tag.alter_tablespace.replace(
                            '#tablespace_name#', node.text)
                    );
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Tablespace',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Tablespace', node.tree
                        .tag.drop_tablespace.replace(
                            '#tablespace_name#', node.text)
                    );
                }
            }]
        },
        'cm_roles': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Role',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create Role', node.tree.tag
                        .create_role);
                }
            }, {
                text: 'Doc: Roles',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Roles',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/user-manag.html');
                }
            }]
        },
        'cm_role': {
            elements: [{
                text: 'Change Password',
                icon: 'fas cm-all fa-key',
                action: function(node) {
                    var v_html =
                        '<div class="form-row">' +
                        '    <div class="col-md-12 mb-3">' +
                        '        <label for="change_pwd_role">Password</label>' +
                        '        <input type="password" id="change_pwd_role" class="form-control" placeholder="password" />' +
                        '    </div>' +
                        '    <div class="col-md-12 mb-3">' +
                        '        <label for="change_pwd_role_confirm">Password confirmation</label>' +
                        '        <input type="password" id="change_pwd_role_confirm" class="form-control" placeholder="password confirmation" />' +
                        '    </div>' +
                        '</div>';

                    showConfirm(
                        v_html,
                        function(p_node) {
                            var v_password = document.getElementById('change_pwd_role').value;
                            var v_password_confirm = document.getElementById('change_pwd_role_confirm').value;

                            if(v_password == '') {
                                showAlert('Password is empty.');
                                return;
                            }

                            if(v_password_confirm == '') {
                                showAlert('Password confirmation is empty.');
                                return;
                            }

                            if(v_password != v_password_confirm) {
                                showAlert('Passwords do not match.');
                                return;
                            }

                            execAjax(
                                '/change_role_password_postgresql/',
                                JSON.stringify({
                                    "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
                                    "p_tab_id": v_connTabControl.selectedTab.id,
                                    "p_role": p_node.text,
                                    "p_password": v_password
                                }),
                                function(p_return) {
                                    showAlert('Password changed successfully.');
                                },
                                function(p_return) {
                                    showAlert(p_return.v_data.message);
                                },
                                'box',
                                false
                            );
                        }.bind(null, node)
                    )
                }
            }, {
                text: 'Alter Role',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Alter Role', node.tree.tag.alter_role
                        .replace('#role_name#', node.text));
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Role',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Role', node.tree.tag.drop_role
                        .replace('#role_name#', node.text));
                }
            }]
        },
        'cm_extensions': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Extension',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create Extension', node.tree
                        .tag.create_extension);
                }
            }, {

                text: 'Create Extension UI',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    createExtensionModal(node, 'Create')
                }

            }, {
                text: 'Doc: Extensions',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Extensions',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/extend-extensions.html');
                }
            }]
        },
        'cm_extension': {
            elements: [{
                text: 'Alter Extension UI',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    createExtensionModal(node, 'Alter')
                }


            }, {
                text: 'Alter Extension',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Alter Extension', node.tree
                        .tag.alter_extension.replace(
                            '#extension_name#', node.text));
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            },{
                text: 'Drop Extension UI',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    createExtensionModal(node, 'Drop')
                }
            },{
                text: 'Drop Extension',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Role', node.tree.tag.drop_extension
                        .replace('#extension_name#', node.text)
                    );
                }
            }]
        },
        'cm_eventtriggers': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                },
            }, {
                text: 'Create Event Trigger',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create Event Trigger', node.tree.tag
                        .create_eventtrigger);
                }
            }, {
                text: 'Doc: Event Triggers',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Event Triggers',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/event-triggers.html');
                }
            }]
        },
        'cm_eventtrigger': {
            elements: [{
                text: 'Alter Event Trigger',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Alter Trigger', node.tree.tag
                        .alter_eventtrigger.replace(
                            '#trigger_name#', node.text));
                }
            }, {
                text: 'Enable Event Trigger',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Enable Event Trigger', node.tree.tag
                        .enable_eventtrigger.replace(
                            '#trigger_name#', node.text));
                }
            }, {
                text: 'Disable Event Trigger',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Disable Event Trigger', node.tree
                        .tag.disable_eventtrigger.replace(
                            '#trigger_name#', node.text));
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Event Trigger',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Event Trigger', node.tree.tag
                        .drop_eventtrigger.replace(
                            '#trigger_name#', node.text));
                }
            }]
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
        'cm_procedures': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Procedure',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create Procedure', node.tree
                        .tag.create_procedure.replace(
                            '#schema_name#', node.tag.schema
                        ));
                }
            }, {
                text: 'Doc: Procedures',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Procedures',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/sql-createprocedure.html');
                }
            }]
        },
        'cm_procedure': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Call Procedure',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    TemplateCallProcedurePostgresql(node.tag.schema, node.text, node.tag.id);
                }
            }, {
                text: 'Edit Procedure',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    v_connTabControl.tag.createQueryTab(
                        node.text);
                    getProcedureDefinitionPostgresql(node);
                }
            }, {
                text: 'Alter Procedure',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate(
                        'Alter Procedure',
                        node.tree.tag.alter_procedure.replace(
                            '#procedure_name#',
                            node.tag.id
                        )
                    );
                }
            },
            //  {
            //     text: 'Debug Procedure',
            //     icon: 'fas cm-all fa-bug',
            //     action: function(node) {
            //         v_connTabControl.tag.createDebuggerTab(
            //             node.text);
            //         setupDebug(node, 'p');
            //     }
            // }
            , {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Procedure',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Procedure', node.tree.tag
                        .drop_procedure.replace(
                            '#procedure_name#', node.tag.id)
                    );
                }
            }]
        },
        'cm_direct_triggerfunction': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Edit Trigger Function',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    v_connTabControl.tag.createQueryTab(
                        node.text);
                    getTriggerFunctionDefinitionPostgresql(node);
                }
            }, {
                text: 'Alter Trigger Function',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate(
                        'Alter Trigger Function',
                        node.tree.tag.alter_triggerfunction.replace(
                            '#function_name#',
                            node.tag.id
                        )
                    );
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Trigger Function',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Trigger Function',
                        node.tree.tag.drop_triggerfunction.replace(
                            '#function_name#', node.tag.id)
                    );
                }
            }]
        },
        'cm_eventtriggerfunctions': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Event Trigger Function',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create Event Trigger Function',
                        node.tree.tag.create_eventtriggerfunction
                        .replace('#schema_name#', node.tag.schema));
                }
            }, {
                text: 'Doc: Event Trigger Functions',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Event Trigger Functions',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/functions-event-triggers.html');
                }
            }]
        },
        'cm_eventtriggerfunction': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Edit Event Trigger Function',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    v_connTabControl.tag.createQueryTab(
                        node.text);
                    getEventTriggerFunctionDefinitionPostgresql(node);
                }
            }, {
                text: 'Alter Event Trigger Function',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate(
                        'Alter Event Trigger Function',
                        node.tree.tag.alter_eventtriggerfunction.replace(
                            '#function_name#',
                            node.tag.id
                        )
                    );
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Event Trigger Function',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Event Trigger Function',
                        node.tree.tag.drop_eventtriggerfunction.replace(
                            '#function_name#', node.tag.id)
                    );
                }
            }]
        },
        'cm_direct_eventtriggerfunction': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Edit Event Trigger Function',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    v_connTabControl.tag.createQueryTab(
                        node.text);
                    getEventTriggerFunctionDefinitionPostgresql(node);
                }
            }, {
                text: 'Alter Event Trigger Function',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate(
                        'Alter Event Trigger Function',
                        node.tree.tag.alter_eventtriggerfunction.replace(
                            '#function_name#',
                            node.tag.id
                        )
                    );
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Event Trigger Function',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Event Trigger Function',
                        node.tree.tag.drop_eventtriggerfunction.replace(
                            '#function_name#', node.tag.id)
                    );
                }
            }]
        },
        'cm_aggregates': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0) {
                        refreshTreePostgresql(node);
                    }
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Aggregate',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate(
                        'Create Aggregate',
                        node.tree.tag.create_aggregate.replace(
                            '#schema_name#',
                            node.tag.schema
                        )
                    );
                }
            }, {
                text: 'Doc: Aggregates',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Aggregates',
                        'https://www.postgresql.org/docs/' + getMajorVersionPostgresql(node.tree.tag.version) + '/static/sql-createaggregate.html');
                }
            }]
        },
        'cm_aggregate': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0) {
                        refreshTreePostgresql(node);
                    }
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Alter Aggregate',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate(
                        'Alter Aggregate',
                        node.tree.tag.alter_aggregate.replace(
                            '#aggregate_name#',
                            node.tag.id
                        )
                    );
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Aggregate',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate(
                        'Drop Aggregate',
                        node.tree.tag.drop_aggregate.replace(
                            '#aggregate_name#',
                            node.tag.id
                        )
                    );
                }
            }]
        },
        'cm_physicalreplicationslots': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Slot',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate(
                        'Create Physical Replication Slot',
                        node.tree.tag
                        .create_physicalreplicationslot);
                }
            }, {
                text: 'Doc: Replication Slots',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Physical Replication Slots',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/warm-standby.html#streaming-replication-slots'
                    );
                }
            }]
        },
        'cm_physicalreplicationslot': {
            elements: [{
                text: 'Drop Slot',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate(
                        'Drop Physical Replication Slot',
                        node.tree.tag
                        .drop_physicalreplicationslot.replace(
                            '#slot_name#', node.text));
                }
            }]
        },
        'cm_logicalreplicationslots': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Slot',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate(
                        'Create Logical Replication Slot',
                        node.tree.tag
                        .create_logicalreplicationslot);
                }
            }, {
                text: 'Doc: Replication Slots',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Logical Replication Slots',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/logicaldecoding-explanation.html#logicaldecoding-replication-slots'
                    );
                }
            }]
        },
        'cm_logicalreplicationslot': {
            elements: [{
                text: 'Drop Slot',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate(
                        'Drop Logical Replication Slot',
                        node.tree.tag
                        .drop_logicalreplicationslot.replace(
                            '#slot_name#', node.text));
                }
            }]
        },
        'cm_publications': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Publication',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create Publication', node.tree
                        .tag.create_publication);
                }
            }, {
                text: 'Doc: Publications',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Publications',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/logical-replication-publication.html'
                    );
                }
            }]
        },
        'cm_publication': {
            elements: [{
                text: 'Alter Publication',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Alter Publication', node.tree
                        .tag.alter_publication
                        .replace('#pub_name#', node.text));
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Publication',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Publication', node.tree
                        .tag.drop_publication
                        .replace('#pub_name#', node.text));
                }
            }]
        },
        'cm_pubtables': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Add Table',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Add Table', node.tree.tag.add_pubtable
                        .replace('#pub_name#', node.parent.text)
                    );
                }
            }]
        },
        'cm_pubtable': {
            elements: [{
                text: 'Drop Table',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Table', node.tree.tag.drop_pubtable
                        .replace('#pub_name#', node.parent.parent
                            .text)
                        .replace('#table_name#', node.text)
                    );
                }
            }]
        },
        'cm_subscriptions': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Subscription',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create Subscription', node.tree
                        .tag.create_subscription);
                }
            }, {
                text: 'Doc: Subscriptions',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Subscriptions',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/logical-replication-subscription.html'
                    );
                }
            }]
        },
        'cm_subscription': {
            elements: [{
                text: 'Alter Subscription',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Alter Subscription', node.tree
                        .tag.alter_subscription
                        .replace('#sub_name#', node.text));
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Subscription',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Subscription', node.tree
                        .tag.drop_subscription
                        .replace('#sub_name#', node.text));
                }
            }]
        },
        'cm_fdws': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Foreign Data Wrapper',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create Foreign Data Wrapper', node.tree
                        .tag.create_fdw);
                }
            }, {
                text: 'Doc: Foreign Data Wrappers',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Foreign Data Wrappers',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/postgres-fdw.html'
                    );
                }
            }]
        },
        'cm_fdw': {
            elements: [{
                text: 'Alter Foreign Data Wrapper',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Alter Foreign Data Wrapper', node.tree
                        .tag.alter_fdw
                        .replace('#fdwname#', node.text));
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Foreign Data Wrapper',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Foreign Data Wrapper', node.tree
                        .tag.drop_fdw
                        .replace('#fdwname#', node.text));
                }
            }]
        },
        'cm_foreign_servers': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Foreign Server',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create Foreign Server', node.tree
                        .tag.create_foreign_server
                        .replace('#fdwname#', node.parent.text));
                }
            }]
        },
        'cm_foreign_server': {
            elements: [{
                text: 'Alter Foreign Server',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Alter Foreign Server', node.tree
                        .tag.alter_foreign_server
                        .replace('#srvname#', node.text));
                }
            }, {
                text: 'Import Foreign Schema',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Import Foreign Schema', node.tree
                        .tag.import_foreign_schema
                        .replace('#srvname#', node.text));
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Foreign Server',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Foreign Server', node.tree
                        .tag.drop_foreign_server
                        .replace('#srvname#', node.text));
                }
            }]
        },
        'cm_user_mappings': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create User Mapping',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create User Mapping', node.tree
                        .tag.create_user_mapping
                        .replace('#srvname#', node.parent.text));
                }
            }]
        },
        'cm_user_mapping': {
            elements: [{
                text: 'Alter User Mapping',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Alter User Mapping', node.tree
                        .tag.alter_user_mapping
                        .replace('#user_name#', node.text)
                        .replace('#srvname#', node.parent.parent.text));
                }
            }, {
                text: 'Drop User Mapping',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop User Mapping', node.tree
                        .tag.drop_user_mapping
                        .replace('#user_name#', node.text)
                        .replace('#srvname#', node.parent.parent.text));
                }
            }]
        },
        
        'cm_types': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Type',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create Type', node.tree
                        .tag.create_type.replace(
                            '#schema_name#', node.tag.schema
                        ));
                }
            }, {
                text: 'Doc: Types',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Types',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/sql-createtype.html');
                }
            }]
        },
        'cm_type': {
            elements: [{
                text: 'Alter Type',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Alter Type', node.tree.tag
                        .alter_type.replace(
                            '#type_name#', node.tag.schema + '.' + node.text));
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Type',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Type', node.tree.tag
                        .drop_type.replace(
                            '#type_name#', node.tag.schema + '.' + node.text));
                }
            }]
        },
        'cm_domains': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }, {
                text: 'Create Domain',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Create Domain', node.tree
                        .tag.create_domain.replace(
                            '#schema_name#', node.tag.schema
                        ));
                }
            }, {
                text: 'Doc: Domains',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: Domains',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/sql-createdomain.html');
                }
            }]
        },
        'cm_domain': {
            elements: [{
                text: 'Alter Domain',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    tabSQLTemplate('Alter Domain', node.tree.tag
                        .alter_domain.replace(
                            '#domain_name#', node.tag.schema + '.' + node.text));
                }
            }, {
                text: 'Edit Comment',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    getObjectDescriptionPostgresql(node);
                }
            }, {
                text: 'Drop Domain',
                icon: 'fas cm-all fa-times',
                action: function(node) {
                    tabSQLTemplate('Drop Domain', node.tree.tag
                        .drop_domain.replace(
                            '#domain_name#', node.tag.schema + '.' + node.text));
                }
            }]
        },
        'cm_refresh': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            }]
        },
        'cm_jobs': {
            elements: [{
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            },
            {
                text: 'New Job',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    createPgCronModal(node, 'Create')
                }
            },
            ]
        },
        'cm_job': {
            elements: [{
                text: 'View/Edit',
                icon: 'fas cm-all fa-edit',
                action: function(node) {
                    createPgCronModal(node, 'Edit')
                }
            },
            {
                text: 'Delete',
                icon: 'fas cm-all fa-xmark',
                action: function(node) {
                    createMessageModal(
                        `Are you sure you want to delete job "${node.text}"`,
                        function() { deleteJobPostgresql(node) },
                        null,
                      )

                }
            },
            ]
        },
    };

    // var tree = createTree(p_div, '#fcfdfd', context_menu);
    // v_connTabControl.selectedTab.tag.tree = tree;
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

    // tree.nodeAfterOpenEvent = function(node) {
    //     refreshTreePostgresql(node);
    //     // Adjusting scroll position of tree
    //     try {
    //       let v_first_child_toggle = node.elementUl.childNodes[0].childNodes[0].childNodes[0].childNodes[0];
    //       let pos_x = v_first_child_toggle.offsetLeft - 24;
    //       let pos_y = v_first_child_toggle.offsetTop - 64;
    //       v_connTabControl.selectedTab.tag.divTree.scrollTo({left: pos_x,
    //                                                          top: pos_y,
    //                                                          behavior: 'smooth'});
    //     }
    //     catch(e) {

    //     }
    // }

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

    // var node_server = tree.createNode('PostgreSQL', false,
    //     'node-postgresql', null, {
    //         type: 'server'
    //     }, 'cm_server');
    // node_server.createChildNode('', true, 'node-spin',
    //     null, null);
    // tree.drawTree();
    // setTimeout(function() {
    //     if (v_connTabControl.selectedTab.tag.selectedDBMS=='postgresql')
    //         tree.expandNode(node_server)
    // }, 200)

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

/// <summary>
/// Refreshing tree node confirm.
/// </summary>
/// <param name="node">Node object.</param>
function refreshTreePostgresqlConfirm(node) {
  if (node.tag != undefined)
      if (node.tag.type == 'procedure_list') {
        getProceduresPostgresql(node);
    } else if (node.tag.type == 'procedure') {
        getProcedureFieldsPostgresql(node);
    } else if (node.tag.type == 'tablespace_list') {
        getTablespacesPostgresql(node);
    } else if (node.tag.type == 'role_list') {
        getRolesPostgresql(node);
    } else if (node.tag.type == 'extension_list') {
        getExtensionsPostgresql(node);
    } else if (node.tag.type == 'eventtrigger_list') {
        getEventTriggersPostgresql(node);
    } else if (node.tag.type == 'eventtriggerfunction_list') {
        getEventTriggerFunctionsPostgresql(node);
    } else if (node.tag.type == 'server') {
        getTreeDetailsPostgresql(node);
    } else if (node.tag.type == 'physicalreplicationslot_list') {
        getPhysicalReplicationSlotsPostgresql(node);
    } else if (node.tag.type == 'logicalreplicationslot_list') {
        getLogicalReplicationSlotsPostgresql(node);
    } else if (node.tag.type == 'publication_list') {
        getPublicationsPostgresql(node);
    } else if (node.tag.type == 'subscription_list') {
        getSubscriptionsPostgresql(node);
    } else if (node.tag.type == 'publication_table_list') {
        getPublicationTablesPostgresql(node);
    } else if (node.tag.type == 'subscription_table_list') {
        getSubscriptionTablesPostgresql(node);
    } else if (node.tag.type == 'fdw_list') {
        getForeignDataWrappersPostgresql(node);
    } else if (node.tag.type == 'foreign_server_list') {
        getForeignServersPostgresql(node);
    } else if (node.tag.type == 'user_mapping_list') {
        getUserMappingsPostgresql(node);
    } else if (node.tag.type == 'type_list') {
        getTypesPostgresql(node);
    } else if (node.tag.type == 'domain_list') {
        getDomainsPostgresql(node);
    } else if (node.tag.type == 'aggregate_list') {
        getAggregatesPostgresql(node);
    } else if (node.tag.type == 'aggregate') {
        getFunctionFieldsPostgresql(node);
    } else if (node.tag.type == 'job_list') {
        getPgCronJobsPostgresql(node);
    } else {
      afterNodeOpenedCallbackPostgreSQL(node);
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
/// Retrieving tree details.
/// </summary>
/// <param name="node">Node object.</param>
function getTreeDetailsPostgresql(node) {
    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_tree_info_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id
        }),
        function(p_return) {
            node.tree.contextMenu.cm_server.elements = []
            node.tree.contextMenu.cm_server.elements.push({
                text: 'Refresh',
                icon: 'fas cm-all fa-sync-alt',
                action: function(node) {
                    if (node.childNodes == 0)
                        refreshTreePostgresql(node);
                    else {
                        node.collapseNode();
                        node.expandNode();
                    }
                }
            });
            node.tree.contextMenu.cm_server.elements.push(
                {
                    text: 'Server Configuration',
                    icon: 'fas cm-all fa-cog',
                    action: function(node) {
                        createConfTab()
                    }
                }
            );
            node.tree.contextMenu.cm_server.elements.push(
                {
                    text: 'Backup Server',
                    icon: 'fa-solid fa-download cm-all',
                    action: function(node) {
                        createUtilityTab(node, 'Backup', 'server')
                    }
                },
            );
            node.tree.contextMenu.cm_server.elements.push(
                {
                    text: 'Restore Server',
                    icon: 'fa-solid fa-upload cm-all',
                    action: function(node) {
                        createUtilityTab(node, 'Restore', 'server')
                    }
                }
            );
            node.tree.contextMenu.cm_server.elements.push({
                text: 'Monitoring',
                icon: 'fas cm-all fa-chart-line',
                action: function(node) {},
                submenu: {
                    elements: [{
                        text: 'Dashboard',
                        icon: 'fas cm-all fa-chart-line',
                        action: function(node) {
                            v_connTabControl.tag.createMonitorDashboardTab();
                            startMonitorDashboard();
                        }
                    }, {
                        text: 'Backends',
                        icon: 'fas cm-all fa-tasks',
                        action: function(node) {
                            v_connTabControl.tag.createMonitoringTab(
                                'Backends',
                                'SELECT * FROM pg_stat_activity', [{
                                    icon: 'fas cm-all fa-times',
                                    title: 'Terminate',
                                    action: 'postgresqlTerminateBackend'
                                }]);
                        }
                    }]
                }
            });
            node.tree.contextMenu.cm_server.elements.push({
                text: 'Doc: PostgreSQL',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: PostgreSQL',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/');
                }
            });
            node.tree.contextMenu.cm_server.elements.push({
                text: 'Doc: SQL Language',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: SQL Language',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/sql.html');

                }
            });
            node.tree.contextMenu.cm_server.elements.push({
                text: 'Doc: SQL Commands',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.openWebSite(
                        'Documentation: SQL Commands',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionPostgresql(node.tree.tag.version) +
                        '/static/sql-commands.html');
                }
            });

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.tree.tag = {
                version: p_return.v_data.v_database_return.version,
                //superuser: p_return.v_data.v_database_return.superuser,
                create_role: p_return.v_data.v_database_return.create_role,
                alter_role: p_return.v_data.v_database_return.alter_role,
                drop_role: p_return.v_data.v_database_return.drop_role,
                create_tablespace: p_return.v_data.v_database_return.create_tablespace,
                alter_tablespace: p_return.v_data.v_database_return.alter_tablespace,
                drop_tablespace: p_return.v_data.v_database_return.drop_tablespace,
                create_database: p_return.v_data.v_database_return.create_database,
                alter_database: p_return.v_data.v_database_return.alter_database,
                drop_database: p_return.v_data.v_database_return.drop_database,
                create_extension: p_return.v_data.v_database_return.create_extension,
                alter_extension: p_return.v_data.v_database_return.alter_extension,
                drop_extension: p_return.v_data.v_database_return.drop_extension,
                create_schema: p_return.v_data.v_database_return.create_schema,
                alter_schema: p_return.v_data.v_database_return.alter_schema,
                drop_schema: p_return.v_data.v_database_return.drop_schema,
                create_sequence: p_return.v_data.v_database_return.create_sequence,
                alter_sequence: p_return.v_data.v_database_return.alter_sequence,
                drop_sequence: p_return.v_data.v_database_return.drop_sequence,
                create_function: p_return.v_data.v_database_return.create_function,
                alter_function: p_return.v_data.v_database_return.alter_function,
                drop_function: p_return.v_data.v_database_return.drop_function,
                create_procedure: p_return.v_data.v_database_return.create_procedure,
                alter_procedure: p_return.v_data.v_database_return.alter_procedure,
                drop_procedure: p_return.v_data.v_database_return.drop_procedure,
                create_triggerfunction: p_return.v_data.v_database_return
                    .create_triggerfunction,
                alter_triggerfunction: p_return.v_data.v_database_return.alter_triggerfunction,
                drop_triggerfunction: p_return.v_data.v_database_return
                    .drop_triggerfunction,
                create_eventtriggerfunction: p_return.v_data.v_database_return
                    .create_eventtriggerfunction,
                alter_eventtriggerfunction: p_return.v_data.v_database_return.drop_eventtriggerfunction,
                drop_eventtriggerfunction: p_return.v_data.v_database_return
                    .drop_eventtriggerfunction,
                create_aggregate: p_return.v_data.v_database_return.create_aggregate,
                alter_aggregate: p_return.v_data.v_database_return.alter_aggregate,
                drop_aggregate: p_return.v_data.v_database_return.drop_aggregate,
                create_view: p_return.v_data.v_database_return.create_view,
                alter_view: p_return.v_data.v_database_return.alter_view,
                drop_view: p_return.v_data.v_database_return.drop_view,
                create_mview: p_return.v_data.v_database_return.create_mview,
                refresh_mview: p_return.v_data.v_database_return.refresh_mview,
                alter_mview: p_return.v_data.v_database_return.alter_mview,
                drop_mview: p_return.v_data.v_database_return.drop_mview,
                create_table: p_return.v_data.v_database_return.create_table,
                alter_table: p_return.v_data.v_database_return.alter_table,
                drop_table: p_return.v_data.v_database_return.drop_table,
                create_column: p_return.v_data.v_database_return.create_column,
                alter_column: p_return.v_data.v_database_return.alter_column,
                drop_column: p_return.v_data.v_database_return.drop_column,
                create_primarykey: p_return.v_data.v_database_return.create_primarykey,
                drop_primarykey: p_return.v_data.v_database_return.drop_primarykey,
                create_unique: p_return.v_data.v_database_return.create_unique,
                drop_unique: p_return.v_data.v_database_return.drop_unique,
                create_foreignkey: p_return.v_data.v_database_return.create_foreignkey,
                drop_foreignkey: p_return.v_data.v_database_return.drop_foreignkey,
                create_index: p_return.v_data.v_database_return.create_index,
                alter_index: p_return.v_data.v_database_return.alter_index,
                reindex: p_return.v_data.v_database_return.reindex,
                drop_index: p_return.v_data.v_database_return.drop_index,
                create_check: p_return.v_data.v_database_return.create_check,
                drop_check: p_return.v_data.v_database_return.drop_check,
                create_exclude: p_return.v_data.v_database_return.create_exclude,
                drop_exclude: p_return.v_data.v_database_return.drop_exclude,
                create_rule: p_return.v_data.v_database_return.create_rule,
                alter_rule: p_return.v_data.v_database_return.alter_rule,
                drop_rule: p_return.v_data.v_database_return.drop_rule,
                create_trigger: p_return.v_data.v_database_return.create_trigger,
                create_view_trigger: p_return.v_data.v_database_return.create_view_trigger,
                alter_trigger: p_return.v_data.v_database_return.alter_trigger,
                enable_trigger: p_return.v_data.v_database_return.enable_trigger,
                disable_trigger: p_return.v_data.v_database_return.disable_trigger,
                drop_trigger: p_return.v_data.v_database_return.drop_trigger,
                create_eventtrigger: p_return.v_data.v_database_return.create_eventtrigger,
                alter_eventtrigger: p_return.v_data.v_database_return.alter_eventtrigger,
                enable_eventtrigger: p_return.v_data.v_database_return.enable_eventtrigger,
                disable_eventtrigger: p_return.v_data.v_database_return.disable_eventtrigger,
                drop_eventtrigger: p_return.v_data.v_database_return.drop_eventtrigger,
                create_inherited: p_return.v_data.v_database_return.create_inherited,
                noinherit_partition: p_return.v_data.v_database_return.noinherit_partition,
                create_partition: p_return.v_data.v_database_return.create_partition,
                detach_partition: p_return.v_data.v_database_return.detach_partition,
                drop_partition: p_return.v_data.v_database_return.drop_partition,
                vacuum: p_return.v_data.v_database_return.vacuum,
                vacuum_table: p_return.v_data.v_database_return.vacuum_table,
                analyze: p_return.v_data.v_database_return.analyze,
                analyze_table: p_return.v_data.v_database_return.analyze_table,
                delete: p_return.v_data.v_database_return.delete,
                truncate: p_return.v_data.v_database_return.truncate,
                create_physicalreplicationslot: p_return.v_data.v_database_return
                    .create_physicalreplicationslot,
                drop_physicalreplicationslot: p_return.v_data.v_database_return
                    .drop_physicalreplicationslot,
                create_logicalreplicationslot: p_return.v_data.v_database_return
                    .create_logicalreplicationslot,
                drop_logicalreplicationslot: p_return.v_data.v_database_return
                    .drop_logicalreplicationslot,
                create_publication: p_return.v_data.v_database_return.create_publication,
                alter_publication: p_return.v_data.v_database_return.alter_publication,
                drop_publication: p_return.v_data.v_database_return.drop_publication,
                add_pubtable: p_return.v_data.v_database_return.add_pubtable,
                drop_pubtable: p_return.v_data.v_database_return.drop_pubtable,
                create_subscription: p_return.v_data.v_database_return.create_subscription,
                alter_subscription: p_return.v_data.v_database_return.alter_subscription,
                drop_subscription: p_return.v_data.v_database_return.drop_subscription,
                create_fdw: p_return.v_data.v_database_return.create_fdw,
                alter_fdw: p_return.v_data.v_database_return.alter_fdw,
                drop_fdw: p_return.v_data.v_database_return.drop_fdw,
                create_foreign_server: p_return.v_data.v_database_return.create_foreign_server,
                alter_foreign_server: p_return.v_data.v_database_return.alter_foreign_server,
                import_foreign_schema: p_return.v_data.v_database_return.import_foreign_schema,
                drop_foreign_server: p_return.v_data.v_database_return.drop_foreign_server,
                create_foreign_table: p_return.v_data.v_database_return.create_foreign_table,
                alter_foreign_table: p_return.v_data.v_database_return.alter_foreign_table,
                drop_foreign_table: p_return.v_data.v_database_return.drop_foreign_table,
                create_foreign_column: p_return.v_data.v_database_return.create_foreign_column,
                alter_foreign_column: p_return.v_data.v_database_return.alter_foreign_column,
                drop_foreign_column: p_return.v_data.v_database_return.drop_foreign_column,
                create_user_mapping: p_return.v_data.v_database_return.create_user_mapping,
                alter_user_mapping: p_return.v_data.v_database_return.alter_user_mapping,
                drop_user_mapping: p_return.v_data.v_database_return.drop_user_mapping,
                create_type: p_return.v_data.v_database_return.create_type,
                alter_type: p_return.v_data.v_database_return.alter_type,
                drop_type: p_return.v_data.v_database_return.drop_type,
                create_domain: p_return.v_data.v_database_return.create_domain,
                alter_domain: p_return.v_data.v_database_return.alter_domain,
                drop_domain: p_return.v_data.v_database_return.drop_domain,
                create_statistics: p_return.v_data.v_database_return.create_statistics,
                alter_statistics: p_return.v_data.v_database_return.alter_statistics,
                drop_statistics: p_return.v_data.v_database_return.drop_statistics,
            }

            node.setText(p_return.v_data.v_database_return.version);

            var node_databases = node.createChildNode('Databases', false,
                'fas node-all fa-database node-database-list', {
                    type: 'database_list',
                    num_databases: 0
                }, 'cm_databases');
            node_databases.createChildNode('', true,
                'node-spin', null, null);
            var node_tablespaces = node.createChildNode('Tablespaces',
                false, 'fas node-all fa-folder-open node-tablespace-list', {
                    type: 'tablespace_list',
                    num_tablespaces: 0
                }, 'cm_tablespaces');
            node_tablespaces.createChildNode('', true,
                'node-spin', null, null);
            var node_roles = node.createChildNode('Roles', false,
                'fas node-all fa-users node-user-list', {
                    type: 'role_list',
                    num_roles: 0
                }, 'cm_roles');
            node_roles.createChildNode('', true,
                'node-spin', null, null);
            if (parseFloat(getMajorVersionPostgresql(node.tree.tag.version)) >= 9.4) {
                var node_replication = node.createChildNode(
                    'Replication Slots', false,
                    'fas node-all fa-sitemap node-repslot-list', {
                        type: 'replication',
                    }, null);
                var node_phyrepslots = node_replication.createChildNode(
                    'Physical Replication Slots', false,
                    'fas node-all fa-sitemap node-repslot-list', {
                        type: 'physicalreplicationslot_list',
                        num_repslots: 0
                    }, 'cm_physicalreplicationslots');
                node_phyrepslots.createChildNode('', true,
                    'node-spin', null, null);
                var node_logrepslots = node_replication.createChildNode(
                    'Logical Replication Slots', false,
                    'fas node-all fa-sitemap node-repslot-list', {
                        type: 'logicalreplicationslot_list',
                        num_repslots: 0
                    }, 'cm_logicalreplicationslots');
                node_logrepslots.createChildNode('', true,
                    'node-spin', null, null);
            }

            if (v_connTabControl.selectedTab.tag.firstTimeOpen) {
                v_connTabControl.selectedTab.tag.firstTimeOpen = false;
                //v_connTabControl.tag.createMonitorDashboardTab();
                //startMonitorDashboard();
            }

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);

}

/// <summary>
/// Retrieving tablespaces.
/// </summary>
/// <param name="node">Node object.</param>
function getTablespacesPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);


    execAjax('/get_tablespaces_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Tablespaces (' + p_return.v_data.length + ')');

            node.tag.num_tablespaces = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

                v_node = node.createChildNode(p_return.v_data[i].v_name,
                    false, 'fas node-all fa-folder node-tablespace', {
                        type: 'tablespace',
                        oid: p_return.v_data[i].v_oid
                    }, 'cm_tablespace', null, false);

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving roles.
/// </summary>
/// <param name="node">Node object.</param>
function getRolesPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    axios.post('/get_roles_postgresql/', {
        database_index: v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
        tab_id: v_connTabControl.selectedTab.id
    })
    .then((resp) => {
        if (node.childNodes.length > 0)
            node.removeChildNodes();
        node.setText(`Roles (${resp.data.data.length})`);

        node.tag.num_tablespaces = resp.data.length

        for (i = 0; i < resp.data.data.length; i++) {

            node.createChildNode(resp.data.data[i].name,
                false, 'fas node-all fa-user node-user', {
                    type: 'role',
                    oid: resp.data.data[i].oid
                }, 'cm_role', null, false);
        }

        node.drawChildNodes();

        afterNodeOpenedCallbackPostgreSQL(node);
    })
    .catch((error) => {
        nodeOpenErrorPostgresqlNew(error, node)
    })
}

/// <summary>
/// Retrieving roles.
/// </summary>
/// <param name="node">Node object.</param>
function getExtensionsPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_extensions_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Extensions (' + p_return.v_data.length + ')');

            node.tag.num_tablespaces = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

                v_node = node.createChildNode(p_return.v_data[i].v_name,
                    false, 'fas node-all fa-cubes node-extension', {
                        type: 'extension',
                        database: v_connTabControl.selectedTab.tag.selectedDatabase,
                        oid: p_return.v_data[i].v_oid
                    }, 'cm_extension', null, false);

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving Event Triggers.
/// </summary>
/// <param name="node">Node object.</param>
function getEventTriggersPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_eventtriggers_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
        }),
        function(p_return) {

            node.setText('Event Triggers (' + p_return.v_data.length + ')');

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            if (p_return.v_data.length > 0) {

                for (i = 0; i < p_return.v_data.length; i++) {

                    var v_node = node.createChildNode(p_return.v_data[i].v_name,
                        false, 'fas node-all fa-bolt node-eventtrigger', {
                            type: 'eventtrigger',
                            database: v_connTabControl.selectedTab.tag.selectedDatabase,
                            oid: p_return.v_data[i].v_oid
                        }, 'cm_eventtrigger', null, true);
                    v_node.createChildNode('Enabled: ' + p_return.v_data[i]
                        .v_enabled, false,
                        'fas node-all fa-ellipsis-h node-bullet', {
                            database: v_connTabControl.selectedTab.tag.selectedDatabase
                        }, null, null, false);
                    v_node.createChildNode('Event: ' + p_return.v_data[i]
                        .v_event, false,
                        'fas node-all fa-ellipsis-h node-bullet', {
                            database: v_connTabControl.selectedTab.tag.selectedDatabase
                        }, null, null, false);

                    v_node.createChildNode(p_return.v_data[i].v_function, false,
                        'fas node-all fa-cog node-etfunction', {
                            type: 'direct_eventtriggerfunction',
                            id: p_return.v_data[i].v_id,
                            database: v_connTabControl.selectedTab.tag.selectedDatabase,
                            function_oid: p_return.v_data[i].v_function_oid
                        }, 'cm_direct_eventtriggerfunction', null, true);

                }

                node.drawChildNodes();

            }

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
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
/// Retrieving function definition.
/// </summary>
/// <param name="node">Node object.</param>
function getFunctionDefinitionPostgresql(node) {

    execAjax('/get_function_definition_postgresql/',
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
            //v_connTabControl.selectedTab.tag.tabControl.selectedTab.renameTab(node.text);
            renameTabConfirm(v_connTabControl.selectedTab.tag.tabControl.selectedTab,
                node.text);

            var v_div_result = v_connTabControl.selectedTab.tag.tabControl.selectedTab
                .tag.div_result;

            if (v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag
                .ht != null) {
                v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag
                    .ht.destroy();
                v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag
                    .ht = null;
            }

            v_div_result.innerHTML = '';

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        true);

}

/// <summary>
/// Retrieving procedures.
/// </summary>
/// <param name="node">Node object.</param>
function getProceduresPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_procedures_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_schema": node.tag.schema
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Procedures (' + p_return.v_data.length + ')');

            node.tag.num_procedures = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

                v_node = node.createChildNode(p_return.v_data[i].v_name,
                    false, 'fas node-all fa-cog node-procedure', {
                        type: 'procedure',
                        id: p_return.v_data[i].v_id,
                        database: v_connTabControl.selectedTab.tag.selectedDatabase,
                        schema: node.tag.schema,
                        function_oid: p_return.v_data[i].v_function_oid
                    }, 'cm_procedure', null, false);
                v_node.createChildNode('', false,
                    'node-spin', {
                        type: 'procedure_field',
                        schema: node.tag.schema
                    }, null, null, false);

            }
            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving procedure fields.
/// </summary>
/// <param name="node">Node object.</param>
function getProcedureFieldsPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_procedure_fields_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_procedure": node.tag.id,
            "p_schema": node.tag.schema
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.tag.num_fields = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

                if (p_return.v_data[i].v_type == 'O')
                    v_node = node.createChildNode(p_return.v_data[i].v_name,
                        false, 'fas node-all fa-arrow-right node-function-field', {
                            database: v_connTabControl.selectedTab.tag.selectedDatabase,
                            schema: node.tag.schema
                        },
                        null, null, false);
                else {
                    if (p_return.v_data[i].v_type == 'I')
                        v_node = node.createChildNode(p_return.v_data[i].v_name,
                            false, 'fas node-all fa-arrow-left node-function-field', {
                                database: v_connTabControl.selectedTab.tag.selectedDatabase,
                                schema: node.tag.schema
                            }, null, null, false);
                    else
                        v_node = node.createChildNode(p_return.v_data[i].v_name,
                            false,
                            'fas node-all fa-exchange-alt node-function-field', {
                                database: v_connTabControl.selectedTab.tag.selectedDatabase,
                                schema: node.tag.schema
                            }, null, null, false);
                }

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
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
/// Retrieving procedure definition.
/// </summary>
/// <param name="node">Node object.</param>
function getProcedureDefinitionPostgresql(node) {

    execAjax('/get_procedure_definition_postgresql/',
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
            //v_connTabControl.selectedTab.tag.tabControl.selectedTab.renameTab(node.text);
            renameTabConfirm(v_connTabControl.selectedTab.tag.tabControl.selectedTab,
                node.text);

            var v_div_result = v_connTabControl.selectedTab.tag.tabControl.selectedTab
                .tag.div_result;

            if (v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag
                .ht != null) {
                v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag
                    .ht.destroy();
                v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag
                    .ht = null;
            }

            v_div_result.innerHTML = '';

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        true);

}

/// <summary>
/// Retrieving event trigger functions.
/// </summary>
/// <param name="node">Node object.</param>
function getEventTriggerFunctionsPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_eventtriggerfunctions_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_schema": node.tag.schema
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Event Trigger Functions (' + p_return.v_data.length +
                ')');

            node.tag.num_tables = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

                node.createChildNode(p_return.v_data[i].v_name, false,
                    'fas node-all fa-cog node-etfunction', {
                        type: 'eventtriggerfunction',
                        id: p_return.v_data[i].v_id,
                        database: v_connTabControl.selectedTab.tag.selectedDatabase,
                        schema: node.tag.schema,
                        function_oid: p_return.v_data[i].v_function_oid
                    }, 'cm_eventtriggerfunction', null, false);

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving event trigger function definition.
/// </summary>
/// <param name="node">Node object.</param>
function getEventTriggerFunctionDefinitionPostgresql(node) {

    execAjax('/get_eventtriggerfunction_definition_postgresql/',
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
            //v_connTabControl.selectedTab.tag.tabControl.selectedTab.renameTab(node.text);
            renameTabConfirm(v_connTabControl.selectedTab.tag.tabControl.selectedTab,
                node.text);

            var v_div_result = v_connTabControl.selectedTab.tag.tabControl.selectedTab
                .tag.div_result;

            if (v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag
                .ht != null) {
                v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag
                    .ht.destroy();
                v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag
                    .ht = null;
            }

            v_div_result.innerHTML = '';

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        true);

}

/// <summary>
/// Retrieving aggregates.
/// </summary>
/// <param name="node">Node object.</param>
function getAggregatesPostgresql(node) {
    node.removeChildNodes();

    node.createChildNode(
        '',
        false,
        'node-spin',
        null,
        null
    );

    execAjax(
        '/get_aggregates_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_schema": node.tag.schema
        }),
        function(p_return) {
            if (node.childNodes.length > 0) {
                node.removeChildNodes();
            }

            node.setText('Aggregates (' + p_return.v_data.length + ')');
            node.tag.num_aggregates = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {
                v_node = node.createChildNode(
                    p_return.v_data[i].v_name,
                    false,
                    'fas node-all fa-cog node-aggregate',
                    {
                        type: 'aggregate',
                        id: p_return.v_data[i].v_id,
                        database: v_connTabControl.selectedTab.tag.selectedDatabase,
                        schema: node.tag.schema,
                        oid: p_return.v_data[i].v_oid
                    },
                    'cm_aggregate',
                    null,
                    false
                );

                v_node.createChildNode(
                    '',
                    false,
                    'node-spin',
                    {
                        type: 'aggregate_field',
                        schema: node.tag.schema
                    },
                    null,
                    null,
                    false
                );
            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);
        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false
    );
}

/// <summary>
/// Retrieving Physical Replication Slots.
/// </summary>
/// <param name="node">Node object.</param>
function getPhysicalReplicationSlotsPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_physicalreplicationslots_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Physical Replication Slots (' + p_return.v_data.length +
                ')');

            node.tag.num_repslots = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

                v_node = node.createChildNode(p_return.v_data[i].v_name,
                    false, 'fas node-all fa-sitemap node-repslot', {
                        type: 'physicalreplicationslot',
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, 'cm_physicalreplicationslot', null, false);

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving Logical Replication Slots.
/// </summary>
/// <param name="node">Node object.</param>
function getLogicalReplicationSlotsPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_logicalreplicationslots_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Logical Replication Slots (' + p_return.v_data.length +
                ')');

            node.tag.num_repslots = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

                v_node = node.createChildNode(p_return.v_data[i].v_name,
                    false, 'fas node-all fa-sitemap node-repslot', {
                        type: 'logicalreplicationslot',
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, 'cm_logicalreplicationslot', null, false);

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving Publications.
/// </summary>
/// <param name="node">Node object.</param>
function getPublicationsPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_publications_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Publications (' + p_return.v_data.length + ')');

            node.tag.num_pubs = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

                v_node = node.createChildNode(p_return.v_data[i].v_name,
                    false, 'fas node-all fa-arrow-alt-circle-down node-publication', {
                        type: 'publication',
                        database: v_connTabControl.selectedTab.tag.selectedDatabase,
                        oid: p_return.v_data[i].v_oid
                    }, 'cm_publication', null, false);
                v_node.createChildNode('All Tables: ' + p_return.v_data[i].v_alltables,
                    false, 'fas node-all fa-ellipsis-h node-bullet', {
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, null, null, false);
                v_node.createChildNode('Insert: ' + p_return.v_data[i].v_insert,
                    false, 'fas node-all fa-ellipsis-h node-bullet', {
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, null, null, false);
                v_node.createChildNode('Update: ' + p_return.v_data[i].v_update,
                    false, 'fas node-all fa-ellipsis-h node-bullet', {
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, null, null, false);
                v_node.createChildNode('Delete: ' + p_return.v_data[i].v_delete,
                    false, 'fas node-all fa-ellipsis-h node-bullet', {
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, null, null, false);
                v_node.createChildNode('Truncate: ' + p_return.v_data[i].v_truncate,
                    false, 'fas node-all fa-ellipsis-h node-bullet', {
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, null, null, false);
                if (p_return.v_data[i].v_alltables == 'False') {
                    v_tables = v_node.createChildNode('Tables',
                        false,
                        'fas node-all fa-th node-table-list', {
                            type: 'publication_table_list',
                            database: v_connTabControl.selectedTab.tag.selectedDatabase
                        }, 'cm_pubtables', null, false);
                    v_tables.createChildNode('', true,
                        'node-spin', null,
                        null, null, false);
                }

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving Publication Tables.
/// </summary>
/// <param name="node">Node object.</param>
function getPublicationTablesPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_publication_tables_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_pub": node.parent.text
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Tables (' + p_return.v_data.length + ')');

            node.tag.num_tables = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

                v_node = node.createChildNode(p_return.v_data[i].v_name,
                    false, 'fas node-all fa-table node-table', {
                        type: 'pubtable',
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, 'cm_pubtable', null, false);

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving Subscriptions.
/// </summary>
/// <param name="node">Node object.</param>
function getSubscriptionsPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_subscriptions_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Subscriptions (' + p_return.v_data.length + ')');

            node.tag.num_subs = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

                v_node = node.createChildNode(p_return.v_data[i].v_name,
                    false, 'fas node-all fa-arrow-alt-circle-up node-subscription', {
                        type: 'subscription',
                        database: v_connTabControl.selectedTab.tag.selectedDatabase,
                        oid: p_return.v_data[i].v_oid
                    }, 'cm_subscription', null, false);
                v_node.createChildNode('Enabled: ' + p_return.v_data[i].v_enabled,
                    false, 'fas node-all fa-ellipsis-h node-bullet', {
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, null, null, false);
                v_node.createChildNode('ConnInfo: ' + p_return.v_data[i].v_conninfo,
                    false, 'fas node-all fa-ellipsis-h node-bullet', {
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, null, null, false);
                v_publications = v_node.createChildNode(
                    'Referenced Publications',
                    false, 'fas node-all fa-arrow-alt-circle-down node-publication', {
                        type: 'subpubs',
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, null, null, false);
                tmp = p_return.v_data[i].v_publications.split(',')
                for (j = 0; j < tmp.length; j++) {
                    v_publications.createChildNode(tmp[j],
                        false,
                        'fas node-all fa-arrow-alt-circle-down node-publication', {
                            type: 'subpub',
                            database: v_connTabControl.selectedTab.tag.selectedDatabase
                        }, null, null, false);
                }
                v_tables = v_node.createChildNode('Tables',
                    false,
                    'fas node-all fa-th node-table-list', {
                        type: 'subscription_table_list',
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, null, null, false);
                v_tables.createChildNode('', true,
                    'node-spin', null, null,
                    null, false);
            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving Subscription Tables.
/// </summary>
/// <param name="node">Node object.</param>
function getSubscriptionTablesPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_subscription_tables_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_sub": node.parent.text
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Tables (' + p_return.v_data.length + ')');

            node.tag.num_tables = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

                v_node = node.createChildNode(p_return.v_data[i].v_name,
                    false, 'fas node-all fa-table node-table', {
                        type: 'subtable',
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, null, null, false);

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving Foreign Data Wrappers.
/// </summary>
/// <param name="node">Node object.</param>
function getForeignDataWrappersPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_foreign_data_wrappers_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Foreign Data Wrappers (' + p_return.v_data.length + ')');

            node.tag.num_fdws = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

              v_node = node.createChildNode(p_return.v_data[i].v_name,
                  false, 'fas node-all fa-cube node-fdw', {
                      type: 'fdw',
                      database: v_connTabControl.selectedTab.tag.selectedDatabase,
                      oid: p_return.v_data[i].v_oid
                  }, 'cm_fdw', null, false);
              v_node = v_node.createChildNode('Foreign Servers',
                  false, 'fas node-all fa-server node-server', {
                      type: 'foreign_server_list',
                      database: v_connTabControl.selectedTab.tag.selectedDatabase
                  }, 'cm_foreign_servers', null, false);
              v_node.createChildNode('', true,
                  'node-spin', null, null, null, false);

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving Foreign Servers.
/// </summary>
/// <param name="node">Node object.</param>
function getForeignServersPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_foreign_servers_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_fdw": node.parent.text
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Foreign Servers (' + p_return.v_data.length + ')');

            node.tag.num_foreign_servers = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

              v_node = node.createChildNode(p_return.v_data[i].v_name,
                  false, 'fas node-all fa-server node-server', {
                      type: 'foreign_server',
                      database: v_connTabControl.selectedTab.tag.selectedDatabase,
                      oid: p_return.v_data[i].v_oid
                  }, 'cm_foreign_server', null, false);

              if (p_return.v_data[i].v_type != null) {
                  v_node.createChildNode('Type: ' + p_return.v_data[i].v_type,
                      true, 'fas node-all fa-ellipsis-h node-bullet', {
                          database: v_connTabControl.selectedTab.tag.selectedDatabase
                      }, null, null, false);
              }

              if (p_return.v_data[i].v_version != null) {
                  v_node.createChildNode('Version: ' + p_return.v_data[i].v_version,
                      true, 'fas node-all fa-ellipsis-h node-bullet', {
                          database: v_connTabControl.selectedTab.tag.selectedDatabase
                      }, null, null, false);
              }

              if (p_return.v_data[i].v_options != null) {
                v_options = p_return.v_data[i].v_options.split(',');
                if (v_options[0] != '') {
                  for (j = 0; j < v_options.length; j++) {
                      v_node.createChildNode(v_options[j],
                          true, 'fas node-all fa-ellipsis-h node-bullet', {
                              database: v_connTabControl.selectedTab.tag.selectedDatabase
                          }, null, null, false);
                  }
                }
              }

              v_node = v_node.createChildNode('User Mappings',
                  false, 'fas node-all fa-user-friends node-user', {
                      type: 'user_mapping_list',
                      database: v_connTabControl.selectedTab.tag.selectedDatabase
                  }, 'cm_user_mappings', null, false);
              v_node.createChildNode('', true,
                  'node-spin', null, null, null, false);

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving User Mappings.
/// </summary>
/// <param name="node">Node object.</param>
function getUserMappingsPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_user_mappings_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_foreign_server": node.parent.text
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('User Mappings (' + p_return.v_data.length + ')');

            node.tag.num_user_mappings = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

              v_node = node.createChildNode(p_return.v_data[i].v_name,
                  false, 'fas node-all fa-user-friends node-user', {
                      type: 'user_mapping',
                      database: v_connTabControl.selectedTab.tag.selectedDatabase,
                      foreign_server: p_return.v_data[i].v_foreign_server
                  }, 'cm_user_mapping', null, false);

              if (p_return.v_data[i].v_options != null) {
                v_options = p_return.v_data[i].v_options.split(',');
                if (v_options[0] != '') {
                  for (j = 0; j < v_options.length; j++) {
                      v_node.createChildNode(v_options[j],
                          true, 'fas node-all fa-ellipsis-h node-bullet', {
                              database: v_connTabControl.selectedTab.tag.selectedDatabase
                          }, null, null, false);
                  }
                }
              }

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving types.
/// </summary>
/// <param name="node">Node object.</param>
function getTypesPostgresql(node) {
    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_types_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_schema": node.tag.schema
        }),
        function(p_return) {

            node.setText('Types (' + p_return.v_data.length + ')');

            node.tag.num_types = p_return.v_data.length;

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            for (i = 0; i < p_return.v_data.length; i++) {

                v_node = node.createChildNode(p_return.v_data[i].v_type_name,
                    false,
                    'fas node-all fa-square node-type', {
                        type: 'type',
                        database: v_connTabControl.selectedTab.tag.selectedDatabase,
                        schema: node.tag.schema,
                        oid: p_return.v_data[i].v_oid
                    }, 'cm_type', null, false);

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving domains.
/// </summary>
/// <param name="node">Node object.</param>
function getDomainsPostgresql(node) {
    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_domains_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_schema": node.tag.schema
        }),
        function(p_return) {

            node.setText('Domains (' + p_return.v_data.length + ')');

            node.tag.num_domains = p_return.v_data.length;

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            for (i = 0; i < p_return.v_data.length; i++) {

                v_node = node.createChildNode(p_return.v_data[i].v_domain_name,
                    false,
                    'fas node-all fa-square node-domain', {
                        type: 'domain',
                        database: v_connTabControl.selectedTab.tag.selectedDatabase,
                        schema: node.tag.schema,
                        oid: p_return.v_data[i].v_oid
                    }, 'cm_domain', null, false);

            }

            node.drawChildNodes();

            afterNodeOpenedCallbackPostgreSQL(node);

        },
        function(p_return) {
            nodeOpenErrorPostgresql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving pg_cron jobs.
/// </summary>
/// <param name="node">Node object.</param>
function getPgCronJobsPostgresql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);
    axios.post('/get_pgcron_jobs/', {
        database_index: v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
        tab_id: v_connTabControl.selectedTab.id
    }).then((resp) => {
        if (node.childNodes.length > 0)
        node.removeChildNodes();

        let jobs = resp.data.data.jobs;
        node.tag.num_jobs = jobs.length;

        jobs.forEach(function(job) {
            node.createChildNode(job.name,
                false,
                'fas node-all fa-clock', {
                    database: v_connTabControl.selectedTab.tag.selectedDatabase,
                    job_meta: job
                }, 'cm_job', null, false);
        })

        node.drawChildNodes();

        afterNodeOpenedCallbackPostgreSQL(node);
    }).catch((error) => {
        nodeOpenErrorPostgresqlNew(error, node)
    })
}

/// <summary>
/// Unschedule pg_cron job.
/// </summary>
/// <param name="node">Node object.</param>
function deleteJobPostgresql(node) {

    axios.post('/delete_pgcron_job/', {
        database_index: v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
        tab_id: v_connTabControl.selectedTab.id,
        job_meta: node.tag.job_meta
    })
    .then((resp) => {
        node.removeNode()

        afterNodeOpenedCallbackPostgreSQL(node);
    })
    .catch((error) => {
        nodeOpenErrorPostgresqlNew(error, node)
    })
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

function postgresqlTerminateBackendConfirm(p_pid) {
    execAjax('/kill_backend_postgresql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_pid": p_pid
        }),
        function(p_return) {

            refreshMonitoring();

        },
        function(p_return) {
            if (p_return.v_data.password_timeout) {
                showPasswordPrompt(
                    v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
                    function() {
                        postgresqlTerminateBackendConfirm(p_pid);
                    },
                    null,
                    p_return.v_data.message
                );
            } else {
                showError(p_return.v_data);
            }
        },
        'box',
        true);

}

function postgresqlTerminateBackend(p_row) {

    var v_pid = p_row[2];

    showConfirm('Are you sure you want to terminate backend ' + v_pid + '?',
        function() {

            postgresqlTerminateBackendConfirm(v_pid);

        });

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
