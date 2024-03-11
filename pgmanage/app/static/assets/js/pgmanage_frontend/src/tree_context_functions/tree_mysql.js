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
import TreeMysql from "../components/TreeMysql.vue";
import { tabSQLTemplate } from "./tree_postgresql";
import { execAjax } from "../ajax_control";
import { showToast } from "../notification_control";
import { emitter } from "../emitter";
import { tabsStore } from "../stores/stores_initializer";


/// <summary>
/// Retrieving tree details.
/// </summary>
/// <param name="node">Node object.</param>
function getTreeDetailsMysql(node) {
  node.removeChildNodes();
  node.createChildNode("", false, "node-spin", null, null);

  execAjax(
    "/get_tree_info_mysql/",
    JSON.stringify({
      p_database_index: v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
      p_tab_id: v_connTabControl.selectedTab.id,
    }),
    function (p_return) {
      node.tree.contextMenu.cm_server.elements = [];
      node.tree.contextMenu.cm_server.elements.push({
        text: "Refresh",
        icon: "fas cm-all fa-sync-alt",
        action: function (node) {
          if (node.childNodes == 0) refreshTreeMysql(node);
          else {
            node.collapseNode();
            node.expandNode();
          }
        },
      });

      /*node.tree.contextMenu.cm_server.elements.push({
                text: 'Doc: PostgreSQL',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.createWebsiteTab(
                        'Documentation: PostgreSQL',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionMysql(node.tree.tag.version) +
                        '/static/');
                }
            });
            node.tree.contextMenu.cm_server.elements.push({
                text: 'Doc: SQL Language',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.createWebsiteTab(
                        'Documentation: SQL Language',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionMysql(node.tree.tag.version) +
                        '/static/sql.html');
                }
            });
            node.tree.contextMenu.cm_server.elements.push({
                text: 'Doc: SQL Commands',
                icon: 'fas cm-all fa-globe-americas',
                action: function(node) {
                    v_connTabControl.tag.createWebsiteTab(
                        'Documentation: SQL Commands',
                        'https://www.postgresql.org/docs/' +
                        getMajorVersionMysql(node.tree.tag.version) +
                        '/static/sql-commands.html');
                }
            });*/

      if (node.childNodes.length > 0) node.removeChildNodes();

      node.tree.tag = {
        v_database: p_return.v_data.v_database_return.v_database,
        version: p_return.v_data.v_database_return.version,
        v_username: p_return.v_data.v_database_return.v_username,
        superuser: p_return.v_data.v_database_return.superuser,
        create_role: p_return.v_data.v_database_return.create_role,
        alter_role: p_return.v_data.v_database_return.alter_role,
        drop_role: p_return.v_data.v_database_return.drop_role,
        create_database: p_return.v_data.v_database_return.create_database,
        alter_database: p_return.v_data.v_database_return.alter_database,
        drop_database: p_return.v_data.v_database_return.drop_database,
        create_function: p_return.v_data.v_database_return.create_function,
        drop_function: p_return.v_data.v_database_return.drop_function,
        create_procedure: p_return.v_data.v_database_return.create_procedure,
        drop_procedure: p_return.v_data.v_database_return.drop_procedure,
        //create_triggerfunction: p_return.v_data.v_database_return
        //    .create_triggerfunction,
        //drop_triggerfunction: p_return.v_data.v_database_return
        //    .drop_triggerfunction,
        create_view: p_return.v_data.v_database_return.create_view,
        drop_view: p_return.v_data.v_database_return.drop_view,
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
        drop_index: p_return.v_data.v_database_return.drop_index,
        //create_trigger: p_return.v_data.v_database_return.create_trigger,
        //create_view_trigger: p_return.v_data.v_database_return.create_view_trigger,
        //alter_trigger: p_return.v_data.v_database_return.alter_trigger,
        //enable_trigger: p_return.v_data.v_database_return.enable_trigger,
        //disable_trigger: p_return.v_data.v_database_return.disable_trigger,
        //drop_trigger: p_return.v_data.v_database_return.drop_trigger,
        //create_partition: p_return.v_data.v_database_return.create_partition,
        //noinherit_partition: p_return.v_data.v_database_return.noinherit_partition,
        //drop_partition: p_return.v_data.v_database_return.drop_partition
        delete: p_return.v_data.v_database_return.delete,
      };

      node.tree.contextMenu.cm_server.elements.push({
        text: "Monitoring",
        icon: "fas cm-all fa-chart-line",
        action: function (node) {},
        submenu: {
          elements: [
            /*{
                        text: 'Dashboard',
                        icon: 'fas cm-all fa-chart-line',
                        action: function(node) {
                            v_connTabControl.tag.createMonitoringDashboardTab();
                        }
                    }, */ {
              text: "Process List",
              icon: "fas cm-all fa-chart-line",
              action: function (node) {
                v_connTabControl.tag.createMonitoringTab(
                  "Process List",
                  "select * from information_schema.processlist",
                  [
                    {
                      icon: "fas cm-all fa-times",
                      title: "Terminate",
                      action: "mysqlTerminateBackend",
                    },
                  ]
                );
              },
            },
          ],
        },
      });

      node.setText(p_return.v_data.v_database_return.version);

      var node_databases = node.createChildNode(
        "Databases",
        false,
        "fas node-all fa-database node-database-list",
        {
          type: "database_list",
          num_databases: 0,
        },
        "cm_databases"
      );
      node_databases.createChildNode("", true, "node-spin", null, null);

      if (node.tree.tag.superuser) {
        var node_roles = node.createChildNode(
          "Roles",
          false,
          "fas node-all fa-users node-user-list",
          {
            type: "role_list",
            num_roles: 0,
          },
          "cm_roles"
        );
        node_roles.createChildNode("", true, "node-spin", null, null);
      }

      if (v_connTabControl.selectedTab.tag.firstTimeOpen) {
        v_connTabControl.selectedTab.tag.firstTimeOpen = false;
        //v_connTabControl.tag.createMonitoringDashboardTab();
      }
    },
    function (p_return) {
      nodeOpenErrorMysql(p_return, node);
    },
    "box",
    false
  );
}

/*
/// <summary>
/// Retrieving Triggers.
/// </summary>
/// <param name="node">Node object.</param>
function getTriggersMysql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_triggers_mysql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_table": node.parent.text,
            "p_schema": null
        }),
        function(p_return) {

            node.setText('Triggers (' + p_return.v_data.length + ')');

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            var v_node;

            if (p_return.v_data.length > 0) {

                for (i = 0; i < p_return.v_data.length; i++) {

                    v_node = node.createChildNode(p_return.v_data[i][0],
                        false, '/static/OmniDB_app/images/trigger.png', {
                            type: 'trigger',
                            database: v_connTabControl.selectedTab.tag.selectedDatabase
                        }, 'cm_trigger');
                    v_node.createChildNode('Enabled: ' + p_return.v_data[i]
                        [1], false,
                        'fas node-all fa-ellipsis-h node-bullet',
                        null, null);
                    v_node.createChildNode('Function: ' + p_return.v_data[i]
                        [2], false,
                        'fas node-all fa-ellipsis-h node-bullet',
                        null, null);

                }

            }

        },
        function(p_return) {
            nodeOpenErrorMysql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving Partitions.
/// </summary>
/// <param name="node">Node object.</param>
function getPartitionsMysql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_partitions_mysql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_table": node.parent.text,
            "p_schema": null
        }),
        function(p_return) {

            node.setText('Partitions (' + p_return.v_data.length + ')');

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            var v_node;

            if (p_return.v_data.length > 0) {

                for (i = 0; i < p_return.v_data.length; i++) {

                    v_node = node.createChildNode(p_return.v_data[i][0],
                        false,
                        '/static/OmniDB_app/images/partition.png', {
                            type: 'partition',
                            database: v_connTabControl.selectedTab.tag.selectedDatabase
                        }, 'cm_partition');

                }

            }

        },
        function(p_return) {
            nodeOpenErrorMysql(p_return, node);
        },
        'box',
        false);
}
*/

/// <summary>
/// Retrieving function definition.
/// </summary>
/// <param name="node">Node object.</param>
/*function getDebugFunctionDefinitionMysql(node) {

    execAjax('/get_function_debug_mysql/',
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
            nodeOpenErrorMysql(p_return, node);
        },
        'box',
        true);

}*/

/// <summary>
/// Retrieving procedure definition.
/// </summary>
/// <param name="node">Node object.</param>
/*function getDebugProcedureDefinitionMysql(node) {

    execAjax('/get_function_debug_mysql/',
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
            nodeOpenErrorMysql(p_return, node);
        },
        'box',
        true);

}*/

/*
/// <summary>
/// Retrieving trigger functions.
/// </summary>
/// <param name="node">Node object.</param>
function getTriggerFunctionsMysql(node) {

    node.removeChildNodes();
    node.createChildNode('', false, 'node-spin', null,
        null);

    execAjax('/get_triggerfunctions_mysql/',
        JSON.stringify({
            "p_database_index": v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
            "p_tab_id": v_connTabControl.selectedTab.id,
            "p_schema": null
        }),
        function(p_return) {

            if (node.childNodes.length > 0)
                node.removeChildNodes();

            node.setText('Trigger Functions (' + p_return.v_data.length +
                ')');

            node.tag.num_tables = p_return.v_data.length;

            for (i = 0; i < p_return.v_data.length; i++) {

                node.createChildNode(p_return.v_data[i].v_name, false,
                    '/static/OmniDB_app/images/gear2.png', {
                        type: 'triggerfunction',
                        id: p_return.v_data[i].v_id,
                        database: v_connTabControl.selectedTab.tag.selectedDatabase
                    }, 'cm_triggerfunction');

            }

        },
        function(p_return) {
            nodeOpenErrorMysql(p_return, node);
        },
        'box',
        false);
}

/// <summary>
/// Retrieving trigger function definition.
/// </summary>
/// <param name="node">Node object.</param>
function getTriggerFunctionDefinitionMysql(node) {

    execAjax('/get_triggerfunction_definition_mysql/',
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
            v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.sel_filtered_data
                .value = 1;

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

            maximizeEditor();

        },
        function(p_return) {
            nodeOpenErrorMysql(p_return, node);
        },
        'box',
        true);

}
*/

/// <summary>
/// Retrieving SELECT SQL template.
/// </summary>
function TemplateSelectMysql(p_schema, p_table) {
  execAjax(
    "/template_select_mysql/",
    JSON.stringify({
      p_database_index: tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      p_tab_id: tabsStore.selectedPrimaryTab.id,
      p_table: p_table,
      p_schema: p_schema,
    }),
    function (p_return) {
      let tab_name = `${p_schema}.${p_table}`
      tabsStore.createQueryTab(tab_name, null, null, p_return.v_data.v_template)

      setTimeout(() => {
          emitter.emit(`${tabsStore.selectedPrimaryTab.metaData.selectedTab.id}_run_query`)
      }, 200)
    },
    function (p_return) {
      showToast("error", p_return.v_data)
      return "";
    },
    "box",
    true
  );
}

/// <summary>
/// Retrieving INSERT SQL template.
/// </summary>
function TemplateInsertMysql(p_schema, p_table) {
  execAjax(
    "/template_insert_mysql/",
    JSON.stringify({
      p_database_index: tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      p_tab_id: tabsStore.selectedPrimaryTab.id,
      p_table: p_table,
      p_schema: p_schema,
    }),
    function (p_return) {
      tabSQLTemplate(
        "Insert " + p_schema + "." + p_table,
        p_return.v_data.v_template
      );
    },
    function (p_return) {
      showToast("error", p_return.v_data)
      return "";
    },
    "box",
    true
  );
}

/// <summary>
/// Retrieving UPDATE SQL template.
/// </summary>
function TemplateUpdateMysql(p_schema, p_table) {
  execAjax(
    "/template_update_mysql/",
    JSON.stringify({
      p_database_index: tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      p_tab_id: tabsStore.selectedPrimaryTab.id,
      p_table: p_table,
      p_schema: p_schema,
    }),
    function (p_return) {
      tabSQLTemplate(
        "Update " + p_schema + "." + p_table,
        p_return.v_data.v_template
      );
    },
    function (p_return) {
      showToast("error", p_return.v_data)
      return "";
    },
    "box",
    true
  );
}

/*function getMajorVersionMysql(p_version) {
    var v_version = p_version.split(' (')[0]
    var tmp = v_version.replace('PostgreSQL ', '').replace('beta', '.').split(
        '.')
    tmp.pop()
    return tmp.join('.')
}*/

export {
  TemplateSelectMysql,
  TemplateInsertMysql,
  TemplateUpdateMysql,
};
