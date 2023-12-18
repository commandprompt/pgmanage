<template>
  <PowerTree ref="tree" v-model="nodes" @nodedblclick="doubleClickNode" @toggle="onToggle"
    @nodecontextmenu="onContextMenu" :allow-multiselect="false" @nodeclick="onClickHandler">
    <template v-slot:toggle="{ node }">
      <i v-if="node.isExpanded" class="exp_col fas fa-chevron-down"></i>
      <i v-if="!node.isExpanded" class="exp_col fas fa-chevron-right"></i>
    </template>

    <template v-slot:title="{ node }">
      <span class="item-icon">
        <i :class="['icon_tree', node.data.icon]"></i>
      </span>
      <span v-if="node.data.raw_html" v-html="node.title"> </span>
      <span v-else>
        {{ formatTitle(node) }}
      </span>
    </template>
  </PowerTree>
</template>

<script>
import TreeMixin from "../mixins/power_tree.js";
import { PowerTree } from "@onekiloparsec/vue-power-tree";
import {
  TemplateSelectSqlite,
  TemplateInsertSqlite,
  TemplateUpdateSqlite,
} from "../tree_context_functions/tree_sqlite";

import { tabSQLTemplate } from "../tree_context_functions/tree_postgresql";
import { getProperties, clearProperties } from "../properties";
import { createDataEditorTab } from "../tab_functions/data_editor_tab";
import { createSchemaEditorTab } from "../tab_functions/schema_editor_tab";
import { emitter } from "../emitter";

export default {
  name: "TreeSqlite",
  components: {
    PowerTree: PowerTree,
  },
  mixins: [TreeMixin],
  props: {
    databaseIndex: {
      type: Number,
      required: true,
    },
    tabId: {
      type: String,
      required: true,
    },
  },
  data() {
    return {
      nodes: [
        {
          title: "Sqlite",
          isExpanded: false,
          isDraggable: false,
          data: {
            icon: "node node-sqlite",
            type: "server",
            contextMenu: "cm_server",
          },
        },
      ],
    };
  },
  computed: {
    contextMenu() {
      return {
        cm_server: [this.cmRefreshObject],
        cm_tables: [
          this.cmRefreshObject,
          {
            label: "ER Diagram",
            icon: "fab cm-all fa-hubspot",
            onClick: () => {
              v_connTabControl.tag.createERDTab();
            },
          },
          {
            label: "Create Table",
            icon: "fas cm-all fa-plus",
            onClick: () => {
              createSchemaEditorTab(this.selectedNode, "create", "sqlite3");
            },
          },
        ],
        cm_table: [
          this.cmRefreshObject,
          {
            label: "Data Actions",
            icon: "fas cm-all fa-list",
            children: [
              {
                label: "Query Data",
                icon: "fas cm-all fa-search",
                onClick: () => {
                  TemplateSelectSqlite(this.selectedNode.title, "t");
                },
              },
              {
                label: "Edit Data",
                icon: "fas cm-all fa-table",
                onClick: () => {
                  createDataEditorTab(
                    this.selectedNode.title,
                    null
                  );
                },
              },
              {
                label: "Insert Record",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  TemplateInsertSqlite(this.selectedNode.title);
                },
              },
              {
                label: "Update Records",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  TemplateUpdateSqlite(this.selectedNode.title);
                },
              },
              {
                label: "Delete Records",
                icon: "fas cm-all fa-times",
                onClick: () => {
                  tabSQLTemplate(
                    "Delete Records",
                    this.templates.delete.replace(
                      "#table_name#",
                      this.selectedNode.data.raw_value
                    )
                  );
                },
              },
            ],
          },
          {
            label: "Table Actions",
            icon: "fas cm-all fa-list",
            children: [
              {
                label: "Alter Table",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  createSchemaEditorTab(this.selectedNode, "alter", "sqlite3");
                },
              },
              {
                label: "Drop Table",
                icon: "fas cm-all fa-times",
                onClick: () => {
                  tabSQLTemplate(
                    "Drop Table",
                    this.templates.drop_table.replace(
                      "#table_name#",
                      this.selectedNode.data.raw_value
                    )
                  );
                },
              },
            ],
          },
        ],
        cm_columns: [
          this.cmRefreshObject,
          {
            label: "Create Column",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Column",
                this.templates.create_column.replace(
                  "#table_name#",
                  this.getParentNode(this.selectedNode).title
                )
              );
            },
          },
        ],
        cm_column: [],
        cm_pks: [this.cmRefreshObject],
        cm_pk: [this.cmRefreshObject],
        cm_fks: [this.cmRefreshObject],
        cm_fk: [this.cmRefreshObject],
        cm_uniques: [this.cmRefreshObject],
        cm_unique: [this.cmRefreshObject],
        cm_indexes: [
          this.cmRefreshObject,
          {
            label: "Create Index",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Index",
                this.templates.create_index.replace(
                  "#table_name#",
                  this.getParentNode(this.selectedNode).title
                )
              );
            },
          },
        ],
        cm_index: [
          this.cmRefreshObject,
          {
            label: "Reindex",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Reindex",
                this.templates.reindex.replace(
                  "#index_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Drop Index",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Index",
                this.templates.drop_index.replace(
                  "#index_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_triggers: [
          this.cmRefreshObject,
          {
            label: "Create Trigger",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Trigger",
                this.templates.create_trigger.replace(
                  "#table_name#",
                  this.getParentNode(this.selectedNode).title
                )
              );
            },
          },
        ],
        cm_trigger: [
          {
            label: "Drop Trigger",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Trigger",
                this.templates.drop_trigger.replace(
                  "#trigger_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_views: [
          this.cmRefreshObject,
          {
            label: "Create View",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate("Create View", this.templates.create_view);
            },
          },
        ],
        cm_view: [
          this.cmRefreshObject,
          {
            label: "Query Data",
            icon: "fas cm-all fa-search",
            onClick: () => {
              TemplateSelectSqlite(this.selectedNode.title, "v");
            },
          },
          {
            label: "Drop View",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop View",
                this.templates.drop_view.replace(
                  "#view_name#",
                  this.selectedNode.data.raw_value
                )
              );
            },
          },
        ],
      };
    },
  },
  mounted() {
    this.$nextTick(() => {
      setTimeout(() => {
        this.doubleClickNode(this.getRootNode())
        this.doubleClickNode(this.$refs.tree.getNode([0, 0]))
      }, 200)
    })

    emitter.on(`schemaChanged_${this.id}`, () => {
      const tree = this.$refs.tree;
      let tables_node = tree.getNextNode([0], (node) => {
        return node.data.type === "table_list";
      });

      this.refreshTree(tables_node);
    });
  },
  methods: {
    refreshTree(node) {
      if (node.children.length == 0) this.insertSpinnerNode(node);
      if (node.data.type == "server") {
        this.getTreeDetailsSqlite(node);
      } else if (node.data.type == "table_list") {
        this.getTablesSqlite(node);
      } else if (node.data.type == "table") {
        this.getColumnsSqlite(node);
      } else if (node.data.type == "primary_key") {
        this.getPKSqlite(node);
      } else if (node.data.type == "pk") {
        this.getPKColumnsSqlite(node);
      } else if (node.data.type == "foreign_keys") {
        this.getFKsSqlite(node);
      } else if (node.data.type == "foreign_key") {
        this.getFKsColumnsSqlite(node);
      } else if (node.data.type == "uniques") {
        this.getUniquesSqlite(node);
      } else if (node.data.type == "unique") {
        this.getUniquesColumnsSqlite(node);
      } else if (node.data.type == "indexes") {
        this.getIndexesSqlite(node);
      } else if (node.data.type == "index") {
        this.getIndexesColumnsSqlite(node);
      } else if (node.data.type == "trigger_list") {
        this.getTriggersSqlite(node);
      } else if (node.data.type == "view_list") {
        this.getViewsSqlite(node);
      } else if (node.data.type == "view") {
        this.getViewsColumnsSqlite(node);
      }
    },
    getProperties(node) {
      let table;
      switch (node.data.type) {
        case "table_field":
          table = this.getParentNodeDeep(node, 2).title;
        case "unique":
          table = this.getParentNodeDeep(node, 2).title;
        default:
          table = null;
      }
      const handledTypes = [
        "table",
        "table_field",
        "view",
        "trigger",
        "index",
        "pk",
        "foreign_key",
        "unique",
      ];
      if (handledTypes.includes(node.data.type)) {
        getProperties("/get_properties_sqlite/", {
          table: table,
          object: node.title,
          type: node.data.type,
        });
      } else {
        clearProperties();
      }
    },
    getTreeDetailsSqlite(node) {
      this.api
        .post("/get_tree_info_sqlite/")
        .then((resp) => {
          this.removeChildNodes(node);
          this.$refs.tree.updateNode(node.path, {
            title: resp.data.version,
          });
          this.templates = resp.data;

          this.insertNode(node, "Views", {
            icon: "fas node-all fa-eye node-view-list",
            type: "view_list",
            contextMenu: "cm_views",
          });
          this.insertNode(node, "Tables", {
            icon: "fas node-all fa-th node-table-list",
            type: "table_list",
            contextMenu: "cm_tables",
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getTablesSqlite(node) {
      this.api
        .post("/get_tables_sqlite/")
        .then((resp) => {
          this.removeChildNodes(node);
          this.$refs.tree.updateNode(node.path, {
            title: `Tables (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-table node-table",
              type: "table",
              contextMenu: "cm_table",
              raw_value: el.name_raw,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getColumnsSqlite(node) {
      this.api
        .post("/get_columns_sqlite/", {
          table: node.title,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.insertNode(node, "Triggers", {
            icon: "fas node-all fa-bolt node-trigger",
            type: "trigger_list",
            contextMenu: "cm_triggers",
          });
          this.insertNode(node, "Indexes", {
            icon: "fas node-all fa-thumbtack node-index",
            type: "indexes",
            contextMenu: "cm_indexes",
          });
          this.insertNode(node, "Uniques", {
            icon: "fas node-all fa-key node-unique",
            type: "uniques",
            contextMenu: "cm_uniques",
          });
          this.insertNode(node, "Foreign Keys", {
            icon: "fas node-all fa-key node-fkey",
            type: "foreign_keys",
            contextMenu: "cm_fks",
          });
          this.insertNode(node, "Primary Key", {
            icon: "fas node-all fa-key node-pkey",
            type: "primary_key",
            contextMenu: "cm_pks",
          });
          this.insertNode(node, `Columns (${resp.data.length})`, {
            icon: "fas node-all fa-columns node-column",
            type: "column_list",
            contextMenu: "cm_columns",
          });
          const columns_node = this.getFirstChildNode(node);
          resp.data.reduceRight((_, el) => {
            this.insertNode(columns_node, el.column_name, {
              icon: "fas node-all fa-columns node-column",
              type: "table_field",
              contextMenu: "cm_column",
            });
            const table_field = this.getFirstChildNode(columns_node);

            this.insertNode(
              table_field,
              `Nullable: ${el.nullable}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
              },
              true
            );
            this.insertNode(
              table_field,
              `Type: ${el.data_type}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
              },
              true
            );
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getPKSqlite(node) {
      this.api
        .post("/get_pk_sqlite/", {
          table: this.getParentNode(node).title,
        })
        .then((resp) => {
          this.removeChildNodes(node);
          this.$refs.tree.updateNode(node.path, {
            title: `Primary Key (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el, {
              icon: "fas node-all fa-key node-pkey",
              type: "pk",
              contextMenu: "cm_pk",
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getPKColumnsSqlite(node) {
      this.api
        .post("/get_pk_columns_sqlite/", {
          table: this.getParentNodeDeep(node, 2).title,
        })
        .then((resp) => {
          this.removeChildNodes(node);
          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el,
              {
                icon: "fas node-all fa-columns node-column",
              },
              true
            );
          });
        });
    },
    getFKsSqlite(node) {
      this.api
        .post("/get_fks_sqlite/", {
          table: this.getParentNode(node).title,
        })
        .then((resp) => {
          this.removeChildNodes(node);
          this.$refs.tree.updateNode(node.path, {
            title: `Foreign Keys (${resp.data.length})`,
          });
          resp.data.reduceRight((_, el) => {
            this.insertNode(node, el, {
              icon: "fas node-all fa-key node-fkey",
              type: "foreign_key",
              contextMenu: "cm_fk",
            });
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getFKsColumnsSqlite(node) {
      this.api
        .post("/get_fks_columns_sqlite/", {
          table: this.getParentNodeDeep(node, 2).title,
          fkey: node.title,
        })
        .then((resp) => {
          this.removeChildNodes(node);
          this.insertNode(
            node,
            `${resp.data.column_name} <i class='fas node-all fa-arrow-right'></i> ${resp.data.r_column_name}`,
            {
              icon: "fas node-all fa-columns node-column",
              raw_html: true,
            },
            true
          );
          this.insertNode(
            node,
            `Update Rule: ${resp.data.update_rule}`,
            {
              icon: "fas node-all fa-ellipsis-h node-bullet",
            },
            true
          );
          this.insertNode(
            node,
            `Delete Rule: ${resp.data.delete_rule}`,
            {
              icon: "fas node-all fa-ellipsis-h node-bullet",
            },
            true
          );
          this.insertNode(
            node,
            `Referenced Table: ${resp.data.r_table_name}`,
            {
              icon: "fas node-all fa-table node-table",
            },
            true
          );
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getUniquesSqlite(node) {
      this.api
        .post("/get_uniques_sqlite/", {
          table: this.getParentNode(node).title,
        })
        .then((resp) => {
          this.removeChildNodes(node);
          this.$refs.tree.updateNode(node.path, {
            title: `Uniques (${resp.data.length})`,
          });
          resp.data.forEach((el) => {
            this.insertNode(node, el, {
              icon: "fas node-all fa-key node-unique",
              type: "unique",
              contextMenu: "cm_unique",
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getUniquesColumnsSqlite(node) {
      this.api
        .post("/get_uniques_columns_sqlite/", {
          table: this.getParentNodeDeep(node, 2).title,
          unique: node.title,
        })
        .then((resp) => {
          this.removeChildNodes(node);
          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el,
              {
                icon: "fas node-all fa-columns node-column",
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getIndexesSqlite(node) {
      this.api
        .post("/get_indexes_sqlite/", {
          table: this.getParentNode(node).title,
        })
        .then((resp) => {
          this.removeChildNodes(node);
          this.$refs.tree.updateNode(node.path, {
            title: `Indexes (${resp.data.length})`,
          });
          resp.data.forEach((el) => {
            this.insertNode(node, `${el.index_name}`, {
              icon: "fas node-all fa-thumbtack node-index",
              type: "index",
              contextMenu: "cm_index",
              uniqueness: el.uniqueness,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getIndexesColumnsSqlite(node) {
      this.api
        .post("/get_indexes_columns_sqlite/", {
          table: this.getParentNodeDeep(node, 2).title,
          index: node.title,
        })
        .then((resp) => {
          this.removeChildNodes(node);
          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el,
              {
                icon: "fas node-all fa-columns node-column",
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getTriggersSqlite(node) {
      this.api
        .post("/get_triggers_sqlite/", {
          table: this.getParentNode(node).title,
        })
        .then((resp) => {
          this.removeChildNodes(node);
          this.$refs.tree.updateNode(node.path, {
            title: `Triggers (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el,
              {
                icon: "fas node-all fa-bolt node-trigger",
                type: "trigger",
                contextMenu: "cm_trigger",
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getViewsSqlite(node) {
      this.api
        .post("/get_views_sqlite/")
        .then((resp) => {
          this.removeChildNodes(node);
          this.$refs.tree.updateNode(node.path, {
            title: `Views (${resp.data.length})`,
          });
          resp.data.forEach((element) => {
            this.insertNode(node, element.name, {
              icon: "fas node-all fa-eye node-view",
              type: "view",
              contextMenu: "cm_view",
              raw_value: element.name_raw
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getViewsColumnsSqlite(node) {
      this.api
        .post("/get_views_columns_sqlite/", {
          table: node.title,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.insertNode(node, "Triggers", {
            icon: "fas node-all fa-bolt node-trigger",
            type: "trigger_list",
            contextMenu: "cm_view_triggers",
          });
          this.insertNode(node, `Columns (${resp.data.length})`, {
            icon: "fas node-all fa-columns node-column",
          });
          const columns_node = this.getFirstChildNode(node);

          resp.data.reduceRight((_, el) => {
            this.insertNode(columns_node, el.column_name, {
              icon: "fas node-all fa-columns node-column",
              type: "table_field",
            });
            const table_field = this.getFirstChildNode(columns_node);

            this.insertNode(
              table_field,
              `Type: ${el.data_type}`,
              {
                icon: "fas node-all fa-ellipsis-h node-bullet",
              },
              true
            );
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
  },
};
</script>
