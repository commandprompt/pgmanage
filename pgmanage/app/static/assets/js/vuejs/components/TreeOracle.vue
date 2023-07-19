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
import TreeMixin from "../mixins/power_tree.mjs";
const { PowerTree } = window["VuePowerTree"];

export default {
  name: "TreeOracle",
  components: {
    PowerTree,
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
          title: "Oracle",
          isExpanded: false,
          isDraggable: false,
          data: {
            icon: "node-oracle",
            type: "server",
            contextMenu: "cm_server",
          },
        },
      ],
      templates: "",
    };
  },
  computed: {
    contextMenu() {
      return {
        cm_server: [this.cmRefreshObject],
        cm_database: [
          {
            label: "Render Graph",
            icon: "fab cm-all fa-hubspot",
            children: [
              {
                label: "Simple Graph",
                icon: "fab cm-all fa-hubspot",
                onClick: () => {
                  v_connTabControl.tag.createGraphTab(this.selectedNode.title);
                  drawGraph(false, this.templates.username);
                },
              },
              {
                label: "Complete Graph",
                icon: "fab cm-all fa-hubspot",
                onClick: () => {
                  v_connTabControl.tag.createGraphTab(this.selectedNode.title);
                  drawGraph(true, this.templates.username);
                },
              },
            ],
          },
        ],
        cm_tables: [
          this.cmRefreshObject,
          {
            label: "Create Table",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Table",
                this.templates.create_table.replace(
                  "#schema_name#",
                  this.templates.username
                )
              );
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
                  TemplateSelectOracle(
                    this.templates.username,
                    this.selectedNode.title
                  );
                },
              },
              {
                label: "Edit Data",
                icon: "fas cm-all fa-table",
                onClick: () => {
                  v_startEditData(
                    this.selectedNode.title,
                    this.templates.username
                  );
                },
              },
              {
                label: "Insert Record",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  TemplateInsertOracle(
                    this.templates.username,
                    this.selectedNode.title
                  );
                },
              },
              {
                label: "Update Records",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  TemplateUpdateOracle(
                    this.templates.username,
                    this.selectedNode.title
                  );
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
                      `${this.templates.username}.${this.selectedNode.title}`
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
                label: "Alter Table (SQL)",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  tabSQLTemplate(
                    "Alter Table",
                    this.templates.alter_table.replace(
                      "#table_name#",
                      `${this.templates.username}.${this.selectedNode.title}`
                    )
                  );
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
                      `${this.templates.username}.${this.selectedNode.title}`
                    )
                  );
                },
              },
            ],
          },
        ],
        cm_columns: [
          {
            label: "Create Column",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Field",
                this.templates.create_column.replace(
                  "#table_name#",
                  `${this.templates.username}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
        ],
        cm_column: [
          {
            label: "Alter Column",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Column",
                this.templates.alter_column
                  .replace(
                    "#table_name#",
                    `${this.templates.username}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace(/#column_name#/g, this.selectedNode.title)
              );
            },
          },
          {
            label: "Drop Column",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Column",
                this.templates.drop_column
                  .replace(
                    "#table_name#",
                    `${this.templates.username}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace(/#column_name#/g, this.selectedNode.title)
              );
            },
          },
        ],
        cm_pks: [
          this.cmRefreshObject,
          {
            label: "Create Primary Key",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Primary Key",
                this.templates.create_primarykey.replace(
                  "#table_name#",
                  `${this.templates.username}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
        ],
        cm_pk: [
          this.cmRefreshObject,
          {
            label: "Drop Primary Key",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Primary Key",
                this.templates.drop_primarykey
                  .replace(
                    "#table_name#",
                    `${this.templates.username}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#constraint_name#", this.selectedNode.title)
              );
            },
          },
        ],
        cm_fks: [
          this.cmRefreshObject,
          {
            label: "Create Foreign Key",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Foreign Key",
                this.templates.create_foreignkey.replace(
                  "#table_name#",
                  `${this.templates.username}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
        ],
        cm_fk: [
          this.cmRefreshObject,
          {
            label: "Drop Foreign Key",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Foreign Key",
                this.templates.drop_foreignkey
                  .replace(
                    "#table_name#",
                    `${this.templates.username}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#constraint_name#", this.selectedNode.title)
              );
            },
          },
        ],
        cm_uniques: [
          this.cmRefreshObject,
          {
            label: "Create Unique",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Unique",
                this.templates.create_unique.replace(
                  "#table_name#",
                  `${this.templates.username}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
        ],
        cm_unique: [
          this.cmRefreshObject,
          {
            label: "Drop Unique",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Unique",
                this.templates.drop_unique
                  .replace(
                    "#table_name#",
                    `${this.templates.username}.${this.getParentNodeDeep(this.selectedNode, 2).title
                    }`
                  )
                  .replace("#constraint_name#", this.selectedNode.title)
              );
            },
          },
        ],
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
                  `${this.templates.username}.${this.getParentNode(this.selectedNode).title
                  }`
                )
              );
            },
          },
        ],
        cm_index: [
          this.cmRefreshObject,
          {
            label: "Alter Index",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Index",
                this.templates.alter_index.replace(
                  "#index_name#",
                  `${this.templates.username}.${this.selectedNode.title}`
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
                  `${this.templates.username}.${this.selectedNode.title}`
                )
              );
            },
          },
        ],
        cm_sequences: [
          this.cmRefreshObject,
          {
            label: "Create Sequence",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Sequence",
                this.templates.create_sequence.replace(
                  "#schema_name#",
                  this.templates.username
                )
              );
            },
          },
        ],
        cm_sequence: [
          {
            label: "Alter Sequence",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Sequence",
                this.templates.alter_sequence.replace(
                  "#sequence_name#",
                  `${this.templates.username}.${this.selectedNode.title}`
                )
              );
            },
          },
          {
            label: "Drop Sequence",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Sequence",
                this.templates.drop_sequence.replace(
                  "#sequence_name#",
                  `${this.templates.username}.${this.selectedNode.title}`
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
              tabSQLTemplate(
                "Create View",
                this.templates.create_view.replace(
                  "#schema_name#",
                  this.templates.username
                )
              );
            },
          },
        ],
        cm_view: [
          this.cmRefreshObject,
          {
            label: "Query Data",
            icon: "fas cm-all fa-search",
            onClick: () => {
              // FIX this to use TemplateSelectMariadb
              let table_name;
              table_name = `${this.templates.username}.${this.selectedNode.title}`;

              v_connTabControl.tag.createQueryTab(this.selectedNode.title);

              v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
                `-- Querying Data\nselect t.*\nfrom ${table_name} t`
              );
              v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
              renameTabConfirm(
                v_connTabControl.selectedTab.tag.tabControl.selectedTab,
                this.selectedNode.title
              );

              querySQL(0);
            },
          },
          {
            label: "Edit View",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              // Fix this not to use v_connTabControl
              v_connTabControl.tag.createQueryTab(this.selectedNode.title);
              this.getViewDefinitionOracle(this.selectedNode);
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
                  `${this.templates.username}.${this.selectedNode.title}`
                )
              );
            },
          },
        ],
        cm_functions: [
          this.cmRefreshObject,
          {
            label: "Create Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Function",
                this.templates.create_function.replace(
                  "#schema_name#",
                  this.templates.username
                )
              );
            },
          },
        ],
        cm_function: [
          this.cmRefreshObject,
          {
            label: "Edit Function",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              //Fix this not to use v_connTabControl
              v_connTabControl.tag.createQueryTab(this.selectedNode.title);
              this.getFunctionDefinitionOracle(this.selectedNode);
            },
          },
          {
            label: "Drop Function",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Function",
                this.templates.drop_function.replace(
                  "#function_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
        ],
        cm_procedures: [
          this.cmRefreshObject,
          {
            label: "Create Procedure",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Procedure",
                this.templates.create_procedure.replace(
                  "#schema_name#",
                  this.templates.username
                )
              );
            },
          },
        ],
        cm_procedure: [
          this.cmRefreshObject,
          {
            label: "Edit Procedure",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              // Fix this not to use v_connTabControl
              v_connTabControl.tag.createQueryTab(this.selectedNode.title);
              this.getProcedureDefinitionOracle(this.selectedNode);
            },
          },
          {
            label: "Drop Procedure",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Procedure",
                this.templates.drop_procedure.replace(
                  "#function_name#",
                  this.selectedNode.data.id
                )
              );
            },
          },
        ],
        cm_tablespaces: [
          this.cmRefreshObject,
          {
            label: "Create Tablespace",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Create Tablespace",
                this.templates.create_tablespace
              );
            },
          },
        ],
        cm_tablespace: [
          {
            label: "Alter Tablespace",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Tablespace",
                this.templates.alter_tablespace.replace(
                  "#tablespace_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Drop Tablespace",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Tablespace",
                this.templates.drop_tablespace.replace(
                  "#tablespace_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
        cm_roles: [
          this.cmRefreshObject,
          {
            label: "Create Role",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate("Create Role", this.templates.create_role);
            },
          },
        ],
        cm_role: [
          {
            label: "Alter Role",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Role",
                this.templates.alter_role.replace(
                  "#role_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Drop Role",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Role",
                this.templates.drop_role.replace(
                  "#role_name#",
                  this.selectedNode.title
                )
              );
            },
          },
        ],
      };
    },
  },
  methods: {
    refreshTree(node) {
      if (node.children.length == 0) this.insertSpinnerNode(node);

      if (node.data.type == "server") {
        this.getTreeDetailsOracle(node);
      } else if (node.data.type == "database") {
        this.getDatabaseObjectsOracle(node);
      } else if (node.data.type == "table_list") {
        this.getTablesOracle(node);
      } else if (node.data.type == "table") {
        this.getColumnsOracle(node);
      } else if (node.data.type == "primary_key") {
        this.getPKOracle(node);
      } else if (node.data.type == "pk") {
        this.getPKColumnsOracle(node);
      } else if (node.data.type == "foreign_keys") {
        this.getFKsOracle(node);
      } else if (node.data.type == "foreign_key") {
        this.getFKsColumnsOracle(node);
      } else if (node.data.type == "uniques") {
        this.getUniquesOracle(node);
      } else if (node.data.type == "unique") {
        this.getUniquesColumnsOracle(node);
      } else if (node.data.type == "indexes") {
        this.getIndexesOracle(node);
      } else if (node.data.type == "index") {
        this.getIndexesColumnsOracle(node);
      } else if (node.data.type == "sequence_list") {
        this.getSequencesOracle(node);
      } else if (node.data.type == "view_list") {
        this.getViewsOracle(node);
      } else if (node.data.type == "view") {
        this.getViewsColumnsOracle(node);
      } else if (node.data.type == "function_list") {
        this.getFunctionsOracle(node);
      } else if (node.data.type == "function") {
        this.getFunctionFieldsOracle(node);
      } else if (node.data.type == "procedure_list") {
        this.getProceduresOracle(node);
      } else if (node.data.type == "procedure") {
        this.getProcedureFieldsOracle(node);
      } else if (node.data.type == "tablespace_list") {
        this.getTablespacesOracle(node);
      } else if (node.data.type == "role_list") {
        this.getRolesOracle(node);
      }
    },
    getProperties(node) {
      let table;
      switch (node.data.type) {
        case "trigger":
          table = this.getParentNodeDeep(node, 2).title;
        default:
          table = null;
      }
      const handledTypes = [
        "role",
        "tablespace",
        "table",
        "sequence",
        "view",
        "mview",
        "function",
        "procedure",
        "trigger",
        "triggerfunction",
      ];

      if (handledTypes.includes(node.data.type)) {
        getPropertiesNew("/get_properties_oracle/", {
          table: table,
          object: node.title,
          type: node.data.type,
          schema: null,
        });
      } else {
        clearProperties();
      }
    },
    getTreeDetailsOracle(node) {
      this.api
        .post("/get_tree_info_oracle/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: resp.data.version,
          });

          this.templates = resp.data;

          if (resp.data.superuser) {
            this.contextMenu.cm_server = [this.cmRefreshObject];
            this.contextMenu.cm_server.push({
              label: "Monitoring",
              icon: "fas cm-all fa-chart-line",
              children: [
                {
                  label: "Sessions",
                  icon: "fas cm-all fa-chart-line",
                  onClick: () => {
                    window.v_connTabControl.tag.createMonitoringTab(
                      "Sessions",
                      "select * from v$session",
                      [
                        {
                          icon: "fas cm-all fa-times",
                          title: "Terminate",
                          action: "oracleTerminateBackend",
                        },
                      ]
                    );
                  },
                },
              ],
            });

            this.insertNode(node, "Roles", {
              icon: "fas node-all fa-users node-user-list",
              type: "role_list",
              contextMenu: "cm_roles",
            });

            this.insertNode(node, "Tablespaces", {
              icon: "fas node-all fa-folder-open node-tablespace-list",
              type: "tablespace_list",
              contextMenu: "cm_tablespaces",
            });
          }

          this.insertNode(node, resp.data.database, {
            icon: "fas node-all fa-database node-database-list",
            type: "database",
            contextMenu: "cm_database",
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getDatabaseObjectsOracle(node) {
      this.removeChildNodes(node);

      this.insertNode(node, "Procedures", {
        icon: "fas node-all fa-cog node-procedure-list",
        type: "procedure_list",
        contextMenu: "cm_procedures",
      });

      this.insertNode(node, "Functions", {
        icon: "fas node-all fa-cog node-function-list",
        type: "function_list",
        contextMenu: "cm_functions",
      });

      this.insertNode(node, "Views", {
        icon: "fas node-all fa-eye node-view-list",
        type: "view_list",
        contextMenu: "cm_views",
      });

      this.insertNode(node, "Sequences", {
        icon: "fas node-all fa-sort-numeric-down node-sequence-list",
        type: "sequence_list",
        contextMenu: "cm_sequences",
      });

      this.insertNode(node, "Tables", {
        icon: "fas node-all fa-th node-table-list",
        type: "table_list",
        contextMenu: "cm_tables",
      });
    },
    getTablesOracle(node) {
      this.api
        .post("/get_tables_oracle/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Tables (${resp.data.length})`,
          });

          resp.data.reduceRight((_, el) => {
            this.insertNode(node, el, {
              icon: "fas node-all fa-table node-table",
              type: "table",
              contextMenu: "cm_table",
            });
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getColumnsOracle(node) {
      this.api
        .post("/get_columns_oracle/", {
          table: node.title,
        })
        .then((resp) => {
          this.removeChildNodes(node);

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
    getPKOracle(node) {
      this.api
        .post("/get_pk_oracle/", {
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
    getPKColumnsOracle(node) {
      this.api
        .post("/get_pk_columns_oracle/", {
          key: node.title,
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
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getFKsOracle(node) {
      this.api
        .post("/get_fks_oracle/", {
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
    getFKsColumnsOracle(node) {
      this.api
        .post("/get_fks_columns_oracle/", {
          fkey: node.title,
          table: this.getParentNodeDeep(node, 2).title,
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
    getUniquesOracle(node) {
      this.api
        .post("/get_uniques_oracle/", {
          table: this.getParentNode(node).title,
          schema: null,
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
    getUniquesColumnsOracle(node) {
      this.api
        .post("/get_uniques_columns_oracle/", {
          unique: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: null,
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
    getIndexesOracle(node) {
      this.api
        .post("/get_indexes_oracle/", {
          table: this.getParentNode(node).title,
          schema: null,
        })
        .then((resp) => {
          this.removeChildNodes(node);
          this.$refs.tree.updateNode(node.path, {
            title: `Indexes (${resp.data.length})`,
          });
          resp.data.forEach((el) => {
            this.insertNode(node, el.index_name, {
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
    getIndexesColumnsOracle(node) {
      this.api
        .post("/get_indexes_columns_oracle/", {
          index: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: null,
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
    getSequencesOracle(node) {
      this.api
        .post("/get_sequences_oracle/")
        .then((resp) => {
          this.removeChildNodes(node);
          this.$refs.tree.updateNode(node.path, {
            title: `Sequences (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(
              node,
              el.sequence_name,
              {
                icon: "fas node-all fa-sort-numeric-down node-sequence",
                type: "sequence",
                contextMenu: "cm_sequence",
              },
              true
            );
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getViewsOracle(node) {
      this.api
        .post("/get_views_oracle/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Views (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-eye node-view",
              type: "view",
              contextMenu: "cm_view",
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getViewsColumnsOracle(node) {
      this.api
        .post("/get_views_columns_oracle/", {
          table: node.title,
          schema: null,
        })
        .then((resp) => {
          this.removeChildNodes(node);

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
    getViewDefinitionOracle(node) {
      this.api
        .post("/get_view_definition_oracle/", {
          view: node.title,
          schema: null,
        })
        .then((resp) => {
          // Fix this not to use v_connTabControl
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
            resp.data.data
          );
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.gotoLine(
            0,
            0,
            true
          );
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getFunctionsOracle(node) {
      this.api
        .post("/get_functions_oracle/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Functions (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-cog node-function",
              type: "function",
              id: el.id,
              contextMenu: "cm_function",
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getFunctionFieldsOracle(node) {
      this.api
        .post("/get_function_fields_oracle/", {
          function: node.data.id,
          schema: null,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          resp.data.reduceRight((_, el) => {
            if (el.type === "O") {
              this.insertNode(
                node,
                el.name,
                {
                  icon: "fas node-all fa-arrow-right node-function-field",
                },
                true
              );
            } else if (el.type === "I") {
              this.insertNode(
                node,
                el.name,
                {
                  icon: "fas node-all fa-arrow-left node-function-field",
                },
                true
              );
            } else {
              this.insertNode(
                node,
                el.name,
                {
                  icon: "fas node-all fa-exchange-alt node-function-field",
                },
                true
              );
            }
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getFunctionDefinitionOracle(node) {
      this.api
        .post("/get_function_definition_oracle/", {
          function: node.data.id,
        })
        .then((resp) => {
          // Fix this not to use v_connTabControl
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
            resp.data.data
          );
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.gotoLine(
            0,
            0,
            true
          );
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getProceduresOracle(node) {
      this.api
        .post("/get_procedures_oracle/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Procedures (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-cog node-procedure",
              type: "procedure",
              contextMenu: "cm_procedure",
              id: el.id,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getProcedureFieldsOracle(node) {
      this.api
        .post("/get_procedure_fields_oracle/", {
          procedure: node.data.id,
          schema: null,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          resp.data.reduceRight((_, el) => {
            if (el.type === "O") {
              this.insertNode(
                node,
                el.name,
                {
                  icon: "fas node-all fa-arrow-right node-function-field",
                },
                true
              );
            } else if (el.type === "I") {
              this.insertNode(
                node,
                el.name,
                {
                  icon: "fas node-all fa-arrow-left node-function-field",
                },
                true
              );
            } else {
              this.insertNode(
                node,
                el.name,
                {
                  icon: "fas node-all fa-exchange-alt node-function-field",
                },
                true
              );
            }
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getProcedureDefinitionOracle(node) {
      this.api
        .post("/get_procedure_definition_oracle/", {
          procedure: node.data.id,
        })
        .then((resp) => {
          // Fix this not to use v_connTabControl
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
            resp.data.data
          );
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
          v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.gotoLine(
            0,
            0,
            true
          );
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getTablespacesOracle(node) {
      this.api
        .post("/get_tablespaces_oracle/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Tablespaces (${resp.data.length})`,
          });

          resp.data.reduceRight((_, el) => {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-folder node-tablespace",
                type: "tablespace",
                contextMenu: "cm_tablespace",
              },
              true
            );
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getRolesOracle(node) {
      this.api
        .post("/get_roles_oracle/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Roles (${resp.data.length})`,
          });

          resp.data.reduceRight((_, el) => {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-user node-user",
                type: "role",
                contextMenu: "cm_role",
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
