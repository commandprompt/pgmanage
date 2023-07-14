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
  name: "TreeMariaDB",
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
          title: "MariaDB",
          isExpanded: false,
          isDraggable: false,
          data: {
            icon: "node-mariadb",
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
        cm_databases: [
          this.cmRefreshObject,
          {
            label: "Create Database",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate("Create Database", this.templates.create_database);
            },
          },
        ],
        cm_database: [
          {
            label: "Render Graph",
            icon: "fab cm-all fa-hubspot",
            children: [
              {
                label: "Simple Graph",
                icon: "fab cm-all fa-hubspot",
                onClick: () => {
                  const node = this.getSelectedNode();
                  window.v_connTabControl.tag.createGraphTab(node.title);
                  drawGraph(false, node.title);
                },
              },
              {
                label: "Complete Graph",
                icon: "fab cm-all fa-hubspot",
                onClick: () => {
                  const node = this.getSelectedNode();
                  v_connTabControl.tag.createGraphTab(node.title);
                  drawGraph(true, node.title);
                },
              },
            ],
          },
          {
            label: "Alter Database",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Alter Database",
                this.templates.alter_database.replace(
                  "#database_name#",
                  node.title
                )
              );
            },
          },
          {
            label: "Drop Database",
            icon: "fas cm-all fa-times",
            onClick: () => {
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Drop Database",
                this.templates.drop_database.replace(
                  "#database_name#",
                  node.title
                )
              );
            },
          },
        ],
        cm_tables: [
          this.cmRefreshObject,
          {
            label: "Create Table",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Create Table",
                this.templates.create_table.replace(
                  "#schema_name#",
                  this.getParentNode(node).title
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
                  const node = this.getSelectedNode();
                  TemplateSelectMariadb(
                    this.getParentNodeDeep(node, 2).title,
                    node.title
                  );
                },
              },
              {
                label: "Edit Data",
                icon: "fas cm-all fa-table",
                onClick: () => {
                  const node = this.getSelectedNode();
                  v_startEditData(
                    node.title,
                    this.getParentNodeDeep(node, 2).title
                  );
                },
              },
              {
                label: "Insert Record",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  const node = this.getSelectedNode();
                  TemplateInsertMariadb(
                    this.getParentNodeDeep(node, 2).title,
                    node.title
                  );
                },
              },
              {
                label: "Update Records",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  const node = this.getSelectedNode();
                  TemplateUpdateMariadb(
                    this.getParentNodeDeep(node, 2).title,
                    node.title
                  );
                },
              },
              {
                label: "Delete Records",
                icon: "fas cm-all fa-times",
                onClick: () => {
                  const node = this.getSelectedNode();
                  tabSQLTemplate(
                    "Delete Records",
                    this.templates.delete.replace(
                      "#table_name#",
                      `${this.getParentNodeDeep(node, 2).title}.${node.title}`
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
                  const node = this.getSelectedNode();
                  tabSQLTemplate(
                    "Alter Table",
                    this.templates.alter_table.replace(
                      "#table_name#",
                      `${this.getParentNodeDeep(node, 2).title}.${node.title}`
                    )
                  );
                },
              },
              {
                label: "Drop Table",
                icon: "fas cm-all fa-times",
                onClick: () => {
                  const node = this.getSelectedNode();
                  tabSQLTemplate(
                    "Drop Table",
                    this.templates.drop_table.replace(
                      "#table_name#",
                      `${this.getParentNodeDeep(node, 2).title}.${node.title}`
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Create Field",
                this.templates.create_column.replace(
                  "#table_name#",
                  `${this.getParentNodeDeep(node, 3).title}.${this.getParentNode(node).title
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Alter Column",
                this.templates.alter_column
                  .replace(
                    "#table_name#",
                    `${this.getParentNodeDeep(node, 4).title}.${this.getParentNodeDeep(node, 2).title
                    }`
                  )
                  .replace(/#column_name#/g, node.title)
              );
            },
          },
          {
            label: "Drop Column",
            icon: "fas cm-all fa-times",
            onClick: () => {
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Drop Column",
                this.templates.alter_column
                  .replace(
                    "#table_name#",
                    `${this.getParentNodeDeep(node, 4).title}.${this.getParentNodeDeep(node, 2).title
                    }`
                  )
                  .replace(/#column_name#/g, node.title)
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Create Primary Key",
                this.templates.create_primarykey.replace(
                  "#table_name#",
                  `${this.getParentNodeDeep(node, 3).title}.${this.getParentNode(node).title
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Drop Primary Key",
                this.templates.drop_primarykey
                  .replace(
                    "#table_name#",
                    `${this.getParentNodeDeep(node, 4).title}.${this.getParentNodeDeep(node, 2).title
                    }`
                  )
                  .replace("#constraint_name#", node.title)
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Create Foreign Key",
                this.templates.create_foreignkey.replace(
                  "#table_name#",
                  `${this.getParentNodeDeep(node, 3).title}.${this.getParentNode(node).title
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Drop Foreign Key",
                this.templates.drop_foreignkey
                  .replace(
                    "#table_name#",
                    `${this.getParentNodeDeep(node, 4).title}.${this.getParentNodeDeep(node, 2).title
                    }`
                  )
                  .replace("#constraint_name#", node.title)
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Create Unique",
                this.templates.create_unique.replace(
                  "#table_name#",
                  `${this.getParentNodeDeep(node, 3).title}.${this.getParentNode(node).title
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Drop Unique",
                this.templates.drop_unique
                  .replace(
                    "#table_name#",
                    `${this.getParentNodeDeep(node, 4).title}.${this.getParentNodeDeep(node, 2).title
                    }`
                  )
                  .replace("#constraint_name#", node.title)
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Create Index",
                this.templates.create_index.replace(
                  "#table_name#",
                  `${this.getParentNodeDeep(node, 3).title}.${this.getParentNode(node).title
                  }`
                )
              );
            },
          },
        ],
        cm_index: [
          this.cmRefreshObject,
          {
            label: "Drop Index",
            icon: "fas cm-all fa-times",
            onClick: () => {
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Drop Index",
                this.templates.drop_index.replace(
                  "#index_name#",
                  `${this.getParentNodeDeep(node, 4).title}.${node.title}`
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Create Sequence",
                this.templates.create_sequence.replace(
                  "#schema_name#",
                  this.getParentNode(node).title
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Alter Sequence",
                this.templates.alter_sequence.replace(
                  "#sequence_name#",
                  `${this.getParentNodeDeep(node, 2).title}.${node.title}`
                )
              );
            },
          },
          {
            label: "Drop Sequence",
            icon: "fas cm-all fa-times",
            onClick: () => {
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Drop Sequence",
                this.templates.drop_sequence.replace(
                  "#sequence_name#",
                  `${this.getParentNodeDeep(node, 2).title}.${node.title}`
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Create View",
                this.templates.create_view.replace(
                  "#schema_name#",
                  this.getParentNode(node).title
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
              const node = this.getSelectedNode();
              let table_name;
              table_name = `${this.getParentNodeDeep(node, 2).title}.${node.title
                }`;

              v_connTabControl.tag.createQueryTab(node.title);

              v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(
                `-- Querying Data\nselect t.*\nfrom ${table_name} t`
              );
              v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
              renameTabConfirm(
                v_connTabControl.selectedTab.tag.tabControl.selectedTab,
                node.title
              );

              querySQL(0);
            },
          },
          {
            label: "Edit View",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              const node = this.getSelectedNode();
              // Fix this not to use v_connTabControl
              v_connTabControl.tag.createQueryTab(node.title);
              this.getViewDefinitionMariadb(node);
            },
          },
          {
            label: "Drop View",
            icon: "fas cm-all fa-times",
            onClick: () => {
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Drop View",
                this.templates.drop_view.replace(
                  "#view_name#",
                  `${this.getParentNodeDeep(node, 2).title}.${node.title}`
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Create Function",
                this.templates.create_function.replace(
                  "#schema_name#",
                  this.getParentNode(node).title
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
              const node = this.getSelectedNode();
              //Fix this not to use v_connTabControl
              v_connTabControl.tag.createQueryTab(node.title);
              this.getFunctionDefinitionMariadb(node);
            },
          },
          {
            label: "Drop Function",
            icon: "fas cm-all fa-times",
            onClick: () => {
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Drop Function",
                this.templates.drop_function.replace(
                  "#function_name#",
                  node.data.id
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Create Procedure",
                this.templates.create_procedure.replace(
                  "#schema_name#",
                  this.getParentNode(node).title
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
              const node = this.getSelectedNode();
              // Fix this not to use v_connTabControl
              v_connTabControl.tag.createQueryTab(node.title);
              this.getProcedureDefinitionMariadb(node);
            },
          },
          {
            label: "Drop Procedure",
            icon: "fas cm-all fa-times",
            onClick: () => {
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Drop Procedure",
                this.templates.drop_procedure.replace(
                  "#function_name#",
                  node.data.id
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
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Alter Role",
                this.templates.alter_role.replace("#role_name#", node.title)
              );
            },
          },
          {
            label: "Drop Role",
            icon: "fas cm-all fa-times",
            onClick: () => {
              const node = this.getSelectedNode();
              tabSQLTemplate(
                "Drop Role",
                this.templates.drop_role.replace("#role_name#", node.title)
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
        this.getTreeDetailsMariadb(node);
      } else if (node.data.type == "database_list") {
        this.getDatabasesMariadb(node);
      } else if (node.data.type == "database") {
        this.getDatabaseObjectsMariadb(node);
      } else if (node.data.type == "table_list") {
        this.getTablesMariadb(node);
      } else if (node.data.type == "table") {
        this.getColumnsMariadb(node);
      } else if (node.data.type == "primary_key") {
        this.getPKMariadb(node);
      } else if (node.data.type == "pk") {
        this.getPKColumnsMariadb(node);
      } else if (node.data.type == "foreign_keys") {
        this.getFKsMariadb(node);
      } else if (node.data.type == "foreign_key") {
        this.getFKsColumnsMariadb(node);
      } else if (node.data.type == "uniques") {
        this.getUniquesMariadb(node);
      } else if (node.data.type == "unique") {
        this.getUniquesColumnsMariadb(node);
      } else if (node.data.type == "indexes") {
        this.getIndexesMariadb(node);
      } else if (node.data.type == "index") {
        this.getIndexesColumnsMariadb(node);
      } else if (node.data.type == "sequence_list") {
        this.getSequencesMariadb(node);
      } else if (node.data.type == "view_list") {
        this.getViewsMariadb(node);
      } else if (node.data.type == "view") {
        this.getViewsColumnsMariadb(node);
      } else if (node.data.type == "function_list") {
        this.getFunctionsMariadb(node);
      } else if (node.data.type == "function") {
        this.getFunctionFieldsMariadb(node);
      } else if (node.data.type == "procedure_list") {
        this.getProceduresMariadb(node);
      } else if (node.data.type == "procedure") {
        this.getProcedureFieldsMariadb(node);
      } else if (node.data.type == "role_list") {
        this.getRolesMariadb(node);
      }
    },
    getProperties(node) {
      const handledTypes = [
        "table",
        "sequence",
        "view",
        "function",
        "procedure",
      ];
      if (handledTypes.includes(node.data.type)) {
        getPropertiesNew("/get_properties_mariadb/", {
          schema: this.getParentNodeDeep(node, 2).title,
          table: null,
          object: node.title,
          type: node.data.type,
        });
      } else {
        clearProperties();
      }
    },
    getTreeDetailsMariadb(node) {
      axios
        .post("/get_tree_info_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: resp.data.version,
          });

          this.templates = resp.data;

          if (resp.data.superuser) {
            this.insertNode(node, "Roles", {
              icon: "fas node-all fa-users node-user-list",
              type: "role_list",
              contextMenu: "cm_roles",
            });
          }

          this.insertNode(node, "Databases", {
            icon: "fas node-all fa-database node-database-list",
            type: "database_list",
            contextMenu: "cm_databases",
          });
          this.contextMenu.cm_server = [this.cmRefreshObject];
          this.contextMenu.cm_server.push({
            label: "Monitoring",
            icon: "fas cm-all fa-chart-line",
            children: [
              {
                label: "Process List",
                icon: "fas cm-all fa-chart-line",
                onClick: () => {
                  window.v_connTabControl.tag.createMonitoringTab(
                    "Process List",
                    "select * from information_schema.processlist",
                    [
                      {
                        icon: "fas cm-all fa-times",
                        title: "Terminate",
                        action: "mariadbTerminateBackend",
                      },
                    ]
                  );
                },
              },
            ],
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getDatabasesMariadb(node) {
      axios
        .post("/get_databases_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
        })
        .then((resp) => {
          this.removeChildNodes(node);
          this.$refs.tree.updateNode(node.path, {
            title: `Databases (${resp.data.length})`,
          });

          resp.data.reduceRight((_, el) => {
            this.insertNode(node, el, {
              icon: "fas node-all fa-database node-database",
              type: "database",
              contextMenu: "cm_database",
            });
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getDatabaseObjectsMariadb(node) {
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
    getTablesMariadb(node) {
      axios
        .post("/get_tables_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          schema: this.getParentNode(node).title,
        })
        .then((resp) => {
          console.log(resp);
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
    getColumnsMariadb(node) {
      axios
        .post("/get_columns_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          schema: this.getParentNodeDeep(node, 2).title,
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
    getPKMariadb(node) {
      axios
        .post("/get_pk_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          table: this.getParentNode(node).title,
          schema: this.getParentNodeDeep(node, 3).title,
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
    getPKColumnsMariadb(node) {
      axios
        .post("/get_pk_columns_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          key: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: this.getParentNodeDeep(node, 4).title,
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
    getFKsMariadb(node) {
      axios
        .post("/get_fks_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          table: this.getParentNode(node).title,
          schema: this.getParentNodeDeep(node, 3).title,
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
    getFKsColumnsMariadb(node) {
      axios
        .post("/get_fks_columns_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          fkey: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: this.getParentNodeDeep(node, 4).title,
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
    getUniquesMariadb(node) {
      axios
        .post("/get_uniques_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          table: this.getParentNode(node).title,
          schema: this.getParentNodeDeep(node, 3).title,
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
    getUniquesColumnsMariadb(node) {
      axios
        .post("/get_uniques_columns_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          unique: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: this.getParentNodeDeep(node, 4).title,
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
    getIndexesMariadb(node) {
      axios
        .post("/get_indexes_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          table: this.getParentNode(node).title,
          schema: this.getParentNodeDeep(node, 3).title,
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
    getIndexesColumnsMariadb(node) {
      axios
        .post("/get_indexes_columns_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          index: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: this.getParentNodeDeep(node, 4).title,
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
    getSequencesMariadb(node) {
      axios
        .post("/get_sequences_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          schema: this.getParentNode(node).title,
        })
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
    getViewsMariadb(node) {
      axios
        .post("/get_views_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          schema: this.getParentNode(node).title,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `View (${resp.data.length})`,
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
    getViewsColumnsMariadb(node) {
      axios
        .post("/get_views_columns_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          schema: this.getParentNodeDeep(node, 2).title,
          table: node.title,
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
    getViewDefinitionMariadb(node) {
      axios
        .post("/get_view_definition_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          view: node.title,
          schema: this.getParentNodeDeep(node, 2).title,
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
    getFunctionsMariadb(node) {
      axios
        .post("/get_functions_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          schema: this.getParentNode(node).title,
        })
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
    getFunctionFieldsMariadb(node) {
      axios
        .post("/get_function_fields_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          schema: this.getParentNodeDeep(node, 2).title,
          function: node.data.id,
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
    getFunctionDefinitionMariadb(node) {
      axios
        .post("/get_function_definition_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
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
    getProceduresMariadb(node) {
      axios
        .post("/get_procedures_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          schema: this.getParentNode(node).title,
        })
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
    getProcedureFieldsMariadb(node) {
      axios
        .post("/get_procedure_fields_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
          schema: this.getParentNodeDeep(node, 2).title,
          procedure: node.data.id,
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
    getProcedureDefinitionMariadb(node) {
      axios
        .post("/get_procedure_definition_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
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
    getRolesMariadb(node) {
      axios
        .post("/get_roles_mariadb/", {
          database_index: this.databaseIndex,
          tab_id: this.tabId,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Roles (${resp.data.length})`,
          });

          resp.data.forEach((el) => {
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
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
  },
};
</script>
