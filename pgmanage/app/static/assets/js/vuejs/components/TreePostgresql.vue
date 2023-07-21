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
      <span v-else-if="node.data.type === 'database' && node.title === selectedDatabase
        ">
        <b>{{ node.title }}</b>
      </span>
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
  name: "TreePostgresql",
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
          title: "PostgreSQL",
          isExpanded: false,
          isDraggable: false,
          data: {
            icon: "node-postgresql",
            type: "server",
            contextMenu: "cm_server",
          },
        },
      ],
      selectedDatabase:
        window.v_connTabControl.selectedTab.tag.selectedDatabase,
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
          {
            label: "Doc: Databases",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/managing-databases.html`
              );
            },
          },
        ],
        cm_database: [
          {
            label: "Alter Database",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              //FIXME rewrite to use vue instance
              tabSQLTemplate(
                "Alter Database",
                this.templates.alter_database.replace(
                  "#database_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              //FIXME: rewrite to use vue instance
              getObjectDescriptionPostgresql(node);
            },
          },
          {
            label: "Drop Database",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Database",
                this.templates.drop_database.replace(
                  "#database_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Backup",
            icon: "fa-solid fa-download cm-all",
            onClick: () => {
              createUtilityTab(this.selectedNode, "Backup");
            },
          },
          {
            label: "Restore",
            icon: "fa-solid fa-upload cm-all",
            onClick: () => {
              createUtilityTab(this.selectedNode, "Restore");
            },
          },
        ],
        cm_schemas: [
          this.cmRefreshObject,
          {
            label: "Create Schema",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate("Create Schema", this.templates.create_schema);
            },
          },
          {
            label: "Doc: Schemas",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/ddl-schemas.html`
              );
            },
          },
        ],
        cm_schema: [
          {
            label: "Render Graph",
            icon: "fab cm-all fa-hubspot",
            children: [
              {
                label: "Simple Graph",
                icon: "fab cm-all fa-hubspot",
                onClick: () => {
                  v_connTabControl.tag.createGraphTab(this.selectedNode.title);
                  drawGraph(false, this.selectedNode.title);
                },
              },
              {
                label: "Complete Graph",
                icon: "fab cm-all fa-hubspot",
                onClick: () => {
                  v_connTabControl.tag.createGraphTab(this.selectedNode.title);
                  drawGraph(true, this.selectedNode.title);
                },
              },
            ],
          },
          {
            label: "Backup",
            icon: "fa-solid fa-download cm-all",
            onClick: () => {
              createUtilityTab(this.selectedNode, "Backup");
            },
          },
          {
            label: "Restore",
            icon: "fa-solid fa-upload cm-all",
            onClick: () => {
              createUtilityTab(node, "Restore");
            },
          },
          {
            label: "Alter Schema",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              tabSQLTemplate(
                "Alter Schema",
                this.templates.alter_schema.replace(
                  "#schema_name#",
                  this.selectedNode.title
                )
              );
            },
          },
          {
            label: "Edit Comment",
            icon: "fas cm-all fa-edit",
            onClick: () => {
              //FIXME: rewrite to use vue instance
              getObjectDescriptionPostgresql(this.selectedNode);
            },
          },
          {
            label: "Drop Schema",
            icon: "fas cm-all fa-times",
            onClick: () => {
              tabSQLTemplate(
                "Drop Schema",
                this.templates.drop_schema.replace(
                  "#schema_name#",
                  this.selectedNode.title
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
              tabSQLTemplate(
                "Create Table",
                this.templates.create_table.replace(
                  "#schema_name#",
                  this.selectedNode.data.schema
                )
              );
            },
          },
          {
            label: "Doc: Basics",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/ddl-basics.html`
              );
            },
          },
          {
            label: "Doc: Constraints",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/ddl-constraints.html`
              );
            },
          },
          {
            label: "Doc: Modifying",
            icon: "fas cm-all fa-globe-americas",
            onClick: () => {
              this.openWebSite(
                `https://www.postgresql.org/docs/${this.getMajorVersion(
                  this.templates.version
                )}/static/ddl-alter.html`
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
                  TemplateSelectPostgresql(
                    this.selectedNode.data.schema,
                    this.selectedNode.title,
                    "t"
                  );
                },
              },
              {
                label: "Edit Data",
                icon: "fas cm-all fa-table",
                onClick: () => {
                  v_startEditData(
                    this.selectedNode.title,
                    this.selectedNode.data.schema
                  );
                },
              },
              {
                label: "Insert Record",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  TemplateInsertPostgresql(
                    this.selectedNode.data.schema,
                    this.selectedNode.title
                  );
                },
              },
              {
                label: "Update Records",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  TemplateUpdatePostgresql(
                    this.selectedNode.data.schema,
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
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
              {
                label: "Truncate Table",
                icon: "fas cm-all fa-cut",
                onClick: () => {
                  tabSQLTemplate(
                    "Truncate Table",
                    this.templates.truncate.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
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
                label: "Vacuum Table",
                icon: "fas cm-all fa-broom",
                onClick: () => {
                  tabSQLTemplate(
                    "Vacuum table",
                    this.templates.vacuum_table.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
              {
                label: "Analyze Table",
                icon: "fas cm-all fa-search-plus",
                onClick: () => {
                  tabSQLTemplate(
                    "Analyze Table",
                    this.templates.analyze_table.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
              {
                label: "Alter Table",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  tabSQLTemplate(
                    "Alter Table",
                    this.templates.alter_table.replace(
                      "#table_name#",
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
              {
                label: "Edit Comment",
                icon: "fas cm-all fa-edit",
                onClick: () => {
                  getObjectDescriptionPostgresql(this.selectedNode);
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
                      `${this.selectedNode.data.schema}.${this.selectedNode.title}`
                    )
                  );
                },
              },
              {
                label: "Backup",
                icon: "fa-solid fa-download cm-all",
                onClick: () => {
                  createUtilityTab(this.selectedNode, "Backup");
                },
              },
              {
                label: "Restore",
                icon: "fa-solid fa-upload cm-all",
                onClick: () => {
                  createUtilityTab(this.selectedNode, "Restore");
                },
              },
            ],
          },
        ],
      };
    },
  },
  methods: {
    refreshTreePostgresqlConfirm(node) {
      if (node.children.length == 0) this.insertSpinnerNode(node);
      if (node.data.type == "server") {
        this.getTreeDetailsPostgresql(node);
      } else if (node.data.type == "database_list") {
        this.getDatabasesPostgresql(node);
      } else if (node.data.type == "database") {
        this.getDatabaseObjectsPostgresql(node);
      } else if (node.data.type == "schema_list") {
        this.getSchemasPostgresql(node);
      } else if (node.data.type == "schema") {
        this.getSchemasObjectsPostgresql(node);
      } else if (node.data.type == "table_list") {
        this.getTablesPostgresql(node);
      } else if (node.data.type == "table") {
        this.getColumnsPostgresql(node);
      }
    },
    refreshTree(node) {
      this.checkCurrentDatabase(
        node,
        true,
        () => {
          this.refreshTreePostgresqlConfirm(node);
        },
        () => {
          this.toggleNode(node);
        }
      );
    },
    checkCurrentDatabase(
      node,
      complete_check,
      callback_continue,
      callback_stop
    ) {
      if (
        !!node.data.database &&
        node.data.database !== this.selectedDatabase &&
        (complete_check || (!complete_check && node.data.type !== "database"))
      ) {
        let isAllowed = checkBeforeChangeDatabase(callback_stop);
        if (isAllowed) {
          this.api
            .post("/change_active_database/", {
              database: node.data.database,
            })
            .then((resp) => {
              v_connTabControl.selectedTab.tag.divDetails.innerHTML = `<i class="fas fa-server mr-1"></i>selected DB: <b>${node.data.database}</b>`;
              const database_nodes = this.$refs.tree.getNode([0, 0]).children;

              database_nodes.forEach((el) => {
                if (node.data.database === el.title) {
                  this.selectedDatabase = node.data.database;
                  v_connTabControl.selectedTab.tag.selectedDatabase =
                    node.data.database;
                  v_connTabControl.selectedTab.tag.selectedDatabaseNode = el;

                  if (v_connTabControl.selectedTab.tag.selectedTitle != "")
                    v_connTabControl.selectedTab.tag.tabTitle.innerHTML = `<img src="${v_url_folder}/static/assets/images/${v_connTabControl.selectedTab.tag.selectedDBMS}_medium.png"/>${v_connTabControl.selectedTab.tag.selectedTitle} - ${this.selectedDatabase}`;
                  else
                    v_connTabControl.selectedTab.tag.tabTitle.innerHTML = `<img src="${v_url_folder}/static/assets/images/${v_connTabControl.selectedTab.tag.selectedDBMS}_medium.png"/>
                      ${this.selectedDatabase}`;
                }
              });
              if (callback_continue) callback_continue();
            })
            .catch((error) => {
              this.nodeOpenError(error, node);
            });
        }
      } else {
        callback_continue();
      }
    },
    getTreeDetailsPostgresql(node) {
      this.api
        .post("/get_tree_info_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.contextMenu.cm_server = [this.cmRefreshObject];
          this.contextMenu.cm_server.push(
            {
              label: "Server Configuration",
              icon: "fas cm-all fa-cog",
              onClick: () => {
                createConfTab();
              },
            },
            {
              label: "Backup Server",
              icon: "fa-solid fa-download cm-all",
              onClick: () => {
                createUtilityTab(this.selectedNode, "Backup", "server");
              },
            },
            {
              label: "Restore Server",
              icon: "fa-solid fa-upload cm-all",
              onClick: () => {
                createUtilityTab(this.selectedNode, "Restore", "server");
              },
            },
            {
              label: "Monitoring",
              icon: "fas cm-all fa-chart-line",
              children: [
                {
                  label: "Dashboard",
                  icon: "fas cm-all fa-chart-line",
                  onClick: () => {
                    v_connTabControl.tag.createMonitorDashboardTab();
                    startMonitorDashboard();
                  },
                },
                {
                  label: "Backends",
                  icon: "fas cm-all fa-tasks",
                  onClick: () => {
                    v_connTabControl.tag.createMonitoringTab(
                      "Backends",
                      "SELECT * FROM pg_stat_activity",
                      [
                        {
                          icon: "fas cm-all fa-times",
                          title: "Terminate",
                          action: "postgresqlTerminateBackend",
                        },
                      ]
                    );
                  },
                },
              ],
            },
            {
              label: "Doc: PostgreSQL",
              icon: "fas cm-all fa-globe-americas",
              onClick: () => {
                this.openWebSite(
                  `https://www.postgresql.org/docs/${this.getMajorVersion(
                    this.templates.version
                  )}/static/`
                );
              },
            },
            {
              label: "Doc: SQL Language",
              icon: "fas cm-all fa-globe-americas",
              onClick: () => {
                this.openWebSite(
                  `https://www.postgresql.org/docs/${this.getMajorVersion(
                    this.templates.version
                  )}/static/sql.html`
                );
              },
            },
            {
              label: "Doc: SQL Commands",
              icon: "fas cm-all fa-globe-americas",
              onClick: () => {
                this.openWebSite(
                  `https://www.postgresql.org/docs/${this.getMajorVersion(
                    this.templates.version
                  )}/static/sql-commands.html`
                );
              },
            }
          );

          this.templates = resp.data;

          this.$refs.tree.updateNode(node.path, {
            title: resp.data.version,
          });

          this.insertNode(node, "Replication Slots", {
            icon: "fas node-all fa-sitemap node-repslot-list",
            type: "replication",
          });

          const replication_node = this.getFirstChildNode(node);

          this.insertNode(replication_node, "Logical Replication Slots", {
            icon: "fas node-all fa-sitemap node-repslot-list",
            type: "logical_replication_slot_list",
            contextMenu: "cm_logical_replication_slots",
          });

          this.insertNode(replication_node, "Physical Replication Slots", {
            icon: "fas node-all fa-sitemap node-repslot-list",
            type: "physical_replication_slot_list",
            contextMenu: "cm_physical_replication_slots",
          });

          this.insertNode(node, "Roles", {
            icon: "fas node-all fa-users node-user-list",
            type: "role_list",
            contextMenu: "cm_roles",
          });

          this.insertNode(node, "Tablespaces", {
            icon: "fas node-all fa-folder-open node-tablespace-list",
            type: "tablespace_list",
            contextMenu: "cm_tablespace_list",
          });

          this.insertNode(node, "Databases", {
            icon: "fas node-all fa-database node-database-list",
            type: "database_list",
            contextMenu: "cm_databases",
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getDatabasesPostgresql(node) {
      this.api
        .post("/get_databases_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Databases (${resp.data.length})`,
          });

          resp.data.reduceRight((_, el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-database node-database",
              type: "database",
              contextMenu: "cm_database",
              database: el.name,
              oid: el.oid,
            });
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getDatabaseObjectsPostgresql(node) {
      this.api
        .post("/get_database_objects_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          if (resp.data.has_pg_cron) {
            this.insertNode(node, "Jobs", {
              icon: "fas node-all fa-clock",
              type: "job_list",
              contextMenu: "cm_jobs",
              database: this.selectedDatabase,
            });
          }

          this.insertNode(node, "Subscriptions", {
            icon: "fas node-all fa-arrow-alt-circle-up node-subscription-list",
            type: "subscription_list",
            contextMenu: "cm_subscriptions",
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Publications", {
            icon: "fas node-all fa-arrow-alt-circle-down node-publication-list",
            type: "publication_list",
            contextMenu: "cm_publications",
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Logical Replication", {
            icon: "fas node-all fa-sitemap node-logrep",
            type: "replication",
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Event Triggers", {
            icon: "fas node-all fa-bolt node-eventtrigger",
            type: "event_trigger_list",
            contextMenu: "cm_event_triggers",
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Foreign Data Wrappers", {
            icon: "fas node-all fa-cube node-fdw-list",
            type: "foreign_data_wrapper_list",
            contextMenu: "cm_foreign_data_wrappers",
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Extensions", {
            icon: "fas node-all fa-cubes node-extension-list",
            type: "extension_list",
            contextMenu: "cm_extensions",
            database: this.selectedDatabase,
          });

          this.insertNode(node, "Schemas", {
            icon: "fas node-all fa-layer-group node-schema-list",
            type: "schema_list",
            contextMenu: "cm_schemas",
            database: this.selectedDatabase,
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getSchemasPostgresql(node) {
      this.api
        .post("/get_schemas_postgresql/")
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Schemas (${resp.data.length})`,
          });

          resp.data.reduceRight((_, el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-layer-group node-schema",
              type: "schema",
              contextMenu: "cm_schema",
              database: this.selectedDatabase,
              schema: el.name,
              oid: el.oid,
            });
          });
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    getSchemasObjectsPostgresql(node) {
      this.removeChildNodes(node);

      this.insertNode(node, "Domains", {
        icon: "fas node-all fa-square node-domain-list",
        type: "domain_list",
        contextMenu: "cm_domains",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Types", {
        icon: "fas node-all fa-square node-type-list",
        type: "type_list",
        contextMenu: "cm_types",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Aggregates", {
        icon: "fas node-all fa-cog node-aggregate-list",
        type: "aggregate_list",
        contextMenu: "cm_aggregates",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Procedures", {
        icon: "fas node-all fa-cog node-procedure-list",
        type: "procedure_list",
        contextMenu: "cm_procedures",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Event Trigger Functions", {
        icon: "fas node-all fa-cog node-etfunction-list",
        type: "event_trigger_function_list",
        contextMenu: "cm_event_trigger_functions",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Trigger Functions", {
        icon: "fas node-all fa-cog node-tfunction-list",
        type: "trigger_function_list",
        contextMenu: "cm_trigger_functions",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Functions", {
        icon: "fas node-all fa-cog node-function-list",
        type: "function_list",
        contextMenu: "cm_functions",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Materialized Views", {
        icon: "fas node-all fa-eye node-mview-list",
        type: "mview_list",
        contextMenu: "cm_mviews",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Views", {
        icon: "fas node-all fa-eye node-view-list",
        type: "view_list",
        contextMenu: "cm_views",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Sequences", {
        icon: "fas node-all fa-sort-numeric-down node-sequence-list",
        type: "sequence_list",
        contextMenu: "cm_sequences",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Foreign Tables", {
        icon: "fas node-all fa-th node-ftable-list",
        type: "foreign_table_list",
        contextMenu: "cm_foreign_tables",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Inheritance Tables", {
        icon: "fas node-all fa-th node-itable-list",
        type: "inherited_table_list",
        contextMenu: "cm_inherited_tables",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Partitioned Tables", {
        icon: "fas node-all fa-th node-ptable-list",
        type: "partitioned_table_list",
        contextMenu: "cm_partitioned_tables",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });

      this.insertNode(node, "Tables", {
        icon: "fas node-all fa-th node-table-list",
        type: "table_list",
        contextMenu: "cm_tables",
        database: this.selectedDatabase,
        schema: node.data.schema,
      });
    },
    getTablesPostgresql(node) {
      this.api
        .post("/get_tables_postgresql/", {
          schema: node.data.schema,
        })
        .then((resp) => {
          this.removeChildNodes(node);

          this.$refs.tree.updateNode(node.path, {
            title: `Tables (${resp.data.length})`,
          });

          resp.data.reduceRight((_, el) => {
            this.insertNode(node, el.name, {
              icon: "fas node-all fa-table node-table",
              type: "table",
              contextMenu: "cm_table",
              schema: node.data.schema,
              database: this.selectedDatabase,
              oid: el.oid,
            });
          }, null);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    openWebSite(site) {
      window.open(site, "_blank");
    },
    getMajorVersion(version) {
      // FIXME
      let v_version = version.split(" (")[0];
      let tmp = v_version
        .replace("PostgreSQL ", "")
        .replace("beta", ".")
        .replace("rc", ".")
        .split(".");
      tmp.pop();
      return tmp.join(".");
    },
  },
};
</script>
