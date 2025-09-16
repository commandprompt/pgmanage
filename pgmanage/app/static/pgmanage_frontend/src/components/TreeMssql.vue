<template>
  <PowerTree
    ref="tree"
    v-model="nodes"
    @nodedblclick="doubleClickNode"
    @toggle="onToggle"
    @nodecontextmenu="onContextMenu"
    :allow-multiselect="false"
    @nodeclick="onClickHandler"
  >
    <template v-slot:toggle="{ node }">
      <i v-if="node.isExpanded" class="exp_col fas fa-chevron-down"></i>
      <i v-if="!node.isExpanded" class="exp_col fas fa-chevron-right"></i>
    </template>

    <template v-slot:title="{ node }">
      <div class="d-flex flex-grow-1 justify-content-between">
        <div>
          <span class="item-icon">
            <i :class="['icon_tree', node.data.icon]"></i>
          </span>

          <span v-if="node.data.raw_html" v-html="node.title"> </span>
          <span
            v-else-if="
              node.data.type === 'database' && node.title === selectedDatabase
            "
          >
            <b>{{ node.title }}</b>
          </span>
          <span v-else>
            {{ formatTitle(node) }}
          </span>
        </div>

        <!-- Pin icon for database nodes -->
        <span>
          <i
            v-if="node.data.type === 'database'"
            class="fas fa-thumbtack database-pin-icon me-2"
            :class="node.data.pinned ? 'text-primary pinned' : 'text-muted'"
            @click.stop="pinDatabase(node)"
            title="Pin this database"
          ></i>
        </span>
      </div>
    </template>
  </PowerTree>
</template>

<script>
import TreeMixin from "../mixins/power_tree.js";
import PinDatabaseMixin from "../mixins/power_tree_pin_database_mixin.js";
import { PowerTree } from "@onekiloparsec/vue-power-tree";

import { checkBeforeChangeDatabase } from "../workspace";
import {
  connectionsStore,
  tabsStore,
  dbMetadataStore,
} from "../stores/stores_initializer";

import { TemplateSelectMssql } from "../tree_context_functions/tree_mssql";

export default {
  name: "TreeMssql",
  components: {
    PowerTree: PowerTree,
  },
  mixins: [TreeMixin, PinDatabaseMixin],
  props: {
    databaseIndex: {
      type: Number,
      required: true,
    },
    workspaceId: {
      type: String,
      required: true,
    },
  },
  data() {
    return {
      nodes: [
        {
          title: "Mssql",
          isExpanded: false,
          isDraggable: false,
          data: {
            icon: "node node-mssql",
            type: "server",
            contextMenu: "cm_server",
          },
        },
      ],
      serverVersion: null,
    };
  },
  computed: {
    contextMenu() {
      return {
        cm_server: [this.cmRefreshObject],
        cm_table: [
          this.cmRefreshObject,
          {
            label: "Query Data",
            icon: "fas fa-search",
            onClick: () => {
              TemplateSelectMssql(
                this.selectedNode.data.schema,
                this.selectedNode.title,
                "t"
              );
            },
          },
        ],
        cm_views: [
          this.cmRefreshObject,
          // {
          //   label: "Create View",
          //   icon: "fas fa-plus",
          //   onClick: () => {
          //     tabSQLTemplate(
          //       "Create View",
          //       this.templates.create_view.replace(
          //         "#schema_name#",
          //         this.selectedNode.data.schema_raw
          //       )
          //     );
          //   },
          // },
          // {
          //   label: "Doc: Views",
          //   icon: "fas fa-globe-americas",
          //   onClick: () => {
          //     this.openWebSite(
          //       `https://www.postgresql.org/docs/${this.serverVersion}/sql-createview.html`
          //     );
          //   },
          // },
        ],
        cm_view: [
          this.cmRefreshObject,
          {
            label: "Query Data",
            icon: "fas fa-search",
            onClick: () => {
              TemplateSelectMssql(
                this.selectedNode.data.schema,
                this.selectedNode.title,
                "v"
              );
            },
          },
          // {
          //   label: "Edit View",
          //   icon: "fas fa-edit",
          //   onClick: () => {
          //     this.getViewDefinitionPostgresql(this.selectedNode);
          //   },
          // },
          // {
          //   label: "Alter View",
          //   icon: "fas fa-edit",
          //   onClick: () => {
          //     tabSQLTemplate(
          //       "Alter View",
          //       this.templates.alter_view.replace(
          //         /#view_name#/g,
          //         `${this.selectedNode.data.schema_raw}.${this.selectedNode.data.raw_value}`
          //       )
          //     );
          //   },
          // },
        ],
      };
    },
  },
  mounted() {
    this.$hooks?.add_tree_context_menu_item?.forEach((hook) => {
      hook.call(this);
    });
    this.doubleClickNode(this.getRootNode());
  },
  unmounted() {},
  methods: {
    refreshTree(node, force = false) {
      return new Promise((resolve, reject) => {
        this.checkCurrentDatabase(
          node,
          true,
          () => {
            setTimeout(() => {
              if (!this.shouldUpdateNode(node, force)) {
                resolve();
                return;
              }

              if (node.children.length === 0) this.insertSpinnerNode(node);

              this.refreshTreeConfirm(node)
                .then(() => {
                  this.$hooks?.add_tree_node_item?.forEach((hook) => {
                    hook(node);
                  });
                  resolve();
                })
                .catch((error) => {
                  this.nodeOpenError(error, node);
                  reject(error);
                });
            }, 100);
          },
          () => {
            this.toggleNode(node);
            resolve();
          }
        );
      });
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
              dbMetadataStore.fetchDbMeta(
                this.databaseIndex,
                this.workspaceId,
                node.data.database
              );
              connectionsStore.updateConnection(this.databaseIndex, {
                last_used_database: node.data.database,
              });
              const database_nodes = this.$refs.tree.getNode([0, 0]).children;

              database_nodes.forEach((el) => {
                if (node.data.database === el.title) {
                  this.selectedDatabase = node.data.database;
                  tabsStore.selectedPrimaryTab.metaData.selectedDatabase =
                    node.data.database;
                }
              });
              if (callback_continue) callback_continue();
            })
            .catch((error) => {
              this.nodeOpenError(error, node);
            });
        }
      } else {
        if (callback_continue) callback_continue();
      }
    },
    refreshTreeConfirm(node) {
      if (node.data.type == "server") {
        return this.getTreeDetails(node);
      } else if (node.data.type == "database_list") {
        return this.getDatabases(node);
      } else if (node.data.type == "database") {
        return this.getDatabaseObjects(node);
      } else if (node.data.type == "schema_list") {
        return this.getSchemas(node);
      } else if (node.data.type == "schema") {
        return this.getSchemasObjects(node);
      } else if (node.data.type == "table_list") {
        return this.getTables(node);
      } else if (node.data.type == "table") {
        return this.getColumns(node);
      } else if (node.data.type == "primary_keys") {
        return this.getPKs(node);
      } else if (node.data.type == "primary_key") {
        return this.getPKsColumns(node);
      } else if (node.data.type == "foreign_keys") {
        return this.getFKs(node);
      } else if (node.data.type == "foreign_key") {
        return this.getFKsColumns(node);
      } else if (node.data.type == "uniques") {
        return this.getUniques(node);
      } else if (node.data.type == "unique") {
        return this.getUniquesColumns(node);
      } else if (node.data.type == "check_list") {
        return this.getChecks(node);
      } else if (node.data.type == "indexes") {
        return this.getIndexes(node);
      } else if (node.data.type == "index") {
        return this.getIndexesColumns(node);
      } else if (node.data.type == "trigger_list") {
        return this.getTriggers(node);
      } else if (node.data.type == "statistics_list") {
        return this.getStatistics(node);
      } else if (node.data.type == "view_list") {
        return this.getViews(node);
      } else if (node.data.type == "view") {
        return this.getViewsColumns(node);
      } else if (node.data.type == "procedure_list") {
        return this.getProcedures(node);
      } else if (node.data.type == "procedure") {
        return this.getProcedureFields(node);
      } else if (node.data.type == "function_list") {
        return this.getFunctions(node);
      } else if (node.data.type == "function") {
        return this.getFunctionFields(node);
      } else if (node.data.type == "server_role_list") {
        return this.getServerRoles(node);
      } else if (node.data.type == "logins_list") {
        return this.getLogins(node);
      } else if (node.data.type == "users_list") {
        return this.getUsers(node);
      } else if (node.data.type == "database_role_list") {
        return this.getDatabaseRoles(node);
      } else {
        return Promise.resolve("success");
      }
    },
    async getTreeDetails(node) {
      try {
        const response = await this.api.post("/get_tree_info_mssql/");
        this.removeChildNodes(node);

        this.serverVersion = response.data.version;

        this.$refs.tree.updateNode(node.path, {
          title: response.data.version,
        });

        this.insertNode(node, "Server Roles", {
          icon: "fas node-all fa-users node-user-list",
          type: "server_role_list",
          contextMenu: "cm_server_roles",
          database: false,
        });

        this.insertNode(node, "Logins", {
          icon: "fas node-all fa-users node-user-list",
          type: "logins_list",
          contextMenu: "cm_logins",
          database: false,
        });
        this.insertNode(node, "Databases", {
          icon: "fas node-all fa-database node-database-list",
          type: "database_list",
          contextMenu: "cm_databases",
          database: false,
        });
      } catch (error) {
        throw error;
      }
    },
    async getDatabases(node) {
      try {
        const response = await this.api.post("/get_databases_mssql/");

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Databases (${response.data.length})`,
        });

        response.data.reduceRight((_, el) => {
          this.insertNode(node, el.name, {
            icon: "fas node-all fa-database node-database",
            type: "database",
            contextMenu: "cm_database",
            database: el.name,
            database_id: el.database_id,
            pinned: el.pinned,
          });
        }, null);
        const databasesNode = this.$refs.tree.getNode(node.path);
        this.sortPinnedNodes(databasesNode);
      } catch (error) {
        throw error;
      }
    },
    async getDatabaseObjects(node) {
      this.removeChildNodes(node);
      return new Promise((resolve, reject) => {
        try {
          this.insertNode(node, "Database Roles", {
            icon: "fas node-all fa-users node-user-list",
            type: "database_role_list",
            contextMenu: "cm_database_roles",
            database: false,
          });

          this.insertNode(node, "Users", {
            icon: "fas node-all fa-users node-user-list",
            type: "users_list",
            contextMenu: "cm_users",
            database: false,
          });
          this.insertNode(node, "Schemas", {
            icon: "fas node-all fa-layer-group node-schema-list",
            type: "schema_list",
            contextMenu: "cm_schemas",
          });
          resolve("success");
        } catch (error) {
          reject(error);
        }
      });
    },
    async getSchemas(node) {
      try {
        const response = await this.api.post("/get_schemas_mssql/");

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Schemas (${response.data.length})`,
        });

        response.data.reduceRight((_, el) => {
          this.insertNode(node, el.name, {
            icon: "fas node-all fa-layer-group node-schema",
            type: "schema",
            contextMenu: "cm_schema",
            schema: el.name,
          });
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getSchemasObjects(node) {
      this.removeChildNodes(node);
      return new Promise((resolve, reject) => {
        try {
          this.insertNode(node, "Procedures", {
            icon: "fas node-all fa-cog node-procedure-list",
            type: "procedure_list",
            contextMenu: "cm_procedures",
            schema: node.data.schema,
          });

          this.insertNode(node, "Functions", {
            icon: "fas node-all fa-cog node-function-list",
            type: "function_list",
            contextMenu: "cm_functions",
            schema: node.data.schema,
          });

          this.insertNode(node, "Views", {
            icon: "fas node-all fa-eye node-view-list",
            type: "view_list",
            contextMenu: "cm_views",
            schema: node.data.schema,
          });

          this.insertNode(node, "Tables", {
            icon: "fas node-all fa-th node-table-list",
            type: "table_list",
            contextMenu: "cm_tables",
            schema: node.data.schema,
          });
          resolve("success");
        } catch (error) {
          reject(error);
        }
      });
    },
    async getTables(node) {
      try {
        const response = await this.api.post("/get_tables_mssql/", {
          schema: node.data.schema,
        });
        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Tables (${response.data.length})`,
        });

        response.data.reduceRight((_, el) => {
          this.insertNode(node, el, {
            icon: "fas node-all fa-table node-table",
            type: "table",
            contextMenu: "cm_table",
            database: node.data.database,
            schema: node.data.schema,
          });
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getColumns(node) {
      try {
        const response = await this.api.post("/get_columns_mssql/", {
          table: node.title,
          schema: node.data.schema,
        });
        this.removeChildNodes(node);

        this.insertNode(node, "Statistics", {
          icon: "fas node-all fa-chart-bar node-statistics",
          type: "statistics_list",
          contextMenu: "cm_statistics",
          schema: node.data.schema,
        });

        //TODO: add proper partition handling

        // this.insertNode(node, "Partitions", {
        //   icon: "fas node-all fa-table node-partition",
        //   type: "partition_list",
        //   contextMenu: "cm_partitions",
        //   schema: node.data.schema,
        // });

        this.insertNode(node, "Triggers", {
          icon: "fas node-all fa-bolt node-trigger",
          type: "trigger_list",
          contextMenu: "cm_triggers",
          schema: node.data.schema,
        });

        this.insertNode(node, "Indexes", {
          icon: "fas node-all fa-thumbtack node-index",
          type: "indexes",
          contextMenu: "cm_indexes",
          schema: node.data.schema,
        });

        this.insertNode(node, "Checks", {
          icon: "fas node-all fa-check-square node-check",
          type: "check_list",
          contextMenu: "cm_checks",
          schema: node.data.schema,
        });

        this.insertNode(node, "Uniques", {
          icon: "fas node-all fa-key node-unique",
          type: "uniques",
          contextMenu: "cm_uniques",
          schema: node.data.schema,
        });

        this.insertNode(node, "Foreign Keys", {
          icon: "fas node-all fa-key node-fkey",
          type: "foreign_keys",
          contextMenu: "cm_fks",
          schema: node.data.schema,
        });

        this.insertNode(node, "Primary Key", {
          icon: "fas node-all fa-key node-pkey",
          type: "primary_keys",
          contextMenu: "cm_pks",
          schema: node.data.schema,
        });

        this.insertNode(node, `Columns (${response.data.length})`, {
          icon: "fas node-all fa-columns node-column",
          type: "column_list",
          contextMenu: "cm_columns",
          schema: node.data.schema,
        });

        const columns_node = this.getFirstChildNode(node);

        response.data.reduceRight((_, el) => {
          this.insertNode(
            columns_node,
            el.column_name,
            {
              icon: "fas node-all fa-columns node-column",
              type: "table_field",
              contextMenu: "cm_column",
              schema: node.data.schema,
              position: el.position,
            },
            null
          );
          const table_field = this.getFirstChildNode(columns_node);

          this.insertNode(
            table_field,
            `Nullable: ${el.nullable}`,
            {
              icon: "fas node-all fa-ellipsis-h node-bullet",
              schema: node.data.schema,
            },
            true
          );
          this.insertNode(
            table_field,
            `Type: ${el.data_type}`,
            {
              icon: "fas node-all fa-ellipsis-h node-bullet",
              schema: node.data.schema,
            },
            true
          );
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getPKs(node) {
      try {
        const response = await this.api.post("/get_pk_mssql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Primary Key (${response.data.length})`,
        });

        response.data.forEach((el) => {
          this.insertNode(node, el.constraint_name, {
            icon: "fas node-all fa-key node-pkey",
            type: "primary_key",
            contextMenu: "cm_pk",
            schema: node.data.schema,
          });
        });
      } catch (error) {
        throw error;
      }
    },
    async getPKsColumns(node) {
      try {
        const response = await this.api.post("/get_pk_columns_mssql/", {
          key: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        response.data.forEach((el) => {
          this.insertNode(
            node,
            el,
            {
              icon: "fas node-all fa-columns node-column",
              schema: node.data.schema,
            },
            true
          );
        });
      } catch (error) {
        throw error;
      }
    },
    async getFKs(node) {
      try {
        const response = await this.api.post("/get_fks_mssql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        });
        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Foreign Keys (${response.data.length})`,
        });

        response.data.reduceRight((_, el) => {
          this.insertNode(node, el.constraint_name, {
            icon: "fas node-all fa-key node-fkey",
            type: "foreign_key",
            contextMenu: "cm_fk",
            schema: node.data.schema,
          });
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getFKsColumns(node) {
      try {
        const response = await this.api.post("/get_fks_columns_mssql/", {
          fkey: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: node.data.schema,
        });
        this.removeChildNodes(node);

        response.data.forEach((el) => {
          this.insertNode(
            node,
            `${el.column_name} <i class='fas node-all fa-arrow-right'></i> ${el.r_column_name}`,
            {
              icon: "fas node-all fa-columns node-column",
              raw_html: true,
              schema: node.data.schema,
            },
            true
          );
          this.insertNode(
            node,
            `Update Rule: ${el.update_rule}`,
            {
              icon: "fas node-all fa-ellipsis-h node-bullet",
              schema: node.data.schema,
            },
            true
          );
          this.insertNode(
            node,
            `Delete Rule: ${el.delete_rule}`,
            {
              icon: "fas node-all fa-ellipsis-h node-bullet",
              schema: node.data.schema,
            },
            true
          );
          this.insertNode(
            node,
            `Referenced Table: ${el.r_table_name}`,
            {
              icon: "fas node-all fa-table node-table",
              schema: node.data.schema,
            },
            true
          );
        });
      } catch (error) {
        throw error;
      }
    },
    async getUniques(node) {
      try {
        const response = await this.api.post("/get_uniques_mssql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        });
        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Uniques (${response.data.length})`,
        });

        response.data.forEach((el) => {
          this.insertNode(node, el.constraint_name, {
            icon: "fas node-all fa-key node-unique",
            type: "unique",
            contextMenu: "cm_unique",
            schema: node.data.schema,
          });
        });
      } catch (error) {
        throw error;
      }
    },
    async getUniquesColumns(node) {
      try {
        const response = await this.api.post("/get_uniques_columns_mssql/", {
          unique: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: node.data.schema,
        });
        this.removeChildNodes(node);

        response.data.forEach((el) => {
          this.insertNode(
            node,
            el,
            {
              icon: "fas node-all fa-columns node-column",
              schema: node.data.schema,
            },
            true
          );
        });
      } catch (error) {
        throw error;
      }
    },
    async getChecks(node) {
      try {
        const response = await this.api.post("/get_checks_mssql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Checks (${response.data.length})`,
        });

        response.data.forEach((el) => {
          this.insertNode(node, el.constraint_name, {
            icon: "fas node-all fa-check-square node-check",
            type: "check",
            contextMenu: "cm_check",
            schema: node.data.schema,
          });

          const check_node = this.getFirstChildNode(node);

          this.insertNode(
            check_node,
            el.check_clause,
            {
              icon: "fas node-all fa-edit node-check-value",
              schema: node.data.schema,
            },
            true
          );
        });
      } catch (error) {
        throw error;
      }
    },
    async getIndexes(node) {
      try {
        const response = await this.api.post("/get_indexes_mssql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Indexes (${response.data.length})`,
        });

        response.data.forEach((el) => {
          this.insertNode(node, el.index_name, {
            icon: "fas node-all fa-thumbtack node-index",
            type: "index",
            contextMenu: "cm_index",
            schema: node.data.schema,
            unique: el.unique ? "Unique" : "Non unique",
          });
        });
      } catch (error) {
        throw error;
      }
    },
    async getIndexesColumns(node) {
      try {
        const response = await this.api.post("/get_indexes_columns_mssql/", {
          index: node.title,
          table: this.getParentNodeDeep(node, 2).title,
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        response.data.forEach((el) => {
          this.insertNode(
            node,
            el,
            {
              icon: "fas node-all fa-columns node-column",
              schema: node.data.schema,
            },
            true
          );
        });
      } catch (error) {
        throw error;
      }
    },
    async getTriggers(node) {
      try {
        const response = await this.api.post("/get_triggers_mssql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Triggers (${response.data.length})`,
        });

        response.data.forEach((el) => {
          this.insertNode(node, el.trigger_name, {
            icon: "fas node-all fa-bolt node-trigger",
            type: "trigger",
            contextMenu: "cm_trigger",
            schema: node.data.schema,
          });

          const trigger_node = this.getFirstChildNode(node);

          this.insertNode(
            trigger_node,
            `Enabled: ${el.enabled}`,
            {
              icon: "fas node-all fa-ellipsis-h node-bullet",
              schema: node.data.schema,
            },
            true
          );
        });
      } catch (error) {
        throw error;
      }
    },
    async getStatistics(node) {
      try {
        const response = await this.api.post("/get_statistics_mssql/", {
          table: this.getParentNode(node).title,
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Statistics (${response.data.length})`,
        });

        response.data.forEach((el) => {
          this.insertNode(
            node,
            el.statistic_name,
            {
              icon: "fas node-all fa-chart-bar node-statistic",
              type: "statistic",
              contextMenu: "cm_statistic",
              schema: el.schema_name,
              statistics: el.statistic_name,
            },
            true
          );
        });
      } catch (error) {
        throw error;
      }
    },
    async getViews(node) {
      try {
        const response = await this.api.post("/get_views_mssql/", {
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Views (${response.data.length})`,
        });

        response.data.reduceRight((_, el) => {
          this.insertNode(node, el.name, {
            icon: "fas node-all fa-eye node-view",
            type: "view",
            contextMenu: "cm_view",
            schema: node.data.schema,
          });
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getViewsColumns(node) {
      try {
        const response = await this.api.post("/get_views_columns_mssql/", {
          table: node.title,
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        this.insertNode(node, "Triggers", {
          icon: "fas node-all fa-bolt node-trigger",
          type: "trigger_list",
          contextMenu: "cm_view_triggers",
          schema: node.data.schema,
        });

        this.insertNode(node, `Columns (${response.data.length})`, {
          icon: "fas node-all fa-columns node-column",
          schema: node.data.schema,
        });

        const columns_node = this.getFirstChildNode(node);

        response.data.reduceRight((_, el) => {
          this.insertNode(
            columns_node,
            el.column_name,
            {
              icon: "fas node-all fa-columns node-column",
              type: "table_field",
              schema: node.data.schema,
            },
            null
          );
          const table_field = this.getFirstChildNode(columns_node);

          this.insertNode(
            table_field,
            `Type: ${el.data_type}`,
            {
              icon: "fas node-all fa-ellipsis-h node-bullet",
              schema: node.data.schema,
            },
            true
          );
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getProcedures(node) {
      try {
        const response = await this.api.post("/get_procedures_mssql/", {
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Procedures (${response.data.length})`,
        });

        response.data.reduceRight((_, el) => {
          this.insertNode(node, el.name, {
            icon: "fas node-all fa-cog node-procedure",
            type: "procedure",
            contextMenu: "cm_procedure",
            schema: node.data.schema,
            id: el.oid,
          });
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getProcedureFields(node) {
      try {
        const response = await this.api.post("/get_procedure_fields_mssql/", {
          procedure: node.title,
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        response.data.reduceRight((_, el) => {
          if (el.type === "O") {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-arrow-right node-function-field",
                schema: node.data.schema,
              },
              true
            );
          } else if (el.type === "I") {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-arrow-left node-function-field",
                schema: node.data.schema,
              },
              true
            );
          } else {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-exchange-alt node-function-field",
                schema: node.data.schema,
              },
              true
            );
          }
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getFunctions(node) {
      try {
        const response = await this.api.post("/get_functions_mssql/", {
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Functions (${response.data.length})`,
        });

        let childNodes = response.data.map((el) => {
          return {
            title: el.name,
            isLeaf: false,
            isExpanded: false,
            isDraggable: false,
            data: {
              database: this.selectedDatabase,
              icon: "fas node-all fa-cog node-function",
              type: "function",
              contextMenu: "cm_function",
              schema: node.data.schema,
            },
          };
        });

        this.insertNodes(node, childNodes);
      } catch (error) {
        throw error;
      }
    },
    async getFunctionFields(node) {
      try {
        const response = await this.api.post("/get_function_fields_mssql/", {
          function: node.title,
          schema: node.data.schema,
        });

        this.removeChildNodes(node);

        response.data.reduceRight((_, el) => {
          if (el.type === "O") {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-arrow-right node-function-field",
                schema: node.data.schema,
              },
              true
            );
          } else if (el.type === "I") {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-arrow-left node-function-field",
                schema: node.data.schema,
              },
              true
            );
          } else {
            this.insertNode(
              node,
              el.name,
              {
                icon: "fas node-all fa-exchange-alt node-function-field",
                schema: node.data.schema,
              },
              true
            );
          }
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getServerRoles(node) {
      try {
        const response = await this.api.post("/get_server_roles_mssql/");

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Server Roles (${response.data.data.length})`,
        });

        response.data.data.reduceRight((_, el) => {
          this.insertNode(
            node,
            el.name,
            {
              icon: "fas node-all fa-user node-user",
              type: "server_role",
              contextMenu: "cm_server_role",
            },
            true
          );
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getDatabaseRoles(node) {
      try {
        const response = await this.api.post("/get_database_roles_mssql/");

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Database Roles (${response.data.data.length})`,
        });

        response.data.data.reduceRight((_, el) => {
          this.insertNode(
            node,
            el.name,
            {
              icon: "fas node-all fa-user node-user",
              type: "database_role",
              contextMenu: "cm_database_role",
            },
            true
          );
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getLogins(node) {
      try {
        const response = await this.api.post("/get_logins_mssql/");

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Logins (${response.data.data.length})`,
        });

        response.data.data.reduceRight((_, el) => {
          this.insertNode(
            node,
            el.name,
            {
              icon: "fas node-all fa-user node-user",
              type: "login",
              contextMenu: "cm_login",
            },
            true
          );
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getUsers(node) {
      try {
        const response = await this.api.post("/get_users_mssql/");

        this.removeChildNodes(node);

        this.$refs.tree.updateNode(node.path, {
          title: `Users (${response.data.data.length})`,
        });

        response.data.data.reduceRight((_, el) => {
          this.insertNode(
            node,
            el.name,
            {
              icon: "fas node-all fa-user node-user",
              type: "user",
              contextMenu: "cm_user",
            },
            true
          );
        }, null);
      } catch (error) {
        throw error;
      }
    },
    getProperties(node) {
      console.log("Not implemented");
    },
  },
};
</script>
