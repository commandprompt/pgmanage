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
import PinDatabaseMixin from "../mixins/power_tree_pin_database_mixin.js";
import { PowerTree } from "@onekiloparsec/vue-power-tree";

import { checkBeforeChangeDatabase } from "../workspace";
import {
  connectionsStore,
  tabsStore,
  dbMetadataStore,
} from "../stores/stores_initializer";

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
          });
        }, null);
      } catch (error) {
        throw error;
      }
    },
    async getColumns(node) {
      console.log("Not implemented");
    },
    getProperties(node) {
      console.log("Not implemented");
    },
  },
};
</script>
