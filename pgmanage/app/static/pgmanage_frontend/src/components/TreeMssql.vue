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
import { PowerTree } from "@onekiloparsec/vue-power-tree";

export default {
  name: "TreeMssql",
  components: {
    PowerTree: PowerTree,
  },
  mixins: [TreeMixin],
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
  mounted() {},
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
  },
};
</script>
