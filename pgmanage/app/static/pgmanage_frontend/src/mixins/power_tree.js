import { emitter } from "../emitter";
import ContextMenu from "@imengyu/vue3-context-menu";
import axios from "axios";
import { tabsStore, settingsStore, connectionsStore } from "../stores/stores_initializer";
import { logger } from "../logging/logger_setup";
import { axiosHooks } from "../logging/service";
import { handleError } from "../logging/utils";

export default {
  emits: ["treeTabsUpdate", "clearTabs"],
  data() {
    return {
      selectedDatabase: tabsStore.selectedPrimaryTab.metaData.selectedDatabase,
    };
  },
  computed: {
    cmRefreshObject() {
      return {
        label: "Refresh",
        icon: "fas fa-sync-alt",
        onClick: this.refreshNode,
      };
    },
    selectedNode() {
      return this.getSelectedNode();
    },
  },
  mounted() {
    this.api = axios.create({
      transformRequest: [
        (data) => {
          const transformedData = {
            ...data,
            database_index: this.databaseIndex,
            workspace_id: this.workspaceId,
          };
          return transformedData;
        },
        ...axios.defaults.transformRequest,
      ],
    });
    axiosHooks(logger, this.api)

    emitter.on(`refreshNode_${this.workspaceId}`, (e) => {
      this.refreshTree(e.node, true);
    });

    emitter.on(`removeNode_${this.workspaceId}`, (e) => {
      this.removeNode(e.node);
    });

    emitter.on(`refreshTreeRecursive_${this.workspaceId}`, (node_type) => {
      this.refreshTreeRecursive(node_type);
    });

    this.$watch("selectedNode", (newVal) => {
      if (newVal === undefined) {
        this.$emit("clearTabs");
      }
    });
  },
  unmounted() {
    emitter.all.delete(`refreshNode_${this.workspaceId}`);
    emitter.all.delete(`removeNode_${this.workspaceId}`);
    emitter.all.delete(`refreshTreeRecursive_${this.workspaceId}`)
  },
  methods: {
    onClickHandler(node, e) {
      if (this.getRootNode().title !== "Snippets") {
        this.getProperties(node);
      }
    },
    onToggle(node, e) {
      this.$refs.tree.select(node.path);
      if (this.getRootNode().title !== "Snippets") {
        this.getProperties(node);
      }
      if (node.isExpanded) return;
      this.refreshTree(node);
      if(settingsStore.scrollTree) {
        this.$nextTick(() => {
          this.scrollIntoViewIfPossible(node)
        })
      }
    },
    doubleClickNode(node, e) {
      if (node.isLeaf) return;
      this.onToggle(node);
      this.toggleNode(node);
    },
    onContextMenu(node, e) {
      this.$refs.tree.select(node.path);
      e.preventDefault();
      if (!!node.data.contextMenu) {
        ContextMenu.showContextMenu({
          theme: "pgmanage",
          x: e.x,
          y: e.y,
          zIndex: 1000,
          minWidth: 230,
          items: this.contextMenu[node.data.contextMenu],
        });
      }
    },
    removeChildNodes(node) {
      this.$refs.tree.updateNode(node.path, { children: [] });
    },
    insertSpinnerNode(node) {
      this.insertNode(
        node,
        "",
        {
          icon: "node-spin",
        },
        true
      );
    },
    insertNode(node, title, data, isLeaf = false) {
      this.$refs.tree.insert(
        { node: node, placement: "inside" },
        {
          title: title,
          isLeaf: isLeaf,
          isExpanded: false,
          isDraggable: false,
          data: {
            database: this.selectedDatabase,
            ...data,
          },
        }
      );
    },
    insertNodes(node, child_nodes) {
      this.$refs.tree.insert({ node: node, placement: "inside" }, child_nodes);
    },
    getParentNode(node) {
      const parentNode = this.$refs.tree.getNode(node.path.slice(0, -1));
      return parentNode;
    },
    getParentNodeDeep(node, depth = 1) {
      if (depth <= 0) {
        return node;
      }

      const parentNode = this.getParentNode(node);
      return this.getParentNodeDeep(parentNode, depth - 1);
    },
    getSelectedNode() {
      return this.$refs.tree.getSelected()[0];
    },
    getFirstChildNode(node) {
      const actualNode = this.$refs.tree.getNode(node.path);
      return actualNode.children[0];
    },
    getNodeEl(path) {
      return this.$refs.tree.$el.querySelector(
        `[path="${JSON.stringify(path)}"]`
      );
    },
    expandNode(node) {
      this.$refs.tree.updateNode(node.path, { isExpanded: true });
    },
    toggleNode(node) {
      this.$refs.tree.updateNode(node.path, { isExpanded: !node.isExpanded });
    },
    refreshNode() {
      const node = this.getSelectedNode();
      this.expandNode(node);
      this.refreshTree(node, true);
    },
    formatTitle(node) {
      if (node.data.unique !== undefined) {
        return `${node.title} (${node.data.unique})`;
      }
      return node.title;
    },
    removeNode(node) {
      this.$refs.tree.remove([node.path]);
    },
    nodeOpenError(error, node) {
      if (error?.response?.data?.password_timeout) {
        emitter.emit('show_password_prompt', {
          databaseIndex: this.databaseIndex,
          successCallback: () => {
            connectionsStore.queueChangeActiveDatabaseThreadSafe({
              database_index: this.databaseIndex,
              workspace_id: this.workspaceId,
              database: this.selectedDatabase,
            });

            this.refreshNode()
          },
          message: error.response.data.data,
          kind: error.response.data.kind})
      } else {
        this.removeChildNodes(node);
        handleError(error);
      }
    },
    getRootNode() {
      return this.$refs.tree.getFirstNode();
    },
    refreshTreeRecursive(node_type) {
      const rootNode = this.getRootNode();
      const getInnerNode = (node, node_type) => {
        if (node.data.type === node_type) {
          this.refreshTree(node, true);
          this.expandNode(node);
        }
        if (!!node.children.length) {

          for (let i = 0; i < node.children.length; i++) {
            let childNode = node.children[i];

            if (childNode.data?.database === this.selectedDatabase) {
              if (
                childNode.data.type === "database" &&
                node_type === "extension_list"
              ) {
                this.refreshTree(childNode, true);

                setTimeout(() => {
                  getInnerNode(childNode, node_type);
                }, 200);
              } else {
                getInnerNode(childNode, node_type);
              }
            }
          }
        }
      };

      for (let i = 0; i < rootNode.children.length; i++) {
        getInnerNode(rootNode.children[i], node_type);
      }
    },
    scrollIntoViewIfPossible(node) {
      const nodeElement = this.getNodeEl(node.path).querySelector('.vue-power-tree-title');
      const nodeRect = nodeElement.getBoundingClientRect();
      const parentElement = this.$refs.tree.$el.parentElement

      const scrollTop = parentElement.scrollTop;
      const scrollHeight = parentElement.scrollHeight;
      const clientHeight = parentElement.clientHeight;

      // Calculate the space needed to scroll the node to the top
      const spaceAvailableForScroll = scrollHeight - scrollTop - clientHeight;
      const spaceNeededForScroll = nodeRect.top - 40

      if (spaceNeededForScroll <= spaceAvailableForScroll) {
        nodeElement.scrollIntoView({
          block: "start",
          inline: "start",
          behavior: "smooth",
        });
      } 
    },
    shouldUpdateNode(node, force) {
      const now = new Date();
      if (!force && !!node?.data?.last_update) {
        const lastUpdateDate = new Date(node.data.last_update);
        const interval = (now - lastUpdateDate) / 1000;
        if (interval < 60) return false;
      }

      this.$refs.tree.updateNode(node.path, {
        data: { ...node.data, last_update: now.toISOString() },
      });
      return true;
    },
  },
};
