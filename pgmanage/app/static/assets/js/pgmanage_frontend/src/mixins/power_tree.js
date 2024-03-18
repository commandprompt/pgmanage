import { emitter } from "../emitter";
import ContextMenu from "@imengyu/vue3-context-menu";
import axios from "axios";
import { showAlert, showToast } from "../notification_control";
import { tabsStore, settingsStore } from "../stores/stores_initializer";
import { logger } from "../logging/logger_setup";
import { axiosHooks } from "../logging/service";

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
        icon: "fas cm-all fa-sync-alt",
        onClick: this.refreshNode,
      };
    },
    selectedNode() {
      return this.getSelectedNode();
    },
  },
  beforeCreate() {
    this.id = Math.random().toString(16).slice(2);
  },
  mounted() {
    this.api = axios.create({
      transformRequest: [
        (data) => {
          const transformedData = {
            ...data,
            database_index: this.databaseIndex,
            tab_id: this.tabId,
          };
          return transformedData;
        },
        ...axios.defaults.transformRequest,
      ],
    });
    axiosHooks(logger, this.api)

    emitter.on(`refreshNode_${this.tabId}`, (e) => {
      this.refreshTree(e.node);
    });

    emitter.on(`removeNode_${this.tabId}`, (e) => {
      this.removeNode(e.node);
    });

    emitter.on(`refreshTreeRecursive_${this.tabId}`, (node_type) => {
      this.refreshTreeRecursive(node_type);
    });

  },
  unmounted() {
    emitter.all.delete(`refreshNode_${this.id}`);
    emitter.all.delete(`removeNode_${this.id}`);
    emitter.all.delete(`refreshTreeRecursive_${this.tabId}`)
  },
  methods: {
    onClickHandler(node, e) {
      if (this.getRootNode().title !== "Snippets") {
        this.getProperties(node);
      }
    },
    onToggle(node, e) {
      this.$refs.tree.select(node.path);
      if (node.isExpanded) return;
      this.refreshTree(node);
      if(settingsStore.scrollTree) {
        this.getNodeEl(node.path).scrollIntoView({
          block: "start",
          inline: "nearest",
          behavior: "smooth",
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
      this.refreshTree(node);
    },
    formatTitle(node) {
      if (node.data.uniqueness !== undefined) {
        return `${node.title} (${node.data.uniqueness})`;
      }
      return node.title;
    },
    removeNode(node) {
      this.$refs.tree.remove([node.path]);
    },
    nodeOpenError(error_response, node) {
      if (error_response.response.data?.password_timeout) {
        emitter.emit('show_password_prompt', {
          databaseIndex: this.databaseIndex,
          successCallback: () => {
            this.refreshNode();
          },
          message: error_response.response.data.data,
          kind: error_response.response.data.kind})
      } else {
        this.removeChildNodes(node);
        showToast("error", error_response.response.data.data);
      }
    },
    getRootNode() {
      return this.$refs.tree.getFirstNode();
    },
    refreshTreeRecursive(node_type) {
      const rootNode = this.getRootNode();
      const getInnerNode = (node, node_type) => {
        if (!!node.children.length) {
          if (node.data.type === node_type) {
            this.refreshTree(node);
            this.expandNode(node);
          }

          for (let i = 0; i < node.children.length; i++) {
            let childNode = node.children[i];

            if (childNode.data?.database === this.selectedDatabase) {
              if (
                childNode.data.type === "database" &&
                node_type === "extension_list"
              ) {
                this.refreshTree(childNode);

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
  },
};
