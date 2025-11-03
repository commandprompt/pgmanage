import { emitter } from "../emitter";
import ContextMenu from "@imengyu/vue3-context-menu";
import axios from "axios";
import { tabsStore, settingsStore, connectionsStore } from "../stores/stores_initializer";
import { logger } from "../logging/logger_setup";
import { axiosHooks } from "../logging/service";
import { handleError } from "../logging/utils";
import { h } from "vue";
import debounce from "lodash/debounce";

let hoverTimer = null;

function delayedEnter(fn, delay = 220) {
  return (e) => {
    if (hoverTimer) clearTimeout(hoverTimer);

    hoverTimer = setTimeout(() => fn(e), delay);
  };
}

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
    const treeEl = document.getElementById(`${this.workspaceId}_tree`);
    if (treeEl) {
      treeEl.addEventListener('keydown', this.handleTreeKeyboardNavigation);
    }
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
        debounce((node) => {
          this.getProperties(node);
        }, 200)(node);
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
    showContextMenu(node, e) {
      ContextMenu.showContextMenu(
        {
          theme: "pgmanage",
          x: e.x,
          y: e.y,
          zIndex: 1000,
          minWidth: 230,
          items: this.contextMenu[node.data.contextMenu],
        },
        {
          itemRender: ({
            disabled,
            label,
            icon,
            showRightArrow,
            onClick,
            onMouseEnter,
          }) =>
            h(
              "div",
              {
                class: ["mx-context-menu-item", disabled ? "disabled" : ""],
                onClick,
                onMouseenter: delayedEnter(onMouseEnter, 200),
              },
              [
                h("div", { class: "mx-item-row" }, [
                  h("div", { class: "mx-icon-placeholder preserve-width" }, [
                    icon
                      ? h("i", { class: [icon, "icon",] })
                      : h("span", { class: "mx-content-menu-icon" }),
                  ]),
                  h("span", { class: "label" }, label),
                ]),
                showRightArrow
                  ? h("div", { class: "mx-item-row" }, [
                      h(
                        "svg",
                        {
                          class: "mx-right-arrow",
                          "aria-hidden": "true",
                          viewBox: "0 0 1024 1024",
                        },
                        [
                          h("path", {
                            d: "M307.018 49.445c11.517 0 23.032 4.394 31.819 13.18L756.404 480.18c8.439 8.438 13.181 19.885 13.181 31.82s-4.741 23.38-13.181 31.82L338.838 961.376c-17.574 17.573-46.065 17.573-63.64-0.001-17.573-17.573-17.573-46.065 0.001-63.64L660.944 512 275.198 126.265c-17.574-17.573-17.574-46.066-0.001-63.64C283.985 53.839 295.501 49.445 307.018 49.445z",
                          }),
                        ]
                      ),
                    ])
                  : null,
              ]
            ),
        }
      );
    },
    onContextMenu(node, e) {
      this.$refs.tree.select(node.path);
      e.preventDefault();
      if (!!node.data.contextMenu) {
        this.showContextMenu(node, e);
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
    async expandAndRefreshIfNeeded(node) {
      if (!node.children || node.children.length === 0) {
        await this.refreshTree(node, true);
        this.expandNode(node);
      } else {
        this.expandNode(node);
      }
    },
    handleTreeKeyboardNavigation(event) {
      const keyCode = event.code;
      const tree = this.$refs.tree;

      const selectedNode = tree.getSelected()[0];
      let nodeToSelect;

      if (keyCode === 'ArrowDown') {
        nodeToSelect = tree.getNextNode(selectedNode.path, node => node.isVisible);
      } else if (keyCode === 'ArrowUp') {
        nodeToSelect = tree.getPrevNode(selectedNode.path, node => node.isVisible);
      } else if (keyCode === 'Enter' || keyCode === 'Space' || keyCode === 'ArrowLeft' || keyCode === 'ArrowRight') {
        if (selectedNode.isLeaf) return;
        this.onToggle(selectedNode);
        this.toggleNode(selectedNode);
      } else if (keyCode === 'ContextMenu' || (event.shiftKey && keyCode === 'F10')) {
        event.preventDefault();
        const nodeEl = this.getNodeEl(selectedNode.path).querySelector('.vue-power-tree-title');
        const rect = nodeEl.getBoundingClientRect();

        const fakeEvent = new MouseEvent("contextmenu", {
          bubbles: true,
          clientX: rect.left + rect.width / 2,
          clientY: rect.top + rect.height / 2,
        });

        this.onContextMenu(selectedNode, fakeEvent);
        return;
      }

      if (!nodeToSelect) return;

      
      const nodeEl = this.getNodeEl(selectedNode.path).querySelector('.vue-power-tree-title');
      const container = tree.$el.parentElement

      if (nodeEl && container) {
        const nodeRect = nodeEl.getBoundingClientRect();
        const containerRect = container.getBoundingClientRect();
        const isVisible =
          nodeRect.top >= containerRect.top &&
          nodeRect.bottom <= containerRect.bottom;

        if (isVisible) {
          event.preventDefault(); // prevent page scroll since we’ll manage focus manually
        }
      }
      tree.select(nodeToSelect.path);
    },
  },
};
