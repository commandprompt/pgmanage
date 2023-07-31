export default {
  data() {
    return {
      emitter: emitter,
      selectedDatabase: window.v_connTabControl.selectedTab.tag.selectedDatabase,
    }
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
      return this.getSelectedNode()
    }
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

    this.api.interceptors.response.use(response => {
      return response;
    }, error => {
      if (error.response.status === 401) {
        showAlert('User not authenticated, please reload the page.');
      }
      return Promise.reject(error);
    });

    this.$toast = VueToast.useToast();

    this.emitter.on('refreshNode', (e) => {
      this.refreshTree(e.node);
    })

    this.emitter.on('removeNode', (e) => {
      this.removeNode(e.node)
    })

    // Temporary solution, use Pinia store later
    v_connTabControl.selectedTab.tag.tree = this
  },
  methods: {
    onClickHandler(node, e) {
      // fix this not to use window
      if (window.v_connTabControl.selectedTab.tag.treeTabsVisible)
        this.getProperties(node);

      if (node.data.type == "error") {
        showError(node.data.message);
      }
    },
    onToggle(node, e) {
      this.$refs.tree.select(node.path);
      if (node.isExpanded) return;
      this.refreshTree(node);
      this.getNodeEl(node.path).scrollIntoView({
        block: "start",
        inline: "nearest",
        behavior: "smooth",
      });
    },
    doubleClickNode(node, e) {
      if (node.isLeaf) return;
      this.onToggle(node);
      this.toggleNode(node);
    },
    onContextMenu(node, e) {
      this.$refs.tree.select(node.path);
      e.preventDefault();
      window["vue3-context-menu"].default.showContextMenu({
        theme: "pgmanage",
        x: e.x,
        y: e.y,
        zIndex: 1000,
        minWidth: 230,
        items: this.contextMenu[node.data.contextMenu],
      });
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
            database: this.selectedDatabase, ...data
          }
        }
      );
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
      this.$refs.tree.remove([node.path])
    },
    nodeOpenError(error_response, node) {
      if (error_response.response.data?.password_timeout) {
        showPasswordPrompt(
          this.database_index,
          () => {
            this.refreshNode();
          },
          null,
          error_response.response.data.data
        );
      } else {
        //FIXME: add proper ui styling and html
        this.removeChildNodes(node);
        this.$toast.error(error_response.response.data.data, {
          duration: 0
        })
      }
    },
    getRootNode() {
      return this.$refs.tree.getFirstNode()
    }
  },
};
