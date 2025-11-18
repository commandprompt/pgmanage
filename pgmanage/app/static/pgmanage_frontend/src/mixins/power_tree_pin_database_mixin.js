export default {
  methods: {
    pinDatabase(node) {
      const pinned = !node.data.pinned;
      this.api
        .post("/pin_database/", {
          database_name: node.title,
          pinned: pinned,
        })
        .then((resp) => {
          this.$refs.tree.updateNode(node.path, {
            data: {
              ...node.data,
              pinned: pinned,
            },
          });
          const parentNode = this.getParentNode(node);
          this.sortPinnedNodes(parentNode);
        })
        .catch((error) => {
          this.nodeOpenError(error, node);
        });
    },
    sortPinnedNodes(node) {
      if (!node || !node.children) return;

      const children = node.children;

      const pinned = [];
      const unpinned = [];

      for (const child of children) {
        (child.data.pinned ? pinned : unpinned).push(child);
      }

      pinned.sort((a, b) => {
        return a.title.localeCompare(b.title);
      });

      unpinned.sort((a, b) => {
        return a.title.localeCompare(b.title);
      });

      // Combine back: pinned DBs first, then unpinned DBs
      const reordered = [...pinned, ...unpinned];

      this.$refs.tree.updateNode(node.path, {
        children: reordered,
      });
    },
  },
};
