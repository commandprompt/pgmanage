import { messageModalStore } from "../stores/stores_initializer";
import { handleError } from '../logging/utils';

export default {
  data() {
    return {
      dropTemplate: null,
      dropNode: null
    };
  },
  methods: {
    parseTemplate(src) {
      const withoutFirst = src.replace(/^(\s*--.*(?:\r?\n|$))/, '');
      const lines = withoutFirst.split(/\r?\n/);
      const opts = [];

      for (let l of lines) {
          l = l.trim();
          if (!l) continue;
          if (l.startsWith('--')) {
          opts.push(l.replace(/^--\s*/, '')); 
          } 
      }

      return { query: withoutFirst, options: opts };
    },
    buildQueryWithOptions(query, options = []) {
      let ret = query
      options.forEach((opt) => {
        ret = ret.replace(`--${opt}`, opt)
      })
      return ret
    },
    prepareDropModal(node, template) {
      let message = `Are you sure you want to drop ${node.data.type} '${node.title}'?`
      this.dropTemplate = this.parseTemplate(template)
      this.dropNode = node

      let checkboxes = this.dropTemplate.options.map((option) => ({ 'label': option, 'checked': false }))
      messageModalStore.showModal(message, this.dropDbObject, null, true, checkboxes)
    },
    dropDbObject() {
      let options = messageModalStore.checkboxes.map((o) => o.checked ? o.label : null)
      let query = this.buildQueryWithOptions(this.dropTemplate.query, options)
      this.api.post('/execute_query/', {
        database_index: this.databaseIndex,
        workspace_id: this.workspaceId,
        query: query
      })
      .then((resp) => {
        if(options.includes('CASCADE')) {
          this.refreshTree(this.getRootNode, true);
        } else {
          let parentNode = this.getParentNode(this.dropNode)
          let childrenCount = parentNode.children.length
          this.removeNode(this.dropNode)
          this.$refs.tree.updateNode(parentNode.path, {
            title: parentNode.title.replace(/\(\d+\)$/, `(${childrenCount - 1})`)
          });
        }
      })
      .catch((error) => {
        handleError(error)
      })
    },
  },
};
  