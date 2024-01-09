import { defineStore } from "pinia";

const useSnippetsStore = defineStore("snippets", {
  state: () => ({
    id: null,
    files: [],
    folders: [],
  }),
  actions: {
    getExistingTab(snippet_id) {
      let snippet_tab_list = v_connTabControl.snippet_tag.tabControl.tabList;
      let [existing_tab] = snippet_tab_list.filter((snippet_tab) => {
        return snippet_tab.tag?.snippetObject?.id === snippet_id;
      });
      return existing_tab;
    },
  },
});

export { useSnippetsStore };
