import { defineStore } from "pinia";

const useSnippetsStore = defineStore('snippets', {
  state: () => ({
    id: null,
    files: [],
    folders: []
  })
})

const snippetsStore = useSnippetsStore();

export { snippetsStore }