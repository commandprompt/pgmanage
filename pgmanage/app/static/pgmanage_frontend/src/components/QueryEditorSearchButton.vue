<template>
  <button
    ref="searchButton"
    id="search-button"
    title="Find/Replace"
    class="btn btn-icon btn-icon-primary btn-sm m-2 d-block top-0 end-0 position-absolute search-button"
    :class="{ 'd-none': isEmpty }"
    @click="openSearchModal"
  >
    <i class="fa-solid fa-magnifying-glass"></i>
  </button>
</template>

<script>
export default {
  name: "SearchButton",
  props: ["editor", "editorInitialized"],
  data() {
    return {
      isEmpty: true,
      eventHideButtonSet: false,
    };
  },
  watch: {
    editorInitialized(newVal) {
      if (newVal) {
        this.setupEvents();
        this.addSearchButton();
      }
    },
  },
  mounted() {},
  methods: {
    setupEvents() {
      this.editor.commands.on("afterExec", (eventData, editor) => {
        const searchButton = this.$refs.searchButton;
        if (eventData.command.name === "closeSearchBar") {
          searchButton.classList.remove("d-none");
        } else if (eventData.command.name === "find") {
          searchButton.classList.add("d-none");
        }
      });

      this.editor.on("findSearchBox", (eventData, editor) => {
        if (editor?.searchBox && !this.eventHideButtonSet) {
          const searchBoxEl = editor.searchBox.element;
          const hideButton = searchBoxEl.querySelector(
            "span.ace_searchbtn_close"
          );
          const searchButton = this.$refs.searchButton;
          if (hideButton) {
            hideButton.onclick = (event) => {
              searchButton.classList.remove("d-none");
            };
            this.eventHideButtonSet = true;
          }
        }
      });

      this.editor.on("change", (obj, editor) => {
        const editorValue = editor.getValue().trim();
        this.isEmpty = !editorValue;
      });
    },
    addSearchButton() {
      const searchButton = this.$refs.searchButton;

      const editorContainer = this.editor.container;
      editorContainer.addEventListener("mouseover", () => {
        if (!this.isEmpty && !this.editor?.searchBox?.active) {
          searchButton.classList.remove("d-none");
        }
      });
      editorContainer.addEventListener("mouseout", (event) => {
        if (
          event.relatedTarget.classList.contains("ace_content") ||
          editorContainer.contains(event.relatedTarget)
        )
          return;
        if (!searchButton.classList.contains("d-none")) {
          searchButton.classList.add("d-none");
        }
      });

      editorContainer.appendChild(searchButton);
    },
    openSearchModal() {
      this.$refs.searchButton.classList.add("d-none");
      this.editor.execCommand("find");
    },
  },
};
</script>

<style scoped>
.search-button {
  z-index: 1000;
}
</style>
