<template>
  <div
    v-if="isOpen"
    ref="searchInput"
    id="searchPalette"
    class="position-absolute top-0 start-50 translate-middle-x w-100"
    style="max-width: 700px; z-index: 1050"
  >
    <div class="bg-dark text-white rounded p-2">
      <input
        type="text"
        class="form-control bg-secondary text-white border-0 form-control-sm"
        placeholder="Search..."
        autofocus
        v-model="query"
      />
      <ul class="list-group" style="max-height: 300px; overflow-y: auto">
        <li
          v-for="(item, idx) in results"
          :key="item.id"
          class="list-group-item bg-dark text-white border-secondary d-flex justify-content-between"
          :class="{ 'mt-2': idx == 0 }"
          @click="selectItem(item)"
          role="button"
        >
          <span>{{ item.name }}</span>
          <span class="text-muted">{{ item.type }}</span>
        </li>
      </ul>
    </div>
  </div>
</template>

<script>
import Fuse from "fuse.js";

export default {
  name: "SearchModal",
  data() {
    return {
      isOpen: false,
      query: "",
      searchInput: null,
      items: [
        { name: "users", type: "table", id: "table:users" },
        { name: "get_user", type: "function", id: "function:get_user" },
        { name: "analytics_view", type: "view", id: "view:analytics_view" },
        { name: "app_db", type: "database", id: "db:app_db" },
      ],
      fuse: null,
    };
  },
  computed: {
    results() {
      if (!this.query) return [];
      return this.fuse.search(this.query).map((r) => r.item);
    },
  },
  mounted() {
    this.fuse = new Fuse(this.items, {
      keys: ["name"],
      threshold: 0.3,
    });

    window.addEventListener("keydown", this.handleShortcut);
    window.addEventListener("keydown", this.handleEsc);
  },
  beforeUnmount() {
    window.removeEventListener("keydown", this.handleShortcut);
    window.removeEventListener("keydown", this.handleEsc);
  },
  methods: {
    handleShortcut(e) {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "p") {
        e.preventDefault();
        this.open();
      }
    },
    open() {
      this.isOpen = true;
      this.$nextTick(() => {
        this.$refs.searchInput?.focus();
      });
    },
    close() {
      this.isOpen = false;
      this.query = "";
    },
    selectItem(item) {
      this.close();
    },
    handleEsc(e) {
      if (e.key === "Escape") {
        this.close();
      }
    },
  },
};
</script>
