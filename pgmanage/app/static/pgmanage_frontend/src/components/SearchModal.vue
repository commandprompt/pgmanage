<template>
  <Teleport to="body">
    <div
      v-if="isOpen"
      id="searchPalette"
      class="search-modal position-absolute top-0 start-50 translate-middle-x w-100"
      style="max-width: 700px; z-index: 1050"
    >
      <div class="search-modal__container p-3">
        <div class="form-group mb-0">
          <input
            :id="`${workspaceId}_search_input`"
            ref="searchInput"
            type="text"
            class="form-control border-0 form-control-sm"
            placeholder="Search..."
            @keydown.up.prevent="keyMonitor"
            @keydown.down.prevent="keyMonitor"
            @keyup.enter="onEnter"
            @keyup.esc="close"
            @blur="close"
            v-model="query"
            autocomplete="off"
          />
        </div>
        <ul class="search-modal__results list-group overflow-y-auto" style="max-height: 300px">
          <li
            ref="searchItems"
            v-for="(item, idx) in results"
            class="search-modal__results_item list-group-item d-flex justify-content-between p-2"
            :class="{ selected: idx === selectedIndex }"
            @mousedown="selectItem(item)"
            role="button"
          >
            <div>
              <div>{{ item.name }}</div>
              <div class="meta-tags">
                <small v-for="meta in getMetaTags(item)" class="text-muted">
                  {{ meta.text }}
                </small>
              </div>
            </div>
            <span class="text-muted">{{ item.type }}</span>
          </li>
        </ul>
      </div>
    </div>
  </Teleport>
</template>

<script>
import Fuse from "fuse.js";
import { emitter } from "../emitter";
import { tabsStore, dbMetadataStore } from "../stores/stores_initializer";

export default {
  name: "SearchModal",
  props: {
    workspaceId: String,
    databaseIndex: Number,
    databaseTechnology: String,
  },
  data() {
    return {
      isOpen: false,
      query: "",
      searchInput: null,
      items: [],
      fuse: null,
      selectedIndex: -1,
    };
  },
  computed: {
    results() {
      if (!this.query) return [];
      return this.fuse.search(this.query).map((r) => r.item);
    },
  },
  mounted() {
    this.fuse = new Fuse([], {
      keys: ["name"],
    });
    emitter.on(`${this.workspaceId}_show_quick_search`, (event) => {
      const databaseName =
        tabsStore.selectedPrimaryTab.metaData.selectedDatabase;
      const newItems = this.flattenSchemas(
        dbMetadataStore.getDbMeta(this.databaseIndex, databaseName)
      );

      const existingItemMap = new Map(
        this.items.map((item) => [item.id, item])
      );

      newItems.forEach((item) => {
        existingItemMap.set(item.id, item);
      });

      this.items = Array.from(existingItemMap.values());
      this.fuse.setCollection(this.items);

      const databases = dbMetadataStore.getDatabases(this.databaseIndex);
      databases.forEach((db) => {
        const id = `database:${db}`;
        if (!this.items.find((item) => item.id === id)) {
          this.fuse.add({
            name: db,
            type: "database",
            database: db,
            id: `database:${db}`,
            clickCallback: () => {
              emitter.emit(`goToNode_${this.workspaceId}`, {
                type: "database",
                name: db,
                database: db,
              });
            },
          });
        }
      });
      this.open();
    });
  },
  beforeUnmount() {
    emitter.all.delete(`${this.workspaceId}_show_quick_search`);
  },
  methods: {
    getMetaTags(item) {
      if (this.databaseTechnology === "sqlite") return [];

      if (item.type === "schema") {
        return [{ text: `@${item.database}` }];
      }

      if (item.type === "database") {
        return [];
      }

      if (["mariadb", "mysql"].includes(this.databaseTechnology)) {
        return [{ text: `${item.name}` }, { text: `@${item.database}` }];
      }
      return [
        { text: item.schema },
        { text: `.${item.name}` },
        { text: `@${item.database}` },
      ];
    },
    open() {
      this.selectedIndex = 0;
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
      if (item?.clickCallback) item.clickCallback();
      this.close();
    },
    flattenSchemas(schemas) {
      const result = [];
      const databaseName =
        tabsStore.selectedPrimaryTab.metaData.selectedDatabase;

      for (const schema of schemas) {
        const schemaName = schema.name;

        // Tables
        for (const table of schema.tables || []) {
          result.push({
            name: table.name,
            type: "table",
            schema: schemaName,
            database: databaseName,
            id: `table:${databaseName}.${schemaName}.${table.name}`,
            clickCallback: () => {
              emitter.emit(`goToNode_${this.workspaceId}`, {
                type: "table",
                name: table.name,
                schema: schemaName,
                database: databaseName,
              });
            },
          });
        }

        // Views
        for (const view of schema.views || []) {
          result.push({
            name: view.name,
            type: "view",
            schema: schemaName,
            database: databaseName,
            id: `view:${databaseName}.${schemaName}.${view.name}`,
            clickCallback: () => {
              emitter.emit(`goToNode_${this.workspaceId}`, {
                type: "view",
                name: view.name,
                schema: schemaName,
                database: databaseName,
              });
            },
          });
        }

        if (!["mariadb", "mysql"].includes(this.databaseTechnology)) {
          result.push({
            name: schemaName,
            type: "schema",
            schema: schemaName,
            database: databaseName,
            id: `schema:${databaseName}.${schemaName}`,
            clickCallback: () => {
              emitter.emit(`goToNode_${this.workspaceId}`, {
                type: "schema",
                name: schemaName,
                schema: schemaName,
                database: databaseName,
              });
            },
          });
        }
      }

      return result;
    },
    keyMonitor: function (event) {
      if (event.key === "ArrowDown") {
        if (this.selectedIndex < this.results.length - 1) {
          this.selectedIndex++;
        } else {
          this.selectedIndex = 0;
        }
      } else if (event.key === "ArrowUp") {
        if (this.selectedIndex > 0) {
          this.selectedIndex--;
        } else {
          this.selectedIndex = this.results.length - 1;
        }
      }
      this.scrollToSelected();
    },
    scrollToSelected() {
      const selectedEl = this.$refs.searchItems?.[this.selectedIndex];
      if (selectedEl) {
        selectedEl.scrollIntoView({ block: "nearest", behavior: "instant" });
      }
    },
    onEnter() {
      if (this.selectedIndex >= 0 && this.results[this.selectedIndex]) {
        const selected = this.results[this.selectedIndex];
        selected.clickCallback?.();
        this.close();
      }
    },
  },
};
</script>