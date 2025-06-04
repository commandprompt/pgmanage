<template>
  <div
    v-if="isOpen"
    id="searchPalette"
    class="position-absolute top-0 start-50 translate-middle-x w-100"
    style="max-width: 700px; z-index: 1050"
  >
    <div class="bg-dark text-white rounded p-2">
      <input
        :id="`${workspaceId}_search_input`"
        ref="searchInput"
        type="text"
        class="form-control bg-secondary text-white border-0 form-control-sm"
        placeholder="Search..."
        @keyup.up="keyMonitor"
        @keyup.down="keyMonitor"
        @keyup.enter="onEnter"
        @keyup.esc="close"
        @blur="close"
        v-model="query"
        autocomplete="off"
      />
      <ul class="list-group" style="max-height: 300px; overflow-y: auto">
        <li
          ref="dropdownItems"
          v-for="(item, idx) in results"
          :key="item.id"
          class="list-group-item d-flex justify-content-between search-item p-1"
          :class="{ 'mt-2': idx == 0, selected: idx === selectedIndex }"
          @mousedown="selectItem(item)"
          role="button"
        >
          <div>
            <div>{{ item.name }}</div>
            <small class="text-muted">{{ item.database }}</small
            >@<small class="text-muted">{{ item.schema }}</small>
          </div>
          <span class="text-muted">{{ item.type }}</span>
        </li>
      </ul>
    </div>
  </div>
</template>

<script>
import Fuse from "fuse.js";
import { emitter } from "../emitter";
import { tabsStore, dbMetadataStore } from "../stores/stores_initializer";

export default {
  name: "SearchModal",
  props: {
    workspaceId: String,
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
      const databaseIndex =
        tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex;
      const databaseName =
        tabsStore.selectedPrimaryTab.metaData.selectedDatabase;
      const newItems = this.flattenSchemas(
        dbMetadataStore.getDbMeta(databaseIndex, databaseName)
      );

      const existingItemMap = new Map(
        this.items.map((item) => [item.id, item])
      );

      newItems.forEach((item) => {
        existingItemMap.set(item.id, item);
      });

      this.items = Array.from(existingItemMap.values());
      this.fuse.setCollection(this.items);

      const databases = dbMetadataStore.getDatabases(databaseIndex);
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
      this.$nextTick(() => {
        const selectedEl = this.$refs.dropdownItems?.[this.selectedIndex];
        if (selectedEl) {
          selectedEl.scrollIntoView({ block: "nearest", behavior: "instant" });
        }
      });
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

<style lang="scss" scoped>

.search-item.selected {
  background-color: rgba($primaryBlue, 0.15);
}

.search-item:hover {
  background-color: rgba($primaryMutedColor, 0.15);
}
</style>
