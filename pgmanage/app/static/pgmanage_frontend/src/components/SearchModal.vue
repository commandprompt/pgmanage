<template>
  <div
    v-if="isOpen"
    id="searchPalette"
    class="position-absolute top-0 start-50 translate-middle-x w-100"
    style="max-width: 700px; z-index: 1050"
  >
    <div class="bg-dark text-white rounded p-2">
      <input
        ref="searchInput"
        type="text"
        class="form-control bg-secondary text-white border-0 form-control-sm"
        placeholder="Search..."
        @blur="close"
        v-model="query"
      />
      <ul class="list-group" style="max-height: 300px; overflow-y: auto">
        <li
          v-for="(item, idx) in results"
          :key="item.id"
          class="list-group-item bg-dark text-white border-secondary d-flex justify-content-between"
          :class="{ 'mt-2': idx == 0 }"
          @mousedown="selectItem(item)"
          role="button"
        >
          <div>
            <div>{{ item.name }}</div>
            <small class="text-muted">{{ item.schema }}</small>
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
  data() {
    return {
      isOpen: false,
      query: "",
      searchInput: null,
      items: [],
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
    emitter.on("show_quick_search", (event) => {
      console.log(tabsStore.selectedPrimaryTab);
      // const workspaceId = tabsStore.selectedPrimaryTab.id
      const databaseIndex =
        tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex;
      const databaseName =
        tabsStore.selectedPrimaryTab.metaData.selectedDatabase;
      this.items = this.flattenSchemas(
        dbMetadataStore.getDbMeta(databaseIndex, databaseName)
      );
      this.fuse = new Fuse(this.items, {
        keys: ["name"],
        threshold: 0.3,
      });
      this.open();
    });
    window.addEventListener("keydown", this.handleEsc);
  },
  beforeUnmount() {
    emitter.all.delete("show_quick_search");
    window.removeEventListener("keydown", this.handleEsc);
  },
  methods: {
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
      if (item?.clickCallback) item.clickCallback();
      this.close();
    },
    handleEsc(e) {
      if (e.key === "Escape") {
        this.close();
      }
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
            id: `table:${schemaName}.${table.name}`,
            clickCallback: function () {
              const workspaceId = tabsStore.selectedPrimaryTab.id;
              emitter.emit(`goToNode_${workspaceId}`, {
                type: "table",
                name: table.name,
                schema: schemaName,
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
            id: `view:${schemaName}.${view.name}`,
            clickCallback: function () {
              const workspaceId = tabsStore.selectedPrimaryTab.id;
              emitter.emit(`goToNode_${workspaceId}`, {
                type: "view",
                name: view.name,
                schema: schemaName,
              });
            },
          });
        }

        result.push({
          name: schemaName,
          type: "schema",
          schema: schemaName,
          database: databaseName,
          id: `schema:${schemaName}`,
        });
      }

      return result;
    },
  },
};
</script>
