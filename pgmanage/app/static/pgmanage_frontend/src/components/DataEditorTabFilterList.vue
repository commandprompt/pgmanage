<template>
  <div
    v-if="mode === modes.BUILDER"
    v-for="(filter, index) in localFilters"
    :key="index"
    class="row mb-2 g-0"
  >
    <div class="col-auto ps-0 pe-1 col-w-fixed">
      <button
        v-if="index === 0"
        class="btn btn-warning btn-w-fixed"
        type="button"
        @click="switchToManual"
        title="Switch to Manual Mode"
      >
        SQL
      </button>
      <button
        v-else
        data-testid="toggle-condition-button"
        class="btn btn-ghost-secondary btn-w-fixed"
        type="button"
        @click.stop="
          filter.condition = filter.condition === 'AND' ? 'OR' : 'AND'
        "
        title="Toggle Filter AND / OR"
      >
        {{ filter.condition }}
      </button>
    </div>
    <FilterItem
      :columns="columns"
      :comparison-operators="localOperators"
      :value="filter"
      @update="updateFilter(index, $event)"
    />
    <div class="col-1 d-flex px-1 ps-0">
      <button
        v-if="localFilters.length === 1"
        class="btn me-2"
        :class="{ invisible: index === 0 }"
        @click="removeFilter(index)"
      >
        <i class="fas fa-minus muted-text"></i>
      </button>
      <button
        v-else
        data-testid="remove-button"
        class="btn me-2"
        @click="removeFilter(index)"
      >
        <i class="fas fa-minus muted-text"></i>
      </button>

      <button
        v-if="index === 0"
        data-testid="add-filter-button"
        class="btn"
        @click="addFilter"
        title="Add filter"
      >
        <i class="fas fa-plus text-success"></i>
      </button>
    </div>
  </div>
  <div v-else class="row mb-2 g-0">
    <div class="col-auto ps-0 pe-1 col-w-fixed">
      <button
        class="btn btn-warning btn-w-fixed"
        type="button"
        @click="switchToBuilder"
        title="Switch to Builder Mode"
      >
        <i class="fas fa-wand-magic-sparkles"></i>
      </button>
    </div>
    <div class="col px-1">
      <input
        v-model="rawQuery"
        class="form-control"
        type="text"
        placeholder="Type your filter query here"
        @input="emitRawQuery"
      />
    </div>
  </div>
</template>

<script>
import DataEditorTabFilterItem from "./DataEditorTabFilterItem.vue";
import { dataEditorFilterModes } from "../constants";
import { extractOrderByClause } from '../utils';

export default {
  name: "DataEditorFilter",
  components: {
    FilterItem: DataEditorTabFilterItem,
  },
  emits: ["update"],
  props: {
    columns: {
      type: Array,
      default: [],
    },
    filters: {
      type: Array,
    },
    updatedRawQuery: {
      type: String
    },
    operators: {
      type: Array,
      default: []
    }
  },
  data() {
    return {
      localFilters: this.filters,
      defaultOperators: ["=", "!=", "<", "<=", ">", ">=", "like", "in"],
      mode: dataEditorFilterModes.BUILDER,
      rawQuery: "",
      rawInputDirty: false,
    };
  },
  computed: {
    modes() {
      return dataEditorFilterModes;
    },
    localOperators() {
      return [...this.defaultOperators, ...this.operators];
    },
  },
  watch: {
    columns(newValue) {
      if (!!newValue) {
        this.localFilters[0]["column"] = newValue[0];
      }
    },
    localFilters: {
      handler(newValue, oldValue) {
        this.$emit("update", { mode: this.mode, filters: newValue });
      },
      deep: true,
    },
    updatedRawQuery(newValue) {
      if (newValue) {
        this.rawQuery = newValue
        this.$emit("update", { mode: this.mode, rawQuery: this.rawQuery });
      }
    }
  },
  methods: {
    addFilter() {
      this.localFilters.push({
        column: this.columns[0] || "",
        operator: "=",
        value: "",
        condition: "AND",
      });
    },
    removeFilter(index) {
      this.localFilters.splice(index, 1);
    },
    updateFilter(index, updatedFilter) {
      this.localFilters[index] = updatedFilter;
    },
    switchToManual() {
      if (!this.rawInputDirty) {
        const trimmedQuery = this.rawQuery.trim().toLowerCase();
        const { orderByClause } = extractOrderByClause(trimmedQuery);
        const convertedFilters = this.convertFiltersToManual(this.localFilters);
        this.rawQuery = `${convertedFilters} ${orderByClause}`
      }
      this.mode = this.modes.MANUAL;
      this.$emit("update", { mode: this.mode, rawQuery: this.rawQuery });
    },
    switchToBuilder() {
      this.mode = this.modes.BUILDER;
      this.$emit("update", { mode: this.mode });
    },
    convertFiltersToManual(filters) {
      const convertedFilters = filters
        .filter((f) => f.operator && f.column && f.value)
        .map((filter, index) => {
          const condition = index === 0 ? "" : filter.condition || "AND";
          if (filter.operator === "in") {
            const values = filter.value
              .split(/\s*,\s*/)
              .map((v) => `'${v}'`)
              .join(", ");
            return `${condition} ${
              filter.column
            } ${filter.operator.toUpperCase()} (${values})`.trim();
          }
          return `${condition} ${filter.column} ${filter.operator} '${filter.value}'`;
        })
        .join("\n");
          return !!convertedFilters ? `where ${convertedFilters}` : "";
    },
    emitRawQuery() {
      this.rawInputDirty = true;
      this.$emit("update", { mode: this.mode, rawQuery: this.rawQuery });
    },
  },
};
</script>

<style scoped>
.col-w-fixed {
  min-width: 3.7rem;
}

.btn-w-fixed {
  min-width: 3.5rem;
}
</style>
