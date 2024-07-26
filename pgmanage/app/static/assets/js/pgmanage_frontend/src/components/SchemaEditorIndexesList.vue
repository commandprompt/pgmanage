<template>
  <div class="mb-2">
    <div class="d-flex row fw-bold text-muted schema-editor__header">
      <div class="col-2">
        <p class="h6">Name</p>
      </div>
      <div class="col-1">
        <p class="h6">indexType</p>
      </div>
      <div v-if="!disabledFeatures?.indexMethod" class="col-1">
        <p class="h6">Method</p>
      </div>
      <div class="col-3">
        <p class="h6">Columns</p>
      </div>
      <div v-if="!disabledFeatures?.indexPredicate" class="col">
        <p class="h6">Predicate</p>
      </div>
      <div class="col">
        <p class="h6">Actions</p>
      </div>
    </div>
    <div
      v-for="(index, idx) in indexes"
      :key="index.index_name"
      :class="[
        'schema-editor__column d-flex row flex-nowrap form-group g-0',
        { 'schema-editor__column-deleted': index.deleted },
        { 'schema-editor__column-new': index.new },
      ]"
    >
      <div class="col-2 d-flex align-items-center">
        <i
          v-if="index.is_primary"
          title="Primary key"
          class="fas fa-key action-key text-secondary me-1"
        ></i>
        <input
          type="text"
          v-model="index.index_name"
          class="form-control mb-0"
          placeholder="NULL"
          :disabled="disabledFeatures?.renameIndex && !index.new"
        />
      </div>

      <div class="col-1 d-flex align-items-center">
        <select class="form-select" v-model="index.type" :disabled="!index.new">
          <option v-for="indexType in indexTypes" :value="indexType">
            {{ indexType }}
          </option>
        </select>
      </div>

      <div
        v-if="!disabledFeatures?.indexMethod"
        class="col-1 d-flex align-items-center"
      >
        <select
          class="form-select"
          v-model="index.method"
          :disabled="!index.new || index.type == 'unique'"
        >
          <option v-for="method in indexMethods" :value="method">
            {{ method }}
          </option>
        </select>
      </div>

      <div class="col-3">
        <SearchableDropdown
          placeholder="type to search"
          :options="columns"
          :maxItem="20"
          v-model="index.columns"
          :multi-select="true"
          :disabled="!index.new"
        />
      </div>

      <div
        v-if="!disabledFeatures?.indexPredicate"
        class="col d-flex align-items-center"
      >
        <input
          type="text"
          v-model="index.predicate"
          class="form-control mb-0"
          :disabled="!index.new"
        />
      </div>

      <div class="col d-flex me-2 justify-content-end">
        <button
          v-if="index.deleted && !index.new"
          @click="index.deleted = false"
          type="button"
          class="btn btn-icon btn-icon-success"
          title="Revert"
        >
          <i class="fas fa-rotate-left"></i>
        </button>

        <button
          v-if="!index.deleted"
          @click="removeIndex(idx)"
          type="button"
          class="btn btn-icon btn-icon-danger"
          title="Remove column"
        >
          <i class="fas fa-circle-xmark"></i>
        </button>
      </div>
    </div>
    <div class="d-flex g-0 fw-bold mt-2">
      <button @click="addIndex" class="btn btn-outline-success ms-auto">
        Add Index
      </button>
    </div>
  </div>
</template>

<script>
import SearchableDropdown from "./SearchableDropdown.vue";
import isEqual from "lodash/isEqual";

export default {
  name: "SchemaEditorIndexesList",
  data() {
    return {
      indexes: [],
    };
  },
  components: {
    SearchableDropdown,
  },
  props: {
    initialIndexes: {
      type: Array,
      default: [],
    },
    indexMethods: {
      type: Array,
      default: [],
    },
    indexTypes: {
      type: Array,
      default: [],
    },
    disabledFeatures: {
      type: Object,
      default: {},
    },
    columns: Array,
  },
  emits: ["indexes:changed"],
  methods: {
    addIndex() {
      let indexName = `index_${this.indexes.length}`;
      const defaultIndex = {
        index_name: indexName,
        is_primary: false,
        columns: [],
        new: true,
        editable: true,
        method: null,
        predicate: "",
        type: "regular",
      };
      this.indexes.push(defaultIndex);
    },
    removeIndex(index) {
      if (!this.indexes[index].new) {
        this.indexes[index].deleted = true;
      } else {
        this.indexes.splice(index, 1);
      }
    },
  },
  watch: {
    initialIndexes: {
      handler(newVal, oldVal) {
        this.indexes = JSON.parse(JSON.stringify(newVal));
      },
      immediate: true,
    },
    indexes: {
      handler(newVal, oldVal) {
        if (!isEqual(newVal, this.initialIndexes)) {
          this.$emit("indexes:changed", newVal);
        }
      },
      deep: true,
    },
  },
};
</script>

<style scoped>
input[type="checkbox"].custom-checkbox:disabled {
  background-color: initial;
  border-color: rgba(118, 118, 118, 0.3);
}
</style>
