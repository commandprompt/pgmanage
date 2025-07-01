<template>
  <div class="mb-2">
    <div class="d-flex row fw-bold text-muted schema-editor__header g-0">
      <div class="col">
        <p class="h6">Name</p>
      </div>
      <div class="col-1">
        <p class="h6">Column Name</p>
      </div>
      <div class="col-1">
        <p class="h6">FK Schema</p>
      </div>
      <div class="col-1">
        <p class="h6">FK Table</p>
      </div>
      <div class="col-2">
        <p class="h6">FK Column</p>
      </div>
      <div class="col-1">
        <p class="h6">On Update</p>
      </div>
      <div class="col-1">
        <p class="h6">On Delete</p>
      </div>
      <div class="col-1">
        <p class="h6">Actions</p>
      </div>
    </div>
    <div
      v-for="(constraint, idx) in constraints"
      :key="idx"
      :class="[
        'schema-editor__column d-flex row flex-nowrap form-group g-0',
        { 'schema-editor__column-deleted': constraint.deleted },
        { 'schema-editor__column-new': constraint.new },
        { 'schema-editor__column-dirty': constraint.is_dirty },
      ]"
    >
      <div class="col d-flex align-items-center">
        <input
          type="text"
          v-model="constraint.constraint_name"
          class="form-control mb-0 ps-2"
          placeholder="constraint name..."
          :disabled="!constraint.new"
        />
      </div>

      <div class="col-1 d-flex align-items-center">
        <input
          type="text"
          v-model="constraint.column_name"
          class="form-control mb-0 ps-2"
          placeholder="column name..."
          :disabled="!constraint.new"
        />
      </div>

      <div class="col-1 d-flex align-items-center">
        <SearchableDropdown
          placeholder="foreign key table schema.."
          :options="tables"
          v-model="constraint.r_table_schema"
          :disabled="!constraint.new"
        />
      </div>

      <div class="col-1 d-flex align-items-center">
        <SearchableDropdown
          placeholder="foreign key table.."
          :options="tables"
          v-model="constraint.r_table_name"
          :disabled="!constraint.new"
        />
      </div>

      <div class="col-2 d-flex align-items-center">
        <SearchableDropdown
          placeholder="foreign key column.."
          :options="columns"
          v-model="constraint.r_column_name"
          :disabled="!constraint.new"
        />
      </div>

      <div class="col-1">
        <SearchableDropdown
          placeholder="foreign key table.."
          :options="constraintActions"
          v-model="constraint.on_update"
          :disabled="!constraint.new"
        />
      </div>

      <div class="col-1 d-flex align-items-center">
        <SearchableDropdown
          placeholder="foreign key table.."
          :options="constraintActions"
          v-model="constraint.on_delete"
          :disabled="!constraint.new"
        />
      </div>

      <div class="col-1 d-flex me-2 justify-content-end">
        <button
          v-if="(constraint.deleted && !constraint.new) || constraint.is_dirty"
          @click="revertConstraint(idx)"
          type="button"
          class="btn btn-icon btn-icon-success"
          title="Revert"
        >
          <i class="fas fa-rotate-left"></i>
        </button>

        <button
          v-if="!constraint.deleted && !constraint.is_dirty"
          @click="removeConstraint(idx)"
          type="button"
          class="btn btn-icon btn-icon-danger"
          title="Remove constraint"
        >
          <i class="fas fa-circle-xmark"></i>
        </button>
      </div>
    </div>
    <div class="d-flex g-0 fw-bold mt-2">
      <button @click="addConstraint" class="btn btn-outline-success ms-auto">
        Add Constraint
      </button>
    </div>
  </div>
</template>

<script>
import SearchableDropdown from "./SearchableDropdown.vue";

export default {
  name: "SchemaEditorConstraintsList",
  components: {
    SearchableDropdown,
  },
  props: {
    initialConstraints: {
      type: Array,
      default: [],
    },
    disabledFeatures: {
      type: Object,
      default: {},
    },
    columns: Array,
  },
  emits: ["constraints:changed"],
  data() {
    return {
      constraints: [],
      constraintActions: [
        "NO ACTION",
        "SET NULL",
        "SET DEFAULT",
        "CASCADE",
        "RESTRICT",
      ],
      tables: [],
    };
  },
  methods: {
    addConstraint() {
      let constraintName = `fk_${this.constraints.length}`;
      const defaultConstraint = {
        name: constraintName,
        fk_column: null,
        new: true,
        editable: true,
        on_update: "NO ACTION",
        on_delete: "NO ACTION",
        is_dirty: false,
      };
      this.constraints.push(defaultConstraint);
    },
    removeConstraint(index) {
      if (!this.constraints[index].new) {
        this.constraints[index].deleted = true;
      } else {
        this.constraints.splice(index, 1);
      }
    },
    revertConstraint(index) {
      this.constraints[index] = JSON.parse(
        JSON.stringify(this.initialConstraints[index])
      );
    },
  },
  watch: {
    initialConstraints: {
      handler(newVal, oldVal) {
        this.constraints = JSON.parse(JSON.stringify(newVal));
      },
      immediate: true,
    },
    constraints: {
      handler(newVal, oldVal) {
        this.$emit("constraints:changed", newVal);
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
