<template>
  <div class="mb-2">
    <div class="d-flex row fw-bold text-muted schema-editor__header g-0">
      <div class="col-2">
        <p class="h6">Name</p>
      </div>
      <div class="col-1">
        <p class="h6">Column Name</p>
      </div>
      <div class="col-2">
        <p class="h6">FK Schema</p>
      </div>
      <div class="col-2">
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
      <div class="col-2 d-flex align-items-center">
        <input
          type="text"
          v-model="constraint.constraint_name"
          class="form-control mb-0 ps-2"
          placeholder="constraint name..."
          :disabled="!constraint.new"
        />
      </div>

      <div class="col-1 d-flex align-items-center">
        <SearchableDropdown
          placeholder="column name..."
          :options="columns"
          v-model="constraint.column_name"
          :disabled="!constraint.new"
        />
      </div>

      <div class="col-2 d-flex align-items-center">
        <SearchableDropdown
          placeholder="foreign key table schema.."
          :options="schemas"
          v-model="constraint.r_table_schema"
          :disabled="!constraint.new"
          @change="onSchemaChange(constraint)"
        />
      </div>

      <div class="col-2 d-flex align-items-center">
        <SearchableDropdown
          placeholder="foreign key table.."
          :options="getTables(constraint.r_table_schema)"
          v-model="constraint.r_table_name"
          :disabled="!constraint.new"
          @change="onTableChange(constraint)"
        />
      </div>

      <div class="col-2 d-flex align-items-center">
        <SearchableDropdown
          placeholder="foreign key column.."
          :options="
            getColumns(constraint.r_table_schema, constraint.r_table_name)
          "
          v-model="constraint.r_column_name"
          :disabled="!constraint.new"
        />
      </div>

      <div class="col-1">
        <SearchableDropdown
          placeholder="on update ..."
          :options="constraintActions"
          v-model="constraint.on_update"
          :disabled="!constraint.new"
        />
      </div>

      <div class="col-1 d-flex align-items-center">
        <SearchableDropdown
          placeholder="on delete ..."
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
    dbMetaData: Array,
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
        constraint_name: constraintName,
        column_name: null,
        r_table_schema: null,
        r_table_name: null,
        r_column_name: null,
        on_update: "NO ACTION",
        on_delete: "NO ACTION",
        new: true,
        editable: true,
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
    getTables(r_table_schema) {
      let schema = this.dbMetaData.find(
        (schema) => schema.name === r_table_schema
      );
      if (!schema) return;
      return schema?.tables.map((table) => table.name) ?? [];
    },
    getColumns(r_table_schema, r_table_name) {
      let schema = this.dbMetaData.find(
        (schema) => schema.name === r_table_schema
      );
      if (!schema) return;

      let table = schema?.tables.find((table) => table.name === r_table_name);

      if (!table) return;

      return table.columns ?? [];
    },
    onSchemaChange(constraint) {
      constraint.r_table_name = null;
      constraint.r_column_name = null;
    },
    onTableChange(constraint) {
      constraint.r_column_name = null;
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
  computed: {
    schemas() {
      return this.dbMetaData.map((schema) => schema.name);
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
