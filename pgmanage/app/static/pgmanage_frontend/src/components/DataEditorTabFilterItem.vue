<template>
  <div class="col-2 px-1">
    <select data-testid="column-select" class="form-select" v-model="selectedColumn" @change="emitUpdate">
      <option v-for="column in columns" :key="column" :value="column">
        {{ column }}
      </option>
    </select>
  </div>
  <div class="col-2 px-1">
    <select data-testid="operator-select" class="form-select" v-model="selectedOperator" @change="emitUpdate">
      <option
        v-for="operator in comparisonOperators"
        :key="operator"
        :value="operator"
      >
        {{ operator }}
      </option>
    </select>
  </div>
  <div class="col-5 px-1 pe-0">
    <input
      data-testid="value-input"
      class="form-control"
      type="text"
      v-model="filterValue"
      @change="emitUpdate"
      :placeholder="
        selectedOperator === 'in'
          ? `Enter values separated by comma, eg: foo,bar`
          : 'Enter Value'
      "
    />
  </div>
</template>

<script>
export default {
  props: {
    columns: Array,
    comparisonOperators: Array,
    value: Object,
  },
  emits: ["update"],
  data() {
    return {
      selectedColumn: this.value.column || "",
      selectedOperator: this.value.operator || "",
      filterValue: this.value.value || "",
      condition: this.value.condition,
    };
  },
  watch: {
    value: {
      handler(newValue, oldValue) {
        if (!!newValue) {
          this.selectedColumn = newValue["column"];
          this.selectedOperator = newValue["operator"];
          this.filterValue = newValue["value"];
          this.condition = newValue["condition"];
        }
      },
      deep: true,
    },
  },
  methods: {
    emitUpdate() {
      this.$emit("update", {
        column: this.selectedColumn,
        operator: this.selectedOperator,
        value: this.filterValue,
        condition: this.condition,
      });
    },
  },
};
</script>

<style scoped>
.filter-item {
  display: flex;
  gap: 0.5rem;
}
</style>
