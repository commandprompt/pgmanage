<template>
  <select
    v-model="blockSize"
    @change="updateBlockSize"
    class="form-select me-2"
    style="width: 90px"
  >
    <option v-for="value in blockSizeOptions" :key="value" :value="value">
      {{ value }} rows
    </option>
  </select>
</template>

<script>
export default {
  name: "BlockSizeSelector",
  props: {
    modelValue: {
      type: Number,
      required: true,
    },
  },
  data() {
    return {
      blockSize: this.modelValue,
      blockSizeOptions: [50, 100, 200],
    };
  },
  watch: {
    modelValue(newVal) {
      this.blockSize = newVal;
    },
  },
  methods: {
    updateBlockSize() {
      this.$emit("update:modelValue", this.blockSize);
    },
  },
};
</script>
