<template>
  <div class="dropdown dropdown-searchable" v-if="options">

    <!-- Dropdown Input -->
    <input class="dropdown-searchable__input form-control pe-4"
      :name="name"
      @focus="showOptions()"
      @blur="exit()"
      @keyup="keyMonitor"
      v-model="searchFilter"
      :disabled="disabled"
      :placeholder="placeholder"
      autocomplete="off"
      ref="input" />

    <!-- Dropdown Menu -->
    <div class="dropdown-searchable__content"
      v-show="optionsShown">
      <div
        class="dropdown-searchable__content_item"
        @mousedown="toggleOption(option)"
        v-for="(option, index) in filteredOptions"
        :key="index"
        :class="{ 'selected': isSelected(option) }"
        >
            {{  option }}
      </div>
    </div>
  </div>
</template>

<script>
  export default {
    name: 'Dropdown',
    template: 'Dropdown',
    props: {
      name: {
        type: String,
        required: false,
        default: 'dropdown',
        note: 'Input name'
      },
      options: {
        type: Array,
        required: true,
        default: [],
        note: 'Options of dropdown. An array of options',
      },
      placeholder: {
        type: String,
        required: false,
        default: 'Please select an option',
        note: 'Placeholder of dropdown'
      },
      disabled: {
        type: Boolean,
        required: false,
        default: false,
        note: 'Disable the dropdown'
      },
      maxItem: {
        type: Number,
        required: false,
        default: 6,
        note: 'Max items showing'
      },
      modelValue: {
        default: null
      },
      multiSelect: {
        type: Boolean,
        default: false
      }
    },
    emits: ['update:modelValue'],
    data() {
      return {
        selected: this.multiSelect ? [] : null,
        optionsShown: false,
        searchFilter: this.modelValue
      }
    },
    computed: {
      filteredOptions() {
        const filtered = [];
        // const regOption = new RegExp(this.searchFilter, 'ig');
        for (const option of this.options) {
          // if (this.searchFilter.length < 1 || option.name.match(regOption)){
          if (this.searchFilter?.length < 1 || option?.includes(this.searchFilter)){
            if (filtered.length < this.maxItem) filtered.push(option);
          }
        }
        return filtered;
      }
    },
    methods: {
      addOption(option) {
        if (this.multiSelect) {
          const index = this.selected.findIndex(selectedOption => selectedOption === option);
          if (index === -1) {
            this.selected.push(option);
          }
        }
      },
      toggleOption(option) {
        if (this.multiSelect) {
          const index = this.selected.findIndex(selectedOption => selectedOption === option);
          if (index === -1) {
            this.selected.push(option);
          } else {
            this.selected.splice(index, 1);
          }
          this.$emit('update:modelValue', this.selected);
        } else {
          this.selected = option;
          this.$emit('update:modelValue', this.selected);
          this.optionsShown = false;
          this.searchFilter = this.selected;
        }
      },
      showOptions(){
        if (!this.disabled) {
          // THIS resets onblur
          this.searchFilter = '';
          this.optionsShown = true;
        }
      },
      exit() {
        if (!this.selected && this.searchFilter.length > 0) {
          this.selected = this.searchFilter
        } else {
          this.searchFilter = this.selected;
        }
        this.$emit('update:modelValue', this.selected);
        this.optionsShown = false;
      },
      // Selecting when pressing Enter
      // FIXME: use hovered element instead of first one
      // TODO: add arrow navigation
      keyMonitor: function(event) {
        if (event.key === "Enter" && this.filteredOptions[0]) {
          this.toggleOption(this.filteredOptions[0]);
        } else if (event.key === "Enter" && this.filteredOptions.length === 0 && this.multiSelect) {
          let options = this.searchFilter.split(',');
          options.forEach((option) => {
            this.addOption(option.trim())
          });
          this.$emit('update:modelValue', this.selected);
        }
      },
      isSelected(option) {
        if (this.multiSelect) {
          return this.selected.some(selectedOption => selectedOption === option);
        } else {
          return this.selected && this.selected === option;
        }
      },
    },
    watch: {
      searchFilter() {
        if (this.filteredOptions.length === 0 && !this.multiSelect) {
          this.selected = null;
        }
      },
      modelValue: {
        handler(newVal) {
          if (this.multiSelect) {
            this.selected = this.options.filter(option => newVal.includes(option));
          } else {
            this.selected = this.options.find(option => option === newVal) || newVal;
            this.searchFilter = this.selected;
          }
        },
        immediate: true
      },
    }
  };
</script>