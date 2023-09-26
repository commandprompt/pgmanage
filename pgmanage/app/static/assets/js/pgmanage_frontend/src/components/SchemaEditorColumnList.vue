<template>
  <div class="mb-2">
    <div class="d-flex form-row font-weight-bold text-muted schema-editor__header">
      <div :class="commentable ? 'col-2' : 'col-3'">
        <p class="h6">Name</p>
      </div>
      <div :class="commentable ? 'col-2' : 'col-3'">
        <p class="h6">Type</p>
      </div>
      <div :class="commentable ? 'col-2' : 'col-3'">
        <p class="h6">Default</p>
      </div>
      <div class="col-1">
        <p class="h6">Nullable</p>
      </div>
      <div class="col-1">
        <p class="h6">Primary Key</p>
      </div>
      <div v-if="commentable" class="col-3">
        <p class="h6">Comment</p>
      </div>
      <div class="col">
        <p class="h6">Actions</p>
      </div>
    </div>
    <div v-for="(column, index) in columns" :key="column.index"
        :class="['schema-editor__column d-flex form-row flex-nowrap form-group no-gutters',
          { 'schema-editor__column-deleted': column.deleted },
          { 'schema-editor__column-new': column.new }]">
          <div :class="commentable ? 'col-2' : 'col-3'">
              <input type="text" v-model="column.name" class="form-control mb-0" placeholder="column name..." />
          </div>

          <div :class="commentable ? 'col-2' : 'col-3'">
            <SearchableDropdown
              placeholder="type to search"
              :options="dataTypes"
              :default="{ name: 'integer' }"
              :maxItem=20
              v-model="column.dataType"
            />
          </div>

          <div :class="commentable ? 'col-2' : 'col-3'">
            <input type='text' v-model="column.defaultValue" class="form-control mb-0" placeholder="NULL" />
          </div>

          <div class="col-1 d-flex align-items-center">
            <input type='checkbox' class="custom-checkbox" v-model="column.nullable"/>
          </div>

          <div class="col-1 d-flex align-items-center">
            <input type='checkbox' class="custom-checkbox" v-model="column.isPK" />
          </div>

          <div v-if="commentable" class="col-3">
            <input :disabled="this.mode == 'alter'" v-model="column.comment" class="form-control mb-0"
            type="text"
            :placeholder="this.mode == 'alter' ? '' : 'column comment...'" />
          </div>

          <div :class="['col d-flex mr-2', this.movable(column) ? 'justify-content-between': 'justify-content-end']">
            <button v-if="column.deleted && !column.new" @click='column.deleted = false' type="button"
              class="btn btn-icon btn-icon-success" title="Revert">
              <i class="fas fa-rotate-left"></i>
            </button>

            <button v-if="this.movable(column)" @click='moveColumnUp(index)'
              class="btn btn-icon btn-icon-secondary" title="Move column up" type="button">
              <i class="fas fa-circle-up"></i>
            </button>

            <button v-if="this.movable(column)" @click='moveColumnDown(index)'
              class="btn btn-icon btn-icon-secondary" title="Move column down" type="button">
              <i class="fas fa-circle-down"></i>
            </button>

            <button v-if="!column.deleted" @click='removeColumn(index)' type="button"
              class="btn btn-icon btn-icon-danger" title="Remove column">
              <i class="fas fa-circle-xmark"></i>
            </button>
          </div>
    </div>
    <div class="d-flex row no-gutters font-weight-bold mt-2">
      <button @click='addColumn' class="btn btn-outline-success ml-auto">
        Add Column
      </button>
    </div>
  </div>
</template>

<script>
  import SearchableDropdown from "./SearchableDropdown.vue";
  export default {
    name: 'SchemaEditorColumnList',
    data() {
      return {
        columns: [],
        value:'',
      }
    },
    components: {
      SearchableDropdown
    },
    props: {
      initialColumns: Array,
      commentable: Boolean,
      mode: String,
      dataTypes: Array,
    },
    emits: ["columns:changed"],
    methods: {
      addColumn() {
        let colName = `column-${this.columns.length}`
        const defaultCol = {
          dataType: 'integer',
          name: colName,
          defaultValue: null,
          nullable: false,
          isPK: false,
          comment:null,
          new: this.mode === 'alter'
        }
        this.columns.push(defaultCol)
      },
      movable(column) {
        //all columns are movable when creating a new table
        if(this.mode !== 'alter') return true;
        if(column.new) return true;
      },
      removeColumn(index) {
        if(this.mode === 'alter' && !this.columns[index].new) {
          this.columns[index].deleted = true;
        } else {
          this.columns.splice(index, 1)
        }
      },
      moveColumnUp(index) {
        // prevent moving newly added column above existing ones in "alter" mode
        if(index == 0) return;
        if(this.mode === 'alter') {
          let colAbove = this.columns[index-1]
          if(!colAbove.new) return;
        }
        let col = this.columns.splice(index, 1)[0]
        this.columns.splice(index-1, 0, col)
      },
      moveColumnDown(index) {
        if(index == this.columns.length-1) return;
        let col = this.columns.splice(index, 1)[0]
        this.columns.splice(index+1, 0, col)
      },
    },
    watch: {
      initialColumns: {
        handler(newVal, oldVal) {
          this.columns = newVal
        },
        immediate: true
      },
      columns: {
        handler(newVal, oldVal) {
          this.$emit('columns:changed', newVal)
        },
        deep: true
      }
    }
  }
</script>