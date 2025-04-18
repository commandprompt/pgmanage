<template>
<div v-if="visible" class="col-9 d-flex ms-auto position-relative">
  <div class="modal-connections__forms position-absolute w-100">
    <div class="modal-connections__forms_group group-edit-form position-absolute">
      <div class="form-group mb-3">
        <label for="groupName" class="fw-bold mb-2">Group Name</label>
        <input v-model="v$.groupLocal.name.$model" type="text"
          :class="['form-control', { 'is-invalid': v$.groupLocal.name.$invalid }]" id="groupName" placeholder="Group name" autocomplete="off">
        <div class="invalid-feedback">
          <span v-for="error of v$.groupLocal.name.$errors" :key="error.$uid">
            {{ error.$message }}
          </span>
        </div>
      </div>

      <label class="fw-bold mb-2">Group connections</label>
      <div class="group-edit-form__list group-list d-flex flex-wrap">
        <div v-for="(connection, index) in candidateConnections" :key=index class="group-list__item">
          <input
            v-bind:id="'connection-' + connection.id"
            v-model="this.groupLocal.conn_list"
            v-bind:value="connection.id"
            class="custom-checkbox"
            type="checkbox">
          <label v-bind:for="'connection-' + connection.id" class="group-list__item_wrap d-flex align-items-center m-0">
            <div class="group-list__item_logo mx-3">
              <div
              :class="['icon', 'icon-' + connection.technology]"
              ></div>
            </div>
            <div class="group-list__item_text d-flex flex-column">
              <p class="group-list__item_title">{{ connection.alias }}</p>
              <span class="group-list__item_subtitle muted-text line-clamp-text clipped-text">{{ this.connectionSubtitle(connection) }}</span>
            </div>
          </label>
        </div>
      </div>
    </div>
  </div>
  <div class="modal-footer mt-auto justify-content-between w-100">
    <ConfirmableButton v-if="groupLocal.id" :callbackFunc="() => $emit('group:delete', groupLocal)" class="btn btn-outline-danger" />
    <button type="button" 
      @click="trySave()" 
      class="btn btn-primary ms-auto"
      :disabled="v$.$invalid || (!isChanged && !!this.groupLocal.id)"
      >Save changes</button>
  </div>
</div>
</template>

<script>
  import { useVuelidate } from '@vuelidate/core'
  import { required, maxLength } from '@vuelidate/validators'
  import isEqual from 'lodash/isEqual';
  import ConfirmableButton from './ConfirmableButton.vue'
  export default {
    name: 'ConnectionsModalGroupForm',
    components: {
      ConfirmableButton
    },
    data() {
      return {
        groupLocal: {
          name: 'New Group',
          connections: [],
          conn_list: []
        },
      }
    },
    validations() {
      let baseRules = {
        groupLocal: {
          name: {
            required: required,
            maxLength: maxLength(30),
          },
        }
      }
      return baseRules
    },
    setup() {
      return { v$: useVuelidate({ $lazy: true }) }
    },
    props: {
      visible: Boolean,
      initialGroup: {
        type: Object,
        required: true,
        default: {
          name: 'New Group',
          connections: [],
          conn_list: []
        }
      },
      connectionSubtitle: Function,
      ungroupedConnections: {
        type: Array,
        required: true
      }
    },
    computed: {
      candidateConnections() {
        return [...this.ungroupedConnections, ...this.initialGroup.connections]
          .sort((a, b) => (a.alias > b.alias) ? 1 : -1)
      },
      isChanged() {
        this.initialGroup.connections.sort()
        this.initialGroup.conn_list.sort()
        this.groupLocal.connections.sort()
        this.groupLocal.conn_list.sort()
        return !isEqual(this.initialGroup, this.groupLocal)
      }
    },
    methods: {
      trySave() {
        this.v$.groupLocal.$validate()
        if(!this.v$.$invalid) {
          this.$emit('group:save', this.groupLocal)
        }
      },
    },
    watch: {
      initialGroup(newVal, oldVal) {
        this.groupLocal = {...newVal}
        this.v$.groupLocal.$reset()
      }
    }
  }
</script>