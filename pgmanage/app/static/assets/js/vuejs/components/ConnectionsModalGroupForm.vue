<template>
<div v-if="visible" class="col-9 d-flex ml-auto">
  <div class="modal-connections__forms position-absolute w-100">
    <div class="modal-connections__forms_group group-edit-form position-absolute">
      <div class="form-group mb-3">
        <label for="groupName" class="font-weight-bold mb-3">Group Name</label>
        <input v-model="groupLocal.name" type="text" class="form-control" id="groupName" placeholder="Group name">
      </div>

      <label class="font-weight-bold mb-3">Group connections</label>
      <div class="group-edit-form__list group-list d-flex flex-wrap">
        <div v-for="(connection, index) in candidateConnections" :key=index class="group-list__item">
          <input
            v-bind:id="'connection-' + connection.id"
            v-model="this.groupLocal.conn_list"
            v-bind:value="connection.id"
            type="checkbox">
          <label v-bind:for="'connection-' + connection.id" class="group-list__item_wrap d-flex align-items-center m-0">
            <div class="group-list__item_logo mx-3">
              <div
              :class="['icon', 'icon-' + connection.technology]"
              ></div>
            </div>
            <div class="group-list__item_text d-flex flex-column">
              <p class="group-list__item_title">{{ connection.alias }}</p>
              <span class="group-list__item_subtitle muted-text line-clamp-text clipped-text">{{ connectionSubtitle(connection) }}</span>
            </div>
          </label>
        </div>
      </div>
    </div>
  </div>
  <div class="modal-footer mt-auto justify-content-between w-100">
    <button v-if="groupLocal.id" type="button" @click="$emit('group:delete', this.groupLocal)" class="btn btn-danger">Delete</button>
    <button type="button" @click="$emit('group:save', this.groupLocal)" class="btn btn-primary ml-auto">Save changes</button>
  </div>
</div>
</template>

<script>
  export default {
    name: 'ConnectionsModalGroupForm',
    data() {
      return {
        groupLocal: {
          name: 'New Group',
          connections: [],
          conn_list: []
        }
      }
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
      ungroupedConnections: []
    },
    computed: {
      candidateConnections() {
        return [...this.ungroupedConnections, ...this.initialGroup.connections]
          .sort((a, b) => (a.alias > b.alias) ? 1 : -1)
      }
    },
    // FIXME: this is ugly, move to mixin?
    methods: {
      connectionSubtitle: function(connection) {
        return this.connectionSubtitle(connection)
      },
    },
    watch: {
      initialGroup(newVal, oldVal) {
        this.groupLocal = {...newVal}
      }
    }
  }
</script>