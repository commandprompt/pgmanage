<template>
  <div class="modal modal-blurr modal-connections" id="connections-modal" refs="connmodal" tabindex="-1">
    <div class="modal-dialog modal-xl modal-connections__dialog">
      <div class="modal-content modal-connections__content h-100">
        <div class="modal-header align-items-center">
          <h2 class="modal-title font-weight-bold">Manage connections</h2>
          <button type="button" class="close" data-dismiss="modal" aria-label="Close">
            <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
          </button>
        </div>
        <div class="modal-body modal-connections__body p-0">
          <div class="row no-gutters h-100">
            <div class="col-3">
              <div class="modal-connections__panel">
                <div class="modal-connections__panel_add add-connection d-flex justify-content-between align-items-center">
                  <p class="add-connection__title p-0 m-0"><span>{{ this.connections.length }}</span> connections</p>
                  <div class="btn-group" role="group" >
                    <button type="button" class="btn btn-success add-connection__btn dropdown-toggle" data-toggle="dropdown" id="add_connection_button">
                      <i class="fa-solid fa-circle-plus"></i> Add
                    </button>
                    <div class="dropdown-menu dropdown-menu-sm" id="add_connection_dropdown_menu">
                      <a class="dropdown-item" @click="newConnection" href="#" id="add_connection_dropdown_item">Connection</a>
                      <a class="dropdown-item" @click="newGroup" href="#">Group</a>
                    </div>
                  </div>
                </div>

                <div class="modal-connections__list position-absolute w-100" id="connectionsList">
                  <div class="accordion">
                      <!-- GROUP ITEM -->
                    <div v-for="(group, index) in groupedConnections" :key=index class="card">
                      <div @click="showForm('group', group)" class="card-header d-flex justify-content-between align-items-center collapsed" v-bind:id="'group-header-' + group.id" v-bind:data-target="'#collapse-group-' + group.id" data-toggle="collapse">
                        <h4 class="clipped-text mb-0">
                          {{ group.name }}
                        </h4>
                        <i class="fa-solid fa-chevron-down"></i>
                      </div>

                      <div v-bind:id="'collapse-group-' + group.id" class="collapse" data-parent="#connectionsList">
                        <div class="card-body p-0">
                          <ul class="list-group">
                            <li @click="showForm('connection', connection)" v-for="(connection, index) in group.connections" :key=index
                              :class="['connection', 'list-group-item', { 'active':connection===selectedConnection }]">
                              <p class="connection__name">{{ connection.alias }}</p>
                              <p class="connection__subtitle muted-text clipped-text">{{ connectionSubtitle(connection) }}</p>
                            </li>
                          </ul>
                        </div>
                      </div>
                    </div>

                      <!-- GROUP ITEM -->
                  </div>

                  <!-- NO GROUP CONNECTION-->
                  <ul class="list-group">
                    <li @click="showForm('connection', connection)"
                      v-for="(connection, index) in ungroupedConnections" :key=index
                      :class="['connection', 'list-group-item']">
                      <p class="connection__name">{{ connection.alias }}</p>
                      <span class="connection__subtitle muted-text">{{ connectionSubtitle(connection) }}</span>
                    </li>
                  </ul>
                  <!-- NO GROUP CONNECTION END -->
                </div>
              </div>
            </div>

                <ConnectionsModalGroupForm
                  @group:save="saveGroup"
                  @group:delete="deleteGroup"
                  :initialGroup="selectedGroup"
                  :ungroupedConnections="ungroupedConnections"
                  :connectionSubtitle="connectionSubtitle"
                  :visible="activeForm == 'group'"
                />

                <ConnectionsModalConnectionForm
                  @connection:save="saveConnection"
                  @connection:delete="deleteConnection"
                  @connection:test="testConnection"
                  :initialConnection="selectedConnection"
                  :technologies="technologies"
                  :groups="groups"
                  :visible="activeForm == 'connection'"
                />
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>

export default {
  name: 'ConnectionsModal',
  data() {
      return {
        groups: [],
        connections: [],
        technologies: [],
        selectedConnection: {},
        selectedGroup: undefined,
        activeForm: 'group'
      }
  },
  components: {
    'ConnectionsModalGroupForm': Vue.defineAsyncComponent(() => loadModule('/static/OmniDB_app/js/vuejs/components/ConnectionsModalGroupForm.vue', options)),
    'ConnectionsModalConnectionForm': Vue.defineAsyncComponent(() => loadModule('/static/OmniDB_app/js/vuejs/components/ConnectionsModalConnectionForm.vue', options)),
  },
  computed: {
    // adds group's connections to its .connections property
    // returns array of resulting groups
    groupedConnections() {
      let groups = this.groups.map(group => {
        const groupClone = {...group}
        groupClone.connections = this.connections
          .filter(conn => group.conn_list.includes(conn.id))
          .sort((a, b) => (a.name > b.name) ? 1 : -1)
        return groupClone
      }).sort((a, b) => (a.name > b.name) ? 1 : -1)

      return groups;
    },
    ungroupedConnections() {
      let grouped_connection_ids = this.groups.map(group => group.conn_list).flat()
      return this.connections
        .filter(conn => !grouped_connection_ids.includes(conn.id))
        .sort((a, b) => (a.name > b.name) ? 1 : -1)
    }
  },
  methods: {
    getGroups() {
      axios({
        method: 'post',
        url: '/get_groups/',
      })
      .then((response) => {
        this.groups = response.data.data
      })
      .catch((error) => {
        this.groups = []
        console.log(error)
      })
    },
    getConnections() {
      axios({
        method: 'post',
        url: '/get_connections/',
      })
      .then((response) => {
        this.connections = response.data.data.connections
        this.technologies = response.data.data.technologies
      })
      .catch((error) => {
        this.groups = []
        console.log(error)
      })
    },
    loadData() {
      this.getGroups();
      this.getConnections();
      this.activeForm = undefined
    },

    saveConnection(connection) {
      axios.post('/save_connection/', connection)
      .then((response) => {
        this.loadData()
        let event = new CustomEvent('connection:changed',)
        document.dispatchEvent(event)
      })
      .catch((error) => {
        console.log(error)
      })
    },
    testConnection(connection) {
      axios.post('/test_connection/', connection)
      .then((response) => {
        let event = new CustomEvent('connection:tested', { detail: response.data })
        document.dispatchEvent(event)
      })
      .catch((error) => {
        console.log(error)
      })
    },
    deleteConnection(connection) {
      axios.post('/delete_connection/', connection)
      .then((response) => {
        this.loadData()
        let event = new CustomEvent('connection:changed',)
        document.dispatchEvent(event)
      })
      .catch((error) => {
        console.log(error)
      })
    },
    saveGroup(group) {
      axios.post('/save_group/', group)
      .then((response) => {
        this.loadData()
        let event = new CustomEvent('connection:changed',)
        document.dispatchEvent(event)
      })
      .catch((error) => {
        console.log(error)
      })
    },
    deleteGroup(group) {
      axios.post('/delete_group/', group)
      .then((response) => {
        this.loadData()
        let event = new CustomEvent('connection:changed',)
        document.dispatchEvent(event)
      })
      .catch((error) => {
        console.log(error)
      })
    },
    connectionSubtitle(connection) {
      const TECHMAP = {
        sqlite: 'SQLite3',
        terminal: 'Terminal',
        mysql: 'MySQL',
        mariadb: 'MariaDB',
        postgresql: 'PostgreSQL',
        oracle: 'Oracle'
      }

      let techname = TECHMAP[connection.technology]

      if(connection.technology === 'sqlite') return `${techname} - ${connection.service}`
      if(connection.technology === 'terminal') return `${techname} - ${connection.tunnel.server}:${connection.tunnel.port}`
      return `${techname} - ${connection.server}:${connection.port}`
    },
    showForm(form_type, object) {
      if(form_type === 'group') {
        this.activeForm = 'group'
        this.selectedGroup = object
      }
      if(form_type === 'connection') {
        this.activeForm = 'connection'
        this.selectedConnection = object
      }
    },
    newConnection() {
      this.activeForm = 'connection'
      this.selectedConnection = undefined
    },
    newGroup() {
      this.activeForm = 'group'
      this.selectedGroup = undefined
    }
  },
  mounted() {
    $('#connections-modal').on("shown.bs.modal", this.loadData)
  },
}
</script>

<style>
</style>