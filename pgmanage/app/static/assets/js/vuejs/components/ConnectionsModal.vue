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
                      :class="['connection', 'list-group-item', { 'active':connection===selectedConnection }]">
                      <p class="connection__name">{{ connection.alias }}</p>
                      <p class="connection__subtitle muted-text clipped-text">{{ connectionSubtitle(connection) }}</p>
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
        technologies: [],
        selectedConnection: {},
        selectedGroup: undefined,
        activeForm: 'group'
      }
  },
  components: {
    'ConnectionsModalGroupForm': Vue.defineAsyncComponent(() => loadModule('/static/assets/js/vuejs/components/ConnectionsModalGroupForm.vue', options)),
    'ConnectionsModalConnectionForm': Vue.defineAsyncComponent(() => loadModule('/static/assets/js/vuejs/components/ConnectionsModalConnectionForm.vue', options)),
  },
  computed: {
    groups() {
      return connectionsStore.groups
    },
    connections() {
      return connectionsStore.connections
    },
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
        connectionsStore.$patch({
          groups: response.data.data
        }
        )
      })
      .catch((error) => {
        connectionsStore.$patch({
          groups: []
        })
        console.log(error)
      })
    },
    getConnections(init) {
      axios({
        method: 'post',
        url: '/get_connections/',
      })
      .then((response) => {
        connectionsStore.$patch({
          connections: response.data.data.connections,
        })
        this.technologies = response.data.data.technologies

        if (init) {
          this.getExistingTabs()
        }
      })
      .catch((error) => {
        connectionsStore.$patch({
          connections: []
        })
        console.log(error)
      })
    },
    loadData(init) {
      this.getGroups();
      this.getConnections(init);
      this.activeForm = undefined
    },

    saveConnection(connection) {
      axios.post('/save_connection/', connection)
      .then((response) => {
        this.loadData()
        this.selectedConnection = {}
      })
      .catch((error) => {
        console.log(error)
      })
    },
    testConnection(connection) {
      axios.post('/test_connection/', connection)
      .then((response) => {
        if(response.data.status !== 'success') {
            showError(response.data.data);
        } else {
            showAlert(response.data.data);
        }
      })
      .catch((error) => {
        console.log(error)
      })
    },
    deleteConnection(connection) {
      axios.post('/delete_connection/', connection)
      .then((response) => {
        this.loadData()
      })
      .catch((error) => {
        console.log(error)
      })
    },
    saveGroup(group) {
      axios.post('/save_group/', group)
      .then((response) => {
        this.loadData()
        // set selected group to something so that if "new group"
        // is clicked again the watcher in group form will fire
        this.selectedGroup = {}
      })
      .catch((error) => {
        console.log(error)
      })
    },
    deleteGroup(group) {
      axios.post('/delete_group/', group)
      .then((response) => {
        this.loadData()
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

      if(connection.conn_string) return `${techname} - ${connection.conn_string}`

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
    },
    getExistingTabs() {
      axios.post('/get_existing_tabs/')
        .then((response) => {
          if (connectionsStore.connections.length > 0) {
            // Create existing tabs
            let currentParent = null;

            response.data.existing_tabs.forEach((tab) => {
              let tooltip_name = "";
              if (currentParent !== tab.index) {
                startLoading();
                const conn = connectionsStore.connections.find((c) => c.id === tab.index)

                if (conn) {
                  if (conn.alias) {
                    tooltip_name +=
                      `<h5 class="mb-1">${conn.alias}</h5>`;
                  }
                  if (conn.details1) {
                    tooltip_name +=
                      `<div class="mb-1">${conn.details1}</div>`;
                  }
                  if (conn.details2) {
                    tooltip_name +=
                      `<div class="mb-1">${conn.details2}</div>`;
                  }

                  window.v_connTabControl.tag.createConnTab(
                    tab.index,
                    false,
                    conn.alias,
                    tooltip_name
                  );
                  window.v_connTabControl.tag.createConsoleTab();

                }

              }
              currentParent = tab.index

              window.v_connTabControl.tag.createQueryTab(tab.title, tab.tab_db_id);
              window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.setValue(tab.snippet);
              window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.clearSelection();
              window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor.gotoLine(
                0,
                0,
                true);
            })
            endLoading();
          }
        })
        .catch((response) => {
          console.log(response)
        })
    }
  },
  mounted() {
    this.loadData(true)
    $('#connections-modal').on("shown.bs.modal", () => {
      this.loadData(false)})
  },
}
</script>

<style>
</style>