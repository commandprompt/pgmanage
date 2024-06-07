<template>
  <div class="modal modal-connections fade" id="connections-modal" ref="connmodal" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog modal-xl modal-connections__dialog">
      <div class="modal-content modal-connections__content h-100">
        <div class="modal-header align-items-center">
          <h2 class="modal-title fw-bold">Manage connections</h2>
          <button type="btn" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Close"></button>
        </div>
        <div class="modal-body modal-connections__body p-0">
          <div class="row g-0 h-100">
            <div class="col-3 position-relative">
              <div class="modal-connections__panel">
                <div class="modal-connections__panel_add add-connection d-flex justify-content-between align-items-center">
                  <p class="add-connection__title p-0 m-0"><span>{{ this.connections.length }}</span> connections</p>
                  <div class="btn-group" role="group" >
                    <button type="button" class="btn btn-success add-connection__btn dropdown-toggle" data-bs-toggle="dropdown" id="add_connection_button">
                      <i class="fa-solid fa-circle-plus"></i> Add
                    </button>
                    <div class="dropdown-menu dropdown-menu-sm" id="add_connection_dropdown_menu">
                      <a class="dropdown-item" @click="showForm('connection')" href="#" id="add_connection_dropdown_item">Connection</a>
                      <a class="dropdown-item" @click="showForm('group')" href="#">Group</a>
                    </div>
                  </div>
                </div>

                <div class="modal-connections__list position-absolute w-100" id="connectionsList">
                  <div class="accordion">
                      <!-- GROUP ITEM -->
                    <div v-for="(group, index) in groupedConnections" :key=index class="card rounded-0 border-0">
                      <div @click.prevent.stop="showForm('group', group)" class="card-header d-flex justify-content-between align-items-center collapsed" v-bind:id="'group-header-' + group.id" v-bind:data-target="'#collapse-group-' + group.id" data-bs-toggle="collapse">
                        <h4 class="clipped-text mb-0">
                          {{ group.name }}
                        </h4>
                        <i class="fa-solid fa-chevron-down"></i>
                      </div>

                      <div v-bind:id="'collapse-group-' + group.id" class="collapse" data-bs-parent="#connectionsList">
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
                  :initialConnection="selectedConnection"
                  :technologies="technologies"
                  :visible="activeForm == 'connection'"
                  ref='connectionForm'
                />
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import ConnectionsModalConnectionForm from './ConnectionsModalConnectionForm.vue'
import ConnectionsModalGroupForm from './ConnectionsModalGroupForm.vue'
import { startLoading, endLoading } from '../ajax_control'
import axios from 'axios'
import { emitter } from '../emitter'
import { messageModalStore, tabsStore } from '../stores/stores_initializer';
import { settingsStore, connectionsStore } from "../stores/stores_initializer";
import { useVuelidate } from '@vuelidate/core'
import { Modal, Collapse } from 'bootstrap'

export default {
  name: 'ConnectionsModal',
  setup() {
    return {
      v$: useVuelidate()
    }
  },
  data() {
      return {
        technologies: [],
        selectedConnection: {},
        selectedGroup: undefined,
        activeForm: undefined
      }
  },
  components: {
    'ConnectionsModalGroupForm': ConnectionsModalGroupForm,
    'ConnectionsModalConnectionForm': ConnectionsModalConnectionForm
  },
  computed: {
    connections() {
      return connectionsStore.connections
    },
    // adds group's connections to its .connections property
    // returns array of resulting groups
    groupedConnections() {
      let groups = connectionsStore.groups.map(group => {
        const groupClone = {...group}
        groupClone.connections = this.connections
          .filter(conn => group.conn_list.includes(conn.id))
          .sort((a, b) => (a.name > b.name) ? 1 : -1)
        return groupClone
      }).sort((a, b) => (a.name > b.name) ? 1 : -1)

      return groups;
    },
    ungroupedConnections() {
      let grouped_connection_ids = connectionsStore.groups.map(group => group.conn_list).flat()
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
    },

    saveConnection(connection) {
      axios.post('/save_connection/', connection)
      .then((response) => {
        let unsubscribe = connectionsStore.$subscribe((mutation, state) => {
          if (mutation.type === 'patch object' && mutation.payload.connections) {
            this.selectedConnection = state.connections.find((conn) => 
            conn.id === connection.id || conn.alias === connection.alias) ?? {}
            unsubscribe()
          }
        });
        this.loadData()
      })
      .catch((error) => {
        console.log(error)
      })
    },
    deleteConnection(connection) {
      axios.post('/delete_connection/', connection)
      .then((response) => {
        this.loadData()
        this.selectedConnection = {}
        this.activeForm = undefined
      })
      .catch((error) => {
        console.log(error)
      })
    },
    saveGroup(group) {
      axios.post('/save_group/', group)
      .then((response) => {
        let unsubscribe = connectionsStore.$subscribe((mutation, state) => {
          if (mutation.type === 'patch object' && mutation.payload.groups) {
            this.selectedGroup = this.groupedConnections.find((groupedConn) => 
            groupedConn.id === group.id || groupedConn.name === group.name) ?? {}
            unsubscribe()
          }
        });
        this.loadData()
      })
      .catch((error) => {
        console.log(error)
      })
    },
    deleteGroup(group) {
      axios.post('/delete_group/', group)
      .then((response) => {
        this.loadData()
        this.selectedGroup = {}
        this.activeForm = undefined
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
    showForm(form_type, object = undefined) {
      if (this.v$.$anyDirty) {
        messageModalStore.showModal(
          "Are you sure you wish to discard the current changes?",
          () => {
            this.v$.$reset();
            this.showForm(form_type, object);
          },
          null
        );
      } else {
        if (form_type === 'group') {
          this.activeForm = 'group';
          this.selectedConnection = {};
          if (object) {
            Collapse.getOrCreateInstance(`#collapse-group-${object.id}`).toggle()
            this.selectedGroup = object;
          } else {
            this.selectedGroup = undefined;
          }
        }
        if (form_type === 'connection') {
          this.selectedGroup = {};
          this.activeForm = 'connection';
          this.selectedConnection = object ? object : undefined;
        }
      }
    },
    getExistingTabs() {
      axios
        .post("/get_existing_tabs/")
        .then((response) => {
          if (connectionsStore.connections.length > 0) {
            // Create existing tabs
            let currentParent = null;

            response.data.existing_tabs.forEach((tab, index) => {
                let tooltip_name = "";
                if (currentParent !== tab.index) {
                  startLoading();
                  const conn = connectionsStore.connections.find(
                    (c) => c.id === tab.index
                  );

                  if (conn) {
                    if (conn.alias) {
                      tooltip_name += `<h5 class="mb-1">${conn.alias}</h5>`;
                    }
                    if (conn.details1) {
                      tooltip_name += `<div class="mb-1">${conn.details1}</div>`;
                    }
                    if (conn.details2) {
                      tooltip_name += `<div class="mb-1">${conn.details2}</div>`;
                    }
                    currentParent = tab.index;
                    tabsStore
                      .createConnectionTab(
                        tab.index,
                        false,
                        conn.alias,
                        tooltip_name
                      )
                      .then((connTab) => {
                        tabsStore.createConsoleTab(connTab.id);

                        response.data.existing_tabs
                          .filter((databaseTab) => databaseTab.index === tab.index)
                          .forEach((databaseTab) => {
                            tabsStore.createQueryTab(
                              databaseTab.title,
                              databaseTab.tab_db_id,
                              databaseTab.database_name,
                              databaseTab.snippet,
                              connTab.id
                            );
                          });
                      });
                  }
                }
                endLoading();
            });
          }
        })
        .catch((response) => {
          console.log(response);
        });
    },
  },
  mounted() {
    this.loadData(settingsStore.restoreTabs)

    
    this.$refs.connmodal.addEventListener("shown.bs.modal", () => {
      this.loadData(false)})
    
    this.$refs.connmodal.addEventListener("hide.bs.modal", (event) => {
      if (this.v$.$anyDirty) {
        event.preventDefault()
        messageModalStore.showModal(
          "Are you sure you wish to discard the current changes?",
          () => {
            this.v$.$reset()
            this.$refs.connectionForm.reset()
            Modal.getOrCreateInstance(this.$refs.connmodal).hide()
          },
          null
        );
      }
    })

    emitter.on("connection-save", (connection) => {
      this.saveConnection(connection)
    })
  },
}
</script>

<style>
</style>