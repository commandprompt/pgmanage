<template>
  <div class="container-fluid position-relative">
    <div class="row">
      <splitpanes class="default-theme">
        <pane size="18">
          <div
            :id="`${connTabId}_div_left`"
            class="omnidb__workspace__div-left col"
          >
            <div class="row">
              <div class="omnidb__workspace__content-left border-right">
                <div class="omnidb__workspace__connection-details">
                  <i class="fas fa-server mr-1"></i>selected DB:
                  <b>{{ databaseName }}</b>
                  <div
                    class="omnidb__switch omnidb__switch--sm float-right"
                    data-toggle="tooltip"
                    data-placement="bottom"
                    data-html="true"
                    title=""
                    data-original-title="<h5>Toggle autocomplete.</h5><div>Switch OFF <b>disables the autocomplete</b> on the inner tabs for this connection.</div>"
                  >
                    <input
                      type="checkbox"
                      :id="`${connTabId}_autocomplete`"
                      class="omnidb__switch--input"
                      v-model="autocompleteStatus"
                      @change="emitConnectionSave"
                    />
                    <label
                      :for="`${connTabId}_autocomplete`"
                      class="omnidb__switch--label"
                    >
                      <span>
                        <i class="fas fa-spell-check"></i>
                      </span>
                    </label>
                  </div>
                </div>
                <splitpanes
                  class="left-div-panes default-theme"
                  horizontal
                  @resize="treeTabsPaneSize = $event[1].size"
                >
                  <pane :size="100 - treeTabsPaneSize">
                    <div :id="`${connTabId}_tree`" class="database-tree">
                      <component
                        :is="treeComponent"
                        :tab-id="connTabId"
                        :database-index="databaseIndex"
                        @tree-tabs-update="getProperties"
                        @clear-tabs="clearTreeTabsData = true"
                      ></component>
                    </div>
                  </pane>

                  <pane
                    min-size="2"
                    :size="treeTabsPaneSize"
                    style="min-height: 2rem"
                  >
                    <TreePropertiesDDL
                      :conn-id="connTabId"
                      :ddl-data="ddlData"
                      :properties-data="propertiesData"
                      :show-loading="showTreeTabsLoading"
                      :clear-data="clearTreeTabsData"
                      @toggle-tree-tabs="toggleTreeTabPane"
                      @data-cleared="clearTreeTabsData = false"
                    />
                  </pane>
                </splitpanes>
              </div>
            </div>
          </div>
        </pane>
        <pane>
          <div
            :id="`${connTabId}_div_right`"
            class="omnidb__workspace__div-right col position-relative"
          >
            <div class="row">
              <DatabaseTabs
                :id="`${connTabId}`"
                class="w-100"
                :tab-id="connTabId"
              />
            </div>
          </div>
        </pane>
      </splitpanes>
    </div>
  </div>
</template>

<script>
import DatabaseTabs from "./DatabaseTabs.vue";
import { tabsStore, connectionsStore } from "../stores/stores_initializer";
import axios from "axios";
import { defineAsyncComponent } from "vue";
import { emitter } from "../emitter";
import { truncateText } from "../utils";
import { Splitpanes, Pane } from "splitpanes";
import TreePropertiesDDL from "./TreePropertiesDDL.vue";
import { showPasswordPrompt } from "../passwords";
import { showToast } from "../notification_control";
import TabTitleUpdateMixin from "../mixins/sidebar_title_update_mixin";

export default {
  name: "ConnectionTab",
  components: {
    DatabaseTabs,
    TreePostgresql: defineAsyncComponent(() => import("./TreePostgresql.vue")),
    TreeSqlite: defineAsyncComponent(() => import("./TreeSqlite.vue")),
    TreeMariaDB: defineAsyncComponent(() => import("./TreeMariaDB.vue")),
    TreeOracle: defineAsyncComponent(() => import("./TreeOracle.vue")),
    TreeMysql: defineAsyncComponent(() => import("./TreeMysql.vue")),
    Splitpanes,
    Pane,
    TreePropertiesDDL,
  },
  mixins: [TabTitleUpdateMixin],
  props: {
    connTabId: String,
  },
  data() {
    return {
      ddlData: "",
      propertiesData: [],
      treeTabsPaneSize: 2,
      lastTreeTabsPaneSize: null,
      showTreeTabsLoading: false,
      clearTreeTabsData: false,
    };
  },
  computed: {
    connectionTab() {
      return tabsStore.getPrimaryTabById(this.connTabId);
    },
    databaseConnection() {
      return connectionsStore.getConnection(this.databaseIndex);
    },
    autocompleteStatus: {
      get() {
        return this.databaseConnection.autocomplete;
      },
      set(value) {
        let connection = connectionsStore.getConnection(this.databaseIndex);
        connection.autocomplete = value;
      },
    },
    databaseIndex() {
      return this.connectionTab.metaData.selectedDatabaseIndex;
    },
    databaseTechnology() {
      return this.connectionTab.metaData.selectedDBMS;
    },
    databaseName() {
      if (this.databaseTechnology === "sqlite") {
        return truncateText(this.connectionTab.metaData.selectedDatabase, 10);
      }
      return this.connectionTab.metaData.selectedDatabase;
    },
    treeComponent() {
      const treeTechnologiesMap = {
        postgresql: "TreePostgresql",
        sqlite: "TreeSqlite",
        mysql: "TreeMysql",
        mariadb: "TreeMariaDB",
        oracle: "TreeOracle",
      };
      return treeTechnologiesMap[this.databaseTechnology];
    },
    isTreeTabsVisible() {
      return this.treeTabsPaneSize !== 2;
    },
  },
  mounted() {
    this.changeDatabase(this.connectionTab.metaData.selectedDatabaseIndex);
    this.subscribeToConnectionChanges(this.connTabId, this.databaseIndex);
    $('[data-toggle="tooltip"]').tooltip({ animation: true }); // Loads or Updates all tooltips
    this.$nextTick(() => {
      if (this.connectionTab.metaData.createInitialTabs) {
        tabsStore.createConsoleTab();
        tabsStore.createQueryTab();
      }
    });
  },
  methods: {
    changeDatabase(value) {
      let connObject = null;

      // Finding the connection object.
      for (let i = 0; i < connectionsStore.connections.length; i++) {
        if (value == connectionsStore.connections[i].id) {
          connObject = connectionsStore.connections[i];
          break;
        }
      }

      // Selecting the first connection when none is found.

      if (!connObject) {
        connObject = connectionsStore.connections[0];
      }

      let tabData = tabsStore.getPrimaryTabById(this.connTabId);

      tabData.metaData.selectedDatabaseIndex = value;
      tabData.metaData.selectedDBMS = connObject.technology;
      tabData.metaData.consoleHelp = connObject.console_help;
      tabData.metaData.selectedDatabase =
        connObject.last_used_database || connObject.service;
      tabData.metaData.selectedTitle = connObject.alias;

      connectionsStore.queueChangeActiveDatabaseThreadSafe({
        database_index: value,
        tab_id: this.connTabId,
        database: tabData.metaData.selectedDatabase,
      });
    },
    emitConnectionSave() {
      let connection = connectionsStore.getConnection(this.databaseIndex);
      emitter.emit("connection-save", connection);
    },
    getProperties({ view, data }) {
      if (!this.isTreeTabsVisible) return;
      this.showTreeTabsLoading = true;
      axios
        .post(view, {
          database_index: this.databaseIndex,
          tab_id: this.connTabId,
          data: data,
        })
        .then((resp) => {
          this.propertiesData = resp.data.properties;
          this.ddlData = resp.data.ddl;
          this.showTreeTabsLoading = false;
        })
        .catch((error) => {
          if (error.response.data.password_timeout) {
            showPasswordPrompt(
              this.databaseIndex,
              () => {
                this.getProperties(view, data);
              },
              null,
              error.response.data.data
            );
          } else {
            showToast("error", error.response.data.data);
          }
          this.showTreeTabsLoading = false;
        });
    },
    toggleTreeTabPane() {
      if (this.treeTabsPaneSize === 2) {
        this.treeTabsPaneSize = this.lastTreeTabsPaneSize || 40;
      } else {
        this.lastTreeTabsPaneSize = this.treeTabsPaneSize;
        this.treeTabsPaneSize = 2;
      }
    },
  },
};
</script>

<style scoped>
.left-div-panes {
  height: calc(100vh - 30px);
}
.database-tree {
  overflow: auto;
  transition: scroll 0.3s;
  height: 100%;
}

.splitpanes .splitpanes__pane {
  transition: none;
}
</style>
