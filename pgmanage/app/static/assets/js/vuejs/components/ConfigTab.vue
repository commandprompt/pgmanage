<template>
  <div class="form-row">
    <div class="form-group col-2">
      <form class="form" role="search" @submit.prevent>
        <label class="font-weight-bold mb-3" for="selectServer">Search</label>
        <input v-model.trim="query_filter" class="form-control" id="inputSearchSettings" name="filter"
          :disabled="v$.$invalid" placeholder="Find in settings" />
      </form>
    </div>

    <div class="form-group col-3">
      <label class="font-weight-bold mb-3" for="selectConfCat">Category</label>
      <select class="form-control text-truncate pr-4" id="selectConfCat" :disabled="!!query_filter || v$.$invalid" v-model="selected">
        <option v-for="(cat, index) in categories" :value="cat" :key="index">
          {{ cat }}
        </option>
      </select>
    </div>

    <div class="form-group col-3">
      <label class="font-weight-bold mb-3" for="selectConf">Config History</label>
      <select class="form-control text-truncate" id="selectConf" v-model="selectedConf">
        <option disabled value="">Please select one</option>
        <option v-for="(config, index) in configHistory" :value="config" :key="index"
          :title="config.commit_comment">
         {{ truncateText(config, 50) }}
        </option>
      </select>
    </div>

    <div class="form-group col-4 d-flex align-items-end pl-0">
      <button class="btn btn-success mr-2" :disabled="!selectedConf" @click="confirmConfig(e, true)">
        <i class="fa-solid fa-arrow-rotate-left"></i>
      </button>
      <button class="btn btn-danger mr-2" :disabled="!selectedConf"
        @click="deleteOldConfig(selectedConf.id)">
        <i class="fa-solid fa-xmark"></i>
      </button>
      <button type="submit" class="btn btn-success ml-auto" :disabled="!hasUpdateValues || v$.$invalid"
        @click.prevent="confirmConfig">
        Apply
      </button>
    </div>
  </div>

  <div v-if="!hasResult" class="row">
    <div class="col-12">
      <p>No results found...</p>
    </div>
  </div>

  <div v-else class="row">
    <div class="col-12">
      <div class="config-tabgroup">

        <div v-if="appliedSettings.restartPending" id="alert-configuration" class="alert alert-warning alert-dismissible" role="alert">
          <h2><i class="fa fa-warning fa-fw"></i>WARNING</h2>
          <p>Some changes are pending and PostgreSQL should be restarted:</p>
          <ul>
            <li v-for="change in appliedSettings.restartChanges" :key="change.name">
              {{ change.name }}
            </li>
          </ul>
        </div>

        <div v-if="hasAppliedValues">
          <div id="ok-configuration" class="alert alert-success alert-dismissible" role="alert">
            <button type="button" class="close" aria-label="Close" @click="appliedSettings.data = ''">
              <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
            </button>
            <p class="text-center">The following changes have been applied:</p>
            <table class="table table-sm table-borderless">
              <tr>
                <th width="30%">Name</th>
                <th width="30%">Prev. value</th>
                <th width="40%">New value</th>
              </tr>
              <tr v-for="setting in appliedSettings.data" :key="setting.name">
                <td>{{ setting.name }}</td>
                <td>{{ setting.previous_setting }}</td>
                <td>
                  <b>{{ setting.setting }}</b>
                </td>
              </tr>
            </table>
          </div>
        </div>

        <ConfigTabGroup v-for="setting_group in currentResult" :initial-group="setting_group"
        :key="setting_group.category" @group-change="changeData" />
      </div>
    </div>
  </div>

  <!--Modal for commit messaging and returning result-->
  <div class="modal fade" :id="modalId" tabindex="-1" role="dialog" aria-hidden="true">
    <div class="modal-dialog" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 v-if="!modalRevertConfig" class="modal-title">Config Management</h2>
          <h2 v-else class="modal-title">Revert Configuration</h2>
          <button type="button" class="close" data-dismiss="modal" aria-label="Close">
            <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
          </button>
        </div>
        <div class="modal-body">
          <p class="text-center">The following changes will be applied:</p>
          <table class="table table-sm">
            <tr>
              <th width="50%" class="border-top-0">Name</th>
              <th width="50%" class="border-top-0">New value</th>
            </tr>
            <template v-if="!modalRevertConfig">
              <tr v-for="(setting_value, setting_name) in updateSettings" :key="setting_value">
                <td>{{ setting_name }}</td>
                <td>
                  <b>{{ setting_value }}</b>
                </td>
              </tr>
            </template>
            <template v-else>
              <tr v-for="(setting_value, setting_name) in configDiffData" :key="setting_value">
                <td>{{ setting_name }}</td>
                <td>
                  <b>{{ setting_value }}</b>
                </td>
              </tr>
            </template>
          </table>
          <div v-if="!modalRevertConfig" class="form-group">
            <label for="commit_message" class="font-weight-bold mb-3">Commit Comment</label>
            <input v-model="commitComment" id="commit_message" class="form-control"
              placeholder="Short description of your changes(optional)" />
          </div>
        </div>
        <div class="modal-footer">
          <button v-if="!modalRevertConfig" id="config_modal_button" type="button" class="btn btn-primary mr-2"
            data-dismiss="modal" @click="saveConfiguration">
            Save configuration
          </button>
          <button v-else id="config_modal_button" type="button" class="btn btn-primary mr-2" data-dismiss="modal"
            @click="revertConfig">
            Revert configuration
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import ConfigTabGroup from "./ConfigTabGroup.vue";

export default {
  name: "Config",
  components: {
    ConfigTabGroup,
  },
  setup() {
    return {
      v$: Vuelidate.useVuelidate()
    }
  },
  data() {
    return {
      data: "",
      updateSettings: {},
      selected: "",
      categories: "",
      query_filter: "",
      configHistory: "",
      selectedConf: "",
      commitComment: "",
      tabId: window.v_connTabControl.selectedTab.id,
      databaseId: window.v_connTabControl.selectedTab.tag.selectedDatabaseIndex,
      appliedSettings: {
        data: "",
        restartChanges: "",
        restartPending: "",
      },
      modalId: `config_modal_${this.tabId}_${Date.now()}`,
      modalRevertConfig: false,
      configDiffData: '',
      intervalId: ''
    };
  },
  computed: {
    currentResult() {
      if (!!this.data) {
        if (!this.query_filter) {
          return this.data.filter((item) => item.category === this.selected);
        } else {
          return this.data
            .map((element) => {
              const rows = element.rows.filter(
                (row) => {
                  let distance_name = distance(this.query_filter, row.name)
                  let distance_desc = distance(this.query_filter, row.desc)

                  return (
                    distance_desc > 0.7 ||
                    distance_name > 0.7 ||
                    row.name.toLowerCase().includes(this.query_filter.toLowerCase()) ||
                    row.desc.toLowerCase().includes(this.query_filter.toLowerCase())
                  )
                }
              );
              return { ...element, rows: rows };
            })
            .filter((element) => !!element.rows.length);
        }
      }
    },
    hasResult() {
      return !!this.currentResult?.length;
    },
    hasAppliedValues() {
      return !!this.appliedSettings.data.length;
    },
    hasUpdateValues() {
      return !!Object.keys(this.updateSettings).length;
    },
  },
  mounted() {
    this.getCategories();
    this.getConfiguration();
    this.getConfigHistory();
    this.getConfigStatus();
  },
  methods: {
    getConfiguration() {
      axios
        .post("/configuration/", {
          database_index: this.databaseId,
          tab_id: this.tabId,
        })
        .then((response) => {
          this.data = response.data.settings;
        })
        .catch((error) => {
          this.data = "";
          showError(error.response.data.data);
        });
    },
    getCategories() {
      axios
        .post("/configuration/categories/", {
          database_index: this.databaseId,
          tab_id: this.tabId,
        })
        .then((response) => {
          this.categories = response.data.categories;
          this.selected = this.categories[0];
        })
        .catch((error) => {
          showError(error.response.data.data);
        });
    },
    changeData(e) {
      const index = this.categories.indexOf(e.changedGroup.category);
      this.data[index] = e.changedGroup;
      if (e.changedSetting.setting !== e.changedSetting.reset_val)
        this.updateSettings[e.changedSetting.name] = e.changedSetting.setting;
      else
        delete this.updateSettings[e.changedSetting.name]
    },
    saveConfiguration(event, newConfig = true) {
      axios
        .post("/save_configuration/", {
          database_index: this.databaseId,
          tab_id: this.tabId,
          settings: this.updateSettings,
          commit_comment: this.commitComment,
          new_config: newConfig,
        })
        .then((response) => {
          this.updateSettings = {};
          this.commitComment = "";
          this.appliedSettings.data = response.data.settings;
          this.query_filter = "";
          this.getConfigHistory();
          this.getConfiguration();
          this.getConfigStatus();
        })
        .catch((error) => {
          showError(error.response.data.data);
        });
    },
    getConfigHistory() {
      axios
        .post("/get_configuration_history/", {
          database_index: this.databaseId,
          tab_id: this.tabId,
        })
        .then((response) => {
          this.configHistory = response.data.config_history.map((el) => {
            return {
              ...el,
              start_time: moment(el.start_time).format("DD/MM/YY hh:mm A"),
            };
          });
        })
        .catch((error) => {
          showError(error.response.data.data);
        });
    },
    confirmConfig(event, revert = false) {
      if (!revert) {
        this.modalRevertConfig = false
        $(`#${this.modalId}`).modal();
      } else {
        this.getConfigurationDiffs()
        this.modalRevertConfig = true
        $(`#${this.modalId}`).modal();
      }
    },
    revertConfig(event) {
      this.updateSettings = this.selectedConf.config_snapshot;
      this.saveConfiguration(event, false);
    },
    truncateText(input, max_length) {
      const text = `${input.start_time} - ${input.commit_comment}`;
      return text.length > max_length
        ? text.slice(0, max_length - 1) + "..."
        : text;
    },
    getConfigStatus() {
      axios
        .post("/configuration/status/", {
          database_index: this.databaseId,
          tab_id: this.tabId,
        })
        .then((response) => {
          this.appliedSettings.restartPending = response.data.restart_pending;
          this.appliedSettings.restartChanges = response.data.restart_changes;
          if (this.appliedSettings.restartPending && !this.intervalId) {
            this.intervalId = setInterval(() => {
              this.getConfigStatus()
            }, 30000)
          }
          if (!this.appliedSettings.restartPending && !!this.intervalId) {
            clearInterval(this.intervalId)
            this.getConfiguration()
          }
        })
        .catch((error) => {
          if (error.response.data.data.includes("SSL connection has been closed unexpectedly"))
            this.getConfigStatus()
          else
            showError(error.response.data.data);
        });
    },
    deleteOldConfig(config_id) {
      axios
        .delete(`/configuration/${config_id}/`)
        .then((resp) => {
          this.selectedConf = "";
          this.getConfigHistory();
        })
        .catch((error) => {
          showError(error.response.data.data);
        });
    },
    getConfigurationDiffs() {
      axios
        .post("/configuration/", {
          database_index: this.databaseId,
          tab_id: this.tabId,
          grouped: false,
        })
        .then((response) => {
          let diff = Object.keys(response.data.settings).reduce((diff, key) => {
            if (this.selectedConf.config_snapshot[key] === response.data.settings[key]) return diff
            return {
              ...diff,
              [key]: this.selectedConf.config_snapshot[key]
            }
          }, {})
          this.configDiffData = diff
        })
        .catch((error) => {
          showError(error.response.data.data);
        });
    }
  },
};
</script>

<style scoped>
  .config-tabgroup {
    height: calc(100vh - 200px);
    overflow-y: scroll;
  }
</style>
