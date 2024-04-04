import { tabSQLTemplate } from "./tree_postgresql";
import { showToast} from "../notification_control";
import { emitter } from "../emitter";
import { tabsStore } from "../stores/stores_initializer";
import axios from 'axios'

function TemplateSelectSqlite(table, kind) {
  axios.post(
    '/template_select_sqlite/',
    {
      "p_database_index": tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      "p_tab_id": tabsStore.selectedPrimaryTab.id,
      "p_table": table,
      "p_kind": kind
    },
    ).then((resp) => {
      tabsStore.createQueryTab(table, null, null, resp.data.v_data.v_template)

      setTimeout(() => {
        emitter.emit(`${tabsStore.selectedPrimaryTab.metaData.selectedTab.id}_run_query`)
      }, 200)
    }).catch((error) => {
      showToast("error", error.response.data.v_data.message)
      return ''
    })
}

function TemplateInsertSqlite(table) {
  axios.post(
    '/template_insert_sqlite/',
    {
      'p_database_index': tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      'p_tab_id': tabsStore.selectedPrimaryTab.id,
      'p_table': table
    },
    ).then((resp) => {
      tabSQLTemplate(`Insert ${table}`, resp.data.v_data.v_template);
    }).catch((error) => {
      showToast("error", error.response.data.v_data.message)
      return ''
    })
}

function TemplateUpdateSqlite(table) {
  axios.post(
    '/template_update_sqlite/',
    {
      'p_database_index': tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      'p_tab_id': tabsStore.selectedPrimaryTab.id,
      'p_table': table
    },
    ).then((resp) => {
      tabSQLTemplate(`Update ${table}`, resp.data.v_data.v_template);
    }).catch((error) => {
      showToast("error", error.response.data.v_data.message)
      return ''
    })
}

export {
  TemplateSelectSqlite,
  TemplateInsertSqlite,
  TemplateUpdateSqlite,
};