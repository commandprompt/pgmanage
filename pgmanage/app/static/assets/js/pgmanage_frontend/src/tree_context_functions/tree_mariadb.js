import { tabSQLTemplate } from "./tree_postgresql";
import { showToast } from "../notification_control";
import { emitter } from "../emitter";
import { tabsStore } from "../stores/stores_initializer";
import axios from 'axios'

function TemplateSelectMariadb(p_schema, p_table) {
  axios.post(
    '/template_select_mariadb/',
    {
      "p_database_index": tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      "p_tab_id": tabsStore.selectedPrimaryTab.id,
      "p_table": p_table,
      "p_schema": p_schema
    }
    ).then((resp) => {
      let tab_name = `${p_schema}.${p_table}`
      tabsStore.createQueryTab(tab_name, null, null, resp.data.v_data.v_template)

      setTimeout(() => {
          emitter.emit(`${tabsStore.selectedPrimaryTab.metaData.selectedTab.id}_run_query`)
      }, 200)
    }).catch((error) => {
      showToast("error", error.response.data.v_data.message)
    })
}

function TemplateInsertMariadb(p_schema, p_table) {
  axios.post(
    '/template_insert_mariadb/',
    {
      "p_database_index": tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      "p_tab_id": tabsStore.selectedPrimaryTab.id,
      "p_table": p_table,
      "p_schema": p_schema
    },
    ).then((resp) => {
      tabSQLTemplate( `Insert ${p_schema}.${p_table}`, resp.data.v_data.v_template);
    }).catch((error) => {
      showToast("error", error.response.data.v_data.message)
    })
}

function TemplateUpdateMariadb(p_schema, p_table) {
  axios.post(
    '/template_update_mariadb/',
    {
      "p_database_index": tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      "p_tab_id": tabsStore.selectedPrimaryTab.id,
      "p_table": p_table,
      "p_schema": p_schema
    },
    ).then((resp) => {
      tabSQLTemplate( `Update ${p_schema}.${p_table}`, resp.data.v_data.v_template);
    }).catch((error) => {
      showToast("error", error.response.data.v_data.message)
    })
}

export {
  TemplateSelectMariadb,
  TemplateInsertMariadb,
  TemplateUpdateMariadb,
};