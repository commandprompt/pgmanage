import { showToast } from "../notification_control";
import { emitter } from "../emitter";
import { tabsStore } from "../stores/stores_initializer";
import axios from 'axios'

function tabSQLTemplate(tab_name, template, showTip=true) {
  tabsStore.createQueryTab(tab_name, null, null, template)
}

function TemplateSelectPostgresql(schema, table, kind) {
  axios.post(
    '/template_select_postgresql/',
    {
      "p_database_index": tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      "p_tab_id": tabsStore.selectedPrimaryTab.id,
      "p_table": table,
      "p_schema": schema,
      "p_kind": kind
    },
    ).then((resp) => {
      let tab_name = `${tabsStore.selectedPrimaryTab.metaData.selectedDatabase}@${schema}.${table}`
      tabsStore.createQueryTab(tab_name, null, null, resp.data.v_data.v_template)
      setTimeout(() => {
        emitter.emit(`${tabsStore.selectedPrimaryTab.metaData.selectedTab.id}_run_query`)
      }, 200)
    }).catch((error) => {
      showToast("error", error.response.data.v_data.message)
      return ''
    })
}


function TemplateInsertPostgresql(schema, table) {
  axios.post(
    '/template_insert_postgresql/',
    {
      "p_database_index": tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      "p_tab_id": tabsStore.selectedPrimaryTab.id,
      "p_table": table,
      "p_schema": schema,
    },
    ).then((resp) => {
        tabSQLTemplate(`Insert ${schema}.${table}`, resp.data.v_data.v_template);
    }).catch((error) => {
      showToast("error", error.response.data.v_data.message)
      return ''
    })
}

function TemplateUpdatePostgresql(schema, table) {
  axios.post(
    '/template_update_postgresql/',
    {
      "p_database_index": tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      "p_tab_id": tabsStore.selectedPrimaryTab.id,
      "p_table": table,
      "p_schema": schema,
    },
    ).then((resp) => {
      tabSQLTemplate(`Update ${schema}.${table}`, resp.data.v_data.v_template);
    }).catch((error) => {
      showToast("error", error.response.data.v_data.message)
      return ''
    })
}

function TemplateSelectFunctionPostgresql(schema, func, functionid) {
  axios.post(
    '/template_select_function_postgresql/',
    {
      "p_database_index": tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      "p_tab_id": tabsStore.selectedPrimaryTab.id,
      "p_function": func,
      "p_functionid": functionid,
      "p_schema": schema
    },
    ).then((resp) => {
      tabSQLTemplate(`Select ${schema}.${func}`, resp.data.v_data.v_template);
    }).catch((error) => {
      showToast("error", error.response.data.v_data.message)
      return ''
    })
}

export {
  tabSQLTemplate,
  TemplateSelectPostgresql,
  TemplateUpdatePostgresql,
  TemplateInsertPostgresql,
  TemplateSelectFunctionPostgresql
};
