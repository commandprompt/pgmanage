import { showToast } from "../notification_control";
import { emitter } from "../emitter";
import { tabsStore } from "../stores/stores_initializer";
import axios from "axios";

function tabSQLTemplate(tab_name, template, showTip = true) {
  tabsStore.createQueryTab(tab_name, null, null, template);
}

function TemplateSelectPostgresql(schema, table, kind) {
  axios
    .post("/template_select_postgresql/", {
      database_index:
        tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      tab_id: tabsStore.selectedPrimaryTab.id,
      table: table,
      schema: schema,
      kind: kind,
    })
    .then((resp) => {
      let tab_name = `${tabsStore.selectedPrimaryTab.metaData.selectedDatabase}@${schema}.${table}`;
      tabsStore.createQueryTab(tab_name, null, null, resp.data.template);
      setTimeout(() => {
        emitter.emit(
          `${tabsStore.selectedPrimaryTab.metaData.selectedTab.id}_run_query`
        );
      }, 200);
    })
    .catch((error) => {
      showToast("error", error.response.data.data);
    });
}

function TemplateInsertPostgresql(schema, table) {
  axios
    .post("/template_insert_postgresql/", {
      database_index:
        tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      tab_id: tabsStore.selectedPrimaryTab.id,
      table: table,
      schema: schema,
    })
    .then((resp) => {
      tabSQLTemplate(`Insert ${schema}.${table}`, resp.data.template);
    })
    .catch((error) => {
      showToast("error", error.response.data.data);
    });
}

function TemplateUpdatePostgresql(schema, table) {
  axios
    .post("/template_update_postgresql/", {
      database_index:
        tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      tab_id: tabsStore.selectedPrimaryTab.id,
      table: table,
      schema: schema,
    })
    .then((resp) => {
      tabSQLTemplate(`Update ${schema}.${table}`, resp.data.template);
    })
    .catch((error) => {
      showToast("error", error.response.data.data);
    });
}

function TemplateSelectFunctionPostgresql(schema, func, functionid) {
  axios
    .post("/template_select_function_postgresql/", {
      database_index:
        tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      tab_id: tabsStore.selectedPrimaryTab.id,
      function: func,
      functionid: functionid,
      schema: schema,
    })
    .then((resp) => {
      tabSQLTemplate(`Select ${schema}.${func}`, resp.data.template);
    })
    .catch((error) => {
      showToast("error", error.response.data.data);
    });
}

export {
  tabSQLTemplate,
  TemplateSelectPostgresql,
  TemplateUpdatePostgresql,
  TemplateInsertPostgresql,
  TemplateSelectFunctionPostgresql,
};
