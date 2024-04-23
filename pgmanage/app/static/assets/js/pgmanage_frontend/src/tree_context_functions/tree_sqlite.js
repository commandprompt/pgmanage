import { tabSQLTemplate } from "./tree_postgresql";
import { showToast } from "../notification_control";
import { emitter } from "../emitter";
import { tabsStore } from "../stores/stores_initializer";
import axios from "axios";

function TemplateSelectSqlite(table, kind) {
  axios
    .post("/template_select_sqlite/", {
      database_index:
        tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      tab_id: tabsStore.selectedPrimaryTab.id,
      table: table,
      kind: kind,
    })
    .then((resp) => {
      tabsStore.createQueryTab(table, null, null, resp.data.template);

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

function TemplateInsertSqlite(table) {
  axios
    .post("/template_insert_sqlite/", {
      database_index:
        tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      tab_id: tabsStore.selectedPrimaryTab.id,
      table: table,
    })
    .then((resp) => {
      tabSQLTemplate(`Insert ${table}`, resp.data.template);
    })
    .catch((error) => {
      showToast("error", error.response.data.data);
    });
}

function TemplateUpdateSqlite(table) {
  axios
    .post("/template_update_sqlite/", {
      database_index:
        tabsStore.selectedPrimaryTab.metaData.selectedDatabaseIndex,
      tab_id: tabsStore.selectedPrimaryTab.id,
      table: table,
    })
    .then((resp) => {
      tabSQLTemplate(`Update ${table}`, resp.data.template);
    })
    .catch((error) => {
      showToast("error", error.response.data.data);
    });
}

export { TemplateSelectSqlite, TemplateInsertSqlite, TemplateUpdateSqlite };
