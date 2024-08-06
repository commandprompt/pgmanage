import { connectionsStore, tabsStore } from "../stores/stores_initializer";
import { Tooltip } from "bootstrap";

export default {
  methods: {
    subscribeToConnectionChanges(tabId, databaseIndex) {
      connectionsStore.$subscribe((mutation, state) => {
        if (!mutation.payload?.connections) return;
        let tab = tabsStore.getPrimaryTabById(tabId);
        let databaseConnection = connectionsStore.getConnection(databaseIndex);

        let tooltipName = "";

        if (
          databaseConnection.conn_string &&
          databaseConnection.conn_string !== ""
        ) {
          if (databaseConnection.alias) {
            tooltipName += `<h5 class="my-1">${databaseConnection.alias}</h5>`;
          }
          tooltipName += `<div class="mb-1">${databaseConnection.conn_string}</div>`;
        } else {
          if (databaseConnection.alias) {
            tooltipName += `<h5 class="my-1">${databaseConnection.alias}</h5>`;
          }
          if (databaseConnection.details1) {
            tooltipName += `<div class="mb-1">${databaseConnection.details1}</div>`;
          }
          if (databaseConnection.details2) {
            tooltipName += `<div class="mb-1">${databaseConnection.details2}</div>`;
          }
        }
        tab.tooltip = tooltipName;
        tab.name = databaseConnection.alias;
        const tooltipEl = document.getElementById(`${tabId}`).querySelector("[data-bs-toggle='tooltip']")
        Tooltip.getInstance(tooltipEl).dispose();
        new Tooltip(tooltipEl, {
          placement: "right",
          boundary: "window",
          sanitize: false,
          title: tab.tooltip,
          html: true,
          delay: { show: 500, hide: 100 },
          offset: [0, 10],
          trigger: 'hover'
        });
      });
    },
  },
};
