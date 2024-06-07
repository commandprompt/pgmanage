import { tabsStore } from "../stores/stores_initializer.js";
import { createRequest } from "../long_polling.js";
import { queryRequestCodes } from "../constants.js";
import { showConfirm } from "../notification_control.js";
import ContextMenu from "@imengyu/vue3-context-menu";
import { emitter } from "../emitter.js";
import { Tooltip } from "bootstrap";

export default {
  mounted() {
    tabsStore.$onAction((action) => {
      if (action.name == "addTab") {
        action.after((result) => {
          if (!result.tooltip) return;
          this.$nextTick(() => {
            const tooltipEl = document.getElementById(`${result.id}`);
            if (tooltipEl) {
              new Tooltip(tooltipEl, {
                placement: "right",
                boundary: "window",
                sanitize: false,
                title: result.tooltip,
                html: true,
                delay: { show: 500, hide: 100 },
              });
            }
          });
        });
      }
    });
  },
  methods: {
    clickHandler(event, tab) {
      if (tab.parentId === null && tab.name !== "Snippets") {
        emitter.emit("hide_snippet_panel");
      }

      if (tab.selectable) {
        tabsStore.selectTab(tab);
      }

      if (tab.clickFunction != null) {
        tab.clickFunction(event);
      }

      if (tab.tooltip) {
        const tooltipTriggerList =
          document.querySelectorAll('[data-bs-toggle="tooltip"]');
        const tooltipList = [...tooltipTriggerList].map((tooltipTriggerEl) =>
          Tooltip.getInstance(tooltipTriggerEl).hide()
        );
      }
    },
    contextMenuHandler(event, tab) {
      if (tab.rightClickFunction) {
        event.stopPropagation();
        event.preventDefault();
        tab.rightClickFunction(event, tab);
      }
      if (tab.tooltip) {
        const tooltipTriggerList =
          document.querySelectorAll('[data-bs-toggle="tooltip"]');
        const tooltipList = [...tooltipTriggerList].map((tooltipTriggerEl) =>
          Tooltip.getInstance(tooltipTriggerEl).hide()
        );
      }
    },
  },
};
