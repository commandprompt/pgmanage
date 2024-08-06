import { tabsStore } from "../stores/stores_initializer.js";
import { emitter } from "../emitter.js";
import { Tooltip } from "bootstrap";

export default {
  mounted() {
    tabsStore.$onAction((action) => {
      if (action.name == "addTab") {
        action.after((result) => {
          if (!result.tooltip) return;
          this.$nextTick(() => {
            const tooltipEl = document.getElementById(`${result.id}`)?.querySelector("[data-bs-toggle='tooltip']")
            if (tooltipEl) {
              new Tooltip(tooltipEl, {
                placement: "right",
                boundary: "window",
                sanitize: false,
                title: result.tooltip,
                html: true,
                delay: { show: 500, hide: 100 },
                offset: [0, 10],
                trigger: 'hover'
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

    },
    contextMenuHandler(event, tab) {
      if (tab.rightClickFunction) {
        event.stopPropagation();
        event.preventDefault();
        tab.rightClickFunction(event, tab);
      }
    },
  },
};
