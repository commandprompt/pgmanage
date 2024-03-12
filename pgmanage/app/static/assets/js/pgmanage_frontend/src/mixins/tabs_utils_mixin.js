import { tabsStore } from "../stores/stores_initializer.js";
import { createRequest } from "../long_polling.js";
import { queryRequestCodes } from "../constants.js";
import { showConfirm } from "../notification_control.js";
import ContextMenu from "@imengyu/vue3-context-menu";

export default {
  mounted() {
    tabsStore.$onAction((action) => {
      if (action.name == "addTab") {
        action.after((result) => {
          if (!result.tooltip) return;
          this.$nextTick(() => {
            $(`#${result.id}`).tooltip({
              placement: "right",
              boundary: "window",
              sanitize: false,
              title: result.tooltip,
              html: true,
              delay: { show: 500, hide: 100 },
            });
          });
        });
      }
    });
  },
  methods: {
    clickHandler(event, tab) {
      if (tab.selectable) {
        tabsStore.selectTab(tab);
      }

      if (tab.clickFunction != null) {
        tab.clickFunction(event);
      }

      if (tab.tooltip) {
        $('[data-toggle="tab"]').tooltip("hide");
      }
    },
    contextMenuHandler(event, tab) {
      if (tab.rightClickFunction) {
        event.stopPropagation();
        event.preventDefault();
        tab.rightClickFunction(event, tab);
      }
      if (tab.tooltip) {
        $('[data-toggle="tab"]').tooltip("hide");
      }
    },
  },
};
