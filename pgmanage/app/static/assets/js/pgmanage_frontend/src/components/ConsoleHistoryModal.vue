<template>
  <div class="modal fade" :id="`modal_console_history_${tabId}`" tabindex="-1" role="dialog" aria-hidden="true">
    <div class="modal-dialog modal-xl" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 class="moda-title font-weight-bold">Console commands history</h2>
          <button :id="`console_history_close_${tabId}`" type="button" class="close" data-dismiss="modal"
            aria-label="Close" @click="closeConsoleHistory">
            <span aria-hidden="true">
              <i class="fa-solid fa-xmark"></i>
            </span>
          </button>
        </div>

        <div class="modal-body">
          <div :id="`console_history_div_${tabId}`" class="console_command_history">
            <div :id="`console_history_header_${tabId}`" class="console_command_history_header"></div>
            <div :id="`console_history_grid_${tabId}`" class="console_command_history_grid" style="
                width: 100%;
                height: calc(100vh - 16.5rem);
                overflow: hidden;
              "></div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
<script>
import { closeConsoleHistory } from "../console";

export default {
  name: "ConsoleHistoryModal",
  props: {
    tabId: String,
  },
  methods: {
    closeConsoleHistory,
  }
};
</script>
