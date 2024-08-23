<template>
  <div class="omnidb__tab-status d-flex align-items-center form-check-inline mb-0 ps-0">
    <i :title="statusText" :class="['fas fa-dot-circle tab-status', statusClass]">
      <div v-if="tabStatus === 1 || tabStatus === 2" class="tab-status-indicator">
        <span :class="circleWavesClass">
          <span v-for="n in 4" :key="n"></span>
        </span>
      </div>
    </i>

    <label :title="statusText" class="mx-1 form-check-label ps-1">
      {{ statusText }}
    </label>
  </div>
</template>

<script>
import { tabStatusMap } from "../constants";

export default {
  name: "TabStatusIndicator",
  props: {
    tabStatus: {
      type: Number,
      validator: function (value) {
        return Object.values(tabStatusMap).includes(value);
      },
    },
  },
  computed: {
    statusText() {
      const statusMap = {
        0: "Not Connected",
        1: "Idle",
        2: "Running",
        3: "Idle in transaction",
        4: "Idle in transaction (aborted)",
      };
      return statusMap[this.tabStatus] || "";
    },
    statusClass() {
      const statusClassMap = {
        0: "tab-status-closed",
        1: "tab-status-idle position-relative",
        2: "tab-status-running position-relative",
        3: "tab-status-idle_in_transaction",
        4: "tab-status-idle_in_transaction_aborted",
      };

      return `${statusClassMap[this.tabStatus] || ""}`;
    },
    circleWavesClass() {
      return {
        "omnis__circle-waves":
          this.tabStatus === tabStatusMap.IDLE ||
          this.tabStatus === tabStatusMap.RUNNING,
        "omnis__circle-waves--idle": this.tabStatus === tabStatusMap.IDLE,
        "omnis__circle-waves--running": this.tabStatus === tabStatusMap.RUNNING,
      };
    },
  },
};
</script>

<style scoped>

.tab-status {
width: 0.875rem;
height: 0.875rem;
}
.tab-status-indicator {
  position: absolute;
  width: 100%;
  height: 100%;
  overflow: visible;
  left: 50%;
  top: 50%;
  display: block;
  transform: translate(-50%, -50%);

}
</style>
