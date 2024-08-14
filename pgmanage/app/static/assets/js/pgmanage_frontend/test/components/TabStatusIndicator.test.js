import { mount } from "@vue/test-utils";
import TabStatusIndicator from "@/components/TabStatusIndicator.vue";
import { tabStatusMap } from "@/constants";
import { describe, it, expect } from "vitest";

describe("TabStatusIndicator.vue", () => {
  const createWrapper = (tabStatus) => {
    return mount(TabStatusIndicator, {
      props: { tabStatus },
    });
  };

  it("renders the correct status text for each tab status", () => {
    const statusTexts = [
      { status: tabStatusMap.NOT_CONNECTED, text: "Not Connected" },
      { status: tabStatusMap.IDLE, text: "Idle" },
      { status: tabStatusMap.RUNNING, text: "Running" },
      { status: tabStatusMap.IDLE_IN_TRANSACTION, text: "Idle in transaction" },
      {
        status: tabStatusMap.IDLE_IN_TRANSACTION_ABORTED,
        text: "Idle in transaction (aborted)",
      },
    ];

    statusTexts.forEach(({ status, text }) => {
      const wrapper = createWrapper(status);
      expect(wrapper.find("label").text()).toBe(text);
      expect(wrapper.find("i").attributes("title")).toBe(text);
    });
  });

  it("applies the correct CSS class based on tabStatus", () => {
    const statusClasses = [
      { status: tabStatusMap.NOT_CONNECTED, class: "tab-status-closed" },
      { status: tabStatusMap.IDLE, class: "tab-status-idle position-relative" },
      {
        status: tabStatusMap.RUNNING,
        class: "tab-status-running position-relative",
      },
      {
        status: tabStatusMap.IDLE_IN_TRANSACTION,
        class: "tab-status-idle_in_transaction",
      },
      {
        status: tabStatusMap.IDLE_IN_TRANSACTION_ABORTED,
        class: "tab-status-idle_in_transaction_aborted",
      },
    ];

    statusClasses.forEach(({ status, class: expectedClass }) => {
      const wrapper = createWrapper(status);
      const classesArray = wrapper.find("i").classes();
      expectedClass.split(" ").forEach((cls) => {
        expect(classesArray).toContain(cls);
      });
    });
  });

  it("displays the circle waves indicator only for IDLE and RUNNING statuses", () => {
    const wrapperIdle = createWrapper(tabStatusMap.IDLE);
    expect(wrapperIdle.find(".tab-status-indicator").exists()).toBe(true);
    expect(wrapperIdle.find(".omnis__circle-waves").exists()).toBe(true);
    expect(wrapperIdle.find(".omnis__circle-waves--idle").exists()).toBe(true);

    const wrapperRunning = createWrapper(tabStatusMap.RUNNING);
    expect(wrapperRunning.find(".tab-status-indicator").exists()).toBe(true);
    expect(wrapperRunning.find(".omnis__circle-waves").exists()).toBe(true);
    expect(wrapperRunning.find(".omnis__circle-waves--running").exists()).toBe(
      true
    );

    const otherStatuses = [
      tabStatusMap.NOT_CONNECTED,
      tabStatusMap.IDLE_IN_TRANSACTION,
      tabStatusMap.IDLE_IN_TRANSACTION_ABORTED,
    ];

    otherStatuses.forEach((status) => {
      const wrapper = createWrapper(status);
      expect(wrapper.find(".tab-status-indicator").exists()).toBe(false);
    });
  });

  it("matches the snapshot for each tabStatus", () => {
    const statuses = [
      tabStatusMap.NOT_CONNECTED,
      tabStatusMap.IDLE,
      tabStatusMap.RUNNING,
      tabStatusMap.IDLE_IN_TRANSACTION,
      tabStatusMap.IDLE_IN_TRANSACTION_ABORTED,
    ];

    statuses.forEach((status) => {
      const wrapper = createWrapper(status);
      expect(wrapper.html()).toMatchSnapshot();
    });
  });
});
