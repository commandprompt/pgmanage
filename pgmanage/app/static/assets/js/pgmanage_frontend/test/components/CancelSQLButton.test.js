import { mount } from "@vue/test-utils";
import CancelButton from "@/components/CancelSQLButton.vue";
import { emitter } from "@/emitter";
import { tabsStore } from "@/stores/stores_initializer";
import { createRequest, removeContext } from "../../src/long_polling";
import { queryRequestCodes } from "@/constants";
import { describe, vi, beforeEach, afterEach, it, expect } from "vitest";

vi.stubGlobal("app_base_path", "test_folder");

vi.mock("@/stores/stores_initializer", () => ({
  tabsStore: {
    getSelectedSecondaryTab: vi.fn(),
  },
}));

vi.mock("@/long_polling", () => ({
  createRequest: vi.fn(),
  removeContext: vi.fn(),
}));

describe("CancelButton.vue", () => {
  const tabId = "tab-1";
  const connId = "conn-1";
  const mockTab = {
    metaData: {
      context: { code: "some-code" },
      isLoading: true,
      isReady: true,
    },
  };

  beforeEach(() => {
    tabsStore.getSelectedSecondaryTab.mockReturnValue(mockTab);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("should call cancelSQL method when button is clicked", async () => {
    const wrapper = mount(CancelButton, {
      props: { tabId, connId },
      shallow: true,
    });

    const cancelSQLSpy = vi.spyOn(wrapper.vm, "cancelSQL");
    await wrapper.find("button").trigger("click");

    expect(cancelSQLSpy).toHaveBeenCalled();
  });

  it("should call removeContext and createRequest in cancelSQL", async () => {
    const wrapper = mount(CancelButton, {
      props: { tabId, connId },
      shallow: true,
    });

    await wrapper.vm.cancelSQL();

    expect(removeContext).toHaveBeenCalledWith(mockTab.metaData.context.code);
    expect(createRequest).toHaveBeenCalledWith(
      queryRequestCodes.CancelThread,
      { tab_id: tabId, conn_tab_id: connId },
      {
        tab: mockTab,
        database_index: undefined,
        callback: expect.any(Function),
      }
    );

    const { callback } = createRequest.mock.calls[0][2];
    expect(callback.name).toBe(`bound ${wrapper.vm.cancelSQLReturn.name}`);
  });

  it("should set tab metadata and emit cancelled event in cancelSQLReturn", async () => {
    const wrapper = mount(CancelButton, {
      props: { tabId, connId },
      shallow: true,
    });

    await wrapper.vm.cancelSQLReturn();

    expect(mockTab.metaData.isLoading).toBe(false);
    expect(mockTab.metaData.isReady).toBe(false);
    expect(wrapper.emitted("cancelled")).toBeTruthy();
  });

  it("should handle and unregister emitter events", () => {
    const wrapper = mount(CancelButton, {
      props: { tabId, connId },
      shallow: true,
    });
    const cancelSQLSpy = vi.spyOn(wrapper.vm, "cancelSQL");
    const emitterDeleteSpy = vi.spyOn(emitter.all, "delete");

    emitter.emit(`${tabId}_cancel_query`);

    expect(cancelSQLSpy).toHaveBeenCalled();

    wrapper.unmount();

    expect(emitterDeleteSpy).toHaveBeenCalledWith(`${tabId}_cancel_query`);
  });
});
