import { flushPromises, mount, enableAutoUnmount } from "@vue/test-utils";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import ConfigTab from "@/components/ConfigTab.vue";
import axios from "axios";
import { showToast } from "@/notification_control";
import { Modal } from "bootstrap";
import { tabsStore } from "@/stores/stores_initializer";

vi.mock("axios");
vi.mock("@/notification_control", () => ({
  showToast: vi.fn(),
}));
vi.mock("bootstrap", () => ({
  Modal: {
    getOrCreateInstance: vi.fn(() => ({
      show: vi.fn(),
    })),
  },
}));
vi.mock("@/stores/stores_initializer", () => ({
  tabsStore: {
    getSecondaryTabById: vi.fn(),
  },
}));

enableAutoUnmount(afterEach);

describe("ConfigTab.vue", () => {
  let wrapper;
  const initialProps = {
    connId: "test-conn-id",
    databaseIndex: 0,
    tabId: "test-tab-id",
  };

  const categoriesResponse = {
    data: { categories: ["Category 1", "Category 2"] },
  };

  const settingsResponse = {
    data: {
      settings: [
        { category: "Category 1", rows: [] },
        { category: "Category 2", rows: [] },
      ],
    },
  };

  const configHistoryResponse = {
    data: {
      config_history: [
        { name: "Category 1", start_time: "2024-08-01T12:00:00Z" },
        { name: "Category 2", start_time: "2024-08-01T12:00:00Z" },
      ],
    },
  };

  const configStatusResponse = {
    data: {
      restart_pending: [],
      restart_changes: [],
    },
  };

  const mountComponent = (options = {}) => {
    return mount(ConfigTab, {
      props: initialProps,
      shallow: true,
      ...options,
    });
  };
  beforeEach(() => {
    vi.clearAllMocks();
    axios.post
      .mockResolvedValueOnce(categoriesResponse)
      .mockResolvedValueOnce(settingsResponse)
      .mockResolvedValueOnce(configHistoryResponse)
      .mockResolvedValueOnce(configStatusResponse);

    wrapper = mountComponent();
  });

  it("renders correctly", () => {
    expect(wrapper.exists()).toBe(true);
  });

  it("fetches categories on mount", async () => {
    await flushPromises();
    expect(axios.post).toHaveBeenCalledWith("/configuration/categories/", {
      database_index: initialProps.databaseIndex,
      tab_id: initialProps.connId,
    });
    expect(wrapper.vm.categories).toEqual(["Category 1", "Category 2"]);
    expect(wrapper.vm.selected).toBe("Category 1");
  });

  it("handles configuration save", async () => {
    wrapper.setData({
      updateSettings: { setting1: { name: "setting1", setting: "value1" } },
    });
    axios.post
      .mockResolvedValueOnce(settingsResponse)
      .mockResolvedValueOnce(configHistoryResponse)
      .mockResolvedValueOnce(settingsResponse)
      .mockResolvedValueOnce(configStatusResponse);
    await wrapper.vm.saveConfiguration();

    await flushPromises();

    expect(axios.post).toHaveBeenNthCalledWith(5, "/save_configuration/", {
      database_index: initialProps.databaseIndex,
      tab_id: initialProps.connId,
      settings: expect.objectContaining({
        setting1: expect.objectContaining({
          name: "setting1",
          setting: "value1",
        }),
      }),
      commit_comment: "",
      new_config: true,
    });
    expect(wrapper.vm.updateSettings).toEqual({});
    expect(showToast).not.toHaveBeenCalled();
  });

  it("displays error on failed configuration save", async () => {
    const errorMessage = "Failed to save configuration";
    axios.post.mockRejectedValueOnce({
      response: { data: { data: errorMessage } },
    });
    wrapper.setData({
      updateSettings: { setting1: { name: "setting1", setting: "value1" } },
    });

    await wrapper.vm.saveConfiguration();

    await flushPromises();

    expect(showToast).toHaveBeenCalledWith("error", errorMessage);
  });

  it("truncates text correctly", () => {
    const input = {
      start_time: "2024-08-09T12:00:00Z",
      commit_comment: "This is a long commit message",
    };
    const truncatedText = wrapper.vm.truncateText(input, 30);

    expect(truncatedText).toBe("2024-08-09T12:00:00Z - This i...");
  });

  it("watches for update values correctly", async () => {
    tabsStore.getSecondaryTabById.mockReturnValue({
      metaData: { hasUnsavedChanges: false },
    });
    wrapper.setData({
      updateSettings: { setting1: { name: "setting1", setting: "value1" } },
    });

    await flushPromises();

    expect(tabsStore.getSecondaryTabById).toHaveBeenCalledWith(
      initialProps.tabId,
      initialProps.connId
    );
    expect(wrapper.vm.$data.updateSettings).toEqual(
      expect.objectContaining({
        setting1: { name: "setting1", setting: "value1" },
      })
    );
  });
});
