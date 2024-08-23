import { mount } from "@vue/test-utils";
import ConfigTabGroupItemInput from "@/components/ConfigTabGroupItemInput.vue";
import { tabsStore } from "@/stores/stores_initializer";
import { describe, vi, it, beforeEach, expect } from "vitest";

vi.mock("@/stores/stores_initializer", () => ({
  tabsStore: {
    selectedPrimaryTab: {
      metaData: {
        selectedTab: {
          id: "tab-id",
        },
      },
    },
  },
}));

describe("ConfigTabGroupItemInput.vue", () => {
  let wrapper;

  const mountComponent = (propsData = {}) => {
    wrapper = mount(ConfigTabGroupItemInput, {
      props: { ...propsData, index: 0 },
      shallow: true,
    });
  };

  const mockSetting = {
    name: "test_setting",
    vartype: "bool", // or 'string', 'enum', etc.
    setting: "off",
    boot_val: "off",
    category: "General",
    enumvals: ["value1", "value2"],
    unit: "MB",
    min_val: "0",
    max_val: "100",
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders boolean input correctly", () => {
    mountComponent({
      initialSetting: { ...mockSetting, setting: "on" },
    });

    const input = wrapper.find('input[type="checkbox"]');
    expect(input.exists()).toBe(true);
    expect(input.element.checked).toBe(true);
  });

  it("renders string input correctly", () => {
    mountComponent({
      initialSetting: {
        ...mockSetting,
        vartype: "string",
        setting: "some_string",
      },
    });

    const input = wrapper.find('input[type="text"]');
    expect(input.exists()).toBe(true);
    expect(input.element.value).toBe("some_string");
  });

  it("renders enum select input correctly", () => {
    mountComponent({
      initialSetting: {
        ...mockSetting,
        vartype: "enum",
        setting: "value1",
      },
    });

    const select = wrapper.find("select");
    expect(select.exists()).toBe(true);
    expect(select.element.value).toBe("value1");
    const options = select.findAll("option");
    expect(options.length).toBe(2);
    expect(options[0].element.value).toBe("value1");
    expect(options[1].element.value).toBe("value2");
  });

  it('emits "settingChange" event when input changes', async () => {
    mountComponent({ initialSetting: mockSetting });

    await wrapper.find('input[type="checkbox"]').setValue("true");

    expect(wrapper.emitted().settingChange).toBeTruthy();
    expect(wrapper.emitted().settingChange[0][0]).toEqual({
      changedSetting: {
        ...mockSetting,
        setting: "on",
      },
      index: 0,
    });
  });

  it("disables input when isReadOnly is true", () => {
    mountComponent({
      initialSetting: {
        ...mockSetting,
        category: "Preset Options",
      },
    });

    const input = wrapper.find("input");
    expect(input.element.disabled).toBe(true);
  });

  it("displays reset button and triggers setDefault on click", async () => {
    mountComponent({
      initialSetting: {
        ...mockSetting,
        setting: "on",
      },
    });

    const resetButton = wrapper.find("button");
    expect(resetButton.exists()).toBe(true);

    await resetButton.trigger("click");
    expect(wrapper.emitted().settingChange).toBeTruthy();
    expect(wrapper.emitted().settingChange[0][0]).toEqual({
      changedSetting: {
        ...mockSetting,
        setting: "off",
      },
      index: 0,
    });
  });
});
