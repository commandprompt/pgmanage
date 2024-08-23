import { mount } from "@vue/test-utils";
import ConfigTabGroup from "@/components/ConfigTabGroup.vue";
import ConfigTabGroupItemInput from "@/components/ConfigTabGroupItemInput.vue";
import { vi, describe, it, beforeEach, expect } from "vitest";

describe("ConfigTabGroup.vue", () => {
  let wrapper;

  const mountComponent = (propsData = {}) => {
    return mount(ConfigTabGroup, {
      props: propsData,
      shallow: true,
    });
  };

  const mockGroup = {
    category: "General Settings",
    rows: [
      {
        name: "setting1",
        desc: "This is setting 1",
        vartype: "bool",
        setting: "off",
        boot_val: "off",
        category: "General",
      },
      {
        name: "setting2",
        desc: "This is setting 2",
        vartype: "string",
        setting: "some_string",
        boot_val: "some_string",
        category: "General",
      },
    ],
  };

  beforeEach(() => {
    vi.clearAllMocks();
    wrapper = mountComponent({
      initialGroup: mockGroup,
    });
  });

  it("renders the category name in the header", () => {
    const header = wrapper.find(".card-header");
    expect(header.text()).toBe(mockGroup.category);
  });

  it("renders a list of settings", () => {
    const items = wrapper.findAll(".list-group-item");
    expect(items.length).toBe(mockGroup.rows.length);
    items.forEach((item, index) => {
      expect(item.find(".fw-semibold").text()).toBe(mockGroup.rows[index].name);
      expect(item.find(".text-muted").text()).toBe(mockGroup.rows[index].desc);
    });
  });

  it("renders ConfigTabGroupItemInput components for each setting", () => {
    const inputs = wrapper.findAllComponents(ConfigTabGroupItemInput);
    expect(inputs.length).toBe(mockGroup.rows.length);

    inputs.forEach((input, index) => {
      expect(input.props("initialSetting")).toEqual(mockGroup.rows[index]);
      expect(input.props("index")).toBe(index);
    });
  });

  it('emits "groupChange" event when a setting is changed', async () => {
    const inputItem = wrapper.findComponent(ConfigTabGroupItemInput);
    const mockEvent = {
      changedSetting: {
        ...mockGroup.rows[0],
        setting: "on",
      },
      index: 0,
    };

    await inputItem.vm.$emit("settingChange", mockEvent);

    expect(wrapper.emitted().groupChange).toBeTruthy();
    expect(wrapper.emitted().groupChange[0][0]).toEqual({
      changedGroup: {
        ...mockGroup,
        rows: [
          {
            ...mockGroup.rows[0],
            setting: "on",
          },
          mockGroup.rows[1],
        ],
      },
      changedSetting: mockEvent.changedSetting,
    });
  });
});
