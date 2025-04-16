import { flushPromises, mount } from "@vue/test-utils";
import { describe, it, expect } from "vitest";
import SearchableDropdown from "@src/components/SearchableDropdown.vue";

describe("SearchableDropdown.vue", () => {
  it("renders dropdown input and options", () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Option 3"],
      },
    });

    const input = wrapper.find("input");
    expect(input.exists()).toBe(true);
    const options = wrapper.findAll(".dropdown-searchable__content_item");
    expect(options.length).toBe(0);
  });

  it("shows options when input is focused", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Option 3"],
      },
    });

    await wrapper.find("input").trigger("focus");
    expect(wrapper.vm.optionsShown).toBe(true);

    const options = wrapper.findAll(".dropdown-searchable__content_item");
    expect(options.length).toBe(3);
  });

  it("filters options based on search input", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Test Option"],
      },
    });

    await wrapper.find("input").setValue("Test");
    expect(wrapper.vm.filteredOptions).toEqual(["Test Option"]);
  });

  it("emits update:modelValue when an option is selected", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Option 3"],
      },
    });

    await wrapper.find("input").trigger("focus");

    await wrapper
      .find('[data-testid="dropdown-searchable-item-0"]')
      .trigger("mousedown");

    expect(wrapper.emitted("update:modelValue")).toHaveLength(1);
    expect(wrapper.emitted("update:modelValue")[0]).toEqual(["Option 1"]);
  });

  it("emits update:modelValue with multiple options selected in multi-select mode", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Option 3"],
        multiSelect: true,
      },
    });

    await wrapper.find("input").trigger("focus");

    await wrapper
      .find('[data-testid="dropdown-searchable-item-0"]')
      .trigger("mousedown");
    await wrapper
      .find('[data-testid="dropdown-searchable-item-1"]')
      .trigger("mousedown");

    expect(wrapper.emitted("update:modelValue")).toHaveLength(2);
    expect(wrapper.emitted("update:modelValue")[0]).toEqual([
      ["Option 1", "Option 2"],
    ]);
  });

  it("adds multiple options from input when filteredOptions is empty in multi-select mode", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2"],
        multiSelect: true,
      },
    });

    const input = wrapper.find("input");
    await input.setValue("Custom 1, Custom 2");
    await input.trigger("keyup", { key: "Enter" });

    expect(wrapper.vm.selected).toEqual(["Custom 1", "Custom 2"]);
    expect(wrapper.emitted("update:modelValue")).toHaveLength(1);
    expect(wrapper.emitted("update:modelValue")[0]).toEqual([
      ["Custom 1", "Custom 2"],
    ]);
  });

  it("updates selected value when modelValue prop changes", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Option 3"],
        modelValue: "Option 2",
      },
    });

    expect(wrapper.vm.selected).toBe("Option 2");

    await wrapper.setProps({ modelValue: "Option 3" });
    expect(wrapper.vm.selected).toBe("Option 3");
  });

  it("selects option when pressing Enter key", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Option 3"],
      },
    });

    const input = wrapper.find("input");
    await input.trigger("focus");
    await input.setValue("Option 2");
    await input.trigger("keyup", { key: "Enter" });

    expect(wrapper.emitted("update:modelValue")).toHaveLength(1);
    expect(wrapper.emitted("update:modelValue")[0]).toEqual(["Option 2"]);
  });

  it("selects highlighted option when Enter is pressed", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Option 3"],
      },
    });

    const input = wrapper.find("input");
    await input.trigger("focus");
    await wrapper.setData({ highlightedIndex: 1 }); // Highlight "Option 2"
    await input.trigger("keyup", { key: "Enter" });

    expect(wrapper.emitted("update:modelValue")).toHaveLength(1);
    expect(wrapper.emitted("update:modelValue")[0]).toEqual(["Option 2"]);
  });

  it("disables input when disabled prop is true", () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2"],
        disabled: true,
      },
    });

    const input = wrapper.find("input");
    expect(input.isDisabled()).toBeTruthy();
  });

  it("highlights options when navigating with arrow keys", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Option 3"],
      },
    });

    await wrapper.find("input").trigger("focus");
    await wrapper.find("input").trigger("keyup", { key: "ArrowDown" });
    expect(wrapper.vm.highlightedIndex).toBe(0);

    await wrapper.find("input").trigger("keyup", { key: "ArrowDown" });
    expect(wrapper.vm.highlightedIndex).toBe(1);
  });

  it("clears selected value and emits null when cleared", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Option 3"],
        modelValue: "Option 1",
      },
    });

    expect(wrapper.vm.selected).toBe("Option 1");
    await wrapper.setProps({ modelValue: null });
    await flushPromises();
    expect(wrapper.vm.selected).toBeNull();
  });

  it("assigns searchFilter to selected if no option is selected on blur", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Option 3"],
      },
    });

    await wrapper.find("input").setValue("New Value");
    await wrapper.find("input").trigger("blur");

    expect(wrapper.vm.selected).toBe("New Value");
    expect(wrapper.emitted("update:modelValue")).toHaveLength(1);
    expect(wrapper.emitted("update:modelValue")[0]).toEqual(["New Value"]);
    expect(wrapper.vm.optionsShown).toBe(false);
    expect(wrapper.vm.highlightedIndex).toBe(-1);
  });

  it("resets searchFilter to selected value if searchFilter is empty on blur", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2"],
        modelValue: "Option 1",
      },
    });

    await wrapper.find("input").setValue("");
    await wrapper.find("input").trigger("blur");

    expect(wrapper.vm.searchFilter).toBe("Option 1");
    expect(wrapper.vm.optionsShown).toBe(false);
    expect(wrapper.vm.highlightedIndex).toBe(-1);
  });

  it("hides dropdown and resets highlighted index on blur", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2"],
      },
    });

    await wrapper.find("input").trigger("focus");
    expect(wrapper.vm.optionsShown).toBe(true);

    await wrapper.find("input").trigger("blur");
    expect(wrapper.vm.optionsShown).toBe(false);
    expect(wrapper.vm.highlightedIndex).toBe(-1);
  });

  it("cycles through options with ArrowDown key", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Option 3"],
      },
    });

    const input = wrapper.find("input");
    await input.trigger("focus");

    await input.trigger("keyup", { key: "ArrowDown" });
    expect(wrapper.vm.highlightedIndex).toBe(0);

    await input.trigger("keyup", { key: "ArrowDown" });
    expect(wrapper.vm.highlightedIndex).toBe(1);

    await input.trigger("keyup", { key: "ArrowDown" });
    expect(wrapper.vm.highlightedIndex).toBe(2);

    await input.trigger("keyup", { key: "ArrowDown" });
    expect(wrapper.vm.highlightedIndex).toBe(0);
  });

  it("cycles through options in reverse with ArrowUp key", async () => {
    const wrapper = mount(SearchableDropdown, {
      props: {
        options: ["Option 1", "Option 2", "Option 3"],
      },
    });

    const input = wrapper.find("input");
    await input.trigger("focus");

    await input.trigger("keyup", { key: "ArrowUp" });
    expect(wrapper.vm.highlightedIndex).toBe(2);

    await input.trigger("keyup", { key: "ArrowUp" });
    expect(wrapper.vm.highlightedIndex).toBe(1);

    await input.trigger("keyup", { key: "ArrowUp" });
    expect(wrapper.vm.highlightedIndex).toBe(0);

    await input.trigger("keyup", { key: "ArrowUp" });
    expect(wrapper.vm.highlightedIndex).toBe(2);
  });
});
