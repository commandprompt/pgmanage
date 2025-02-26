import { mount } from "@vue/test-utils";
import { describe, test, expect, beforeEach } from "vitest";
import DataEditorTabFilterList from "@/components/DataEditorTabFilterList.vue";
import DataEditorTabFilterItem from "@/components/DataEditorTabFilterItem.vue";
import { dataEditorFilterModes } from "@/constants";

describe("DataEditorFilter.vue", () => {
  let wrapper;
  const columns = ["id", "name", "email"];
  const filters = [
    { column: "id", operator: "=", value: "123", condition: "AND" },
  ];

  beforeEach(() => {
    wrapper = mount(DataEditorTabFilterList, {
      props: {
        columns: columns,
        filters: [...filters],
      },
    });
  });

  test("renders initial filter in builder mode", () => {
    const filterItem = wrapper.findComponent(DataEditorTabFilterItem);
    expect(filterItem.exists()).toBe(true);
    expect(wrapper.vm.mode).toBe(dataEditorFilterModes.BUILDER);
  });

  test("adds a new filter row", async () => {
    const addButton = wrapper.find('button[data-testid="add-filter-button"]');
    await addButton.trigger("click");

    expect(wrapper.vm.localFilters).toHaveLength(2);
    expect(wrapper.vm.localFilters[1]).toEqual({
      column: "id",
      operator: "=",
      value: "",
      condition: "AND",
    });
  });

  test("removes a filter row", async () => {
    // first adding new filter row
    const addButton = wrapper.find('button[data-testid="add-filter-button"]');
    await addButton.trigger("click");

    expect(wrapper.vm.localFilters).toHaveLength(2);

    const removeButton = wrapper.find('button[data-testid="remove-button"]');
    await removeButton.trigger("click");

    expect(wrapper.vm.localFilters).toHaveLength(1);
  });

  test("toggles condition between AND and OR", async () => {
    // first adding new filter row
    const addButton = wrapper.find('button[data-testid="add-filter-button"]');
    await addButton.trigger("click");

    const conditionButton = wrapper.find(
      'button[data-testid="toggle-condition-button"]'
    );
    expect(wrapper.vm.localFilters[1].condition).toBe("AND");

    await conditionButton.trigger("click");

    expect(wrapper.vm.localFilters[1].condition).toBe("OR");

    await conditionButton.trigger("click");
    expect(wrapper.vm.localFilters[1].condition).toBe("AND");
  });

  test("switches to manual mode and emits update", async () => {
    const manualModeButton = wrapper.find("button.btn-warning");
    await manualModeButton.trigger("click");

    expect(wrapper.vm.mode).toBe(dataEditorFilterModes.MANUAL);
    expect(wrapper.emitted("update")[0][0]).toEqual({
      mode: dataEditorFilterModes.MANUAL,
      rawQuery: " id = '123'",
    });

    const manualInput = wrapper.find("input[type='text']");
    expect(manualInput.exists()).toBe(true);
  });

  test("switches to builder mode from manual mode", async () => {
    await wrapper.setData({ mode: dataEditorFilterModes.MANUAL });
    const builderModeButton = wrapper.find("button.btn-warning");
    await builderModeButton.trigger("click");

    expect(wrapper.vm.mode).toBe(dataEditorFilterModes.BUILDER);
    expect(wrapper.emitted("update")[0][0]).toEqual({
      mode: dataEditorFilterModes.BUILDER,
    });
  });

  test("converts filters to raw query in manual mode", async () => {
    wrapper.vm.switchToManual();
    expect(wrapper.vm.rawQuery).toBe(" id = '123'");

    // Adding another filter and verifying conversion
    await wrapper.setData({
      localFilters: [
        ...filters,
        { column: "name", operator: "in", value: "John,Doe", condition: "OR" },
      ],
    });

    wrapper.vm.switchToManual();
    expect(wrapper.vm.rawQuery).toBe(" id = '123'\nOR name IN ('John', 'Doe')");
  });

  test("emits raw query update on manual input change", async () => {
    await wrapper.setData({ mode: dataEditorFilterModes.MANUAL, rawQuery: "" });

    const input = wrapper.find("input[type='text']");
    await input.setValue("id = '456'");
    expect(wrapper.vm.rawInputDirty).toBe(true);

    expect(wrapper.emitted("update")[0][0]).toEqual({
      mode: dataEditorFilterModes.MANUAL,
      rawQuery: "id = '456'",
    });
  });

  test("updates local filter when filter item emits update", async () => {
    const filterItem = wrapper.findComponent(DataEditorTabFilterItem);
    await filterItem.vm.$emit("update", {
      column: "name",
      operator: "=",
      value: "John",
      condition: "AND",
    });

    expect(wrapper.vm.localFilters[0]).toEqual({
      column: "name",
      operator: "=",
      value: "John",
      condition: "AND",
    });
  });
});
