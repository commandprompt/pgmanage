import { mount } from "@vue/test-utils";
import { describe, it, expect } from "vitest";
import BlockSizeSelector from "@/components/BlockSizeSelector.vue";

describe("BlockSizeSelector.vue", () => {
  it("renders the correct options based on blockSizeOptions", () => {
    const wrapper = mount(BlockSizeSelector, {
      props: {
        modelValue: 50,
      },
    });

    const options = wrapper.findAll("option");
    const optionValues = options.map((option) => option.text());

    expect(optionValues).toEqual(["50 rows", "100 rows", "200 rows"]);
  });

  it("sets the blockSize to the modelValue prop on mount", () => {
    const wrapper = mount(BlockSizeSelector, {
      props: {
        modelValue: 100,
      },
    });

    expect(wrapper.vm.blockSize).toBe(100);
  });

  it("emits update:modelValue event with the correct value when selection changes", async () => {
    const wrapper = mount(BlockSizeSelector, {
      props: {
        modelValue: 50,
      },
    });

    let selectEl = wrapper.get("select").element;
    selectEl.selectedIndex = 1;
    selectEl.dispatchEvent(new Event("change"));

    expect(wrapper.emitted("update:modelValue")).toHaveLength(1);
    expect(wrapper.emitted("update:modelValue")[0]).toEqual([100]);
  });

  it("updates blockSize when modelValue prop changes", async () => {
    const wrapper = mount(BlockSizeSelector, {
      props: {
        modelValue: 50,
      },
    });

    await wrapper.setProps({ modelValue: 200 });

    expect(wrapper.vm.blockSize).toBe(200);
  });
});
