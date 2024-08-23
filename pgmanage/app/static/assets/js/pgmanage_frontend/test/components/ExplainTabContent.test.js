import { mount } from "@vue/test-utils";
import ExplainTab from "@/components/ExplainTabContent.vue";
import { Plan } from "pev2";
import { describe, it, expect } from "vitest";

describe("ExplainTab.vue", () => {
  let wrapper;

  const createComponent = (props = {}) => {
    wrapper = mount(ExplainTab, {
      props: {
        tabId: "tab-1",
        plan: "",
        query: "",
        ...props,
      },
      shallow: true,
    });
  };

  it("should render nothing to visualize message when both plan and query are empty", () => {
    createComponent();

    expect(wrapper.text()).toContain(
      "Nothing to visualize. Please click Explain or Analyze button on the toolbar above."
    );
    expect(wrapper.findComponent(Plan).exists()).toBe(false);
  });

  it("should render the Plan component when plan and query are provided", async () => {
    createComponent({ plan: "some-plan", query: "some-query" });

    const planComponent = wrapper.findComponent(Plan);
    expect(planComponent.exists()).toBe(true);
    expect(planComponent.props("planSource")).toBe("some-plan");
    expect(planComponent.props("planQuery")).toBe("some-query");
  });

  it("should increment reRenderCounter when the plan prop changes", async () => {
    createComponent({ plan: "initial-plan", query: "some-query" });

    const initialCounter = wrapper.vm.reRenderCounter;

    await wrapper.setProps({ plan: "updated-plan" });

    expect(wrapper.vm.reRenderCounter).toBe(initialCounter + 1);
  });

  it("should not increment reRenderCounter when the plan prop remains the same", async () => {
    createComponent({ plan: "initial-plan", query: "some-query" });

    const initialCounter = wrapper.vm.reRenderCounter;

    await wrapper.setProps({ plan: "initial-plan" });

    expect(wrapper.vm.reRenderCounter).toBe(initialCounter);
  });
});
