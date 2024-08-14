import { mount } from "@vue/test-utils";
import { describe, it, expect, vi } from "vitest";
import ConfirmableButton from "@/components/ConfirmableButton.vue";

describe("ConfirmableButton.vue", () => {
  it("should render default slot content and trigger defaultClick on click", async () => {
    const wrapper = mount(ConfirmableButton, {
      props: {
        callbackFunc: vi.fn(),
      },
    });

    expect(wrapper.text()).toContain("Delete");
    expect(wrapper.find("button").exists()).toBe(true);

    await wrapper.find("button").trigger("click");

    expect(wrapper.vm.clicked).toBe(true);
    expect(wrapper.text()).toContain("Confirm Delete?");
  });

  it("should render custom confirmText when clicked", async () => {
    const wrapper = mount(ConfirmableButton, {
      props: {
        callbackFunc: vi.fn(),
        confirmText: "Are you sure?",
      },
    });

    await wrapper.find("button").trigger("click");

    expect(wrapper.text()).toContain("Are you sure?");
  });

  it("should call callbackFunc when confirmClick is triggered", async () => {
    const callbackFunc = vi.fn();
    const wrapper = mount(ConfirmableButton, {
      props: {
        callbackFunc,
      },
    });

    await wrapper.find("button").trigger("click");
    await wrapper.find("button").trigger("click");

    expect(callbackFunc).toHaveBeenCalled();
  });

  it("should reset clicked to false after 3 seconds", async () => {
    vi.useFakeTimers();
    const wrapper = mount(ConfirmableButton, {
      props: {
        callbackFunc: vi.fn(),
      },
    });

    await wrapper.find("button").trigger("click");
    expect(wrapper.vm.clicked).toBe(true);

    vi.advanceTimersByTime(3000);
    expect(wrapper.vm.clicked).toBe(false);

    vi.useRealTimers();
  });
});
