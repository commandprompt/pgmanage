import { mount } from "@vue/test-utils";
import { describe, it, expect, vi } from "vitest";
import SnippetPanel from "@/components/SnippetPanel.vue";
import axios from "axios";
import { emitter } from "@/emitter";

vi.hoisted(() => {
  vi.stubGlobal("v_csrf_cookie_name", "test_cookie");
});

vi.mock("axios");

describe("SnippetPanel.vue", () => {
  it("renders the component correctly", () => {
    axios.get.mockResolvedValue({
      data: { files: [], folders: [] },
    });
    const wrapper = mount(SnippetPanel, {
      props: { tabId: "test" },
      shallow: true,
    });
    expect(wrapper.exists()).toBe(true);
    expect(wrapper.find(".omnidb__panel").exists()).toBe(true);
  });

  it("toggles the panel visibility", async () => {
    const wrapper = mount(SnippetPanel, {
      props: { tabId: "test" },
      shallow: true,
    });
    expect(wrapper.vm.isVisible).toBe(false);
    await wrapper.find(".omnidb__panel__toggler").trigger("click");
    expect(wrapper.vm.isVisible).toBe(true);
    await wrapper.find(".omnidb__panel__toggler").trigger("click");
    expect(wrapper.vm.isVisible).toBe(false);
  });

  it("fetches all snippets on mount", () => {
    axios.get.mockResolvedValue({
      data: { files: [], folders: [] },
    });
    mount(SnippetPanel, {
      props: { tabId: "test" },
      shallow: true,
    });
    expect(axios.get).toHaveBeenCalledWith("/get_all_snippets/");
  });
  describe("Events", () => {
    it("handles emitter events", () => {
      axios.post.mockResolvedValue({ data: { data: "test data" } });
      const wrapper = mount(SnippetPanel, {
        props: { tabId: "test" },
        shallow: true,
      });
      const togglePanelSpy = vi.spyOn(wrapper.vm, "togglePanel");
      const hidePanelSpy = vi.spyOn(wrapper.vm, "hidePanel");
      const getAllSnippetsSpy = vi.spyOn(wrapper.vm, "getAllSnippets");
      const saveSnippetTextConfirmSpy = vi.spyOn(
        wrapper.vm,
        "saveSnippetTextConfirm"
      );

      emitter.emit("toggle_snippet_panel");
      expect(togglePanelSpy).toHaveBeenCalled();

      emitter.emit("hide_snippet_panel");
      expect(hidePanelSpy).toHaveBeenCalled();

      emitter.emit("get_all_snippets");
      expect(getAllSnippetsSpy).toHaveBeenCalled();

      emitter.emit("save_snippet_text_confirm", {
        saveObject: {
          id: 1,
          name: null,
          parent: 2,
        },
        text: "test text",
        callback: () => {},
      });
      expect(saveSnippetTextConfirmSpy).toHaveBeenCalled();
    });

    it("clears emitter events on unmount", () => {
      const wrapper = mount(SnippetPanel, {
        props: { tabId: "test" },
        shallow: true,
      });
      const clearEventsSpy = vi.spyOn(wrapper.vm, "clearEvents");

      wrapper.unmount();

      expect(clearEventsSpy).toHaveBeenCalled();
    });
  });
});
