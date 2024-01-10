import { mount } from "@vue/test-utils";
import { describe, expect, test, vi } from "vitest";
import SnippetTab from "../../src/components/SnippetTab.vue";
import "ace-builds/src-noconflict/mode-sql";
import "../../src/ace_themes/theme-omnidb.js";

import { useSettingsStore } from "../../src/stores/settings.js";

describe("SnippetTab", () => {
  const settings = useSettingsStore();
  settings.setEditorTheme("omnidb");

  const wrapper = mount(SnippetTab, {
    props: {
      tabId: "uniqueTabID",
    },
  });

  test("should mount SnippetTab component", () => {
    expect(wrapper.html()).toContain("Indent");
    expect(wrapper.html()).toContain("Save");
  });

  test("should set up Ace editor with correct configuration", () => {
    const editorInstance = wrapper.vm.editor;

    expect(editorInstance).toBeDefined();
    expect(editorInstance.getTheme()).toBe("ace/theme/omnidb");
    expect(editorInstance.session.getMode().$id).toBe("ace/mode/sql");
  });

  test("should indent SQL when 'Indent' button is clicked", async () => {
    const editorInstance = wrapper.vm.editor;
    editorInstance.setValue("SELECT * FROM table");

    await wrapper.find("[data-testid='indent-button']").trigger("click");

    expect(editorInstance.getValue()).toContain("SELECT\n  *");
  });

  test("should call saveSnippetText method when 'Save' button is clicked", async () => {
    const saveSnippetTextMock = vi.spyOn(wrapper.vm, "saveSnippetText");

    await wrapper.find("[data-testid='save-button']").trigger("click");

    expect(saveSnippetTextMock).toHaveBeenCalled();
  });

  test("should cleanup events on unmount", () => {
    const clearEventsSpy = vi.spyOn(wrapper.vm, "clearEvents");

    wrapper.unmount();

    expect(clearEventsSpy).toHaveBeenCalled();
  });
});
