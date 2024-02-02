import { mount } from "@vue/test-utils";
import {
  beforeAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from "vitest";
import "ace-builds";
import "ace-builds/esm-resolver";
import SnippetTab from "../../src/components/SnippetTab.vue";
import "../../src/ace_themes/theme-omnidb.js";
import { emitter } from "../../src/emitter.js";

import { useSettingsStore } from "../../src/stores/settings.js";

window.$ = vi.fn().mockImplementation(() => {
  return {
    on: vi.fn(),
  };
});

describe("SnippetTab", () => {
  let wrapper;
  let settingsStore;
  const tabId = "uniqueTabID";

  beforeAll(() => {
    settingsStore = useSettingsStore();
    settingsStore.setEditorTheme("omnidb");
  });

  beforeEach(() => {
    wrapper = mount(SnippetTab, {
      props: {
        tabId: tabId,
      },
      attachTo: document.body,
    });
  });

  afterEach(() => {
    wrapper.unmount();
  });

  test("should render SnippetTab component with expected elements", () => {
    expect(wrapper.html()).toContain("Indent");
    expect(wrapper.html()).toContain("Save");
  });

  test("should initialize Ace editor with correct configuration", () => {
    const editorInstance = wrapper.vm.editor;

    expect(editorInstance).toBeDefined();
    expect(editorInstance.getTheme()).toBe("ace/theme/omnidb");
    expect(editorInstance.session.getMode().$id).toBe("ace/mode/sql");
  });

  test("should indent SQL when 'Indent' button is clicked", async () => {
    const editorInstance = wrapper.vm.editor;
    editorInstance.setValue("SELECT * FROM table");

    await wrapper
      .find("[data-testid='snippet-tab-indent-button']")
      .trigger("click");

    expect(editorInstance.getValue()).toContain("SELECT\n  *");
  });

  test("should call saveSnippetText method when 'Save' button is clicked", async () => {
    const saveSnippetTextMock = vi.spyOn(wrapper.vm, "saveSnippetText");

    await wrapper
      .find("[data-testid='snippet-tab-save-button']")
      .trigger("click");

    expect(saveSnippetTextMock).toHaveBeenCalled();
  });

  describe("Events", () => {
    test("should focus editor on focus event", async () => {
      const editorInstance = wrapper.vm.editor;

      emitter.emit(`${tabId}_editor_focus`);

      expect(editorInstance.isFocused()).toBeTruthy();
    });
    test("should copy snippet to editor on copy event", async () => {
      const editorInstance = wrapper.vm.editor;

      emitter.emit(`${tabId}_copy_to_editor`, "SELECT * FROM table");

      expect(editorInstance.getValue()).toEqual("SELECT * FROM table");
    });
    test("should call handleResize method on resize event", async () => {
      const handleResizeSpy = vi.spyOn(wrapper.vm, "handleResize");

      emitter.emit(`${tabId}_resize`);

      expect(handleResizeSpy).toHaveBeenCalledOnce();
    });
    test("should call handleResize on settingsStore fontsize change", async () => {
      const handleResizeSpy = vi.spyOn(wrapper.vm, "handleResize");
      const editorSetFontSizeSpy = vi.spyOn(wrapper.vm.editor, "setFontSize");
      const fontSize = 14;

      settingsStore.setFontSize(fontSize);

      expect(handleResizeSpy).toHaveBeenCalledOnce();

      expect(editorSetFontSizeSpy).toHaveBeenCalledOnce();
      expect(editorSetFontSizeSpy).toHaveBeenCalledWith(fontSize);
      expect(wrapper.vm.editor.getFontSize()).toEqual(fontSize);
    });

    test("should cleanup events on unmount", () => {
      const clearEventsSpy = vi.spyOn(wrapper.vm, "clearEvents");

      wrapper.unmount();

      expect(clearEventsSpy).toHaveBeenCalled();
    });
  });
});
