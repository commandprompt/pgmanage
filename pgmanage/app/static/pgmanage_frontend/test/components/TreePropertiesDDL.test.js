import { describe, test, expect, vi, beforeAll } from "vitest";
import { mount } from "@vue/test-utils";
import "ace-builds";
import "ace-builds/esm-resolver";
import "../../src/ace_extras/themes/theme-omnidb.js";
import TreePropertiesDDL from "@/components/TreePropertiesDDL.vue";
import { useSettingsStore } from "../../src/stores/settings.js";

vi.hoisted(() => {
  vi.stubGlobal("v_csrf_cookie_name", "test_cookie");
  vi.stubGlobal("app_base_path", "test_folder");
});

vi.mock("tabulator-tables", () => {
  const TabulatorFull = vi.fn();
  TabulatorFull.prototype.redraw = vi.fn();
  TabulatorFull.prototype.setData = vi.fn();
  return { TabulatorFull };
});

describe("TreePropertiesDDL.vue", () => {
  let settingsStore;
  beforeAll(() => {
    settingsStore = useSettingsStore();
    settingsStore.setEditorTheme("omnidb");
  });

  test("renders component", () => {
    const wrapper = mount(TreePropertiesDDL);
    expect(wrapper.html()).toContain("Properties");
    expect(wrapper.html()).toContain("DDL");
  });
  test("emits toggleTreeTabs event on toggle button click", () => {
    const wrapper = mount(TreePropertiesDDL);

    wrapper.find('[data-testid="tree-tabs-toggler"]').trigger("click");

    expect(wrapper.emitted()).toHaveProperty("toggleTreeTabs");
  });
  test("shows loading spinner when showLoading prop is true", async () => {
    const wrapper = mount(TreePropertiesDDL);

    await wrapper.setProps({ showLoading: true });

    expect(wrapper.html()).toContain("Loading...");
  });
});
