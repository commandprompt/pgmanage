import { describe, test, vi, expect, beforeEach, beforeAll } from "vitest";
import axios from "axios";
import { flushPromises, mount } from "@vue/test-utils";
import "ace-builds/src-noconflict/ace";
import "ace-builds/src-noconflict/mode-python";
import "../../src/ace_themes/theme-omnidb.js";

import MonitoringWidgetEditModal from "../../src/components/MonitoringWidgetEditModal.vue";
import { useSettingsStore } from "../../src/stores/settings.js";

vi.mock("axios");

window.$ = vi.fn().mockImplementation(() => {
  return {
    modal: vi.fn(),
    on: vi.fn(),
  };
});

vi.mock("@/workspace.js", () => {
  const renameTab = vi.fn();
  const showMenuNewTabOuter = vi.fn();
  return { renameTab, showMenuNewTabOuter };
});

describe("MonitoringWidgetEditModal", () => {
  let wrapper, settingsStore;

  beforeAll(() => {
    settingsStore = useSettingsStore();
    settingsStore.setEditorTheme("omnidb");
    axios.put.mockResolvedValue("success response");
    axios.post.mockResolvedValue({ data: { data: "test data" } });
    axios.get.mockResolvedValue({ data: { data: "test data" } });
  });

  beforeEach(async () => {
    wrapper = mount(MonitoringWidgetEditModal, {
      shallow: true,
      attachTo: document.body,
      global: {
        stubs: {
          teleport: true,
        },
      },
      props: {
        showTestWidget: false,
      },
    });

    await wrapper.setProps({ modalVisible: true });
  });
  test("should render MonitoringWidgetEditModal component with expected elements", () => {
    const title = wrapper.get("[data-testid='widget-edit-header-title']");

    expect(title.text()).toBe("Monitoring Widget");
  });

  test("should call updateMonitoringWidget method on widget save", async () => {
    const updateMonitoringWidgetSpy = vi.spyOn(
      wrapper.vm,
      "updateMonitoringWidget"
    );

    await wrapper.setProps({ widgetId: 123 });
    await wrapper.setData({ widgetName: "Test Widget" });

    await wrapper
      .get("[data-testid='widget-edit-name']")
      .setValue("Test Widget");
    await wrapper
      .get("[data-testid='widget-edit-refresh-interval']")
      .setValue(10);

    await wrapper
      .get("[data-testid='widget-edit-save-button']")
      .trigger("click");
    expect(updateMonitoringWidgetSpy).toHaveBeenCalledOnce();
  });
  test("should call createMonitoringWidget method on widget save", async () => {
    const createMonitoringWidget = vi.spyOn(
      wrapper.vm,
      "createMonitoringWidget"
    );

    await wrapper
      .get("[data-testid='widget-edit-name']")
      .setValue("Test Widget");
    await wrapper
      .get("[data-testid='widget-edit-refresh-interval']")
      .setValue(10);

    await wrapper
      .get("[data-testid='widget-edit-save-button']")
      .trigger("click");
    expect(createMonitoringWidget).toHaveBeenCalledOnce();
  });
  test("validation error on change widget name to empty", async () => {
    await wrapper
      .get("[data-testid='widget-edit-name']")
      .setValue("Test Widget");
    await wrapper
      .get("[data-testid='widget-edit-refresh-interval']")
      .setValue(10);

    wrapper.vm.v$.$validate();
    expect(wrapper.vm.v$.widgetName.$invalid).toBeFalsy();

    await wrapper.get("[data-testid='widget-edit-name']").setValue("");

    wrapper.vm.v$.$validate();
    expect(wrapper.vm.v$.widgetName.$invalid).toBeTruthy();
  });
  test("validation error on change widget interval to below 5 or empty", async () => {
    await wrapper
      .get("[data-testid='widget-edit-name']")
      .setValue("Test Widget");
    await wrapper
      .get("[data-testid='widget-edit-refresh-interval']")
      .setValue(10);
    wrapper.vm.v$.$validate();
    expect(wrapper.vm.v$.widgetInterval.$invalid).toBeFalsy();

    await wrapper
      .get("[data-testid='widget-edit-refresh-interval']")
      .setValue(4);

    wrapper.vm.v$.$validate();

    expect(wrapper.vm.v$.widgetInterval.$invalid).toBeTruthy();

    await wrapper
      .get("[data-testid='widget-edit-refresh-interval']")
      .setValue("");

    wrapper.vm.v$.$validate();
    expect(wrapper.vm.v$.widgetInterval.$invalid).toBeTruthy();
  });
  test("disabled 'Save' button when validation error", async () => {
    await wrapper.get("[data-testid='widget-edit-name']").setValue("");
    await wrapper
      .get("[data-testid='widget-edit-refresh-interval']")
      .setValue("4");

    wrapper.vm.v$.$validate();

    const saveBtn = wrapper.get("[data-testid='widget-edit-save-button']");
    expect(saveBtn.attributes()).toHaveProperty("disabled");
  });
  test("paste template to editors on template change", async () => {
    const changeTemplateSpy = vi.spyOn(wrapper.vm, "changeTemplate");
    const dataEditorInstance = wrapper.vm.dataEditor;
    const scriptEditorInstance = wrapper.vm.scriptEditor;

    axios.post.mockResolvedValue({
      data: {
        script_data: "Test script data",
        script_chart: "Test script chart",
      },
    });
    await wrapper.setData({
      widgets: [{ id: 1, type: "chart", title: "Chart template" }],
    });

    let selectEl = await wrapper.get(
      "[data-testid='widget-edit-template-select']"
    ).element;

    selectEl.selectedIndex = 1;
    selectEl.dispatchEvent(new Event("change"));

    await flushPromises();

    expect(changeTemplateSpy).toBeCalledTimes(1);

    expect(dataEditorInstance.getValue()).toContain("Test script data");
    expect(scriptEditorInstance.getValue()).toContain("Test script chart");
  });
  test("show test widget on 'Test' button click", async () => {
    expect(
      wrapper.find('[data-testid="widget-edit-test-wrapper"]').exists()
    ).toBeFalsy();
    await wrapper
      .get('[data-testid="widget-edit-test-button"]')
      .trigger("click");

    expect(
      wrapper.find('[data-testid="widget-edit-test-wrapper"]').exists()
    ).toBeTruthy();
  });
});
