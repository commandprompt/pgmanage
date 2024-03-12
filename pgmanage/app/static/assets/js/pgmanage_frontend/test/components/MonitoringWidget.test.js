import { mount } from "@vue/test-utils";
import {
  beforeEach,
  afterEach,
  describe,
  expect,
  test,
  beforeAll,
  vi,
} from "vitest";
import MonitoringWidget from "../../src/components/MonitoringWidget.vue";
import axios from "axios";
import { emitter } from "../../src/emitter";

vi.mock("axios");

vi.mock("tabulator-tables", () => {
  const TabulatorFull = vi.fn();
  TabulatorFull.prototype.redraw = vi.fn();
  TabulatorFull.prototype.setData = vi.fn();
  return { TabulatorFull };
});

vi.mock("@/workspace.js", () => {
  const renameTab = vi.fn();
  const showMenuNewTabOuter = vi.fn();
  return { renameTab, showMenuNewTabOuter };
});

describe("MonitoringWidget", () => {
  let wrapper;
  const monitoringWidget = {
    interval: 10,
    saved_id: 1,
    title: "Test Widget",
    type: "grid",
  };
  const tabId = "testTabId";

  beforeAll(() => {
    axios.post.mockResolvedValue({ data: { data: "1234" } });
    axios.patch.mockResolvedValue("Success");
  });

  beforeEach(() => {
    vi.useFakeTimers();
    wrapper = mount(MonitoringWidget, {
      props: {
        monitoringWidget: monitoringWidget,
        connId: "testConnId",
        tabId: tabId,
        databaseIndex: 1,
        refreshWidget: false,
      },
      attachTo: document.body,
      shallow: true,
    });
  });

  afterEach(() => {
    wrapper.unmount();
  });

  test("should render MonitoringWidget component with expected elements", () => {
    const title = wrapper.get("[data-testid='widget-title']");

    expect(title.text()).toBe("Test Widget");
  });

  test("should call testMonitoringWidget on mount when test widget", () => {
    wrapper.unmount();

    const testMonitoringWidgetSpy = vi.spyOn(
      MonitoringWidget.methods,
      "testMonitoringWidget"
    );

    wrapper = mount(MonitoringWidget, {
      props: {
        monitoringWidget: monitoringWidget,
        connId: "ConnectionId",
        isTestWidget: true,
      },
      attachTo: document.body,
    });

    expect(testMonitoringWidgetSpy).toBeCalled();
  });
  test("should call refreshMonitoringWidget on mount", () => {
    wrapper.unmount();

    const refreshMonitoringWidgetSpy = vi.spyOn(
      MonitoringWidget.methods,
      "refreshMonitoringWidget"
    );

    wrapper = mount(MonitoringWidget, {
      props: {
        monitoringWidget: monitoringWidget,
        connId: "ConnectionId",
      },
      attachTo: document.body,
    });
    expect(refreshMonitoringWidgetSpy).toBeCalled();
  });

  test("should pause monitoring widget on pause button click", async () => {
    const pauseMonitoringWidgetSpy = vi.spyOn(
      wrapper.vm,
      "pauseMonitoringWidget"
    );

    await wrapper.find("[data-testid='widget-pause-button']").trigger("click");

    expect(pauseMonitoringWidgetSpy).toHaveBeenCalledOnce();

    expect(wrapper.vm.isActive).toBeFalsy();
  });
  test("should enable monitoring widget on play button click", async () => {
    const playMonitoringWidgetSpy = vi.spyOn(
      wrapper.vm,
      "playMonitoringWidget"
    );

    await wrapper.find("[data-testid='widget-pause-button']").trigger("click");

    await wrapper.find("[data-testid='widget-play-button']").trigger("click");

    expect(playMonitoringWidgetSpy).toHaveBeenCalledOnce();

    expect(wrapper.vm.isActive).toBeTruthy();
  });
  test("should call closeMonitoringWidget on close button click", async () => {
    const closeMonitoringWidgetSpy = vi.spyOn(
      wrapper.vm,
      "closeMonitoringWidget"
    );

    await wrapper.find("[data-testid='widget-close-button']").trigger("click");

    expect(closeMonitoringWidgetSpy).toHaveBeenCalledOnce();

    expect(wrapper.vm.isActive).toBeTruthy();

    expect(wrapper.emitted()).toHaveProperty("widgetClose");
  });
  test("should call refreshMonitoringWidget on refresh button click", async () => {
    const refreshMonitoringWidgetSpy = vi.spyOn(
      wrapper.vm,
      "refreshMonitoringWidget"
    );

    await wrapper
      .find("[data-testid='widget-refresh-button']")
      .trigger("click");

    expect(refreshMonitoringWidgetSpy).toHaveBeenCalledOnce();
  });
  test("should call updateInterval method on interval input change", async () => {
    const updateIntervalSpy = vi.spyOn(wrapper.vm, "updateInterval");
    const intervalInput = wrapper.find('[data-testid="widget-interval-input"]');

    await intervalInput.setValue("10");

    expect(intervalInput.element.value).toBe("10");
    expect(updateIntervalSpy).toBeCalledTimes(1);
  });
  test("validation error after input interval change to < 5", async () => {
    const intervalInput = wrapper.find('[data-testid="widget-interval-input"]');

    await intervalInput.setValue("2");

    expect(wrapper.vm.v$.$invalid).toBeTruthy();
  });
  test("should call buildGrid if widget type is 'grid'", async () => {
    const buildGridSpy = vi.spyOn(wrapper.vm, "buildGrid");

    await wrapper
      .find("[data-testid='widget-refresh-button']")
      .trigger("click");

    expect(buildGridSpy).toBeCalledTimes(1);
  });
  test.todo(
    "should call buildChart if widget type is chart,timeseries or chart_append"
  );
  describe("Events", () => {
    test("should emit 'widgetRefreshed' event on refreshWidget 'true'", async () => {
      await wrapper.setProps({ refreshWidget: true });

      expect(wrapper.emitted()).toHaveProperty("widgetRefreshed");
    });
    test("should emit 'intervalUpdated' after success patch request", async () => {
      const intervalInput = wrapper.find(
        '[data-testid="widget-interval-input"]'
      );

      await intervalInput.setValue("11");

      expect(wrapper.emitted()).toHaveProperty("intervalUpdated");
      expect(wrapper.emitted("intervalUpdated")[0]).toEqual([
        {
          saved_id: monitoringWidget.saved_id,
          interval: 11,
        },
      ]);
    });
    test("should redraw widget grid on redraw_widget_grid event", () => {
      const redrawSpy = vi.spyOn(wrapper.vm.visualizationObject, "redraw");

      emitter.emit(`${tabId}_redraw_widget_grid`);

      expect(redrawSpy).toBeCalledTimes(1);
    });
    test("should clean up events on unmount", () => {
      const clearEventsAndTimeoutSpy = vi.spyOn(
        wrapper.vm,
        "clearEventsAndTimeout"
      );

      wrapper.unmount();

      expect(clearEventsAndTimeoutSpy).toHaveBeenCalled();
    });
  });
});
