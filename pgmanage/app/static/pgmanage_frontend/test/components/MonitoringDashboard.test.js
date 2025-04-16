import { flushPromises, mount } from "@vue/test-utils";
import { test, describe, beforeEach, vi, expect } from "vitest";
import MonitoringDashboard from "@src/components/MonitoringDashboard.vue";
import MonitoringWidget from "@src/components/MonitoringWidget.vue";

import axios from "axios";

vi.hoisted(() => {
  vi.stubGlobal("v_csrf_cookie_name", "test_cookie");
  vi.stubGlobal("app_base_path", "test_folder");
});

vi.mock("axios");

vi.mock("tabulator-tables", () => {
  const TabulatorFull = vi.fn();
  TabulatorFull.prototype.redraw = vi.fn();
  TabulatorFull.prototype.setData = vi.fn();
  TabulatorFull.prototype.replaceData = vi.fn();
  return { TabulatorFull };
});

const mockWidgetsData = [
  {
    saved_id: 1,
    id: 0,
    title: "Backends",
    plugin_name: "postgresql",
    interval: 5,
    type: "grid",
    widget_data: null,
  },
];

describe("MonitoringDashboard", () => {
  let dashboardWrapper;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.restoreAllMocks();
    axios.delete.mockResolvedValue("Deleted");
    axios.post
      .mockResolvedValue({ data: { data: "1234" } })
      .mockResolvedValueOnce({ data: { widgets: [...mockWidgetsData] } })
      .mockResolvedValueOnce({ data: { data: "1234" } });
    dashboardWrapper = mount(MonitoringDashboard, {
      props: {
        workspaceId: "workspaceId",
      },
    });
  });

  test("should render MonitoringDashboard component with expected elements", () => {
    expect(dashboardWrapper.html()).toContain("Backends");
    expect(dashboardWrapper.html()).toContain("Manage Widgets");
    expect(dashboardWrapper.html()).toContain("Refresh All");
  });

  test("should refresh all widgets on 'Refresh All' button click", async () => {
    const refreshWidgetsSpy = vi.spyOn(dashboardWrapper.vm, "refreshWidgets");

    await dashboardWrapper
      .get('[data-testid="refresh-all-widgets-button"]')
      .trigger("click");

    expect(refreshWidgetsSpy).toBeCalledTimes(1);

    expect(
      dashboardWrapper.getComponent(MonitoringWidget).emitted()
    ).toHaveProperty("widgetRefreshed");
  });

  test("should not refresh widgets when none exist", async () => {
    await dashboardWrapper.vm.closeWidget(1);
    await flushPromises();

    const refreshWidgetsSpy = vi.spyOn(dashboardWrapper.vm, "refreshWidgets");
    await dashboardWrapper
      .get('[data-testid="refresh-all-widgets-button"]')
      .trigger("click");

    expect(refreshWidgetsSpy).toBeCalledTimes(1);
    expect(dashboardWrapper.vm.refreshWidget).toBe(false);
  });

  test("should remove widget from dashboard on 'close' button click", async () => {
    expect(dashboardWrapper.vm.widgets).toStrictEqual(mockWidgetsData);

    await dashboardWrapper
      .get("[data-testid='widget-close-button']")
      .trigger("click");

    expect(dashboardWrapper.vm.widgets).toStrictEqual([]);
  });
});
