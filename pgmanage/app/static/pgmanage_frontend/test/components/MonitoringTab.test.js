import { mount } from "@vue/test-utils";
import { test, describe, beforeEach, vi, expect } from "vitest";
import MonitoringTab from "@src/components/MonitoringTab.vue";

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

describe("MonitoringTab", () => {
  let monTabWrapper;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.restoreAllMocks();
    axios.delete.mockResolvedValue("Deleted");
    axios.post
      .mockResolvedValue({ data: { data: [{ name: "1234" }] } })
      .mockResolvedValueOnce({ data: { data: [{ name: "1234" }] } });
    monTabWrapper = mount(MonitoringTab, {
      props: {
        workspaceId: "workspaceId",
      },
    });
  });

  test("should render MonitoringTab component with expected elements", () => {
    expect(monTabWrapper.html()).toContain("Refresh");
    expect(monTabWrapper.html()).toContain("Pause");
    expect(monTabWrapper.html()).toContain("refresh-menu__link");
  });

  test("should refresh grid data on 'Refresh' button click", async () => {
    const refreshWidgetsSpy = vi.spyOn(monTabWrapper.vm, "refreshMonitoring");

    await monTabWrapper
      .get('[data-testid="monitoring-refresh-button"]')
      .trigger("click");

    expect(refreshWidgetsSpy).toBeCalledTimes(1);
  });

  test("should change refresh interval when different refresh option is clicked", async () => {
    await monTabWrapper
      .get('[data-testid="refresh-option-300"]')
      .trigger("click");
    expect(monTabWrapper.vm.monitoringInterval).toBe(300);
  });

  test("should pause refresh when pause button is is clicked", async () => {
    const pauseSpy = vi.spyOn(monTabWrapper.vm, "pauseMonitoring");
    await monTabWrapper
      .get('[data-testid="monitoring-pause-button"]')
      .trigger("click");
    expect(pauseSpy).toBeCalledTimes(1);
  });

  test("should resume refresh when play button is is clicked", async () => {
    const playSpy = vi.spyOn(monTabWrapper.vm, "playMonitoring");
    await monTabWrapper
      .get('[data-testid="monitoring-pause-button"]')
      .trigger("click");
    await monTabWrapper
      .get('[data-testid="monitoring-play-button"]')
      .trigger("click");
    expect(playSpy).toBeCalledTimes(1);
  });
});
