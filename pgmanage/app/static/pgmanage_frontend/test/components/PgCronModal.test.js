import { flushPromises, mount } from "@vue/test-utils";
import PgCronModal from "@/components/PgCronModal.vue";
import { operationModes } from "@/constants";
import axios from "axios";
import { vi, describe, it, expect, beforeAll, afterEach } from "vitest";
import { useSettingsStore } from "@/stores/settings.js";
import "@/ace_extras/themes/theme-omnidb.js";
import "ace-builds/src-noconflict/mode-sql";
import { emitter } from "@/emitter";
import { operationModes } from "@/constants";
import { handleError } from "@/logging/utils";

vi.hoisted(() => {
  vi.stubGlobal("v_csrf_cookie_name", "test_cookie");
  vi.stubGlobal("app_base_path", "test_folder");
});

vi.mock("@/logging/utils", () => ({
  handleError: vi.fn(),
}));

vi.mock("@/notification_control", () => ({
  showToast: vi.fn(),
}));

vi.mock("tabulator-tables", () => {
  const TabulatorFull = vi.fn();
  TabulatorFull.prototype.redraw = vi.fn();
  TabulatorFull.prototype.setData = vi.fn();
  TabulatorFull.prototype.replaceData = vi.fn();
  return { TabulatorFull };
});

vi.mock("axios");
vi.mock("bootstrap", () => ({
  Modal: {
    getOrCreateInstance: vi.fn(() => ({ show: vi.fn(), hide: vi.fn() })),
  },
}));

const mountComponent = (props = {}) => {
  return mount(PgCronModal, {
    props: {
      mode: operationModes.CREATE,
      treeNode: {
        data: {
          job_meta: "job_meta",
        },
      },
      workspaceId: "test-workspace",
      databaseIndex: 1,
      ...props,
    },
    shallow: true,
  });
};

describe("PgCronModal.vue", () => {
  let settingsStore;

  beforeAll(() => {
    settingsStore = useSettingsStore();
    settingsStore.setEditorTheme("omnidb");
    axios.post.mockResolvedValue({
      data: [{ name: "test_db" }],
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("renders correctly", () => {
    const wrapper = mountComponent();
    expect(wrapper.exists()).toBe(true);
  });

  it("displays the correct modal title", async () => {
    const wrapper = mountComponent({ mode: operationModes.UPDATE });
    expect(wrapper.find(".modal-title").text()).toBe("Edit Job");

    await wrapper.setProps({ mode: operationModes.CREATE });
    expect(wrapper.find(".modal-title").text()).toBe("Create Job");
  });

  it('renders the "Job Schedule" tab as active', () => {
    const wrapper = mountComponent();
    expect(wrapper.find("#job_schedule-tab").classes()).toContain("active");
    expect(wrapper.find("#job_schedule").classes()).toContain("show", "active");
  });

  it('renders the "Job Statistics" tab only in update mode', async () => {
    const wrapper = mountComponent();
    expect(wrapper.find("#job_statistics-tab").exists()).toBe(false);
    await wrapper.setProps({ mode: operationModes.UPDATE });
    expect(wrapper.find("#job_statistics-tab").exists()).toBe(true);
  });

  it("validates job name correctly", async () => {
    const wrapper = mountComponent();
    await wrapper.setData({ jobName: "" });
    await wrapper.vm.v$.$validate();
    expect(wrapper.vm.v$.jobName.$invalid).toBe(true);
  });

  it("calls getDatabases on mount", () => {
    const wrapper = mountComponent();
    expect(axios.post).toHaveBeenCalledWith("/get_databases_postgresql/", {
      database_index: 1,
      workspace_id: "test-workspace",
    });
  });

  it("saves a job successfully", async () => {
    const wrapper = mountComponent();
    await wrapper.setData({
      jobName: "Test Job",
      command: "SELECT 1;",
      schedule: "* * * * *",
      inDatabase: "test_db",
    });
    await wrapper.vm.saveJob();
    expect(axios.post).toHaveBeenCalledWith(
      "/save_pgcron_job/",
      expect.any(Object)
    );
  });

  it("disables schedule input when manual input is off", async () => {
    const wrapper = mountComponent();
    expect(wrapper.find("#schedule_override").attributes()).toHaveProperty(
      "disabled"
    );
  });

  it("shows job statistics tab in update mode", async () => {
    const wrapper = mountComponent({ mode: operationModes.UPDATE });
    expect(wrapper.find("#job_statistics-tab").exists()).toBe(true);
  });

  it("calls axios and sets job logs when setupJobStatisticsTab is called", async () => {
    const mockResponse = {
      data: {
        logs: [
          {
            runid: 1,
            job_pid: 123,
            database: "db1",
            username: "user1",
            status: "succeeded",
            start_time: "2025-02-17T12:00:00Z",
            end_time: "2025-02-17T12:05:00Z",
            return_message: "Success",
            command: "SELECT 1;",
          },
        ],
        stats: { total_runs: 5 },
      },
    };

    const wrapper = mountComponent();
    axios.post.mockResolvedValueOnce(mockResponse);
    await wrapper.vm.setupJobStatisticsTab();

    await flushPromises();
    expect(axios.post).toHaveBeenCalledWith("/get_pgcron_job_logs/", {
      database_index: wrapper.vm.databaseIndex,
      workspace_id: wrapper.vm.workspaceId,
      job_meta: wrapper.vm.treeNode.data.job_meta,
    });

    expect(wrapper.vm.jobLogs).toHaveLength(1);
    expect(wrapper.vm.jobLogs[0].job_pid).toBe(123);
    expect(wrapper.vm.jobStats).toEqual(mockResponse.data.stats);
  });

  it("handles API error in setupJobStatisticsTab", async () => {
    const errorResponse = {
      response: { data: { data: "Error fetching logs" } },
    };
    const wrapper = mountComponent();
    axios.post.mockRejectedValue(errorResponse);

    await wrapper.vm.setupJobStatisticsTab();
    await flushPromises();

    expect(axios.post).toHaveBeenCalled();
    expect(handleError).toHaveBeenCalledWith(errorResponse);
  });

  it("calls axios and refreshes job statistics when clearJobStats is called", async () => {
    const mockResponse = {
      data: {
        logs: [
          {
            runid: 1,
            job_pid: 123,
            database: "db1",
            username: "user1",
            status: "succeeded",
            start_time: "2025-02-17T12:00:00Z",
            end_time: "2025-02-17T12:05:00Z",
            return_message: "Success",
            command: "SELECT 1;",
          },
        ],
        stats: { total_runs: 5 },
      },
    };
    const wrapper = mountComponent();
    axios.post.mockResolvedValueOnce({}).mockResolvedValueOnce(mockResponse);

    const setupJobStatisticsTabSpy = vi.spyOn(
      wrapper.vm,
      "setupJobStatisticsTab"
    );

    await wrapper.vm.clearJobStats();

    await flushPromises();

    expect(axios.post).toHaveBeenCalledWith("/delete_pgcron_job_logs/", {
      database_index: wrapper.vm.databaseIndex,
      workspace_id: wrapper.vm.workspaceId,
      job_meta: wrapper.vm.treeNode.data.job_meta,
    });

    expect(setupJobStatisticsTabSpy).toHaveBeenCalled();
  });

  it("handles API error in clearJobStats", async () => {
    const errorResponse = {
      response: { data: { data: "Error clearing logs" } },
    };
    const wrapper = mountComponent();
    axios.post.mockRejectedValue(errorResponse);

    await wrapper.vm.clearJobStats();

    await flushPromises();

    expect(axios.post).toHaveBeenCalled();
    expect(handleError).toHaveBeenCalledWith(errorResponse);
  });

  it("calls API, emits event, and hides modal when deleteJob is successful", async () => {
    const emitterDeleteSpy = vi.spyOn(emitter, "emit");

    const wrapper = mountComponent();
    axios.post.mockResolvedValueOnce({ data: { success: true } });

    await wrapper.vm.deleteJob();
    await flushPromises();

    expect(axios.post).toHaveBeenCalledWith("/delete_pgcron_job/", {
      database_index: wrapper.vm.databaseIndex,
      workspace_id: wrapper.vm.workspaceId,
      job_meta: wrapper.vm.treeNode.data.job_meta,
    });

    expect(emitterDeleteSpy).toHaveBeenCalledWith(
      `removeNode_${wrapper.vm.workspaceId}`,
      {
        node: wrapper.vm.treeNode,
      }
    );
  });

  it("handles API error and calls showToast when deleteJob fails", async () => {
    const errorResponse = {
      response: { data: { data: "Error deleting job" } },
    };
    const wrapper = mountComponent();

    axios.post.mockRejectedValue(errorResponse);

    await wrapper.vm.deleteJob();
    await flushPromises();

    expect(handleError).toHaveBeenCalledWith(errorResponse);
  });
});
