import { mount, flushPromises } from "@vue/test-utils";
import { describe, it, expect, vi, beforeEach } from "vitest";
import MonitoringTabLogs from "@src/components/MonitoringTabLogs.vue";
import axios from "axios";
import { nextTick } from "vue";

let aceMockEditor;
beforeEach(() => {
  axios.post
    .mockResolvedValueOnce({ data: { formats: ["stderr", "jsonlog"] } })
    .mockResolvedValueOnce({ data: { version: "15" } });

  aceMockEditor = {
    setValue: vi.fn(),
    clearSelection: vi.fn(),
    session: {
      setMode: vi.fn(),
      insert: vi.fn(),
      getLength: vi.fn(() => 1),
    },
    renderer: {
      scrollToLine: vi.fn(),
    },
    setTheme: vi.fn(),
    setFontSize: vi.fn(),
    setShowPrintMargin: vi.fn(),
    setReadOnly: vi.fn(),
    execCommand: vi.fn(),
    commands: { bindKey: vi.fn() },
  };

  global.ace = {
    edit: vi.fn(() => aceMockEditor),
  };

  document.getElementById = vi.fn(() => ({
    addEventListener: vi.fn(),
  }));
});

describe("MonitoringTabLogs.vue", () => {
  let wrapper;

  const defaultProps = {
    databaseIndex: 1,
    workspaceId: "ws1",
    tabId: "test-tab",
  };

  const mountComponent = () =>
    mount(MonitoringTabLogs, {
      props: defaultProps,
    });

  it("renders warning when logging is disabled", async () => {
    axios.post.mockResolvedValueOnce({
      data: { logs: null, current_logfile: "logfile", log_offset: 0 },
    });

    wrapper = mountComponent();
    await wrapper.vm.getLog();
    await nextTick();

    expect(wrapper.text()).toContain("Please enable logging");
    expect(wrapper.find("a").attributes("href")).toContain(
      "/runtime-config-logging.html"
    );
  });

  it("calls getLogFormat and getServerVersion on mount", async () => {
    wrapper = mountComponent();
    await flushPromises();

    expect(axios.post).toHaveBeenCalledWith(
      "/get_postgres_server_log_formats/",
      {
        database_index: defaultProps.databaseIndex,
        workspace_id: defaultProps.workspaceId,
      }
    );

    expect(axios.post).toHaveBeenCalledWith("/get_postgresql_version/", {
      database_index: defaultProps.databaseIndex,
      workspace_id: defaultProps.workspaceId,
    });

    expect(wrapper.vm.formatModes.jsonlog.available).toBe(true);
  });

  it("applies log data to editor and enables autoscroll", async () => {
    axios.post.mockResolvedValueOnce({
      data: {
        logs: "some logs",
        current_logfile: "logfile",
        log_offset: 123,
      },
    });

    wrapper = mountComponent();
    await wrapper.vm.getLog();
    expect(aceMockEditor.setValue).toHaveBeenCalledWith("some logs");
    expect(aceMockEditor.clearSelection).toHaveBeenCalled();
    await nextTick();
    expect(aceMockEditor.renderer.scrollToLine).toHaveBeenCalledWith(
      Number.POSITIVE_INFINITY
    );
  });

  it("watcher triggers log reload and mode update", async () => {
    axios.post.mockResolvedValueOnce({
      data: { logs: null, current_logfile: "logfile", log_offset: 0 },
    });
    wrapper = mountComponent();

    wrapper.vm.formatModes = {
      jsonlog: {
        ace_mode: "ace/mode/json",
        available: true,
        text: "JSON",
      },
    };

    const getLogSpy = vi.spyOn(wrapper.vm, "getLog");
    wrapper.vm.formatMode = "jsonlog";
    await nextTick();

    expect(aceMockEditor.session.setMode).toHaveBeenCalledWith("ace/mode/json");
    expect(getLogSpy).toHaveBeenCalled();
  });

  it("calls showFind and editor.execCommand", () => {
    wrapper = mountComponent();
    wrapper.vm.showFind();
    expect(aceMockEditor.execCommand).toHaveBeenCalledWith("find");
  });

  it("clears interval and event listeners on unmount", () => {
    const clearSpy = vi.spyOn(window, "removeEventListener");
    wrapper = mountComponent();
    wrapper.unmount();
    expect(clearSpy).toHaveBeenCalled();
  });

  it("toggles autoScroll checkbox and formats dropdown", async () => {
    wrapper = mountComponent();
    wrapper.vm.formatModes = {
      stderr: { available: true, text: "TEXT" },
      jsonlog: { available: true, text: "JSON" },
    };
    await nextTick();
    const checkbox = wrapper.find("input[type='checkbox']");
    expect(checkbox.exists()).toBe(true);
    checkbox.setValue(false);
    expect(wrapper.vm.autoScroll).toBe(false);

    const options = wrapper.findAll("option");
    expect(options.length).toBe(2);
    expect(options[0].text()).toBe("TEXT");
    expect(options[1].text()).toBe("JSON");
  });
});
