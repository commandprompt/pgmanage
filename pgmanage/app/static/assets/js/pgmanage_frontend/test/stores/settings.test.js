import { setActivePinia, createPinia } from "pinia";
import { describe, it, expect, beforeEach, vi } from "vitest";
import { useSettingsStore } from "@/stores/settings";
import axios from "axios";
import { showToast } from "@/notification_control";
import moment from "moment";
import { Modal } from "bootstrap";

vi.mock("axios");
vi.mock("@/notification_control", () => ({
  showToast: vi.fn(),
}));

vi.mock("bootstrap", () => ({
  Modal: {
    getOrCreateInstance: vi.fn(() => ({
      show: vi.fn(),
    })),
  },
}));

describe("settings store", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("initializes with default state", () => {
    const store = useSettingsStore();
    expect(store.desktopMode).toBe(window.gv_desktopMode);
    expect(store.fontSize).toBe("");
    expect(store.theme).toBe("");
    expect(store.editorTheme).toBe("");
    expect(store.terminalTheme).toBe("");
    expect(store.restoreTabs).toBe("");
    expect(store.scrollTree).toBe("");
    expect(store.binaryPath).toBe("");
    expect(store.pigzPath).toBe("");
    expect(store.csvEncoding).toBe("");
    expect(store.csvDelimiter).toBe("");
    expect(store.dateFormat).toBe("");
    expect(store.shortcuts).toEqual({});
    expect(store.currentOS).toBe("Unknown OS");
  });

  it("fetches settings and updates state", async () => {
    const store = useSettingsStore();
    axios.get.mockResolvedValue({
      data: {
        settings: {
          font_size: "16",
          theme: "dark",
          editor_theme: "monokai",
          restore_tabs: true,
          scroll_tree: false,
          binary_path: "/usr/bin",
          pigz_path: "/usr/local/bin",
          csv_encoding: "UTF-8",
          csv_delimiter: ",",
          date_format: "YYYY-MM-DD",
        },
        shortcuts: { copy: "Ctrl+C" },
      },
    });

    const response = await store.getSettings();
    expect(response).toEqual({
      settings: {
        font_size: "16",
        theme: "dark",
        editor_theme: "monokai",
        restore_tabs: true,
        scroll_tree: false,
        binary_path: "/usr/bin",
        pigz_path: "/usr/local/bin",
        csv_encoding: "UTF-8",
        csv_delimiter: ",",
        date_format: "YYYY-MM-DD",
      },
      shortcuts: { copy: "Ctrl+C" },
    });

    expect(store.fontSize).toBe("16");
    expect(store.theme).toBe("dark");
    expect(store.editorTheme).toBe("monokai");
    expect(store.restoreTabs).toBe(true);
    expect(store.scrollTree).toBe(false);
    expect(store.binaryPath).toBe("/usr/bin");
    expect(store.pigzPath).toBe("/usr/local/bin");
    expect(store.csvEncoding).toBe("UTF-8");
    expect(store.csvDelimiter).toBe(",");
    expect(store.dateFormat).toBe("YYYY-MM-DD");
    expect(store.shortcuts).toEqual({ copy: "Ctrl+C" });
    expect(moment.defaultFormat).toBe("YYYY-MM-DD");
    expect(document.documentElement.style.fontSize).toBe("16px");
  });

  it("uses default date format during fetch settings", async () => {
    const store = useSettingsStore();
    axios.get.mockResolvedValue({
      data: {
        settings: {
          font_size: "16",
          theme: "dark",
          editor_theme: "monokai",
          restore_tabs: true,
          scroll_tree: false,
          binary_path: "/usr/bin",
          pigz_path: "/usr/local/bin",
          csv_encoding: "UTF-8",
          csv_delimiter: ",",
        },
        shortcuts: { copy: "Ctrl+C" },
      },
    });

    const response = await store.getSettings();

    expect(store.dateFormat).toBe("YYYY-MM-DD, HH:mm:ss");
    expect(moment.defaultFormat).toBe("YYYY-MM-DD, HH:mm:ss");
  });

  it("handles error in getSettings", async () => {
    const store = useSettingsStore();
    axios.get.mockRejectedValue({
      response: {
        data: { data: "Error message" },
      },
    });

    const response = await store.getSettings();
    expect(response.response.data.data).toBe("Error message");
    expect(showToast).toHaveBeenCalledWith("error", "Error message");
  });

  it("saves settings and shows success toast", async () => {
    const store = useSettingsStore();
    store.fontSize = "16";
    store.theme = "dark";
    store.csvEncoding = "UTF-8";
    store.csvDelimiter = ",";
    store.binaryPath = "/usr/bin";
    store.dateFormat = "YYYY-MM-DD";
    store.pigzPath = "/usr/local/bin";
    store.restoreTabs = true;
    store.scrollTree = false;
    store.currentOS = "Linux";
    store.shortcuts = { copy: "Ctrl+C" };

    axios.post.mockResolvedValue({
      data: "Success",
    });

    const response = await store.saveSettings();
    expect(response).toBe("Success");
    expect(showToast).toHaveBeenCalledWith("success", "Configuration saved.");
    expect(moment.defaultFormat).toBe("YYYY-MM-DD");
  });

  it("handles error in saveSettings", async () => {
    const store = useSettingsStore();
    axios.post.mockRejectedValue({
      response: {
        data: { data: "Error message" },
      },
    });

    const response = await store.saveSettings();
    expect(response.response.data.data).toBe("Error message");
    expect(showToast).toHaveBeenCalledWith("error", "Error message");
  });

  it("sets fontSize", () => {
    const store = useSettingsStore();
    store.setFontSize("16");
    expect(store.fontSize).toBe("16");
  });

  it("sets theme", () => {
    const store = useSettingsStore();
    store.setTheme("dark");
    expect(store.theme).toBe("dark");
  });

  it("sets editorTheme", () => {
    const store = useSettingsStore();
    store.setEditorTheme("monokai");
    expect(store.editorTheme).toBe("monokai");
  });

  it("sets terminalTheme", () => {
    const store = useSettingsStore();
    store.setTerminalTheme("monokai");
    expect(store.terminalTheme).toBe("monokai");
  });

  it("sets shortcuts", () => {
    const store = useSettingsStore();
    store.setShortcuts({ copy: "Ctrl+C" });
    expect(store.shortcuts).toEqual({ copy: "Ctrl+C" });
  });

  it("sets csvEncoding", () => {
    const store = useSettingsStore();
    store.setCSVEncoding("UTF-8");
    expect(store.csvEncoding).toBe("UTF-8");
  });

  it("sets csvDelimiter", () => {
    const store = useSettingsStore();
    store.setCSVDelimiter(",");
    expect(store.csvDelimiter).toBe(",");
  });

  it("sets binaryPath", () => {
    const store = useSettingsStore();
    store.setBinaryPath("/usr/bin");
    expect(store.binaryPath).toBe("/usr/bin");
  });

  it("sets pigzPath", () => {
    const store = useSettingsStore();
    store.setPigzPath("/usr/local/bin");
    expect(store.pigzPath).toBe("/usr/local/bin");
  });

  it("sets dateFormat", () => {
    const store = useSettingsStore();
    store.setDateFormat("YYYY-MM-DD");
    expect(store.dateFormat).toBe("YYYY-MM-DD");
  });

  it("sets restoreTabs", () => {
    const store = useSettingsStore();
    store.setRestoreTabs(true);
    expect(store.restoreTabs).toBe(true);
  });

  it("sets scrollTree", () => {
    const store = useSettingsStore();
    store.setScrollTree(false);
    expect(store.scrollTree).toBe(false);
  });

  it("shows modal", () => {
    const store = useSettingsStore();
    store.showModal();
    expect(Modal.getOrCreateInstance).toHaveBeenCalledWith("#modal_settings", {
      backdrop: "static",
      keyboard: false,
    });
  });
});
