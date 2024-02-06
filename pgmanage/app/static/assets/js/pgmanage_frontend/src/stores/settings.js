import { defineStore } from "pinia";

const useSettingsStore = defineStore("settings", {
  state: () => ({
    fontSize: window.v_font_size,
    theme: window.v_theme,
    editorTheme: window.v_editor_theme,
    terminalTheme: "",
    desktopMode: window.gv_desktopMode,
  }),
  actions: {
    setFontSize(fontSize) {
      this.fontSize = fontSize;
    },
    setTheme(theme) {
      this.theme = theme;
    },
    setEditorTheme(theme) {
      this.editorTheme = theme;
    },
    setTerminalTheme(theme) {
      this.terminalTheme = theme;
    },
  },
});

export { useSettingsStore };
