<template>
  <div class="modal fade" id="modal_settings" tabindex="-1" role="dialog" aria-hidden="true">
    <div class="modal-dialog" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 class="modal-title font-weight-bold">Settings</h2>
          <button type="button" class="close" data-dismiss="modal" aria-label="Close">
            <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
          </button>
        </div>
        <div class="modal-body">
          <ul class="nav nav-tabs" role="tablist">
            <li class="nav-item">
              <a class="nav-link active" id="settings_shortcuts-tab" data-toggle="tab" href="#settings_shortcuts"
                role="tab" aria-controls="settings_shortcuts" aria-selected="true">Shortcuts</a>
            </li>
            <li class="nav-item">
              <a class="nav-link" id="settings_options-tab" data-toggle="tab" href="#settings_options" role="tab"
                aria-controls="settings_options" aria-selected="false">Options</a>
            </li>
            <li v-if="!desktopMode" class="nav-item">
              <a class="nav-link" id="settings_password-tab" data-toggle="tab" href="#settings_password" role="tab"
                aria-controls="settings_password" aria-selected="false">Password</a>
            </li>
          </ul>

          <div class="tab-content p-3">
            <div class="tab-pane fade show active" id="settings_shortcuts" role="tabpanel"
              aria-labelledby="settings_shortcuts-tab">
              <div id="div_shortcut_background_dark" ref="shortcutBackground">
                <div style="position: absolute; top: 50%; width: 100%;">Press key combination... (ESC to cancel)</div>
              </div>

              <div v-for="(shortcut, shortcut_id, index) in shortcutObject.shortcuts" :key="index" class="form-group row">
                <label :for="shortcut_id" class="col-sm-6 col-form-label">{{ shortcutLabels[index] }}</label>
                <div class="col-sm-6">
                  <button :id="shortcut_id" class='btn btn-secondary btn-sm btn-block' @click="startSetShortcut">{{
                    buildButtonText(shortcut)
                  }}</button>
                </div>
              </div>

              <div class="text-right">
                <button class='btn btn-success' @click='saveShortcuts'>Save</button>
              </div>
            </div>

            <div class="tab-pane fade" id="settings_options" role="tabpanel" aria-labelledby="settings_options-tab">
              <div class="form-row">
                <div class="form-group col-6">
                  <label for="sel_theme" class="font-weight-bold mb-2">Theme</label>
                  <select id="sel_theme" class="form-control" @change="changeTheme"
                    v-model="theme">
                    <option value="light">Light</option>
                    <option value="dark">Dark</option>
                  </select>
                </div>
                <div class="form-group col-6">
                  <label for="sel_interface_font_size" class="font-weight-bold mb-2">Font Size</label>
                  <select id="sel_interface_font_size" class="form-control" @change="changeInterfaceFontSize"
                    v-model="fontSize">
                    <option v-for="font_size in fontSizeOptions" :key="font_size" :value="font_size">{{ font_size }}
                    </option>
                  </select>
                </div>
              </div>

              <div class="form-row">
                <div class="form-group col-6">
                  <label for="sel_csv_encoding" class="font-weight-bold mb-2">CSV Encoding</label>
                  <select id="sel_csv_encoding" class="form-control" v-model="selectedCSVEncoding">
                    <option v-for="encoding in encodingValues" :key="encoding" :value="encoding">{{ encoding }}</option>
                  </select>
                </div>

                <div class="form-group col-6">
                  <label for="txt_csv_delimiter" class="font-weight-bold mb-2">CSV Delimiter</label>
                  <input type="text" class="form-control" id="txt_csv_delimiter" placeholder="Delimiter"
                    v-model="csvDelimiter">
                </div>
              </div>

              <div class="form-row">
                <div class="form-group col-6">
                  <label for="date_format" class="font-weight-bold mb-2">Date format</label>
                  <select id="date_format" class="form-control" v-model="selectedDateFormat">
                    <option v-for="dateFormat in dateFormats" :key="dateFormat" :value="dateFormat">{{ dateFormat }}
                    </option>
                  </select>
                </div>

                <div class="form-group col-6">
                  <label class="font-weight-bold mb-3">Preview</label>
                  <p class="font-weight-bold"> {{ formattedDatePreview }}</p>
                </div>

              </div>

              <div class="form-row">
                <div class="form-group col-12">
                  <label for="binary_path" class="font-weight-bold mb-2">PostgreSQL Binary Path</label>
                  <div class="d-flex">
                    <div class="input-group">
                      <input type="text" class="form-control" v-model="binaryPath"
                        :placeholder="`${action} binary path..`">
                      <div v-if="desktopMode" class="input-group-append">
                        <label class="btn btn-outline-secondary mb-0" type="button">
                          Select
                          <input type="file" @change="onFile" nwdirectory hidden>
                        </label>
                      </div>
                    </div>
                    <a class="btn btn-outline-primary ml-2" @click="validateBinaryPath(binaryPath, ['pg_dump', 'pg_dumpall', 'pg_restore', 'psql'])" title="Validate">
                      Validate
                    </a>
                  </div>
                </div>
              </div>

              <div v-if="!isWindowsOS" class="form-row">
                <div class="form-group col-12">
                  <label for="pigz_path" class="font-weight-bold mb-2">Pigz Binary Path</label>
                  <div class="d-flex">
                    <div class="input-group">
                      <input type="text" class="form-control" v-model="pigzPath"
                        :placeholder="`${action} binary path..`">
                      <div v-if="desktopMode" class="input-group-append">
                        <label class="btn btn-outline-secondary mb-0" type="button">
                          Select
                          <input type="file" @change="onFile" nwdirectory hidden>
                        </label>
                      </div>
                    </div>
                    <a class="btn btn-outline-primary ml-2" @click="validateBinaryPath(pigzPath, ['pigz'])" title="Validate">
                      Validate
                    </a>
                  </div>
                </div>
              </div>

              <div class="text-right">
                <button class='btn btn-success' @click='saveSettingsUser'>Save</button>
              </div>
            </div>

            <div class="tab-pane fade" id="settings_password" role="tabpanel" aria-labelledby="settings_password-tab">
              <div class="form-row">
                <div class="form-group col-6">
                  <label for="txt_new_pwd" class="font-weight-bold mb-2">New Password</label>
                  <input v-model="password" id="txt_new_pwd" type="password" class="form-control" @input="checkPassword"
                    minlength="8" required>
                </div>
                <div class="form-group col-6">
                  <label for="txt_confirm_new_pwd" class="font-weight-bold mb-2">Confirm</label>
                  <input ref="passwordConfirm" v-model="passwordConfirm" id="txt_confirm_new_pwd" type="password"
                    class="form-control" @input="checkPassword" minlength="8" required>
                  <div class="invalid-tooltip">
                    Password must be matching.
                  </div>
                </div>
              </div>
              <div class="text-right">
                <button class='btn btn-success' @click='saveSettingsUser' :disabled="buttonFormDisabled">Save</button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { refreshHeights } from '../workspace'
import { getExplain } from '../tree_context_functions/tree_postgresql'
import { terminalRun } from '../terminal'
import { autocomplete_start } from '../autocomplete'
import { queryEditData } from '../tree_context_functions/edit_data'
import { default_shortcuts } from '../shortcuts'
import { changeTheme } from '../header_actions'
import ace from 'ace-builds'
import axios from 'axios'
import { showAlert, showToast } from '../notification_control'
import moment from 'moment'
import { emitter } from '../emitter'
import { settingsStore } from '../stores/settings'

const light_terminal_theme = {
      background: '#e8eff8',
      brightBlue: '#006de2',
      brightGreen: '#4b9800',
      foreground: '#454545',
      cursor: '#454545',
      cursorAccent: '#454545',
      selection: '#00000030'
    }

const dark_terminal_theme = {
      background: '#1D273B',
      selection: '#1560AD',
      foreground: '#F8FAFD',
    }

export default {
  name: 'SettingsModal',
  data() {
    return {
      currentOS: '',
      desktopMode: window.gv_desktopMode,
      shortcutObject: {
        shortcuts: {},
        button: null,
        actions: null
      },
      shortcutList: [],
      selectedCSVEncoding: window.v_csv_encoding,
      selectedDateFormat: window.date_format,
      csvDelimiter: window.v_csv_delimiter,
      binaryPath: window.binary_path,
      pigzPath: window.pigz_path,
      buttonFormDisabled: true,
      password: '',
      passwordConfirm: '',
      encodingValues: [
        "ascii", "big5", "big5hkscs", "cp037", "cp273", "cp424",
        "cp437", "cp500", "cp720", "cp737", "cp775", "cp850",
        "cp852", "cp855", "cp856", "cp857", "cp858", "cp860",
        "cp861", "cp862", "cp863", "cp864", "cp865", "cp866",
        "cp869", "cp874", "cp875", "cp932", "cp949", "cp950",
        "cp1006", "cp1026", "cp1125", "cp1140", "cp1250", "cp1251",
        "cp1252", "cp1253", "cp1254", "cp1255", "cp1256", "cp1257",
        "cp1258", "cp65001", "euc-jp", "euc-jis-2004", "euc-jisx0213",
        "euc-kr", "gb2312", "gbk", "gb18030", "hz", "iso2022-jp",
        "iso2022-jp-1", "iso2022-jp-2", "iso2022-jp-2004", "iso2022-jp-3",
        "iso2022-jp-ext", "iso2022-kr", "latin-1", "iso8859-2", "iso8859-3",
        "iso8859-4", "iso8859-5", "iso8859-6", "iso8859-7", "iso8859-8", "iso8859-9",
        "iso8859-10", "iso8859-11", "iso8859-13", "iso8859-14", "iso8859-15",
        "iso8859-16", "johab", "koi8-r", "koi8-t", "koi8-u", "kz1048", "mac-cyrillic",
        "mac-greek", "mac-iceland", "mac-latin2", "mac-roman", "mac-turkish", "ptcp154",
        "shift-jis", "shift-jis-2004", "shift-jisx0213", "utf-32", "utf-32-be",
        "utf-32-le", "utf-16", "utf-16-be", "utf-16-le", "utf-7", "utf-8",
        "utf-8-sig", "windows-1252"
      ],
      shortcutLabels: [
        "Run Query", "Cancel Query", "Indent", "New Inner Tab",
        "Remove Current Inner Tab", "Select Left Inner Tab", "Select Right Inner Tab",
        "Autocomplete", "Run Explain", "Run Explain Analyze",
      ],
      dateFormats: ['YYYY-MM-DD, HH:mm:ss', 'MM/D/YYYY, h:mm:ss A', 'MMM D YYYY, h:mm:ss A']
    }
  },
  computed: {
    fontSizeOptions() {
      return Array(11).fill(10).map((x, y) => x + y)
    },
    action() {
      return this.desktopMode ? 'Select' : 'Enter'
    },
    formattedDatePreview() {
      return moment().format(this.selectedDateFormat)
    },
    isWindowsOS() {
      return navigator.userAgent.indexOf("Win") != -1
    },
    fontSize: {
      get() {
        return settingsStore.fontSize;
      },
      set(value) {
        settingsStore.setFontSize(value);
      },
    },
    theme: {
      get() {
        return settingsStore.theme
      },
      set(value) {
        settingsStore.setTheme(value);
      },
    },
  },
  created() {
    this.getShortcuts();
  },
  mounted() {
    this.currentOS = "Unknown OS";
    if (navigator.userAgent.indexOf("Win") != -1) this.currentOS = "windows";
    if (navigator.userAgent.indexOf("Mac") != -1) this.currentOS = "macos";
    if (navigator.userAgent.indexOf("X11") != -1) this.currentOS = "linux";
    if (navigator.userAgent.indexOf("Linux") != -1) this.currentOS = "linux";

    //Shortcut actions
    this.shortcutObject.actions = {
      shortcut_run_query: function () {

        if (window.v_connTabControl.selectedTab.tag.mode == 'connection') {
          if (window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.mode == 'query')
            window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.bt_start.click();
          else if (window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.mode == 'console')
            emitter.emit(`${window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.id}_run_console`, false)
          else if (window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.mode == 'edit')
            queryEditData();
        }
        else if (window.v_connTabControl.selectedTab.tag.mode == 'outer_terminal')
          terminalRun();
      },
      shortcut_explain: function () {

        if (window.v_connTabControl.selectedTab.tag.mode == 'connection') {
          if (window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.mode == 'query')
            getExplain(0);
        }
      },
      shortcut_explain_analyze: function () {

        if (window.v_connTabControl.selectedTab.tag.mode == 'connection') {
          if (window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.mode == 'query')
            getExplain(1);
        }
      },
      shortcut_cancel_query: function () {

        if (window.v_connTabControl.selectedTab.tag.mode == 'connection') {
          if (window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.mode == 'query')
            if (window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.bt_cancel.style.display != 'none')
              window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.bt_cancel.click();
          else if(window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.mode == 'console') {
            emitter.emit(`${window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.id}_cancel_query`)
          }
        }
      },
      shortcut_indent: function () {

        if (window.v_connTabControl.selectedTab.tag.mode == 'connection') {
          if (window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.mode == 'query')
            window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.bt_indent.click();
          else if ( window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.mode == 'console') {
            emitter.emit(`${window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.id}_indent_sql`)
          }
        }

      },
      shortcut_new_inner_tab: function () {

        if (window.v_connTabControl.selectedTab.tag.mode == 'connection' || window.v_connTabControl.selectedTab.tag.mode == 'snippets') {
          window.v_connTabControl.tag.createQueryTab();
        }
        else if (window.v_connTabControl.selectedTab.tag.mode == 'snippets') {
          // check this
          let tabControl = window.v_connTabControl.selectedTab.tag.tabControl;
          tabControl.tabList[tabControl.tabList.length - 1].elementLi.click();
        }
      },
      shortcut_remove_inner_tab: function () {
        if (window.v_connTabControl.selectedTab.tag.mode == 'connection') {
          let tab = window.v_connTabControl.selectedTab.tag.tabControl.selectedTab;
          if (tab) {
            if (tab.closeFunction && tab.closeFunction != null) {
              tab.closeFunction(null, tab);
            }
            else {
              window.v_connTabControl.selectedTab.tag.tabControl.removeTab(tab);
            }
          }
        }
      },
      shortcut_left_inner_tab: function () {

        if (window.v_connTabControl.selectedTab.tag.mode == 'connection' || window.v_connTabControl.selectedTab.tag.mode == 'snippets') {
          let tabControl = window.v_connTabControl.selectedTab.tag.tabControl;
          let actualIndex = tabControl.tabList.indexOf(tabControl.selectedTab);

          if (actualIndex == 0) //avoid triggering click on '+' tab
            tabControl.tabList[tabControl.tabList.length - 2].elementA.click();
          else
            tabControl.tabList[actualIndex - 1].elementA.click();
        }

      },
      shortcut_right_inner_tab: function () {

        if (window.v_connTabControl.selectedTab.tag.mode == 'connection') {
          let tabControl = window.v_connTabControl.selectedTab.tag.tabControl;
          let actualIndex = tabControl.tabList.indexOf(tabControl.selectedTab);

          if (actualIndex == tabControl.tabList.length - 2) //avoid triggering click on '+' tab
            tabControl.tabList[0].elementA.click();
          else
            tabControl.tabList[actualIndex + 1].elementA.click();
        }

      },
      shortcut_autocomplete: function (e) {
        if (window.v_connTabControl.selectedTab.tag.mode == 'connection') {
          if (window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.mode == 'query') {
              let editor = null;
              editor = window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.editor
              autocomplete_start(editor, 0, e, true);
            }
            else if (window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.mode == 'console') {
              emitter.emit(`${window.v_connTabControl.selectedTab.tag.tabControl.selectedTab.id}_show_autocomplete_results`, e)
            }
        }
      }
    }
    // Go over default shortcuts
    for (let default_code in default_shortcuts) {
      if (default_shortcuts.hasOwnProperty(default_code)) {
        // Find corresponding user defined
        let found = false;

        for (let user_code in this.shortcutObject.shortcuts) {
          if (this.shortcutObject.shortcuts.hasOwnProperty(user_code)) {
            if ((default_code == user_code) && (this.currentOS == this.shortcutObject.shortcuts[user_code]['os'])) {
              found = true;
              break
            }
          }
        }
        if (!found) {
          this.shortcutObject.shortcuts[default_code] = default_shortcuts[default_code][this.currentOS]
          this.shortcutObject.shortcuts[default_code]['shortcut_code'] = default_code
        }
      }
    }

    document.body.addEventListener('keydown', this.keyBoardShortcuts);

    this.$nextTick(() => {
      $('#modal_settings').on('hidden.bs.modal', (e) => {
        this.password = '';
        this.passwordConfirm = '';
        this.buttonFormDisabled = true;
        this.$refs.passwordConfirm.classList.remove('is-invalid', 'is-valid')
        // workaround for removing validation indicator when the empty form is closed
        setTimeout(function () {
          $('#txt_new_pwd').keydown()
        }, 100);

      });
    })

    this.applyThemes()
  },
  methods: {
    getShortcuts() {
      axios.get('/shortcuts')
        .then((resp) => {
          this.shortcutObject.shortcuts = Object.assign({}, this.shortcutObject.shortcuts, resp.data.data)
        })
        .catch((error) => {
          console.log(error)
        })
    },
    saveShortcuts() {

      for (let property in this.shortcutObject.shortcuts) {
        if (this.shortcutObject.shortcuts.hasOwnProperty(property)) {
          this.shortcutList.push(this.shortcutObject.shortcuts[property]);
        }
      }

      axios.post('/shortcuts/', {
        current_os: this.currentOS,
        shortcuts: this.shortcutList
      })
        .then((resp) => {
          showToast("success", "Shortcuts saved.");
        })
        .catch((error) => {
          console.log(error);
        })

    },
    startSetShortcut(event) {
      this.$refs.shortcutBackground.style.display = 'block'
      event.target.style['z-index'] = 1002;
      this.shortcutObject.button = event.target;

      document.body.removeEventListener('keydown', this.keyBoardShortcuts);

      document.body.removeEventListener('keydown', this.setShortcutEvent);
      document.body.addEventListener('keydown', this.setShortcutEvent);

    },
    setShortcutEvent(event) {
      event.preventDefault();
      event.stopPropagation();

      //16 - Shift
      //17 - Ctrl
      //18 - Alt
      //91 - Meta (Windows and Mac)

      if (event.keyCode == 27) {
        this.finishSetShortcut();
        return;
      }

      if (event.keyCode == 16 || event.keyCode == 17 || event.keyCode == 18 || event.keyCode == 91)
        return;

      let shortcutElement = this.shortcutObject.shortcuts[this.shortcutObject.button.id];

      if (shortcutElement) {
        shortcutElement.ctrl_pressed = event.ctrlKey;
        shortcutElement.shift_pressed = event.shiftKey;
        shortcutElement.alt_pressed = event.altKey;
        shortcutElement.meta_pressed = event.metaKey;

        if (event.code.toUpperCase() != 'SPACE')
          shortcutElement.shortcut_key = event.key.toUpperCase();
        else
          shortcutElement.shortcut_key = 'SPACE';
        this.buildButtonText(shortcutElement, this.shortcutObject.button);
      }

      this.finishSetShortcut();
    },
    finishSetShortcut() {
      this.shortcutObject.button.style['z-index'] = 0;
      this.shortcutObject.button = null;
      this.$refs.shortcutBackground.style.display = 'none';

      document.body.removeEventListener('keydown', this.setShortcutEvent);
      document.body.addEventListener('keydown', this.keyBoardShortcuts);
    },
    changeInterfaceFontSize() {
      document.getElementsByTagName('html')[0].style['font-size'] = `${this.fontSize}px`;
      $('.ace_editor').each(function (index) {
        let editor = ace.edit(this);
        editor.setFontSize(`${this.fontSize}px`);
      });
      let outer_tab_list = window.v_connTabControl.tabList;
      for (let i = 0; i < outer_tab_list.length; i++) {
        let outer_tab_tag = outer_tab_list[i].tag;
        if (outer_tab_tag) {
          let outer_tab_tag_inner_tab_control = outer_tab_tag.tabControl;
          if (outer_tab_tag_inner_tab_control) {
            let outer_tab_tag_inner_tab_list = outer_tab_tag_inner_tab_control.tabList;
            for (let j = 0; j < outer_tab_tag_inner_tab_list.length; j++) {
              let inner_tab_tag = outer_tab_tag_inner_tab_list[j].tag;
              if (inner_tab_tag) {
                if (inner_tab_tag.editor_console) {
                  inner_tab_tag.editor_console.options.fontSize = Number(this.fontSize);
                }
              }
            }
          }
        }
      }

      refreshHeights();
    },
    changeTheme() {
      this.applyThemes();
      changeTheme();
    },
    applyThemes() {
      if (this.theme === 'dark') {
        settingsStore.setEditorTheme('omnidb_dark')
        settingsStore.setTerminalTheme(dark_terminal_theme)

        document.body.classList.remove('pgmanage-theme--light', 'omnidb--theme-light');
		    document.body.classList.add('pgmanage-theme--dark', 'omnidb--theme-dark');

      } else {
        settingsStore.setEditorTheme('omnidb')
        settingsStore.setTerminalTheme(light_terminal_theme)

        document.body.classList.remove('pgmanage-theme--dark', 'omnidb--theme-dark',);
		    document.body.classList.add('pgmanage-theme--light', 'omnidb--theme-light');
      }
    },
    buildButtonText(shortcut_object, button = null) {
      let text = '';
      if (shortcut_object.ctrl_pressed)
        text += 'Ctrl+';
      if (shortcut_object.shift_pressed)
        text += 'Shift+';
      if (shortcut_object.alt_pressed)
        text += 'Alt+';
      if (shortcut_object.meta_pressed)
        text += 'Meta+';
      if (!!button)
        button.innerHTML = text + shortcut_object.shortcut_key;
      else
        return text + shortcut_object.shortcut_key
    },
    saveSettingsUser() {
      if ((this.passwordConfirm != '' || this.password != '') && (this.password != this.passwordConfirm))
        showToast("error", "New Password and Confirm New Password fields do not match.")
      else if ((this.password === this.passwordConfirm) && (this.password.length < 8 && this.password.length >= 1))
        showToast("error", "New Password and Confirm New Password fields must be longer than 8.")
      else {
        axios.post('/save_config_user/', {
          "font_size": this.fontSize,
          "theme": this.theme,
          "password": this.password,
          "csv_encoding": this.selectedCSVEncoding,
          "csv_delimiter": this.csvDelimiter,
          "binary_path": this.binaryPath,
          "date_format": this.selectedDateFormat,
          "pigz_path": this.pigzPath
        })
          .then((resp) => {
            $('#modal_settings').modal('hide');
            moment.defaultFormat = this.selectedDateFormat
            showToast("success", "Configuration saved.");
          })
          .catch((error) => {
            console.log(error)
          })
      }
    },
    checkPassword() {
      let password1 = document.getElementById('txt_new_pwd');
      let password2 = document.getElementById('txt_confirm_new_pwd');
      if (password1.checkValidity() && password2.value === password1.value) {
        password2.classList.remove("is-invalid");
        password2.classList.add('is-valid');
        this.buttonFormDisabled = false
      } else if (password2.value.length >= password1.value.length && password2.value !== password1.value) {
        password2.classList.add("is-invalid");
        password2.classList.remove('is-valid');
        this.buttonFormDisabled = true;
      }
      else {
        password2.classList.remove('is-invalid', 'is-valid');
        this.buttonFormDisabled = true;
      }
    },
    keyBoardShortcuts(event) {

      //16 - Shift
      //17 - Ctrl
      //18 - Alt
      //91 - Meta (Windows and Mac)
      //27 - Esc

      if (event.keyCode == 16 || event.keyCode == 17 || event.keyCode == 18 || event.keyCode == 91 || event.keyCode == 27)
        return;

      for (let property in this.shortcutObject.shortcuts) {
        if (this.shortcutObject.shortcuts.hasOwnProperty(property)) {
          let element = this.shortcutObject.shortcuts[property];
          if (this.checkShortcutPressed(event, element)) {
            event.preventDefault();
            event.stopPropagation();
            let action = this.shortcutObject.actions[property];
            if (action)
              action(event);
          }
        }
      }
    },
    checkShortcutPressed(event, shortcut_element) {
      if ((event.ctrlKey && shortcut_element.ctrl_pressed == 0) || (!event.ctrlKey && shortcut_element.ctrl_pressed == 1))
        return false;
      if ((event.shiftKey && shortcut_element.shift_pressed == 0) || (!event.shiftKey && shortcut_element.shift_pressed == 1))
        return false;
      if ((event.altKey && shortcut_element.alt_pressed == 0) || (!event.altKey && shortcut_element.alt_pressed == 1))
        return false;
      if ((event.metaKey && shortcut_element.meta_pressed == 0) || (!event.metaKey && shortcut_element.meta_pressed == 1))
        return false;
      if (event.key.toUpperCase() == shortcut_element.shortcut_key || event.code.toUpperCase() == shortcut_element.shortcut_key)
        return true;

      return false;
    },
    validateBinaryPath(binary_path,utilies) {
      axios.post('/validate_binary_path/', {
        binary_path: binary_path,
        utilities: utilies
      })
        .then((resp) => {
          const binary_paths = Object.entries(resp.data.data)
            .map(([key, value]) => `<p>${key}: ${value}</p>`).join('')
          showAlert(binary_paths)
        })
        .catch((error) => {
          showToast("error", error.response.data.data)
        })
    },
    onFile(e) {
      const [file] = e.target.files
      this.binaryPath = file?.path
    },
  }
}
</script>
