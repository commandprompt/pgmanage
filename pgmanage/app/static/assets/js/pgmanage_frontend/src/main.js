import 'vite/modulepreload-polyfill';
import 'bootstrap/scss/bootstrap.scss'
import './assets/css/font-poppins.css'
import './assets/css/font-ubuntu-mono.css'
import $ from 'jquery';
import 'daterangepicker'
import ace from 'ace-builds'
import 'ace-builds/src-noconflict/mode-python';
import 'ace-builds/src-noconflict/mode-sql'
import 'ace-builds/src-noconflict/mode-pgsql'
import 'ace-builds/src-noconflict/mode-mysql'
import 'ace-builds/src-noconflict/mode-plsql'
import 'ace-builds/src-noconflict/ext-language_tools'
import 'ace-builds/src-noconflict/ext-searchbox'
import './ace_themes/theme-omnidb.js';
import './ace_themes/theme-omnidb_dark.js';
import './workspace'
import './components/postgresql_modals'
import 'vue-toast-notification/dist/theme-sugar.css';
import '@xterm/xterm/css/xterm.css'
import '@imengyu/vue3-context-menu/lib/vue3-context-menu.css'
import './assets/scss/omnidb.scss'
import './assets/scss/pgmanage.scss'
import omniURL from './ace_themes/theme-omnidb.js?url'
import omniDarkURL from './ace_themes/theme-omnidb_dark.js?url'
import axios from 'axios'
import { getCookie } from './ajax_control.js';
import "tabulator-tables/dist/css/tabulator.min.css"
import App from './App.vue'
import { createApp } from 'vue';
import ToastPlugin from 'vue-toast-notification';
import { setupLogger } from './logging/logger_setup.js';
import { settingsStore, pinia } from './stores/stores_initializer.js';

window.jQuery = window.$ = $;
ace.config.setModuleUrl('ace/theme/omnidb', omniURL)
ace.config.setModuleUrl('ace/theme/omnidb_dark', omniDarkURL)

axios.defaults.headers.common['X-CSRFToken'] = getCookie(v_csrf_cookie_name);

settingsStore.getSettings().then(() => {
  const app = createApp(App);
  setupLogger(app);
  if (__VITE_ENTERPRISE__) {
    import("@enterprise/index").then(({ default: enterprisePlugin }) => {
      app.use(enterprisePlugin);
      app.use(ToastPlugin, {
        duration: 0,
      });
      app.use(pinia);
      app.mount("#app");
    });
  } else {
    app.use(ToastPlugin, {
      duration: 0,
    });
    app.use(pinia);
    app.mount("#app");
  }
});
