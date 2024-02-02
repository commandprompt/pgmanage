import 'vite/modulepreload-polyfill';
import 'bootstrap/scss/bootstrap.scss'
import $ from 'jquery';
import 'daterangepicker'
import 'bootstrap';
import ace from 'ace-builds'
import 'ace-builds/src-noconflict/mode-python';
import 'ace-builds/src-noconflict/mode-sql'
import 'ace-builds/src-noconflict/ext-language_tools'
import './ace_themes/theme-omnidb.js';
import './ace_themes/theme-omnidb_dark.js';
import './workspace'
import './components/postgresql_modals'
import 'vue-toast-notification/dist/theme-sugar.css';
import 'xterm/css/xterm.css'
import '@imengyu/vue3-context-menu/lib/vue3-context-menu.css'
import './assets/scss/omnidb.scss'
import './assets/scss/pgmanage.scss'
import omniURL from './ace_themes/theme-omnidb.js?url'
import omniDarkURL from './ace_themes/theme-omnidb_dark.js?url'
import axios from 'axios'
import { getCookie } from './ajax_control.js';
import { showAlert } from './notification_control.js';
import moment from 'moment';
import "tabulator-tables/dist/css/tabulator.min.css"

window.jQuery = window.$ = $;
ace.config.setModuleUrl('ace/theme/omnidb', omniURL)
ace.config.setModuleUrl('ace/theme/omnidb_dark', omniDarkURL)

axios.defaults.headers.common['X-CSRFToken'] = getCookie(v_csrf_cookie_name);
axios.interceptors.response.use(response => {
  return response;
}, error => {
  if (error.response && error.response.status === 401) {
    showAlert('User not authenticated, please reload the page.');
  } else if (error.code === 'ERR_NETWORK') {
    showAlert(`${error.message}. Try reloading the application if the issue persists.`)
  }
  return Promise.reject(error);
});

moment.defaultFormat = date_format;
