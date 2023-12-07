<template>
<div>
    <div class='container-fluid'>
        <h1 class='text-center display-2 text-muted'>Welcome!</h1>
        <div class='row'>
            <div class='col-4'>
                <h1>Recent Connections</h1>
                <div class="recent-conections d-flex flex-column">
                  <div v-for="(connection, idx) in recentConnections" :key="idx" @click="dispatchConnectionSelected(connection)" class="recent-conections__item">
                    <div class="recent-conections__item_wrap d-flex align-items-center m-0">
                        <div class="recent-conections__item_logo mr-3">
                            <div :class="['icon', 'icon-' + connection.technology]"></div>
                        </div>
                        <div class="recent-conections__item_text d-flex flex-column">
                            <p class="recent-conections__item_title">{{ connection.alias }}</p>
                            <span class="recent-conections__item_subtitle muted-text line-clamp-text clipped-text">{{connectionSubtitle(connection)}}</span>
                            <span class="muted-text">{{ ago(connection.last_access_date) }}</span>
                        </div>
                    </div>
                  </div>
                  <div class='muted-text' v-if="!recentConnections.length">
                      You don't have any recent connections yet...
                  </div>
                </div>
            </div>
            <div class='col-4'>
                <div><h1 class='d-inline-block mr-2'>Hotkeys</h1>
                  <button @click="showSettings" class="btn btn-sm btn-icon btn-icon-secondary">
                      <i class="fas fa-tools" title="Customize"></i>
                  </button>
                </div>
                <div class='shortcut-widget'>
                  <div v-for="(shortcut, idx) in shortcuts" :key="idx" class="mb-2">
                      <div class='shortcut-label'>{{shortcutLabel(shortcut)}}</div>
                      <span v-for="(button, idx) in shortcutKeyNames(shortcut)" :key="idx" class='shortcut-button mr-2'>
                        {{button}}
                      </span>
                  </div>
                </div>
            </div>
            <div class='col-4'>
                <h1>Explore</h1>
                <a href='#' @click="showTutorial" class='welcome-link text-muted d-block' title="Get Started">
                  <i class="fa-solid fa-circle-info"></i>
                  Get Started
                </a>
                <a href='https://pgmanage.readthedocs.io' class='welcome-link text-muted d-block mb-4' title="Handbook">
                  <i class="fa-solid fa-book-bookmark"></i>
                  PgManage Handbook
                </a>
                <h1>Get Involved</h1>
                <a href='https://github.com/commandprompt/pgmanage/discussions/' class='welcome-link text-muted d-block' title="Discuss">
                  <i class="fa-solid fa-comments"></i>
                  Discuss
                </a>
                <a href='https://github.com/commandprompt/pgmanage/issues' class='welcome-link text-muted d-block' title="Report a Bug">
                  <i class="fa-solid fa-bug"></i>
                  Report a Bug
                </a>
            </div>
        </div>
    </div>
</div>
</template>

<script>
import axios from 'axios'
import moment from 'moment'
import { emitter } from '../emitter'
import { connectionsStore } from '../stores/connections.js'
import { showConfigUser } from '../header_actions'
import { startTutorial } from '../tutorial'
import { endLoading } from "../ajax_control";
import { default_shortcuts } from '../shortcuts'

export default {
  name: "WelcomeScreen",
  props: {

  },
  setup(props) {

  },
  computed: {
    recentConnections() {
      return connectionsStore.connections
        .filter(conn => !!conn.last_access_date)
        .sort((a, b) => (moment(a.last_access_date) > moment(b.last_access_date)) ? -1 : 1)
        .slice(0, 5)
    },
    currentOS() {
      if (navigator.userAgent.indexOf("Win") != -1) return "windows"
      if (navigator.userAgent.indexOf("Mac") != -1) return  "macos"
      if (navigator.userAgent.indexOf("X11") != -1) return  "linux"
      if (navigator.userAgent.indexOf("Linux") != -1) return  "linux"
      return "Unknown OS"
    }
  },
  data() {
    return {
        shortcuts: []
    };
  },
  mounted() {
    this.loadShortcuts()
    this.setupEvents()
    endLoading()
  },
  methods: {
    ago(date) {
      return moment(date).fromNow()
    },
    connectionSubtitle(connection) {
      if(connection.conn_string) return connection.conn_string
      if(connection.technology === 'sqlite') return connection.service
      if(connection.technology === 'terminal') return `${connection.tunnel.server}:${connection.tunnel.port}`
      return `${connection.server}:${connection.port}/${connection.service}`
    },
    dispatchConnectionSelected(connection) {
      let event = new CustomEvent('connection:selected', { detail: connection })
      document.dispatchEvent(event)
    },
    loadShortcuts() {
      axios.get('/shortcuts/')
        .then((resp) => {
          this.shortcuts = resp.data.data
          let shortcut_keys = Object.keys(this.shortcuts)
          Object.keys(default_shortcuts).forEach((key) => {
            if(!shortcut_keys.includes(key)) {
              this.shortcuts[key] = Object.assign({}, default_shortcuts[key][this.currentOS])
            }
          })
          // merge missing shortcuts forom default_shortcuts
        })
        .catch((error) => {
          console.log(error)
        })
    },
    shortcutKeyNames(shortcut) {
      let keys = []
      if (shortcut.ctrl_pressed)
        keys.push('Ctrl')
      if (shortcut.shift_pressed)
        keys.push('Shift')
      if (shortcut.alt_pressed)
        keys.push('Alt')
      if (shortcut.meta_pressed)
        keys.push('Meta')
      if (shortcut.shortcut_key)
        keys.push(shortcut.shortcut_key)

      return keys
    },
    shortcutLabel(shortcut) {
      const LABEL_MAP = {
        'shortcut_run_query': 'Run Query',
        'shortcut_run_selection': 'Run Selection',
        'shortcut_cancel_query': 'Cancel Query',
        'shortcut_indent': 'Indent Code',
        'shortcut_new_inner_tab': 'New Tab',
        'shortcut_remove_inner_tab': 'Close Tab',
        'shortcut_left_inner_tab': 'Switch Tab Left',
        'shortcut_right_inner_tab': 'Switch Tab Right',
        'shortcut_autocomplete': 'Autocomplete',
        'shortcut_explain': 'Explain Query',
        'shortcut_explain_analyze': 'Analyze Query'
      };
      return LABEL_MAP[shortcut.shortcut_code] || 'unknown'
    },
    showSettings() {
      showConfigUser()
    },
    showTutorial() {
      startTutorial('getting_started')
    },
    setupEvents() {
      emitter.on('shortcuts_updated', (event) => {
        this.loadShortcuts()
      });
    },
  },
};
</script>

<style scoped>
  .welcome-link {
    font-size: 1.4em;
    line-height: 2em;
  }
  .shortcut-widget {
    font-size: 1.2rem;

  }
  .shortcut-widget .shortcut-label {
    font-weight: 400;
    display: inline-block;
    min-width: 30%;
  }
  .shortcut-widget .shortcut-button {
    display: inline-block;
    padding: 0 .35rem;
    border-radius: 4px;
    line-height: 1.6;
    text-align: center;
    white-space: nowrap;
    vertical-align: baseline;
    background: rgba(0,0,0,.05);
    -webkit-box-shadow: inset 0 0 0 1px rgba(0,0,0,.05);
    box-shadow: inset 0 0 0 1px rgba(0,0,0,.05);
    color: rgba(0,0,0,.67);
    margin: 0 .14rem;
  }
</style>