<template>
<div v-if="visible" class="col-9 d-flex ml-auto">
  <div class="modal-connections__forms position-absolute w-100">
    <div  class="modal-connections__forms_connection connection-form position-absolute">
        <div class="connection-form__header d-flex justify-content-between align-items-center pb-3">
        <!-- TODO: integrate with active connection list -->
        <h3 class="connection-form__header_title mb-0">{{initialConnection.alias}} {{connectionLocal.locked ? "(Active/Read Only)": ""}}</h3>
          <div>
            <button @click="testConnection(this.connectionLocal)" class="btn btn-outline-primary mr-2" id="connectionTestButton">Test</button>
            <button v-if="this.connectionLocal.id" @click="dispatchConnectionSelected(this.connectionLocal)" class="btn btn-success">Connect</button>
          </div>
        </div>

        <form>
          <div class="form-row mt-3">
              <div class="form-group col-6">
                <label for="connectionName" class="font-weight-bold mb-2">Name</label>
                <input v-model="connectionLocal.alias" type="text"
                  :class="['form-control', { 'is-invalid': v$.connectionLocal.alias.$invalid }]" id="connectionName" placeholder="Connection name">
                <div class="invalid-feedback">
                  <span v-for="error of v$.connectionLocal.alias.$errors" :key="error.$uid">
                    {{ error.$message }}
                  </span>
                </div>
              </div>
              <div class="form-group col-3">
              <label for="connectionType" class="font-weight-bold mb-2">Type</label>
              <select v-model="connectionLocal.technology" @change="handleTypeChange" id="connectionType" class="form-control" placeholder="Connection type">
                  <option disabled>Choose...</option>
                  <option v-for="(technology, index) in technologies"
                    :key=index
                    :value="technology">
                      {{technology}}
                  </option>
              </select>
              </div>
              <div class="form-group col-3">
              <label for="connectionGroup" class="font-weight-bold mb-2">Group</label>
              <select v-model="connectionLocal.group" id="connectionGroup" class="form-control" placeholder="Connection group">
                  <option value=""></option>
                  <option v-for="(group, index) in connectionGroups"
                    :key=index
                    :value="group.id">
                      {{group.name}}
                  </option>
              </select>
              </div>
          </div>

          <div class="form-row">
              <div class="form-group col-6">
              <label for="connectionName" class="font-weight-bold mb-2">Server</label>
              <input v-model="connectionLocal.server" type="text" class="form-control" id="connectionServer"
                :class="['form-control', { 'is-invalid': v$.connectionLocal.server.$invalid }]"
                :placeholder="placeholder.server"
                :disabled="dbFormDisabled">
                <div class="invalid-feedback">
                  <span v-for="error of v$.connectionLocal.server.$errors" :key="error.$uid">
                    {{ error.$message }}
                  </span>
                </div>
              </div>

              <div class="form-group col-3">
              <label for="connectionPort" class="font-weight-bold mb-2">Port</label>
              <input v-model="connectionLocal.port" type="text" class="form-control" id="connectionPort"
                :class="['form-control', { 'is-invalid': v$.connectionLocal.port.$invalid }]"
                :placeholder="placeholder.port"
                :disabled="dbFormDisabled">
                <div class="invalid-feedback">
                  <span v-for="error of v$.connectionLocal.port.$errors" :key="error.$uid">
                    {{ error.$message }}
                  </span>
                </div>
              </div>

              <div class="form-group col-3">
              <label for="connectionSSL" class="font-weight-bold mb-2">SSL</label>
                <select v-if="connectionLocal.technology === 'postgresql'" id="connectionSSL" class="form-control" v-model="connectionLocal.connection_params.sslmode" :disabled="dbFormDisabled">
                    <option v-for="mode in sslModes" :key="mode" :value="mode">{{ mode }}</option>
                </select>
                <select v-else-if="connectionLocal.technology === 'oracle'" id="connectionSSL" class="form-control" v-model="connectionLocal.connection_params.protocol" :disabled="dbFormDisabled">
                    <option v-for="mode in sslModes" :key="mode" :value="mode">{{ mode }}</option>
                </select>

                <select v-else id="connectionSSL" class="form-control" :value='tempMode' @change="changeSelect" :disabled="dbFormDisabled">
                    <option v-for="mode in sslModes" :key="mode.text" :value="mode.value">{{ mode.text }}</option>
                </select>
              </div>
          </div>

          <div class="form-row">
              <div class="form-group col-6">
              <label for="connectionDatabase" class="font-weight-bold mb-2">Database</label>
              <input v-model="connectionLocal.service" type="text" class="form-control" id="connectionDatabase"
                :class="['form-control', { 'is-invalid': v$.connectionLocal.service.$invalid }]"
                :placeholder="placeholder.service"
                :disabled="connectionLocal.technology==='terminal' || !!connectionLocal.conn_string.length">
                <div class="invalid-feedback">
                  <span v-for="error of v$.connectionLocal.service.$errors" :key="error.$uid">
                    {{ error.$message }}
                  </span>
                </div>
              </div>

              <div class="form-group col-3">
              <label for="connectionUsername" class="font-weight-bold mb-2">Username</label>
              <input v-model="connectionLocal.user" type="text" class="form-control" id="connectionUsername"
                :class="['form-control', { 'is-invalid': v$.connectionLocal.user.$invalid }]"
                :placeholder="placeholder.user"
                :disabled="dbFormDisabled">
                <div class="invalid-feedback">
                  <span v-for="error of v$.connectionLocal.user.$errors" :key="error.$uid">
                    {{ error.$message }}
                  </span>
                </div>
              </div>

              <div class="form-group col-3">
              <label for="connectionPassword" class="font-weight-bold mb-2">Password</label>
              <input v-model="connectionLocal.password" type="password" class="form-control" id="connectionPassword"
                :disabled="dbFormDisabled">
              </div>
          </div>

          <div class="connection-form__divider d-flex align-items-center my-3">
              <span class="connection-form__divider_text">OR</span>
          </div>

          <div class="form-group">
              <label for="connectionSring" class="font-weight-bold mb-2">Use a connection string</label>
              <input v-model="connectionLocal.conn_string" @input="clearPort" type="text" class="form-control" id="connectionSring"
                :class="['form-control', { 'is-invalid': v$.connectionLocal.conn_string.$invalid }]"
                :placeholder="placeholder.conn_string"
                :disabled="connStringDisabled">
                <div class="invalid-feedback">
                  <span v-for="error of v$.connectionLocal.user.$errors" :key="error.$uid">
                    {{ error.$message }}
                  </span>
                </div>
          </div>

          <div class="custom-control custom-switch mb-3">
              <input v-model="connectionLocal.tunnel.enabled" @change="scrollToTunnel" type="checkbox" class="custom-control-input" id="sshTunel" data-toggle="collapse" data-target="#sshSettings">
              <label class="custom-control-label font-weight-bold" for="sshTunel">Use SSH tunnel</label>
          </div>

          <div id="sshSettings" :class="(connectionLocal.tunnel.enabled) ? 'collapse show':'collapse'">
              <div class="form-row">
              <div class="form-group col-6">
                  <label for="sshServer" class="font-weight-bold mb-2">SSH Server</label>
                  <input v-model="connectionLocal.tunnel.server"
                    :class="['form-control', { 'is-invalid': v$.connectionLocal.tunnel.server.$invalid }]"
                    type="text" id="sshServer" placeholder="SSH Server">
                  <div class="invalid-feedback">
                    <span v-for="error of v$.connectionLocal.tunnel.server.$errors" :key="error.$uid">
                      {{ error.$message }}
                    </span>
                  </div>
              </div>

              <div class="form-group col-3">
                  <label for="sshPort" class="font-weight-bold mb-2">SSH Port</label>
                  <input v-model="connectionLocal.tunnel.port"
                    :class="['form-control', { 'is-invalid': v$.connectionLocal.tunnel.port.$invalid }]"
                    type="text" class="form-control" id="sshPort" placeholder="SSH Port">
                  <div class="invalid-feedback">
                    <span v-for="error of v$.connectionLocal.tunnel.port.$errors" :key="error.$uid">
                      {{ error.$message }}
                    </span>
                  </div>
              </div>

              <div class="form-group col-3">
                  <label for="sshUsername" class="font-weight-bold mb-2">SSH Username</label>
                  <input v-model="connectionLocal.tunnel.user"
                    :class="['form-control', { 'is-invalid': v$.connectionLocal.tunnel.user.$invalid }]"
                    type="text" class="form-control" id="sshUsername" placeholder="SSH Username">
                  <div class="invalid-feedback">
                    <span v-for="error of v$.connectionLocal.tunnel.user.$errors" :key="error.$uid">
                      {{ error.$message }}
                    </span>
                  </div>
              </div>
              </div>

              <div class="form-row">
              <div class="form-group col-6">
                  <label for="sshPassphrase" class="font-weight-bold mb-2">SSH Passphrase</label>
                  <input v-model="connectionLocal.tunnel.password" type="password" class="form-control" id="sshPassphrase" placeholder="SSH Passphrase">
              </div>

              <div class="form-group col-6">
                  <p class="font-weight-bold mb-2">Select file</p>
                  <label class="btn btn-secondary" id="sshFileLabel">
                  {{connectionLocal.tunnel.key ? 'Key File Loaded' : 'Select Key' }} <input type="file" @change="updateConnectionKey" hidden>
                  </label>
              </div>
              </div>
          </div>
        </form>
    </div>
  </div>
  <div class="modal-footer mt-auto justify-content-between w-100">
    <ConfirmableButton v-if="connectionLocal.id" :callbackFunc="deleteConnection" class="btn btn-danger" />
    <button type="button"
      @click="trySave(this.connectionLocal)"
      :disabled="connectionLocal.locked"
      class="btn btn-primary ml-auto">Save changes</button>
  </div>
</div>
</template>


<script>
import { useVuelidate } from '@vuelidate/core'
import { required, between, maxLength, helpers } from '@vuelidate/validators'

import {connectionsStore} from '../stores/connections.js'

import ConfirmableButton from './ConfirmableButton.vue'
  export default {
    name: 'ConnectionsModalConnectionForm',
    components: {
      ConfirmableButton
    },
    data() {
      return {
        connectionLocal: {
          alias: ''
        },
        postgresql_ssl_modes: ["allow", "prefer", "require", "disable", "verify-full", "verify-ca"],
        mysql_mariadb_modes: [
          { text: 'disable', value: 'ssl_disabled'},
          { text: 'require', value: 'ssl' },
          { text: 'verify certificate', value: "ssl_verify_cert" },
          { text: 'verify server identity', value: "ssl_verify_identity" }],
        oracle_modes: ['tcp', 'tcps'],
        tempMode: "ssl"
      }
    },
    validations() {
      const needsServer = !['terminal', 'sqlite'].includes(this.connectionLocal.technology)
      const needsTunnel = this.connectionLocal.technology === 'terminal' || (this.connectionLocal.tunnel && this.connectionLocal.tunnel.enabled)
      const hostOrIp = function(value) {
        const ipre = /^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$/
        const hostre = /^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$/
        return ipre.test(value) || hostre.test(value)
      }

      let baseRules = {
        connectionLocal: {
          alias: {
            required: required,
            maxLength: maxLength(30),
          },
          technology: {
            required,
          },
          server: {},
          port: {},
          service: {},
          user: {},
          conn_string: {},
          tunnel: {
            user: {},
            server: {},
            port: {}
          }
        }
      }

      if(needsServer) {
        if(!this.connectionLocal.conn_string) {
          baseRules.connectionLocal.server = {
            required,
            hostOrIp: helpers.withMessage('Must be a valid hostname or IP', hostOrIp)
          }

          baseRules.connectionLocal.port = {
            required,
            between: between(1,65535),
          }

          baseRules.connectionLocal.service = {
            required,
          }

          baseRules.connectionLocal.user = {
            required,
          }

        } else {
          baseRules.connectionLocal.conn_string = {
            required,
          }
        }
      }

      if(this.connectionLocal.technology === 'sqlite') {
        baseRules.connectionLocal.service = {
          required,
        }
      }

      if(needsTunnel) {
        console.log('tunnel')
        baseRules.connectionLocal.tunnel.server = {
          required,
        },
        baseRules.connectionLocal.tunnel.port = {
          required,
          between: between(1,65535),
        },
        baseRules.connectionLocal.tunnel.user = {
          required,
        }
      }

      return baseRules;
    },
    setup() {
      return { v$: useVuelidate({ $lazy: true }) }
    },
    props: {
      visible: Boolean,
      initialConnection: {
        type: Object,
        required: true,
        default: {
          id: null,
          alias: 'New Connection',
          locked: false,
          public: false,
          is_mine: true,
          technology: 'postgresql',
          group: null,
          conn_string: "",
          server: "",
          port: "5432",
          service: "",
          user: "",
          password: "",
          password_set: false,
          tunnel: {
            enabled: false,
            server: "",
            port: "",
            user: "",
            password: "",
            password_set: false,
            key: "",
            key_set: false
          },
          connection_params: {
            sslmode: "prefer"
          }
        }
      },
      technologies: Array,
    },
    computed: {
      placeholder() {
        const placeholderMap = {
          'postgresql': {
            'server': 'ex: 127.0.0.1',
            'port': 'ex: 5432',
            'service': 'ex: postgres',
            'user': 'ex: postgres',
            'conn_string': 'ex: postgresql://postgres@localhost:5432/postgres'
          },
          'mysql': {
            'server': 'ex: 127.0.0.1',
            'port': 'ex: 3306',
            'service': 'ex: db',
            'user': 'ex: root',
            'conn_string': 'ex: mysql://root@localhost:3306/db'
          },
          'mariadb': {
            'server': 'ex: 127.0.0.1',
            'port': 'ex: 3306',
            'service': 'ex: db',
            'user': 'ex: root',
            'conn_string': 'ex: mysql://root@localhost:3306/db'
          },
          'oracle': {
            'server': 'ex: 127.0.0.1',
            'port': 'ex: 1521',
            'service': 'ex: xe',
            'user': 'ex: system',
            'conn_string': 'ex: oracle://system@localhost:1521/xe'
          },
          'sqlite': {
            'server': '',
            'port': '',
            'service': 'ex: /home/user/sqlite_file.db',
            'user': '',
            'conn_string': ''
          },
          'terminal': {
            'server': '',
            'port': '',
            'service': '',
            'user': '',
            'conn_string': ''
          },
        }
        let current_db = this.connectionLocal.technology || 'postgresql'
        return placeholderMap[current_db]
      },
      dbFormDisabled() {
        return ['sqlite', 'terminal'].includes(this.connectionLocal.technology) || !!this.connectionLocal.conn_string.length
      },
      connStringDisabled() {
        return (!!this.connectionLocal.server ||
        !!this.connectionLocal.user ||
        !!this.connectionLocal.password ||
        !!this.connectionLocal.service ||
        ['terminal', 'sqlite'].includes(this.connectionLocal.technology))
      },
      sslModes() {
        if (this.connectionLocal.technology === 'postgresql') {
          return this.postgresql_ssl_modes
        } else if (this.connectionLocal.technology === 'oracle') {
          return this.oracle_modes
        } else if (['mysql', 'mariadb'].includes(this.connectionLocal.technology)) {
          return this.mysql_mariadb_modes
        } else {
          return []
        }
      },
      connectionGroups() {
        return connectionsStore.groups
      }
    },
    methods: {
      changeSelect(e){
        const value = e.target.value;
        if (value === 'ssl'){
          this.connectionLocal.connection_params = {"ssl": {"ssl": true}}
        } else {
          this.connectionLocal.connection_params = {[value]: true}
        }
        this.tempMode = value;
      },
      clearPort() {
        this.connectionLocal.port = ''
      },
      dispatchConnectionSelected(connection) {
        let event = new CustomEvent('connection:selected', { detail: connection })
        document.dispatchEvent(event)
      },
      trySave(connection) {
        this.v$.connectionLocal.$validate()
        if(!this.v$.$invalid) {
          this.$emit('connection:save', this.connectionLocal)
        }
      },
      testConnection(connection) {
        this.v$.connectionLocal.$validate()
        if(!this.v$.$invalid) {
          this.$emit('connection:test', this.connectionLocal)
        }
      },
      updateConnectionKey(event) {
        let file = (event.target.files) ? event.target.files[0] : false;
        let reader = new FileReader();
        reader.onload = (e) => { this.connectionLocal.tunnel.key = e.target.result; };
        reader.readAsText(file);
      },
      handleTypeChange(event) {
        let technology = event.target.value
        if(technology === 'terminal') {
          // erase db fields
          this.connectionLocal.server = ''
          this.connectionLocal.port = ''
          this.connectionLocal.service = ''
          this.connectionLocal.user = ''
          this.connectionLocal.password = ''
          // open ssh subform
          this.connectionLocal.tunnel.enabled = true
          setTimeout(this.scrollToTunnel,10)
        } else {
          this.connectionLocal.tunnel.enabled = false
        }

        if(technology === 'sqlite') {
          this.connectionLocal.server = ''
          this.connectionLocal.port = ''
          this.connectionLocal.user = ''
          this.connectionLocal.password = ''
        }
      },
      scrollToTunnel(){
        // scroll the tunnel form into viewport if tunnel was enabled
        if(this.connectionLocal.tunnel.enabled) {
          document.getElementById('sshSettings').scrollIntoView()
        }
      },
      deleteConnection() {
        this.$emit('connection:delete', this.connectionLocal)
      }
    },
    watch: {
      initialConnection(newVal, oldVal) {
        this.connectionLocal = {...newVal}
        this.connectionLocal.tunnel = {...newVal.tunnel}
        this.v$.connectionLocal.$reset()
      },
      'connectionLocal.technology': function (newVal, oldVal) {
        if (oldVal === undefined || this.connectionLocal.id) {
          this.tempMode  = Object.keys(this.connectionLocal.connection_params)[0]
          return
        }
        if (newVal === 'postgresql') {
          this.connectionLocal.connection_params =  {sslmode: 'prefer'}
        } else if (newVal === 'oracle') {
          this.connectionLocal.connection_params = {protocol: "tcps"}
        } else if (['mysql', 'mariadb'].includes(newVal)){
          this.connectionLocal.connection_params = {'ssl': {'ssl': true}}
          this.tempMode = 'ssl'
        } else {
          this.connectionLocal.connection_params = {}
        }
        if (newVal)
          this.connectionLocal.port = this.placeholder.port.replace('ex: ','')
    },
    }
  }
</script>