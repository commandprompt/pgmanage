<template>
    <div class="modal fade" id="roleModal" tabindex="-1" role="dialog" aria-hidden="true">
      <div class="modal-dialog modal-lg" role="document">
        <div class="modal-content">
          <div class="modal-header align-items-center">
            <h2 class="modal-title fw-bold">{{ modalTitle }}</h2>
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
          </div>

          <div class="modal-body">
            <ul class="nav nav-tabs" role="tablist">
              <li class="nav-item">
                <a class="nav-link active" id="role_general-tab" data-bs-toggle="tab" href="#role_general"
                  role="tab" aria-controls="role_general" aria-selected="true">General</a>
              </li>
              <li class="nav-item">
                <a class="nav-link" id="role_memberships-tab" data-bs-toggle="tab" href="#role_memberships" role="tab"
                  aria-controls="role_memberships" aria-selected="false">Memberships</a>
              </li>
            </ul>
            <div class="tab-content p-3  flex-grow-1">
              <!-- General tab -->
              <div class="tab-pane fade show active" id="role_general" role="tabpanel"
                  aria-labelledby="role_general-tab">
                <div class="row">
                  <!-- left col -->
                  <div class="form-group col-6 mb-2">
                    <label for="role_name" class="fw-bold mb-2">Name</label>
                    <input
                      v-model="localRole.name" id="role_name" type="text"
                      :class="['form-control', { 'is-invalid': v$.localRole.name.$invalid }]">

                    <label for="role_password" class="fw-bold mb-2">Password</label>
                    <input
                      v-model="localRole.password" id="role_password" type="password"
                      :class="['form-control', { 'is-invalid': v$.localRole.password.$invalid }]">

                    <label for="role_valid_until" class="fw-bold mb-2">Valid Until</label>
                    <input
                      v-model="localRole.validUntil" id="role_valid_until" ref="datepicker"
                      :class="['form-control', { 'is-invalid': v$.localRole.validUntil.$invalid }]">

                    <div ref="daterangePicker" class="position-relative"></div>

                    <label for="role_connlimit" class="fw-bold mb-2">Connection Limit</label>
                    <input
                      v-model="localRole.connectionLimit" id="role_connlimit" type="text"
                      :class="['form-control', { 'is-invalid': v$.localRole.connectionLimit.$invalid }]">
                  </div>

                  <!-- right col -->
                  <div class="form-group col-6 mb-2">
                    <label for="in_database" class="fw-bold mb-2">Permissions</label>
                    <div class="form-check form-switch">
                      <input class="form-check-input" type="checkbox" id="roleCanLogin"
                        v-model="localRole.canLogin" >
                      <label class="form-check-label" for="roleCanLogin">
                        Can Login
                      </label>
                    </div>

                    <div class="form-check form-switch">
                      <input class="form-check-input" type="checkbox" id="roleSuperuser"
                        v-model="localRole.superuser" >
                      <label class="form-check-label" for="roleSuperuser">
                        Superuser
                      </label>
                    </div>

                    <div class="form-check form-switch">
                      <input class="form-check-input" type="checkbox" id="roleCanCreateUsers"
                        v-model="localRole.canCreateUsers" >
                      <label class="form-check-label" for="roleCanCreateUsers">
                        Can Create Users
                      </label>
                    </div>

                    <div class="form-check form-switch">
                      <input class="form-check-input" type="checkbox" id="roleCanCreateDatabases"
                        v-model="localRole.canCreateDatabases" >
                      <label class="form-check-label" for="roleCanCreateDatabases">
                        Can Create Databases
                      </label>
                    </div>

                    <div class="form-check form-switch">
                      <input class="form-check-input" type="checkbox" id="roleInherit"
                        v-model="localRole.inherit" >
                      <label class="form-check-label" for="roleInherit">
                        Inherit Permissions From Memberships
                      </label>
                    </div>

                    <div class="form-check form-switch">
                      <input class="form-check-input" type="checkbox" id="roleCanReplicate"
                        v-model="localRole.canReplicate" >
                      <label class="form-check-label" for="roleCanReplicate">
                        Can Initiate Replications and Back-up
                      </label>
                    </div>

                    <div class="form-check form-switch">
                      <input class="form-check-input" type="checkbox" id="roleCanBypassRLS"
                        v-model="localRole.canBypassRLS" >
                      <label class="form-check-label" for="roleCanBypassRLS">
                        Can Bypass Row-level Security Policies
                      </label>
                    </div>

                  </div>
                </div>

                <div class="form-group mb-2">
                  <p class="fw-bold mb-2">Preview</p>
                  <div id="role_sql_command" style="height: 20vh"></div>
                </div>
              </div>

              <!-- Memberships tab -->
              <div class="tab-pane fade show" id="role_memberships" role="tabpanel"
                  aria-labelledby="role_memberships-tab" style="height: 50vh">
                  <div>
                    <div class='pb-3'>
                      <h3 class="mb-0">Members:</h3>

                        <div class="d-flex row fw-bold text-muted schema-editor__header">
                          <div class="col-8">
                            <p class="h6">Role Name</p>
                          </div>
                          <div class="col-4">
                            <p class="h6">Admin</p>
                          </div>
                        </div>

                        <div :class="['schema-editor__column d-flex row flex-nowrap form-group g-0']">
                          <div class="col-8">
                            smth
                          </div>
                          <div class="col-4 d-flex align-items-center">
                            <input type='checkbox' class="custom-checkbox"/>
                          </div>
                        </div>
                    </div>

                  </div>
              </div>
            </div>
          </div>

          <div class="modal-footer mt-auto justify-content-between">
            <button type="button" class="btn btn-primary m-0 ms-auto"
              :disabled="v$.$invalid"
              @click="saveRole">
              Save
            </button>
          </div>
        </div>
      </div>
    </div>

  </template>

  <script>

  import ConfirmableButton from './ConfirmableButton.vue'
  import { required, between, maxLength, helpers } from '@vuelidate/validators'
  import { useVuelidate } from '@vuelidate/core'
  import { isEmpty, capitalize } from 'lodash';
  import { emitter } from '../emitter'
  import axios from 'axios'
  import { showToast } from '../notification_control'
  import moment from 'moment'
  import { settingsStore } from '../stores/stores_initializer'
  import { Modal } from 'bootstrap'

  export default {
    name: 'RoleModal',
    components: {
        ConfirmableButton
    },
    props: {
      mode: String,
      treeNode: Object,
      connId: String,
      databaseIndex: Number,
    },
    data() {
      return {
        error: '',
        manualInput: false,
        jobName: '',
        localRole: {},
        initialRole: {
          name: 'NewRole',
          password: '',
          validUntil: null,
          connectionLimit: -1,
          canLogin: true,
          superuser: false,
          canCreateUsers: false,
          canCreateDatabases: false,
          inherit: false,
          canReplicate: false,
          canBypassRLS: false,
        },
        generatedSQL: null,
      }
    },

    validations() {
      const validDate = function(value) {
        return !helpers.req(value) || moment(value).isValid() || value.toLowerCase() === 'infinity'
      }
      let baseRules = {
        localRole: {
          name: {
            required: required,
            maxLength: maxLength(63),
          },
          validUntil: {
            validDate: helpers.withMessage('Must be a parsable date/time, infinity, or empty string', validDate)
          },
          password: {
            maxLength: maxLength(1024),
          },

          connectionLimit: {
            between: between(-1,65535)
          }
        }
      }
      return baseRules;
    },

    setup() {
      return { v$: useVuelidate({ $lazy: false }) }
    },

    computed: {
      modalTitle() {
        if (this.mode === 'Edit') return 'Edit Role'
        return 'Create Role'
      },
    },

    watch: {
      generatedSQL() {
        this.editor.setValue(this.generatedSQL)
        this.editor.clearSelection();
      },
      // watch initialRole for changes for cases when it is changed by requesting role from the api
      initialRole: {
        handler(newVal, oldVal) {
          this.localRole = JSON.parse(JSON.stringify(newVal))
        },
        deep: true
      },
      // watch our local working copy for changes, generate new SQL when the change occcurs
      localRole: {
        handler(newVal, oldVal) {
          this.v$.$validate()
          if(!this.v$.$invalid) {
            this.generateSQL()
          } else {
            let errs = this.v$.$errors.map((e) => `-- ${capitalize(e.$property)}: ${e.$message}`).join('\n')
            this.generatedSQL = `-- Invalid role definition --\n${errs}`
          }
        },
        deep: true
      }
    },

    mounted() {
      if (this.mode === 'Edit') {
          this.getRoleDetails()
          // let tabEl = document.getElementById('role_memberships-tab')
          // tabEl.addEventListener('shown.bs.tab', this.setupRoleMembershipsTab)
      } else {
        this.localRole = {...this.initialRole}
      }
      this.setupEditor()
      this.setupDatePicker()
      Modal.getOrCreateInstance('#roleModal').show()
    },

    methods: {
      getRoleDetails() {
        axios.post('/get_role_details/', {
          database_index: this.databaseIndex,
          tab_id: this.connId,
          oid: this.treeNode.data.oid
        })
          .then((resp) => {
            this.initialRole.name = resp.data.name
            this.initialRole.connectionLimit = resp.data.rolconnlimit
            this.initialRole.validUntil = resp.data.rolvaliduntil ? moment(resp.data.rolvaliduntil).format('YYYY-MM-DD HH:mm:ssZ') : null

            this.initialRole.canLogin = resp.data.rolcanlogin
            this.initialRole.canCreateDatabases = resp.data.rolcreatedb
            this.initialRole.canCreateUsers = resp.data.rolcreaterole
            this.initialRole.inherit = resp.data.rolinherit
            this.initialRole.superuser = resp.data.rolsuper
            this.initialRole.canReplicate = resp.data.rolreplication
            this.initialRole.canBypassRLS = resp.data.rolbypassrls
          })
          .catch((error) => {
              console.log(error)
          })
      },
      generateSQL() {
        let ret = ''
        const permVals = {
            'canLogin': ['NOLOGIN', 'LOGIN'],
            'superuser': ['NOSUPERUSER', 'SUPERUSER'],
            'canCreateUsers': ['NOCREATEROLE', 'CREATEROLE'],
            'canCreateDatabases': ['NOCREATEDB', 'CREATEDB'],
            'inherit': ['NOINHERIT', 'INHERIT'],
            'canReplicate': ['NOREPLICATION', 'REPLICATION'],
            'canBypassRLS': ['NOBYPASSRLS', 'BYPASSRLS']
        }
        let formatPermission = (permName) => {
          if(!Object.keys(permVals).includes(permName))
            return ''

          return permVals[permName][Number(this.localRole[permName])]
        }

        if (this.mode === 'Create') {
          let permissions = Object.keys(permVals).map(
              (k) => {  return formatPermission(k) }
            )
            .filter(item => typeof item ==='string')
            .join(' ')

          let parts = [
            `CREATE ROLE "${this.localRole.name}"`,
            `${permissions}`
          ]

          if(this.localRole.password)
            parts.push(`PASSWORD '${this.localRole.password}'`)

          if(!isEmpty(this.localRole.validUntil))
            parts.push(`VALID UNTIL \'${moment(this.localRole.validUntil).toISOString() || 'infinity'}\'`)

          if(this.localRole.connectionLimit)
            parts.push(`CONNECTION LIMIT ${this.localRole.connectionLimit}`)

          ret = parts.join('\n')
        } else if (this.mode === 'Edit') {
          ret = '-- No changes --'
          let parts = []
          let permissions = Object.keys(permVals)
          .filter(key => this.initialRole[key] != this.localRole[key])
          .map((k) => { return formatPermission(k) })
          .filter(item => typeof item ==='string')
          .join('\n')

          if(permissions)
            parts.push(permissions)

            if(this.initialRole.password != this.localRole.password)
            parts.push(`PASSWORD '${this.localRole.password}'`)

          if(this.initialRole.validUntil != this.localRole.validUntil)
            if(this.localRole.validUntil)
              parts.push(`VALID UNTIL \'${moment(this.localRole.validUntil).toISOString()}\'`)
            else
              parts.push(`VALID UNTIL 'infinity'`)

          if(this.initialRole.connectionLimit != this.localRole.connectionLimit)
            parts.push(`CONNECTION LIMIT ${this.localRole.connectionLimit}`)

          if(parts.length > 0)
            parts.unshift(`ALTER ROLE "${this.localRole.name}"`)

          if(this.initialRole.name != this.localRole.name)
            parts.unshift(`ALTER ROLE "${this.initialRole.name}" RENAME TO "${this.localRole.name}";`)

          if(parts.length > 0)
            ret = parts.join('\n')
        }
        this.generatedSQL = ret
      },

      setupDatePicker(){
        $(this.$refs.datepicker).daterangepicker({
          autoUpdateInput: false,
          singleDatePicker: true,
          showDropdowns: true,
          previewUTC: true,
          timePicker: true,
          timePicker24Hour: true,
          locale: {
            format: moment.defaultFormat,
          },
          parentEl: this.$refs.daterangePicker,
        }, (start, end, label) => {
          this.localRole.validUntil = moment(start).format('YYYY-MM-DD HH:mm:ssZ')
        });
      },
      // setupRoleMembershipsTab() {

      // },
      saveRole() {
        this.v$.$validate()
        if(!this.v$.$invalid) {
          // .then((resp) => {
          //   emitter.emit(`refreshNode_${this.connId}`, {"node": this.treeNode})
          //   Modal.getOrCreateInstance('#pgCronModal').hide()
          // })
          // .catch((error) => {
          //   showToast("error", error.response.data.data)
          // })
        }
      },
      setupEditor() {
        this.editor = ace.edit('role_sql_command');
        this.editor.setTheme("ace/theme/" + settingsStore.editorTheme);
        this.editor.session.setMode("ace/mode/sql");
        this.editor.setFontSize(Number(settingsStore.fontSize));
        this.editor.setReadOnly(true);
        this.editor.$blockScrolling = Infinity;

        // this.editor.setValue(this.generatedSQL)
        // this.editor.clearSelection();
      },
    },
  }
  </script>


  <style scoped>
  /* .modal-content {
    min-height: calc(100vh - 200px);
  }

  .modal-body {
    display: flex;
    flex-direction: column;
  }

  .modal-footer {
    z-index: unset;
  } */
</style>