<template>
  <div
    ref="masterPasswordModal"
    class="modal fade"
    id="master_password_modal"
    tabindex="-1"
    role="dialog"
    aria-hidden="true"
  >
    <div class="modal-dialog" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <h2 class="modal-title font-weight-bold">Master Password</h2>
        </div>

        <div class="modal-body">
          <div id="modal_password_content" class="mb-3">
            <span v-if="isNewPassword">
              Please set your master password. It will be used to secure your
              connection credentials.
            </span>
            <span v-else>
              {{ passwordMessage }}
            </span>
          </div>

          <div v-if="isNewPassword" class="form-row">
            <div class="form-group col-6">
              <input
                ref="newMasterPassInput"
                v-model="v$.password.$model"
                type="password"
                class="form-control"
                :class="{ 'is-invalid': v$.password.$invalid }"
                placeholder="New Password"
              />
              <div class="invalid-feedback">
                <a v-for="error of v$.password.$errors" :key="error.$uid">
                  {{ error.$message }}
                  <br />
                </a>
              </div>
            </div>

            <div class="form-group col-6">
              <input
                v-model="v$.passwordConfirm.$model"
                type="password"
                class="form-control"
                :class="{ 'is-invalid': v$.passwordConfirm.$invalid }"
                placeholder="Confirm Password"
              />
              <div class="invalid-feedback">
                <a
                  v-for="error of v$.passwordConfirm.$errors"
                  :key="error.$uid"
                >
                  {{ error.$message }}
                  <br />
                </a>
              </div>
            </div>
          </div>

          <div v-else class="form-group">
            <input
              ref="masterPassCheckInput"
              v-model="v$.checkPassword.$model"
              class="form-control"
              :class="{ 'is-invalid': v$.checkPassword.$invalid }"
              type="password"
              placeholder="Password"
              @keydown.enter="checkMasterPassword"
            />
            <div class="invalid-feedback">
              <a v-for="error of v$.checkPassword.$errors" :key="error.$uid">
                {{ error.$message }}
                <br />
              </a>
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button
            v-if="isNewPassword"
            type="button"
            class="btn btn-success"
            data-dismiss="modal"
            @click="saveMasterPass"
            :disabled="isNewPasswordValid"
          >
            Set Master password
          </button>

          <template v-else>
            <button
              id="password_check_button"
              type="button"
              class="btn btn-success"
              data-dismiss="modal"
              @click="checkMasterPassword"
            >
              Ok
            </button>
            <button
              id="password_reset_button"
              type="button"
              class="btn btn-danger"
              data-dismiss="modal"
              @click="resetPassword"
            >
              Reset Master Password
            </button>
          </template>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { useVuelidate } from "@vuelidate/core";
import { required, minLength, sameAs, helpers } from "@vuelidate/validators";
import axios from "axios";
import { conn_app } from "../connections_modal.js";
import { showToast } from "../notification_control.js";
import { emitter } from "../emitter";
import { createMessageModal } from "../notification_control.js";

export default {
  name: "MasterPasswordModal",
  setup() {
    return {
      v$: useVuelidate({ $lazy: true }),
    };
  },
  data() {
    return {
      password: "",
      passwordConfirm: "",
      checkPassword: "",
      isNewPassword: false,
      passwordMessage:
        "Please provide your master password to unlock your connection credentials for this session.",
      masterPassConfirmed: true,
    };
  },
  computed: {
    isNewPasswordValid() {
      return !this.password || !this.passwordConfirm ? true : this.v$.$invalid;
    },
  },
  validations() {
    return {
      checkPassword: { required: required },
      password: { required: required, minLength: minLength(8) },
      passwordConfirm: {
        sameAs: helpers.withMessage(
          "Passwords must be matching.",
          sameAs(this.password)
        ),
      },
    };
  },
  mounted() {
    this.setupEvents();
  },
  methods: {
    setupEvents() {
      emitter.on("show_master_pass_prompt", (isNewPassword) => {
        this.isNewPassword = isNewPassword;
        this.showModal();
      });

      $(this.$refs.masterPasswordModal).on("shown.bs.modal", () => {
        if (this.isNewPassword) {
          this.$refs.newMasterPassInput.focus();
        } else {
          this.$refs.masterPassCheckInput.focus();
        }
      });

      $(this.$refs.masterPasswordModal).on("hidden.bs.modal", () => {
        if (!this.masterPassConfirmed) {
          this.showModal();
        }
      });
    },
    showModal() {
      $(this.$refs.masterPasswordModal).modal({
        backdrop: "static",
        keyboard: false,
      });
    },
    saveMasterPass() {
      axios
        .post("/master_password/", {
          master_password: this.password,
        })
        .then(() => {
          conn_app.mount("#connections-modal-wrap"); //TODO: Change this behaviour of mouning connections modal
          v_omnis.div.style.opacity = 1;
          showToast("success", "Master password created.");
        });
    },
    checkMasterPassword() {
      $(this.$refs.masterPasswordModal).modal("hide");
      axios
        .post("/master_password/", {
          master_password: this.checkPassword,
        })
        .then(() => {
          conn_app.mount("#connections-modal-wrap"); //TODO: Change this behaviour of mouning connections modal
          v_omnis.div.style.opacity = 1;
          this.masterPassConfirmed = true;
        })
        .catch((error) => {
          this.passwordMessage = error.response.data.data;
          this.masterPassConfirmed = false;
        });
    },
    resetPassword() {
      this.masterPassConfirmed = true;
      createMessageModal(
        `Are you sure you want to reset you master password?
                               You will lose your saved connection passwords.`,
        () => {
          axios.post("/reset_master_password/").then(() => {
            this.isNewPassword = true;
            this.showModal();
          });
        },
        () => {
          this.showModal();
        },
        false
      );
    },
  },
};
</script>
