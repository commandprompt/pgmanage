<template>
  <div
    id="modal_password"
    ref="modalPassword"
    class="modal fade"
    tabindex="-1"
    role="dialog"
    aria-hidden="true"
  >
    <div class="modal-dialog" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <button
            type="button"
            class="btn-close"
            data-bs-dismiss="modal"
            aria-label="Close"
            @click="cancel"
          >
          </button>
        </div>

        <div class="modal-body">
          <div class="mb-3">{{ message }}</div>
          <div class="form-group">
            <input
              ref="passwordInput"
              v-model="password"
              class="form-control"
              type="password"
              placeholder="Password"
              @keyup.enter="submit"
            />
          </div>
        </div>

        <div class="modal-footer">
          <button
            type="button"
            class="btn btn-primary"
            data-bs-dismiss="modal"
            @click="submit"
          >
            Ok
          </button>
          <button
            type="button"
            class="btn btn-secondary"
            data-bs-dismiss="modal"
            @click="cancel"
          >
            Cancel
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { emitter } from "../emitter";
import axios from "axios";
import { tabsStore } from "../stores/stores_initializer";
import { Modal } from "bootstrap";

export default {
  name: "PasswordModal",
  data() {
    return {
      password: null,
      message: "",
      successCallback: null,
      cancelCallback: null,
      databaseIndex: null,
      passwordKind: null,
      resetToDefault: false,
      modalInstance: null,
    };
  },
  mounted() {
    emitter.on(
      "show_password_prompt",
      ({
        databaseIndex,
        successCallback,
        cancelCallback,
        message,
        kind = "database",
      }) => {
        this.message = message;
        this.successCallback = successCallback;
        this.cancelCallback = cancelCallback;
        this.databaseIndex = databaseIndex;
        this.passwordKind = kind;
        this.showModal();
      }
    );

    this.$refs.modalPassword.addEventListener("hidden.bs.modal", () => {
      if (!this.resetToDefault) {
        this.showModal();
      } else {
        this.resetData();
      }
    });

    this.$refs.modalPassword.addEventListener("shown.bs.modal", () => {
      this.$refs.passwordInput.focus();
    });
  },
  methods: {
    showModal() {
      this.modalInstance = Modal.getOrCreateInstance(this.$refs.modalPassword, {
        backdrop: "static",
        keyboard: false,
      });
      this.modalInstance.show();
    },
    submit() {
      this.modalInstance.hide();
      this.renewPassword();
    },
    cancel() {
      if (!!this.cancelCallback) {
        this.cancelCallback();
      }
      this.resetToDefault = true;
    },
    renewPassword() {
      axios
        .post("/renew_password/", {
          database_index: this.databaseIndex,
          workspace_id: tabsStore.selectedPrimaryTab.id,
          password: this.password,
          password_kind: this.passwordKind,
        })
        .then(() => {
          this.resetToDefault = true;
          if (this.successCallback) this.successCallback();
        })
        .catch((error) => {
          this.message = error?.response?.data?.data ?? error;
        });
    },
    resetData() {
      this.message = "";
      this.successCallback = null;
      this.cancelCallback = null;
      this.databaseIndex = null;
      this.passwordKind = null;
      this.resetToDefault = false;
    },
  },
};
</script>
