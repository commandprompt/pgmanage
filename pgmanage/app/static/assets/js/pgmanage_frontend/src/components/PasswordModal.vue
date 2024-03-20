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
            class="close"
            data-dismiss="modal"
            aria-label="Close"
            @click="cancel"
          >
            <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
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
            data-dismiss="modal"
            @click="submit"
          >
            Ok
          </button>
          <button
            type="button"
            class="btn btn-secondary"
            data-dismiss="modal"
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

    $(this.$refs.modalPassword).on("hidden.bs.modal", () => {
      if (!this.resetToDefault) {
        this.showModal();
      } else {
        this.resetData();
      }
    });

    $(this.$refs.modalPassword).on("shown.bs.modal", () => {
      this.$refs.passwordInput.focus();
    });
  },
  methods: {
    showModal() {
      $(this.$refs.modalPassword).modal({
        backdrop: "static",
        keyboard: false,
      });
    },
    submit() {
      $(this.$refs.modalPassword).modal("hide");
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
          tab_id: tabsStore.selectedPrimaryTab.id,
          password: this.password,
          password_kind: this.passwordKind,
        })
        .then(() => {
          this.resetToDefault = true;
          if (this.successCallback) this.successCallback();
        })
        .catch((error) => {
          this.message = error?.response?.data?.data;
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
