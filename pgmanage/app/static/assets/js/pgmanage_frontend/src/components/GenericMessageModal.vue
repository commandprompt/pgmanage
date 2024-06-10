<template>
  <Teleport to="body">
    <div
      class="modal fade"
      id="generic_modal_message"
      tabindex="-1"
      role="dialog"
      aria-hidden="true"
    >
      <div class="modal-dialog modal-dialog-centered" role="document">
        <div class="modal-content">
          <div class="modal-header align-items-center">
            <button
              v-if="store.closable"
              type="button"
              class="btn-close"
              data-bs-dismiss="modal"
              aria-label="Close"
              @click="store.hideModal"
            ></button>
          </div>
          <div
            id="generic_modal_message_content"
            class="modal-body"
            style="white-space: pre-line; word-break: break-all"
          >
            {{ store.message }}
            <div
              v-for="(checkbox, index) in store.checkboxes"
              :key="index"
              class="form-check form-switch"
            >
              <input
                class="form-check-input"
                type="checkbox"
                :id="`generic_modal_message_content_${index}`"
                v-model="checkbox.checked"
              />
              <label
                class="form-check-label"
                :for="`generic_modal_message_content_${index}`"
              >
                {{ checkbox.label }}
              </label>
            </div>
          </div>
          <div class="modal-footer">
            <button
              id="generic_modal_message_yes"
              type="button"
              class="btn btn-primary"
              data-bs-dismiss="modal"
              @click="store.executeSuccess"
            >
              Yes
            </button>
            <button
              id="generic_modal_message_no"
              type="button"
              class="btn btn-danger"
              data-bs-dismiss="modal"
              @click="store.executeCancel"
            >
              No
            </button>
          </div>
        </div>
      </div>
    </div>
  </Teleport>
</template>

<script>
import { messageModalStore } from "../stores/stores_initializer";
import { Modal } from "bootstrap";

export default {
  data() {
    return {
      modalInstance: null,
    };
  },
  computed: {
    store() {
      return messageModalStore;
    },
  },
  mounted() {
    messageModalStore.$onAction((action) => {
      if (action.name === "showModal") {
        this.modalInstance = Modal.getOrCreateInstance(
          "#generic_modal_message",
          {
            backdrop: "static",
            keyboard: false,
          }
        );
        this.modalInstance.show();
      }
      if (action.name === "hideModal") {
        this.modalInstance.hide();
      }
    });
  },
};
</script>

<style>
#generic_modal_message {
  z-index: 9999;
}
</style>
