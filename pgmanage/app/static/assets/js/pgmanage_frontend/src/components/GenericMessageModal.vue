<template>
  <Teleport to="body">
  <div class="modal show" id="generic_modal_message" tabindex="-1" role="dialog" aria-hidden="true">
    <div class="modal-dialog" role="document">
      <div class="modal-content">
        <div class="modal-header align-items-center">
          <button type="button" class="close" data-dismiss="modal" aria-label="Close">
            <span aria-hidden="true"><i class="fa-solid fa-xmark"></i></span>
          </button>
        </div>
        <div id="generic_modal_message_content" class="modal-body" style='white-space: pre-line; word-break: break-all;'>
          {{ message }}
          <div v-for="(checkbox, index) in checkboxes" :key="index" class="custom-control custom-switch">
            <input class="custom-control-input" type="checkbox" :id="`generic_modal_message_content_${index}`"
              v-model="checkbox.checked">
            <label class="custom-control-label" :for="`generic_modal_message_content_${index}`">
              {{ checkbox.label }}
            </label>
          </div>
        </div>
        <div class="modal-footer">
          <button id="generic_modal_message_yes" type="button" class="btn btn-primary" data-dismiss="modal"
            @click="successFunc">Yes</button>
          <button id="generic_modal_message_no" type="button" class="btn btn-danger" data-dismiss="modal"
            @click="cancelFunc">No</button>
        </div>
      </div>
    </div>
  </div>
</Teleport>
</template>


<script>
export default {
  props: {
    successFunc: {
      type: Function,
      default: () => { }
    },
    cancelFunc: {
      type: Function,
      default: () => { }
    },
    message: {
      type: String,
    },
    checkboxes: {
      type: Array,
      default: () => []
    }
  },
}
</script>