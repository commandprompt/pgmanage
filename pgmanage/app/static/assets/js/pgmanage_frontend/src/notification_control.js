import { createApp } from "vue";
import GenericMessageModal from "./components/GenericMessageModal.vue";
import { execAjax } from "./ajax_control";
import { useToast } from 'vue-toast-notification';

let v_message_modal_animating = false;
let v_message_modal_queued = false;
let v_message_modal_queued_function = null;
let v_shown_callback = null;

$(function () {
  $('#modal_message').on('hide.bs.modal', function (e) {
    v_message_modal_animating = true;
  });
  $('#modal_message').on('show.bs.modal', function (e) {
    v_message_modal_animating = true;
  });
  $('#modal_message').on('hidden.bs.modal', function (e) {
		document.getElementById('modal_message_content').innerHTML = '';
    v_message_modal_animating = false;
    if (v_message_modal_queued == true) {
			if (v_message_modal_queued_function!=null)
				v_message_modal_queued_function();
      $('#modal_message').modal();
		}
    v_message_modal_queued = false;
		v_message_modal_queued_function = null;
  });
  $('#modal_message').on('shown.bs.modal', function (e) {
    v_message_modal_animating = false;
    if (v_shown_callback) {
      v_shown_callback();
      v_shown_callback = null;
    }
  });
});

/**
 * Creates a generic message modal using Vue.js.
 * @param {string} message - The message to display in the modal.
 * @param {Function} successFunc - The function to call when the success action is triggered.
 * @param {Function} cancelFunc - The function to call when the cancel action is triggered.
 */
function createMessageModal(message, successFunc, cancelFunc, closable) {
  const wrap_div = document.getElementById("generic-message-modal-wrap");

  wrap_div.innerHTML = `<generic-modal :message="message" :success-func="successFunc" :cancel-func="cancelFunc" :closable="closable"></generic-modal>`;

  const app = createApp({
    components: {
      "generic-modal": GenericMessageModal,
    },
    data() {
      return {
        message: message,
        successFunc: successFunc,
        cancelFunc: cancelFunc,
        closable: closable
      };
    },
    mounted() {
      setTimeout(() => {
        $("#generic_modal_message").on("hidden.bs.modal", () => {
          app.unmount();
        });
        $("#generic_modal_message").modal({
          backdrop: "static",
          keyboard: false,
        });
      }, 500);
    },
  });
  app.mount(`#generic-message-modal-wrap`);
}

function showMessageModal(p_content_function, p_large) {

	var v_dialog = document.getElementById('modal_message_dialog');

	if (p_large==null || p_large==false) {
		v_dialog.classList.remove('modal-xl');
	}
	else {
		v_dialog.classList.add('modal-xl');
	}

  if (!v_message_modal_animating) {
		if (p_content_function!=null)
			p_content_function();
    $('#modal_message').modal();
	}
  else {
    v_message_modal_queued = true;
		v_message_modal_queued_function = p_content_function;
	}

}

function showAlert(p_info, p_funcYes = null, p_large = null)
{

	var v_create_content_function = function() {
	  var v_content_div = document.getElementById('modal_message_content');
	  var v_button_yes = document.getElementById('modal_message_yes');
	  var v_button_ok = document.getElementById('modal_message_ok');
	  var v_button_no = document.getElementById('modal_message_no');
	  var v_button_cancel = document.getElementById('modal_message_cancel');

	  v_content_div.innerHTML = p_info;

		v_button_ok.onclick = function() {
	    if (p_funcYes!=null)
			  p_funcYes();
		};

	  v_button_yes.style.display = 'none';
	  v_button_ok.style.display = '';
	  v_button_no.style.display = 'none';
	  v_button_cancel.style.display = 'none';

	}


	showMessageModal(v_create_content_function, p_large);


}

function showConfirm(p_info,p_funcYes = null,p_funcNo = null, p_shownCallback = null, p_large = null)
{
	var v_create_content_function = function() {
	  if (p_shownCallback != null)
	    v_shown_callback = p_shownCallback;

	  var v_content_div = document.getElementById('modal_message_content');
	  var v_button_yes = document.getElementById('modal_message_yes');
	  var v_button_ok = document.getElementById('modal_message_ok');
	  var v_button_no = document.getElementById('modal_message_no');
	  var v_button_cancel = document.getElementById('modal_message_cancel');

	  v_content_div.innerHTML = p_info;

		v_button_ok.onclick = function() {
			p_funcYes();
		};

		v_button_cancel.onclick = function() {
			if (p_funcNo)
				p_funcNo();
		};

	  v_button_yes.style.display = 'none';
	  v_button_no.style.display = 'none';
	  v_button_ok.style.display = '';
	  v_button_cancel.style.display = '';
	}

	showMessageModal(v_create_content_function, p_large);

}

function checkSessionMessage() {

	execAjax('/check_session_message/',
				JSON.stringify({}),
        function(p_return) {
          if (p_return.v_data!='')
          	showAlert(p_return.v_data);
        },
        null,
    'box');

}

/**
 * Show a toast notification with a specified type and message.
 * 
 * @param {string} type - The type of the toast notification.
 *                       Possible values: 'success', 'info', 'warning', 'error', 'default'
 * @param {string} message - The message to display in the toast.
 */
function showToast(type, message) {
  const titleMap = {
    'error': 'Failed',
    'success': 'Success',
    'info': 'Information',
    'default': 'Done'
  };

  let title = titleMap[type] || titleMap['default']

  let html_msg = `<div class="v-toast__body p-0">
                    <h3 class="font-weight-bold">${title}</h3>
                    <p>${message}</p>
                  </div>`
  const $toast = useToast()

  $toast.open({
    message: html_msg,
    type: type,
    duration: type === 'error' ? 0 : 3000
  })
}

export { createMessageModal, showAlert, showConfirm, checkSessionMessage, showToast};
