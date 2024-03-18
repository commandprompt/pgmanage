// FIXME: this is ugly and needs to be rewritten from scratch ASAP
var v_modal_password_ok_function = null;
var v_modal_password_input = null;

import { conn_app } from './connections_modal.js'
import { execAjax } from './ajax_control.js';
import { showToast } from './notification_control.js';
import { tabsStore } from './stores/stores_initializer.js';

function passwordModalsInit() {

  let master_pass_input = document.getElementById('master_password')
  master_pass_input.oninput = function() { checkMasterPassword }

  let master_pass_input_confirm = document.getElementById('master_password_confirm')
  master_pass_input_confirm.oninput = function() { checkMasterPassword()}

  let password_set = document.getElementById('password_set')
  password_set.onclick = function() { saveMasterPass() }
}


function showNewMasterPassPrompt(p_message) {
  let v_modal = document.getElementById('modal_password_set')
  let v_content_div = v_modal.querySelector('#modal_password_content');

  if (p_message)
    v_content_div.innerHTML = p_message;

  // open modal with preventing closing by pressing on background or by keywords

  $('#modal_password_set').modal({backdrop: 'static', keyboard: false});
  $('#master_password').passtrength({passwordToggle: false});

  setTimeout(function() {
    $('#master_password').focus();
  }, 500)

}

function saveMasterPass() {

	let v_confirm_pwd = document.getElementById('master_password_confirm');
	let v_pwd = document.getElementById('master_password');

	if ((v_confirm_pwd.value!='' || v_pwd.value!='') && (v_pwd.value!=v_confirm_pwd.value))
    showToast("error", "Password and Confirm Password fields do not match.")
	else if ((v_pwd.value === v_confirm_pwd.value) && (v_pwd.value.length < 8 && v_pwd.value.length >= 1))
    showToast("error", "Password and Confirm Password fields must be longer than 8.")
	else {
		execAjax('/master_password/',
        JSON.stringify({"master_password": v_pwd.value}),
				function(p_return) {
          conn_app.mount("#connections-modal-wrap");
          v_omnis.div.style.opacity = 1
          showToast("success", "Master password created.")
				});
	}
}

function checkMasterPassword() {
	let password1 = document.getElementById('master_password');
	let password2 = document.getElementById('master_password_confirm');
	let form_button = document.getElementById('password_set');

	if (password1.checkValidity() && password2.value === password1.value){
		password2.classList.remove("is-invalid");
		password2.classList.add('is-valid');
		form_button.disabled = false;
	}else if (password2.value.length >= password1.value.length && password2.value !== password1.value) {
		password2.classList.add("is-invalid");
		password2.classList.remove('is-valid');
		form_button.disabled = true;}
	else {
		password2.classList.remove('is-invalid', 'is-valid');
		form_button.disabled = true;
	}
}

function showMasterPassPrompt(p_message) {
  let v_content_div = document.getElementById('master_password_content');
  let v_button_check = document.getElementById('password_check_button');
  let v_button_reset = document.getElementById('password_reset_button');
  let v_modal_password_input = document.getElementById('master_password_check');

  if (p_message)
    v_content_div.innerHTML = p_message;

  $('#modal_password_check').modal({backdrop: 'static', keyboard: false});

  setTimeout(function () {
  	v_modal_password_input.focus();
  },500);

  v_modal_password_ok_function = function() {
    execAjax('/master_password/',
      JSON.stringify({"master_password": v_modal_password_input.value}),
      function(p_return) {
        conn_app.mount("#connections-modal-wrap");
        v_omnis.div.style.opacity = 1
      },
      function(p_return) {
        setTimeout(function() {showMasterPassPrompt(p_return.v_data)}
          , 300)
      },
      'box'
    );
  }

  let v_modal_password_reset_function = function () {

    let v_content_div = document.getElementById('modal_message_content');
	  let v_button_yes = document.getElementById('modal_message_yes');
	  let v_button_ok = document.getElementById('modal_message_ok');
	  let v_button_no = document.getElementById('modal_message_no');
	  let v_button_cancel = document.getElementById('modal_message_cancel');

    v_button_ok.style.display = 'none';
	  v_button_cancel.style.display = 'none';
    v_button_no.className = 'btn btn-primary';
    v_button_yes.className = 'btn btn-danger';

    $('#modal_message_dialog > div > div > button.close').css('display', 'none');

    v_content_div.innerHTML = `Are you sure you want to reset you master password?
                               You will lose your saved connection passwords.`

    v_button_yes.onclick = function () {
      execAjax(
        '/reset_master_password/',
        JSON.stringify({}),
        function(p_return){
          showNewMasterPassPrompt(`Please set your master password. It will be used to secure your connection credentials.`);
        }
      )
    }
		v_button_no.onclick = function() {
      showMasterPassPrompt(`Please provide your master password to unlock your connection credentials for this session.`);
    }
    $('#modal_message').modal({backdrop: 'static', keyboard: false});
  }

  v_modal_password_input.onkeydown = function (event) {
    if (event.key === 'Enter') {
      v_modal_password_ok_function();
      $('#modal_password_check').modal('hide');
    }
  }

  v_button_check.onclick = v_modal_password_ok_function;

  v_button_reset.onclick = v_modal_password_reset_function;
}

export { passwordModalsInit, showNewMasterPassPrompt, showMasterPassPrompt }