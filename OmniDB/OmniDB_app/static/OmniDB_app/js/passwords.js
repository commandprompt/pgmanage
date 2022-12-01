/*
This file is part of OmniDB.
OmniDB is open-source software, distributed "AS IS" under the MIT license in the hope that it will be useful.

The MIT License (MIT)

Portions Copyright (c) 2015-2020, The OmniDB Team
Portions Copyright (c) 2017-2020, 2ndQuadrant Limited

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/// <summary>
/// Startup function.
/// </summary>
$(function () {

  $('#modal_password').on('hidden.bs.modal', function (e) {
    if (v_modal_password_ok_clicked !=true && v_modal_password_cancel_callback!=null) {
      v_modal_password_cancel_callback();
    }
    else if(v_modal_password_ok_clicked == true && v_modal_password_ok_after_hide_function!=null) {
      v_modal_password_ok_after_hide_function();
    }
  });

  $('#modal_password').on('shown.bs.modal', function (e) {
    if (v_modal_password_input!=null) {
      v_modal_password_input.focus();
      v_modal_password_input.onkeydown = function(event) {
          if (event.keyCode == 13) {
              v_modal_password_ok_function();
              $('#modal_password').modal('hide');
           }
      };
    }
  });

  v_modal_password_ok_clicked = false;
  v_modal_password_ok_function = null;
  v_modal_password_ok_after_hide_function = null;
  v_modal_password_cancel_callback = null;
  v_modal_password_input = null;
});

function showPasswordPrompt(p_database_index, p_callback_function, p_cancel_callback_function, p_message, p_send_tab_id = true) {
  v_modal_password_ok_clicked = false;
  v_modal_password_cancel_callback = p_cancel_callback_function;
  var v_content_div = document.getElementById('modal_password_content');
  var v_button_ok = document.getElementById('modal_password_ok');
  var v_button_cancel = document.getElementById('modal_password_cancel');
  v_modal_password_input = document.getElementById('txt_password_prompt');

  if (p_message)
    v_content_div.innerHTML = p_message;

  $('#modal_password').modal();

  v_modal_password_ok_function = function() {
    v_modal_password_ok_clicked = true;
    checkPasswordPrompt(p_database_index, p_callback_function, p_cancel_callback_function, p_send_tab_id);
  }

  v_button_ok.onclick = v_modal_password_ok_function;

  v_button_cancel.onclick = function() {
    v_modal_password_ok_clicked = false;
    if (p_cancel_callback_function)
      p_cancel_callback_function();
  }

}

function checkPasswordPrompt(p_database_index, p_callback_function, p_cancel_callback_function, p_send_tab_id) {

  var v_password = document.getElementById('txt_password_prompt').value;
  var v_tab_id = '';
  if (p_send_tab_id)
    v_tab_id = v_connTabControl.selectedTab.id;

  v_modal_password_ok_after_hide_function = function() {
    execAjax('/renew_password/',
			JSON.stringify({"p_database_index": p_database_index,
                      "p_tab_id": v_tab_id,
                      "p_password": v_password}),
			function(p_return) {

        if (p_callback_function)
          p_callback_function();

			},
			function(p_return) {
        showPasswordPrompt(p_database_index, p_callback_function, p_cancel_callback_function, p_return.v_data, p_send_tab_id);
      },
			'box'
    );
  }
}

function showNewMasterPassPrompt(p_message) {
  let v_modal = document.getElementById('modal_password_set')
  let v_content_div = v_modal.querySelector('#modal_password_content');

  if (p_message)
    v_content_div.innerHTML = p_message;

  // open modal with preventing closing by pressing on background or by keywords

  $('#modal_password_set').modal({backdrop: 'static', keyboard: false});
  $('#master_password').passtrength({passwordToggle: false});
}

function saveMasterPass() {

	let v_confirm_pwd = document.getElementById('master_password_confirm');
	let v_pwd = document.getElementById('master_password');

	if ((v_confirm_pwd.value!='' || v_pwd.value!='') && (v_pwd.value!=v_confirm_pwd.value))
		showAlert('Password and Confirm Password fields do not match.');
	else if ((v_pwd.value === v_confirm_pwd.value) && (v_pwd.value.length < 8 && v_pwd.value.length >= 1))
		showAlert('Password and Confirm Password fields must be longer than 8.');
	else {
		execAjax('/master_password/',
        JSON.stringify({"master_password": v_pwd.value}),
				function(p_return) {
					showAlert('Master password created.');
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

  v_modal_password_ok_function = function() {
    execAjax('/master_password/',
      JSON.stringify({"master_password": v_modal_password_input.value}),
      function(p_return) {

        // Retrieving database list.
        getDatabaseList(true);

        // Retrieving connection list
        showConnectionList(false, false);
      },
      function(p_return) {
        setTimeout(function() {showMasterPassPrompt(p_return.v_data)}
          , 300)
      },
      'box'
    );
  }

  v_modal_password_reset_function = function () {

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

  v_button_check.onclick = v_modal_password_ok_function;

  v_button_reset.onclick = v_modal_password_reset_function;
}
