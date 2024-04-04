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
import { startLoading, endLoading, getCookie } from "./ajax_control";
import { showConfirm, showAlert } from "./notification_control";
import axios from 'axios'

let v_usersObject;
let newUsersObject;
/// <summary>
/// Add a virtual new user with pending information.
/// </summary>
function newUser() {
    document.getElementById('div_save_users').disabled = true;
    document.getElementById('div_save_users').classList.remove('d-none');

    if (newUsersObject.newUsers.length === 0){

        newUsersObject.newUsers.push(["", "", 0])
        listUsers(true,{adding_user:true});

    } else {
        listUsers(true,{adding_user:true});
    }
}

/// <summary>
/// Removes specific user.
/// </summary>
/// <param name="id">User ID.</param>
function removeUserConfirm(id) {
    axios.post(
        '/remove_user/',
            {'id': id},
            {headers: { "X-CSRFToken": getCookie(v_csrf_cookie_name) }}
        ).then((resp) => {
            if (v_usersObject.v_cellChanges.length === 0 && newUsersObject.newUsers.length === 0)
                document.getElementById('div_save_users').disabled = true;
            listUsers(true);
        }).catch((error) => {
            showAlert('Request error')
        })
}

/// <summary>
/// Displays question to remove specific user and removes if accepted.
/// </summary>
/// <param name="id">User ID.</param>
function removeUser(id) {
    showConfirm('Are you sure you want to remove this user?', ()=>{ removeUserConfirm(id) });
}

/// <summary>
/// Undo adding specific new user.
/// </summary>
/// <param name="p_index">Connection index in the connection list.</param>
function removeNewUserConfirm(p_index) {

    if (newUsersObject.newUsers.length == 1)
        newUsersObject.newUsers = [];
    else if (p_index == 0)
        newUsersObject.newUsers.shift();
    else if (p_index + 1 == newUsersObject.newUsers.length)
        newUsersObject.newUsers.pop();
    else
        newUsersObject.newUsers.splice(p_index,1);
    listUsers(true);

}

/// <summary>
/// Undo add new user from virtual users
/// </summary>
/// <param name="p_id">User ID.</param>
function removeNewUser(p_index) {

    showConfirm('Are you sure you want to undo adding this user?',
  function() {
        removeNewUserConfirm(p_index);
    });

}

/// <summary>
/// Saves all changes in the user list, then calls to save new users.
/// </summary>
function saveUsers() {

    if (v_usersObject.v_cellChanges.length==0 && newUsersObject.newUsers.length==0)
            return;

    var v_unique_rows_changed = [];
    var v_data_changed = [];
    var v_user_id_list = [];

    for (let row in v_usersObject.v_cellChanges) {
        v_unique_rows_changed.push(row);
    };

    $.each(v_unique_rows_changed, function(i, el){
            v_data_changed[i] = v_usersObject.v_cellChanges[el].p_data;
        v_user_id_list[i] = v_usersObject.v_user_ids[el];
    });

    var changes = {
        edited: v_data_changed,
        new: newUsersObject.newUsers
    }

    axios.post(
        '/save_users/',
            {'changes': changes, "user_id_list": v_user_id_list},
            {headers: { "X-CSRFToken": getCookie(v_csrf_cookie_name) }}
        ).then((resp) => {
            v_usersObject.v_cellChanges = [];
            newUsersObject.newUsers = [];
            if (v_usersObject.v_cellChanges.length === 0 && newUsersObject.newUsers.length === 0) {
                document.getElementById('div_save_users').disabled = true;
            }
            listUsers(true, {users_update: v_data});
        }).catch((error) => {
            showAlert('Request error')
        })

}

$('#modal_users').on('shown.bs.modal', function (e) {
  getUsers();
});

$('#modal_users').on('hidden.bs.modal', function (e) {
    newUsersObject.newUsers = [];
});


function changeUser(event, p_row_index, p_col_index) {
    let username = document.getElementById(`user_item_username_${p_row_index}`);
    let password = document.getElementById(`user_item_password_${p_row_index}`);
    let v_user_is_superuser = (document.getElementById("user_item_superuser_" + p_row_index).checked) ? 1 : 0;
    let p_data_template = [
        username.value,
        password.value,
        v_user_is_superuser,
    ];

    let cellChange = {
            'rowIndex': p_row_index,
            'columnIndex': p_col_index,
            'p_data': p_data_template
    };
    v_usersObject.v_cellChanges[p_row_index] = cellChange;

    if (username.validity.valid && password.validity.valid){
        document.getElementById('div_save_users').disabled = false;
    }
    else {
        document.getElementById('div_save_users').disabled = true;
    };

    $('.omnidb__user-list__item--changed').removeClass('omnidb__user-list__item--changed');
    for (let row in v_usersObject.v_cellChanges) {
        $('#omnidb_user_item_' + row).addClass('omnidb__user-list__item--changed');
        $('#omnidb_user_select option[value="' + row + '"]').addClass('bg-warning');}
}

function changeNewUser(event, p_row_index, p_col_index) {
    let username = document.getElementById(`new_user_item_username_${p_row_index}`);
    let password = document.getElementById(`new_user_item_password_${p_row_index}`);
    let v_user_is_superuser = (document.getElementById(`new_user_item_superuser_${p_row_index}`).checked) ? 1 : 0;
    let p_data_template = [
        username.value,
        password.value,
        v_user_is_superuser,
    ];

    newUsersObject.newUsers[p_row_index] = p_data_template;


    if (username.validity.valid && password.validity.valid){
        document.getElementById('div_save_users').disabled = false;
    }
    else {
        document.getElementById('div_save_users').disabled = true;
    };

}

function getUsers(p_options = false) {

    if (p_options.adding_user) {
        var v_new_value = v_usersObject.list.length + newUsersObject.newUsers.length - 1;
        if ($('#omnidb_user_select option:last-child').text() !== '(pending info)') {
            $('#omnidb_user_select').append(new Option('(pending info)', v_new_value));
            $('#omnidb_user_select option:last-child').addClass('bg-success');
            $('#omnidb_user_select option:last-child').trigger('change');
            $('#omnidb_user_select').val(v_new_value);
        }
        else {
            $('#omnidb_user_select').val(v_new_value);
            $('#omnidb_user_select option:last-child').trigger('change');
        }
        endLoading();

    }
    else {
        if (!newUsersObject) {
            newUsersObject = new Object();
        }
        if (newUsersObject.newUsers == undefined) {
            newUsersObject.newUsers = [];
        }

        axios.post(
            '/get_users/',
                {},
                {headers: { "X-CSRFToken": getCookie(v_csrf_cookie_name) }}
            ).then((resp) => {
                v_usersObject = new Object();
                v_usersObject.v_user_ids = resp.data.v_data.v_user_ids;
                v_usersObject.v_cellChanges = [];
                v_usersObject.list = resp.data.v_data.v_data;

                var v_users_update_html = '';
                if (p_options) {
                    if (p_options.users_update) {
                        if (p_options.users_update.edited.length > 0) {
                            v_users_update_html +=
                            '<div class="card p-4 mx-auto">' +
                            '<div><h5>Edited Users:</h5></div>' +
                            '<ul class="pl-4">';
                            for (let i = 0; i < p_options.users_update.edited.length; i++) {
                                v_users_update_html += '<li class="mt-2"> - ' + p_options.users_update.edited[i][0] + '</li>';
                            }
                            v_users_update_html +=
                            '</ul>' +
                            '</div>';
                        }
                        if (p_options.users_update.new.length > 0) {
                            v_users_update_html +=
                            '<div class="card p-4 mx-auto">' +
                            '<div><h5>New Users:</h5></div>' +
                            '<ul class="pl-4">';
                            for (let i = 0; i < p_options.users_update.new.length; i++) {
                                v_users_update_html += '<li class="mt-2"> - ' + p_options.users_update.new[i][0] + '</li>';
                            }
                            v_users_update_html +=
                            '</ul>' +
                            '</div>';
                        }
                    }
                }

                var v_user_list_data = resp.data.v_data.v_data;
                var v_user_list_element = document.createElement('div');
                v_user_list_element.classList = ["omnidb__user-list"];
                var v_user_count = 0;
                var v_user_list_html =
                "<form class='d-none' autofill='false' onsubmit='(event)=>{event.preventDefault();};'>" +
                    "<input id='fake_username' type='text' placeholder='User name' value=''>" +
                    "<input id='fake_password' type='password' placeholder='Password' value=''>" +
                    "<button type='submit' disabled aria-hidden='true'></button>" +
                "</form>" +
                "<form class='omnidb__user-list__form' autofill='false' autocomplete='disabled'>" +
                    "<input tabIndex='-1' style='opacity:0;height:0px;overflow:hidden;pointer-events:none;' autofill='false' autocomplete='disabled' name='no-autofill' id='no-autofill-autofill-name' type='text' class='m-0 p-0' placeholder='Username' value=''>" +
                    "<input tabIndex='-1' style='opacity:0;height:0px;overflow:hidden;pointer-events:none;' autofill='false' autocomplete='disabled' name='no-autofill' id='no-autofill-password' type='password' class='m-0 p-0' placeholder='Password' value=''>" +
                    "<div class='form-inline mb-4'>" +
                        "<h5 class='mr-2'>Select an user</h5>" +
                        "<select id='omnidb_user_select' class='form-control'>";
                        if (p_options.focus_last)
                            v_user_list_html += "<option value=''> </option>";
                        else
                            v_user_list_html += "<option value='' selected> </option>";
                        for (var i = 0; i < v_user_list_data.length; i++) {
                            var v_user_item = v_user_list_data[i];
                            var v_user_is_superuser = (v_user_item[2] === 1) ? ' (superuser)' : '';
                            v_user_list_html +=
                            "<option value='" + i + "'>" + v_user_item[0] + v_user_is_superuser + "</option>";
                            v_user_count++;
                        }
                        for (var i = 0; i < newUsersObject.newUsers.length; i++) {
                            var v_user_item = newUsersObject.newUsers[i];
                            var v_user_is_superuser = (v_user_item[2] === 1) ? ' (superuser)' : '';
                            var v_user_item_index = parseInt(v_user_count) + parseInt(i);
                            var v_user_item_name = (v_user_item[0] === "") ? '(pending info)' : v_user_item[0] + v_user_is_superuser + ' (pending save)';
                            var v_user_is_selected = (p_options.focus_last && i + 1 == newUsersObject.newUsers.length) ? ' selected ' : '';
                            v_user_list_html +=
                            "<option class='bg-warning' value='" + v_user_item_index + "' " + v_user_is_selected + ">" + v_user_item_name + "</option>";
                        }
                        v_user_list_html +=
                        "</select>" +
                        "<button id='omnidb_utilities_menu_btn_new_user' type='button' class='btn btn-primary ml-2'><i class='fas fa-user-plus'></i><span class='ml-2'>Add new user</span></button>" +
                    "</div>" +
                    "<div id='omnidb_user_content' class='row'>" +
                        v_users_update_html +
                    "</div>" +
                    "<div class='text-center'>" +
                        "<button type='button' id='div_save_users' class='btn btn-success ml-1 d-none' disabled>Save</button>" +
                    "</div>" +
                    "<button type='submit' disabled style='display: none' aria-hidden='true'></button>" +
                "</div>";
                v_user_list_element.innerHTML = v_user_list_html;

                $('#div_users').addClass('isActive');

                window.scrollTo(0,0);

                var v_div_result = document.getElementById('div_user_list');
                var container = v_div_result;
                container.appendChild(v_user_list_element);

                let save_users = document.getElementById('div_save_users')
                save_users.onclick = function() { saveUsers() }

                let user_select = document.getElementById('omnidb_user_select')
                user_select.onchange = (event) => { renderSelectedUser(event) }

                let new_user_btn = document.getElementById('omnidb_utilities_menu_btn_new_user')
                new_user_btn.onclick = function() { newUser() }
                if (p_options) {
                    if (p_options.focus_last) {
                        setTimeout(function(){
                            $('#omnidb_user_select option:last-child').trigger('change');
                        },300);
                    }
                }
                if (v_usersObject.v_cellChanges.length > 0 || newUsersObject.newUsers.length > 0)
                    document.getElementById('div_save_users').disabled = false;
                $('[data-toggle="tooltip"]').tooltip({animation:true});// Loads or Updates all tooltips
                endLoading();
            }).catch((error) => {
                showAlert('Request error')
            })
    }


}

/// <summary>
/// Retrieving and displaying users.
/// </summary>
function listUsers(p_refresh,p_options = false) {

  startLoading();

  var v_save_button = document.getElementById('div_save_users');
    if (v_save_button !== null) {
        if (v_usersObject.v_cellChanges.length === 0 && newUsersObject.newUsers.length === 0) {
            v_save_button.disabled = true;
        }
    }

  var v_div_result = document.getElementById('div_user_list');

    if (v_div_result.innerHTML!='' && !p_options.adding_user) {
        v_div_result.innerHTML = '';
    }

  if (p_refresh==null) {
        $('#modal_users').modal();
    }
  else {
        getUsers(p_options);
    }
}

/// <summary>
/// Rendering selected user.
/// </summary>
function renderSelectedUser(event) {
    if (event.target.id === 'omnidb_user_select') {
        document.getElementById('div_save_users').disabled = true;
        document.getElementById('div_save_users').classList.remove('d-none');
    }
    var v_index = event.target.value;
    if (+v_index === (event.target.length - 2) && newUsersObject.newUsers.length !== 0){
        if (newUsersObject.newUsers[0][0].length > 1 && newUsersObject.newUsers[0][1].length >= 8)
            document.getElementById('div_save_users').disabled = false;
    }
    if (v_usersObject.v_cellChanges[v_index] !== undefined)
      if ((v_usersObject.v_cellChanges[v_index].p_data[0].length > 1) && (v_usersObject.v_cellChanges[v_index].p_data[1].length === 0 || v_usersObject.v_cellChanges[v_index].p_data[1].length >= 8))
          document.getElementById('div_save_users').disabled = false;
    var v_user_div_content = document.getElementById('omnidb_user_content');
    if (v_index == "") {
        v_user_div_content.innerHTML = "<div class='col-12 text-center'><h5 class='my-4'>No users selected, select an user or click add new user.</h5></div>";
    }
    else {
        var v_user_count = 0;
        for (let i = 0; i < v_usersObject.list.length; i++) {
            if (v_usersObject.v_cellChanges[i] !== undefined) {
                var v_user_item = v_usersObject.v_cellChanges[i].p_data;
            } else {
                var v_user_item = v_usersObject.list[i];
            }
            var v_superuser_checked = (v_user_item[2] === 1) ? 'checked' : '';
            if (i == v_index) {
                let user_id = v_usersObject.v_user_ids[v_index]
                v_user_div_content.innerHTML =
                "<div class='col-12 mb-4'>" +
                "<div id='omnidb_user_item_" + i + "' class='omnidb__user-list__item card'>" +
                "<div class='d-flex align-items-center'>" +
                    "<div class='input-group mb-2'>" +
                        "<div class='input-group-prepend'>" +
                            "<label for='user_item_username_" + i  + "' type='button' class='input-group-text'>" +
                                "<i class='fas fa-user'></i>" +
                            "</label>" +
                        "</div>" +
                        "<input autofill='false' autocomplete='disabled' name='notChromeUsername' id='user_item_username_" + i  + "' type='text' class='form-control my-0' placeholder='User name' value='" + v_user_item[0] + "' minlength='1'>" +
                    "</div>" +
                    "<span class='ml-2'>Superuser?</span>" +
                    "<div class='ml-2 mb-2'>" +
                        "<div class='omnidb__switch mr-2' data-toggle='tooltip' data-placement='bottom' data-html='true' title='<h5>Toggle superuser status. To enable again, simply turn the switch on.</h5>'>" +
                            "<input type='checkbox' id='user_item_superuser_" + i  + "' class='omnidb__switch--input' " + v_superuser_checked + ">" +
                            "<label for='user_item_superuser_" + i  + "' class='omnidb__switch--label'><span><i class='fas fa-star'></i></span></label>" +
                        "</div>" +
                    "</div>" +
                "</div>" +
                "<div class='d-flex mb-2'>" +
                "<div class='input-group-prepend'>" +
                "<label for='user_item_password_" + i  + "' type='button' class='input-group-text'>" +
                "<i class='fas fa-key'></i>" +
                "</label>" +
                "</div>" +
                "<input autofill='false' autocomplete='disabled' name='new-password' id='user_item_password_" + i  + "' type='password' class='form-control my-0' placeholder='New password' value='" + v_user_item[1] + "' minlength='8'>" +
                "</div>" +
                "<span class='mr-2 text-danger omnidb__user-list__close'>" +
                "<i id='user_remove_" + i  + "' title='Remove User' class='fas fa-times action-grid action-close'></i>"
                "</span>" +
                "</div>" +
                "</div>";
                let user_item_username = document.getElementById(`user_item_username_${i}`)
                user_item_username.oninput = (event) => { changeUser(event.target.value, i, 0)}

                let user_item_superuser = document.getElementById(`user_item_superuser_${i}`)
                user_item_superuser.onchange = (event) => { changeUser(event.target.value, i, 2)}

                let user_item_password = document.getElementById(`user_item_password_${i}`)
                user_item_password.oninput = (event) => { changeUser(event.target.value, i, 1)}

                let user_remove = document.getElementById(`user_remove_${i}`)
                user_remove.onclick = function() { removeUser(user_id)}

                $(`#user_item_password_${i}`).passtrength({
                    passwordToggle: false
                });
            }
            v_user_count++;
        }
        for (let i = 0; i < newUsersObject.newUsers.length; i++) {
            let v_user_item = newUsersObject.newUsers[i];
            let v_superuser_checked = (v_user_item[2] === 1) ? 'checked' : '';
            let v_user_item_index = parseInt(v_user_count) + parseInt(i);
            let v_user_div_content = document.getElementById('omnidb_user_content');
            if (v_user_item_index == v_index) {
                v_user_div_content.innerHTML =
                "<div class='col-12 mb-4'>" +
                "<div id='omnidb_user_item_" + i + "' class='omnidb__user-list__item card'>" +
                "<div class='d-flex align-items-center'>" +
                    "<div class='input-group mb-2'>" +
                        "<div class='input-group-prepend'>" +
                            "<label for='new_user_item_username_" + i  + "' type='button' class='input-group-text'>" +
                                "<i class='fas fa-user'></i>" +
                            "</label>" +
                        "</div>" +
                        "<input autofill='false' autocomplete='off' name='off' id='new_user_item_username_" + i  + "' type='text' class='form-control my-0' placeholder='User name' value='" + v_user_item[0] + "' required minlength='1'>" +
                    "</div>" +
                    "<span class='ml-2'>Superuser?</span>" +
                    "<div class='ml-2 mb-2'>" +
                        "<div class='omnidb__switch mr-2' data-toggle='tooltip' data-placement='bottom' data-html='true' title='<h5>Toggle superuser status. To enable again, simply turn the switch on.</h5>'>" +
                            "<input type='checkbox' id='new_user_item_superuser_" + i  + "' class='omnidb__switch--input' " + v_superuser_checked + ">" +
                            "<label for='new_user_item_superuser_" + i  + "' class='omnidb__switch--label'><span><i class='fas fa-star'></i></span></label>" +
                        "</div>" +
                    "</div>" +
                "</div>" +
                "<div class='d-flex mb-2'>" +
                "<div class='input-group-prepend'>" +
                "<label for='new_user_item_password_" + i  + "' type='button' class='input-group-text'>" +
                "<i class='fas fa-key'></i>" +
                "</label>" +
                "</div>" +
                "<input autofill='false' autocomplete='off' name='off' id='new_user_item_password_" + i  + "' type='password' class='form-control my-0' placeholder='New password' value='" + v_user_item[1] + "' required minlength='8'>" +
                "</div>" +
                "<span class='mr-2 text-danger omnidb__user-list__close'>" +
                    "<i id='new_user_remove_" + i  + "'  title='Remove User' class='fas fa-times action-grid action-close text-danger'></i>" +
                "</span>" +
                "</div>" +
                "</div>";
                        let new_user_username = document.getElementById(`new_user_item_username_${i}`)
                        new_user_username.oninput = (event) => { changeNewUser(event.target.value, i, 0)}

                        let new_user_item_superuser = document.getElementById(`new_user_item_superuser_${i}`)
                        new_user_item_superuser.onchange = (event) => { changeNewUser(event.target.value, i, 2)}

                        let new_user_item_password = document.getElementById(`new_user_item_password_${i}`)
                        new_user_item_password.oninput = (event) => { changeNewUser(event.target.value, i, 1)}

                        let new_user_remove = document.getElementById(`new_user_remove_${i}`)
                        new_user_remove.onclick = function() { removeNewUser(i)}

                        $(`#new_user_item_password_${i}`).passtrength({
                    passwordToggle: false
                });
            }
        }
    }

}

export { listUsers, newUser }