import 'vite/modulepreload-polyfill';
import 'bootstrap/scss/bootstrap.scss'
import 'bootstrap';
import './assets/scss/omnidb.scss'
import './assets/scss/login.scss'
import { showAlert } from './notification_control';
import { getCookie } from './ajax_control'
import axios from 'axios'

document.addEventListener('DOMContentLoaded', function() {
  let txt_user = document.getElementById('txt_user')
  let txt_pwd = document.getElementById('txt_pwd')
  let btn_sign_in = document.getElementById('btn_sign_in')

  txt_user.onkeydown = function(event) {
    if (event.key === 'Enter') signIn();
  }
  txt_pwd.onkeydown = function(event) {
    if (event.key === 'Enter') signIn();
  }

  btn_sign_in.onclick = function() { signIn() }
});

function signIn() {
  let username = document.getElementById('txt_user')
  let password = document.getElementById('txt_pwd')

  axios.post(
    `/sign_in/`,
    {'username': username.value, 'password': password.value},
    {headers: { "X-CSRFToken": getCookie(v_csrf_cookie_name) }}
  ).then((resp) => {
      if(resp.data.v_data >= 0) {
        window.open(v_url_folder + "/workspace", '_self');
      } else if (resp.data.v_data >= 0) {
        showAlert('Invalid authentication token, use pgmanage-server to support multiple users.');
      } else {
        showAlert('Invalid username or password.');
      }
  })

}