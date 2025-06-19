import "vite/modulepreload-polyfill";
import "bootstrap/scss/bootstrap.scss";
import "./assets/css/font-poppins.css";
import "./assets/scss/login.scss";
import { getCookie } from "./ajax_control";
import axios from "axios";

document.addEventListener("DOMContentLoaded", function () {
  let txt_user = document.getElementById("txt_user");
  let txt_pwd = document.getElementById("txt_pwd");
  let btn_sign_in = document.getElementById("btn_sign_in");

  txt_user.onkeydown = function (event) {
    if (event.key === "Enter") signIn();
  };
  txt_pwd.onkeydown = function (event) {
    if (event.key === "Enter") signIn();
  };

  btn_sign_in.onclick = function () {
    signIn();
  };
});

window.addEventListener("load", function () {
  document.querySelector("body").classList.add("loaded");
});

function signIn() {
  let username = document.getElementById("txt_user");
  let password = document.getElementById("txt_pwd");
  let alertBox = document.getElementById("alert_box");

  axios
    .post(
      `${app_base_path}/sign_in/`,
      { username: username.value, password: password.value },
      { headers: { "X-CSRFToken": getCookie(v_csrf_cookie_name) } }
    )
    .then((resp) => {
      if (resp.data.data >= 0) {
        window.open(app_base_path + "/workspace/", "_self");
      } else if (resp.data.data == -2) {
        alertBox.innerText = "Invalid authentication token, use pgmanage-server to support multiple users.";
        alertBox.classList.remove("d-none");
      } else {
        alertBox.innerText = "Invalid username or password.";
        alertBox.classList.remove("d-none");
      }
    });
}
