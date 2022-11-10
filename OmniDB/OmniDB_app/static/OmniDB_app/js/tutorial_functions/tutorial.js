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

function startTutorial(p_tutorial_name) {
  if (v_omnis.omnis_ui_assistant) {
    v_omnis.omnis_ui_assistant.self_destruct();
  }
  // Disabling interactions with omnis.
  v_omnis.div.classList.add('omnis--active');
  // Instantiate the component.
  v_omnis.omnis_ui_assistant = createOmnisUiAssistant({
    p_callback_end: function(){
      // Configuring to delete the componente when it's no longer used.
      delete v_omnis.omnis_ui_assistant;
      // Enabling interactions with omnis.
      v_omnis.div.classList.remove('omnis--active');
    },
    // Omnis Object
    p_omnis: v_omnis
  });
  // Setting the tutorial to the default example tutorial `main`.
  var v_tutorial_name = (p_tutorial_name) ? p_tutorial_name : 'main';
  var v_button_inner_query_attr = ' disabled title="Open a new connection first." ';
  if (v_connTabControl.selectedTab.tag.tabControl) {
    if (v_connTabControl.selectedTab.tag.tabControl.tabList.length > 0) {
      v_button_inner_query_attr = '';
    }
  }
  var v_button_inner_query =
  '<li class="mb-2">' +
    `<button ` + v_button_inner_query_attr + ` type="button" class="btn btn-primary d-flex align-items-center" onclick="startTutorial('connection_tab');">` +
      '<i class="fas fa-list mr-2"></i>The Connection Tab' +
    '</button>' +
  '</li>';
  // Configuring the available tutorials.
  var v_tutorials = {
    'main': [
      {
        p_message: 'This contains the outer connection and global panels [ connections_list_manager, snippets_panel, [conn_1, conn_2, ...], add_connection]',
        p_target: document.getElementsByClassName('omnidb__tab-menu omnidb__tab-menu--primary')[0],
        p_title: 'Primary menu'
      },
      {
        p_message: 'This contains general settings and options, such as [ versioning, connections_list_manager, user_setting, plugins...]',
        p_target: document.getElementsByClassName('omnidb__utilities-menu')[0],
        p_title: 'Utilities menu'
      }
    ],
    'utilities_menu': [
      {
        p_callback_end: function() {$('.omnidb__utilities-menu').removeClass('omnidb__utilities-menu--show');},
        p_callback_start: function() {$('.omnidb__utilities-menu').addClass('omnidb__utilities-menu--show');},
        p_clone_target: true,
        p_message: `
        <p>Contains general settings and options:</p>
        <ul>
        <li>Username and versioning.</li>
        <li><i class="fas fa-plug omnidb__theme__text--primary mr-2"></i>Connection management.</li>
        <li><i class="fas fa-user omnidb__theme__text--primary mr-2"></i>User management.</li>
        <li><i class="fas fa-cog omnidb__theme__text--primary mr-2"></i>UI settings (shortcuts, theme, fonts...).</li>
        <li><i class="fas fa-cube omnidb__theme__text--primary mr-2"></i>Plugins management.</li>
        <li><i class="fas fa-info-circle omnidb__theme__text--primary mr-2"></i>About.</li>
        <li><i class="fas fa-sign-out-alt omnidb__theme__text--primary mr-2"></i>Sign Out.</li>
        </ul>
        `,
        p_target: document.getElementsByClassName('omnidb__utilities-menu')[0],
        p_title: 'Utilities Menu',
        p_update_delay: 350
      },
      {
        p_callback_end: function() {$('.omnidb__utilities-menu').removeClass('omnidb__utilities-menu--show');},
        p_callback_start: function() {$('.omnidb__utilities-menu').addClass('omnidb__utilities-menu--show');},
        p_clone_target: true,
        p_message: `
        <p>If you just configured OmniDB and logged with the default <strong>admin</strong> user, you should create the first user.</p>
        <p>Follow this walkthrough if you want to create other users as well.</p>
        `,
        p_next_button: false,
        p_target: document.getElementById('omnidb__utilities-menu__link-user'),
        p_title: 'Managing Users'
      },
      {
        p_callback_after_update_start: function() {setTimeout(function(){
            if (v_omnis.omnis_ui_assistant.divClonedElement.children[0]) {
              v_omnis.omnis_ui_assistant.divClonedElement.children[0].classList.remove('ml-2');
            }
          },50);
        },
        p_clone_target: true,
        p_message: `
        <p>Click on <strong>Add new user</strong>.</p>
        `,
        p_next_button: false,
        p_target: function() {var v_target = document.getElementById('omnidb_utilities_menu_btn_new_user'); return v_target},
        p_title: 'Add a New User',
        p_update_delay: 1000
      },
      {
        p_message: `
        <ul>
        <li><i class="fas fa-user omnidb__theme__text--primary mr-2"></i>OmniDB login name.</li>
        <li><i class="fas fa-key omnidb__theme__text--primary mr-2"></i>OmniDB login password.</li>
        <li><i class="fas fa-star omnidb__theme__text--primary mr-2"></i>Defines if the user can manage other OmniDB users.</li>
        </ul>
        <div class="alert alert-danger">The default <strong>admin user</strong> should be deleted once a new super user has been created.</div>
        `,
        p_target: function() {var v_target = document.getElementById('omnidb_user_content'); return v_target},
        p_title: 'User Options',
        p_update_delay: 350
      }
    ],
    'connections_menu': [
      {
        p_clone_target: true,
        p_message: `
        <p>This is the outer connections menu. Each connection added becomes a new item in this menu.</p>
        <p>The menu initially contains.</p>
        <ul>
        <li>Connections manager.</li>
        <li>Welcome, tutorial and useful links.</li>
        <li>Snippets panel toggler.</li>
        </ul>
        <p>Let's first <span class="badge badge-info">add a new connection</span>.</p>
        <p>Please, click on the
        <svg height="27" viewBox="0 5 58 58" xmlns="http://www.w3.org/2000/svg">
          <path d="M40.8635 21.9291C39.8124 20.2722 38.403 18.8703 36.7378 17.827C37.1953 18.7168 37.5976 19.6889 37.9355 20.7357C38.9901 21.0739 39.9683 21.4761 40.8635 21.9291Z" fill="#878FC6"></path>
          <path d="M30.6628 15.8552V19.5423C32.3082 19.5931 33.8876 19.7769 35.3722 20.0791C34.6859 18.4279 33.8104 17.0944 32.8125 16.1828C32.1155 16.0165 31.3987 15.9066 30.6628 15.8552Z" fill="#878FC6"></path>
          <path d="M40.8621 35.8357C39.9661 36.2894 38.9901 36.6916 37.9355 37.0269C37.5976 38.0759 37.1946 39.0466 36.7378 39.9377C38.4037 38.8937 39.8124 37.4919 40.8621 35.8357Z" fill="#878FC6"></path>
          <path d="M42.6299 31.4759C42.3757 30.9532 42.1965 30.3931 42.103 29.7986H39.1338C39.0829 31.4343 38.8987 33.0046 38.5984 34.4797C40.2586 33.7971 41.6001 32.925 42.5166 31.931C42.5534 31.7781 42.5995 31.6302 42.6299 31.4759Z" fill="#878FC6"></path>
          <path d="M42.5173 25.8359C41.6015 24.8398 40.26 23.9677 38.5977 23.283C38.898 24.7602 39.0821 26.3297 39.1331 27.9655H42.1022C42.1957 27.3709 42.3749 26.8109 42.6313 26.2903C42.5994 26.136 42.5534 25.9867 42.5173 25.8359Z" fill="#878FC6"></path>
          <path d="M36.9496 29.7986H30.6636V36.0492C32.6397 35.9844 34.5032 35.7146 36.1754 35.2785C36.6131 33.6153 36.8844 31.764 36.9496 29.7986Z" fill="#878FC6"></path>
          <path d="M30.6628 27.9663H36.9488C36.8837 26.0008 36.6124 24.1474 36.174 22.4842C34.5017 22.0481 32.6382 21.7783 30.6621 21.7135V27.9663H30.6628Z" fill="#878FC6"></path>
          <path d="M30.6628 41.9103C31.3987 41.8596 32.1148 41.749 32.8132 41.582C33.8112 40.669 34.6866 39.3369 35.3729 37.6857C33.8884 37.9865 32.3082 38.1696 30.6635 38.2204V41.9103H30.6628Z" fill="#878FC6"></path>
          <path d="M20.8935 34.4797C20.5911 33.0046 20.4069 31.435 20.3559 29.7986H16.6445C16.6948 30.5284 16.806 31.2399 16.9718 31.9303C17.889 32.9271 19.2312 33.7971 20.8935 34.4797Z" fill="#878FC6"></path>
          <path d="M28.8234 15.8552C28.0883 15.9059 27.3715 16.0165 26.6745 16.1828C25.6766 17.0944 24.8011 18.4286 24.1162 20.077C25.6008 19.7769 27.1788 19.5931 28.8234 19.5423V15.8552Z" fill="#878FC6"></path>
          <path d="M28.8234 21.7135C26.848 21.7783 24.9852 22.0481 23.3137 22.4842C22.8738 24.1474 22.604 26.0008 22.5388 27.9663H28.8234V21.7135Z" fill="#878FC6"></path>
          <path d="M22.7519 17.8256C21.0853 18.8689 19.6751 20.2714 18.6248 21.9269C19.5207 21.4739 20.4982 21.0717 21.5535 20.7357C21.8928 19.6874 22.2958 18.7146 22.7519 17.8256Z" fill="#878FC6"></path>
          <path d="M28.8234 41.9103V38.2204C27.1788 38.1696 25.5994 37.9865 24.1162 37.6857C24.8011 39.3376 25.6758 40.6712 26.6745 41.582C27.3722 41.7483 28.0883 41.8589 28.8234 41.9103Z" fill="#878FC6"></path>
          <path d="M16.6438 27.9662H20.3559C20.4062 26.3305 20.591 24.7602 20.8935 23.2837C19.2304 23.9663 17.8882 24.8405 16.971 25.8366C16.8053 26.527 16.6948 27.2378 16.6438 27.9662Z" fill="#878FC6"></path>
          <path d="M22.752 39.9392C22.2958 39.0481 21.8928 38.0774 21.5543 37.0291C20.4996 36.6924 19.5229 36.2902 18.627 35.8379C19.6766 37.4934 21.0861 38.8952 22.752 39.9392Z" fill="#878FC6"></path>
          <path d="M28.8234 29.7986H22.5388C22.604 31.764 22.8753 33.6153 23.3137 35.2785C24.9845 35.7146 26.848 35.9844 28.8234 36.0492V29.7986Z" fill="#878FC6"></path>
          <path d="M25.8068 10.1681C26.2615 10.95 26.5002 11.7926 26.5732 12.6372C27.6009 12.4392 28.6584 12.3286 29.7441 12.3286C37.0387 12.3286 43.2227 17.0027 45.4701 23.4971C46.245 23.1357 47.0999 22.9173 48.0128 22.9173C48.3181 22.9173 48.612 22.9624 48.9053 23.0068C46.3831 14.8619 38.7591 8.94092 29.7449 8.94092C28.2114 8.94092 26.7247 9.1283 25.2891 9.45235C25.4704 9.6813 25.656 9.90814 25.8068 10.1681Z" fill="#878FC6"></path>
          <path d="M15.4199 41.6361C15.8774 40.8507 16.4958 40.2223 17.1998 39.7376C14.6641 36.8444 13.1166 33.0713 13.1038 28.932C13.1038 28.9165 13.0996 28.901 13.0996 28.8841C13.0996 28.8707 13.1031 28.8587 13.1031 28.8453C13.1123 24.7073 14.6564 20.9364 17.187 18.0397C16.4858 17.5543 15.8739 16.9175 15.4185 16.1334C15.269 15.8756 15.1663 15.6072 15.0587 15.3395C11.7404 18.8984 9.69556 23.6485 9.69556 28.8841C9.69556 34.1224 11.7418 38.8725 15.0615 42.4293C15.1677 42.1624 15.2711 41.8925 15.4199 41.6361Z" fill="#878FC6"></path>
          <path d="M48.0121 34.8495C47.0991 34.8495 46.2421 34.6304 45.4679 34.2676C43.2191 40.7641 37.0387 45.4353 29.7441 45.4353C28.6569 45.4353 27.5973 45.3233 26.5689 45.1268C26.4959 45.9735 26.2629 46.8182 25.8096 47.6029C25.6594 47.8622 25.4753 48.0869 25.2939 48.3144C26.7268 48.6385 28.2128 48.8252 29.7441 48.8252C38.7591 48.8252 46.383 42.9056 48.9066 34.7607C48.6105 34.8037 48.3173 34.8495 48.0121 34.8495Z" fill="#878FC6"></path>
          <path d="M52.0317 28.8833C52.0317 26.6734 50.2334 24.8834 48.0115 24.8834C45.7896 24.8834 43.9863 26.6734 43.9863 28.8833C43.9863 31.0932 45.7896 32.8804 48.0115 32.8804C50.2334 32.8804 52.0317 31.0932 52.0317 28.8833Z" fill="#525678"></path>
          <path d="M18.6008 9.68901C16.6757 10.7922 16.0177 13.2345 17.1283 15.1507C18.2375 17.0633 20.6995 17.7184 22.6225 16.6152C24.5476 15.5106 25.2041 13.0612 24.095 11.1522C22.9809 9.23957 20.5252 8.5809 18.6008 9.68901Z" fill="#525678"></path>
          <path d="M18.6043 48.0832C20.5308 49.1878 22.9864 48.5334 24.097 46.6208C25.2062 44.7082 24.5475 42.2609 22.6259 41.1577C20.6994 40.051 18.2409 40.7076 17.1353 42.6188C16.0226 44.5349 16.6792 46.9808 18.6043 48.0832Z" fill="#525678"></path>
        </svg> 
        button.</p>
        `,
        p_target: document.getElementsByClassName('omnidb__tab-menu omnidb__tab-menu--primary')[0],
        p_title: 'Primary menu'
      },
      {
        p_callback_after_update_start: function() {setTimeout(function(){var v_target = document.getElementById('button_new_connection'); v_omnis.omnis_ui_assistant.divClonedElement.children[0].classList.remove('ml-2');},50);},
        p_callback_start: function() {startConnectionManagement();},
        p_clone_target: true,
        p_message: `
        <p>Click on <strong>New Connection</strong>.</p>
        `,
        p_next_button: false,
        p_target: function() {var v_target = document.getElementById('button_new_connection'); return v_target},
        p_title: 'Add a New Connection',
        p_update_delay: 1000
      },
      {
        p_message: `
        <p>Select the proper DBMS technology.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_type'); return v_target},
        p_title: 'Connection Type',
        p_update_delay: 300
      },
      {
        p_message: `
        <p>Type a helpful name for the connection.</p>
        <p>This is used as name reference on many UI areas.</p>
        <p>i.e: Local dvdrental barman.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_title'); return v_target},
        p_title: 'Title'
      },
      {
        p_message: `
        <p>Type the server address. Do not include ports.</p>
        <p>i.e:127.0.0.1</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_server'); return v_target},
        p_title: 'Server'
      },
      {
        p_message: `
        <p>Type the port of the server.</p>
        <p>i.e: PostgreSQL uses 5432 by default, but if you are using pgbouncer, you may want to use 6432 as the entry point.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_port'); return v_target},
        p_title: 'Port'
      },
      {
        p_message: `
        <p>Type the name of the database.</p>
        <p>i.e: postgres, dvdrental.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_database'); return v_target},
        p_title: 'Database'
      },
      {
        p_message: `
        <p>Type the name of the user with priviledges to access the database.</p>
        <p>i.e: postgres.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_user'); return v_target},
        p_title: 'User'
      },
      {
        p_message: `
        <p>This is <strong>optional</strong>.</p>
        <p>If you don't save the user password, you will be required to manually input it everytime a new connection to this database is started.</p>
        <p>If saved, this password will be stored in the database configured for OmniDB (default is omnidb.db).</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_user_pass'); return v_target},
        p_title: 'User password'
      },
      {
        p_message: `
        <p>You may want to hit 'test' before saving the conntion.</p>
        <p>After that, click save.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_button_test_connection'); return v_target},
        p_title: 'Test the Connection'
      }
    ],
    'terminal_connection': [
      {
        p_clone_target: true,
        p_message: `
        <p>First let's open the <strong>connections management</strong> interface.</p>
        <p>Please, click on the OmniDB Icon button.</p>
        `,
        p_target: document.getElementsByClassName('omnidb__tab-menu omnidb__tab-menu--primary')[0],
        p_title: 'Accessing connections managemnet'
      },
      {
        p_callback_after_update_start: function() {setTimeout(function(){var v_target = document.getElementById('button_new_connection'); v_omnis.omnis_ui_assistant.divClonedElement.children[0].classList.remove('ml-2');},50);},
        p_callback_start: function() {startConnectionManagement();},
        p_clone_target: true,
        p_message: `
        <p>Click on <strong>New Connection</strong>.</p>
        `,
        p_next_button: false,
        p_target: function() {var v_target = document.getElementById('button_new_connection'); return v_target},
        p_title: 'Add a New Connection',
        p_update_delay: 1000
      },
      {
        p_message: `
        <p>Select the Terminal technology.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_type'); return v_target},
        p_title: 'Connection Type',
        p_update_delay: 300
      },
      {
        p_message: `
        <p>Type a helpful name for the terminal connection.</p>
        <p>This is used as name reference on many UI areas.</p>
        <p>i.e: Local terminal.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_title'); return v_target},
        p_title: 'Title'
      },
      {
        p_message: `
        <p>The terminal utilizes SSH technology.</p>
        <p>As you can see, in this case SSH parameters are mandatory.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_use_tunnel'); return v_target},
        p_title: 'SSH parameters'
      },
      {
        p_message: `
        <p>Type the ssh server address. Do not include ports.</p>
        <p>i.e:127.0.0.1</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_ssh_server'); return v_target},
        p_title: 'SSH server'
      },
      {
        p_message: `
        <p>Type the port of the SSH server.</p>
        <p>i.e: 22 is a default port for working with SSH tunnels.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_ssh_port'); return v_target},
        p_title: 'SSH Port'
      },
      {
        p_message: `
        <p>Type the name of the SSH user.</p>
        <p>i.e: If you are on linux, your linux user is available for a local connection.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_ssh_user'); return v_target},
        p_title: 'SSH User'
      },
      {
        p_message: `
        <p>If you want you can save the password of your user.</p>
        <p>* Leaving this empty will force the tool to request for your password everytime you open a terminal connection.</p>
        <p>i.e: If you are on linux, your linux user is available for a local connection.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_ssh_password'); return v_target},
        p_title: 'SSH Password (optional)'
      },
      {
        p_message: `
        <p>This is <strong>optional</strong>.</p>
        <p>It allows you to configure a SSH key.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_ssh_key_input_label'); return v_target},
        p_title: 'SSH Key'
      },
      {
        p_message: `
        <p>You may want to hit 'test' before saving the conntion.</p>
        <p>After that, click save.</p>
        `,
        p_target: function() {var v_target = document.getElementById('conn_form_button_test_connection'); return v_target},
        p_title: 'Test the Connection'
      }
    ],
    'snippets': [
      {
        p_clone_target: true,
        p_message: `
        <p>The snippet panel is now accessible globally.</p>
        <p>Please, click on the <i class="fas fa-file-code"></i> button.</p>
        `,
        p_target: document.getElementsByClassName('omnidb__tab-menu omnidb__tab-menu--primary')[0],
        p_title: 'Global Snippet Panel'
      },
      {
        // p_callback_after_update_start: function() {setTimeout(function(){var v_target = document.getElementById(v_connTabControl.snippet_tag.tabControl.selectedTab.tag.editorDivId);},50);},
        p_callback_start: function() {toggleSnippetPanel();},
        p_message: `
        <p>Inside this tab you can create and edit a snippet.</p>
        <p>Go ahead and try to create some simple snippet, i.e:</p>
        <code>WHERE true SELECT 1;</code>
        <p>Then experiment clicking on the <strong>indent button</strong> below the editor, and then <strong>next</strong>.</p>
        `,
        p_next_button: true,
        p_target: function() {var v_target = document.getElementById('a_' + v_connTabControl.snippet_tag.tabControl.selectedTab.tag.tab_id); return v_target},
        p_title: 'Snippets editor',
        p_update_delay: 600
      },
      {
        p_message: `
        <p>As you can see, the identation feature automatically adjusts your code following a pattern.</p>
        <p>Now go ahead and click <strong>save</strong></p>
        `,
        p_next_button: true,
        p_target: function() {var v_target = document.getElementById('a_' + v_connTabControl.snippet_tag.tabControl.selectedTab.tag.tab_id); return v_target},
        p_title: 'Indenting'
      },
      {
        p_message: `
        <p>Every snippet you save is stored under your user.</p>
        <p>The tree on the left allows you to easily access it by double-clicking on the snippet.</p>
        `,
        p_next_button: false,
        p_target: function() {var v_target = document.getElementById(v_connTabControl.snippet_tag.divTree.getAttribute('id')); return v_target},
        p_title: 'Saved Snippets',
        p_update_delay: 600
      }
    ],
    'selecting_connection': [
      {
        p_message: `
        <p>The <strong>outer_tab</strong> contains global panels related to workspace and also access to created connections.</p>
        <ol style="padding-left: 1.5rem;">
          <li class="mb-2">
            To access a connection, click on the <i class="fas fa-plus"></i> button.
          </li>
          <li class="mb-2">
            Navigate to the proper technology on the custom menu.
          </li>
          <li class="mb-2">
            Click on the connection.
          </li>
        </ol>
        <p>Now you can close this walkthrough and open a new connection.</p>
        `,
        p_position: function() {var v_target = v_connTabControl.tabList[v_connTabControl.tabList.length - 1].elementA; return {x:v_target.getBoundingClientRect().x + 40,y:v_target.getBoundingClientRect().y}},
        p_target: function(){var v_target = v_connTabControl.tabList[v_connTabControl.tabList.length - 1].elementA; return v_target;},
        p_title: 'Selecting a Connection'
      }
    ],
    'connection_tab': [
      {
        p_message: `
        <p>This identifies the database you are connected with:</p>
        `,
        p_target: function(){var v_target = v_connTabControl.selectedTab.tag.divDetails; return v_target;},
        p_title: 'Current Connection'
      },
      {
        p_message: `
        <p>This tree is main your access point to this connection.</p>
        <p><strong>How-to</strong>:</p>
        <ul style="padding-left: 1.5rem;">
          <li class="mb-1">
            <strong>Double-click</strong>: expands child nodes based on the database internal structure.
          </li>
          <li class="mb-2">
            <strong>Right-click</strong>: Context menu with actions based on the node type.
          </li>
        </ul>
        `,
        p_target: function(){var v_target = v_connTabControl.selectedTab.tag.divTree; return v_target;},
        p_title: 'Aimara Tree'
      },
      {
        p_message: `
        <p>These tabs provide additional info to the node you interact with in the Aimara Tree.</p>
        <p>Keep in mind that every node interaction that returns this type of info needs to query for consistency.</p>
        <p>To minimize queries, these only run when one of these tabs is visible.</p>
        <p><strong>Recommendation</strong>: Only open the property/ddl when you need to update this info.</p>
        `,
        p_target: function(){var v_target = v_connTabControl.selectedTab.tag.divTreeTabs; return v_target;},
        p_title: 'Properties / DDL'
      },
      {
        p_message: `
        <p>There are two types of inner_tabs available.</p>
        <ol style="padding-left: 1.5rem;">
          <li class="mb-1">
            <strong><i class="fas fa-terminal"></i> Console Tab</strong>: Contains a psql console.
          </li>
          <li class="mb-1">
            <strong>Query Tabs</strong>: These have SQL editors whose commands are executed on the selected database.
          </li>
        </ol>
        <div class="alert-info p-2">Keep in mind that when you run a query from the contextual menu of the Aimara Tree, it will open a new query tab and execute it.</div>
        `,
        p_target: function(){var v_target = v_connTabControl.selectedTab.tag.tabControl.tabList[0].elementA; return v_target;},
        p_title: 'Inner Tabs'
      },
      {
        p_message: `
        <p>These buttons request actions based on the SQL editor and the querying status.</p>
        <p>For example, you can <span class="bg-info rounded px-1 text-white">run</span> a query, <span class="bg-info rounded px-1 text-white">cancel</span> an ongoing query, <span class="bg-info rounded px-1 text-white">fetch more</span>, <span class="bg-info rounded px-1 text-white">explain</span>, <span class="bg-info rounded px-1 text-white">explain analyze</span>.</p>
        <p>If you navigate the Tree on the left to find a table and use the action Query Table from it's context menu, the editor will autofill and the run query will be issued.</p>
        `,
        p_position: function() {var v_target = $(v_connTabControl.selectedTab.tag.tabControl.selectedTab.elementDiv).find('.omnidb__tab-actions')[0]; return {x:v_target.getBoundingClientRect().x + 40,y:v_target.getBoundingClientRect().y}},
        p_target: function(){var v_target = $(v_connTabControl.selectedTab.tag.tabControl.selectedTab.elementDiv).find('.omnidb__tab-actions')[0]; return v_target;},
        p_title: 'Actions Panel'
      },
      {
        p_message: `
        <p>Query returns will fill the area below your screen, even when they return errors.</p>
        <p>After running a query, this area will contain 3 special tabs.</p>
        <ol style="padding-left: 1.5rem;">
          <li class="mb-1">
            <strong>Data</strong>: Contains a table with query results, when successful.
          </li>
          <li class="mb-1">
            <strong>Messages</strong>: Displays error messages.
          </li>
          <li class="mb-1">
            <strong>Explain</strong>: Contains a special component to display explain/explain analyze results.
          </li>
        </ol>
        `,
        p_position: function() {var v_target = v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.div_result; return {x:v_target.getBoundingClientRect().x + 40,y:v_target.getBoundingClientRect().y + 40}},
        p_target: function(){var v_target = $(v_connTabControl.selectedTab.tag.tabControl.selectedTab.tag.divResult).find('.omnidb__tab-actions')[0]; return v_target;},
        p_title: 'Query Result'
      }
    ]
  }
  // Configuring tutorial getting started, changes based on gv_desktopMode
  let v_tutorial_link_creating_user = (gv_desktopMode)
  ? ''
  : `
  <li class="mb-2">
    <button type="button" class="btn btn-primary d-flex align-items-center" onclick="startTutorial('utilities_menu');">
      <i class="fas fa-user-plus mr-2"></i>Create an omnidb user
    </button>
  </li>`;
  v_tutorials.getting_started = [
    {
      p_message:
      '<ol style="padding-left: 1.5rem;">' +
        v_tutorial_link_creating_user +
        `
        <li class="mb-2">
          <button type="button" class="btn btn-primary d-flex align-items-center" onclick="startTutorial('connections_menu');">
            <i class="fas fa-plug mr-2"></i>Create a database connection
          </button>
        </li>
        <li class="mb-2">
          <button type="button" class="btn btn-primary d-flex align-items-center" onclick="startTutorial('terminal_connection');">
            <i class="fas fa-terminal mr-2"></i>Create a terminal connection
          </button>
        </li>
        <li class="mb-2">
          <button type="button" class="btn btn-primary d-flex align-items-center" onclick="startTutorial('snippets');">
            <i class="fas fa-file-code mr-2"></i>Meet the snippets panel
          </button>
        </li>
        <li class="mb-2">
          <button type="button" class="btn btn-primary d-flex align-items-center" onclick="startTutorial('selecting_connection');">
            <i class="fas fa-plus mr-2"></i>Using a connection
          </button>
        </li>
        ` +
        v_button_inner_query +
      '</ol>',
      p_title: '<i class="fas fa-list mr-2"></i> Getting started'
    }
  ];

  // Selecting a tutorial
  var v_steps = v_tutorials[v_tutorial_name];
  // Update the step list with the new walkthrough
  v_omnis.omnis_ui_assistant.updateStepList(v_steps);
  // Go to the first step of the walkthrough
  v_omnis.omnis_ui_assistant.goToStep(0);
}
