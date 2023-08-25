import axios from 'axios'

import { terminalReturn } from "./terminal";
import { querySQLReturn, cancelSQLTab, querySQL, v_queryResponseCodes } from "./query";
import { consoleReturn, cancelConsoleTab, consoleSQL } from "./console";
import { queryEditDataReturn, saveEditDataReturn, cancelEditDataTab } from "./tree_context_functions/edit_data";
import { debugResponse } from "./debug";
import { showPasswordPrompt } from "./passwords";
import { getCookie, csrfSafeMethod, execAjax } from './ajax_control'
import { showAlert, showToast } from "./notification_control";

let polling_busy = null;

var v_context_object = {
  'contextCode': 0,
  'contextList': []
}

// heartbeat to prevent db session back-end termination
$(function () {
  setInterval(function() {
    axios.get(`${app_base_path}/client_keep_alive/`)
  }, 60000);
});

// notify back-end about session termination
$(window).on('beforeunload', () => {
  const data = new FormData();
  data.append('csrfmiddlewaretoken', getCookie('pgmanage_csrftoken'))
  navigator.sendBeacon(`${app_base_path}/clear_client/`, data)
})

function call_polling(startup) {
    polling_busy = true
    axios.post(
      `${app_base_path}/long_polling/`,
      {p_startup: startup}
    ).then((resp) => {
      polling_busy = false
      resp.data.returning_rows.forEach(chunk => {
        try {
          polling_response(chunk)
        } catch(err) {
          console.log(err)
        }
      });
      call_polling(false)
    })
}

function polling_response(p_message) {
  var v_message = p_message;

  var p_context_code = null;
  var p_context = null;

  if (v_message.v_context_code!=0 && v_message.v_context_code!=null) {

    for (var i=0; i<v_context_object.contextList.length; i++) {

      if (v_context_object.contextList[i].code == v_message.v_context_code) {
        p_context = v_context_object.contextList[i].context;
        p_context_code = v_context_object.contextList[i].code;
        break;
      }
    }
  }

  switch(v_message.v_code) {
    case parseInt(v_queryResponseCodes.Pong): {
      websocketPong();
      break;
    }
    case parseInt(v_queryResponseCodes.SessionMissing): {
      showAlert('Session not found please reload the page.');
      break;
    }
    case parseInt(v_queryResponseCodes.MessageException): {
      showToast("error", p_message.v_data);
      break;
    }
    case parseInt(v_queryResponseCodes.PasswordRequired): {
      if (p_context) {
        SetAcked(p_context);
        QueryPasswordRequired(p_context,v_message.v_data);
        break;
      }
    }
    case parseInt(v_queryResponseCodes.QueryAck): {
      if (p_context) {
        SetAcked(p_context);
        break;
      }
    }
    case parseInt(v_queryResponseCodes.QueryResult): {
      if (p_context) {
        SetAcked(p_context);
        if (!v_message.v_error || v_message.v_data.v_chunks) {
          p_context.tab_tag.tempData = p_context.tab_tag.tempData.concat(v_message.v_data.v_data);
        }
        if (!v_message.v_data.v_chunks || v_message.v_data.v_last_block || v_message.v_error) {
          v_message.v_data.v_data = [];
          if(p_context.simple && p_context.callback!=null) { //used by schema editor only, dont run any legacy rendering for simple requests
            p_context.callback(p_message)
          } else {
            querySQLReturn(v_message,p_context);
          }

          //Remove context
          removeContext(p_context_code);
        }

      }
      break;
    }
    case parseInt(v_queryResponseCodes.ConsoleResult): {
      if (p_context) {
        p_context.tab_tag.tempData = p_context.tab_tag.tempData += v_message.v_data.v_data;
        if (v_message.v_data.v_last_block || v_message.v_error) {
          v_message.v_data.v_data = [];
          consoleReturn(v_message,p_context);
          //Remove context
          removeContext(p_context_code);
        }
      }
      break;
    }
    case parseInt(v_queryResponseCodes.TerminalResult): {
      if (p_context) {
        terminalReturn(v_message,p_context);
      }
      break;
    }
    case parseInt(v_queryResponseCodes.QueryEditDataResult): {
      if (p_context) {
        SetAcked(p_context);
        queryEditDataReturn(v_message,p_context);
        removeContext(p_context_code);
      }
      break;
    }
    case parseInt(v_queryResponseCodes.SaveEditDataResult): {
      if (p_context) {
        saveEditDataReturn(v_message,p_context);
        removeContext(p_context_code);
      }
      break;
    }
    case parseInt(v_queryResponseCodes.DebugResponse): {
      if (p_context) {
        SetAcked(p_context);
        debugResponse(p_message, p_context);
        if (p_message.v_data.v_remove_context) {
          removeContext(p_context_code);
        }
      }
      break;
    }
    case parseInt(v_queryResponseCodes.RemoveContext): {
      if (p_context) {
        removeContext(p_context_code);
      }
      break;
    }
    default: {
      break;
    }
    case parseInt(v_queryResponseCodes.AdvancedObjectSearchResult): {
      if (p_context) {
        SetAcked(p_context);
        advancedObjectSearchReturn(v_message, p_context);
        //Remove context
        removeContext(p_context_code);
      }
      break;
    }
  }
}

function QueryPasswordRequired(p_context, p_message) {
	if (p_context.tab_tag.mode=='query') {
		showPasswordPrompt(
			p_context.database_index,
			function() {
				cancelSQLTab(p_context.tab_tag);
				//querySQL(p_context.mode);
				querySQL(p_context.mode,
								 p_context.all_data,
								 p_context.query,
								 p_context.callback,
								 p_context.log_query,
								 p_context.save_query,
								 p_context.cmd_type,
								 p_context.clear_data,
								 p_context.tab_title);
			},
			function() {
				cancelSQLTab(p_context.tab_tag);
			},
			p_message
		);
	}
	else if (p_context.tab_tag.mode=='edit') {
		showPasswordPrompt(
			p_context.database_index,
			function() {
				cancelEditDataTab(p_context.tab_tag);
				//queryEditData();
			},
			function() {
				cancelEditDataTab(p_context.tab_tag);
			},
			p_message
		);
	}
	else if (p_context.tab_tag.mode=='console') {
		showPasswordPrompt(
			p_context.database_index,
			function() {
				cancelConsoleTab(p_context.tab_tag);
				p_context.tab_tag.editor_input.setValue(p_context.tab_tag.last_command);
        p_context.tab_tag.editor_input.clearSelection();
				consoleSQL(p_context.check_command,
									 p_context.mode);
			},
			function() {
				cancelConsoleTab(p_context.tab_tag);
			},
			p_message
		);
	}
}

function createContext(p_context) {
	v_context_object.contextCode += 1;
	let v_context_code = v_context_object.contextCode;
	p_context.v_context_code = v_context_code;
	var v_context = {
		code: v_context_code,
		context: p_context
	}
	v_context_object.contextList.push(v_context);
	return v_context;
}

function removeContext(p_context_code) {
	for (var i=0; i<v_context_object.contextList.length; i++) {
		if (v_context_object.contextList[i].code == p_context_code) {
			v_context_object.contextList.splice(i,1);
			break;
		}
	}
}

function createRequest(p_messageCode, p_messageData, p_context) {

  var v_context_code = 0;

	//Configuring context
	if (p_context!=null) {

		//Context code is passed
		if (p_context === parseInt(p_context, 10)) {
			v_context_code = p_context;
		}
		else {
			v_context_object.contextCode += 1;
			v_context_code = v_context_object.contextCode;
			p_context.v_context_code = v_context_code;
			var v_context = {
				code: v_context_code,
				context: p_context
			}
			v_context_object.contextList.push(v_context);
		}
	}

  // synchronize call_polling requests, do not run new one when there is a request in progress
  console.log(polling_busy)
  if (polling_busy === null)
    call_polling(true)
  else if (polling_busy === false) {
    call_polling(false)
  }

  execAjax('/create_request/',
			JSON.stringify({
        v_code: p_messageCode,
        v_context_code: v_context_code,
        v_data: p_messageData
      }),
			function(p_return) {
			},
			null,
			'box',
      false);
}

function SetAcked(p_context) {
	if (p_context)
		p_context.acked = true;
}

export { createContext, createRequest, SetAcked, removeContext }