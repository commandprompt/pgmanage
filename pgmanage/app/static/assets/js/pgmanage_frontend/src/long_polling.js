import axios from 'axios'
import ShortUniqueId from 'short-unique-id';

import { terminalReturn } from "./terminal";
import { querySQLReturn, cancelSQLTab, querySQL, v_queryResponseCodes } from "./query";
import { consoleReturn, cancelConsoleTab, consoleSQL } from "./console";
import { queryEditDataReturn, saveEditDataReturn, cancelEditDataTab } from "./tree_context_functions/edit_data";
import { debugResponse } from "./debug";
import { showPasswordPrompt } from "./passwords";
import { getCookie } from './ajax_control'
import { showAlert, showToast } from "./notification_control";

const uid = new ShortUniqueId({dictionary: 'alpha_upper', length: 4})

let polling_busy = null;
let request_map = new Map()

// send heartbeat to prevent db session from being terminated by back-end
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

function polling_response(message) {

  let context_code = null;
  let context = null;

  if(message.v_context_code) {
    let entry = request_map.get(message.v_context_code)
    if(entry) {
      context_code = entry.code
      context = entry.context
    }
  }

  switch(message.v_code) {
    case parseInt(v_queryResponseCodes.Pong): {
      websocketPong();
      break;
    }
    case parseInt(v_queryResponseCodes.SessionMissing): {
      showAlert('Session not found please reload the page.');
      break;
    }
    case parseInt(v_queryResponseCodes.MessageException): {
      showToast("error", message.v_data);
      break;
    }
    case parseInt(v_queryResponseCodes.PasswordRequired): {
      if (context) {
        SetAcked(context);
        QueryPasswordRequired(context, message.v_data);
        break;
      }
    }
    case parseInt(v_queryResponseCodes.QueryAck): {
      if (context) {
        SetAcked(context);
        break;
      }
    }
    case parseInt(v_queryResponseCodes.QueryResult): {
      if (context) {
        SetAcked(context);
        if (!message.v_error || message.v_data.v_chunks) {
          context.tab_tag.tempData = context.tab_tag.tempData.concat(message.v_data.v_data);
        }
        if (!message.v_data.v_chunks || message.v_data.v_last_block || message.v_error) {
          message.v_data.v_data = [];
          if(context.simple && context.callback!=null) { //used by schema editor only, dont run any legacy rendering for simple requests
            context.callback(message)
          } else {
            querySQLReturn(message, context);
          }

          //Remove context
          removeContext(context_code);
        }

      }
      break;
    }
    case parseInt(v_queryResponseCodes.ConsoleResult): {
      if (context) {
        context.tab_tag.tempData = context.tab_tag.tempData += message.v_data.v_data;
        if (message.v_data.v_last_block || message.v_error) {
          message.v_data.v_data = [];
          consoleReturn(message, context);
          //Remove context
          removeContext(context_code);
        }
      }
      break;
    }
    case parseInt(v_queryResponseCodes.TerminalResult): {
      if (context) {
        terminalReturn(message, context);
      }
      break;
    }
    case parseInt(v_queryResponseCodes.QueryEditDataResult): {
      if (context) {
        SetAcked(context);
        queryEditDataReturn(message, context);
        removeContext(context_code);
      }
      break;
    }
    case parseInt(v_queryResponseCodes.SaveEditDataResult): {
      if (context) {
        saveEditDataReturn(message, context);
        removeContext(context_code);
      }
      break;
    }
    case parseInt(v_queryResponseCodes.DebugResponse): {
      if (context) {
        SetAcked(context);
        debugResponse(message, context);
        if (message.v_data.v_remove_context) {
          removeContext(context_code);
        }
      }
      break;
    }
    case parseInt(v_queryResponseCodes.RemoveContext): {
      if (context) {
        removeContext(context_code);
      }
      break;
    }
    default: {
      break;
    }
    case parseInt(v_queryResponseCodes.AdvancedObjectSearchResult): {
      if (context) {
        SetAcked(context);
        advancedObjectSearchReturn(message, context);
        removeContext(context_code);
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

function createContext(context) {
  let context_code = uid.seq()
  if(context.code) {
    context_code = context.code
  } else {
    context.code = context_code
  }

  request_map.set(context_code, context)

	return context;
}

function removeContext(context_code) {
  request_map.delete(context_code)
}

function createRequest(message_code, message_data, context) {
  let context_code = undefined;
	if (context != null) {
		// Context code is passed
    // context is sometimes a number, usually it is an object... duh
		if (context === parseInt(context, 10)) {
			context_code = context;
		}
		else {
      let ctx = {
        code: context_code,
        context: context
      }
      createContext(ctx)
		}
	}

  // synchronize call_polling requests, do not run new one when there is a request in progress
  if (polling_busy === null)
    call_polling(true)
  else if (polling_busy === false) {
    call_polling(false)
  }

  axios.post(
    `${app_base_path}/create_request/`,
    {
      v_code: message_code,
      v_context_code: ctx.code,
      v_data: message_data
    }
  )
}

function SetAcked(context) {
	if (context)
		context.acked = true;
}

export { createContext, createRequest, SetAcked, removeContext }