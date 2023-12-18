import axios from 'axios'
import ShortUniqueId from 'short-unique-id';

import { terminalReturn } from "./terminal";
import { queryResponseCodes } from "./constants";
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
    .catch((error) => {
      polling_busy = false
      console.log(error)
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
    case parseInt(queryResponseCodes.Pong): {
      websocketPong();
      break;
    }
    case parseInt(queryResponseCodes.SessionMissing): {
      showAlert('Session not found please reload the page.');
      break;
    }
    case parseInt(queryResponseCodes.MessageException): {
      showToast("error", message.v_data);
      break;
    }
    case parseInt(queryResponseCodes.PasswordRequired): {
      if (context) {
        SetAcked(context);
        QueryPasswordRequired(context, message.v_data);
        break;
      }
    }
    case parseInt(queryResponseCodes.QueryAck): {
      if (context) {
        SetAcked(context);
        break;
      }
    }
    case parseInt(queryResponseCodes.QueryResult): {
      if (context) {
        SetAcked(context);
        if(context.simple && context.callback!=null) { //used by schema editor only, dont run any legacy rendering for simple requests
          context.callback(message)
        } else  {
          context.callback(message, context)
        }
        //Remove context
        if (message.v_data.last_block || message.v_error) {
          removeContext(context_code)
        }

      }
      break;
    }
    case parseInt(queryResponseCodes.ConsoleResult): {
      if (context) {
        if (message.v_data.v_last_block || message.v_error) {
          context.callback(message, context)

          //Remove context
          removeContext(context_code);
        }
      }
      break;
    }
    case parseInt(queryResponseCodes.TerminalResult): {
      if (context) {
        terminalReturn(message, context);
      }
      break;
    }
    case parseInt(queryResponseCodes.QueryEditDataResult): {
      if (context) {
        context.callback(message)
        removeContext(context_code);
      }
      break;
    }
    case parseInt(queryResponseCodes.SaveEditDataResult): {
      if (context) {
        context.callback(message)
        removeContext(context_code);
      }
      break;
    }
    case parseInt(queryResponseCodes.DebugResponse): {
      if (context) {
        SetAcked(context);
        debugResponse(message, context);
        if (message.v_data.v_remove_context) {
          removeContext(context_code);
        }
      }
      break;
    }
    case parseInt(queryResponseCodes.RemoveContext): {
      if (context) {
        removeContext(context_code);
      }
      break;
    }
    default: {
      break;
    }
    case parseInt(queryResponseCodes.AdvancedObjectSearchResult): {
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
        p_context.passwordSuccessCallback(p_context)
			},
			function() {
        p_context.passwordFailCalback(p_context)
			},
			p_message
		);
	}
	else if (p_context.tab_tag.mode=='console') {
		showPasswordPrompt(
			p_context.database_index,
			function() {
        p_context.passwordSuccessCallback(p_context)
			},
			function() {
        p_context.passwordFailCalback(p_context.tab_tag)
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
		if (typeof(context) === 'object') {
      let ctx = {
        code: context_code,
        context: context
      }
      createContext(ctx)
      context_code = ctx.code
		}
    // if context code is passed do not create a new context
		else {
			context_code = context;
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
      v_context_code: context_code || 0,
      v_data: message_data
    }
  )
}

function SetAcked(context) {
	if (context)
		context.acked = true;
}

export { createContext, createRequest, SetAcked, removeContext }