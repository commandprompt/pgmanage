from typing import Optional, Tuple
import io
from django.http import HttpResponse
from django.template import loader
from django.http import JsonResponse
from django.core import serializers
from django.shortcuts import redirect
from datetime import datetime
from math import ceil
import os
import time
import threading
import app.include.Spartacus as Spartacus
import app.include.Spartacus.Database as Database
import app.include.Spartacus.Utils as Utils
import app.include.OmniDatabase as OmniDatabase
from pgmanage.startup import clean_temp_folder

from enum import IntEnum
from datetime import datetime, timezone
from pgmanage import settings
import sys
import sqlparse

import paramiko
import app.include.custom_paramiko_expect as custom_paramiko_expect

from django.contrib.auth.models import User
from app.models.main import *

from app.client_manager import client_manager, Client
from app.utils.decorators import session_required
from app.utils.response_helpers import create_response_template
from app.include.Session import Session

import traceback

import logging
logger = logging.getLogger('app.QueryServer')


class queryModes(IntEnum):
    DATA_OPERATION = 0
    FETCH_MORE = 1
    FETCH_ALL = 2
    COMMIT = 3
    ROLLBACK = 4


class requestType(IntEnum):
  Login          = 0
  Query          = 1
  Execute        = 2
  Script         = 3
  QueryEditData  = 4
  SaveEditData   = 5
  CancelThread   = 6
  Debug          = 7
  CloseTab       = 8
  AdvancedObjectSearch     = 9
  Console        = 10
  Terminal       = 11
  Ping           = 12

class response(IntEnum):
  LoginResult         = 0
  QueryResult         = 1
  QueryEditDataResult = 2
  SaveEditDataResult  = 3
  SessionMissing      = 4
  PasswordRequired    = 5
  QueryAck            = 6
  MessageException    = 7
  DebugResponse       = 8
  RemoveContext       = 9
  AdvancedObjectSearchResult    = 10
  ConsoleResult       = 11
  TerminalResult      = 12
  Pong                = 13

class debugState(IntEnum):
  Initial  = 0
  Starting = 1
  Ready    = 2
  Step     = 3
  Finished = 4
  Cancel   = 5

class StoppableThread(threading.Thread):
    def __init__(self,p1,p2):
        super(StoppableThread, self).__init__(target=p1, args=(self,p2,))
        self.cancel = False
    def stop(self):
        self.cancel = True

import time

def clear_client(request):
    client_manager.clear_client(request.session.session_key)
    return JsonResponse(
    {}
    )

def client_keep_alive(request):
    client = client_manager.get_or_create_client(client_id=request.session.session_key)
    client.last_update = datetime.now()

    return JsonResponse(
    {}
    )

@session_required(use_old_error_format=True, include_session=False)
def long_polling(request):
    startup = request.data['p_startup']

    client_object = client_manager.get_or_create_client(client_id=request.session.session_key)

    if startup:
        try:
            client_object.release_polling_lock()
        except:
            None

    # Acquire client polling lock to read returning data
    client_object.polling_lock.acquire()

    v_returning_data = []

    client_object.returning_data_lock.acquire()

    while len(client_object.returning_data)>0:
        v_returning_data.append(client_object.returning_data.pop(0))

    client_object.release_returning_data_lock()

    return JsonResponse(
    {
        'returning_rows': v_returning_data
    }
    )

def queue_response(client: Client, p_data):

    client.returning_data_lock.acquire()

    client.returning_data.append(p_data)

    try:
        # Attempt to release client polling lock so that the polling thread can read data
        client.release_polling_lock()
    except RuntimeError:
        pass
    client.release_returning_data_lock()

@session_required(use_old_error_format=True)
def create_request(request, session):
    v_return = create_response_template()

    json_object = request.data
    v_code = json_object['v_code']
    v_context_code = json_object['v_context_code']
    v_data = json_object['v_data']

    client_object = client_manager.get_or_create_client(client_id=request.session.session_key)

    # Release lock to avoid dangling ajax polling requests
    try:
        client_object.release_polling_lock()
    except RuntimeError:
        pass

    #Cancel thread
    if v_code == requestType.CancelThread:
        try:
            thread_data = client_object.get_tab(tab_id=v_data.get('tab_id'), conn_tab_id=v_data.get('conn_tab_id'))
            if thread_data:
                if thread_data['type'] == 'advancedobjectsearch':
                    def callback(self):
                        try:
                            self.tag['lock'].acquire()

                            for v_activeConnection in self.tag['activeConnections']:
                                v_activeConnection.Cancel(False)
                        finally:
                            self.tag['lock'].release()

                    thread_data['thread_pool'].stop(p_callback=callback)
                else:
                    thread_data['thread'].stop()
                    thread_data['omnidatabase'].v_connection.Cancel(False)
        except Exception:
            pass

    #Close Tab
    elif v_code == requestType.CloseTab:
        for v_tab_close_data in v_data:
            client_object.close_tab(tab_id=v_tab_close_data.get('tab_id'), conn_tab_id=v_tab_close_data.get('conn_tab_id'))
            #remove from tabs table if db_tab_id is not null
            if v_tab_close_data.get('tab_db_id'):
                try:
                    tab = Tab.objects.get(id=v_tab_close_data.get('tab_db_id'))
                    tab.delete()
                except Exception:
                    pass

    else:

        #Check database prompt timeout
        if v_data['v_db_index']!=None:
            v_timeout = session.DatabaseReachPasswordTimeout(v_data['v_db_index'])
            if v_timeout['timeout']:
                v_return['v_code'] = response.PasswordRequired
                v_return['v_context_code'] = v_context_code
                v_return['v_data'] = v_timeout['message']
                queue_response(client_object,v_return)
                return JsonResponse(
                {}
                )

        if v_code == requestType.Terminal:
            tab_object = client_object.get_tab(conn_tab_id=v_data['v_tab_id'])

            if tab_object is None or not tab_object.get('terminal_transport').is_active():
                tab_object = client_object.create_main_tab(
                conn_tab_id=v_data['v_tab_id'],
                tab={
                    "thread": None,
                    "terminal_object": None
                    }
                )
                start_thread = True

                try:
                    v_conn_object = session.v_databases[v_data['v_ssh_id']]

                    client = paramiko.SSHClient()
                    client.load_system_host_keys()
                    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                    #ssh key provided
                    if v_conn_object['tunnel']['key'].strip() != '':
                        key = paramiko.RSAKey.from_private_key(io.StringIO(v_conn_object['tunnel']['key']), password=v_conn_object['tunnel']['password'])
                        client.connect(hostname=v_conn_object['tunnel']['server'],username=v_conn_object['tunnel']['user'],pkey=key,passphrase=v_conn_object['tunnel']['password'],port=int(v_conn_object['tunnel']['port']))
                    else:
                        client.connect(hostname=v_conn_object['tunnel']['server'],username=v_conn_object['tunnel']['user'],password=v_conn_object['tunnel']['password'],port=int(v_conn_object['tunnel']['port']))

                    transport = client.get_transport()
                    transport.set_keepalive(120)

                    tab_object['terminal_ssh_client'] = client
                    tab_object['terminal_transport'] = transport
                    tab_object['terminal_object'] = custom_paramiko_expect.SSHClientInteraction(client,timeout=60, display=False)
                    tab_object['terminal_object'].send(v_data['v_cmd'])

                    tab_object['terminal_type'] = 'remote'

                except Exception as exc:
                    start_thread = False
                    v_return['v_code'] = response.MessageException
                    v_return['v_context_code'] = v_context_code
                    v_return['v_data'] = str(exc)
                    queue_response(client_object,v_return)

                if start_thread:
                    v_data['v_context_code'] = v_context_code
                    v_data['v_tab_object'] = tab_object
                    v_data['v_client_object'] = client_object
                    v_data['session'] = session
                    t = StoppableThread(thread_terminal,v_data)
                    tab_object['thread'] = t
                    tab_object['type'] = 'terminal'
                    tab_object['tab_id'] = v_data['v_tab_id']
                    t.start()
            else:
                try:
                    tab_object['last_update'] = datetime.now()
                    tab_object['terminal_object'].send(v_data['v_cmd'])
                except OSError:
                    pass

        elif v_code in [
            requestType.Query,
            requestType.QueryEditData,
            requestType.SaveEditData,
            requestType.AdvancedObjectSearch,
            requestType.Console
            ]:
            #create tab object if it doesn't exist
            tab_object = client_object.get_tab(conn_tab_id=v_data['v_conn_tab_id'],
                                                tab_id=v_data['v_tab_id'])
            if tab_object is None:
                tab_object = client_object.create_tab(
                    conn_tab_id=v_data['v_conn_tab_id'],
                    tab_id=v_data['v_tab_id'],
                    tab={
                        'thread': None,
                        'omnidatabase': None,
                        'inserted_tab': False
                    }

                )

            try:
                client_object.get_tab_database(session=session,
                                                        tab=tab_object,
                                                        conn_tab_id=v_data['v_conn_tab_id'],
                                                        database_index=v_data['v_db_index'],
                                                        attempt_to_open_connection=True,
                                                        current_database=v_data.get('database_name'))
            except Exception as exc:
                v_return['v_code'] = response.PasswordRequired
                v_return['v_context_code'] = v_context_code
                v_return['v_data'] = str(exc)
                queue_response(client_object,v_return)
                return JsonResponse(
                {}
                )

            v_data['v_context_code'] = v_context_code
            v_data['v_database'] = tab_object['omnidatabase']
            v_data['v_client_object'] = client_object
            v_data['session'] = session
            #Query request
            if v_code == requestType.Query:
                tab_object['tab_db_id'] = v_data['tab_db_id']
                v_data['tab_object'] = tab_object
                t = StoppableThread(thread_query,v_data)
                tab_object['thread'] = t
                tab_object['type'] = 'query'
                tab_object['sql_cmd'] = v_data['sql_cmd']
                tab_object['sql_save'] = v_data['sql_save']
                tab_object['tab_id'] = v_data['v_tab_id']
                #t.setDaemon(True)
                t.start()

            #Console request
            elif v_code == requestType.Console:
                v_data['v_tab_object'] = tab_object
                t = StoppableThread(thread_console,v_data)
                tab_object['thread'] = t
                tab_object['type'] = 'console'
                tab_object['sql_cmd'] = v_data['v_sql_cmd']
                tab_object['tab_id'] = v_data['v_tab_id']
                #t.setDaemon(True)
                t.start()

            #Query edit data
            elif v_code == requestType.QueryEditData:
                t = StoppableThread(thread_query_edit_data,v_data)
                tab_object['thread'] = t
                tab_object['type'] = 'edit'
                #t.setDaemon(True)
                t.start()

            #Save edit data
            elif v_code == requestType.SaveEditData:
                t = StoppableThread(thread_save_edit_data,v_data)
                tab_object['thread'] = t
                tab_object['type'] = 'edit'
                #t.setDaemon(True)
                t.start()

        #Debugger
        elif v_code == requestType.Debug:

            #create tab object if it doesn't exist
            tab_object = client_object.get_tab(conn_tab_id=v_data.get('v_conn_tab_id'),
                                                tab_id=v_data.get('v_tab_id'))

            if tab_object is None:
                tab_object = client_object.create_tab(
                    conn_tab_id=v_data.get('v_conn_tab_id'),
                    tab_id=v_data.get('v_tab_id'),
                    tab={
                        "thread": None,
                        'omnidatabase_debug': None,
                        'omnidatabase_control': None,
                        'port': None,
                        'debug_pid': -1,
                        'cancelled': False,
                        'tab_id': v_data['v_tab_id'],
                        'type': 'debug'
                    }

                )

            #New debugger, create connections
            if v_data['v_state'] == debugState.Starting:
                try:
                    v_conn_tab_connection = session.v_databases[v_data['v_db_index']]['database']

                    v_database_debug = OmniDatabase.Generic.InstantiateDatabase(
                        v_conn_tab_connection.v_db_type,
                        v_conn_tab_connection.v_connection.v_host,
                        str(v_conn_tab_connection.v_connection.v_port),
                        v_conn_tab_connection.v_active_service,
                        v_conn_tab_connection.v_active_user,
                        v_conn_tab_connection.v_connection.v_password,
                        v_conn_tab_connection.v_conn_id,
                        v_conn_tab_connection.v_alias,
                        p_conn_string = v_conn_tab_connection.v_conn_string,
                        p_parse_conn_string = False
                    )
                    v_database_control = OmniDatabase.Generic.InstantiateDatabase(
                        v_conn_tab_connection.v_db_type,
                        v_conn_tab_connection.v_connection.v_host,
                        str(v_conn_tab_connection.v_connection.v_port),
                        v_conn_tab_connection.v_active_service,
                        v_conn_tab_connection.v_active_user,
                        v_conn_tab_connection.v_connection.v_password,
                        v_conn_tab_connection.v_conn_id,
                        v_conn_tab_connection.v_alias,
                        p_conn_string = v_conn_tab_connection.v_conn_string,
                        p_parse_conn_string = False
                    )
                    tab_object['omnidatabase_debug'] = v_database_debug
                    tab_object['cancelled'] = False
                    tab_object['omnidatabase_control'] = v_database_control
                    tab_object['port'] = v_database_debug.v_connection.ExecuteScalar('show port')
                except Exception as exc:
                    logger.error('''*** Exception ***\n{0}'''.format(traceback.format_exc()))

                    v_response = {}
                    v_response['v_context_code'] = v_context_code
                    v_response['v_code'] = response.MessageException
                    v_response['v_data'] = traceback.format_exc().replace('\n','<br>')
                    queue_response(client_object,v_response)

            v_data['v_context_code'] = v_context_code
            v_data['v_tab_object'] = tab_object
            v_data['v_client_object'] = client_object

            t = StoppableThread(thread_debug,v_data)
            tab_object['thread'] = t
            #t.setDaemon(True)
            t.start()

    return JsonResponse(
    {}
    )

def thread_debug(self,args):
    v_response = {
        'v_code': -1,
        'v_context_code': args['v_context_code'],
        'v_error': False,
        'v_data': 1
    }
    v_state = args['v_state']
    v_tab_id = args['v_tab_id']
    v_tab_object = args['v_tab_object']
    v_client_object  = args['v_client_object']
    v_database_debug = v_tab_object['omnidatabase_debug']
    v_database_control = v_tab_object['omnidatabase_control']

    try:

        if v_state == debugState.Starting:

            #Start debugger and return ready state
            v_database_debug.v_connection.Open()
            v_database_control.v_connection.Open()

            #Cleaning contexts table
            v_database_debug.v_connection.Execute('delete from omnidb.contexts t where t.pid not in (select pid from pg_stat_activity where pid = t.pid)')

            connections_details = v_database_debug.v_connection.Query('select pg_backend_pid()',True)
            pid = connections_details.Rows[0][0]

            v_database_debug.v_connection.Execute('insert into omnidb.contexts (pid, function, hook, lineno, stmttype, breakpoint, finished) values ({0}, null, null, null, null, 0, false)'.format(pid))

            #lock row for current pid
            v_database_control.v_connection.Execute('select pg_advisory_lock({0}) from omnidb.contexts where pid = {0}'.format(pid))

            #updating pid and port in tab object
            v_tab_object['debug_pid'] = pid

            #Run thread that will execute the function
            t = StoppableThread(thread_debug_run_func,{ 'v_tab_object': v_tab_object, 'v_context_code': args['v_context_code'], 'v_function': args['v_function'], 'v_type': args['v_type'], 'v_client_object': v_client_object})
            v_tab_object['thread'] = t
            #t.setDaemon(True)
            t.start()

            #ws_object.v_list_tab_objects[v_tab_id] = v_tab_object

            v_lineno = None
            #wait for context to be ready or thread ends
            while v_lineno == None and t.isAlive():
                time.sleep(0.5)
                v_lineno = v_database_control.v_connection.ExecuteScalar('select lineno from omnidb.contexts where pid = {0} and lineno is not null'.format(pid))

            # Function ended instantly
            if not t.isAlive():
                v_database_control.v_connection.Close()
            else:
                v_variables = v_database_control.v_connection.Query('select name,attribute,vartype,value from omnidb.variables where pid = {0}'.format(pid),True)

                v_response['v_code'] = response.DebugResponse
                v_response['v_data'] = {
                'v_state': debugState.Ready,
                'v_remove_context': False,
                'v_variables': v_variables.Rows,
                'v_lineno': v_lineno
                }
                queue_response(v_client_object,v_response)

        elif v_state == debugState.Step:

            v_database_control.v_connection.Execute('update omnidb.contexts set breakpoint = {0} where pid = {1}'.format(args['v_next_breakpoint'],v_tab_object['debug_pid']))

            try:
                v_database_control.v_connection.Execute('select pg_advisory_unlock({0}) from omnidb.contexts where pid = {0}; select pg_advisory_lock({0}) from omnidb.contexts where pid = {0};'.format(v_tab_object['debug_pid']))

                #acquired the lock, get variables and lineno
                v_variables = v_database_control.v_connection.Query('select name,attribute,vartype,value from omnidb.variables where pid = {0}'.format(v_tab_object['debug_pid']),True)
                v_context_data = v_database_control.v_connection.Query('select lineno,finished from omnidb.contexts where pid = {0}'.format(v_tab_object['debug_pid']),True)

                #not last statement
                if (v_context_data.Rows[0][1]!='True'):
                    v_response['v_code'] = response.DebugResponse
                    v_response['v_data'] = {
                    'v_state': debugState.Ready,
                    'v_remove_context': True,
                    'v_variables': v_variables.Rows,
                    'v_lineno': v_context_data.Rows[0][0]
                    }
                    queue_response(v_client_object,v_response)
                else:
                    v_database_control.v_connection.Execute('select pg_advisory_unlock({0}) from omnidb.contexts where pid = {0};'.format(v_tab_object['debug_pid']))
                    v_database_control.v_connection.Close()
                    v_response['v_code'] = response.RemoveContext
                    queue_response(v_client_object,v_response)

            except Exception:
                v_response['v_code'] = response.RemoveContext
                queue_response(v_client_object,v_response)

        #Cancelling debugger, the thread executing the function will return the cancel status
        elif v_state == debugState.Cancel:
            v_tab_object['cancelled'] = True
            v_database_control.v_connection.Cancel(False)
            v_database_control.v_connection.Terminate(v_tab_object['debug_pid'])
            v_database_control.v_connection.Close()

    except Exception as exc:
        v_response['v_code'] = response.DebugResponse
        v_response['v_data'] = {
            'v_state': debugState.Finished,
            'v_remove_context': True,
            'v_error': True,
            'v_error_msg': str(exc)
        }

        try:
            v_database_debug.v_connection.Close()
            v_database_control.v_connection.Close()
        except Exception:
            None

        queue_response(v_client_object,v_response)

def thread_debug_run_func(self,args):
    v_response = {
        'v_code': -1,
        'v_context_code': args['v_context_code'],
        'v_error': False,
        'v_data': 1
    }
    v_tab_object = args['v_tab_object']
    v_client_object  = args['v_client_object']
    v_database_debug = v_tab_object['omnidatabase_debug']
    v_database_control = v_tab_object['omnidatabase_control']

    try:
        #enable debugger for current connection
        v_conn_string = "host=''localhost'' port={0} dbname=''{1}'' user=''{2}''".format(v_tab_object['port'],v_database_debug.v_service,v_database_debug.v_user);

        v_database_debug.v_connection.Execute("select omnidb.omnidb_enable_debugger('{0}')".format(v_conn_string))

        #run function it will lock until the function ends
        if args['v_type'] == 'f':
            v_func_return = v_database_debug.v_connection.Query('select * from {0} limit 1000'.format(args['v_function']),True)
        else:
            v_func_return = v_database_debug.v_connection.Query('call {0}'.format(args['v_function']),True)

        #Not cancelled, return all data
        if not v_tab_object['cancelled']:

            #retrieve variables
            v_variables = v_database_debug.v_connection.Query('select name,attribute,vartype,value from omnidb.variables where pid = {0}'.format(v_tab_object['debug_pid']),True)

            #retrieve statistics
            v_statistics = v_database_debug.v_connection.Query('select lineno,coalesce(trunc((extract("epoch" from tend)  - extract("epoch" from tstart))::numeric,4),0) as msec from omnidb.statistics where pid = {0} order by step'.format(v_tab_object['debug_pid']),True)

            #retrieve statistics summary
            v_statistics_summary = v_database_debug.v_connection.Query('''
            select lineno, max(msec) as msec
            from (select lineno,coalesce(trunc((extract("epoch" from tend) - extract("epoch" from tstart))::numeric,4),0) as msec from omnidb.statistics where pid = {0}) t
            group by lineno
            order by lineno
            '''.format(v_tab_object['debug_pid']),True)

            #retrieve notices
            v_notices = v_database_debug.v_connection.GetNotices()
            v_notices_text = ''
            if len(v_notices) > 0:
                for v_notice in v_notices:
                    v_notices_text += v_notice.replace('\n','<br/>')

            v_response['v_data'] = {
                'v_state': debugState.Finished,
                'v_remove_context': True,
                'v_result_rows': v_func_return.Rows,
                'v_result_columns': v_func_return.Columns,
                'v_result_statistics': v_statistics.Rows,
                'v_result_statistics_summary': v_statistics_summary.Rows,
                'v_result_notices': v_notices_text,
                'v_result_notices_length': len(v_notices),
                'v_variables': v_variables.Rows,
                'v_error': False
            }

            v_database_debug.v_connection.Close()

            #send debugger finished message
            v_response['v_code'] = response.DebugResponse

            queue_response(v_client_object,v_response)
        #Cancelled, return cancelled status
        else:
            v_response['v_code'] = response.DebugResponse
            v_response['v_data'] = {
                'v_state': debugState.Cancel,
                'v_remove_context': True,
                'v_error': False
            }
            queue_response(v_client_object,v_response)

    except Exception as exc:
        #Not cancelled
        if not v_tab_object['cancelled']:
            v_response['v_code'] = response.DebugResponse
            v_response['v_data'] = {
                'v_state': debugState.Finished,
                'v_remove_context': True,
                'v_error': True,
                'v_error_msg': str(exc)
            }
            try:
                v_database_debug.v_connection.Close()
            except Exception:
                None
            try:
                v_database_control.v_connection.Close()
            except Exception:
                None

            queue_response(v_client_object,v_response)
        else:
            v_response['v_code'] = response.DebugResponse
            v_response['v_data'] = {
                'v_state': debugState.Cancel,
                'v_remove_context': True,
                'v_error': False
            }
            queue_response(v_client_object,v_response)

def GetDuration(p_start, p_end):
    duration = ''
    time_diff = p_end - p_start
    if time_diff.days==0 and time_diff.seconds==0:
        duration = str(time_diff.microseconds/1000) + ' ms'
    else:
        days, seconds = time_diff.days, time_diff.seconds
        hours = days * 24 + seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        duration = '{0}:{1}:{2}'.format("%02d" % (hours,),"%02d" % (minutes,),"%02d" % (seconds,))

    return duration

def LogHistory(p_user_id,
               p_user_name,
               p_sql,
               p_start,
               p_end,
               p_duration,
               p_status,
               p_conn_id,
               database):

    try:

        query_object = QueryHistory(
            user=User.objects.get(id=p_user_id),
            connection=Connection.objects.get(id=p_conn_id),
            start_time=p_start,
            end_time=p_end,
            duration=p_duration,
            status=p_status,
            snippet=p_sql,
            database=database
        )
        query_object.save()
    except Exception as exc:
        logger.error('''*** Exception ***\n{0}'''.format(traceback.format_exc()))

def thread_terminal(self,args):

    try:
        v_cmd             = args['v_cmd']
        v_tab_id          = args['v_tab_id']
        v_tab_object      = args['v_tab_object']
        v_terminal_object = v_tab_object['terminal_object']
        v_terminal_ssh_client = v_tab_object['terminal_ssh_client']
        v_client_object  = args['v_client_object']

        while not self.cancel:
            try:
                if v_tab_object['terminal_type'] == 'local':
                    v_data_return = v_terminal_object.read_nonblocking(size=1024)
                else:
                    v_data_return = v_terminal_object.read_current()

                #send data in chunks to avoid blocking the websocket server
                chunks = [v_data_return[x:x+10000] for x in range(0, len(v_data_return), 10000)]

                if len(chunks)>0:
                    for count in range(0,len(chunks)):
                        if self.cancel:
                            break

                        v_response = {
                            'v_code': response.TerminalResult,
                            'v_context_code': args['v_context_code'],
                            'v_error': False,
                            'v_data': 1
                        }

                        if not count==len(chunks)-1:
                            v_response['v_data'] = {
                                'v_data' : chunks[count],
                                'v_last_block': False
                            }
                        else:
                            v_response['v_data'] = {
                                'v_data' : chunks[count],
                                'v_last_block': True
                            }
                        if not self.cancel:
                            queue_response(v_client_object,v_response)
                else:
                    if not self.cancel:
                        queue_response(v_client_object,v_response)

            except Exception as exc:
                transport = v_terminal_ssh_client.get_transport()
                if transport == None or transport.is_active() == False:
                    break
                if 'EOF' in str(exc):
                    break


    except Exception as exc:
        logger.error('''*** Exception ***\n{0}'''.format(traceback.format_exc()))
        v_response['v_data'] = {
            'v_data': str(exc),
            'v_duration': ''
        }
        if not self.cancel:
            queue_response(v_client_object,v_response)


def thread_query(self, args):
    response_data = {
        'v_code': response.QueryResult,
        'v_context_code': args['v_context_code'],
        'v_error': False,
        'v_data': 1
    }

    try:
        sql_cmd: str = args.get('sql_cmd')
        cmd_type: Optional[str] = args.get('cmd_type')
        tab_object: dict = args.get('tab_object')
        mode: queryModes = args.get('mode')
        all_data: bool = args.get('all_data')
        log_query: bool = args.get('log_query')
        tab_title: str = args.get('tab_title')
        autocommit: bool = args.get('autocommit')
        client_object: Client = args.get('client_object') or args.get('v_client_object')

        session: Session = args.get('session')
        database = args.get('database') or args.get('v_database')

        log_start_time = datetime.now(timezone.utc)
        log_status = 'success'

        inserted_id = None
        if not tab_object.get('tab_db_id') and not tab_object.get('inserted_tab') and log_query:
            db_tab = Tab(
                user=User.objects.get(id=session.v_user_id),
                connection=Connection.objects.get(id=database.v_conn_id),
                title=tab_title,
                snippet = tab_object.get('sql_save'),
                database=database.v_active_service
            )
            db_tab.save()
            inserted_id = db_tab.id
            tab_object['inserted_tab'] = True

        log_end_time = datetime.now(timezone.utc)
        duration = GetDuration(log_start_time, log_end_time)

        if cmd_type in ['export_csv','export_xlsx', 'export_csv-no_headers', 'export_xlsx-no_headers']:
            file_name, extension = export_data(sql_cmd=sql_cmd, database=database, encoding=session.v_csv_encoding, delimiter=session.v_csv_delimiter, cmd_type=cmd_type)

            log_end_time = datetime.now(timezone.utc)
            duration = GetDuration(log_start_time, log_end_time)

            response_data['v_data'] = {
                'file_name': f"{settings.PATH}/static/temp/{file_name}",
                'download_name': f"pgmanage_exported-{log_end_time}.{extension}",
                'duration': duration,
                'inserted_id': inserted_id,
                'con_status': database.v_connection.GetConStatus(),
                'chunks': False
            }

            if not self.cancel:
                queue_response(client_object, response_data)
        else:
            if mode == queryModes.DATA_OPERATION:
                database.v_connection.v_autocommit = autocommit
                if not database.v_connection.v_con or database.v_connection.GetConStatus() == 0:
                    database.v_connection.Open()
                else:
                    database.v_connection.v_start = True

            if mode in (queryModes.DATA_OPERATION, queryModes.FETCH_MORE) and not all_data:
                data = database.v_connection.QueryBlock(sql_cmd, 50, True, True)

                notices = database.v_connection.GetNotices()[:]

                database.v_connection.ClearNotices()

                log_end_time = datetime.now(timezone.utc)
                duration = GetDuration(log_start_time, log_end_time)


                response_data['v_data'] = {
                    'col_names': data.Columns,
                    'data': data.Rows,
                    'last_block': True,
                    'duration': duration,
                    'notices': notices,
                    'inserted_id': inserted_id,
                    'status': database.v_connection.GetStatus(),
                    'con_status': database.v_connection.GetConStatus(),
                    'chunks': True
                }

                if not self.cancel:
                    queue_response(client_object, response_data)
            elif mode == queryModes.FETCH_ALL or all_data:
                has_more_records = True

                while has_more_records:

                    data = database.v_connection.QueryBlock(sql_cmd, 10000, True, True)

                    notices = database.v_connection.GetNotices()

                    database.v_connection.ClearNotices()

                    log_end_time = datetime.now(timezone.utc)

                    duration = GetDuration(log_start_time, log_end_time)

                    response_data['v_data'] = {
                        'col_names': data.Columns,
                        'data': data.Rows,
                        'last_block': False,
                        'duration': duration,
                        'notices': notices,
                        'inserted_id': inserted_id,
                        'status': '',
                        'con_status': 0,
                        'chunks': True
                    }

                    if database.v_connection.v_start:
                        has_more_records = False
                    elif len(data.Rows) > 0:
                        has_more_records = True
                    else:
                        has_more_records = False


                    if self.cancel:
                            break
                    if has_more_records:
                        queue_response(client_object, response_data)

                if not self.cancel:

                    notices = database.v_connection.GetNotices()

                    log_end_time = datetime.now(timezone.utc)
                    duration = GetDuration(log_start_time, log_end_time)

                    response_data['v_data'] = {
                        'col_names': data.Columns,
                        'data': data.Rows,
                        'last_block': True,
                        'duration': duration,
                        'notices': notices,
                        'inserted_id': inserted_id,
                        'status': database.v_connection.GetStatus(),
                        'con_status': database.v_connection.GetConStatus(),
                        'chunks': True
                    }
                    queue_response(client_object, response_data)
            elif mode in (queryModes.COMMIT, queryModes.ROLLBACK):
                duration = GetDuration(log_start_time, log_end_time)

                if mode == queryModes.COMMIT:
                    database.v_connection.Query('COMMIT;', True)
                else:
                    database.v_connection.Query('ROLLBACK;', True)

                response_data['v_data'] = {
                    'col_names': None,
                    'data': [],
                    'last_block': True,
                    'duration': duration,
                    'notices': [],
                    'inserted_id': inserted_id,
                    'status': database.v_connection.GetStatus(),
                    'con_status': database.v_connection.GetConStatus(),
                    'chunks': False
                }
                queue_response(client_object, response_data)
    except Exception as exc:
        if not self.cancel:
            notices = database.v_connection.GetNotices()

            log_end_time = datetime.now(timezone.utc)
            duration = GetDuration(log_start_time, log_end_time)

            log_status = "error" # ????

            response_data['v_data'] = {
                'position': database.GetErrorPosition(str(exc)),
                'message': str(exc),
                'duration': duration,
                'notices': notices,
                'inserted_id': inserted_id,
                'status': 0,
                'con_status': database.v_connection.GetConStatus(),
                'chunks': False
            }

            response_data['v_error'] = True

            queue_response(client_object, response_data)

    if mode == queryModes.DATA_OPERATION and log_query:
        LogHistory(p_user_id=session.v_user_id,
                   p_user_name=session.v_user_name,
                   p_sql=sql_cmd,
                   p_start=log_start_time,
                   p_end=log_end_time,
                   p_duration=duration,
                   p_status=log_status,
                   p_conn_id=database.v_conn_id,
                   database=database.v_active_service)


    if mode == queryModes.DATA_OPERATION and tab_object.get('tab_db_id') and log_query:
        tab = Tab.objects.filter(id=tab_object.get('tab_db_id')).first()
        if tab:
            tab.snippet = tab_object.get('sql_save')
            tab.title = tab_title
            tab.save()


def export_data(sql_cmd: str, database, encoding: str, delimiter: str, cmd_type: str) -> Tuple[str, str]:
    skip_headers = False
    #cleaning temp folder
    clean_temp_folder()

    if len(cmd_type.split('-')) == 2:
        cmd_type = cmd_type.split('-')[0]
        skip_headers = True

    if cmd_type=='export_csv':
        extension = 'csv'
    else:
        extension = 'xlsx'

    export_dir = settings.TEMP_DIR

    if not os.path.exists(export_dir):
        os.makedirs(export_dir)

    database.v_connection.Open()

    file_name = f'${str(time.time()).replace(".", "_")}.{extension}'

    data = database.v_connection.QueryBlock(sql_cmd, 1000, False, True)

    file_path = os.path.join(export_dir, file_name)

    file = Utils.DataFileWriter(file_path, data.Columns, encoding, delimiter, skip_headers=skip_headers)

    file.Open()

    if database.v_connection.v_start:
        file.Write(data)
        has_more_records = False
    elif len(data.Rows) > 0:
        file.Write(data)
        has_more_records = True
    else:
        has_more_records = False

    while has_more_records:
        data = database.v_connection.QueryBlock(sql_cmd, 1000, False, True)

        if database.v_connection.v_start:
            file.Write(data)
            has_more_records = False
        elif len(data.Rows) > 0:
            file.Write(data)
            has_more_records = True
        else:
            has_more_records = False
    database.v_connection.Close()

    file.Flush()

    return file_name, extension


def thread_console(self,args):
    v_response = {
        'v_code': response.ConsoleResult,
        'v_context_code': args['v_context_code'],
        'v_error': False,
        'v_data': 1
    }

    try:
        v_sql            = args['v_sql_cmd']
        v_tab_id         = args['v_tab_id']
        v_tab_object     = args['v_tab_object']
        v_autocommit     = args['v_autocommit']
        v_mode           = args['v_mode']
        v_client_object  = args['v_client_object']

        session = args['session']
        v_database = args['v_database']

        #Removing last character if it is a semi-colon
        if v_sql[-1:]==';':
            v_sql = v_sql[:-1]

        log_start_time = datetime.now(timezone.utc)
        log_status = 'success'

        try:
            list_sql = sqlparse.split(v_sql)

            v_data_return = ''
            run_command_list = True

            if v_mode==0:
                v_database.v_connection.v_autocommit = v_autocommit
                if not v_database.v_connection.v_con or v_database.v_connection.GetConStatus() == 0:
                    v_database.v_connection.Open()
                else:
                    v_database.v_connection.v_start=True

            if v_mode == 1 or v_mode ==2:
                v_table = v_database.v_connection.QueryBlock('', 50, True, True)
                #need to stop again
                if not v_database.v_connection.v_start or len(v_table.Rows)>=50:
                    v_data_return += '\n' + v_table.Pretty(v_database.v_connection.v_expanded) + '\n' + v_database.v_connection.GetStatus()
                    run_command_list = False
                    v_show_fetch_button = True
                else:
                    v_data_return += '\n' + v_table.Pretty(v_database.v_connection.v_expanded) + '\n' + v_database.v_connection.GetStatus()
                    run_command_list = True
                    list_sql = v_tab_object['remaining_commands']

            if v_mode == 3:
                run_command_list = True
                list_sql = v_tab_object['remaining_commands']

            if run_command_list:
                counter = 0
                v_show_fetch_button = False
                for sql in list_sql:
                    counter = counter + 1
                    try:
                        formated_sql = sql.strip()
                        v_data_return += '\n' + v_database.v_active_service + '=# ' + formated_sql + '\n'

                        v_database.v_connection.ClearNotices()
                        v_database.v_connection.v_start=True
                        v_data1 = v_database.v_connection.Special(sql)

                        v_notices = v_database.v_connection.GetNotices()
                        v_notices_text = ''
                        if len(v_notices) > 0:
                            for v_notice in v_notices:
                                v_notices_text += v_notice
                            v_data_return += v_notices_text

                        v_data_return += v_data1

                        if v_database.v_use_server_cursor:
                            if v_database.v_connection.v_last_fetched_size == 50:
                                v_tab_object['remaining_commands'] = list_sql[counter:]
                                v_show_fetch_button = True
                                break
                    except Exception as exc:
                        try:
                            v_notices = v_database.v_connection.GetNotices()
                            v_notices_text = ''
                            if len(v_notices) > 0:
                                for v_notice in v_notices:
                                    v_notices_text += v_notice
                                v_data_return += v_notices_text
                        except Exception as exc:
                            None
                        v_response['v_error'] = True
                        v_data_return += str(exc)
                    v_tab_object['remaining_commands'] = []

            log_end_time = datetime.now(timezone.utc)
            v_duration = GetDuration(log_start_time,log_end_time)

            v_data_return = v_data_return.replace("\n","\r\n")

            v_response['v_data'] = {
                'v_data' : v_data_return,
                'v_last_block': True,
                'v_duration': v_duration,
                'v_con_status': v_database.v_connection.GetConStatus(),
            }

            #send data in chunks to avoid blocking the websocket server
            chunks = [v_data_return[x:x+10000] for x in range(0, len(v_data_return), 10000)]
            if len(chunks)>0:
                for count in range(0,len(chunks)):
                    if self.cancel:
                        break
                    if not count==len(chunks)-1:
                        v_response['v_data'] = {
                            'v_data' : chunks[count],
                            'v_last_block': False,
                            'v_duration': v_duration,
                            'v_show_fetch_button': v_show_fetch_button,
                            'v_con_status': '',
                        }
                    else:
                        v_response['v_data'] = {
                            'v_data' : chunks[count],
                            'v_last_block': True,
                            'v_duration': v_duration,
                            'v_show_fetch_button': v_show_fetch_button,
                            'v_con_status': v_database.v_connection.GetConStatus(),
                            'v_status': v_database.v_connection.GetStatus(),
                        }
                    if not self.cancel:
                        queue_response(v_client_object,v_response)
            else:
                if not self.cancel:
                    queue_response(v_client_object,v_response)

            try:
                v_database.v_connection.ClearNotices()
            except Exception:
                None
        except Exception as exc:
            #try:
            #    v_database.v_connection.Close()
            #except:
            #    pass
            log_end_time = datetime.now(timezone.utc)
            v_duration = GetDuration(log_start_time,log_end_time)
            log_status = 'error'
            v_response['v_error'] = True
            v_response['v_data'] = {
                'v_data': str(exc),
                'v_duration': v_duration
            }

            if not self.cancel:
                queue_response(v_client_object,v_response)

        if v_mode == 0:
            #logging to console history
            query_object = ConsoleHistory(
                user=User.objects.get(id=session.v_user_id),
                connection=Connection.objects.get(id=v_database.v_conn_id),
                start_time=datetime.now(timezone.utc),
                snippet=v_sql.replace("'","''"),
                database=v_database.v_active_service
            )

            query_object.save()


    except Exception as exc:
        logger.error('''*** Exception ***\n{0}'''.format(traceback.format_exc()))
        v_response['v_error'] = True
        v_response['v_data'] = {
            'v_data': str(exc),
            'v_duration': ''
        }
        if not self.cancel:
            queue_response(v_client_object,v_response)


def thread_query_edit_data(self,args):
    res = {
        'v_code': response.QueryEditDataResult,
        'v_context_code': args['v_context_code'],
        'v_error': False,
        'v_data': {
            'rows' : [],
        }
    }

    try:
        database = args['v_database']
        table          = args['v_table']
        schema         = args['v_schema']
        filter         = args['v_filter']
        count          = str(args['v_count'])
        client_object  = args['v_client_object']

        try:
            if database.v_has_schema:
                table_data = database.QueryTableRecords('*', table, schema, filter, count)
            else:
                table_data = database.QueryTableRecords('*', table, filter, count)

            table_rows = []
            for row in table_data.Rows:
                row_data = []

                for col in table_data.Columns:
                    if row[col] == None:
                        row_data.append(None)
                    else:
                        row_data.append(str(row[col]))
                table_rows.append(row_data)
            res['v_data']['rows'] = table_rows

        except Exception as exc:
            res['v_data'] = str(exc)
            res['v_error'] = True

        if not self.cancel:
            queue_response(client_object,res)
    except Exception as exc:
        logger.error('''*** Exception ***\n{0}'''.format(traceback.format_exc()))
        res['v_error'] = True
        res['v_data'] = traceback.format_exc().replace('\n','<br>')
        if not self.cancel:
            queue_response(client_object,res)


def thread_save_edit_data(self,args):
    res = {
        'v_code': response.SaveEditDataResult,
        'v_context_code': args['v_context_code'],
        'v_error': False,
        'v_data': []
    }

    try:
        database = args['v_database']
        client_object  = args['v_client_object']
        command = args['v_sql_cmd']
        #TODO: add transaction support
        #TODO: run each statement separately
        database.v_connection.Execute(command)

        if not self.cancel:
            queue_response(client_object, res)
    except Exception as exc:
        logger.error('''*** Exception ***\n{0}'''.format(traceback.format_exc()))
        res['v_error'] = True
        res['v_data'] = str(exc)
        if not self.cancel:
            queue_response(client_object, res)
