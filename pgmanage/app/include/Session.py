import io
import logging
from collections import OrderedDict
from datetime import datetime, timedelta

import paramiko
from app.include import OmniDatabase
from app.models.main import Connection, UserDetails
from app.utils.crypto import decrypt
from app.utils.key_manager import key_manager
from django.contrib.sessions.backends.db import SessionStore
from sshtunnel import SSHTunnelForwarder

from pgmanage import settings

logger = logging.getLogger("app.Session")

import threading

from django.db.models import Q

tunnels = dict([])

tunnel_locks = dict([])

"""
------------------------------------------------------------------------
Session
------------------------------------------------------------------------
"""


class Session(object):
    def __init__(self,
                p_user_id,
                p_user_name,
                p_theme,
                p_font_size,
                p_super_user,
                p_user_key,
                p_csv_encoding,
                p_csv_delimiter):
        self.v_user_id = p_user_id
        self.v_user_name = p_user_name
        self.v_theme = p_theme
        self.v_font_size = p_font_size
        self.v_super_user = p_super_user
        self.v_database_index = -1
        self.v_databases = OrderedDict()
        self.v_user_key = p_user_key
        self.v_csv_encoding = p_csv_encoding
        self.v_csv_delimiter = p_csv_delimiter
        self.v_tabs_databases = dict([])


    def AddDatabase(self,
                    p_conn_id = None,
                    p_technology = None,
                    p_database = None,
                    p_prompt_password = True,
                    p_tunnel_information = None,
                    p_alias = None,
                    p_public = None,
                    decryption_failed=False):
        if len(self.v_databases)==0:
            self.v_database_index = 0

        self.v_databases[p_conn_id] = {
                                'database': p_database,
                                'prompt_password': p_prompt_password,
                                'prompt_timeout': None,
                                'tunnel': p_tunnel_information,
                                'tunnel_object': None,
                                'alias': p_alias,
                                'technology': p_technology,
                                'public': p_public,
                                'decryption_failed': decryption_failed
                                }

    def RemoveDatabase(self,
                       p_conn_id = None):
       self.v_databases.pop(p_conn_id, None)

    def DatabaseReachPasswordTimeout(self,p_database_index):
        v_return = { 'timeout': False, 'message': '', 'kind': 'database'}
        # This region of the code cannot be accessed by multiple threads, so locking is required
        try:
            lock_object = tunnel_locks[self.v_databases[p_database_index]['database'].v_conn_id]
        except:
            lock_object = threading.Lock()
            tunnel_locks[self.v_databases[p_database_index]['database'].v_conn_id] = lock_object

        try:
            lock_object.acquire()
        except:
            None

        try:
            #Create tunnel if enabled
            if self.v_databases[p_database_index]['tunnel']['enabled']:
                v_create_tunnel = False
                if self.v_databases[p_database_index]['tunnel_object'] != None:

                    try:
                        result = 0
                        v_tunnel_object = tunnels[self.v_databases[p_database_index]['database'].v_conn_id]
                        if not v_tunnel_object.is_active:
                            v_tunnel_object.stop()
                            v_create_tunnel = True
                    except Exception as exc:
                        v_create_tunnel = True
                        None

                if self.v_databases[p_database_index]['tunnel_object'] == None or v_create_tunnel:
                    try:
                        if self.v_databases[p_database_index]['tunnel']['key'].strip() != '':
                            key = paramiko.RSAKey.from_private_key(
                                io.StringIO(self.v_databases[p_database_index]['tunnel']['key']),
                                password=self.v_databases[p_database_index]['tunnel']['password'])
                            server = SSHTunnelForwarder(
                                (self.v_databases[p_database_index]['tunnel']['server'], int(self.v_databases[p_database_index]['tunnel']['port'])),
                                ssh_username=self.v_databases[p_database_index]['tunnel']['user'],
                                ssh_private_key_password=self.v_databases[p_database_index]['tunnel']['password'],
                                ssh_pkey = key,
                                remote_bind_address=(self.v_databases[p_database_index]['database'].v_active_server, int(self.v_databases[p_database_index]['database'].v_active_port)),
                                logger=logger
                            )
                        else:
                            server = SSHTunnelForwarder(
                                (self.v_databases[p_database_index]['tunnel']['server'], int(self.v_databases[p_database_index]['tunnel']['port'])),
                                ssh_username=self.v_databases[p_database_index]['tunnel']['user'],
                                ssh_password=self.v_databases[p_database_index]['tunnel']['password'],
                                remote_bind_address=(self.v_databases[p_database_index]['database'].v_active_server, int(self.v_databases[p_database_index]['database'].v_active_port)),
                                logger=logger
                            )
                        server.set_keepalive = 120
                        server.start()

                        s = SessionStore(session_key=self.v_user_key)
                        tunnels[self.v_databases[p_database_index]['database'].v_conn_id] = server

                        self.v_databases[p_database_index]['tunnel_object'] = str(server.local_bind_port)
                        self.v_databases[p_database_index]['database'].v_connection.v_host = '127.0.0.1'
                        self.v_databases[p_database_index]['database'].v_connection.v_port = server.local_bind_port

                        s['pgmanage_session'] = self
                        s.save()

                    except Exception as exc:
                        # release the lock on failure, so the subsequent call can acquire it
                        lock_object.release()
                        msg = str(exc)
                        if "checkints" in msg:
                            msg = "Unable to decrypt SSH Key. Wrong passphrase?"
                        return { 'timeout': True, 'message': msg,  'kind': 'tunnel'}

            if self.v_databases[p_database_index]['prompt_password']:
                #Reached timeout, must request password
                if not self.v_databases[p_database_index]['prompt_timeout'] or datetime.now() > self.v_databases[p_database_index]['prompt_timeout'] + timedelta(0,settings.PWD_TIMEOUT_TOTAL):
                    #Try passwordless connection
                    self.v_databases[p_database_index]['database'].v_connection.v_password = ''
                    v_test = self.v_databases[p_database_index]['database'].TestConnection()

                    if v_test=='Connection successful.':
                        s = SessionStore(session_key=self.v_user_key)
                        s['pgmanage_session'].v_databases[p_database_index]['prompt_timeout'] = datetime.now()
                        s['pgmanage_session'].v_databases[p_database_index]['database'].v_connection.v_password = ''
                        s.save()
                        v_return = { 'timeout': False, 'message': '', 'kind':'database'}
                    else:
                        v_return = { 'timeout': True, 'message': v_test, 'kind':'database'}
                #Reached half way to timeout, update prompt_timeout
                if datetime.now() > self.v_databases[p_database_index]['prompt_timeout'] + timedelta(0,settings.PWD_TIMEOUT_REFRESH):
                    s = SessionStore(session_key=self.v_user_key)
                    s['pgmanage_session'].v_databases[p_database_index]['prompt_timeout'] = datetime.now()
                    s.save()
        except:
            None

        try:
            lock_object.release()
        except:
            None

        return v_return

    def GetSelectedDatabase(self):
        return self.v_databases(self.v_database_index)

    def RefreshDatabaseList(self):
        self.v_databases = {}

        try:
            decryption_failed=False
            current_user = UserDetails.objects.filter(user__id=self.v_user_id).select_related("user").first()
            key = key_manager.get(current_user.user)
            connections = Connection.objects.filter(Q(user=current_user.user) | Q(public=True)).prefetch_related("technology")
            for conn in connections:
                try:
                    tunnel_information = {
                        'enabled': conn.use_tunnel,
                        'server': conn.ssh_server,
                        'port': conn.ssh_port,
                        'user': conn.ssh_user,
                        'password': decrypt(conn.ssh_password, key) if conn.ssh_password else '',
                        'key': decrypt(conn.ssh_key, key) if conn.ssh_key else ''
                    }
                    # this is for sqlite3 db connection because it has now password
                    password = decrypt(conn.password, key) if conn.password else ''
                except UnicodeDecodeError:
                    password = "wrong decrypted password"
                    decryption_failed = True
                # in case of decrypt error, set up bad_decrypt variable as True.
                database = OmniDatabase.Generic.InstantiateDatabase(
    				conn.technology.name,
    				conn.server,
    				conn.port,
    				conn.database,
    				conn.username,
                    password,
                    conn.id,
                    conn.alias,
                    p_conn_string = conn.conn_string,
                    p_parse_conn_string = True,
                    connection_params=conn.connection_params
                )

                prompt_password = conn.password == ''

                self.AddDatabase(conn.id,conn.technology.name,database,prompt_password,tunnel_information,conn.alias,conn.public, decryption_failed=decryption_failed)
        # No connections
        except Exception as exc:
            logger.error(str(exc))

    def Execute(self,
                p_database,
                p_sql,
                p_loghistory):
        v_table = p_database.v_connection.Execute(p_sql)

        return v_table

    def Query(self,
                p_database,
                p_sql,
                p_loghistory):
        v_table = p_database.v_connection.Query(p_sql,True)

        return v_table

    def QueryDataLimited(self,
                p_database,
                p_sql,
                p_count,
                p_loghistory):
        v_table = p_database.QueryDataLimited(p_sql, p_count)

        return v_table
