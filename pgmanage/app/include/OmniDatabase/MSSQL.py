import app.include.Spartacus as Spartacus



class MSSQL:
    def __init__(self, server, port, service, user, password, conn_id=0, alias='', conn_string='', parse_conn_string = False, connection_params=None):
            self.lock = None
            self.connection_params = connection_params if connection_params else {}
            self.alias = alias
            self.db_type = 'mysql'
            self.conn_string = conn_string
            self.conn_string_error = ''
            self.password = password
            self.conn_id = conn_id

            self.server = server
            self.active_server = server
            self.user = user
            self.active_user = user
            self.schema = service
            self.service = service
            self.active_service = service

            self.port = port

            if port is None or port == '':
                self.active_port = '1433'
            else:
                self.active_port = port

            self.connection = Spartacus.Database.MSSQL(self.active_server, self.active_port, self.active_service, self.active_user, self.password)

    

    # Decorator to acquire lock before performing action
    def lock_required(function):
        def wrap(self, *args, **kwargs):
            try:
                if self.lock != None:
                    self.lock.acquire()
            except:
                None
            try:
                r = function(self, *args, **kwargs)
            except:
                try:
                    if self.lock != None:
                        self.lock.release()
                except:
                    None
                raise
            try:
                if self.lock != None:
                    self.lock.release()
            except:
                None
            return r
        wrap.__doc__ = function.__doc__
        wrap.__name__ = function.__name__
        return wrap

    

    def GetVersion(self):
        return self.ExecuteScalar('SELECT @@VERSION;')
    

    @lock_required
    def ExecuteScalar(self, sql):
        return self.connection.ExecuteScalar(sql)