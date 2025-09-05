import app.include.Spartacus as Spartacus


class MSSQL:
    def __init__(self, server, port, service, user, password):
        self.lock = None
        self.db_type = "mysql"
        self.password = password

        self.server = server
        self.active_server = server
        self.user = user
        self.active_user = user
        self.service = service
        self.active_service = service

        self.port = port

        if port is None or port == "":
            self.active_port = "1433"
        else:
            self.active_port = port

        self.connection = Spartacus.Database.MSSQL(
            self.active_server, self.active_port, self.active_service, self.active_user, self.password
        )

        self.console_help = "Console tab. Type the commands in the editor below this box. \? to view command list."

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
        return self.ExecuteScalar("SELECT @@VERSION;")

    @lock_required
    def ExecuteScalar(self, sql):
        return self.connection.ExecuteScalar(sql)

    def TestConnection(self):
        return_data = ""

        try:
            self.connection.Open()
            self.connection.Close()
            return_data = "Connection successful."
        except Exception as exc:
            return_data = str(exc)
        return return_data

    def PrintDatabaseDetails(self):
        return self.active_server + ":" + self.active_port

    def PrintDatabaseInfo(self):
        return self.active_user + "@" + self.active_service
