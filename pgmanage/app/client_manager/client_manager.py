import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union

from app.include import OmniDatabase
from app.include.Session import Session


class Client:
    """
    Represents a client object that manages connection sessions and tabs.

    Attributes:
        _to_be_removed (List[Any]): A list of tabs to be removed.
        id (str): The unique identifier of the client.
        polling_lock (threading.Lock): A lock used for synchronization during polling.
        returning_data_lock (threading.Lock): A lock used for synchronization while accessing returning_data.
        returning_data (List[Any]): A list containing data returned by the client.
        _connection_sessions (dict): A dictionary storing connection sessions for the client.
        last_update (datetime.datetime): The timestamp of the client's last update.
    """

    to_be_removed = []

    def __init__(self, client_id: str) -> None:
        self.id = client_id
        self.polling_lock = threading.Lock()
        self.returning_data_lock = threading.Lock()
        self.returning_data = []
        self._connection_sessions = {}
        self.last_update = datetime.now()

    @property
    def connection_sessions(self) -> Union[Dict[str, Any], Dict]:
        """
        Property to access the connection sessions of the client.

        Returns:
            Union[Dict[str, Any], Dict]: The connection sessions dictionary.
        """
        return self._connection_sessions

    def release_polling_lock(self) -> None:
        """
        Releases the polling lock if acquired.
        """
        try:
            self.polling_lock.release()
        except RuntimeError:
            pass

    def release_returning_data_lock(self) -> None:
        """
        Releases the returning data lock if acquired.
        """
        try:
            self.returning_data_lock.release()
        except RuntimeError:
            pass

    def get_tab(
        self, conn_tab_id: str, tab_id: Optional[str] = None
    ) -> Union[Dict[str, Any], None]:
        """Retrieves the tab with the given IDs from the client.

        Args:
            conn_tab_id (str): The ID of the connection tab.
            tab_id (Optional[str], None): The ID of the tab. Defaults to None.

        Returns:
            Union[Dict[str, Any], None]: The tab object if found, otherwise None.
        """

        main_tab = self.connection_sessions.get(conn_tab_id)

        if tab_id is None:
            return main_tab

        if main_tab:
            return main_tab["tab_list"].get(tab_id)
        return None

    def create_tab(
        self,
        conn_tab_id: str,
        tab: Dict[str, Any],
        tab_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Creates a new tab with the given details and
        adds it to the tab_list of client's connection session.

        Args:
            conn_tab_id (str): The ID of the connection tab.
            tab (Dict[str, Any]): The details of the tab.
            tab_id (Optional[str], optional): The ID of the tab. Defaults to None.

        Returns:
            Dict[str, Any]: The created tab object.
        """

        tab = self._create_tab_internal(
            conn_tab_id=conn_tab_id,
            tab=tab,
            tab_id=tab_id,
            is_main_tab=False,
        )
        return tab

    def create_main_tab(self, conn_tab_id: str, tab: Dict[str, Any]) -> Dict[str, Any]:
        """Creates a new main tab with the given details and
        adds it to the client's connection sessions.

        Args:
            conn_tab_id (str): The ID of the connection tab.
            tab (Dict[str, Any]): The details of the main tab.

        Returns:
            Dict[str, Any]: The created main tab object.
        """
        tab = self._create_tab_internal(
            conn_tab_id=conn_tab_id, tab=tab, is_main_tab=True
        )
        return tab

    def _create_tab_internal(
        self,
        conn_tab_id: str,
        tab: Dict[str, Any],
        tab_id: Optional[str] = None,
        is_main_tab: bool = False,
    ) -> Dict[str, Any]:
        """Internal method to create a new tab with the given details and
        add it to the client's connection sessions.

        Args:
            conn_tab_id (str): The ID of the connection tab.
            tab (Dict[str, Any]): The details of the tab.
            tab_id (Optional[str], optional): The ID of the tab. Defaults to None.
            is_main_tab (bool, optional): Specifies if the tab is a main tab. Defaults to False.

        Returns:
            Dict[str, Any]: The created tab object.
        """

        tab["last_update"] = datetime.now()
        tab["to_be_removed"] = False

        if is_main_tab:
            self._connection_sessions[conn_tab_id] = tab
        else:
            main_tab = self.get_tab(conn_tab_id=conn_tab_id)
            if not main_tab:
                self.create_main_tab(
                    tab={"omnidatabase": None, "type": "connection", "tab_list": {}},
                    conn_tab_id=conn_tab_id,
                )

            self._connection_sessions[conn_tab_id]["tab_list"][tab_id] = tab

        return tab

    def close_tab(self, conn_tab_id: str, tab_id: Optional[str] = None) -> None:
        """Closes the tab with the given IDs by removing it from the client's connection sessions
        and closing associated connections.

        Args:
            conn_tab_id (str): The ID of the connection tab.
            tab_id (Optional[str], optional): The ID of the tab. Defaults to None.

        Returns:
            None
        """

        tab = self.get_tab(tab_id=tab_id, conn_tab_id=conn_tab_id)
        if tab is None:
            return

        if tab_id:
            del self._connection_sessions[conn_tab_id]["tab_list"][tab_id]
        else:
            del self._connection_sessions[conn_tab_id]

        if tab["type"] in ["query", "console", "connection", "edit"]:
            try:
                tab["omnidatabase"].v_connection.Cancel(False)
            except Exception:
                pass
            try:
                tab["omnidatabase"].v_connection.Close()
            except Exception:
                pass

        elif tab["type"] == "debug":
            self._close_debug_tab(tab)

        elif tab["type"] == "terminal":
            self._close_terminal_tab(tab)

    def _close_debug_tab(self, tab: Dict[str, Any]) -> None:
        """Closes a debug tab by canceling, terminating, and closing the associated connections.

        Args:
            tab (Dict[str, Any]): The debug tab dictionary.

        Returns:
            None
        """
        tab["cancelled"] = True
        try:
            tab["omnidatabase_control"].v_connection.Cancel(False)
        except Exception:
            pass
        try:
            tab["omnidatabase_control"].v_connection.Terminate(tab["debug_pid"])
        except Exception:
            pass
        try:
            tab["omnidatabase_control"].v_connection.Close()
        except Exception:
            pass
        try:
            tab["omnidatabase_debug"].v_connection.Close()
        except Exception:
            pass

    def _close_terminal_tab(self, tab: Dict[str, Any]) -> None:
        """Closes a terminal tab by stopping the associated thread and closing the connections.

        Args:
            tab (Dict[str, Any]): The terminal tab dictionary.

        Returns:
            None
        """

        if tab["thread"] is not None:
            tab["thread"].stop()
        if tab["terminal_type"] == "local":
            tab["terminal_object"].terminate()
        else:
            tab["terminal_object"].close()
            tab["terminal_ssh_client"].close()

    def get_main_tab_database(
        self,
        session: Session,
        conn_tab_id: str,
        database_index: int,
        attempt_to_open_connection: bool = False,
    ):
        """Retrieves the database for the specified session and connection tab.

        Args:
            session (Session): The session object.
            conn_tab_id (str): The ID of the connection tab.
            database_index (int): The index of the database.
            attempt_to_open_connection (bool, optional): Specifies whether
            to attempt opening the connection if not already open. Defaults to False.

        Returns:
            The database object.
        """

        # Retrieving tab object
        tab = self.get_tab(conn_tab_id=conn_tab_id)
        if tab is None:
            tab = self.create_main_tab(
                tab={"omnidatabase": None, "type": "connection", "tab_list": {}},
                conn_tab_id=conn_tab_id,
            )

        return self.get_tab_database(
            session=session,
            tab=tab,
            conn_tab_id=conn_tab_id,
            database_index=database_index,
            attempt_to_open_connection=attempt_to_open_connection,
            use_lock=True,
        )

    def _should_update_database(
        self, tab: Dict[str, Any], current_tab_database: str, main_tab_database
    ) -> bool:
        """
        Determines whether the database for a given tab should be updated.

        Args:
            tab (Dict[str, Any]): The tab dictionary representing a client tab.
            current_tab_database (str): The name of the current database associated with the tab.
            main_tab_database: The main tab's database object to compare with.

        Returns:
            bool: True if the database should be updated, False otherwise.
        """
        omni_database = tab["omnidatabase"]
        connection = main_tab_database.v_connection
        return (
            omni_database is None
            or main_tab_database.v_db_type != omni_database.v_db_type
            or connection.v_host != omni_database.v_connection.v_host
            or str(connection.v_port) != str(omni_database.v_connection.v_port)
            or current_tab_database != omni_database.v_active_service
            or connection.v_password != omni_database.v_connection.v_password
        )

    def _replace_database(
        self,
        tab: Dict[str, Any],
        current_tab_database: str,
        main_tab_database,
        use_lock: bool,
    ) -> None:
        """
        Replaces the database for a given tab with a new database object.

        Args:
            tab (Dict[str, Any]): The tab dictionary representing a client tab.
            current_tab_database (str): The name of the current database associated with the tab.
            main_tab_database: The main tab's database object to use for replacement.
            use_lock (bool): Flag indicating whether to assign a lock to the new database.

        Returns:
            None.
        """
        connection_params = (
            main_tab_database.connection_params
            if hasattr(main_tab_database, "connection_params")
            else None
        )

        database_new = OmniDatabase.Generic.InstantiateDatabase(
            p_db_type=main_tab_database.v_db_type,
            p_server=main_tab_database.v_connection.v_host,
            p_port=str(main_tab_database.v_connection.v_port),
            p_service=current_tab_database,
            p_user=main_tab_database.v_active_user,
            p_password=main_tab_database.v_connection.v_password,
            p_conn_id=main_tab_database.v_conn_id,
            p_alias=main_tab_database.v_alias,
            p_conn_string=main_tab_database.v_conn_string,
            p_parse_conn_string=False,
            connection_params=connection_params,
        )

        # check if database connection is valid
        try:
            database_new.v_connection.Open()
        except Exception:
            # otherwise revert to main connection
            database_new = main_tab_database

        if use_lock:
            database_new.v_lock = threading.Lock()

        if tab["omnidatabase"]:
            self.to_be_removed.append(tab["omnidatabase"])

        tab["omnidatabase"] = database_new

    def get_tab_database(
        self,
        session: Session,
        tab: Dict[str, Any],
        conn_tab_id: str,
        database_index: int,
        current_database: str = None,
        attempt_to_open_connection: bool = False,
        use_lock: bool = False,
    ):
        """Retrieves the database for the specified session and tab,
        and performs necessary updates if the database object has changed.

        Args:
            session (Session): The session object.
            tab (Dict[str, Any]): The tab object.
            conn_tab_id (str): The ID of the connection tab.
            database_index (int): The index of the database.
            current_database (str, optional): The current database. Defaults to None.
            attempt_to_open_connection (bool, optional): Specifies whether
            to attempt opening the connection if not already open. Defaults to False.
            use_lock (bool, optional): Specifies whether
            to use a lock for thread safety. Defaults to False.

        Returns:
            The tab's database object.
        """
        main_tab_database = session.v_databases[database_index]["database"]
        current_tab_database = current_database or session.v_tabs_databases.get(conn_tab_id)

        # Updating time
        tab["last_update"] = datetime.now()

        if self._should_update_database(
            tab=tab,
            current_tab_database=current_tab_database,
            main_tab_database=main_tab_database,
        ):
            self._replace_database(
                tab=tab,
                current_tab_database=current_tab_database,
                main_tab_database=main_tab_database,
                use_lock=use_lock,
            )

        # Try to open connection if not opened yet
        if attempt_to_open_connection and (
            not tab["omnidatabase"].v_connection.v_con
            or tab["omnidatabase"].v_connection.GetConStatus() == 0
        ):
            tab["omnidatabase"].v_connection.Open()

        return tab["omnidatabase"]


class ClientManager:
    """
    The ClientManager class manages the clients.

    Attributes:
        _clients (Dict[str, Client]): A dictionary of clients, where the client ID is the key.
        _instance (ClientManager): The singleton instance of the ClientManager class.
        _lock (threading.Lock): A lock for thread safety.
    """

    _clients = {}
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super().__new__(cls)

        return cls._instance

    @property
    def clients(self) -> Union[Dict[str, Client], Dict]:
        """
        Get the dictionary of clients.

        Returns:
            Union[Dict[str, Any], Dict]: The dictionary of clients.
        """

        return self._clients

    def get_client(self, client_id: str) -> Union[Client, None]:
        """
        Get the client with the specified client ID.

        Args:
            client_id (str): The ID of the client.

        Returns:
            Union[Dict[str, Client], None]: The client object if found, None otherwise.
        """
        client = self._clients.get(client_id)
        return client

    def create_client(self, client_id: str) -> Client:
        """
        Create a new client with the specified client ID.

        Args:
            client_id (str): The ID of the client.

        Returns:
            Dict[str, Client]: The newly created client object.
        """

        client = Client(client_id=client_id)

        self._clients[client_id] = client

        return client

    def get_or_create_client(self, client_id: str) -> Client:
        """Retrieves the client with the given client_id. If the client does not exist,
        creates a new client with the given client_id.

        Args:
            client_id (str): The ID of the client.

        Returns:
            Dict[str, Client]: The client object.
        """

        client = self.get_client(client_id=client_id)

        if client is None:
            client = self.create_client(client_id=client_id)

        return client

    def clear_client(self, client_id: str) -> None:
        """Clears the client with the given client_id by removing
        all associated connection sessions and releasing locks.

        Args:
            client_id (str): The ID of the client.

        Returns:
            None
        """

        client = self.get_or_create_client(client_id=client_id)

        for conn_tab in client.connection_sessions.values():
            for tab in conn_tab.get("tab_list", {}).values():
                tab["to_be_removed"] = True
            conn_tab["to_be_removed"] = True

        client.release_polling_lock()

        client.release_returning_data_lock()


client_manager = ClientManager()


def cleanup_thread():
    """Thread function that continuously cleans up inactive client connections and tabs.

    The function runs in an infinite loop and periodically checks
    for inactive client connections and tabs. If a client or tab has exceeded
    the timeout period or flagged for removal,
    it will be closed and removed from the client manager.

    Returns:
        None
    """
    while True:
        while Client.to_be_removed:
            conn = Client.to_be_removed.pop(0)
            conn.v_connection.Close()

        for client_id in list(client_manager.clients):
            client = client_manager.get_client(client_id=client_id)
            client_timeout_reached = datetime.now() > client.last_update + timedelta(
                0, 3600
            )

            for conn_tab_id in list(client.connection_sessions):
                for tab_id in list(
                    client.connection_sessions[conn_tab_id].get("tab_list", [])
                ):
                    tab = client.get_tab(conn_tab_id=conn_tab_id, tab_id=tab_id)

                    if is_tab_expired(tab, client_timeout_reached):
                        client.close_tab(tab_id=tab_id, conn_tab_id=conn_tab_id)

                if not client.connection_sessions[conn_tab_id].get("tab_list"):
                    tab = client.get_tab(conn_tab_id=conn_tab_id)
                    if is_tab_expired(tab, client_timeout_reached):
                        client.close_tab(conn_tab_id=conn_tab_id)
        time.sleep(30)


def is_tab_expired(tab: Dict[str, Any], client_timeout_reached: bool) -> bool:
    """Check if a tab has expired based on the timeout and removal flags.

    Args:
        tab: Dictionary representing a tab object.
        client_timeout_reached: Boolean indicating if the client timeout has been reached.

    Returns:
        bool: True if the tab has expired, False otherwise.
    """
    tab_timeout_reached = datetime.now() > tab["last_update"] + timedelta(0, 3600)
    return (
        client_timeout_reached or tab_timeout_reached or tab.get("to_be_removed", False)
    )


t = threading.Thread(target=cleanup_thread)
t.setDaemon(True)
t.start()
