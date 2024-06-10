from datetime import datetime, timedelta
from functools import partial
from unittest.mock import Mock, patch

from app.include import OmniDatabase
from app.include.OmniDatabase import PostgreSQL
from app.models.main import ConfigHistory, Connection, Technology
from app.tests.utils_testing import USERS, execute_client_login
from app.views.configuration import (
    delete_config,
    get_configuration,
    get_configuration_categories,
    get_configuration_history,
    get_status,
    save_configuration,
)
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db import DatabaseError
from django.test import TestCase
from django.urls import resolve, reverse
from django.utils.timezone import make_aware

User = get_user_model()


class ConfigurationViewTests(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.host = "127.0.0.1"
        cls.port = "5433"
        cls.service = "dellstore"
        cls.role = "postgres"
        cls.password = "postgres"
        cls.db_type = "postgresql"
        cls.test_connection = Connection.objects.create(
            user=User.objects.get(username="admin"),
            technology=Technology.objects.filter(name=cls.db_type).first(),
            server=cls.host,
            port=cls.port,
            database=cls.service,
            username=cls.role,
            password=cls.password,
            alias="Pgmanage Tests",
        )
        cls.database = OmniDatabase.Generic.InstantiateDatabase(
            p_db_type=cls.db_type,
            p_server=cls.host,
            p_port=cls.port,
            p_service=cls.service,
            p_user=cls.role,
            p_password=cls.password,
            p_conn_id=cls.test_connection.id,
            p_application_name="Pgmanage Tests",
        )

        cls.tab_data = {"database_index": cls.test_connection.id, "tab_id": 0}
        cls.invalid_tab_data = {"database_index": 999, "tab_id": 0}

    def setUp(self):
        self.test_user = get_user_model().objects.create_user(
            username="test_user",
        )

        self.configHistoryMock = ConfigHistory.objects.create(
            user=User.objects.get(username="admin"),
            connection=self.test_connection,
            start_time=make_aware(datetime.now()),
            config_snapshot={},
        )

        self.configHistoryTestUserMock = ConfigHistory.objects.create(
            user=User.objects.get(username="test_user"),
            connection=self.test_connection,
            start_time=make_aware(datetime.now()),
            config_snapshot={},
        )

        self.user = {
            "user": USERS["ADMIN"]["USER"],
            "password": USERS["ADMIN"]["PASSWORD"],
        }

        execute_client_login(
            p_client=self.client,
            p_username=self.user["user"],
            p_password=self.user["password"],
        )
        session = self.client.session

        session["pgmanage_session"].v_databases = {
            self.test_connection.id: {
                "database": self.database,
                "prompt_password": False,
                "prompt_timeout": datetime.now() + timedelta(0, 60000),
            }
        }
        session["pgmanage_session"].v_tab_connections = {
            self.test_connection.id: self.database
        }
        session["pgmanage_session"].v_tabs_databases = {0: "dellstore"}
        session.save()

        self.client.post = partial(self.client.post, content_type="application/json")

    @patch("app.views.configuration.get_settings")
    def test_get_configuration_logged_in(self, get_settings_mock):
        get_settings_mock.return_value = "mocked value"
        response = self.client.post(reverse("get_configuration"), data=self.tab_data)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json().get("settings"), "mocked value")

    @patch("app.views.configuration.get_settings")
    def test_get_configuration_logged_out(self, get_settings_mock):
        self.client.logout()
        get_settings_mock.return_value = "mocked value"
        response = self.client.post(reverse("get_configuration"), data=self.tab_data)

        self.assertEqual(response.status_code, 401)

    def test_get_configuration_with_invalid_database_id(self):
        response = self.client.post(
            reverse("get_configuration"), data=self.invalid_tab_data
        )
        self.assertEqual(response.status_code, 400)

    @patch("app.views.configuration.get_settings")
    def test_get_configuration_view_with_get_settings_returns_error(
        self, get_settings_mock
    ):
        get_settings_mock.side_effect = DatabaseError("mocked error")

        response = self.client.post(reverse("get_configuration"), data=self.tab_data)
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json().get("data"), "mocked error")

    def test_get_configuration_url_resolves_get_configuration_view(self):
        view = resolve("/configuration/")
        self.assertEqual(view.func.__name__, get_configuration.__name__)

    @patch.object(PostgreSQL, "QueryConfigCategories")
    def test_get_configuration_categories_view_logged_in(self, query_mock):
        query_mock.return_value = Mock(Rows=[["1"], ["2"]])
        response = self.client.post(
            reverse("get_configuration_categories"), data=self.tab_data
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json().get("categories"), ["1", "2"])

    def test_get_configuration_categories_view_logged_out(self):
        self.client.logout()
        response = self.client.post(
            reverse("get_configuration_categories"), data=self.tab_data
        )
        self.assertEqual(response.status_code, 401)

    @patch.object(PostgreSQL, "QueryConfigCategories")
    def test_get_configuration_categories_with_query_returns_error(self, query_mock):
        query_mock.side_effect = DatabaseError("mocked error")
        response = self.client.post(
            reverse("get_configuration_categories"), data=self.tab_data
        )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json().get("data"), "mocked error")

    def test_get_configuration_categories_url_resolves_get_configuration_categories_view(
        self,
    ):
        view = resolve("/configuration/categories/")
        self.assertEqual(view.func.__name__, get_configuration_categories.__name__)

    @patch("app.views.configuration.post_settings")
    def test_save_configuration_view(self, post_settings_mock):
        post_settings_mock.return_value = {"settings": "saved settings"}
        response = self.client.post(reverse("save_configuration"), data=self.tab_data)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json().get("settings"), "saved settings")

    @patch("app.views.configuration.post_settings")
    def test_save_configuration_view_with_validation_error_response(
        self, post_settings_mock
    ):
        post_settings_mock.side_effect = ValidationError("mocked validation error")
        response = self.client.post(reverse("save_configuration"), data=self.tab_data)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json().get("data"), "mocked validation error")

    @patch("app.views.configuration.post_settings")
    def test_save_configuration_view_with_database_error_response(
        self, post_settings_mock
    ):
        post_settings_mock.side_effect = DatabaseError("mocked database error")
        response = self.client.post(reverse("save_configuration"), data=self.tab_data)

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json().get("data"), "mocked database error")

    def test_save_configuration_url_resolves_save_configuration_view(self):
        view = resolve("/save_configuration/")
        self.assertEqual(view.func.__name__, save_configuration.__name__)

    def test_get_configuration_history_logged_in(self):
        response = self.client.post(
            reverse("get_configuration_history"), data=self.tab_data
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json().get("config_history")), 1)

    def test_get_configuration_history_logged_out(self):
        self.client.logout()
        response = self.client.post(
            reverse("get_configuration_history"), data=self.tab_data
        )

        self.assertEqual(response.status_code, 401)

    def test_get_configuration_history_url_resolves_get_configuration_history_view(
        self,
    ):
        view = resolve("/get_configuration_history//")
        self.assertEqual(view.func.__name__, get_configuration_history.__name__)

    @patch("app.views.configuration.get_settings_status")
    def test_get_status_view(self, get_settings_status_mock):
        get_settings_status_mock.return_value = {"data": "mocked result"}
        response = self.client.post(reverse("settings_status"), data=self.tab_data)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json().get("data"), "mocked result")

    @patch("app.views.configuration.get_settings_status")
    def test_get_status_view_with_get_settings_status_returns_error(
        self, get_settings_status_mock
    ):
        get_settings_status_mock.side_effect = DatabaseError("mocked error")
        response = self.client.post(reverse("settings_status"), data=self.tab_data)

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json().get("data"), "mocked error")

    def test_get_status_url_resolves_get_status_view(self):
        view = resolve("/configuration/status/")
        self.assertEqual(view.func.__name__, get_status.__name__)

    def test_delete_config_view_with_valid_id(self):
        self.assertEqual(len(ConfigHistory.objects.all()), 2)
        response = self.client.delete(
            reverse("delete_configuration", args=[self.configHistoryMock.id])
        )
        self.assertEqual(response.status_code, 204)
        self.assertEqual(len(ConfigHistory.objects.all()), 1)

    def test_delete_config_view_with_invalid_id(self):
        response = self.client.delete(reverse("delete_configuration", args=[999]))
        self.assertEqual(response.status_code, 204)
        self.assertEqual(len(ConfigHistory.objects.all()), 2)

    def test_delete_config_with_wrong_http_method(self):
        response = self.client.post(reverse("delete_configuration", args=[999]))
        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.reason_phrase, "Method Not Allowed")

    def test_delete_config_as_another_user(self):
        response = self.client.delete(
            reverse("delete_configuration", args=[self.configHistoryTestUserMock.id])
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(
            response.json().get("message"),
            "You are not allowed to delete not yours configurations.",
        )
        self.assertEqual(len(ConfigHistory.objects.all()), 2)

    def test_delete_config_url_resolves_delete_config_view(self):
        view = resolve(f"/configuration/{self.configHistoryMock.id}/")

        self.assertEqual(view.func.__name__, delete_config.__name__)
