from datetime import datetime
from functools import partial

from app.models import Connection, ConsoleHistory, QueryHistory, Technology
from django.contrib.auth.models import User
from django.test import Client, TestCase
from django.urls import reverse
from django.utils.timezone import make_aware


class CommandsHistoryTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = User.objects.create_user(username="testuser", password="testpass")
        self.connection = Connection.objects.create(
            user=self.user,
            technology=Technology.objects.filter(name="postgresql").first(),
        )
        self.clear_url = reverse("clear_commands_history")
        self.get_url = reverse("get_commands_history")

        self.client.post = partial(self.client.post, content_type="application/json")

    def login(self):
        self.client.login(username="testuser", password="testpass")

    def test_clear_commands_history_success(self):
        self.login()

        ConsoleHistory.objects.create(
            user=self.user,
            connection=self.connection,
            snippet="ls",
            start_time=make_aware(datetime.now()),
        )

        self.assertEqual(ConsoleHistory.objects.count(), 1)

        response = self.client.post(
            self.clear_url,
            data={
                "database_index": self.connection.id,
                "command_type": "Console",
                "command_contains": "l",
            },
        )
        self.assertEqual(response.status_code, 204)
        self.assertEqual(ConsoleHistory.objects.count(), 0)

    def test_clear_commands_history_with_date_and_database_filter(self):
        self.login()
        now = make_aware(datetime.now())

        ConsoleHistory.objects.create(
            user=self.user,
            connection=self.connection,
            snippet="history command",
            start_time=now,
            database="testdb",
        )

        response = self.client.post(
            self.clear_url,
            data={
                "database_index": self.connection.id,
                "command_type": "Console",
                "command_contains": "history",
                "command_from": now.isoformat(),
                "command_to": now.isoformat(),
                "database_filter": "testdb",
            },
        )
        self.assertEqual(response.status_code, 204)
        self.assertEqual(ConsoleHistory.objects.count(), 0)

    def test_clear_commands_history_query_type(self):
        self.login()
        now = make_aware(datetime.now())

        QueryHistory.objects.create(
            user=self.user,
            connection=self.connection,
            snippet="SELECT 1",
            start_time=now,
            end_time=now,
            database="filter_db",
        )

        response = self.client.post(
            self.clear_url,
            data={
                "database_index": self.connection.id,
                "command_type": "Query",
                "command_contains": "SELECT",
                "database_filter": "filter_db",
                "command_from": now.isoformat(),
                "command_to": now.isoformat(),
            },
        )
        self.assertEqual(response.status_code, 204)
        self.assertEqual(QueryHistory.objects.count(), 0)

    def test_clear_commands_history_invalid_connection(self):
        self.login()
        response = self.client.post(
            self.clear_url,
            data={
                "database_index": 999,
                "command_type": "Console",
                "command_contains": "l",
            },
        )
        self.assertEqual(response.status_code, 400)

    def test_clear_commands_history_unauthenticated(self):
        response = self.client.post(self.clear_url)
        self.assertEqual(response.status_code, 401)

    def test_get_commands_history_success(self):
        self.login()

        ConsoleHistory.objects.create(
            user=self.user,
            connection=self.connection,
            snippet="console ls",
            start_time=make_aware(datetime.now()),
        )

        response = self.client.post(
            self.get_url,
            data={
                "database_index": self.connection.id,
                "command_type": "Console",
                "command_contains": "l",
                "current_page": 0,
            },
        )
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn("command_list", data)
        self.assertEqual(len(data["command_list"]), 1)

    def test_get_commands_history_with_all_filters_query_type(self):
        self.login()
        now = make_aware(datetime.now())

        QueryHistory.objects.create(
            user=self.user,
            connection=self.connection,
            snippet="SELECT something",
            start_time=now,
            end_time=now,
            database="analytics",
        )

        response = self.client.post(
            self.get_url,
            data={
                "database_index": self.connection.id,
                "command_type": "Query",
                "command_contains": "something",
                "current_page": 0,
                "database_filter": "analytics",
                "command_from": now.isoformat(),
                "command_to": now.isoformat(),
            },
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data["command_list"]), 1)
        self.assertEqual(data["command_list"][0]["database"], "analytics")
        self.assertIn("pages", data)
        self.assertIn("database_names", data)
        self.assertIn("analytics", data["database_names"])

    def test_get_commands_history_with_query_type_and_empty_database(self):
        self.login()
        now = make_aware(datetime.now())

        QueryHistory.objects.create(
            user=self.user,
            connection=self.connection,
            snippet="SELECT empty_db",
            start_time=now,
            end_time=now,
            database=None,
        )

        response = self.client.post(
            self.get_url,
            data={
                "database_index": self.connection.id,
                "command_type": "Query",
                "command_contains": "empty_db",
                "current_page": 0,
            },
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data["command_list"]), 1)

    def test_get_commands_history_invalid_connection(self):
        self.login()
        response = self.client.post(
            self.get_url,
            data={
                "database_index": 999,
                "current_page": 0,
                "command_type": "Query",
                "command_contains": "ls",
            },
        )
        self.assertEqual(response.status_code, 400)

    def test_get_commands_history_unauthenticated(self):
        response = self.client.post(self.get_url)
        self.assertEqual(response.status_code, 401)
