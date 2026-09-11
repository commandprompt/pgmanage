from app.include.Spartacus import Database as SpartacusDatabase
from app.utils.auth_methods import uses_iam_auth
from django.test import SimpleTestCase


class UsesIamAuthTests(SimpleTestCase):
    def test_empty_credentials(self):
        self.assertFalse(uses_iam_auth(None))
        self.assertFalse(uses_iam_auth({}))

    def test_password_auth(self):
        self.assertFalse(uses_iam_auth({"auth_method": "user-pass"}))

    def test_iam_auth(self):
        self.assertTrue(uses_iam_auth({"auth_method": "iam"}))


class PostgreSQLGetPasswordTests(SimpleTestCase):
    def test_returns_the_stored_password(self):
        connection = SpartacusDatabase.PostgreSQL(
            "db.example.com", 5432, "postgres", "postgres", "stored-password"
        )

        self.assertEqual(connection.GetPassword(), "stored-password")
        self.assertIn("password='stored-password'", connection.GetConnectionString())
