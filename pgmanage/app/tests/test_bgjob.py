from unittest.mock import patch

from app.bgjob.jobs import PROCESS_NOT_FOUND, PROCESS_TERMINATED, BatchJob
from app.models.main import Job
from app.tests.utils_testing import USERS, execute_client_login
from app.views.bgjob import delete_job, details
from app.views.bgjob import index as jobs_list
from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import resolve, reverse

User = get_user_model()


class BackgroundJobsTests(TestCase):

    def setUp(self):
        self.user = {
            "user": USERS["ADMIN"]["USER"],
            "password": USERS["ADMIN"]["PASSWORD"],
        }
        self.job = Job.objects.create(id=1, user=User.objects.get(username="admin"))

        execute_client_login(
            p_client=self.client,
            p_username=self.user["user"],
            p_password=self.user["password"],
        )

    def test_get_background_jobs_list_view(self):
        response = self.client.get(reverse("job_list"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json().get("data"), [])

    def test_get_background_jobs_list_view_for_logged_out_user(self):
        self.client.logout()
        response = self.client.get(reverse("job_list"))
        self.assertEqual(response.status_code, 401)

    def test_get_background_jobs_list_url_resolves_jobs_list_view(self):
        view = resolve("/bgprocess/")
        self.assertEqual(view.func.__name__, jobs_list.__name__)

    @patch("psutil.Process")
    def test_delete_job_view_for_logged_in_user(self, mock_process):
        self.assertEqual(len(Job.objects.all()), 1)
        response = self.client.post(reverse("delete_job", args=[self.job.id]))
        self.assertEqual(response.status_code, 204)
        self.assertEqual(len(Job.objects.all()), 0)

    def test_delete_job_view_for_logged_out_user(self):
        self.client.logout()
        response = self.client.post(reverse("delete_job", args=[self.job.id]))
        self.assertEqual(response.status_code, 401)

    def test_delete_job_view_with_invalid_job_id(self):
        response = self.client.post(reverse("delete_job", args=[999]))
        self.assertEqual(response.status_code, 410)
        self.assertEqual(
            response.json().get("data"),
            PROCESS_NOT_FOUND,
        )

    def test_delete_job_url_resolves_delete_job_view(self):
        view = resolve(f"/bgprocess/delete/{self.job.id}/")
        self.assertEqual(view.func.__name__, delete_job.__name__)

    @patch.object(BatchJob, "status")
    @patch.object(BatchJob, "_retrieve_job")
    def test_detail_job_view(self, retrieve_mock, status_mock):
        status_mock.return_value = "test success"
        retrieve_mock.return_value = "test retrieve"
        response = self.client.get(reverse("job_details", args=[self.job.id, 0, 0]))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json().get("data"), "test success")

    def test_detail_job_view_with_invalid_id(self):
        response = self.client.get(reverse("job_details", args=[999, 0, 0]))
        self.assertEqual(response.status_code, 410)
        self.assertEqual(
            response.json().get("data"),
            PROCESS_NOT_FOUND,
        )

    def test_detail_job_url_resolves_detail_job_view(self):
        view = resolve(f"/bgprocess/{self.job.id}/0/0/")
        self.assertEqual(view.func.__name__, details.__name__)

    @patch("psutil.Process")
    def test_stop_detail_job_view(self, mock_process):
        response = self.client.post(reverse("stop_job", args=[self.job.id]))

        self.assertEqual(response.status_code, 204)
        self.assertEqual(
            Job.objects.get(id=self.job.id).process_state, PROCESS_TERMINATED
        )

    @patch("psutil.Process")
    def test_stop_detail_job_view_with_invalid_id(self, mock_process):
        response = self.client.post(reverse("stop_job", args=[999]))

        self.assertEqual(response.status_code, 410)
        self.assertEqual(response.json().get("data"), PROCESS_NOT_FOUND)
