import unittest
from unittest.mock import Mock, patch
import pandas as pd
from pyiron_base.jobs.job.extension.server.queuestatus import (
    queue_table,
    queue_check_job_is_waiting_or_running,
    queue_info_by_job_id,
    queue_is_empty,
    queue_delete_job,
    queue_enable_reservation,
    wait_for_job,
    wait_for_jobs,
    update_from_remote,
    retrieve_job,
    validate_que_request,
)
from pyiron_base.state import state
from pyiron_base._tests import PyironTestCase
from pyiron_base.project.generic import Project
import os


class TestQueueStatus(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = os.path.dirname(os.path.abspath(__file__))
        state.settings.configuration["project_paths"] = cls.file_location
        cls.project = Project(os.path.join(cls.file_location, "test_project"))

    @classmethod
    def tearDownClass(cls):
        state.settings.configuration["project_paths"] = []
        cls.project.remove(enable=True)

    def setUp(self):
        self.job = self.project.create_job("ScriptJob", "test_job")
        self.job.server.queue_id = 123

    def test_queue_table(self):
        with patch("pyiron_base.state.state.queue_adapter", new_callable=Mock) as mock_adapter:
            mock_adapter.get_status_of_my_jobs.return_value = pd.DataFrame(
                {"jobname": ["pi_1", "pi_2"], "status": ["running", "pending"]}
            )
            df = queue_table(job_ids=[1, 2])
            self.assertEqual(len(df), 2)

    def test_queue_check_job_is_waiting_or_running(self):
        with patch("pyiron_base.state.state.queue_adapter", new_callable=Mock) as mock_adapter:
            mock_adapter.get_status_of_job.return_value = "running"
            self.assertTrue(queue_check_job_is_waiting_or_running(self.job))
            mock_adapter.get_status_of_job.return_value = "pending"
            self.assertTrue(queue_check_job_is_waiting_or_running(self.job))
            mock_adapter.get_status_of_job.return_value = "finished"
            self.assertFalse(queue_check_job_is_waiting_or_running(self.job))

    def test_queue_info_by_job_id(self):
        with patch("pyiron_base.state.state.queue_adapter", new_callable=Mock) as mock_adapter:
            mock_adapter.get_status_of_job.return_value = {"status": "running"}
            info = queue_info_by_job_id(123)
            self.assertEqual(info, {"status": "running"})

    def test_queue_is_empty(self):
        with patch("pyiron_base.state.state.queue_adapter", new_callable=Mock) as mock_adapter:
            mock_adapter.get_status_of_my_jobs.return_value = pd.DataFrame()
            self.assertTrue(queue_is_empty())
            mock_adapter.get_status_of_my_jobs.return_value = pd.DataFrame(
                {"jobname": ["pi_1"]}
            )
            self.assertFalse(queue_is_empty())

    def test_queue_delete_job(self):
        with patch("pyiron_base.state.state.queue_adapter", new_callable=Mock) as mock_adapter:
            queue_delete_job(self.job)
            mock_adapter.delete_job.assert_called_with(process_id=123)

    def test_queue_enable_reservation(self):
        with patch("pyiron_base.state.state.queue_adapter", new_callable=Mock) as mock_adapter:
            queue_enable_reservation(self.job)
            mock_adapter.enable_reservation.assert_called_with(process_id=123)

    def test_wait_for_job(self):
        self.job.status.created = True
        with patch("pyiron_base.jobs.job.extension.server.queuestatus.queue_check_job_is_waiting_or_running", return_value=False):
            with patch("pyiron_base.state.state.queue_adapter", new_callable=Mock):
                wait_for_job(self.job, interval_in_s=0.01, max_iterations=1)
                self.assertEqual(self.job.status.string, "finished")

    def test_wait_for_jobs(self):
        self.job.status.created = True
        with patch("pyiron_base.jobs.job.extension.server.queuestatus.update_from_remote") as mock_update:
            with patch("pyiron_base.state.state.queue_adapter", new_callable=Mock):
                wait_for_jobs(self.project, interval_in_s=0.01, max_iterations=1)
                mock_update.assert_called()

    def test_update_from_remote(self):
        with patch("pyiron_base.state.state.queue_adapter", new_callable=Mock) as mock_adapter:
            mock_adapter.remote_flag = True
            mock_adapter.get_status_of_my_jobs.return_value = pd.DataFrame(
                {"jobname": ["pi_" + str(self.job.job_id)], "status": ["running"], "pyiron_id": [self.job.job_id]}
            )
            self.job.status.submitted = True
            update_from_remote(self.project)
            self.assertEqual(self.job.status.string, "running")

    def test_retrieve_job(self):
        self.job.status.submitted = True
        with patch.object(self.job, "transfer_from_remote") as mock_transfer:
            retrieve_job(self.job)
            mock_transfer.assert_called()

    def test_validate_que_request(self):
        self.assertEqual(validate_que_request(self.job), 123)
        with self.assertRaises(ValueError):
            job_no_id = self.project.create_job("ScriptJob", "no_id")
            job_no_id.server.queue_id = None
            validate_que_request(job_no_id)


if __name__ == "__main__":
    unittest.main()
