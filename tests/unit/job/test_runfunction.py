# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from unittest.mock import Mock, patch, MagicMock
from pyiron_base._tests import TestWithProject
from pyiron_base.jobs.job.runfunction import (
    run_job_with_status_refresh,
    run_job_with_status_busy,
    run_job_with_runmode_manually,
    run_job_with_runmode_srun,
    run_job_with_runmode_queue,
    run_job_with_status_submitted,
    run_job_with_status_running,
    multiprocess_wrapper,
    handle_failed_job,
    write_input_files_from_input_dict,
    run_job_with_status_collect,
    run_job_with_status_suspended,
    execute_command_with_error_handling,
    CalculateFunctionCaller,
)
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.state import state
import os
import subprocess
import io


class TestRunfunction(TestWithProject):
    def setUp(self):
        super().setUp()
        self.job = self.project.create_job(GenericJob, "test_job")
        self.job._job_id = 1

    def tearDown(self):
        self.project.remove_job("test_job")
        super().tearDown()

    def test_run_job_with_status_refresh(self):
        with self.assertRaises(NotImplementedError):
            run_job_with_status_refresh(self.job)

    def test_run_job_with_status_busy(self):
        with self.assertRaises(NotImplementedError):
            run_job_with_status_busy(self.job)

    def test_run_job_with_runmode_manually(self):
        with patch("sys.stdout", new_callable=io.StringIO) as fake_out:
            run_job_with_runmode_manually(self.job, _manually_print=False)
            self.assertEqual(fake_out.getvalue(), "")

    def test_run_job_with_status_running(self):
        self.job.status.running = True
        self.job.server.run_mode.queue = True
        self.job.project.queue_check_job_is_waiting_or_running = Mock(
            return_value=False
        )
        self.job.run = Mock()
        run_job_with_status_running(self.job)
        self.job.run.assert_called_with(delete_existing_job=True)

    @patch("pyiron_base.jobs.job.runfunction.JobWrapper")
    def test_multiprocess_wrapper(self, mock_job_wrapper):
        with self.assertRaises(ValueError):
            multiprocess_wrapper(working_directory=".", job_id=None, file_path=None)

        multiprocess_wrapper(
            working_directory=".", job_id=None, file_path="test.h5/h5_path"
        )
        mock_job_wrapper.assert_called_with(
            ".",
            job_id=None,
            hdf5_file="test.h5",
            h5_path="/h5_path",
            debug=False,
            connection_string=None,
        )

    def test_write_input_files_from_input_dict(self):
        input_dict = {"files_to_create": {"file1": "content1"}, "files_to_copy": {}}
        with patch("os.listdir", return_value=["file1"]):
            write_input_files_from_input_dict(input_dict, self.job.working_directory)
            # test that nothing is written if file exists
            self.assertFalse(
                os.path.exists(os.path.join(self.job.working_directory, "file1"))
            )

    def test_run_job_with_status_suspended(self):
        self.job.run = Mock()
        run_job_with_status_suspended(self.job)
        self.assertTrue(self.job.status.refresh)
        self.job.run.assert_called_once()

    @patch("subprocess.run")
    def test_execute_command_with_error_handling_error(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, "cmd", output="error")
        with self.assertRaises(RuntimeError):
            execute_command_with_error_handling("cmd", True, ".")

        mock_run.side_effect = FileNotFoundError("file not found")
        with self.assertRaises(RuntimeError):
            execute_command_with_error_handling("cmd", True, ".")


if __name__ == "__main__":
    unittest.main()
