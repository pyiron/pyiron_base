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
        self.job.job_id = 1

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

    @patch("subprocess.Popen")
    def test_run_job_with_runmode_srun(self, mock_popen):
        self.job.server.run_mode.srun = True
        self.job.project_hdf5.create_working_directory()
        with patch(
            "pyiron_base.state.database.database_is_disabled", return_value=False
        ):
            with patch(
                "pyiron_base.state.database.using_local_database", return_value=True
            ):
                with self.assertRaises(ValueError):
                    run_job_with_runmode_srun(self.job)

        with patch(
            "pyiron_base.state.database.database_is_disabled", return_value=True
        ):
            run_job_with_runmode_srun(self.job)
            command = (
                "srun python -m pyiron_base.cli wrapper -p "
                + self.job.working_directory
                + " -f "
                + self.job.project_hdf5.file_name
                + self.job.project_hdf5.h5_path
            )
            mock_popen.assert_called_with(
                command,
                cwd=self.job.working_directory,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                env=os.environ.copy(),
            )

    def test_run_job_with_runmode_srun_no_db(self):
        self.job.server.run_mode.srun = True
        self.job.project_hdf5.create_working_directory()
        state.database.database_is_disabled = True
        with patch("subprocess.Popen") as mock_popen:
            run_job_with_runmode_srun(self.job)
        state.database.database_is_disabled = False

    @patch("pyiron_base.state.queue_adapter", new_callable=MagicMock)
    def test_run_job_with_runmode_queue(self, mock_adapter):
        self.job.server.run_mode.queue = True
        mock_adapter.remote_flag = True
        mock_adapter.convert_path_to_remote.side_effect = lambda path: "remote_" + path
        mock_adapter.submit_job.return_value = 123
        self.job.project_hdf5.create_working_directory()
        self.job.write_input = Mock()

        run_job_with_runmode_queue(self.job)

        self.job.write_input.assert_called_once()
        mock_adapter.transfer_file_to_remote.assert_called_once_with(
            file=self.job.project_hdf5.file_name, transfer_back=False
        )
        command = (
            "python -m pyiron_base.cli wrapper -p "
            + "remote_"
            + self.job.working_directory
            + " -f "
            + "remote_"
            + self.job.project_hdf5.file_name
            + self.job.project_hdf5.h5_path
            + " --submit"
        )
        mock_adapter.submit_job.assert_called_once()
        self.assertEqual(self.job.server.queue_id, 123)

    @patch("pyiron_base.state.queue_adapter", new_callable=MagicMock)
    def test_run_job_with_runmode_queue_no_id(self, mock_adapter):
        self.job.server.run_mode.queue = True
        mock_adapter.remote_flag = False
        mock_adapter.submit_job.return_value = None
        with self.assertRaises(ValueError):
            run_job_with_runmode_queue(self.job)
        self.assertTrue(self.job.status.aborted)

    def test_run_job_with_status_submitted(self):
        self.job.status.submitted = True
        self.job.server.run_mode.queue = True
        self.job.project.queue_check_job_is_waiting_or_running = Mock(
            return_value=False
        )
        self.job.run = Mock()
        self.job.transfer_from_remote = Mock()

        with patch("pyiron_base.state.queue_adapter.remote_flag", True):
            run_job_with_status_submitted(self.job)
            self.job.transfer_from_remote.assert_called_once()
            self.job.run.assert_not_called()

        with patch("pyiron_base.state.queue_adapter.remote_flag", False):
            run_job_with_status_submitted(self.job)
            self.job.run.assert_called_with(delete_existing_job=True)

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

    def test_handle_failed_job(self):
        self.job.server.accept_crash = False
        error = subprocess.CalledProcessError(1, "cmd", output="error")
        with patch(
            "pyiron_base.jobs.job.runfunction.raise_runtimeerror_for_failed_job"
        ) as mock_raise:
            handle_failed_job(self.job, error)
            mock_raise.assert_called_once()

        self.job.server.accept_crash = True
        crashed, out = handle_failed_job(self.job, error)
        self.assertTrue(crashed)
        self.assertEqual(out, "error")

        error = subprocess.CalledProcessError(0, "cmd", output="ok")
        self.job.executable.accepted_return_codes = [0]
        crashed, out = handle_failed_job(self.job, error)
        self.assertFalse(crashed)
        self.assertEqual(out, "ok")

    def test_write_input_files_from_input_dict(self):
        input_dict = {"files_to_create": {"file1": "content1"}, "files_to_copy": {}}
        with patch("os.listdir", return_value=["file1"]):
            write_input_files_from_input_dict(input_dict, self.job.working_directory)
            # test that nothing is written if file exists
            self.assertFalse(
                os.path.exists(os.path.join(self.job.working_directory, "file1"))
            )

    def test_run_job_with_status_collect(self):
        self.job._job_with_calculate_function = True
        self.job._collect_output_funct = Mock()
        self.job.get_output_parameter_dict = Mock(return_value={})
        self.job.save_output = Mock()
        self.job.status.collect = True
        self.job.convergence_check = Mock(return_value=False)
        run_job_with_status_collect(self.job)
        self.assertTrue(self.job.status.not_converged)

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

    def test_calculate_function_caller(self):
        caller = CalculateFunctionCaller(collect_output_funct=Mock())
        with patch(
            "pyiron_base.jobs.job.runfunction.execute_command_with_error_handling"
        ) as mock_exec:
            mock_exec.return_value = (False, "output")
            caller("wd", {}, "exec", False)
            caller.collect_output_funct.assert_called_once()


if __name__ == "__main__":
    unittest.main()
