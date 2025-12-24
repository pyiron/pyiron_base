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
    run_job_with_runmode_non_modal,
    run_job_with_status_submitted,
    run_job_with_status_running,
    run_job_with_status_initialized,
    multiprocess_wrapper,
    handle_failed_job,
    write_input_files_from_input_dict,
    run_job_with_status_collect,
    run_job_with_parameter_repair,
    run_job_with_status_finished,
    run_job_with_status_suspended,
    execute_command_with_error_handling,
    execute_job_with_external_executable,
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

    @patch("pyiron_base.jobs.job.runfunction.state")
    @patch("subprocess.Popen")
    def test_run_job_with_runmode_srun(self, mock_popen, mock_state):
        mock_state.database.database_is_disabled = False
        mock_state.database.using_local_database = False
        run_job_with_runmode_srun(self.job)
        mock_popen.assert_called_once()
        mock_state.database.using_local_database = True
        with self.assertRaises(ValueError):
            run_job_with_runmode_srun(self.job)

    @patch("pyiron_base.jobs.job.runfunction.state")
    def test_run_job_with_runmode_queue_no_adapter(self, mock_state):
        mock_state.queue_adapter = None
        with self.assertRaises(TypeError):
            run_job_with_runmode_queue(self.job)

    @patch("pyiron_base.jobs.job.runfunction.state")
    def test_run_job_with_status_submitted(self, mock_state):
        self.job.status.submitted = True
        self.job.server.run_mode.queue = True
        self.job.project.queue_check_job_is_waiting_or_running = Mock(
            return_value=False
        )
        self.job.run = Mock()
        mock_state.queue_adapter.remote_flag = False
        run_job_with_status_submitted(self.job)
        self.job.run.assert_called_with(delete_existing_job=True)

    def test_run_job_with_status_collect(self):
        self.job.status.collect = True
        self.job._job_with_calculate_function = True
        self.job._collect_output_funct = Mock(return_value={"output": 1})
        self.job.get_output_parameter_dict = Mock(return_value={})
        self.job.save_output = Mock()
        self.job.convergence_check = Mock(return_value=True)
        self.job.run_time_to_db = Mock()
        self.job.update_master = Mock()
        run_job_with_status_collect(self.job)
        self.job.save_output.assert_called_with(output_dict={"output": 1})

    def test_run_job_with_status_initialized(self):
        self.job.status.initialized = True
        self.job.validate_ready_to_run = Mock()
        self.job.check_if_job_exists = Mock(return_value=False)
        self.job.save = Mock()
        self.job.run = Mock()
        run_job_with_status_initialized(self.job)
        self.job.run.assert_called_once()

    def test_run_job_with_parameter_repair(self):
        self.job._run_if_created = Mock()
        run_job_with_parameter_repair(self.job)
        self.job._run_if_created.assert_called_once()

    def test_run_job_with_status_finished(self):
        self.job.from_hdf = Mock()
        with self.assertLogs(self.job.logger.name, level="WARNING"):
            run_job_with_status_finished(self.job)
        self.job.from_hdf.assert_called_once()

    def test_handle_failed_job(self):
        self.job.server.accept_crash = True
        error = subprocess.CalledProcessError(1, "cmd", output="error")
        crashed, out = handle_failed_job(self.job, error)
        self.assertTrue(crashed)
        self.assertEqual(out, "error")

        self.job.executable.accepted_return_codes = [1]
        crashed, out = handle_failed_job(self.job, error)
        self.assertFalse(crashed)
        self.assertEqual(out, "error")

    @patch("multiprocessing.Process")
    def test_run_job_with_runmode_non_modal(self, mock_process):
        with patch(
            "pyiron_base.jobs.job.generic.GenericJob.master_id",
            new_callable=unittest.mock.PropertyMock,
        ) as mock_master_id:
            mock_master_id.return_value = None
            run_job_with_runmode_non_modal(self.job)
            mock_process.assert_called_once()

    @patch("pyiron_base.jobs.job.runfunction.state")
    @patch("pyiron_base.jobs.job.runfunction.execute_subprocess")
    def test_execute_job_with_external_executable(self, mock_execute, mock_state):
        mock_state.database.database_is_disabled = True
        mock_execute.return_value = "test"
        self.job.executable.executable_path = "echo"
        self.job.executable.get_input_for_subprocess_call = Mock(
            return_value=("echo", True)
        )
        self.job.collect_output = Mock()
        self.job.run_time_to_db = Mock()
        self.job.refresh_job_status = Mock()
        self.job.update_master = Mock()
        os.makedirs(self.job.working_directory, exist_ok=True)
        execute_job_with_external_executable(self.job)
        mock_execute.assert_called_once()

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


class TestCalculateFunctionCaller(unittest.TestCase):
    def setUp(self):
        self.caller = CalculateFunctionCaller()

    @patch("pyiron_base.jobs.job.runfunction.execute_command_with_error_handling")
    def test_call(self, mock_execute):
        mock_execute.return_value = (False, "shell_output")
        self.caller.write_input_funct = Mock()
        self.caller.collect_output_funct = Mock(return_value="parsed_output")

        shell_output, parsed_output, job_crashed = self.caller(
            working_directory=".",
            input_parameter_dict={},
            executable_script="test.sh",
            shell_parameter=True,
        )

        self.caller.write_input_funct.assert_called_once()
        mock_execute.assert_called_once()
        self.caller.collect_output_funct.assert_called_once()
        self.assertEqual(shell_output, "shell_output")
        self.assertEqual(parsed_output, "parsed_output")
        self.assertFalse(job_crashed)


if __name__ == "__main__":
    unittest.main()
