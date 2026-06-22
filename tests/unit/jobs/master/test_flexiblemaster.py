# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from unittest.mock import MagicMock, patch

from pyiron_base import FlexibleMaster
from pyiron_base._tests import TestWithCleanProject, ToyJob


def transfer_output_to_input(job_old, job_new):
    job_new.input.data_in = job_old.output.data_out


class TestFlexibleMaster(TestWithCleanProject):
    def test_workflow(self):
        self.master_toy = self.project.create_job(FlexibleMaster, "master_flex")
        self.master_toy.append(self.project.create_job(ToyJob, "toy_1"))
        self.master_toy.append(self.project.create_job(ToyJob, "toy_2"))
        self.master_toy.function_lst.append(transfer_output_to_input)
        self.master_toy.run()
        self.assertEqual(len(self.project.job_table()), 3)
        self.assertEqual(
            self.project.load("toy_1").output.data_out,
            self.project.load("toy_2").input.data_in,
        )

    def test_validate_ready_to_run_not_enough_job_names(self):
        master = self.project.create_job(FlexibleMaster, "master_not_enough_names")
        master.append(self.project.create_job(ToyJob, "names_toy_1"))
        master.function_lst.append(transfer_output_to_input)
        with self.assertRaises(ValueError):
            master.validate_ready_to_run()

    def test_validate_ready_to_run_not_enough_step_functions(self):
        master = self.project.create_job(FlexibleMaster, "master_not_enough_functs")
        master.append(self.project.create_job(ToyJob, "functs_toy_1"))
        master.append(self.project.create_job(ToyJob, "functs_toy_2"))
        with self.assertRaises(ValueError):
            master.validate_ready_to_run()

    def test_is_finished_when_status_finished(self):
        master = self.project.create_job(FlexibleMaster, "master_is_finished")
        master.status.finished = True
        self.assertTrue(master.is_finished())

    def test_is_finished_false_when_jobs_pending(self):
        master = self.project.create_job(FlexibleMaster, "master_pending")
        master.append(self.project.create_job(ToyJob, "pending_toy_1"))
        self.assertFalse(master.is_finished())

    def test_collect_output_and_run_if_interactive_are_noops(self):
        master = self.project.create_job(FlexibleMaster, "master_noops")
        self.assertIsNone(master.collect_output())
        self.assertIsNone(master.run_if_interactive())

    def test_run_if_refresh_when_already_finished(self):
        master = self.project.create_job(FlexibleMaster, "master_refresh_finished")
        master.status.finished = True
        master.run = MagicMock()
        master.run_if_refresh()
        self.assertTrue(master.status.collect)
        master.run.assert_called_once()

    def test_run_if_refresh_modal_calls_run_static(self):
        master = self.project.create_job(FlexibleMaster, "master_refresh_modal")
        master.append(self.project.create_job(ToyJob, "refresh_toy_1"))
        master.run_static = MagicMock()
        master.run_if_refresh()
        master.run_static.assert_called_once()

    def test_run_if_refresh_manual_mode_refreshes_status(self):
        master = self.project.create_job(FlexibleMaster, "master_refresh_manual")
        master.append(self.project.create_job(ToyJob, "refresh_toy_2"))
        master.server.run_mode.mode = "manual"
        master.refresh_job_status = MagicMock()
        master.run_if_refresh()
        master.refresh_job_status.assert_called_once()

    def test_to_hdf_ignores_io_error_from_getsource(self):
        master = self.project.create_job(FlexibleMaster, "master_hdf_ioerror")
        master.append(self.project.create_job(ToyJob, "ioerror_toy_1"))
        master.append(self.project.create_job(ToyJob, "ioerror_toy_2"))
        master.function_lst.append(transfer_output_to_input)
        with patch(
            "pyiron_base.jobs.master.flexible.inspect.getsource",
            side_effect=IOError,
        ):
            master.to_hdf()


if __name__ == "__main__":
    unittest.main()
