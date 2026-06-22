# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from unittest.mock import MagicMock

from pyiron_base import ListMaster
from pyiron_base._tests import TestWithCleanProject, ToyJob


def transfer_output_to_input(job_old, job_new):
    job_new.input.data_in = job_old.output.data_out


class TestListMaster(TestWithCleanProject):
    def test_workflow(self):
        self.master_toy = self.project.create_job(ListMaster, "master_list")
        self.master_toy.append(self.project.create_job(ToyJob, "toy_1"))
        self.master_toy.append(self.project.create_job(ToyJob, "toy_2"))
        self.master_toy.run()
        self.assertEqual(len(self.project.job_table()), 3)
        self.assertEqual(
            self.project.load("toy_1").input.data_in,
            self.project.load("toy_2").input.data_in,
        )
        self.assertEqual(
            self.project.load("toy_1").output.data_out,
            self.project.load("toy_2").output.data_out,
        )

    def test_append_invalid_type_raises_type_error(self):
        master = self.project.create_job(ListMaster, "master_append_invalid")
        with self.assertRaises(TypeError):
            master.append("not a job")

    def test_append_finished_job_reconnects_as_child(self):
        standalone = self.project.create_job(ToyJob, "standalone_finished")
        standalone.run()
        self.assertIsNone(standalone.master_id)

        master = self.project.create_job(ListMaster, "master_reconnect")
        master.append(standalone)

        self.assertEqual(len(master.child_ids), 1)
        reloaded_child = self.project.load(master.child_ids[0])
        self.assertEqual(reloaded_child.master_id, master.job_id)
        self.assertTrue(master.status.finished)

    def test_append_already_connected_job_raises_value_error(self):
        standalone = self.project.create_job(ToyJob, "standalone_connected")
        standalone.run()

        first_master = self.project.create_job(ListMaster, "master_first_owner")
        first_master.append(standalone)
        reloaded_child = self.project.load(first_master.child_ids[0])

        second_master = self.project.create_job(ListMaster, "master_second_owner")
        with self.assertRaises(ValueError):
            second_master.append(reloaded_child)

    def test_is_finished_true_when_status_finished(self):
        master = self.project.create_job(ListMaster, "master_is_finished")
        master.status.finished = True
        self.assertTrue(master.is_finished())

    def test_is_finished_false_when_submission_not_finished(self):
        master = self.project.create_job(ListMaster, "master_submission_pending")
        master.submission_status.total_jobs = 2
        master.submission_status.submit_next()
        self.assertFalse(master.is_finished())

    def test_collect_output_and_run_if_interactive_are_noops(self):
        master = self.project.create_job(ListMaster, "master_noops")
        self.assertIsNone(master.collect_output())
        self.assertIsNone(master.run_if_interactive())

    def test_copy_preserves_child_ids(self):
        standalone = self.project.create_job(ToyJob, "copy_standalone")
        standalone.run()
        master = self.project.create_job(ListMaster, "master_copy_source")
        master.append(standalone)

        master_copy = master.copy()
        self.assertEqual(master_copy.child_ids, master.child_ids)

    def test_iter_jobs_yields_children(self):
        standalone_1 = self.project.create_job(ToyJob, "iter_standalone_1")
        standalone_1.run()
        standalone_2 = self.project.create_job(ToyJob, "iter_standalone_2")
        standalone_2.run()
        master = self.project.create_job(ListMaster, "master_iter")
        master.append(standalone_1)
        master.append(standalone_2)

        names = sorted(job.job_name for job in master.iter_jobs())
        self.assertEqual(names, ["iter_standalone_1", "iter_standalone_2"])

    def test_run_if_refresh_non_modal_finished_sets_status(self):
        master = self.project.create_job(ListMaster, "master_refresh_nonmodal")
        master.status.finished = True
        master.server.run_mode.mode = "non_modal"
        master.run_if_refresh()
        self.assertTrue(master.status.finished)

    def test_run_if_refresh_non_modal_pending_calls_run_static(self):
        master = self.project.create_job(ListMaster, "master_refresh_runstatic")
        master.append(self.project.create_job(ToyJob, "refresh_toy_1"))
        master.server.run_mode.mode = "non_modal"
        master.run_static = MagicMock()
        master.run_if_refresh()
        master.run_static.assert_called_once()

    def test_run_if_refresh_modal_refreshes_status(self):
        master = self.project.create_job(ListMaster, "master_refresh_modal")
        master.refresh_job_status = MagicMock()
        master.run_if_refresh()
        master.refresh_job_status.assert_called_once()

    def test_len_counts_children_and_pending_jobs(self):
        master = self.project.create_job(ListMaster, "master_len")
        master.append(self.project.create_job(ToyJob, "len_toy_1"))
        master.append(self.project.create_job(ToyJob, "len_toy_2"))
        self.assertEqual(len(master), 2)


if __name__ == "__main__":
    unittest.main()
