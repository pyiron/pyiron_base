# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
from unittest.mock import MagicMock

from pyiron_base.project.generic import Project
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.jobs.master.interactivewrapper import InteractiveWrapper
from pyiron_base._tests import PyironTestCase


class TestInteractiveWrapper(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.file_location = os.path.dirname(os.path.abspath(__file__)).replace(
            "\\", "/"
        )
        cls.project = Project(os.path.join(cls.file_location, "test_interactive"))

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.project.remove_jobs(recursive=True, progress=False, silently=True)

    def test_child_creation(self):
        """When creating an interactive wrapper from another job, that should be set as the wrapper's reference job."""
        j = self.project.create.job.ScriptJob("test_parent")
        j.server.run_mode = "interactive"
        i = j.create_job(InteractiveWrapper, "test_child")
        self.assertEqual(
            i.ref_job, j, "Reference job of interactive wrapper to set after creation."
        )

    def test_ref_job_is_none_when_empty(self):
        wrapper = self.project.create_job(InteractiveWrapper, "wrapper_empty")
        self.assertIsNone(wrapper.ref_job)

    def test_ref_job_setter_warns_if_not_interactive(self):
        wrapper = self.project.create_job(InteractiveWrapper, "wrapper_warn")
        ref_job = self.project.create_job(GenericJob, "ref_job_not_interactive")
        with self.assertWarns(UserWarning):
            wrapper.ref_job = ref_job
        self.assertEqual(wrapper.ref_job, ref_job)

    def test_set_input_to_read_only(self):
        wrapper = self.project.create_job(InteractiveWrapper, "wrapper_read_only")
        wrapper.set_input_to_read_only()
        self.assertTrue(wrapper.input.read_only)

    def test_validate_ready_to_run_delegates_to_ref_job(self):
        wrapper = self.project.create_job(InteractiveWrapper, "wrapper_validate")
        wrapper.append(
            self.project.create_job(GenericJob, "ref_job_validate")
        )
        wrapper.ref_job.validate_ready_to_run = MagicMock()
        wrapper.validate_ready_to_run()
        wrapper.ref_job.validate_ready_to_run.assert_called_once()

    def test_check_setup_swallows_attribute_error_when_no_ref_job(self):
        wrapper = self.project.create_job(InteractiveWrapper, "wrapper_check_setup")
        wrapper.check_setup()

    def test_ref_job_initialize_sets_master_id(self):
        wrapper = self.project.create_job(InteractiveWrapper, "wrapper_ref_init")
        wrapper.append(self.project.create_job(GenericJob, "ref_job_init"))
        wrapper.save()
        wrapper._ref_job = None
        wrapper.ref_job_initialize()
        self.assertEqual(wrapper.ref_job.master_id, wrapper.job_id)

    def test_to_hdf_from_hdf_round_trip(self):
        wrapper = self.project.create_job(InteractiveWrapper, "wrapper_hdf")
        wrapper.append(self.project.create_job(GenericJob, "ref_job_hdf"))
        wrapper.input["alpha"] = 42
        wrapper.to_hdf()

        reloaded = self.project.create_job(InteractiveWrapper, "wrapper_hdf_reload")
        reloaded.project_hdf5 = wrapper.project_hdf5
        reloaded.from_hdf()
        self.assertEqual(reloaded.input["alpha"], 42)

    def test_db_entry_update_run_time(self):
        wrapper = self.project.create_job(InteractiveWrapper, "wrapper_runtime")
        wrapper.save()
        wrapper._db_entry_update_run_time()
        entry = self.project.db.get_item_by_id(wrapper.job_id)
        self.assertIsNotNone(entry["totalcputime"])

    def test_finish_job_sets_status_finished(self):
        wrapper = self.project.create_job(InteractiveWrapper, "wrapper_finish")
        wrapper.save()
        wrapper._finish_job()
        self.assertTrue(wrapper.status.finished)
