# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from pyiron_base.jobs.job.interactive import InteractiveBase
from pyiron_base._tests import TestWithProject


class TestJobInteractive(TestWithProject):
    def test_job_with(self):
        job = self.project.create_job(InteractiveBase, "job_modal")
        with self.assertRaises((TypeError, AttributeError)):
            with job as _:
                pass

    def test_job_interactive_with(self):
        job = self.project.create_job(InteractiveBase, "job_interactive")
        job.project.db.add_item_dict(job.db_entry())
        with job.interactive_open() as job_int:
            job_int.to_hdf()
        self.assertTrue(job.server.run_mode.interactive)

    def test_interactive_flush_frequency_setter(self):
        job = self.project.create_job(InteractiveBase, "job_interactive_flush_frequency")
        with self.assertRaises(AssertionError):
            job.interactive_flush_frequency = 0
        with self.assertRaises(AssertionError):
            job.interactive_flush_frequency = -10
        with self.assertRaises(AssertionError):
            job.interactive_flush_frequency = 1.5

        job.interactive_flush_frequency = 10
        self.assertEqual(job.interactive_flush_frequency, 10)

    def test_interactive_write_frequency_setter(self):
        job = self.project.create_job(InteractiveBase, "job_interactive_write_frequency")
        with self.assertRaises(AssertionError):
            job.interactive_write_frequency = 0
        with self.assertRaises(AssertionError):
            job.interactive_write_frequency = -10
        with self.assertRaises(AssertionError):
            job.interactive_write_frequency = 1.5

        # default flush_frequency is 10000
        job.interactive_write_frequency = 10
        self.assertEqual(job.interactive_write_frequency, 10)
        self.assertEqual(job.interactive_flush_frequency, 10000)

        job.interactive_write_frequency = 20000
        self.assertEqual(job.interactive_write_frequency, 20000)
        self.assertEqual(job.interactive_flush_frequency, 20000)

    def test_validate_ready_to_run(self):
        job = self.project.create_job(InteractiveBase, "job_validate_ready_to_run")
        job._interactive_flush_frequency = 5
        job._interactive_write_frequency = 10
        with self.assertRaises(ValueError):
            job.validate_ready_to_run()

    def test_run_if_interactive(self):
        job = self.project.create_job(InteractiveBase, "job_run_if_interactive")
        with self.assertRaises(NotImplementedError):
            job.run_if_interactive()

    def test_run_if_interactive_non_modal(self):
        job = self.project.create_job(InteractiveBase, "job_run_if_interactive_non_modal")
        with self.assertRaises(NotImplementedError):
            job.run_if_interactive_non_modal()

    def test_run_if_running(self):
        job = self.project.create_job(InteractiveBase, "job_run_if_running")
        job.server.run_mode.interactive = True
        with self.assertRaises(NotImplementedError):
            job._run_if_running()
        job.server.run_mode.interactive_non_modal = True
        with self.assertRaises(NotImplementedError):
            job._run_if_running()

    def test_hdf_io(self):
        job = self.project.create_job(InteractiveBase, "job_hdf_io")
        job.interactive_flush_frequency = 20
        job.interactive_write_frequency = 5
        job.to_hdf()
        job.from_hdf()
        self.assertEqual(job.interactive_flush_frequency, 20)
        self.assertEqual(job.interactive_write_frequency, 5)

    def test_interactive_flush(self):
        job = self.project.create_job(InteractiveBase, "job_interactive_flush")
        job.interactive_cache["test_key"] = ["test_value"]
        job.interactive_flush(path="test_path")
        self.assertEqual(job.project_hdf5["output/test_path/test_key"], "['test_value']")

    def test_interactive_close(self):
        job = self.project.create_job(InteractiveBase, "job_interactive_close")
        job.interactive_cache["test_key"] = ["test_value"]
        job.interactive_close()
        self.assertEqual(job.status.string, "finished")
        self.assertEqual(job.project_hdf5["output/interactive/test_key"], "['test_value']")

    def test_interactive_store_in_cache(self):
        job = self.project.create_job(InteractiveBase, "job_interactive_store_in_cache")
        job.interactive_store_in_cache("test_key", "test_value")
        self.assertEqual(job.interactive_cache["test_key"], "test_value")

    def test_check_if_input_should_be_written(self):
        job = self.project.create_job(InteractiveBase, "job_check_if_input_should_be_written")
        job.status.created = True
        self.assertTrue(job._check_if_input_should_be_written())
        job._interactive_write_input_files = True
        self.assertTrue(job._check_if_input_should_be_written())

    def test_interactive_is_activated(self):
        job = self.project.create_job(InteractiveBase, "job_interactive_is_activated")
        self.assertFalse(job.interactive_is_activated())
        job._interactive_library = "test"
        self.assertTrue(job.interactive_is_activated())

    def test_with_interactive_open(self):
        job = self.project.create_job(InteractiveBase, "job_with_interactive_open")
        interactive_open_context = job.interactive_open()
        self.assertEqual(repr(interactive_open_context), "Interactive ready")
        with interactive_open_context as job_int:
            self.assertIs(job, job_int)
        self.assertTrue(job.status.finished)
        with self.assertRaises(ValueError):
            job.interactive_open().some_random_method()

    def test_include_last_step(self):
        job = self.project.create_job(InteractiveBase, "job_include_last_step")
        self.assertEqual(job._include_last_step([1, 2, 3], 2, True), [1, 3])
        self.assertEqual(job._include_last_step([1, 2, 3], 2, False), [1, 3])
        self.assertEqual(job._include_last_step([1, 2, 3], 1, True), [1, 2, 3])
        self.assertEqual(job._include_last_step([1, 2, 3], 4, True), [3])
        self.assertEqual(job._include_last_step([], 2, True), [])
