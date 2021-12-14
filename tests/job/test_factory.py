# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base._tests import TestWithCleanProject
from pyiron_base.job.factory import JobFactory
from pyiron_base.job.script import ScriptJob
from pyiron_base.state import state
from pyiron_base.job.template import PythonTemplateJob


class CustomJob(PythonTemplateJob):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self.input.foo = 42

    def run_static(self):
        self.status.running = True
        self.output.bar = self.input.foo / 6
        self.status.finished = True
        self.to_hdf()


class TestJobFactory(TestWithCleanProject):
    def setUp(self) -> None:
        super().setUp()
        self.factory = JobFactory(self.project)

    def test_core(self):
        job = self.factory.ScriptJob('foo')

        job.status.aborted = True
        self.assertLogs(self.factory.ScriptJob('foo'), level='WARNING')  # Job aborted warning

        job.input.foo = 'foo'
        job = self.factory.ScriptJob('foo', delete_existing_job=True)
        with self.assertRaises(Exception):
            job.input.foo  # Shouldn't exist after deleting existing job

    def test_base(self):
        self.assertGreater(len(self.factory._job_class_dict), 0)

    def test_call_standard(self):
        job = self.factory('ScriptJob', 'foo.bar')
        self.assertIsInstance(job, ScriptJob, msg=f"Got a {type(job)} instead of a ScriptJob")
        self.assertEqual('foodbar', job.name, msg=f"Job name failed to set, expected foo but got {job.name}")
        self.assertEqual(
            state.settings.login_user, job.user,
            msg=f"Expected user from settings, {state.settings.login_user}, but got user {job.user}."
        )

    def test_call_custom(self):
        job = self.factory(CustomJob, 'custom')
        job.run()
        table = self.project.job_table()
        self.assertIn(job.name, table.job.values, msg="Project failed to save job name")
        self.assertIn(job.__class__.__name__, table.hamilton.values, msg="Project failed to save job type")
        self.assertEqual(7, job.output.bar, msg="Job failed to save output correctly")

        loaded = self.project.load(job.name)
        self.assertIsInstance(loaded, CustomJob, msg="Loading caused custom job to lose its type")
        self.assertEqual(7, loaded.output.bar, msg="Loading caused custom job to lose its stored output")
