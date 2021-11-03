# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base._tests import TestWithCleanProject
from pyiron_base.job.factory import JobFactory
from pyiron_base.job.script import ScriptJob
from pyiron_base.state import state


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

    def test_call(self):
        job = self.factory('ScriptJob', 'foo.bar')
        self.assertIsInstance(job, ScriptJob, msg=f"Got a {type(job)} instead of a ScriptJob")
        self.assertEqual('foo_bar', job.name, msg=f"Job name failed to set, expected foo but got {job.name}")
        self.assertEqual(
            state.settings.login_user, job.user,
            msg=f"Expected user from settings, {state.settings.login_user}, but got user {job.user}."
        )
