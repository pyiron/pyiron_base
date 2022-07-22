# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base._tests import TestWithProject
from pyiron_base.jobs.job.toolkit import BaseTools


class TestToolkit(TestWithProject):
    def setUp(self):
        super().setUp()
        self.tools = BaseTools(self.project)

    def test_job(self):
        self.tools.job.ScriptJob('foo')
        with self.assertRaises(AttributeError):
            self.tools.job.NotAJobAtAll('bar')
