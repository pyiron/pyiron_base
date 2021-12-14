# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from pyiron_base.job.interactive import InteractiveBase
from pyiron_base._tests import TestWithProject


class TestJobInteractive(TestWithProject):
    def test_job_with(self):
        unittest.skipTest("Currently the interactive with statement does not work!")
        job = self.project.create_job(InteractiveBase, "job_modal")
        with self.assertRaises(AttributeError):
            with job as _:
                pass
                
    def test_job_interactive_with(self):
        unittest.skipTest("Currently the interactive with statement does not work!")
        job = self.project.create_job(InteractiveBase, "job_interactive")
        job.project.db.add_item_dict(job.db_entry())
        with job.interactive_open() as job_int:
            job_int.to_hdf()
        self.assertTrue(job.server.run_mode.interactive)
