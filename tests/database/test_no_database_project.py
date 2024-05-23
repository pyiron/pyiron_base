# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
import unittest
from pyiron_base._tests import TestWithProject
from pyiron_base import state


class TestNoDatabaseProject(TestWithProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        state.update(config_dict={"disable_database": True})

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_validate_database_is_disables(self):
        self.assertTrue(state.settings.configuration["disable_database"])

    @unittest.skipIf(os.name == "nt", "FileTable is not supported on windows.")
    def test_deleted_jobs_jobstatus(self):
        state.update(config_dict={"disable_database": True})
        job = self.project.create.job.ScriptJob("test")
        job.script_path = __file__
        job.server.run_mode.manual = True
        job.run()
        df = self.project.job_table()
        self.assertEqual(len(df), 1)
        self.assertEqual(df.status.values[0], "initialized")
        os.remove(job.project_hdf5.file_name)
        self.project.db.force_reset()
        df = self.project.job_table()
        self.assertEqual(len(df), 0)
        job.remove()  # Clean up the orphaned job HDF


if __name__ == "__main__":
    unittest.main()
