# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
import os
from pyiron_base.project.generic import Project


class TestScriptJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = os.path.dirname(os.path.abspath(__file__)).replace(
            "\\", "/"
        )
        cls.project = Project(os.path.join(cls.file_location, "test_sriptjob"))

    @classmethod
    def tearDownClass(cls):
        cls.project.remove(enable=True)

    def test_script_path(self):
        job = self.project.create_job('ScriptJob', 'script')
        with self.assertRaises(TypeError):
            job.run()

if __name__ == "__main__":
    unittest.main()
