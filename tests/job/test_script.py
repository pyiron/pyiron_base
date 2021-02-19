# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from os.path import dirname, abspath, join
from pyiron_base.project.generic import Project


class TestScriptJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = dirname(abspath(__file__)).replace("\\", "/")
        cls.project = Project(join(cls.file_location, "test_sriptjob"))

    @classmethod
    def tearDownClass(cls):
        cls.project.remove(enable=True)

    def test_script_path(self):
        job = self.project.create.job.ScriptJob('script')
        with self.assertRaises(TypeError):
            job.run()


if __name__ == "__main__":
    unittest.main()
