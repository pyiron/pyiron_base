# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from os.path import dirname, abspath, join
from os import remove
from pyiron_base.project.generic import Project


class TestScriptJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = dirname(abspath(__file__)).replace("\\", "/")
        cls.project_abspath = join(cls.file_location, "test_sriptjob").replace("\\", "/")  # replace to satisfy windows
        cls.project = Project(cls.project_abspath)
        cls.simple_script = join(cls.file_location, 'simple.py')
        cls.complex_script = join(cls.file_location, 'complex.py')

    @classmethod
    def tearDownClass(cls):
        cls.project.remove(enable=True)
        for file in [cls.simple_script, cls.complex_script]:
            try:
                remove(file)
            except FileNotFoundError:
                continue

    def setUp(self):
        self.job = self.project.create.job.ScriptJob('script')

    def tearDown(self):
        self.project.remove_job('script')

    def test_script_path(self):
        with self.assertRaises(TypeError):
            self.job.run()

        with open(self.simple_script, 'w') as f:
            f.write("print(42)")
        self.job.script_path = self.simple_script
        self.job.run()

    def test_project_data(self):
        self.project.data.in_ = 6
        self.project.data.write()
        with open(self.complex_script, 'w') as f:
            f.write("from pyiron_base import Project\n")
            f.write(f"pr = Project('{self.project_abspath}')\n")
            f.write("pr.data.out = pr.data.in_ * 7\n")
            f.write("pr.data.write()\n")
        # WARNING: If a user parallelizes this with multiple ScriptJobs, it would be possible to get a log jam with
        #          multiple simultaneous write-calls.
        self.job.script_path = self.complex_script
        self.job.run()
        self.project.data.read()
        self.assertEqual(42, self.project.data.out)


if __name__ == "__main__":
    unittest.main()
