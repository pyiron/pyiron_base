# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
import os
from pyiron_base.project.generic import Project


class DatabasePropertyIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = os.path.dirname(os.path.abspath(__file__))
        cls.project = Project(os.path.join(cls.file_location, "hdf5_content"))
        cls.ham = cls.project.create_job('ScriptJob', "job_test_run")
        cls.ham.save()

    @classmethod
    def tearDownClass(cls):
        project = Project(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "hdf5_content")
        )
        ham = project.load(project.get_job_ids()[0])
        ham.remove()
        project.remove(enable=True)

    def test_inspect_job(self):
        job_inspect = self.project.inspect(self.ham.job_name)
        self.assertIsNotNone(job_inspect)
        self.assertEqual(
            job_inspect.content.input.__repr__(), job_inspect["input"].__repr__()
        )
        self.assertEqual(
            sorted(dir(job_inspect.content.input)),
            sorted(job_inspect["input"].list_nodes()
                    + job_inspect["input"].list_groups())
        )

    def test_setitem(self):
        self.ham['user/output/some_value'] = 0.3
        self.assertEqual(self.ham['user/output/some_value'], 0.3)
        with self.assertRaises(ValueError):
            self.ham['input/value'] = 1


if __name__ == "__main__":
    unittest.main()
