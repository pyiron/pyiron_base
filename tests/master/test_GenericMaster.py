# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
import unittest
from pyiron_base.project.generic import Project
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.jobs.master.generic import GenericMaster
from pyiron_base._tests import PyironTestCase


class TestGenericJob(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = os.path.dirname(os.path.abspath(__file__))
        cls.project = Project(os.path.join(cls.file_location, "master_copy"))

    @classmethod
    def tearDownClass(cls):
        file_location = os.path.dirname(os.path.abspath(__file__))
        project = Project(os.path.join(file_location, "master_copy"))
        project.remove(enforce=True, enable=True)

    def test_copy_to(self):
        child = self.project.create_job(
            job_type=GenericJob,
            job_name='child'
        )
        master = self.project.create_job(
            job_type=GenericMaster,
            job_name='master'
        )
        master.append(child)
        master_copy = master.copy_to(
            project=self.project,
            new_job_name='copy',
            new_database_entry=False
        )
        self.assertEqual(
            len(master._job_object_dict.keys()),
            len(master_copy._job_object_dict.keys())
        )
        self.assertTrue(isinstance(master[0], GenericJob))
        self.assertTrue(isinstance(master_copy[0], GenericJob))


if __name__ == "__main__":
    unittest.main()
