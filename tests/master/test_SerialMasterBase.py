# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
import os
from pyiron_base.project.generic import Project
from pyiron_base.master.serial import SerialMasterBase

class TestGenericJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = os.path.dirname(os.path.abspath(__file__))
        cls.project = Project(os.path.join(cls.file_location, "jobs_testing"))

    @classmethod
    def tearDownClass(cls):
        file_location = os.path.dirname(os.path.abspath(__file__))
        project = Project(os.path.join(file_location, "jobs_testing"))
        project.remove(enforce=True, enable=True)

    def test_generic_jobs(self):
        ham = self.project.create.job.ScriptJob("job_single")
        job_ser = self.project.create.job.SerialMasterBase("job_list")
        job_ser.append(ham)
        job_ser.to_hdf()
        job_ser_reload = self.project.create.job.SerialMasterBase("job_list")
        job_ser_reload.from_hdf()
        self.assertTrue(job_ser_reload['job_single/input/custom_dict'])
        job_ser.remove()
        ham.remove()

    def test_copy(self):
        ham = self.project.create.job.ScriptJob("job_single")
        job_ser = self.project.create.job.SerialMasterBase("job_list")
        job_ser.append(ham)
        job_ser_copied = job_ser.copy()
        self.assertTrue(job_ser.job_name, job_ser_copied.job_name)

    def test_generic_jobs_ex(self):
        ham = self.project.create.job.ScriptJob("job_single_ex")
        ham.to_hdf()
        job_ser = self.project.create.job.SerialMasterBase("job_list_ex")
        job_ser.append(ham)
        job_ser.to_hdf()
        self.assertTrue(job_ser['job_single_ex/input/custom_dict'])
        job_ser_reload = self.project.create.job.SerialMasterBase("job_list_ex")
        job_ser_reload.from_hdf()
        self.assertTrue(job_ser_reload['job_single_ex/input/custom_dict'])
        job_ser.remove()
        ham.remove()

    def test_child_creation(self):
        """When creating an interactive wrapper from another job, that should be set as the wrapper's reference job."""
        j = self.project.create.job.ScriptJob("test_parent")
        j.server.run_mode = 'interactive'
        i = j.create_job(SerialMasterBase, "test_child")
        self.assertEqual(i.ref_job, j, "Reference job of interactive wrapper to set after creation.")

if __name__ == "__main__":
    unittest.main()
