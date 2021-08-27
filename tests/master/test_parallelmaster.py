# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
import os
from pyiron_base.project.generic import Project
from pyiron_base import JobGenerator, ParallelMaster

class TestGenerator(JobGenerator):

    test_length = 10

    @property
    def parameter_list(self):
        return list(range(self.test_length))

    def job_name(self, parameter):
        return "test_{}".format(parameter)

    @staticmethod
    def modify_job(job, parameter):
        job.input['parameter'] = parameter
        return job

class TestMaster(ParallelMaster):

    def __init__(self, job_name, project):
        super().__init__(job_name, project)
        self._job_generator = TestGenerator(self)

class TestParallelMaster(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = os.path.dirname(os.path.abspath(__file__))
        cls.project = Project(os.path.join(cls.file_location, "jobs_testing"))
        cls.master = cls.project.create_job(TestMaster, "master")
        cls.master.ref_job = cls.project.create_job(cls.project.job_type.ScriptJob, "ref")

    def test_jobgenerator_name(self):
        """Generated jobs have to be unique instances, in order, the correct name and correct parameters."""
        self.assertEqual(len(self.master._job_generator), TestGenerator.test_length,
                         "Incorrect length.")
        job_set = set()
        for i, j in zip(self.master._job_generator.parameter_list,
                        self.master._job_generator):
            self.assertTrue(j not in job_set,
                            "Returned job instance is not a copy.")
            self.assertEqual(j.name, "test_{}".format(i),
                             "Incorrect job name.")
            self.assertEqual(j.input['parameter'], i,
                             "Incorrect parameter set on job.")

    def test_child_creation(self):
        """When creating an interactive wrapper from another job, that should be set as the wrapper's reference job."""
        j = self.project.create.job.ScriptJob("test_parent")
        j.server.run_mode = 'interactive'
        i = j.create_job(TestMaster, "test_child")
        self.assertEqual(i.ref_job, j, "Reference job of interactive wrapper to set after creation.")


if __name__ == "__main__":
    unittest.main()
