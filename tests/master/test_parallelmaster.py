# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from pyiron_base import JobGenerator, ParallelMaster
from pyiron_base._tests import TestWithProject, ToyJob


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


class TestParallelMaster(TestWithProject):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.master = cls.project.create_job(TestMaster, "master")
        cls.master.ref_job = cls.project.create_job(cls.project.job_type.ScriptJob, "ref")
        cls.master_toy = cls.project.create_job(TestMaster, "master_toy")
        cls.master_toy.ref_job = cls.project.create_job(ToyJob, "ref")
        cls.master_toy.run()

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

    def test_convergence(self):
        self.assertTrue(self.master_toy.convergence_check())
        self.assertTrue(self.master_toy.status.finished)
        # Make one of the children have a non-finished status
        self.master_toy[-1].status.aborted = True
        self.master_toy.status.collect = True
        self.master_toy.run()
        self.assertFalse(self.master_toy.convergence_check())
        self.assertTrue(self.master_toy.status.not_converged)


if __name__ == "__main__":
    unittest.main()
