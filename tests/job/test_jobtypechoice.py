# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from pyiron_base.job.jobtype import JobTypeChoice, JobCreator
from pyiron_base import JOB_CLASS_DICT


class TestJobTypeChoice(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.jobtypechoice = JobTypeChoice()

    def test_dir(self):
        """
        All job types in JOB_CLASS_DICT need to be returned in __dir__ for
        autocompletion.
        """
        self.assertTrue(all(k in dir(self.jobtypechoice)
                                for k in JOB_CLASS_DICT),
                        "Not all job classes returned by dir()")

    def test_attr(self):
        """
        All job types in JOB_CLASS_DICT need to be available as attributes and
        should have their string names as values.
        """
        try:
            for k in JOB_CLASS_DICT:
                getattr(self.jobtypechoice, k)
        except AttributeError:
            self.fail("job class {} in JOB_CLASS_DICT, but not on "
                      "JobTypeChoice".format(k))

    def test_extend_job_class_dict(self):
        """
        Attributes on JobTypeChoice should reflect all job types set in
        JOB_CLASS_DICT even if it changes after it is created.
        """
        JOB_CLASS_DICT["TestClass"] = "my.own.test.module"
        self.assertTrue("TestClass" in dir(self.jobtypechoice),
                        "new job class added to JOB_CLASS_DICT, but not "
                        "returned in dir()")
        try:
            getattr(self.jobtypechoice, "TestClass")
        except AttributeError:
            self.fail("new job class added to JOB_CLASS_DICT, but not defined "
                      "JobTypeChoice")


class TestJobCreator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.jobcreator = JobCreator(project=None)

    def test_dir(self):
        """
        All job types in JOB_CLASS_DICT need to be returned in __dir__ for
        autocompletion.
        """
        self.assertTrue(all(k in dir(self.jobcreator)
                            for k in JOB_CLASS_DICT),
                        "Not all job classes returned by dir()")

    def test_attr(self):
        """
        All job types in JOB_CLASS_DICT need to be available as attributes and
        should have their string names as values.
        """
        try:
            for k in JOB_CLASS_DICT:
                getattr(self.jobcreator, k)
        except AttributeError:
            self.fail("job class {} in JOB_CLASS_DICT, but not on "
                      "JobTypeChoice".format(k))
