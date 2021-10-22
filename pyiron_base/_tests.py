# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""Classes to help developers avoid code duplication when writing tests for pyiron."""

from contextlib import redirect_stdout
import doctest
from io import StringIO
import unittest
from os.path import split, join
from os import remove
from pyiron_base import PythonTemplateJob
from pyiron_base.project.generic import Project
from abc import ABC
from inspect import getfile


__author__ = "Liam Huber"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "0.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "development"
__date__ = "Mar 23, 2021"


class PyironTestCase(unittest.TestCase, ABC):

    """
    Tests that also include testing the docstrings in the specified modules
    """

    @classmethod
    def setUpClass(cls):
        if any([cls is c for c in _TO_SKIP]):
            raise unittest.SkipTest(f"{cls.__name__} tests, it's a base class")
        super().setUpClass()

    @property
    def docstring_module(self):
        """
        Define module whose docstrings will be tested
        """
        return None

    def test_docstrings(self):
        """
        Fails with output if docstrings in the given module fails.

        Output capturing adapted from https://stackoverflow.com/a/22434594/12332968
        """
        with StringIO() as buf, redirect_stdout(buf):
            result = doctest.testmod(self.docstring_module)
            output = buf.getvalue()
        self.failIf(result.failed > 0, msg=output)


class TestWithProject(PyironTestCase, ABC):
    """
    Tests that start and remove a project for their suite.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        print("TestWithProject: Setting up test project")
        cls.project_path = getfile(cls)[:-3].replace("\\", "/")
        cls.file_location, cls.project_name = split(cls.project_path)
        cls.project = Project(cls.project_path)

    @classmethod
    def tearDownClass(cls):
        cls.project.remove(enable=True)
        try:
            remove(join(cls.file_location, "pyiron.log"))
        except FileNotFoundError:
            pass


class TestWithCleanProject(TestWithProject, ABC):
    """
    Tests that start and remove a project for their suite, and remove jobs from the project for each test.
    """
    def tearDown(self):
        self.project.remove_jobs_silently(recursive=True)


class ToyJob(PythonTemplateJob):
    def __init__(self, project, job_name):
        """A toyjob to test export/import functionalities."""
        super(ToyJob, self).__init__(project, job_name)
        self.input.data_in = 100

    # This function is executed
    def run_static(self):
        self.status.running = True
        self.output.data_out = self.input.data_in + 1
        self.status.finished = True
        self.to_hdf()
        

class TestWithFilledProject(TestWithProject, ABC):

    """
    Tests that creates a projects, creates jobs and sub jobs in it, and at the end of unit testing,
    removes the project.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        job = cls.project.create_job(job_type=ToyJob, job_name="toy_1")
        job.run()
        job = cls.project.create_job(job_type=ToyJob, job_name="toy_2")
        job.run()
        job.status.aborted = True

        with cls.project.open("sub_project") as pr_sub:
            job = pr_sub.create_job(job_type=ToyJob, job_name="toy_1")
            job.run()
            job = pr_sub.create_job(job_type=ToyJob, job_name="toy_2")
            job.run()
            job.status.suspended = True


_TO_SKIP = [PyironTestCase, TestWithProject, TestWithCleanProject, TestWithFilledProject]
