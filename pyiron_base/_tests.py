# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""Classes to help developers avoid code duplication when writing tests for pyiron."""

import doctest
import os
import unittest
from abc import ABC
from contextlib import redirect_stdout
from inspect import getfile
from io import StringIO

import numpy as np

from pyiron_base import PythonTemplateJob, state
from pyiron_base.project.generic import Project

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
    Base class for all pyiron unit tets.

    Registers utility type equality functions:
        - np.testing.assert_array_equal

    Optionally includes testing the docstrings in the specified module by
    overloading :attr:`~.docstring_module`.
    """

    def setUp(self):
        self.addTypeEqualityFunc(np.ndarray, self._assert_equal_numpy)

    def _assert_equal_numpy(self, a, b, msg=None):
        try:
            np.testing.assert_array_equal(a, b, err_msg=msg if msg is not None else "")
        except AssertionError as e:
            raise self.failureException(*e.args) from None

    @classmethod
    def setUpClass(cls):
        cls._initial_settings_configuration = state.settings.configuration.copy()
        if any([cls is c for c in _TO_SKIP]):
            raise unittest.SkipTest(f"{cls.__name__} tests, it's a base class")
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        state.update(cls._initial_settings_configuration)

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
        self.assertFalse(result.failed > 0, msg=output)


class TestWithProject(PyironTestCase, ABC):
    """
    Tests that start and remove a project for their suite.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        print("TestWithProject: Setting up test project")
        cls.project_path = getfile(cls)[:-3].replace("\\", "/")
        cls.file_location, cls.project_name = os.path.split(cls.project_path)
        cls.project = Project(cls.project_path)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.project.remove(enable=True)
        try:
            os.remove(os.path.join(cls.file_location, "pyiron.log"))
        except FileNotFoundError:
            pass


class TestWithCleanProject(TestWithProject, ABC):
    """
    Tests that start and remove a project for their suite, and remove jobs from the project for each test.
    """

    def tearDown(self):
        super().tearDown()
        self.project.remove_jobs(recursive=True, progress=False, silently=True)


class ToyJob(PythonTemplateJob):
    def __init__(self, project, job_name):
        """A toyjob to test export/import functionalities."""
        super(ToyJob, self).__init__(project, job_name)
        self.input.data_in = 100

    def write_input(self):
        super().write_input()
        self.input.write(os.path.join(self.working_directory, "input.yml"))

    # Allow writing of the input file
    def _check_if_input_should_be_written(self):
        return True

    # Check for valid input
    def validate_ready_to_run(self):
        if not isinstance(self.input.data_in, int):
            raise ValueError(
                f"data_in in should be of type int, not {type(self.input.data_in)}."
            )

    # This function is executed
    def run_static(self):
        self.status.running = True
        self.output.data_out = self.input.data_in + 1
        self.status.finished = True
        self.to_hdf()
        self.compress()


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

        cls.pr_sub = cls.project.open("sub_project")
        job = cls.pr_sub.create_job(job_type=ToyJob, job_name="toy_1")
        job.run()
        job = cls.pr_sub.create_job(job_type=ToyJob, job_name="toy_2")
        job.run()
        job.status.suspended = True
        job = cls.pr_sub.create_job(job_type=ToyJob, job_name="toy_3")
        job.run()

        cls.n_jobs_filled_with = 5
        # In a number of tests we compare the found jobs to an expected number of jobs
        # Let's code that number once here instead of magic-numbering it throughout
        # the tests


_TO_SKIP = [
    PyironTestCase,
    TestWithProject,
    TestWithCleanProject,
    TestWithFilledProject,
]
