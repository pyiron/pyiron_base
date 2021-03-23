# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""Classes to help developers avoid code duplication when writing tests for pyiron."""

import unittest
from os.path import split, join
from os import remove
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


class TestWithProject(unittest.TestCase, ABC):
    """
    Tests that start and remove a project for their suite, and remove jobs from the project for each test.
    """

    @classmethod
    def setUpClass(cls):
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

    def tearDown(self):
        self.project.remove_jobs_silently(recursive=True)
