# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base._tests import TestWithCleanProject
from os.path import abspath, dirname


class TestTestWithProject(TestWithCleanProject):
    def test_location(self):
        self.assertEqual(
            dirname(abspath(__file__)).replace("\\", "/"),
            self.file_location,
            msg="Projects will not be instantiated where their invoking script is."
        )
