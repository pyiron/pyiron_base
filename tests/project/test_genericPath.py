# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
import unittest
from pyiron_base.project.generic import Project
from pyiron_base.project.path import GenericPath
from pyiron_base._tests import PyironTestCase


class TestGenericPath(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        cls.path_project = GenericPath(
            root_path=cls.current_dir, project_path="project/path/"
        )

    def test_root_path(self):
        self.assertEqual(self.path_project.root_path, self.current_dir + "/")

    def test_project_path(self):
        self.assertEqual(self.path_project.project_path, "project/path/")

    def test_path(self):
        self.assertEqual(self.path_project.path, self.current_dir + "/project/path/")


class TestProject(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        cls.project = Project(os.path.join(cls.current_dir, "sub_folder"))

    @classmethod
    def tearDownClass(cls):
        cls.project.remove(enable=True)

    def test_repr(self):
        self.assertEqual([], self.project.list_groups())
        pr_down_one = self.project[".."]
        pr_down_two = self.project["../.."]
        pr_down_twice = self.project[".."][".."]
        self.assertEqual(pr_down_two.__repr__(), pr_down_twice.__repr__())
        self.assertEqual(
            sorted(
                [
                    directory
                    for directory in os.listdir(self.current_dir)
                    if not os.path.isfile(os.path.join(self.current_dir, directory))
                ]
            ),
            pr_down_one.list_groups(),
        )
        self.assertEqual(
            sorted(
                [
                    directory
                    for directory in os.listdir(os.path.join(self.current_dir, ".."))
                    if not os.path.isfile(
                        os.path.join(self.current_dir, "..", directory)
                    )
                ]
            ),
            pr_down_two.list_groups(),
        )
        self.assertEqual(pr_down_two.list_groups(), pr_down_twice.list_groups())


if __name__ == "__main__":
    unittest.main()
