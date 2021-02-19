# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from os.path import dirname, join, abspath
from os import remove
from pyiron_base.project.generic import Project
from pyiron_base.project.data import ProjectData


class TestProjectData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = dirname(abspath(__file__)).replace("\\", "/")
        cls.project_name = join(cls.file_location, "test_data")

    @classmethod
    def tearDownClass(cls):
        remove(join(cls.file_location, "pyiron.log"))

    def tearDown(self):
        self.project.remove(enable=True)

    def setUp(self):
        self.project = Project(self.project_name)
        self.data = ProjectData(self.project, table_name="data")
        self.data.foo = "foo"
        self.data.bar = 42

    def test_empty_reading(self):
        self.assertRaises(KeyError, self.data.read)
        self.data.write()
        self.data.read()

    def test_new_instance(self):
        self.data.write()

        data2 = ProjectData(self.project, table_name="data")
        self.assertEqual(len(data2), 0)
        data2.read()
        self.assertEqual(data2.foo, self.data.foo)
        self.assertEqual(data2.bar, self.data.bar)

    def test_reading_recursion(self):
        self.data.baz = [1, 2, [3, 3]]
        self.data.write()
        self.assertEqual(3, len(self.data.baz))
        self.assertEqual(2, len(self.data.baz[-1]))

        data2 = ProjectData(self.project, table_name="data")
        data2.read()
        self.assertEqual(3, len(data2.baz))
        self.assertEqual(2, len(data2.baz[-1]))


if __name__ == '__main__':
    unittest.main()
