# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from os.path import dirname, join, abspath
from os import remove
import pint
from pyiron_base.project.generic import Project
from pyiron_base._tests import PyironTestCase, TestWithProject, TestWithFilledProject, ToyJob
from pyiron_base.toolkit import BaseTools


class TestProjectData(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = dirname(abspath(__file__)).replace("\\", "/")
        cls.project_name = join(cls.file_location, "test_project")

    @classmethod
    def tearDownClass(cls):
        try:
            remove(join(cls.file_location, "pyiron.log"))
        except FileNotFoundError:
            pass

    def setUp(self):
        self.project = Project(self.project_name)

    def tearDown(self):
        self.project.remove(enable=True)

    def test_data(self):
        self.assertRaises(KeyError, self.project.data.read)

        self.project.data.foo = "foo"
        self.project.data.write()
        self.project.data.read()
        self.assertEqual(1, len(self.project.data))

        project2 = Project(self.project_name)
        self.assertEqual(1, len(project2.data))
        self.assertEqual(self.project.data.foo, project2.data.foo)

    def test_iterjobs(self):

        try:
            for j in self.project.iter_jobs():
                pass
        except:
            self.fail("Iterating over empty project should not raise exception.")


        try:
            for j in self.project.iter_jobs(status="finished"):
                pass
        except:
            self.fail("Iterating over empty project with set status flag should not raise exception.")

    def test_create_job_name(self):
        self.assertEqual(self.project.create.job_name(['job', 0.1]), 'job_0d1')
        self.assertEqual(self.project.create.job_name(['job', 0.1], special_symbols={'.': 'c'}), 'job_0c1')
        self.assertEqual(self.project.create.job_name(['job', 1.0000000000005]), 'job_1d0')
        self.assertEqual(self.project.create.job_name(['job', 1.0000000000005], ndigits=None), 'job_1d0000000000005')


class TestProjectOperations(TestWithFilledProject):

    def test_size(self):
        self.assertTrue(self.project.size > 0)

    def test__size_conversion(self):
        conv_check = {
            -2000: (-1.953125, "kibibyte"),
            0: (0, "byte"),
            50: (50, "byte"),
            2000: (1.953125, "kibibyte"),
            2**20: (1.0, "mebibyte"),
            2**30: (1.0, "gibibyte"),
            2**40: (1.0, "tebibyte"),
            2**50: (1.0, "pebibyte"),
            2**60: (1024.0, "pebibyte"),
        }

        byte = pint.UnitRegistry().byte
        for value in conv_check.keys():
            with self.subTest(value):
                converted_size = self.project._size_conversion(value * byte)
                self.assertEqual(converted_size.m, conv_check[value][0])
                self.assertEqual(str(converted_size.u), conv_check[value][1])

    def test_job_table(self):
        df = self.project.job_table()
        self.assertEqual(len(df), 4)
        self.assertEqual(" ".join(df.status.sort_values().unique()), "aborted finished suspended")

    def test_filtered_job_table(self):
        self.assertEqual(len(self.project.job_table(recursive=False)), 2)
        self.assertEqual(len(self.project.job_table(recursive=True)), 4)
        self.assertEqual(len(self.project.job_table(recursive=True, status="finished")), 2)
        self.assertEqual(len(self.project.job_table(recursive=False, status="finished")), 1)
        self.assertEqual(len(self.project.job_table(recursive=True, status="suspended")), 1)
        self.assertEqual(len(self.project.job_table(recursive=True, status="aborted")), 1)
        self.assertEqual(len(self.project.job_table(recursive=False, status="suspended")), 0)
        self.assertEqual(len(self.project.job_table(recursive=False, hamilton="ToyJob")), 2)
        self.assertEqual(len(self.project.job_table(recursive=True, parentid=None)), 4)
        self.assertEqual(len(self.project.job_table(recursive=True, status="finished", job="toy_1")), 2)
        self.assertEqual(len(self.project.job_table(recursive=True, job="toy*")), 4)
        self.assertEqual(len(self.project.job_table(recursive=True, job="*_1*")), 2)
        self.assertEqual(len(self.project.job_table(recursive=True, job="*_*")), 4)
        self.assertEqual(len(self.project.job_table(recursive=False, status="finished", job="toy_1")), 1)
        self.assertEqual(len(self.project.job_table(recursive=True, status="!finished")), 2)
        self.assertEqual(len(self.project.job_table(recursive=True, status="!aborted")), 3)
        self.assertEqual(len(self.project.job_table(recursive=True, job="!toy_1")), 2)
        self.assertEqual(len(self.project.job_table(recursive=True, job="!toy_*")), 0)
        self.assertRaises(ValueError, self.project.job_table, gibberish=True)

    def test_get_iter_jobs(self):
        self.assertEqual([job.output.data_out for job in self.project.iter_jobs(recursive=True,
                                                                                convert_to_object=True)], [101] * 4)
        self.assertEqual([val for val in self.project.iter_jobs(recursive=False, status="suspended")], [])
        self.assertIsInstance([val for val in self.project.iter_jobs(recursive=True, status="suspended",
                                                                     convert_to_object=True)][0], ToyJob)

    def test_maintenance_get_repository_status(self):
        df = self.project.maintenance.get_repository_status()
        self.assertIn('pyiron_base', df.Module)


class TestToolRegistration(TestWithProject):
    def setUp(self) -> None:
        self.tools = BaseTools(self.project)

    def test_registration(self):
        self.project.register_tools('foo', self.tools)
        with self.assertRaises(AttributeError):
            self.project.register_tools('foo', self.tools)  # Name taken
        with self.assertRaises(AttributeError):
            self.project.register_tools('load', self.tools)  # Already another method


if __name__ == '__main__':
    unittest.main()
