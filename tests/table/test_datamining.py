# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
import numpy as np

import pyiron_base
from pyiron_base._tests import TestWithProject, ToyJob


try:
    import pympipool
    skip_parallel_test = False
except ImportError:
    skip_parallel_test = True


class TestProjectData(TestWithProject):

    @classmethod
    def setUpClass(cls):

        super().setUpClass()

        for i, c in enumerate("abcd"):
            j = cls.project.create_job(ToyJob, f"test_{c}")
            j.input['input_energy'] = i
            j.run()

    def setUp(self):
        super().setUp()
        self.table: pyiron_base.TableJob = self.project.create.table('test_table')
        self.table.filter_function = lambda j: j.name in ["test_a", "test_b"]
        self.table.add['name'] = lambda j: j.name
        self.table.add['array'] = lambda j: np.arange(8)
        self.table.run()

    def tearDown(self):
        self.project.remove_job(self.table.name)

    def test_analysis_project(self):
        self.assertIs(self.project, self.table.analysis_project)
        self.assertEqual(self.project.path, self.project.load(self.table.name).analysis_project.path)

    def test_filter(self):
        """Filter functions should restrict jobs included in the table."""

        df = self.table.get_dataframe()
        self.assertEqual(2, len(df), "Table not correctly filtered.")
        self.assertEqual(["test_a", "test_b"], df.name.to_list(),  "Table not correctly filtered.")

    def test_filter_reload(self):
        """Lambdas should work as filter functions even if read from HDF5."""
        try:
            table_loaded = self.project.load(self.table.name)
        except:
            self.fail("Error on reloading table with filter lambda.")

    def test_numpy_reload(self):
        """Numpy arrays should be reloaded as such, not as strings."""
        # regression test: previously tables were converted to json then saved, which caused numpy arrays to be loaded
        # as strings
        table_loaded = self.project.load(self.table.name)
        df = table_loaded.get_dataframe()
        self.assertTrue(isinstance(df.array[0], np.ndarray),
                        "Numpy values not read correctly.")


@unittest.skipIf(skip_parallel_test, "pympipool is not installed, so the pympipool based tests are skipped.")
class TestProjectDataParallel(TestWithProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        for i, c in enumerate("abcd"):
            j = cls.project.create_job(ToyJob, f"test_{c}")
            j.input['input_energy'] = i
            j.run()

    def test_parallel(self):
        """Filter functions should restrict jobs included in the table."""
        table: pyiron_base.TableJob = self.project.create.table('test_table')
        table.filter_function = lambda j: j.name in ["test_a", "test_b"]
        table.add['name'] = lambda j: j.name
        table.add['array'] = lambda j: np.arange(8)
        table.server.cores = 2
        table.executor_type = "pympipool.mpi.executor.PyMPIExecutor"
        table.run()
        df = table.get_dataframe()
        self.assertEqual(2, len(df), "Table not correctly filtered.")
        self.assertEqual(["test_a", "test_b"], df.name.to_list(),  "Table not correctly filtered.")
        self.assertTrue(table.status.finished)
        self.project.remove_job(table.name)


if __name__ == '__main__':
    unittest.main()
