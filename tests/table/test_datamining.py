# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base.job.template import PythonTemplateJob
from pyiron_base.project.generic import Project
from pyiron_base._tests import TestWithProject

class ToyJob(PythonTemplateJob):
    def __init__(self, project, job_name):
        super(ToyJob, self).__init__(project, job_name)
        self.input['input_energy'] = 100

    def run_static(self):
        with self.project_hdf5.open("output/generic") as h5out:
            h5out["energy_tot"] = self.input["input_energy"]
        self.status.finished = True

class TestProjectData(TestWithProject):

    @classmethod
    def setUpClass(cls):

        super().setUpClass()

        for i, c in enumerate("abcd"):
            j = cls.project.create_job(ToyJob, f"test_{c}")
            j.input['input_energy'] = i
            j.run()

    def setUp(self):
        self.table = self.project.create.table('test_table')
        self.table.filter_function = lambda j: j.name in ["test_a", "test_b"]
        self.table.add['name'] = lambda j: j.name
        self.table.run()

    def tearDown(self):
        self.project.remove_job(self.table.name)

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


if __name__ == '__main__':
    unittest.main()


if __name__ == '__main__':
    unittest.main()
