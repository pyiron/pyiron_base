# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
from os import remove
from pyiron_base._tests import TestWithCleanProject


class TestScriptJob(TestWithCleanProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.simple_script = os.path.join(cls.file_location, "simple.py")
        cls.complex_script = os.path.join(cls.file_location, "complex.py")

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        for file in [cls.simple_script, cls.complex_script]:
            try:
                remove(file)
            except FileNotFoundError:
                continue

    def setUp(self):
        super().setUp()
        self.job = self.project.create.job.ScriptJob("script")

    def tearDown(self):
        super().tearDown()
        self.project.remove_job("script")

    def test_script_path(self):
        with self.assertRaises(TypeError):
            self.job.run()

        with open(self.simple_script, "w") as f:
            f.write("print(42)")
        self.job.script_path = self.simple_script
        self.job.run(delete_existing_job=True)

    def test_project_data(self):
        self.project.data.in_ = 6
        self.project.data.write()
        with open(self.complex_script, "w") as f:
            f.write("from pyiron_base import Project, state\n")
            f.write("state.settings.configuration['project_check_enabled'] = False\n")
            f.write(f"pr = Project('{self.project_path}')\n")
            f.write("pr.data.out = pr.data.in_ * 7\n")
            f.write("pr.data.write()\n")
        # WARNING: If a user parallelizes this with multiple ScriptJobs, it would be
        #          possible to get a log jam with multiple simultaneous write-calls.
        self.job.script_path = self.complex_script
        self.job.run()
        self.project.data.read()
        self.assertEqual(42, self.project.data.out)

    def test_notebook_input(self):
        """
        Makes sure that the ScriptJob saves its input class in
        hdf["input/custom_group"] as this is needed when running external
        Notebook jobs c.f. `Notebook.get_custom_dict()`.
        """
        self.job.input["value"] = 300
        self.job.save()
        self.assertTrue(
            "custom_dict" in self.job["input"].list_nodes(),
            msg="Input not saved in the 'custom_dict' group in HDF",
        )

    def test_python_input(self):
        file_name = "test.py"
        with open(file_name, "w") as f:
            f.writelines(
                """
from pyiron_base import load, dump
input_dict = load()
output_dict = input_dict.copy()
dump(output_dict)
                """
            )

        with self.subTest("Use the written script"):
            input_dict = {"a": 1, "b": [1, 2, 3]}

            self.job.script_path = os.path.abspath(file_name)
            self.job.input.update(input_dict)
            self.job.run()

            data_dict = self.job["output"]
            for k, v in input_dict.items():
                self.assertTrue(data_dict[k], v)

        os.remove(file_name)
