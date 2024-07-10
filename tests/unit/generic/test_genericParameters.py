# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from copy import deepcopy
import pandas
import os
from pyiron_base.storage.parameters import GenericParameters
from pyiron_base.storage.hdfio import ProjectHDFio
from pyiron_base.project.generic import Project
from pyiron_base._tests import PyironTestCase
import unittest


class TestGenericParameters(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.generic_parameters_empty = GenericParameters(table_name="empty")
        cls.generic_parameters_str = GenericParameters(table_name="str")
        my_str = """\
                par___1 1
                par_2 all
                count 0
                write_restart True
                dict {"a": 1, "b": 2}
                list [1, "s"]
                read_restart False"""
        cls.file_location = os.path.dirname(os.path.abspath(__file__))
        cls.generic_parameters_str.load_string(my_str)

    def test_load_string(self):
        self.assertEqual(self.generic_parameters_str.get("par___1"), 1)
        self.assertEqual(self.generic_parameters_str.get("par_2"), "all")
        self.assertEqual(self.generic_parameters_str.get("count"), 0)
        self.assertEqual(self.generic_parameters_str.get("dict"), {"a": 1, "b": 2})
        self.assertEqual(self.generic_parameters_str.get("list"), [1, "s"])
        self.assertTrue(self.generic_parameters_str.get("write_restart"))
        self.assertFalse(self.generic_parameters_str.get("read_restart"))

    def test_get_pandas(self):
        self.assertEqual(
            str(self.generic_parameters_empty.get_pandas()),
            str(pandas.DataFrame(columns=["Parameter", "Value", "Comment"])),
        )

    def test_modify(self):
        self.assertEqual(self.generic_parameters_str.get("par___1"), 1)
        self.generic_parameters_str.modify(par___1=3)
        self.assertEqual(self.generic_parameters_str.get("par___1"), 3)
        self.generic_parameters_str.modify(par___1=1)

    def test_write_to_file(self):
        self.generic_parameters_str.write_file(
            file_name="genpar.txt", cwd=self.file_location
        )
        file_name = os.path.join(self.file_location, "genpar.txt")
        with open(file_name, "r") as f:
            lines = f.readlines()
        self.assertEqual(lines[0], "par 1 1\n")
        self.assertEqual(lines[1], "par_2 all\n")
        self.assertEqual(lines[2], "count 0\n")
        self.assertEqual(lines[3], "write_restart True\n")
        self.assertEqual(lines[4], "dict {'a': 1, 'b': 2}\n")
        self.assertEqual(lines[5], "list [1, 's']\n")
        self.assertEqual(lines[6], "read_restart False\n")
        os.remove(file_name)

    def test_hdf(self):
        pr = Project(self.file_location)
        file_name = os.path.join(self.file_location, "genericpara.h5")
        hdf = ProjectHDFio(project=pr, file_name=file_name, h5_path="/test", mode="a")
        hdf.create_group("test")
        self.generic_parameters_str.to_hdf(hdf=hdf, group_name="input")
        gp_reload = GenericParameters(table_name="str")
        gp_reload.from_hdf(hdf=hdf, group_name="input")
        self.assertEqual(gp_reload.get("par___1"), 1)
        self.assertEqual(gp_reload.get("par_2"), "all")
        self.assertEqual(gp_reload.get("count"), 0)
        self.assertTrue(gp_reload.get("write_restart"))
        self.assertFalse(gp_reload.get("read_restart"))
        self.assertEqual(gp_reload.get("dict"), {"a": 1, "b": 2})
        self.assertEqual(gp_reload.get("list"), [1, "s"])
        os.remove(file_name)

    def test_remove_keys(self):
        self.assertFalse(self.generic_parameters_str.get("read_restart"))
        data_frame_all_entries = deepcopy(self.generic_parameters_str)
        self.generic_parameters_str.remove_keys(["read_restart"])
        self.assertNotEqual(
            str(self.generic_parameters_str.get_pandas()),
            str(data_frame_all_entries.get_pandas()),
        )
        self.generic_parameters_str.set(read_restart=False)
        self.assertFalse(self.generic_parameters_str.get("read_restart"))
        self.assertEqual(
            str(self.generic_parameters_str.get_pandas()),
            str(data_frame_all_entries.get_pandas()),
        )


if __name__ == "__main__":
    unittest.main()
