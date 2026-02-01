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

    def test_init_with_input_file(self):
        # Create a dummy input file
        with open("test_input.txt", "w") as f:
            f.write("param1 1\n")
            f.write("param2 True\n")

        gp = GenericParameters(input_file_name="test_input.txt")
        self.assertEqual(gp.get("param1"), 1)
        self.assertEqual(gp.get("param2"), True)

        os.remove("test_input.txt")

    def test_file_name_property(self):
        gp = GenericParameters()
        gp.file_name = "new_name"
        self.assertEqual(gp.file_name, "new_name")

    def test_table_name_property(self):
        gp = GenericParameters()
        gp.table_name = "new_table"
        self.assertEqual(gp.table_name, "new_table")

    def test_val_only_property(self):
        gp = GenericParameters()
        gp.val_only = True
        self.assertTrue(gp.val_only)

    def test_comment_char_property(self):
        gp = GenericParameters()
        gp.comment_char = ";"
        self.assertEqual(gp.comment_char, ";")

    def test_separator_char_property(self):
        gp = GenericParameters()
        gp.separator_char = "="
        self.assertEqual(gp.separator_char, "=")

    def test_multi_word_separator_property(self):
        gp = GenericParameters()
        gp.multi_word_separator = "-"
        self.assertEqual(gp.multi_word_separator, "-")

    def test_end_value_char_property(self):
        gp = GenericParameters()
        gp.end_value_char = "!"
        self.assertEqual(gp.end_value_char, "!")

    def test_replace_char_dict_property(self):
        gp = GenericParameters()
        gp.replace_char_dict = {"a": "b"}
        self.assertEqual(gp.replace_char_dict, {"a": "b"})

    def test_read_only(self):
        gp = GenericParameters()
        gp.read_only = True
        with self.assertWarns(Warning):
            gp.load_string("param1 1")
        with self.assertWarns(Warning):
            gp.load_default()
        with self.assertWarns(Warning):
            gp.set_value(0, "1")
        gp.set(param1=1)
        with self.assertWarns(Warning):
            gp.remove_keys(["param1"])
        with self.assertWarns(Warning):
            gp["param1"] = 1
        gp = GenericParameters()
        gp.set(param1=1)
        gp.read_only = True
        with self.assertWarns(Warning):
            gp.modify(param1=2)
        with self.assertWarns(Warning):
            gp.modify(param1=(2, "a comment"))

    def test_read_input(self):
        gp = GenericParameters()
        with self.assertRaises(ValueError):
            gp.read_input("non_existent_file.txt")

        with open("test_input.txt", "w") as f:
            f.write("param1 1\n")
            f.write("!ignore this line\n")
            f.write("param2 2\n")

        gp.read_input("test_input.txt", ignore_trigger="!")
        self.assertEqual(gp.get("param1"), 1)
        self.assertEqual(gp.get("param2"), 2)
        self.assertNotIn("!ignore", "".join(gp.get_string_lst()))

        os.remove("test_input.txt")

    def test_lines_to_dict(self):
        gp = GenericParameters()
        lines = ["param1 1 # comment", "param2 2"]
        data_dict = gp._lines_to_dict(lines)
        self.assertEqual(data_dict["Parameter"], ["param1", "param2"])
        self.assertEqual(data_dict["Value"], ["1", "2"])
        self.assertEqual(data_dict["Comment"], ["comment", ""])

        gp_val_only = GenericParameters(val_only=True)
        lines = ["value1 # comment", "value2"]
        data_dict = gp_val_only._lines_to_dict(lines)
        self.assertEqual(data_dict["Parameter"], ["", ""])
        self.assertEqual(data_dict["Value"], ["value1", "value2"])
        self.assertEqual(data_dict["Comment"], ["comment", ""])

        gp_replace = GenericParameters()
        gp_replace.replace_char_dict = {"a": "b"}
        lines = ["parbm1 ab"]
        data_dict = gp_replace._lines_to_dict(lines)
        self.assertEqual(data_dict["Parameter"], ["pbrbm1"])
        self.assertEqual(data_dict["Value"], ["bb"])

    def test_get_set_del(self):
        gp = GenericParameters()
        gp.set(param1=1, param2="test", param3=True)
        self.assertEqual(gp.get("param1"), 1)
        self.assertEqual(gp.get("param2"), "test")
        self.assertTrue(gp.get("param3"))
        self.assertEqual(gp["param1"], 1)
        self.assertEqual(gp[1], "test")

        gp["param1"] = 2
        self.assertEqual(gp.get("param1"), 2)

        del gp["param2"]
        with self.assertRaises(NameError):
            gp.get("param2")

        self.assertEqual(gp.get("non_existent", default_value="default"), "default")
        with self.assertRaises(NameError):
            gp.get("non_existent")

    def test_modify(self):
        gp = GenericParameters()
        gp.set(param1=1)
        gp.modify(param1=2)
        self.assertEqual(gp.get("param1"), 2)

        gp.modify(param1=(3, "a comment"))
        self.assertEqual(gp.get("param1"), 3)
        self.assertEqual(gp._dataset["Comment"][0], "a comment")

        with self.assertRaises(ValueError):
            gp.modify(non_existent=1)

        gp.modify(non_existent=1, append_if_not_present=True)
        self.assertEqual(gp.get("non_existent"), 1)

    def test_set_value(self):
        gp = GenericParameters()
        gp.set_value(0, "test")
        self.assertEqual(gp[0], "test")

        gp.set_value(1, "test2")
        self.assertEqual(gp[1], "test2")

    def test_remove_keys(self):
        gp = GenericParameters()
        gp.set(param1=1, param2=2)
        gp.remove_keys(["param1"])
        self.assertNotIn("param1", gp.keys())
        # test that removing non-existent key does not raise error
        gp.remove_keys(["non_existent"])

    def test_block_operations(self):
        gp = GenericParameters()
        gp.set(
            block1_param1=1,
            block1_param2=2,
            block2_param1=3,
            block2_param2=4,
            other_param=5,
        )
        block_dict = {
            "block1": ["block1_param1", "block1_param2"],
            "block2": ["block2_param1", "block2_param2"],
        }
        from collections import OrderedDict

        gp.define_blocks(OrderedDict(block_dict))

        block1 = gp._get_block("block1")
        self.assertEqual(block1["Parameter"], ["block1_param1", "block1_param2"])
        self.assertEqual(block1["Value"], [1, 2])

        gp._remove_block("block1")
        self.assertNotIn("block1_param1", gp.keys())
        self.assertNotIn("block1_param2", gp.keys())

        insert_block_dict = {"Parameter": ["new_param"], "Value": [6], "Comment": [""]}
        gp._insert_block(insert_block_dict, next_block="block2")
        self.assertIn("new_param", gp.keys())

        update_block_dict = {
            "Parameter": ["block2_param1"],
            "Value": [10],
            "Comment": [""],
        }
        gp._update_block(update_block_dict)
        self.assertEqual(gp.get("block2_param1"), 10)

        with self.assertRaises(ValueError):
            gp._get_block("non_existent_block")

        with self.assertRaises(ValueError):
            gp._remove_block("non_existent_block")

        with self.assertRaises(AssertionError):
            gp.define_blocks(block_dict)

    def test_hdf_from_dict(self):
        pr = Project(self.file_location)
        file_name = os.path.join(self.file_location, "genericpara.h5")
        hdf = ProjectHDFio(project=pr, file_name=file_name, h5_path="/test", mode="a")
        self.generic_parameters_str.to_hdf(hdf=hdf)

        # Create a new GenericParameters object and load from HDF
        gp_reload = GenericParameters(table_name="str")
        gp_reload.from_hdf(hdf=hdf)

        self.assertEqual(gp_reload.get("par___1"), 1)
        os.remove(file_name)

    def test_get_string_lst(self):
        gp = GenericParameters()
        gp.set(param1=1, param2=True, param3="test")
        string_lst = gp.get_string_lst()
        self.assertIn("param1 1\n", string_lst)
        self.assertIn("param2 True\n", string_lst)
        self.assertIn("param3 test\n", string_lst)

        gp.val_only = True
        string_lst = gp.get_string_lst()
        self.assertIn("1\n", string_lst)
        self.assertIn("True\n", string_lst)
        self.assertIn("test\n", string_lst)

    def test_get_with_multiple_keys(self):
        gp = GenericParameters()
        gp.set(param1=1)
        gp.set(param2=2)
        # Manually add a duplicate key to test the error handling
        gp._dataset["Parameter"].append("param1")
        gp._dataset["Value"].append(3)
        gp._dataset["Comment"].append("")
        with self.assertRaises(ValueError):
            gp.get("param1")

    def test_clear_all(self):
        gp = GenericParameters()
        gp.set(param1=1)
        gp.clear_all()
        self.assertEqual(len(gp.keys()), 0)


if __name__ == "__main__":
    unittest.main()
