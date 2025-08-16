import unittest
import os
import tempfile
from collections import OrderedDict

from pyiron_base.storage.parameters import GenericParameters


class TestGenericParameters(unittest.TestCase):
    def test_load_default(self):
        gp = GenericParameters(input_file_name=None)
        self.assertEqual(gp.keys(), [])
        df = gp.get_pandas()
        self.assertEqual(list(df.columns), ["Parameter", "Value", "Comment"])
        self.assertTrue(df.empty)

    def test_load_string_and_get(self):
        gp = GenericParameters(input_file_name=None)
        test_str = "A 1\nB 2"
        gp.load_string(test_str)
        self.assertEqual(gp.get("A"), 1)
        self.assertEqual(gp.get("B"), 2)
        self.assertEqual(gp.keys(), ["A", "B"])

    def test_set_and_modify(self):
        gp = GenericParameters(input_file_name=None)
        gp.set(A=5)
        self.assertEqual(gp.get("A"), 5)

        gp.modify(A=10)
        self.assertEqual(gp.get("A"), 10)

        with self.assertRaises(ValueError):
            gp.modify(B=3)

    def test_setitem_getitem_and_delitem(self):
        gp = GenericParameters(input_file_name=None)
        gp["A"] = 1
        self.assertEqual(gp["A"], 1)

        gp["A"] = 2
        self.assertEqual(gp["A"], 2)

        del gp["A"]
        with self.assertRaises(NameError):
            gp.get("A")

    def test_remove_keys(self):
        gp = GenericParameters(input_file_name=None)
        gp.set(A=1, B=2)
        gp.remove_keys(["A"])
        self.assertEqual(gp.keys(), ["B"])

        # should not raise:
        gp.remove_keys(["X"])
        self.assertEqual(gp.keys(), ["B"])

    def test_get_default_value(self):
        gp = GenericParameters(input_file_name=None)
        self.assertEqual(gp.get("missing", default_value="fallback"), "fallback")
        with self.assertRaises(NameError):
            gp.get("missing")

    def test_write_and_read_input(self):
        gp = GenericParameters(input_file_name=None)
        gp.set(A=1, B=2)
        # write to a temp file
        with tempfile.TemporaryDirectory() as d:
            fname = os.path.join(d, "input.txt")
            gp.write_file(fname)
            gp2 = GenericParameters(input_file_name=None)
            gp2.read_input(fname)
            self.assertEqual(gp2.get("A"), 1)
            self.assertEqual(gp2.get("B"), 2)

    def test_get_string_lst_and_bool_conversion(self):
        gp = GenericParameters(input_file_name=None)
        gp.set(FLAG=True)
        lines = gp.get_string_lst()
        # should contain "True"
        self.assertIn("True", lines[0])
        # check bool conversion backwards
        val = gp._bool_str_to_bool("True")
        self.assertTrue(val)
        # value untouched if not boolean
        self.assertEqual(gp._bool_str_to_bool("abc"), "abc")

    def test_block_insert_update_delete(self):
        gp = GenericParameters(input_file_name=None)
        # define block and append parameters
        block = OrderedDict()
        block["Parameter"] = ["A", "B"]
        block["Value"] = ["1", "2"]
        block["Comment"] = ["", ""]
        # define dictionary for block name -> parameter names
        gp.define_blocks(OrderedDict([("myblock", ["A", "B"])]))
        gp._insert_block(block)

        # now update the block
        block_update = {"Parameter": ["A"], "Value": ["3"]}
        gp._update_block(block_update)
        self.assertEqual(gp.get("A"), "3")

        # removing the block
        gp._remove_block("myblock")
        self.assertEqual(gp.keys(), [])

    def test_multiple_occurrences_findline(self):
        gp = GenericParameters(input_file_name=None)
        gp.set(A=1)
        gp.set(B=2)
        # inject duplicate key into internal structure to test ValueError path
        gp._dataset["Parameter"].append("A")
        gp._dataset["Value"].append("3")
        gp._dataset["Comment"].append("")
        with self.assertRaises(ValueError):
            gp._find_line("A")


if __name__ == "__main__":
    unittest.main()
