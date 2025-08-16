import unittest
import os
import tempfile
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


if __name__ == "__main__":
    unittest.main()