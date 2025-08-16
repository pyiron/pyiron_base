import os
import unittest
import numpy as np
from unittest import TestCase
from unittest.mock import Mock
from pyiron_base.utils.parser import (
    extract_data_from_str_lst,
    extract_data_from_file,
    Logstatus,
    LogTag,
)


class TestParser(TestCase):
    def setUp(self):
        self.file_name = "test_parser.txt"

    def tearDown(self):
        if os.path.exists(self.file_name):
            os.remove(self.file_name)

    def test_log_status_parser(self):
        input_str = """\
        alat    3.2     # lattice constant (would be in a more realistic example in the structure file)
        alpha   0.1     # noise amplitude
        a_0     3       # equilibrium lattice constant
        a_1     0
        a_2     1.0     # 2nd order in energy (corresponds to bulk modulus)
        a_3     0.0     # 3rd order
        a_4     0.0     # 4th order
        count   10      # number of calls (dummy)
        epsilon 0.2     # energy prefactor of lennard jones
        sigma   2.4     # distance unit of lennard jones
        cutoff  4.0     # cutoff length (relative to sigma)
        write_restart True
        read_restart False
        """
        with open(self.file_name, "w") as f:
            f.writelines(input_str)

        tag_dict = {
            "alat": {"arg": "0", "rows": 0},
            "count": {"arg": "0", "rows": 0},
            "energy": {"arg": "0", "rows": 0},
        }
        lf = Logstatus()
        lf.extract_file(file_name=self.file_name, tag_dict=tag_dict)
        self.assertEqual(lf.status_dict["alat"], [[[0], 3.2]])
        self.assertEqual(lf.status_dict["count"], [[[0], 10]])

    def test_log_status_iter(self):
        lf = Logstatus(iter_levels=2)
        self.assertEqual(lf.iter, [0, 0])
        lf.raise_iter()
        self.assertEqual(lf.iter, [1, 0])
        lf.raise_iter(dim=1)
        self.assertEqual(lf.iter, [1, 1])
        lf.reset_iter(dim=1)
        self.assertEqual(lf.iter, [1, 0])
        lf.reset_iter()
        self.assertEqual(lf.iter, [0, 0])

    def test_log_status_append(self):
        lf = Logstatus()
        lf.append("test", [1, 2, 3])
        self.assertEqual(lf.status_dict["test"], [[[0], [1, 2, 3]]])
        lf.append("test", [4, 5, 6])
        self.assertEqual(lf.status_dict["test"], [[[0], [1, 2, 3]], [[0], [4, 5, 6]]])
        with self.assertRaises(ValueError):
            lf.append("test", [7, 8, 9], vec=True)


class TestParserExtended(unittest.TestCase):
    def setUp(self):
        self.file_name = "test_parser_extended.txt"

    def tearDown(self):
        if os.path.exists(self.file_name):
            os.remove(self.file_name)

    def test_extract_data_from_str_lst(self):
        str_lst = ["tag 1", "another_tag 2", "tag 3"]
        self.assertEqual(extract_data_from_str_lst(str_lst, "tag"), ["3"])
        self.assertEqual(extract_data_from_str_lst(str_lst, "tag", num_args=1), ["3"])
        str_lst_multi = ["tag 1 2 3", "another_tag 2", "tag 4 5 6"]
        self.assertEqual(
            extract_data_from_str_lst(str_lst_multi, "tag", num_args=3),
            [["4", "5", "6"]],
        )
        str_lst_comma = ["tag,1,2,3", "another_tag 2", "tag,4,5,6"]
        self.assertEqual(
            extract_data_from_str_lst(str_lst_comma, "tag", num_args=3),
            [["4", "5", "6"]],
        )
        str_lst_newline = ["tag 1 2 3\n", "another_tag 2\n", "tag 4 5 6\n"]
        self.assertEqual(
            extract_data_from_str_lst(str_lst_newline, "tag", num_args=3),
            [["4", "5", "6\n"]],
        )

    def test_extract_data_from_file(self):
        with open(self.file_name, "w") as f:
            f.write("tag 1 2 3\n")
            f.write("another_tag 2\n")
            f.write("tag 4 5 6\n")
        self.assertEqual(
            extract_data_from_file(self.file_name, "tag", num_args=3),
            [["4", "5", "6\n"]],
        )

    def test_log_tag(self):
        tag_dict = {
            "tag1": {"arg": "0", "h5": "h5_tag1"},
            "tag2": {
                "arg": "1",
                "type": int,
                "rows": 2,
                "lineSkip": 1,
                "splitArg": True,
                "func": lambda x: x + 1,
            },
            "$DYN_TAG": {"key": "dyn_key", "val": "dyn_val"},
        }
        h5_dict = {"py_tag": "h5_py_tag"}
        key_dict = {"dyn_key": "dyn_val"}
        lt = LogTag(tag_dict, h5_dict, key_dict)

        # Test properties
        self.assertEqual(lt.tag_dict, tag_dict)
        self.assertEqual(lt.h5_dict, h5_dict)
        self.assertEqual(lt.key_dict, key_dict)
        self.assertEqual("$DYN_TAG", lt.dyn_tags["DYN_TAG"])

        # Test is_item
        self.assertTrue(lt.is_item("tag1 value1"))
        self.assertEqual(lt.tag_name, "tag1")
        self.assertEqual(lt.val_list, ["value1"])
        self.assertFalse(lt.is_item("non_existing_tag"))

        # Test current and get_item
        with self.assertRaises(ValueError):
            lt.current = "non_existing_tag"
        lt.current = "tag2"
        self.assertEqual(lt.current, tag_dict["tag2"])
        self.assertEqual(lt.get_item("arg", 0), "1")
        self.assertEqual(lt.get_item("non_existing_item", "default"), "default")
        lt._current = None
        with self.assertRaises(ValueError):
            lt.get_item("arg", 0)

        # Test h5 and translate
        lt.current = "tag1"
        self.assertEqual(lt.h5(), "h5_tag1")
        lt.current = "tag2"  # No h5 key
        self.assertEqual(lt.h5(), "tag2")
        self.assertEqual(lt.translate("py_tag"), "h5_py_tag")
        with self.assertRaises(ValueError):
            lt.translate("non_existing_py_tag")
        lt.h5_dict = None
        with self.assertRaises(ValueError):
            lt.translate("py_tag")

        # Test arg, line_skip, rows
        lt.current = "tag2"
        self.assertEqual(lt.arg(), "1")
        self.assertTrue(lt.line_skip())
        self.assertEqual(lt.rows(), 2)
        lt.current = "tag1"
        self.assertEqual(lt.arg(), "0")
        self.assertFalse(lt.line_skip())
        self.assertEqual(lt.rows(), 0)
        tag_dict_str_rows = {"tag_str_rows": {"rows": "end_of_block"}}
        lt.tag_dict = tag_dict_str_rows
        lt.current = "tag_str_rows"
        self.assertEqual(lt.rows(), "end_of_block")

        # Test test_split, is_func, apply_func
        lt.tag_dict = tag_dict
        lt.current = "tag2"
        self.assertTrue(lt.test_split())
        self.assertTrue(lt.is_func())
        self.assertEqual(lt.apply_func(5), 6)
        lt.current = "tag1"
        self.assertFalse(lt.test_split())
        self.assertFalse(lt.is_func())
        self.assertIsNone(lt.apply_func(5))  # No function, should return None

        # Test set_item
        ls = Logstatus()
        lt.is_item("tag1 123")
        tag_name, tag_vals, rows, line_skip = lt.set_item({}, ls)
        self.assertEqual(tag_name, "tag1")
        self.assertEqual(ls.status_dict["h5_tag1"], [[[0], 123]])
        self.assertEqual(rows, 0)
        self.assertFalse(line_skip)

        # Test resolve_dynamic_variable
        lt.tag_dict = tag_dict.copy()
        lt.key_dict = key_dict
        lt.dyn_tags = lt.tag_dict
        lt._tag_name = "DYN_TAG"  # Manually set the tag name without '$'
        lt.resolve_dynamic_variable(["dyn_key"])
        self.assertIn("dyn_val", lt.tag_dict)
        self.assertNotIn("$DYN_TAG", lt.tag_dict)

    def test_logstatus_init_with_h5(self):
        h5_mock = Mock()
        h5_mock.getGroup.return_value.logStatus = "some_value"
        ls = Logstatus(h5=h5_mock)
        h5_mock.add_group.assert_called_with("generic")
        h5_mock.move_up.assert_called_once()
        self.assertIsNotNone(ls.h5)
        self.assertEqual(ls.h5_group_data, "some_value")

    def test_to_hdf(self):
        hdf_mock = {}
        ls = Logstatus()
        ls.append("test_data", [1, 2, 3])
        ls.append("another_data", [4])
        ls.store_as_vector.append("another_data")
        ls.to_hdf(hdf_mock)

        self.assertIn("test_data", hdf_mock)
        np.testing.assert_array_equal(hdf_mock["test_data"], np.array([[1, 2, 3]]))
        self.assertIn("another_data", hdf_mock)
        np.testing.assert_array_equal(hdf_mock["another_data"], np.array([4]))

        # Test ValueError
        ls.append("another_data", [5])
        with self.assertRaises(ValueError):
            ls.to_hdf(hdf_mock)

    def test_combine_xyz(self):
        ls = Logstatus()
        ls.append("x", [1, 2, 3])
        ls.append("y", [4, 5, 6])
        ls.append("z", [7, 8, 9])
        ls.combine_xyz("x", "y", "z", "coords")
        self.assertNotIn("x", ls.status_dict)
        self.assertNotIn("y", ls.status_dict)
        self.assertNotIn("z", ls.status_dict)
        self.assertIn("coords", ls.status_dict)
        expected = [[[0], [[1, 4, 7], [2, 5, 8], [3, 6, 9]]]]
        self.assertEqual(ls.status_dict["coords"], expected)

        ls = Logstatus()
        ls.append("x", [1, 2, 3])
        ls.append("y", [4, 5, 6])
        ls.append("z", [7, 8, 9])
        ls.combine_xyz("x", "y", "z", "coords", as_vector=True)
        self.assertNotIn("x", ls.status_dict)
        self.assertNotIn("y", ls.status_dict)
        self.assertNotIn("z", ls.status_dict)
        self.assertIn("coords", ls.status_dict)
        expected = [[[0], [1, 4, 7]], [[0], [2, 5, 8]], [[0], [3, 6, 9]]]
        self.assertEqual(ls.status_dict["coords"], expected)

        ls = Logstatus()
        ls.append("x", [1, 2, 3])
        ls.append("y", [4, 5, 6])
        ls.combine_xyz("x", "y", "z", "coords")
        self.assertIn("x", ls.status_dict)
        self.assertIn("y", ls.status_dict)
        self.assertNotIn("z", ls.status_dict)
        self.assertNotIn("coords", ls.status_dict)

    def test_combine_mat(self):
        ls = Logstatus()
        ls.append("xx", [1, 2])
        ls.append("xy", [3, 4])
        ls.append("xz", [5, 6])
        ls.append("yy", [7, 8])
        ls.append("yz", [9, 10])
        ls.append("zz", [11, 12])
        ls.combine_mat("xx", "xy", "xz", "yy", "yz", "zz", "matrix")
        self.assertNotIn("xx", ls.status_dict)
        self.assertIn("matrix", ls.status_dict)
        expected = [
            [
                [0],
                [
                    [[1, 3, 5], [3, 7, 9], [5, 9, 11]],
                    [[2, 4, 6], [4, 8, 10], [6, 10, 12]],
                ],
            ]
        ]
        self.assertEqual(ls.status_dict["matrix"], expected)

        ls = Logstatus()
        ls.append("xx", [1, 2])
        ls.combine_mat("xx", "xy", "xz", "yy", "yz", "zz", "matrix")
        self.assertIn("xx", ls.status_dict)
        self.assertNotIn("matrix", ls.status_dict)

    def test_convert_unit(self):
        ls = Logstatus()
        ls.append("energy", [10, 20, 30])
        ls.convert_unit("energy", 0.5)
        expected = [[[0], [5.0, 10.0, 15.0]]]
        self.assertEqual(ls.status_dict["energy"], expected)

        ls.convert_unit("non_existing_key", 0.5)
        self.assertNotIn("non_existing_key", ls.status_dict)

    def test_extract_item(self):
        tag, args = Logstatus.extract_item("ITEM: NUMBER OF IONS = 2")
        self.assertEqual(tag, "NUMBER OF IONS")
        self.assertEqual(args, ["=", "2"])

        tag, args = Logstatus.extract_item("ITEM: BOX BOUNDS pp pp pp")
        self.assertEqual(tag, "BOX BOUNDS")
        self.assertEqual(args, ["pp", "pp", "pp"])

        tag, args = Logstatus.extract_item("ITEM: ATOMS id type x y z")
        self.assertEqual(tag, "ATOMS")
        self.assertEqual(args, ["id", "type", "x", "y", "z"])

        tag, args = Logstatus.extract_item("ITEM: TIMESTEP")
        self.assertEqual(tag, "TIMESTEP")
        self.assertIsNone(args)

    def test_extract_from_list_complex(self):
        log_lines = [
            "total_energy = -123.45",
            "Forces:",
            "  -0.1 -0.2 -0.3",
            "   0.1  0.2  0.3",
            "Stress:",
            "  1.0 2.0 3.0",
            "  2.0 4.0 5.0",
            "  3.0 5.0 6.0",
            "end_of_stress",
            "a_tag b_tag",
            "1 2",
            "3 4",
            "end_a_tag",
            "c_tag val_c",
            "5 6",
            "7 8",
            "end_c_tag",
            "a_func_tag",
            "10",
            "end_func_tag",
            "a_warning_tag",
            "1 2 3",
            "WARNING: something went wrong",
        ]
        tag_dict = {
            "total_energy =": {"arg": "0", "h5": "energy"},
            "Forces:": {"rows": 2, "h5": "forces", "lineSkip": 0},
            "Stress:": {"rows": "end_of_stress", "h5": "stress"},
            "a_tag b_tag": {"rows": "end_a_tag", "splitTag": True},
            "c_tag": {"rows": "end_c_tag", "splitArg": True},
            "a_func_tag": {
                "func": lambda x: x * 2,
                "rows": "end_func_tag",
                "h5": "doubled",
            },
            "a_warning_tag": {"rows": "end_of_loop", "h5": "warning_data"},
        }
        h5_dict = {"a_tag": "a", "b_tag": "b", "val_c": "c"}

        ls = Logstatus()
        ls.extract_from_list(log_lines, tag_dict, h5_dict=h5_dict)

        self.assertEqual(ls.status_dict["energy"], [[[0], -123.45]])
        expected_forces = np.array([[-0.1, -0.2, -0.3], [0.1, 0.2, 0.3]])
        np.testing.assert_array_almost_equal(
            ls.status_dict["forces"][0][1], expected_forces
        )
        expected_stress = np.array([[1.0, 2.0, 3.0], [2.0, 4.0, 5.0], [3.0, 5.0, 6.0]])
        np.testing.assert_array_almost_equal(
            ls.status_dict["stress"][0][1], expected_stress
        )
        np.testing.assert_array_equal(ls.status_dict["a"][0][1], np.array([1, 3]))
        np.testing.assert_array_equal(ls.status_dict["b"][0][1], np.array([2, 4]))
        np.testing.assert_array_equal(ls.status_dict["c"][0][1], np.array([5, 7]))
        self.assertEqual(ls.status_dict["doubled"], [[[0], 20]])
        self.assertIn("warning_data", ls.status_dict)
        self.assertEqual(len(ls.status_dict["warning_data"][0][1]), 1)
        np.testing.assert_array_equal(
            ls.status_dict["warning_data"][0][1], np.array([[1, 2, 3]])
        )
