# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
from pyiron_base.generic.datacontainer import DataContainer
from pyiron_base.generic.inputlist import InputList
from pyiron_base.generic.hdfio import ProjectHDFio
from pyiron_base.project.generic import Project
from collections import Iterator
import copy
import os
import unittest
import warnings
import numpy as np
import yaml
import xmltodict
from dicttoxml import dicttoxml
from defusedxml.minidom import parseString


class TestDataContainer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.pl = DataContainer([
                {"foo": "bar"},
                2,
                42,
                {"next":
                    [0,
                        {"depth": 23}
                    ]
                }
        ], table_name = "input")
        cls.pl["tail"] = DataContainer([2,4,8])

        file_location = os.path.dirname(os.path.abspath(__file__))
        pr = Project(file_location)
        cls.file_name = os.path.join(file_location, "input.h5")
        cls.hdf = ProjectHDFio(project=pr, file_name=cls.file_name,
                               h5_path="/test", mode="a")
        cls.file_input_yaml = "test_input.yaml"
        cls.file_output_yaml = "test_output.yaml"
        cls.file_input_xml = "test_input.xml"
        cls.file_output_xml = "test_output.xml"

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.file_name)

    ## Init tests
    def test_init_none(self):
        pl = DataContainer()
        self.assertEqual(len(pl), 0, "not empty after initialized with None")

    def test_init_list(self):
        l = [1, 2, 3, 4]
        pl = DataContainer(l)
        self.assertEqual(len(pl), len(l),
                "not the same length as source list")
        self.assertEqual(list(pl.values()), l,
                "conversion to list not the same as source list")

    def test_init_tuple(self):
        t = (1, 2, 3, 4)
        pl = DataContainer(t)
        self.assertEqual(len(pl), len(t),
                "not the same length as source tuple")
        self.assertEqual(tuple(pl.values()), t,
                "conversion to tuple not the same as source tuple")

    def test_init_set(self):
        s = {1, 2, 3, 4}
        pl = DataContainer(s)
        self.assertEqual(len(pl), len(s),
                "not the same length as source set")
        self.assertEqual(set(pl.values()), s,
                "conversion to set not the same as source set")

    def test_init_dict(self):
        d = {"foo": 23, "test case": "bar"}
        pl = DataContainer(d)
        self.assertEqual(tuple(pl.items()), tuple(d.items()),
                "source dict items not preserved")
        with self.assertRaises(ValueError,
                               msg = "no ValueError on invalid initializer"):
            pl = DataContainer({2: 0, 1: 1})


    # access tests
    def test_get_nested(self):
        n = [
                {"foo": "bar"},
                2,
                42,
                {"next":
                    [0,
                        {"depth": 23}
                    ]
                }
        ]
        pl = DataContainer(n)
        self.assertEqual(type(pl[0]), DataContainer,
                "nested dict not converted to DataContainer")
        self.assertEqual(type(pl["3/next"]), DataContainer,
                "nested list not converted to DataContainer")
        self.assertEqual(type(pl["0/foo"]), str,
                "nested str converted to DataContainer")

    def test_get_item(self):
        self.assertEqual(self.pl[0], {"foo": "bar"},
                "index with integer does not give correct element")
        self.assertEqual(self.pl[0]["foo"], "bar",
                "index with string does not give correct element")
        with self.assertRaises(IndexError,
                               msg = "no IndexError on out of bounds index"):
            print(self.pl[15])
        with self.assertRaises(ValueError,
                               msg = "no ValueError on invalid index type"):
            print(self.pl[{}])

    def test_get_attr(self):
        self.assertEqual(self.pl.tail, DataContainer([2, 4, 8]),
                "attribute access does not give correct element")
        self.assertEqual(self.pl[3].next, DataContainer([0, DataContainer({"depth": 23})]),
                "nested attribute access does not give correct element")

    def test_get_sempath(self):
        self.assertEqual(self.pl["0"], {"foo": "bar"},
                "decimal string not converted to integer")
        self.assertEqual(self.pl["0/foo"], "bar",
                "nested access does not give correct element")
        self.assertEqual(self.pl["3/next/1/depth"], 23,
                "nested access does not give correct element")
        self.assertEqual(self.pl["3/next/0"], 0,
                "nested access does not give correct element")
        self.assertEqual(self.pl["3/next/1/depth"],
                         self.pl[3, "next", 1, "depth"],
                         "access via semantic path and tuple not the same")

    def test_get_string_int(self):
        self.assertEqual(self.pl[0], self.pl["0"],
                         "access via index and digit-only string not the same")

    def test_set_item(self):
        self.pl[1] = 4
        self.assertEqual(self.pl[1], 4,
                "setitem does not properly set value on int index")
        self.pl[1] = 2

        self.pl[0, "foo"] = "baz"
        self.assertEqual(self.pl[0, "foo"], "baz",
                "setitem does not properly set value on tuple index")
        self.pl[0, "foo"] = "bar"

    def test_set_errors(self):
        with self.assertRaises(IndexError,
                               msg = "no IndexError on out of bounds index"):
            self.pl[15] = 42
        with self.assertRaises(ValueError,
                               msg = "no ValueError on invalid index type"):
            self.pl[{}] = 42

    def test_set_some_keys(self):
        pl = DataContainer([1,2])
        pl["end"] = 3
        self.assertEqual(pl, DataContainer({0: 1, 1: 2, "end": 3}))

    def test_set_append(self):
        pl = DataContainer()
        # should not raise and exception
        pl[0] = 1
        pl[1] = 2
        self.assertEqual(pl[0], 1,
                         "append via index broken on empty list")
        self.assertEqual(pl[1], 2,
                         "append via index broken on non-empty list")

    def test_update(self):
        pl = DataContainer()
        d = self.pl.to_builtin()
        pl.update(d, wrap = True)
        self.assertEqual(pl, self.pl,
                         "update from to_builtin does not restore list")
        with self.assertRaises(ValueError,
                               msg = "no ValueError on invalid initializer"):
            pl.update("asdf")

        pl = self.pl.copy()
        pl.update({}, pyiron = "yes", test = "case")
        self.assertEqual( (pl.pyiron, pl.test), ("yes", "case"),
                "update via kwargs does not set values")
        pl.clear()
        d = {"a": 0, "b": 1, "c": 2}
        pl.update(d)
        self.assertEqual(dict(pl), d,
                "update without options does not call generic method")

    def test_extend(self):
        pl = DataContainer()
        pl.extend([1,2,3])
        self.assertEqual(list(pl.values()), [1,2,3],
                "extend from list does not set values")

    def test_insert(self):
        pl = DataContainer([1,2,3])
        pl.insert(1, 42, key = "foo")
        self.assertTrue(pl[0] == 1 and pl[1] == 42 and pl[2] == 2,
                "insert does not properly set value")
        pl.insert(1, 24, key = "bar")
        self.assertTrue(pl[0] == 1 and pl.bar == 24 and pl.foo == 42,
                "insert does not properly update keys")
        pl.insert(10, 4)
        self.assertEqual(pl[-1], 4,
                "insert does not handle out of bounds gracefully")

    def test_mark(self):
        pl = DataContainer([1,2,3])
        pl.mark(1, "foo")
        self.assertEqual(pl[1], pl.foo,
                "marked element does not refer to correct element")
        pl.mark(2, "foo")
        self.assertEqual(pl[2], pl.foo,
                "marking with existing key broken")
        with self.assertRaises(IndexError,
                               msg = "no IndexError on invalid index"):
            pl.mark(10, "foo")

    def test_deep_copy(self):
        pl = self.pl.copy()
        self.assertTrue(pl is not self.pl,
                "deep copy returns same object")
        self.assertTrue(all(
                pl[k1] is not self.pl[k2]
                    for k1, k2 in zip(pl, self.pl)
                        # int/str may be interned by python and always the same
                        # object when equal, so exclude from the check
                        if not isinstance(pl[k1], (int, str))),
                "not a deep copy")
        self.assertTrue(all(
                (k1 == k2) and (pl[k1] == self.pl[k2])
                    for k1, k2 in zip(pl, self.pl)),
                "copy not equal to original")

    def test_shallow_copy(self):
        pl = copy.copy(self.pl)
        self.assertTrue(pl is not self.pl,
                "shallow copy returns same object")
        self.assertTrue(all(
                (k1 is k2) and (pl[k1] is self.pl[k2])
                    for k1, k2 in zip(pl, self.pl)),
                "not a shallow copy")
        self.assertTrue(all(
                (k1 == k2) and (pl[k1] == self.pl[k2])
                    for k1, k2 in zip(pl, self.pl)),
                "copy not equal to original")

    def test_del_item(self):
        pl = DataContainer({0: 1, "a": 2, "foo": 3})

        with self.assertRaises(ValueError,
                               msg = "no ValueError on invalid index type"):
            del pl[{}]

        del pl["a"]
        self.assertTrue("a" not in pl, "delitem does not delete with str key")
        del pl[0]
        self.assertTrue(pl[0] != 1, "delitem does not delete with index")

    def test_del_attr(self):
        class SubDataContainer(DataContainer):
            def __init__(self):
                object.__setattr__(self, "attr", 42)
        s = SubDataContainer()
        del s.attr
        self.assertFalse(hasattr(s, "attr"),
                "delattr does not work with instance attributes")

    def test_numpy_array(self):
        pl = DataContainer([1,2,3])
        self.assertTrue((np.array(pl) == np.array([1,2,3])).all(),
                "conversion to numpy array broken")

    def test_repr_json(self):
        def rec(m):
            """
            Small helper to recurse through nested lists/dicts and check if all
            keys and values are strings.  This should be the output format of
            _repr_json_
            """

            if isinstance(m, list):
                for v in m:
                    if isinstance(v, (list, dict)):
                        if not rec(v):
                            return False
                    elif not isinstance(v, str):
                        return False
            elif isinstance(m, dict):
                for k, v in m.items():
                    if not isinstance(k, str):
                        return False
                    if isinstance(v, (list, dict)):
                        if not rec(v):
                            return False
                    elif not isinstance(v, str):
                        return False
            return True
        self.assertTrue(rec(self.pl._repr_json_()),
                "_repr_json_ output not all str")

    # hdf tests
    def test_to_hdf(self):
        self.pl.to_hdf(hdf=self.hdf)
        self.assertEqual(self.hdf["input/NAME"],
                         "DataContainer")
        self.assertEqual(self.hdf["input/OBJECT"],
                         "DataContainer")
        self.assertEqual(self.hdf["input/TYPE"],
                         "<class 'pyiron_base.generic.datacontainer.DataContainer'>")
        l = DataContainer(self.hdf["input/data"])
        self.assertEqual(self.pl, l)

        pl = DataContainer(self.pl)
        pl.to_hdf(hdf=self.hdf)
        self.assertEqual(self.hdf["NAME"],
                         "DataContainer")
        self.assertEqual(self.hdf["OBJECT"],
                         "DataContainer")
        self.assertEqual(self.hdf["TYPE"],
                         "<class 'pyiron_base.generic.datacontainer.DataContainer'>")
        l = DataContainer(self.hdf["data"])
        self.assertEqual(pl, l)

    def test_to_hdf_group(self):
        self.pl.to_hdf(hdf=self.hdf, group_name = "test_group")
        self.assertEqual(self.hdf["test_group/NAME"],
                         "DataContainer")
        self.assertEqual(self.hdf["test_group/TYPE"],
                         "<class 'pyiron_base.generic.datacontainer.DataContainer'>")
        self.assertEqual(self.hdf["test_group/OBJECT"],
                         "DataContainer")
        l = DataContainer(self.hdf["test_group/data"])
        self.assertEqual(self.pl, l)

    def test_to_hdf_readonly(self):
        self.pl.to_hdf(hdf=self.hdf, group_name = "read_only_f")
        self.assertTrue("read_only" in self.hdf.list_nodes(),
                        "read-only parameter not saved in HDF")
        self.assertEqual(self.pl.read_only,
                         self.hdf[self.pl.table_name]["read_only"],
                         "read-only parameter not correctly written to HDF")

        pl = self.pl.copy()
        pl.read_only = True
        pl.to_hdf(hdf=self.hdf, group_name = "read_only_t")
        self.assertEqual(pl.read_only,
                         self.hdf["read_only_t/read_only"],
                         "read-only parameter not correctly written to HDF")

    def test_from_hdf(self):
        self.pl.to_hdf(hdf=self.hdf)
        l = DataContainer(table_name = "input")
        l.from_hdf(hdf=self.hdf)
        self.assertEqual(self.pl, l)

    def test_from_hdf_group(self):
        self.pl.to_hdf(hdf=self.hdf, group_name = "test_group")
        l = DataContainer(table_name = "input")
        l.from_hdf(hdf=self.hdf, group_name = "test_group")
        self.assertEqual(self.pl, l)

    def test_from_hdf_readonly(self):
        self.pl.to_hdf(hdf=self.hdf, group_name = "read_only_from")
        pl = DataContainer()
        pl.from_hdf(self.hdf, group_name = "read_only_from")
        self.assertEqual(pl.read_only, self.hdf["read_only_from/read_only"],
                         "read-only parameter not correctly read from HDF")

        self.hdf["read_only_from/read_only"] = True
        pl.from_hdf(self.hdf, group_name = "read_only_from")
        self.assertEqual(pl.read_only, self.hdf["read_only_from/read_only"],
                         "read-only parameter not correctly read from HDF")


    def test_groups_nodes(self):
        self.assertTrue(isinstance(self.pl.nodes(), Iterator),
                        "nodes does not return an Iterator")
        self.assertTrue(isinstance(self.pl.groups(), Iterator),
                        "groups does not return an Iterator")
        self.assertTrue(isinstance(self.pl.list_nodes(), list),
                        "nodes does not return an list")
        self.assertTrue(isinstance(self.pl.list_groups(), list),
                        "groups does not return an list")

        for v1, v2 in zip(self.pl.list_groups(), self.pl.groups()):
            self.assertEqual(v1, v2, "list and iterator over groups not the same")

        for v1, v2 in zip(self.pl.list_nodes(), self.pl.nodes()):
            self.assertEqual(v1, v2, "list and iterator over nodes not the same")

        for g in self.pl.groups():
            self.assertTrue(isinstance(self.pl[g], DataContainer),
                            "groups returns a node")

        for n in self.pl.nodes():
            self.assertFalse(isinstance(self.pl[n], DataContainer),
                            "nodes returns a group")


    def test_read_only(self):
        pl = self.pl.copy()
        pl.read_only = True
        with warnings.catch_warnings(record=True) as w:
            pl[1] = 42
            self.assertEqual(len(w), 1,
                    "Writing to read-only list didn't raise warning.")

        with warnings.catch_warnings(record=True) as w:
            del pl[0]
            self.assertEqual(len(w), 1,
                    "Writing to read-only list didn't raise warning.")

        with warnings.catch_warnings(record=True) as w:
            pl.read_only = False
            self.assertEqual(len(w), 1,
                    "Trying to change read-only flag back didn't raise warning.")


    def test_read_write_consistency(self):
        """Writing a datacontainer, then reading it back in, should leave it unchanged."""
        fn = "pl.yml"
        self.pl.write(fn)
        pl = DataContainer()
        pl.read(fn)
        self.assertEqual(self.pl, pl, "Read container from yaml, is not the same as written.")
        os.remove(fn)

        fn = "pl.xml"
        self.pl.write(fn)
        pl = DataContainer()
        pl.read(fn)
        self.assertEqual(self.pl, pl, "Read container from xml, is not the same as written.")
        os.remove(fn)

class TestInputList(unittest.TestCase):

    def test_deprecation_warning(self):
        """Instantiating an InputList should raise a warning."""
        with self.assertWarns(DeprecationWarning, msg="InputList raises no DeprecationWarning!"):
            i = InputList([1,2,3])

if __name__ == "__main__":
    unittest.main()
