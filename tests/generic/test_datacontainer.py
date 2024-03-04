# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
import pyiron_base
from pyiron_base._tests import TestWithCleanProject, PyironTestCase
from pyiron_base.storage.datacontainer import DataContainer, DataContainerBase
from pyiron_base.storage.hdfstub import HDFStub
from pyiron_base.storage.inputlist import InputList
from collections.abc import Iterator
import copy
import os
import sys
import unittest
import warnings
import h5py
import numpy as np
import pandas as pd


class Sub(DataContainer):
    pass

class TestDataContainer(TestWithCleanProject):

    @property
    def docstring_module(self):
        return pyiron_base.storage.datacontainer

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.pl = DataContainer([
            {"foo": "bar"},
            2,
            42,
            {"next": [
                0,
                {"depth": 23}
            ]}
        ], table_name="input")
        cls.pl["tail"] = DataContainer([2, 4, 8])

    def setUp(self):
        super().setUp()
        self.hdf = self.project.create_hdf(self.project.path, "test")

    def tearDown(self):
        self.hdf.remove_file()
        self.hdf = None

    # Init tests
    def test_init_none(self):
        pl = DataContainer()
        self.assertEqual(len(pl), 0, "not empty after initialized with None")

    def test_init_list(self):
        l = [1, 2, 3, 4]
        pl = DataContainer(l)
        self.assertEqual(len(pl), len(l), "not the same length as source list")
        self.assertEqual(list(pl.values()), l, "conversion to list not the same as source list")

    def test_init_tuple(self):
        t = (1, 2, 3, 4)
        pl = DataContainer(t)
        self.assertEqual(len(pl), len(t), "not the same length as source tuple")
        self.assertEqual(tuple(pl.values()), t, "conversion to tuple not the same as source tuple")

    def test_init_set(self):
        s = {1, 2, 3, 4}
        pl = DataContainer(s)
        self.assertEqual(len(pl), len(s), "not the same length as source set")
        self.assertEqual(set(pl.values()), s, "conversion to set not the same as source set")

    def test_init_dict(self):
        d = {"foo": 23, "test case": "bar"}
        pl = DataContainer(d)
        self.assertEqual(tuple(pl.items()), tuple(d.items()), "source dict items not preserved")
        with self.assertRaises(ValueError, msg="no ValueError on invalid initializer"):
            DataContainer({2: 0, 1: 1})

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
        self.assertEqual(type(pl[0]), DataContainer, "nested dict not converted to DataContainer")
        self.assertEqual(type(pl["3/next"]), DataContainer, "nested list not converted to DataContainer")
        self.assertEqual(type(pl["0/foo"]), str, "nested str converted to DataContainer")

    def test_get_item(self):
        self.assertEqual(self.pl[0], {"foo": "bar"}, "index with integer does not give correct element")
        self.assertEqual(self.pl[0]["foo"], "bar", "index with string does not give correct element")
        with self.assertRaises(IndexError, msg="no IndexError on out of bounds index"):
            print(self.pl[15])
        with self.assertRaises(ValueError, msg="no ValueError on invalid index type"):
            print(self.pl[{}])

    def test_search(self):
        self.assertEqual(self.pl.search("depth"), 23, "search does not give correct element")
        with self.assertRaises(KeyError, msg="search: no IndexError on inexistent key"):
            print(self.pl.search("inexistent_key"))
        with self.assertRaises(TypeError, msg="search: no TypeError if key is not a string"):
            print(self.pl.search(0.0))
        # test if '...' in slash-notation triggers search
        self.assertEqual(self.pl[".../depth"], 23, "'.../' in key does not trigger search")
        self.pl["next/foo/bar/depth"] = 23
        # test if .../ works in setting when search is intermediate (some more items follow)
        self.pl[".../bar/extra"] = "stuff"
        self.assertEqual(self.pl["next/foo/bar/extra"], "stuff", "'.../' in setitem does not work (intermediate item search)")
        # test if .../ works in setting when search is final (no more items follow)
        self.pl[".../extra"] = "other"
        self.assertEqual(self.pl["next/foo/bar/extra"], "other", "'.../' in setitem does not work (final item search)")
        # test errors for multiple keys
        with self.assertRaises(ValueError, msg="search: no ValueError on multiple keys"):
            print(self.pl.search("depth", False))
        with self.assertRaises(ValueError, msg="search: no ValueError on multiple keys"):
            print(self.pl[".../depth"])
        # test errors for deletion
        del self.pl[".../bar/depth"]
        with self.assertRaises(KeyError, msg="search: '.../' in del does not work (intermediate item search)"):
            print(self.pl["next/foo/bar/depth"])
        del self.pl[".../bar"]
        with self.assertRaises(KeyError, msg="search: '.../' in del does not work (final item search)"):
            print(self.pl["next/foo/bar"])

    def test_get_attr(self):
        self.assertEqual(self.pl.tail, DataContainer([2, 4, 8]), "attribute access does not give correct element")
        self.assertEqual(
            self.pl[3].next,
            DataContainer([0, DataContainer({"depth": 23})]),
            "nested attribute access does not give correct element"
        )

    def test_get_sempath(self):
        self.assertEqual(self.pl["0"], {"foo": "bar"}, "decimal string not converted to integer")
        self.assertEqual(self.pl["0/foo"], "bar", "nested access does not give correct element")
        self.assertEqual(self.pl["3/next/1/depth"], 23, "nested access does not give correct element")
        self.assertEqual(self.pl["3/next/0"], 0, "nested access does not give correct element")
        self.assertEqual(
            self.pl["3/next/1/depth"],
            self.pl[3, "next", 1, "depth"],
            "access via semantic path and tuple not the same"
        )

    def test_get_string_int(self):
        self.assertEqual(self.pl[0], self.pl["0"], "access via index and digit-only string not the same")

    def test_set_item(self):
        self.pl[1] = 4
        self.assertEqual(self.pl[1], 4, "setitem does not properly set value on int index")
        self.pl[1] = 2

        self.pl[0, "foo"] = "baz"
        self.assertEqual(self.pl[0, "foo"], "baz", "setitem does not properly set value on tuple index")
        self.pl[0, "foo"] = "bar"

    def test_set_errors(self):
        with self.assertRaises(IndexError, msg="no IndexError on out of bounds index"):
            self.pl[15] = 42
        with self.assertRaises(ValueError, msg="no ValueError on invalid index type"):
            self.pl[{}] = 42

    def test_set_some_keys(self):
        pl = DataContainer([1, 2])
        pl["end"] = 3
        self.assertEqual(pl, DataContainer({0: 1, 1: 2, "end": 3}))

    def test_set_append(self):
        pl = DataContainer()
        # should not raise and exception
        pl[0] = 1
        pl[1] = 2
        self.assertEqual(pl[0], 1, "append via index broken on empty list")
        self.assertEqual(pl[1], 2, "append via index broken on non-empty list")
        pl.append([])
        self.assertTrue(isinstance(pl[-1], list), "append wraps sequences as DataContainer, but should not")
        pl.append({})
        self.assertTrue(isinstance(pl[-1], dict), "append wraps mappings as DataContainer, but should not")

    def test_update(self):
        pl = DataContainer()
        d = self.pl.to_builtin()
        pl.update(d, wrap=True)
        self.assertEqual(pl, self.pl, "update from to_builtin does not restore list")
        with self.assertRaises(ValueError, msg="no ValueError on invalid initializer"):
            pl.update("asdf")

        pl = self.pl.copy()
        pl.update({}, pyiron="yes", test="case")
        self.assertEqual((pl.pyiron, pl.test), ("yes", "case"), "update via kwargs does not set values")
        pl.clear()
        d = {"a": 0, "b": 1, "c": 2}
        pl.update(d)
        self.assertEqual(dict(pl), d, "update without options does not call generic method")

    def test_update_blacklist(self):
        """Wrapping nested mapping should only apply to types not in the blacklist."""
        pl = DataContainer()
        pl.update([ {"a": 1, "b": 2}, [{"c": 3, "d": 4}] ], wrap=True, blacklist=(dict,))
        self.assertTrue(isinstance(pl[0], dict), "nested dict wrapped, even if black listed")
        self.assertTrue(isinstance(pl[1][0], dict), "nested dict wrapped, even if black listed")
        pl.clear()

        pl.update({"a": [1, 2, 3], "b": {"c": [4, 5, 6]}}, wrap=True, blacklist=(list,))
        self.assertTrue(isinstance(pl.a, list), "nested list wrapped, even if black listed")
        self.assertTrue(isinstance(pl.b.c, list), "nested list wrapped, even if black listed")
        pl.clear()

    def test_wrap_hdf(self):
        """DataContainer should be able to be initialized by HDF objects."""
        h = self.project.create_hdf(self.project.path, "wrap_test")
        h["foo"] = 42
        h.create_group("bar")["test"] = 23
        h["bar"].create_group("nested")["test"] = 23
        d = DataContainer(h)
        self.assertTrue(isinstance(d.bar, DataContainer),
                        "HDF group not wrapped from ProjectHDFio.")
        self.assertTrue(isinstance(d.bar.nested, DataContainer),
                        "Nested HDF group not wrapped from ProjectHDFio.")
        self.assertEqual(d.foo, 42, "Top-level node not correctly wrapped from ProjectHDFio.")
        self.assertEqual(d.bar.test, 23, "Nested node not correctly wrapped from ProjectHDFio.")
        self.assertEqual(d.bar.nested.test, 23, "Nested node not correctly wrapped from ProjectHDFio.")

        h = h5py.File(h.file_name)
        d = DataContainer(h)
        self.assertTrue(isinstance(d.wrap_test.bar, DataContainer),
                        "HDF group not wrapped from h5py.File.")
        self.assertTrue(isinstance(d.wrap_test.bar.nested, DataContainer),
                        "Nested HDF group not wrapped from h5py.File.")
        self.assertEqual(d.wrap_test.foo, h["wrap_test/foo"],
                         "Top-level node not correctly wrapped from h5py.File.")
        self.assertEqual(d.wrap_test.bar.test, h["wrap_test/bar/test"],
                         "Nested node not correctly wrapped from h5py.File.")
        self.assertEqual(d.wrap_test.bar.nested.test, h["wrap_test/bar/nested/test"],
                         "Nested node not correctly wrapped from h5py.File.")

    def test_extend(self):
        pl = DataContainer()
        pl.extend([1, 2, 3])
        self.assertEqual(list(pl.values()), [1, 2, 3], "extend from list does not set values")

    def test_insert(self):
        pl = DataContainer([1, 2, 3])
        pl.insert(1, 42, key="foo")
        self.assertTrue(pl[0] == 1 and pl[1] == 42 and pl[2] == 2, "insert does not properly set value")
        pl.insert(1, 24, key="bar")
        self.assertTrue(pl[0] == 1 and pl.bar == 24 and pl.foo == 42, "insert does not properly update keys")
        pl.insert(10, 4)
        self.assertEqual(pl[-1], 4, "insert does not handle out of bounds gracefully")

    def test_mark(self):
        pl = DataContainer([1, 2, 3])
        pl.mark(1, "foo")
        self.assertEqual(pl[1], pl.foo, "marked element does not refer to correct element")
        pl.mark(2, "foo")
        self.assertEqual(pl[2], pl.foo, "marking with existing key broken")
        with self.assertRaises(IndexError, msg="no IndexError on invalid index"):
            pl.mark(10, "foo")

    def test_deep_copy(self):
        pl = self.pl.copy()
        self.assertTrue(pl is not self.pl, "deep copy returns same object")
        self.assertTrue(
            all(
                pl[k1] is not self.pl[k2]
                for k1, k2 in zip(pl, self.pl)
                # int/str may be interned by python and always the same
                # object when equal, so exclude from the check
                if not isinstance(pl[k1], (int, str))),
            "not a deep copy"
        )
        self.assertTrue(
            all(
                (k1 == k2) and (pl[k1] == self.pl[k2])
                for k1, k2 in zip(pl, self.pl)),
            "copy not equal to original"
        )

    def test_shallow_copy(self):
        pl = copy.copy(self.pl)
        self.assertTrue(pl is not self.pl, "shallow copy returns same object")
        self.assertTrue(
            all(
                (k1 is k2) and (pl[k1] is self.pl[k2])
                for k1, k2 in zip(pl, self.pl)),
            "not a shallow copy"
        )
        self.assertTrue(
            all(
                (k1 == k2) and (pl[k1] == self.pl[k2])
                for k1, k2 in zip(pl, self.pl)),
            "copy not equal to original"
        )

    def test_del_item(self):
        pl = DataContainer({0: 1, "a": 2, "foo": 3})

        with self.assertRaises(ValueError, msg="no ValueError on invalid index type"):
            del pl[{}]

        del pl["a"]
        self.assertTrue("a" not in pl, "delitem does not delete with str key")
        del pl[0]
        self.assertTrue(pl[0] != 1, "delitem does not delete with index")

    def test_del_attr(self):
        class SubDataContainer(DataContainer):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                object.__setattr__(self, "attr", 42)
        s = SubDataContainer()
        del s.attr
        self.assertFalse(hasattr(s, "attr"), "delattr does not work with instance attributes")

    def test_numpy_array(self):
        pl = DataContainer([1, 2, 3])
        self.assertTrue((np.array(pl) == np.array([1, 2, 3])).all(), "conversion to numpy array broken")

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
        self.assertTrue(rec(self.pl._repr_json_()), "_repr_json_ output not all str")

    def test_create_group(self):
        """create_group should not erase existing groups."""
        cont = DataContainer()
        sub1 = cont.create_group("sub")
        self.assertTrue(isinstance(sub1, DataContainer), "create_group doesn't return DataContainer")
        sub1.foo = 42
        sub2 = cont.create_group("sub")
        self.assertEqual(sub1.foo, sub2.foo, "create_group overwrites existing data.")
        self.assertTrue(sub1 is sub2, "create_group return new DataContainer group instead of existing one.")
        with self.assertRaises(ValueError, msg="No ValueError on existing data in Container"):
            sub1.create_group("foo")

    def test_to_hdf_type(self):
        """Should write correct type information."""
        self.pl.to_hdf(hdf=self.hdf)
        self.assertEqual(self.hdf["input/NAME"], "DataContainer")
        self.assertEqual(self.hdf["input/OBJECT"], "DataContainer")
        self.assertEqual(self.hdf["input/TYPE"], "<class 'pyiron_base.storage.datacontainer.DataContainer'>")

        h = self.hdf.open('nested')
        pl = DataContainer(self.pl)
        pl.to_hdf(hdf=h)
        self.assertEqual(h["NAME"], "DataContainer")
        self.assertEqual(h["OBJECT"], "DataContainer")
        self.assertEqual(h["TYPE"], "<class 'pyiron_base.storage.datacontainer.DataContainer'>")

    def test_to_hdf_items(self):
        """Should write all sublists to HDF groups and simple items to HDF datasets."""
        self.pl.to_hdf(hdf=self.hdf)
        for i, (k, v) in enumerate(self.pl.items()):
            k = "{}__index_{}".format(k if isinstance(k, str) else "", i)
            if isinstance(v, DataContainer):
                self.assertTrue(k in self.hdf["input"].list_groups(), "Sublist '{}' not a sub group in hdf!".format(k))
            else:
                self.assertTrue(k in self.hdf["input"].list_nodes(), "Item '{}' not a dataset in hdf!".format(k))

    def test_to_hdf_name(self):
        """Should raise error if clashing names are given."""
        with self.assertRaises(ValueError, msg="Cannot have names clashing with index mangling."):
            DataContainer({'__index_0': 42}).to_hdf(hdf=self.hdf)

    def test_to_hdf_group(self):
        """Should be possible to give a custom group name."""
        self.pl.to_hdf(hdf=self.hdf, group_name="test_group")
        self.assertEqual(self.hdf["test_group/NAME"], "DataContainer")
        self.assertEqual(self.hdf["test_group/TYPE"], "<class 'pyiron_base.storage.datacontainer.DataContainer'>")
        self.assertEqual(self.hdf["test_group/OBJECT"], "DataContainer")

    def test_to_hdf_readonly(self):
        """Read-only property should be stored."""
        self.pl.to_hdf(hdf=self.hdf, group_name="read_only_f")
        self.assertTrue("READ_ONLY" in self.hdf["read_only_f"].list_nodes(), "read-only parameter not saved in HDF")
        self.assertEqual(
            self.pl.read_only,
            self.hdf["read_only_f"]["READ_ONLY"],
            "read-only parameter not correctly written to HDF"
        )

        pl = self.pl.copy()
        pl.read_only = True
        pl.to_hdf(hdf=self.hdf, group_name="read_only_t")
        self.assertEqual(
            pl.read_only,
            self.hdf["read_only_t/READ_ONLY"],
            "read-only parameter not correctly written to HDF"
        )

    def test_from_hdf(self):
        """Reading from HDF should give back the same list as written."""
        self.pl.to_hdf(hdf=self.hdf)
        l = DataContainer(table_name="input")
        l.from_hdf(hdf=self.hdf)
        self.assertEqual(self.pl, l)

    def test_from_hdf_group(self):
        """Reading from HDF should give back the same list as written even with custom group name."""
        self.pl.to_hdf(hdf=self.hdf, group_name="test_group")
        l = DataContainer(table_name="input")
        l.from_hdf(hdf=self.hdf, group_name="test_group")
        self.assertEqual(self.pl, l)

    def test_from_hdf_readonly(self):
        """Reading from HDF should restore the read-only property."""
        self.pl.to_hdf(hdf=self.hdf, group_name="read_only_from")
        pl = DataContainer()
        pl.from_hdf(self.hdf, group_name="read_only_from")
        self.assertEqual(
            pl.read_only,
            self.hdf["read_only_from/READ_ONLY"],
            "read-only parameter not correctly read from HDF"
        )

        self.hdf["read_only_from/READ_ONLY"] = True
        with warnings.catch_warnings(record=True) as w:
            pl.from_hdf(self.hdf, group_name="read_only_from")
            self.assertEqual(len(w), 0, "from_hdf on read_only DataContainer should not call _read_only_error.")
        self.assertEqual(
            pl.read_only,
            self.hdf["read_only_from/READ_ONLY"],
            "read-only parameter not correctly read from HDF"
        )

    def test_hdf_complex_members(self):
        """Values that implement to_hdf/from_hdf, should write themselves to the HDF file correctly."""
        pl = DataContainer(table_name="complex")
        pl.append(self.project.create_job(self.project.job_type.ScriptJob, "dummy1"))
        pl.append(self.project.create_job(self.project.job_type.ScriptJob, "dummy2"))
        pl.append(42)
        pl["foo"] = "bar"
        pl.to_hdf(hdf=self.hdf)
        pl2 = self.hdf["complex"].to_object()
        self.assertEqual(type(pl[0]), type(pl2[0]))
        self.assertEqual(type(pl[1]), type(pl2[1]))

    def test_hdf_empty_group(self):
        """Writing a list without table_name or group_name should only work if the HDF group is empty."""
        l = DataContainer([1, 2, 3])
        self.hdf["dummy"] = True
        with self.assertRaises(ValueError, msg="No exception when writing to full hdf group."):
            l.to_hdf(self.hdf)
        h = self.hdf.create_group("empty_group")
        l.to_hdf(h)
        self.assertEqual(l, h.to_object())

    def test_hdf_empty_list(self):
        """Writing and reading an empty list should work."""
        l = DataContainer(table_name="empty_list")
        l.to_hdf(self.hdf)
        l.from_hdf(self.hdf)
        self.assertEqual(len(l), 0, "Empty list read from HDF not empty.")

    def test_hdf_no_wrap(self):
        """Nested mappings should not be wrapped as DataContainer after reading."""
        l = DataContainer(table_name="mappings")
        l.append({"foo": "bar"})
        l.append([1, 2, 3])
        l.to_hdf(self.hdf)
        m = l.copy()
        m.from_hdf(self.hdf, group_name="mappings")
        self.assertEqual(l, m, "List with nested mappings not restored from HDF.")
        self.assertTrue(isinstance(m[0], dict), "dicts wrapped after reading from HDF.")
        self.assertTrue(isinstance(m[1], list), "lists wrapped after reading from HDF.")

    def test_hdf_pandas(self):
        """Values that implement to_hdf/from_hdf, should write themselves to the HDF file correctly."""
        pl = DataContainer(table_name="pandas")
        pl.append(pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}))
        pl.to_hdf(hdf=self.hdf)
        pl2 = self.hdf["pandas"].to_object()
        self.assertEqual(type(pl[0]), type(pl2[0]))

    def test_groups_nodes(self):
        self.assertTrue(isinstance(self.pl.nodes(), Iterator), "nodes does not return an Iterator")
        self.assertTrue(isinstance(self.pl.groups(), Iterator), "groups does not return an Iterator")
        self.assertTrue(isinstance(self.pl.list_nodes(), list), "nodes does not return an list")
        self.assertTrue(isinstance(self.pl.list_groups(), list), "groups does not return an list")

        for v1, v2 in zip(self.pl.list_groups(), self.pl.groups()):
            self.assertEqual(v1, v2, "list and iterator over groups not the same")

        for v1, v2 in zip(self.pl.list_nodes(), self.pl.nodes()):
            self.assertEqual(v1, v2, "list and iterator over nodes not the same")

        for g in self.pl.groups():
            self.assertTrue(isinstance(self.pl[g], DataContainer), "groups returns a node")

        for n in self.pl.nodes():
            self.assertFalse(isinstance(self.pl[n], DataContainer), "nodes returns a group")

    def test_read_only(self):
        pl = self.pl.copy()
        pl.read_only = True
        with warnings.catch_warnings(record=True) as w:
            pl[1] = 42
            self.assertEqual(len(w), 1, "Writing to read-only list didn't raise warning.")

        with warnings.catch_warnings(record=True) as w:
            del pl[0]
            self.assertEqual(len(w), 1, "Writing to read-only list didn't raise warning.")

        with warnings.catch_warnings(record=True) as w:
            pl.read_only = False
            self.assertEqual(len(w), 1, "Trying to change read-only flag back didn't raise warning.")

    def test_recursive_append(self):
        input_tmp = DataContainer()
        input_tmp['some/argument/inside/another/argument'] = 3
        self.assertEqual(input_tmp['some/argument/inside/another/argument'], 3)
        self.assertEqual(input_tmp.some.argument.inside.another.argument, 3)
        self.assertEqual(type(input_tmp.some), DataContainer)

    def test_read_write_consistency(self):
        """Writing a datacontainer, then reading it back in, should leave it unchanged."""
        fn = "pl.yml"
        self.pl.write(fn)
        pl = DataContainer()
        pl.read(fn)
        self.assertEqual(self.pl, pl, "Read container from yaml, is not the same as written.")
        os.remove(fn)

    def test_subclass_preservation(self):
        self.pl.subclass = Sub(table_name='subclass')
        self.pl.to_hdf(hdf=self.hdf)
        loaded = DataContainer(table_name="input")
        loaded.from_hdf(hdf=self.hdf)
        self.assertIsInstance(
            loaded.subclass,
            Sub,
            f"Subclass not preserved on loading. "
            f"Expected {Sub.__name__} but got {type(loaded.subclass).__name__}."
        )
        self.pl.pop('subclass')

    def test_stub(self):
        """Lazily loaded containers should contain only stubs and only force them when directly accessed."""

        self.pl.to_hdf(self.hdf, "lazy")
        ll = self.hdf["lazy"].to_object(lazy=True)
        self.assertTrue(all(isinstance(v, HDFStub) for v in ll._store),
                        "Not all values loaded as stubs!")

        repr(ll)
        self.assertTrue(all(isinstance(v, HDFStub) for v in ll._store),
                        "Some stubs have been loaded after getting string repr of container!")

        ll0 = ll[0]
        self.assertTrue(all(isinstance(v, HDFStub) for v in ll0._store),
                        "Recursive datacontainers not lazily loaded!")

        self.assertEqual(ll[0].foo, self.pl[0].foo,
                         "Lazily loaded list not equal to orignal list!")

        self.assertTrue(not isinstance(ll._store[0], HDFStub),
                        "Loaded value not stored back into container!")

    def test_force_stubs(self):
        """Calling _force_load on a lazy loaded container should load all data from HDF."""

        self.pl.to_hdf(self.hdf, "lazy")
        ll = self.hdf["lazy"].to_object(lazy=True)
        ll._force_load(recursive=False)
        self.assertTrue(all(not isinstance(v, HDFStub) for v in ll._store),
                        "Not all values loaded after force!")
        ll0 = ll[0]
        self.assertTrue(all(isinstance(v, HDFStub) for v in ll0._store),
                        "Nested values loaded after force even though recursive==False!")

        ll._force_load()
        self.assertTrue(all(not isinstance(v, HDFStub) for v in ll._store),
                        "Not all values loaded after force!")
        ll0 = ll[0]
        self.assertTrue(all(not isinstance(v, HDFStub) for v in ll0._store),
                        "Nested values not loaded after force even though recursive==True!")

    def test_lazy_copy(self):
        """Copying lazy data containers should not throw an error."""
        try:
            self.pl.to_hdf(self.hdf, "lazy")
            ll = self.hdf["lazy"].to_object(lazy=True)
            ll.copy()
        except Exception as e:
            self.fail(f"Copy of a lazy data container raised {e}!")

    def test_stub_sublasses(self):
        """Sub classes of DataContainer should also be able to be lazily loaded."""

        sl = Sub(self.pl.to_builtin())

        sl.to_hdf(self.hdf, "lazy_sub")
        ll = Sub(lazy=True)
        ll.from_hdf(self.hdf, "lazy_sub")
        self.assertTrue(all(isinstance(v, HDFStub) for v in ll._store),
                        "Not all values loaded as stubs!")

        repr(ll)
        self.assertTrue(all(isinstance(v, HDFStub) for v in ll._store),
                        "Some stubs have been loaded after getting string repr of container!")

        ll0 = ll[0]
        self.assertTrue(all(isinstance(v, HDFStub) for v in ll0._store),
                        "Recursive datacontainers not lazily loaded!")

        self.assertEqual(ll[0].foo, sl[0].foo,
                         "Lazily loaded list not equal to orignal list!")

        self.assertTrue(not isinstance(ll._store[0], HDFStub),
                        "Loaded value not stored back into container!")

    def test_overwrite_with_group(self):
        """Writing to HDF second time after replacing a node by a group should not raise an error."""
        d = DataContainer({"test": 42})
        d.to_hdf(hdf=self.hdf, group_name="overwrite")
        del d.test
        d.create_group("test")
        d.test.foo = 42
        try:
            d.to_hdf(hdf=self.hdf, group_name="overwrite")
        except Exception as e:
            self.fail(f"to_hdf raised \"{e}\"!")

    def test_overwrite_with_node(self):
        """Writing to HDF second time after replacing a group by a node should not raise an error."""
        d = DataContainer({"test": {"foo": 42}})
        d.to_hdf(hdf=self.hdf, group_name="overwrite")
        del d.test
        d.create_group("test")
        d.test = 42
        try:
            d.to_hdf(hdf=self.hdf, group_name="overwrite")
        except Exception as e:
            self.fail(f"to_hdf raised \"{e}\"!")

    def test_overwrite_no_dangling_items(self):
        """Writing to HDF a second time should leave only items in HDF that are currently in the container."""
        d = self.pl.copy()
        d.to_hdf(self.hdf)
        del d[len(d) - 1]
        d.to_hdf(self.hdf)
        items = [k for k in self.hdf[d.table_name].list_nodes() if "__index_" in k] \
              + [k for k in self.hdf[d.table_name].list_groups() if "__index_" in k]
        self.assertEqual(len(d), len(items),
                         "Number of items in HDF does not match length of container!")

    def test_overwrite_ordering(self):
        """Writing to HDF a second time with different item order should not leave other items in the HDF."""
        d = self.pl.copy()
        d.to_hdf(self.hdf)
        d = DataContainer(list(reversed(list(d.values()))),
                          table_name=d.table_name)
        d.to_hdf(self.hdf)
        items = [k for k in self.hdf[d.table_name].list_nodes() if "__index_" in k] \
              + [k for k in self.hdf[d.table_name].list_groups() if "__index_" in k]
        self.assertEqual(len(d), len(items),
                         "Number of items in HDF does not match length of container!")

    @unittest.skipIf(sys.version_info < (3, 11), "__getstate__() and __setstate__() support in h5io requires Python 3.11")
    def test_project_in_datacontainer(self):
        """DataContainer should be able to save Project to HDF."""
        pl = DataContainer(table_name="project")
        pl.update({"project": self.project})
        pl.to_hdf(hdf=self.hdf)
        pl_reload = DataContainer(table_name="project")
        pl_reload.from_hdf(hdf=self.hdf)
        self.assertEqual(pl_reload.project.project_path, self.project.project_path)
        self.assertEqual(pl_reload.project.root_path, self.project.root_path)

class TestDataContainerBase(unittest.TestCase):
    """Test interactions between base class and full data container."""

    @classmethod
    def setUpClass(cls):
        first = {"foo": "bar"}
        inner = [0, {"depth": 23}]
        cls.body = [
            first,
            2,
            42,
            {"next": inner}
        ]
        cls.base_inside = DataContainer(cls.body)
        cls.base_inside[0] = Sub(first)
        cls.base_inside[-1]["next"] = DataContainerBase(inner)
        cls.base_outside = DataContainerBase(cls.body)
        cls.base_outside[-1]["next"] = DataContainer(inner)
        cls.base_outside[0] = Sub(first)

    def test_to_builtin(self):
        """to_builtin should recurse fully even if DataContainer and DataContainerBase are nested."""
        self.assertEqual(self.body, self.base_inside.to_builtin(),
                         "incorrect when DataContainerBase is inside")
        self.assertEqual(self.body, self.base_outside.to_builtin(),
                         "incorrect when DataContainer is inside")

    def test_nodes(self):
        """list_nodes() should never return sub classes of DataContainerBase"""
        self.assertEqual(self.base_inside.list_nodes(), [1, 2],
                         "nodes should not contain first and last key.")
        self.assertEqual(self.base_outside.list_nodes(), [1, 2],
                         "nodes should not contain first and last key.")

    def test_groups(self):
        """list_groups() should never return sub classes of DataContainerBase"""
        self.assertEqual(self.base_inside.list_groups(), [0, 3],
                         "groups should not contain second and third key.")
        self.assertEqual(self.base_outside.list_groups(), [0, 3],
                         "groups should not contain second and third key.")

class TestInputList(PyironTestCase):

    def test_deprecation_warning(self):
        """Instantiating an InputList should raise a warning."""
        with self.assertWarns(DeprecationWarning, msg="InputList raises no DeprecationWarning!"):
            InputList([1, 2, 3])


if __name__ == "__main__":
    unittest.main()
