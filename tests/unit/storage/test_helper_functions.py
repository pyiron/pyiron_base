import os
import posixpath
import numpy as np
import h5py
from unittest import TestCase
from pyiron_base.storage.hdfio import _list_groups_and_nodes
from h5io_browser import read_nested_dict_from_hdf
from h5io_browser.base import write_dict_to_hdf, _read_hdf, _write_hdf


def get_hdf5_raw_content(file_name):
    item_lst = []

    def collect_attrs(name, obj):
        item_lst.append({name: {k: v for k, v in obj.attrs.items()}})

    with h5py.File(file_name, "r") as f:
        f.visititems(collect_attrs)

    return item_lst


class TestWriteHdfIO(TestCase):
    def setUp(self):
        self.file_name = "test_write_hdf5.h5"
        self.h5_path = "data_hierarchical"
        self.data_hierarchical = {"a": np.array([1, 2]), "b": 3, "c": {"d": np.array([4, 5]), "e": np.array([6, 7])}}
        write_dict_to_hdf(
            file_name=self.file_name,
            data_dict={
                posixpath.join(self.h5_path, "a"): self.data_hierarchical["a"],
                posixpath.join(self.h5_path, "b"): self.data_hierarchical["b"],
                posixpath.join(self.h5_path, "c", "d"): self.data_hierarchical["c"]["d"],
                posixpath.join(self.h5_path, "c", "e"): self.data_hierarchical["c"]["e"],
            },
        )

    def tearDown(self):
        os.remove(self.file_name)

    def test_read_dict_hierarchical(self):
        output = read_nested_dict_from_hdf(file_name=self.file_name, h5_path=self.h5_path)
        self.assertTrue(
            np.all(np.equal(output["a"], np.array([1, 2]))),
        )
        self.assertEqual(output["b"], 3)
        output = read_nested_dict_from_hdf(
            file_name=self.file_name,
            h5_path=self.h5_path,
            group_paths=["c"],
        )
        self.assertTrue(
            np.all(np.equal(output["a"], np.array([1, 2]))),
        )
        self.assertEqual(output["b"], 3)
        self.assertTrue(
            np.all(np.equal(output["c"]["d"], np.array([4, 5]))),
        )
        self.assertTrue(
            np.all(np.equal(output["c"]["e"], np.array([6, 7]))),
        )
        output = read_nested_dict_from_hdf(
            file_name=self.file_name,
            h5_path=self.h5_path,
            recursive=True,
        )
        self.assertTrue(
            np.all(np.equal(output["a"], np.array([1, 2]))),
        )
        self.assertEqual(output["b"], 3)
        self.assertTrue(
            np.all(np.equal(output["c"]["d"], np.array([4, 5]))),
        )
        self.assertTrue(
            np.all(np.equal(output["c"]["e"], np.array([6, 7]))),
        )

    def test_write_overwrite_error(self):
        with self.assertRaises(OSError):
            _write_hdf(
                hdf_filehandle=self.file_name,
                data=self.data_hierarchical,
                h5_path=self.h5_path,
                overwrite=False,
            )

    def test_hdf5_structure(self):
        self.assertEqual(
            get_hdf5_raw_content(file_name=self.file_name),
            [
                {"data_hierarchical": {}},
                {"data_hierarchical/a": {"TITLE": "ndarray"}},
                {"data_hierarchical/b": {"TITLE": "int"}},
                {"data_hierarchical/c": {}},
                {"data_hierarchical/c/d": {"TITLE": "ndarray"}},
                {"data_hierarchical/c/e": {"TITLE": "ndarray"}},
            ],
        )

    def test_list_groups(self):
        with h5py.File(self.file_name, "r") as f:
            groups, nodes = _list_groups_and_nodes(hdf=f, h5_path="data_hierarchical")
        self.assertEqual(list(sorted(groups)), ["c"])
        self.assertEqual(list(sorted(nodes)), ["a", "b"])


class TestWriteDictHdfIO(TestCase):
    def setUp(self):
        self.file_name = "test_write_dict_to_hdf.h5"
        self.h5_path = "data_hierarchical"
        self.data_hierarchical = {"a": [1, 2], "b": 3, "c": {"d": 4, "e": 5}}
        write_dict_to_hdf(
            file_name=self.file_name,
            data_dict={
                posixpath.join(self.h5_path, k): v
                for k, v in self.data_hierarchical.items()
            },
        )

    def tearDown(self):
        os.remove(self.file_name)

    def test_read_hierarchical(self):
        with self.assertRaises(ValueError):
            _read_hdf(hdf_filehandle=self.file_name, h5_path=self.h5_path)

    def test_read_dict_hierarchical(self):
        self.assertEqual(
            self.data_hierarchical,
            read_nested_dict_from_hdf(file_name=self.file_name, h5_path=self.h5_path),
        )

    def test_hdf5_structure(self):
        self.assertEqual(
            get_hdf5_raw_content(file_name=self.file_name),
            [
                {"data_hierarchical": {}},
                {"data_hierarchical/a": {"TITLE": "json"}},
                {"data_hierarchical/b": {"TITLE": "int"}},
                {"data_hierarchical/c": {"TITLE": "json"}},
            ],
        )

    def test_list_groups(self):
        with h5py.File(self.file_name, "r") as f:
            groups, nodes = _list_groups_and_nodes(hdf=f, h5_path="data_hierarchical")
        self.assertEqual(groups, [])
        self.assertEqual(list(sorted(nodes)), ["a", "b", "c"])
