import os
import posixpath
import h5py
from unittest import TestCase
from pyiron_base.storage.helper_functions import (
    list_groups_and_nodes,
    read_dict_from_hdf,
)
from h5io_browser.base import write_dict_to_hdf, _read_hdf, _write_hdf


def get_hdf5_raw_content(file_name):
    item_lst = []

    def collect_attrs(name, obj):
        item_lst.append({name: {k: v for k, v in obj.attrs.items()}})

    with h5py.File(file_name, 'r') as f:
        f.visititems(collect_attrs)

    return item_lst


class TestWriteHdfIO(TestCase):
    def setUp(self):
        self.file_name = "test_write_hdf5.h5"
        self.h5_path = "data_hierarchical"
        self.data_hierarchical = {"a": [1, 2], "b": 3, "c": {"d": 4, "e": 5}}
        _write_hdf(hdf_filehandle=self.file_name, data=self.data_hierarchical, h5_path=self.h5_path)

    def tearDown(self):
        os.remove(self.file_name)

    def test_read_hierarchical(self):
        self.assertEqual(self.data_hierarchical, _read_hdf(hdf_filehandle=self.file_name, h5_path=self.h5_path))

    def test_read_dict_hierarchical(self):
        self.assertEqual({'key_b': 3}, read_dict_from_hdf(file_name=self.file_name, h5_path=self.h5_path))
        self.assertEqual(
            {'key_a': {'idx_0': 1, 'idx_1': 2}, 'key_b': 3, 'key_c': {'key_d': 4, 'key_e': 5}},
            read_dict_from_hdf(
                file_name=self.file_name,
                h5_path=self.h5_path,
                group_paths=["key_a", "key_c"],
            )
        )
        self.assertEqual(
            {'key_a': {'idx_0': 1, 'idx_1': 2}, 'key_b': 3, 'key_c': {'key_d': 4, 'key_e': 5}},
            read_dict_from_hdf(
                file_name=self.file_name,
                h5_path=self.h5_path,
                recursive=True,
            )
        )

    def test_write_overwrite_error(self):
        with self.assertRaises(OSError):
            _write_hdf(hdf_filehandle=self.file_name, data=self.data_hierarchical, h5_path=self.h5_path, overwrite=False)

    def test_hdf5_structure(self):
        self.assertEqual(get_hdf5_raw_content(file_name=self.file_name), [
            {'data_hierarchical': {'TITLE': 'dict'}},
            {'data_hierarchical/key_a': {'TITLE': 'list'}},
            {'data_hierarchical/key_a/idx_0': {'TITLE': 'int'}},
            {'data_hierarchical/key_a/idx_1': {'TITLE': 'int'}},
            {'data_hierarchical/key_b': {'TITLE': 'int'}},
            {'data_hierarchical/key_c': {'TITLE': 'dict'}},
            {'data_hierarchical/key_c/key_d': {'TITLE': 'int'}},
            {'data_hierarchical/key_c/key_e': {'TITLE': 'int'}}
        ])

    def test_list_groups(self):
        with h5py.File(self.file_name, 'r') as f:
            groups, nodes = list_groups_and_nodes(hdf=f, h5_path="data_hierarchical")
        self.assertEqual(list(sorted(groups)), ['key_a', 'key_c'])
        self.assertEqual(nodes, ['key_b'])


class TestWriteDictHdfIO(TestCase):
    def setUp(self):
        self.file_name = "test_write_dict_to_hdf.h5"
        self.h5_path = "data_hierarchical"
        self.data_hierarchical = {"a": [1, 2], "b": 3, "c": {"d": 4, "e": 5}}
        write_dict_to_hdf(
            file_name=self.file_name,
            data_dict={posixpath.join(self.h5_path, k): v for k, v in self.data_hierarchical.items()}
        )

    def tearDown(self):
        os.remove(self.file_name)

    def test_read_hierarchical(self):
        with self.assertRaises(ValueError):
            _read_hdf(hdf_filehandle=self.file_name, h5_path=self.h5_path)

    def test_read_dict_hierarchical(self):
        self.assertEqual(self.data_hierarchical, read_dict_from_hdf(file_name=self.file_name, h5_path=self.h5_path))

    def test_hdf5_structure(self):
        self.assertEqual(get_hdf5_raw_content(file_name=self.file_name), [
            {'data_hierarchical': {}},
            {'data_hierarchical/a': {'TITLE': 'json'}},
            {'data_hierarchical/b': {'TITLE': 'int'}},
            {'data_hierarchical/c': {'TITLE': 'json'}}
        ])

    def test_list_groups(self):
        with h5py.File(self.file_name, 'r') as f:
            groups, nodes = list_groups_and_nodes(hdf=f, h5_path="data_hierarchical")
        self.assertEqual(groups, [])
        self.assertEqual(list(sorted(nodes)), ['a', 'b', 'c'])