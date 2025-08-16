# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
import os
import sys
import warnings
from io import StringIO
import numpy as np
import pandas as pd
from pyiron_base.storage.hdfio import (
    FileHDFio,
    _is_ragged_in_1st_dim_only,
    state,
    _import_class,
)
from pyiron_base._tests import PyironTestCase, TestWithProject, ToyJob as BaseToyJob
from pyiron_base import GenericJob, JobType
import unittest


# Defining a ToyJob at an importable position. This is used for ProjectHDFio.import_class testing.
class ToyJob(GenericJob):
    pass


def _write_full_hdf_content(hdf):
    hdf["array"] = np.array([1, 2, 3, 4, 5, 6])
    hdf["array_3d"] = np.array([[1, 2, 3], [4, 5, 6]])
    hdf["traj"] = np.array([[[1, 2, 3], [4, 5, 6]], [[7, 8, 9]]], dtype=object)
    hdf["dict"] = {"key_1": 1, "key_2": "hallo"}
    hdf["dict_numpy"] = {"key_1": 1, "key_2": np.array([1, 2, 3, 4, 5, 6])}
    hdf["indices"] = np.array([1, 1, 1, 1, 6], dtype=int)
    with hdf.open("group") as grp:
        grp["some_entry"] = "present"


def _check_full_hdf_values(self, hdf, group="content"):
    with self.subTest(group + "/array"):
        array = hdf[group + "/array"]
        self.assertEqual(array, np.array([1, 2, 3, 4, 5, 6]))
        self.assertIsInstance(array, np.ndarray)
        self.assertEqual(array.dtype, np.dtype(int))

    with self.subTest(group + "/array_3d"):
        array = hdf[group]["array_3d"]
        self.assertEqual(
            array,
            np.array([[1, 2, 3], [4, 5, 6]]),
        )
        self.assertIsInstance(array, np.ndarray)
        self.assertEqual(array.dtype, np.dtype(int))

    with self.subTest(group + "/indices"):
        array = hdf[group + "/indices"]
        self.assertEqual(array, np.array([1, 1, 1, 1, 6]))
        self.assertIsInstance(array, np.ndarray)
        self.assertEqual(array.dtype, np.dtype(int))

    with self.subTest(group + "/traj"):
        array = hdf[group + "/traj"]
        self.assertEqual(array[0], np.array([[1, 2, 3], [4, 5, 6]]))
        self.assertEqual(array[1], np.array([[7, 8, 9]]))
        self.assertIsInstance(array, np.ndarray)
        self.assertEqual(array.dtype, np.dtype(object))

    with self.subTest(group + "/dict"):
        content_dict = hdf[group + "/dict"]
        self.assertEqual(content_dict["key_1"], 1)
        self.assertEqual(content_dict["key_2"], "hallo")
        self.assertIsInstance(content_dict, dict)

    with self.subTest(group + "/dict_numpy"):
        content_dict = hdf[group + "/dict_numpy"]
        self.assertEqual(content_dict["key_1"], 1)
        self.assertEqual(
            content_dict["key_2"],
            np.array([1, 2, 3, 4, 5, 6]),
        )

    with self.subTest(group + "/group/some_entry"):
        self.assertEqual(hdf[group + "/group/some_entry"], "present")


class TestFileHDFio(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        cls.empty_hdf5 = FileHDFio(file_name=cls.current_dir + "/filehdfio_empty.h5")
        cls.full_hdf5 = FileHDFio(file_name=cls.current_dir + "/filehdfio_full.h5")
        cls.i_o_hdf5 = FileHDFio(file_name=cls.current_dir + "/filehdfio_io.h5")
        cls.es_hdf5 = FileHDFio(
            file_name=cls.current_dir + "/../../static/dft/es_hdf.h5"
        )
        with cls.full_hdf5.open("content") as hdf:
            _write_full_hdf_content(hdf=hdf)
        with cls.i_o_hdf5.open("content") as hdf:
            hdf["exists"] = True
        # Open and store value in a hdf file to use test_remove_file on it, do not use otherwise
        cls.to_be_removed_hdf = FileHDFio(
            file_name=cls.current_dir + "/filehdfio_tbr.h5"
        )
        with cls.to_be_removed_hdf.open("content") as hdf:
            hdf["value"] = 1
        # Remains open to be closed by test_close, do not use otherwise
        cls.opened_hdf = cls.full_hdf5.open("content")

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        os.remove(cls.current_dir + "/filehdfio_full.h5")
        os.remove(cls.current_dir + "/filehdfio_io.h5")

    def test__is_convertable_dtype_object_array(self):
        object_array_with_lists = np.array(
            [[[1, 2, 3], [2, 3, 4]], [[4, 5, 6]]], dtype=object
        )
        int_array_as_objects_array = np.array([[1, 2, 3], [3, 4, 5]], dtype=object)
        float_array_as_objects_array = np.array(
            [[1.1, 1.3, 1.5], [2, 2.1, 2.2]], dtype=object
        )
        object_array_with_none = np.array(
            [[1.1, None, 1.5], [None, 2.1, 2.2]], dtype=object
        )

        self.assertFalse(
            self.i_o_hdf5._is_convertable_dtype_object_array(object_array_with_lists)
        )
        self.assertTrue(
            self.i_o_hdf5._is_convertable_dtype_object_array(int_array_as_objects_array)
        )
        self.assertTrue(
            self.i_o_hdf5._is_convertable_dtype_object_array(
                float_array_as_objects_array
            )
        )
        self.assertTrue(
            self.i_o_hdf5._is_convertable_dtype_object_array(object_array_with_none),
            msg="This array should be considered convertable, since first and last element are numbers.",
        )

    def test__convert_dtype_obj_array(self):
        object_array_with_lists = np.array(
            [[[1, 2, 3], [2, 3, 4]], [[4, 5, 6]]], dtype=object
        )
        int_array_as_objects_array = np.array([[1, 2, 3], [3, 4, 5]], dtype=object)
        float_array_as_objects_array = np.array(
            [[1.1, 1.3, 1.5], [2, 2.1, 2.2]], dtype=object
        )
        object_array_with_none = np.array(
            [[1.1, None, 1.5], [None, 2.1, 2.2]], dtype=object
        )

        self.assertIs(
            self.i_o_hdf5._convert_dtype_obj_array(object_array_with_lists),
            object_array_with_lists,
        )
        self.assertIs(
            self.i_o_hdf5._convert_dtype_obj_array(object_array_with_none),
            object_array_with_none,
        )

        with self.assertLogs(state.logger):
            array = self.i_o_hdf5._convert_dtype_obj_array(int_array_as_objects_array)
        self.assertEqual(array, int_array_as_objects_array)
        self.assertEqual(array.dtype, np.dtype(int))

        with self.assertLogs(state.logger):
            array = self.i_o_hdf5._convert_dtype_obj_array(float_array_as_objects_array)
        self.assertEqual(array, float_array_as_objects_array)
        self.assertEqual(array.dtype, np.dtype(float))

    def test_array_type_conversion(self):
        object_array_with_lists = np.array(
            [[[1, 2, 3], [2, 3, 4]], [[4, 5, 6]]], dtype=object
        )
        int_array_as_objects_array = np.array([[1, 2, 3], [3, 4, 5]], dtype=object)
        float_array_as_objects_array = np.array(
            [[1.1, 1.3, 1.5], [2, 2.1, 2.2]], dtype=object
        )

        hdf = self.i_o_hdf5.open("arrays")

        hdf["object_array_with_lists"] = object_array_with_lists
        hdf["int_array_as_objects_array"] = int_array_as_objects_array
        hdf["float_array_as_objects_array"] = float_array_as_objects_array

        warn_msg_start = "WARNING:pyiron_log:Deprecated data structure! Returned array was converted "

        with self.subTest("object_array_with_lists"):
            array = hdf["object_array_with_lists"]
            self.assertEqual(
                len(array),
                len(object_array_with_lists),
                msg="object array read with incorrect length!",
            )
            for a_read, a_written in zip(array, object_array_with_lists):
                # ProjectHDFio coerces lists inside numpy object arrays to
                # arrays, because h5io cannot write them otherwise, so we have
                # to do the same here
                self.assertEqual(
                    a_read,
                    np.asarray(a_written),
                    msg="object array contents not the same!",
                )
            self.assertIsInstance(array, np.ndarray)
            self.assertEqual(
                array.dtype,
                np.dtype(object),
                msg="dtype=object array falsely converted.",
            )

        #  Here I got:  TypeError: Object dtype dtype('O') has no native HDF5 equivalent
        #
        # object_array_with_none = np.array([[1.1, None, 1.5], [None, 2.1, 2.2]], dtype=object)
        # hdf['object_array_with_none'] = object_array_with_none
        # with self.subTest("object_array_with_none"):
        #     array = hdf['object_array_with_none']
        #     np.array_equal(array, object_array_with_none)
        #     self.assertIsInstance(array, np.ndarray)
        #     self.assertTrue(array.dtype == np.dtype(object))

        with self.subTest("int_array_as_objects_array"):
            array = hdf["int_array_as_objects_array"]
            self.assertEqual(array, int_array_as_objects_array)
            self.assertIsInstance(array, np.ndarray)

        with self.subTest("float_array_as_objects_array"):
            array = hdf["float_array_as_objects_array"]
            self.assertEqual(array, float_array_as_objects_array)
            self.assertIsInstance(array, np.ndarray)

        hdf.remove_group()

    def test_get_item(self):
        _check_full_hdf_values(self, self.full_hdf5)
        # Test leaving to pyiron Project at hdf file location:
        pr = self.full_hdf5["content/.."]
        from pyiron_base import Project

        self.assertIsInstance(pr, Project)
        self.assertEqual(pr.path, self.full_hdf5.file_path + "/")
        # Test leaving to pyiron Project at other than hdf file location:
        pr = self.full_hdf5[".."]
        self.assertIsInstance(pr, Project)
        self.assertEqual(
            pr.path.replace("\\", "/"),
            os.path.normpath(os.path.join(self.full_hdf5.file_path, "..")).replace(
                "\\", "/"
            )
            + "/",
        )
        # Test getting a new FileHDFio object:
        group_hdf = self.full_hdf5["content/group"]
        self.assertIsInstance(group_hdf, FileHDFio)
        self.assertEqual(group_hdf.h5_path, "/content/group")
        # Test getting the parent FileHDFio object:
        content_hdf = group_hdf[".."]
        self.assertIsInstance(content_hdf, FileHDFio)
        self.assertEqual(content_hdf.h5_path, self.full_hdf5.h5_path + "content")
        # Getting the '/' of the hdf would result in a path which already belongs to the project.
        # Therefore, the project is returned instead.
        pr = content_hdf[".."]
        self.assertIsInstance(pr, Project)
        self.assertEqual(pr.path, self.full_hdf5.file_path + "/")
        # Test getting the same object directly:
        pr = group_hdf["../.."]
        self.assertIsInstance(pr, Project)
        self.assertEqual(pr.path, self.full_hdf5.file_path + "/")

    def test_file_name(self):
        self.assertEqual(
            self.empty_hdf5.file_name, self.current_dir + "/filehdfio_empty.h5"
        )
        self.assertEqual(
            self.full_hdf5.file_name, self.current_dir + "/filehdfio_full.h5"
        )

    def test_h5_path(self):
        self.assertEqual(self.full_hdf5.h5_path, "/")

    def test_open(self):
        opened_hdf = self.full_hdf5.open("content")
        self.assertEqual(opened_hdf.h5_path, "/content")
        self.assertEqual(opened_hdf.history[-1], "content")

    def test_close(self):
        self.opened_hdf.close()
        self.assertEqual(self.opened_hdf.h5_path, "/")

    def test_remove_file(self):
        path = self.to_be_removed_hdf.file_name
        self.to_be_removed_hdf.remove_file()
        self.assertFalse(os.path.isfile(path))

    def test_delitem(self):
        """After deleting an entry, it should not be accessible anymore."""
        with self.full_hdf5.open("content") as opened_hdf:
            opened_hdf["dummy"] = 42
            del opened_hdf["dummy"]
            self.assertNotIn(
                "dummy", opened_hdf.list_nodes(), msg="Entry still in HDF after del!"
            )

    def test_get_from_table(self):
        pass

    def test_get_pandas(self):
        pass

    def test_get(self):
        self.assertEqual(
            self.full_hdf5.get("doesnotexist", default=42),
            42,
            "default value not returned when value doesn't exist.",
        )
        self.assertEqual(
            self.full_hdf5.get("content/array", default=42),
            np.array([1, 2, 3, 4, 5, 6]),
            "default value returned when value does exist.",
        )
        with self.assertRaises(ValueError):
            self.empty_hdf5.get("non_existing_key")

    def test_hd_copy(self):
        new_hdf_file = os.path.join(self.current_dir, "copy_full.h5")
        new_hdf = FileHDFio(file_name=new_hdf_file)
        new_hdf = self.full_hdf5.hd_copy(self.full_hdf5, new_hdf)
        _check_full_hdf_values(self, new_hdf)
        os.remove(new_hdf_file)

    def test_groups(self):
        groups = self.full_hdf5.groups()
        # _filter is actually relies on the _filter property of the Project, thus groups does not do anything.
        self.assertIsInstance(groups, FileHDFio)

    def test_rewrite_hdf5(self):
        with self.subTest("directly rewrite"):
            initial_file_size = self.full_hdf5.file_size()
            self.full_hdf5.rewrite_hdf5()
            _check_full_hdf_values(self, self.full_hdf5)
            initial_rewrite_file_size = self.full_hdf5.file_size()
            self.assertLess(initial_rewrite_file_size, initial_file_size)

        with self.subTest("increase file size"):
            with self.full_hdf5.open("content") as hdf:
                _write_full_hdf_content(hdf)
            increased_file_size = self.full_hdf5.file_size()
            _check_full_hdf_values(self, self.full_hdf5)
            self.assertGreater(
                increased_file_size,
                1.5 * initial_rewrite_file_size,
                msg="Expected the re-filled hdf file to be substantially larger",
            )

        with self.subTest("rewrite again"):
            self.full_hdf5.rewrite_hdf5()
            final_file_size = self.full_hdf5.file_size()
            _check_full_hdf_values(self, self.full_hdf5)
            self.assertLess(
                final_file_size,
                increased_file_size,
                msg="rewriting the hdf did not reduce file size.",
            )
            self.assertLess(
                abs(final_file_size - initial_rewrite_file_size),
                0.01 * initial_file_size,
                msg="Final file size not within 5% of the initial file size.",
            )

        with self.subTest("hdf with two groups"):
            new_hdf_file = os.path.join(self.current_dir, "twice_full.h5")
            new_hdf = FileHDFio(file_name=new_hdf_file)
            new_hdf = self.full_hdf5.hd_copy(self.full_hdf5, new_hdf)

            with new_hdf.open("content_job2") as hdf:
                _write_full_hdf_content(hdf)
            _check_full_hdf_values(self, new_hdf)
            _check_full_hdf_values(self, new_hdf, group="content_job2")

            new_hdf.rewrite_hdf5()
            _check_full_hdf_values(self, new_hdf)
            _check_full_hdf_values(self, new_hdf, group="content_job2")

            os.remove(new_hdf_file)

        new_hdf_file = os.path.join(self.current_dir, "twice_full.h5")
        int_array_as_objects_array = np.array([[1, 2, 3], [3, 4, 5]], dtype=object)
        with new_hdf.open("content") as hdf:
            hdf["int_array"] = int_array_as_objects_array

        with self.subTest("warning handling - suppress warning"):
            with self.assertLogs(logger=state.logger) as w:
                msg = "Nothing, only asserting logs..."
                state.logger.info(msg)
                new_hdf.rewrite_hdf5()
                self.assertEqual(w.output, ["INFO:pyiron_log:" + msg])

        with self.subTest("warning handling - log job_name warning"):
            with self.assertLogs(logger=state.logger) as w:
                new_hdf.rewrite_hdf5("job_name")
                self.assertEqual(
                    w.output,
                    [
                        "WARNING:pyiron_log:Specifying job_name is deprecated and ignored! "
                        "Future versions will change signature."
                    ],
                )

        with self.subTest("warning handling - log and catch job_name warning"):
            with self.assertLogs(logger=state.logger) as lw:
                with warnings.catch_warnings(record=True) as w:
                    new_hdf.rewrite_hdf5(job_name="job_name")
                    self.assertTrue(len(w) <= 2)
                    self.assertEqual(
                        str(w[0].message),
                        "pyiron_base.storage.hdfio.rewrite_hdf5(job_name=job_name) "
                        + "is deprecated.",
                    )
                self.assertEqual(
                    lw.output,
                    [
                        "WARNING:pyiron_log:Specifying job_name is deprecated and ignored! "
                        "Future versions will change signature."
                    ],
                )

        with self.subTest("warning handling - deprecate exclude_groups"):
            with warnings.catch_warnings(record=True) as w:
                new_hdf.rewrite_hdf5(exclude_groups="some")
                self.assertTrue(len(w) <= 2)
                self.assertEqual(
                    str(w[0].message),
                    "pyiron_base.storage.hdfio.rewrite_hdf5(exclude_groups=some) "
                    + "is deprecated.",
                )

        with self.subTest("warning handling - deprecate exclude_nodes"):
            with warnings.catch_warnings(record=True) as w:
                new_hdf.rewrite_hdf5(exclude_nodes="any")
                self.assertTrue(len(w) <= 2)
                self.assertEqual(
                    str(w[0].message),
                    "pyiron_base.storage.hdfio.rewrite_hdf5(exclude_nodes=any) "
                    + "is deprecated.",
                )

        os.remove(new_hdf_file)

    def test_to_object(self):
        pass

    def test_put(self):
        self.i_o_hdf5.put("answer", 42)
        self.assertEqual(self.i_o_hdf5["answer"], 42)

    def test_list_all(self):
        empty_file_dict = self.empty_hdf5.list_all()
        self.assertEqual(empty_file_dict["groups"], [])
        self.assertEqual(empty_file_dict["nodes"], [])
        es_file_dict = self.es_hdf5.list_all()
        self.assertEqual(es_file_dict["groups"], ["es_new", "es_old"])
        self.assertEqual(es_file_dict["nodes"], [])
        es_group_dict = self.es_hdf5["es_new"].list_all()
        self.assertEqual(es_group_dict["groups"], ["dos"])
        self.assertEqual(
            es_group_dict["nodes"],
            ["TYPE", "efermi", "eig_matrix", "k_points", "k_weights", "occ_matrix"],
        )

    def test_list_nodes(self):
        self.assertEqual(self.empty_hdf5.list_nodes(), [])
        self.assertEqual(
            self.es_hdf5["es_new"].list_nodes(),
            ["TYPE", "efermi", "eig_matrix", "k_points", "k_weights", "occ_matrix"],
        )

    def test_list_groups(self):
        self.assertEqual(self.empty_hdf5.list_groups(), [])
        self.assertEqual(self.es_hdf5.list_groups(), ["es_new", "es_old"])

    def test_listdirs(self):
        self.assertEqual(self.empty_hdf5.listdirs(), [])
        self.assertEqual(self.es_hdf5.listdirs(), ["es_new", "es_old"])

    def test_show_hdf(self):
        sys_stdout = sys.stdout
        result = StringIO()
        sys.stdout = result
        self.full_hdf5.show_hdf()
        result_string = result.getvalue()
        sys.stdout = sys_stdout
        self.assertEqual(
            result_string,
            "group:  content\n  node array\n  node array_3d\n  node dict\n  node dict_numpy\n"
            + "  node indices\n  node traj\n  group:  group\n    node some_entry\n",
        )

    def test_is_empty(self):
        self.assertTrue(self.empty_hdf5.is_empty)
        self.assertFalse(self.full_hdf5.is_empty)

    def test_is_root(self):
        self.assertTrue(self.full_hdf5.is_root)
        hdf = self.full_hdf5["content"]
        self.assertFalse(hdf.is_root)

    def test_base_name(self):
        self.assertEqual(self.full_hdf5.base_name, "filehdfio_full")
        self.assertEqual(self.empty_hdf5.base_name, "filehdfio_empty")
        self.assertEqual(self.i_o_hdf5.base_name, "filehdfio_io")

    def test_file_size(self):
        self.assertTrue(self.es_hdf5.file_size() > 0)

    def test_get_size(self):
        self.assertTrue(self.es_hdf5.get_size(self.es_hdf5) > 0)

    def test_copy(self):
        copy = self.es_hdf5.copy()
        self.assertIsInstance(copy, FileHDFio)
        self.assertEqual(copy.h5_path, self.es_hdf5.h5_path)

    # results in an Error
    #   File "C:\Users\Siemer\pyiron_git\pyiron_base\tests\generic\test_fileHDFio.py", line 249, in test_copy_to
    #     copy = self.full_hdf5.copy_to(destination)
    #   File "C:\Users\Siemer\pyiron_git\pyiron_base\pyiron_base\generic\hdfio.py", line 355, in copy_to
    #     _internal_copy(source=f_source, source_path=self._h5_path, target=f_target,
    #   File "C:\Users\Siemer\pyiron_git\pyiron_base\pyiron_base\generic\hdfio.py", line 332, in _internal_copy
    #     source.copy(source_path, target, name=target_path)
    #   File "C:\Users\Siemer\anaconda3\envs\pyiron_git\lib\site-packages\h5py\_hl\group.py", line 494, in copy
    #     h5o.copy(source.id, self._e(source_path), dest.id, self._e(dest_path),
    #   File "h5py\_objects.pyx", line 54, in h5py._objects.with_phil.wrapper
    #   File "h5py\_objects.pyx", line 55, in h5py._objects.with_phil.wrapper
    #   File "h5py\h5o.pyx", line 217, in h5py.h5o.copy
    #   ValueError: No destination name specified (no destination name specified)
    # def test_copy_to(self):
    #    file_name = self.current_dir + '/filehdfio_tmp'
    #    destination = FileHDFio(file_name=file_name)
    #    copy = self.full_hdf5.copy_to(destination)
    #    self._check_full_hdf_values(copy)
    #    os.remove(file_name)

    def test_remove_group(self):
        grp = "group_to_be_removed"
        hdf = self.i_o_hdf5.create_group(grp)
        # If nothing is written to the group, the creation is not reflected by the HDF5 file
        hdf["key"] = 1
        self.assertTrue(grp in self.i_o_hdf5.list_groups())
        hdf.remove_group()
        self.assertFalse(grp in self.i_o_hdf5.list_nodes())
        # This should not raise an error, albeit the group of hdf is removed
        hdf.remove_group()

    def test_ragged_array(self):
        """Should correctly identify ragged arrays/lists."""
        self.assertTrue(
            _is_ragged_in_1st_dim_only([[1], [1, 2]]),
            "Ragged nested list not detected!",
        )
        self.assertTrue(
            _is_ragged_in_1st_dim_only([np.array([1]), np.array([1, 2])]),
            "Ragged list of arrays not detected!",
        )
        self.assertFalse(
            _is_ragged_in_1st_dim_only([[1, 2], [3, 4]]),
            "Non-ragged nested list detected incorrectly!",
        )
        self.assertFalse(
            _is_ragged_in_1st_dim_only(np.array([[1, 2], [3, 4]])),
            "Non-ragged array detected incorrectly!",
        )
        self.assertTrue(
            _is_ragged_in_1st_dim_only([[[1]], [[2], [3]]]),
            "Ragged nested list not detected even though shape[1:] matches!",
        )
        self.assertFalse(
            _is_ragged_in_1st_dim_only([[[1, 2, 3]], [[2]], [[3]]]),
            "Ragged nested list detected incorrectly even though shape[1:] don't match!",
        )


class TestProjectHDFio(TestWithProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.empty_hdf5 = cls.project.create_hdf(cls.project.path, "projhdfio_empty")
        cls.full_hdf5 = cls.project.create_hdf(cls.project.path, "projhdfio_full")
        cls.i_o_hdf5 = cls.project.create_hdf(cls.project.path, "projhdfio_io")
        with cls.full_hdf5.open("content") as hdf:
            _write_full_hdf_content(hdf=hdf)
        with cls.i_o_hdf5.open("content") as hdf:
            hdf["exists"] = True
        # Open and store value in a hdf file to use test_remove_file on it, do not use otherwise
        cls.to_be_removed_hdf = cls.project.create_hdf(
            cls.project.path, "projhdfio_tbr"
        )
        with cls.to_be_removed_hdf.open("content") as hdf:
            hdf["value"] = 1
        # Remains open to be closed by test_close, do not use otherwise
        cls.opened_hdf = cls.full_hdf5.open("content")

    def test_close(self):
        self.assertEqual(self.opened_hdf.h5_path, "/projhdfio_full/content")
        self.opened_hdf.close()
        self.assertEqual(self.opened_hdf.h5_path, "/projhdfio_full")

    def test_remove_file(self):
        path = self.to_be_removed_hdf.file_name
        self.to_be_removed_hdf.remove_file()
        self.assertFalse(os.path.isfile(path))

    def test_content(self):
        _check_full_hdf_values(self, self.full_hdf5)

    def test_rewrite_hdf5(self):
        self.full_hdf5.rewrite_hdf5()
        _check_full_hdf_values(self, self.full_hdf5)

        full_hdf5 = self.project.create_hdf(self.project.path, "projhdfio_full_2")
        hdf = full_hdf5.open("content")
        _write_full_hdf_content(hdf=hdf)
        _check_full_hdf_values(self, full_hdf5)

        with hdf.open("content") as inner_hdf:
            _write_full_hdf_content(inner_hdf)
        _check_full_hdf_values(self, hdf)

        hdf.rewrite_hdf5()
        _check_full_hdf_values(self, full_hdf5)

        # with self.subTest("Adding job-sibling"):
        #    new_hdf = ProjectHDFio(self.project, 'twice_full')
        #    self.full_hdf5.hd_copy(self.full_hdf5, new_hdf)
        #    _check_full_hdf_values(self, new_hdf)
        #    old_path = new_hdf.h5_path
        #    new_hdf.h5_path = '/'
        #    with new_hdf.open('job_sibling') as hdf:
        #        _write_full_hdf_content(hdf)
        #    _check_full_hdf_values(self, new_hdf, group='job_sibling')
        #    new_hdf.h5_path = old_path
        #    new_hdf.rewrite_hdf5('projhdfio_full')

        #    new_hdf.h5_path = '/'
        #    _check_full_hdf_values(self, new_hdf, group='job_sibling')

    def test_import_class(self):
        with self.subTest("import ToyJob without interfering:"):
            toy_job_cls = _import_class(BaseToyJob.__module__, BaseToyJob.__name__)
            self.assertIs(
                toy_job_cls, BaseToyJob, msg="Did not return the requested class."
            )

        try:
            JobType.register(ToyJob)

            with self.subTest("Import ToyJob while another ToyJob is registered"):
                with self.assertLogs(state.logger) as log:
                    toy_job_cls = _import_class(
                        BaseToyJob.__module__, BaseToyJob.__name__
                    )
                    self.assertEqual(
                        len(log.output),
                        1,
                        msg="The conversion info should be the only thing logged here.",
                    )
                    log_msg = log.output[0]
                    self.assertTrue(
                        log_msg.startswith('INFO:pyiron_log:Using registered module "'),
                        msg="Unexpected log message.",
                    )
                    self.assertTrue(
                        log_msg.endswith(
                            'test_fileHDFio" instead of custom/old module '
                            '"pyiron_base._tests" to import job type "ToyJob"!'
                        ),
                        msg="Unexpected log message.",
                    )
                self.assertIs(
                    toy_job_cls,
                    ToyJob,
                    msg="Did not convert to internal (registered) JobClass.",
                )
        finally:
            JobType.unregister(ToyJob)


class TestFileHDFioCoverage(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.current_dir = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
        cls.hdf5_file = os.path.join(cls.current_dir, "test_coverage.h5")

    def setUp(self):
        super().setUp()
        self.hdf = FileHDFio(file_name=self.hdf5_file)
        _write_full_hdf_content(self.hdf.create_group("test_content"))

    def tearDown(self):
        super().tearDown()
        if os.path.exists(self.hdf5_file):
            os.remove(self.hdf5_file)
        copy_exclude_file = os.path.join(self.current_dir, "copy_exclude.h5")
        if os.path.exists(copy_exclude_file):
            os.remove(copy_exclude_file)

    def test_getitem_slice(self):
        with self.assertRaises(NotImplementedError):
            _ = self.hdf[1:]

    def test_getitem_unknown(self):
        with self.assertRaises(ValueError):
            _ = self.hdf["unknown_item"]

    def test_repr(self):
        self.assertEqual(
            repr(self.hdf),
            str(self.hdf.list_all())
        )

    def test_items(self):
        items = self.hdf.items()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0][0], "test_content")
        self.assertIsInstance(items[0][1], FileHDFio)

    def test_rewrite_hdf5_info(self):
        from io import StringIO
        import sys

        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        self.hdf.rewrite_hdf5(info=True)
        sys.stdout = old_stdout
        self.assertIn("compression rate", captured_output.getvalue())

    def test_hd_copy_exclude(self):
        new_hdf_file = os.path.join(self.current_dir, "copy_exclude.h5")
        new_hdf = FileHDFio(file_name=new_hdf_file)
        self.hdf.hd_copy(self.hdf, new_hdf, exclude_groups=["test_content"])
        self.assertEqual(len(new_hdf.list_groups()), 0)
        new_hdf.remove_file()

        new_hdf = FileHDFio(file_name=new_hdf_file)
        self.hdf.hd_copy(self.hdf['test_content'], new_hdf, exclude_nodes=["array"])
        self.assertNotIn("array", new_hdf.list_nodes())
        new_hdf.remove_file()

    def test_read_dict_from_hdf(self):
        test_dict = self.hdf['test_content'].read_dict_from_hdf()
        self.assertEqual(test_dict['array'][0], 1)

    def test_get_from_table(self):
        df = pd.DataFrame({"Parameter": ["a", "b"], "Value": [1, 2]})
        self.hdf["my_table"] = df
        self.assertEqual(self.hdf.get_from_table("my_table", "a"), 1)
        with self.assertRaises(ValueError):
            self.hdf.get_from_table("my_table", "c")

    def test_get_pandas(self):
        d = {"a": [1, 2], "b": [3, 4]}
        self.hdf["my_dict"] = d
        df = self.hdf.get_pandas("my_dict")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df["a"][0], 1)
        self.assertIsNone(self.hdf.get_pandas("test_content/array"))

    def test_values(self):
        values = self.hdf.values()
        self.assertEqual(len(values), 1)
        self.assertIsInstance(values[0], FileHDFio)

    def test_nodes_filter(self):
        nodes_hdf = self.hdf.nodes()
        self.assertIn("_filter", nodes_hdf.__dict__.keys())

    def test_groups_filter(self):
        groups_hdf = self.hdf.groups()
        self.assertIn("_filter", groups_hdf.__dict__.keys())

    def test_list_all_non_existent(self):
        hdf = self.hdf.open('non_existent')
        self.assertEqual(hdf.list_all(), {'groups': [], 'nodes': []})

    def test_import_class_error(self):
        with self.assertRaises(ImportError):
            _import_class("non_existent_module", "NonExistentClass")

    def test_create_group_value_error(self):
        self.hdf.create_group("my_group")
        # Creating the same group again should not raise an error
        self.hdf.create_group("my_group")

    def test_exit(self):
        with self.hdf.open('test_content') as hdf:
            self.assertEqual(hdf.h5_path, '/test_content')
        self.assertEqual(self.hdf.h5_path, '/')


class FHA(object):
    def __init__(self, a=None, b=None):
        self.a = a
        self.b = b

    @staticmethod
    def from_hdf_args(hdf):
        return {"a": hdf["a"], "b": hdf["b"]}

    def to_hdf(self, hdf, group_name):
        with hdf.open(group_name) as hdf_group:
            hdf_group["a"] = self.a
            hdf_group["b"] = self.b
            hdf_group["TYPE"] = str(type(self))

    def from_hdf(self, hdf, group_name):
        pass


class TestProjectHDFioCoverage(TestWithProject):

    def test_to_object_no_type(self):
        hdf = self.project.create_hdf(self.project.path, "no_type.h5")
        hdf.create_group("no_type_group")
        with self.assertRaises(ValueError):
            hdf['no_type_group'].to_object()
        hdf.remove_file()

    def test_to_object_with_from_hdf_args(self):
        obj = FHA(a=1, b=2)
        hdf = self.project.create_hdf(self.project.path, 'fha_test.h5')
        obj.to_hdf(hdf, "fha_test")
        reloaded_obj = hdf['fha_test'].to_object()
        self.assertEqual(obj.a, reloaded_obj.a)
        self.assertEqual(obj.b, reloaded_obj.b)
        hdf.remove_file()

    def test_to_object_generic_job(self):
        job = self.project.create_job(BaseToyJob, "toy_job")
        job.run()
        reloaded_job = job.project_hdf5.to_object()
        self.assertEqual(job.job_name, reloaded_job.job_name)
        job.remove()


if __name__ == "__main__":
    unittest.main()
