# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
import json
import os
import unittest
import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

from pyiron_base.project.external import Notebook, dump, load
from pyiron_base._tests import TestWithProject, PyironTestCase


class TestExternal(TestWithProject):
    def test_dump_and_load(self):
        current_dir = os.getcwd()
        truth_dict = {"a": 1, "b": 2, "c": 3}
        self.scriptjob = self.project.create.job.ScriptJob("script")
        for k, v in truth_dict.items():
            self.scriptjob.input[k] = v
        self.scriptjob.server.run_mode.manual = True
        self.scriptjob.script_path = __file__
        self.scriptjob.run()
        os.chdir(self.scriptjob.working_directory)
        reload_dict = load()
        for k, v in truth_dict.items():
            self.assertEqual(reload_dict[k], v)
        reload_dict["d"] = 4
        dump(output_dict=reload_dict)
        os.chdir(current_dir)
        script_job_reload = self.project.load(self.scriptjob.job_id)
        self.assertEqual(script_job_reload["output"].to_builtin()["d"], 4)


class TestNotebook(PyironTestCase):
    """Tests for Notebook static methods."""

    @property
    def docstring_module(self):
        import pyiron_base.project.external
        return pyiron_base.project.external

    def test_notebook_get_custom_dict_delegates_to_load(self):
        sentinel = object()
        with patch("pyiron_base.project.external.load", return_value=sentinel) as mock_load:
            result = Notebook.get_custom_dict()
        mock_load.assert_called_once()
        self.assertIs(result, sentinel)

    def test_notebook_store_custom_output_dict_delegates_to_dump(self):
        output = {"x": 1}
        with patch("pyiron_base.project.external.dump") as mock_dump:
            Notebook.store_custom_output_dict(output)
        mock_dump.assert_called_once_with(output_dict=output)


class FakePathBase:
    """Helper fake Path class for load() tests."""

    def __init__(self, p="."):
        self._p = str(p)

    def cwd(self):
        return self

    @property
    def parts(self):
        return ("", "home", "user", "project", "job")

    @property
    def parents(self):
        return [
            FakePathBase("/home/user/project/job"),
            FakePathBase("/home/user/project"),
        ]

    def __truediv__(self, other):
        return FakePathBase(self._p + "/" + str(other))

    def __str__(self):
        return self._p


class TestLoadPaths(PyironTestCase):
    """Tests for the various code paths inside load()."""

    @property
    def docstring_module(self):
        import pyiron_base.project.external
        return pyiron_base.project.external

    def _make_hdf_mock(self, include_custom_dict=True):
        hdf_input_mock = MagicMock()
        hdf_input_mock.list_nodes.return_value = (
            ["custom_dict"] if include_custom_dict else []
        )
        hdf_file_mock = MagicMock()
        hdf_file_mock.__getitem__ = MagicMock(return_value=hdf_input_mock)
        return hdf_file_mock

    def test_load_backwards_compat_path(self):
        """Line 61: obj.from_hdf() called when 'custom_dict' not in list_nodes."""
        import pyiron_base.project.external as ext_mod

        class FakePath(FakePathBase):
            def exists(self):
                return ".h5" in self._p

        original_path = ext_mod.Path
        ext_mod.Path = FakePath
        try:
            hdf_mock = self._make_hdf_mock(include_custom_dict=False)
            obj_mock = MagicMock()
            obj_mock.__getitem__ = MagicMock(return_value=1)

            with patch.object(ext_mod, "FileHDFio", return_value=hdf_mock), \
                 patch.object(ext_mod, "DataContainer", return_value=obj_mock):
                result = ext_mod.load()

            obj_mock.from_hdf.assert_called_once()
        finally:
            ext_mod.Path = original_path

    def test_load_json_path(self):
        """Lines 64-66: JSON file loading path."""
        import pyiron_base.project.external as ext_mod

        class FakePath(FakePathBase):
            def exists(self):
                if ".h5" in self._p:
                    return False
                if "input.json" in self._p:
                    return True
                return False

        original_path = ext_mod.Path
        ext_mod.Path = FakePath
        try:
            json_data = {"key": "value"}
            with patch("builtins.open", mock_open(read_data=json.dumps(json_data))), \
                 patch.object(ext_mod.json, "load", return_value=json_data):
                result = ext_mod.load()
            self.assertEqual(result, json_data)
        finally:
            ext_mod.Path = original_path

    def test_load_warning_path(self):
        """Lines 68-69: Warning when neither HDF nor JSON file exists."""
        import pyiron_base.project.external as ext_mod

        class FakePath(FakePathBase):
            def exists(self):
                return False

        original_path = ext_mod.Path
        ext_mod.Path = FakePath
        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = ext_mod.load()
            self.assertIsNone(result)
            self.assertTrue(len(w) >= 1)
            self.assertIn("not found", str(w[0].message))
        finally:
            ext_mod.Path = original_path
