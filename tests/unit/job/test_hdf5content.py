# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
import os
from pyiron_base import DataContainer
from pyiron_base.project.generic import Project
from pyiron_base._tests import PyironTestCase

test_keys = ["my", "recursive", "test", "data"]


def _wrap(k, *vs):
    if vs == ():
        return k
    else:
        return {k: _wrap(*vs)}


test_data = _wrap(*test_keys)


class InspectTest(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = os.path.dirname(os.path.abspath(__file__))
        cls.project = Project(os.path.join(cls.file_location, "hdf5_content"))
        cls.ham = cls.project.create_job("ScriptJob", "job_test_run")
        cls.ham["user/test"] = DataContainer(test_data)
        cls.ham.save()

    @classmethod
    def tearDownClass(cls):
        project = Project(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "hdf5_content")
        )
        ham = project.load(project.get_job_ids()[0])
        ham.remove()
        project.remove(enable=True)

    def test_inspect_job(self):
        job_inspect = self.project.inspect(self.ham.job_name)
        self.assertIsNotNone(job_inspect)
        self.assertEqual(
            job_inspect.content.input.__repr__(), job_inspect["input"].__repr__()
        )
        self.assertEqual(
            sorted((job_inspect.content.input).keys()),
            sorted(
                job_inspect["input"].list_nodes() + job_inspect["input"].list_groups()
            ),
        )

    def test_recusive_load(self):
        """DataContainer values should be accessible at any (recursive) level
        without explicit to_object() from job.content."""
        for i in range(len(test_keys)):
            container_path = "/".join(test_keys[:i])
            with self.subTest(container_path=container_path):
                try:
                    val = self.ham.content["user/test/" + container_path]
                    self.assertEqual(
                        _wrap(*test_keys[i:]),
                        val,
                        "HDF5Content did not return correct value from "
                        "recursive DataContainer!",
                    )
                    # last val we get will be a str
                    if i + 1 != len(test_keys):
                        self.assertIsInstance(
                            val,
                            DataContainer,
                            "HDF5Content did not return a DataContainers!",
                        )
                except KeyError:
                    self.fail(
                        "HDF5Content should not raise errors in partial "
                        "access to recursive DataContainers"
                    )

    def test_setitem(self):
        self.ham["user/output/some_value"] = 0.3
        self.assertEqual(self.ham["user/output/some_value"], 0.3)
        with self.assertRaises(ValueError):
            self.ham["input/value"] = 1


if __name__ == "__main__":
    unittest.main()
