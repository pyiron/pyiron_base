# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base.generic.fileio import read, write
import os
import unittest

class TestFileIO(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = {"a": 2, "b": {"c": 4, "d": {"e": 5,"f": 6}}, 'g': [1,2,3]}
        cls.yaml_name = "data.yaml"

    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(cls.yaml_name)
        except FileNotFoundError:
            pass

    def test_yaml_consistency(self):
        """Writing the test data to yaml then reading it should leave it unchanged."""
        write(self.data, self.yaml_name)
        self.assertEqual(self.data, read(self.yaml_name),
                         "Read data not the same as written data.")

    def test_unsupported_file_raises_error(self):
        """Trying to write to a file with an unknown file extension should raise an error."""
        with self.assertRaises(ValueError, msg="write didn't raise ValueError"):
            write(self.data, "data.foo")
