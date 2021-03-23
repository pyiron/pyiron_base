# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from os.path import dirname, join, abspath
from os import remove
from pyiron_base.project.generic import Project
from pyiron_base.generic.object import HasStorage, HasDatabase, PyironObject
from pyiron_base import DataContainer
from pyiron_base.database.generic import DatabaseAccess


class TestWithProject(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = dirname(abspath(__file__)).replace("\\", "/")
        cls.project_name = join(cls.file_location, "test_project")

    @classmethod
    def tearDownClass(cls):
        try:
            remove(join(cls.file_location, "pyiron.log"))
        except FileNotFoundError:
            pass

    def tearDown(self):
        self.project.remove(enable=True)

    def setUp(self):
        self.project = Project(self.project_name)


class TestHasStorage(TestWithProject):

    def setUp(self):
        super().setUp()
        self.has_storage = HasStorage()

    def test_storage(self):
        self.assertIsInstance(
            self.has_storage.storage,
            DataContainer,
            msg=f"Expected storage to be a {DataContainer.__name__} but got {type(self.has_storage.storage).__name__}"
        )
        with self.assertRaises(AttributeError, msg="Expected the storage field to be read-only."):
            self.has_storage.storage = 42

    def test_hdf(self):
        self.has_storage.storage.foo = 'foo'
        hdf = self.project.create_hdf(path=self.project.path, job_name='hasstorage')
        self.has_storage.to_hdf(hdf=hdf)
        new_instance = HasStorage()
        new_instance.from_hdf(hdf=hdf)
        self.assertEqual(
            self.has_storage.storage.foo,
            new_instance.storage.foo,
            msg="Loaded object does not have same data."
        )


class TestHasDatabase(TestWithProject):

    def setUp(self):
        super().setUp()
        self.has_database = HasDatabase()

    def test_database(self):
        self.assertIsInstance(
            self.has_database.database,
            DatabaseAccess,
            msg=f"Expected storage to be a {DatabaseAccess.__name__} "
                f"but got {type(self.has_database.database).__name__}"
        )
        with self.assertRaises(AttributeError, msg="Expected the database field to be read-only."):
            self.has_database.database = 42


class TestPyironObject(TestWithProject):
    def setUp(self):
        super().setUp()
        self.obj = PyironObject()

    def test_has_storage(self):
        self.assertIsInstance(self.obj.storage, DataContainer, msg=f"Expected storage to be {DataContainer.__name__}.")

    def test_has_database(self):
        self.assertIsInstance(
            self.obj.database,
            DatabaseAccess,
            msg=f"Expected database to be {DatabaseAccess.__name__}."
        )
