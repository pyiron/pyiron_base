# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base._tests import PyironTestCase
from pyiron_base.database.manager import DatabaseManager
from pyiron_base.settings.generic import Settings


class TestDatabaseManager(PyironTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.dbm = DatabaseManager()
        cls.s = Settings()

    def test_database_is_disabled(self):
        self.assertEqual(self.s.configuration["disable_database"], self.dbm.database_is_disabled,
                         msg="Database manager should be initialized by settings.")
        self.dbm._database_is_disabled = True
        self.assertNotEqual(self.s.configuration["disable_database"], self.dbm.database_is_disabled,
                            msg="But after that it should be independent from the settings")
        self.dbm._database_is_disabled = False  # Re-enable it at the end of the test
