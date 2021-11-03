# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base._tests import TestWithProject
from pyiron_base import state
from os import getcwd


class TestDatabaseManager(TestWithProject):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.dbm = state.database
        cls.s = state.settings

    def test_database_is_disabled(self):
        self.assertEqual(self.s.configuration["disable_database"], self.dbm.database_is_disabled,
                         msg="Database manager should be initialized by settings.")
        self.dbm._database_is_disabled = True
        self.assertNotEqual(self.s.configuration["disable_database"], self.dbm.database_is_disabled,
                            msg="But after that it should be independent from the settings")
        self.dbm._database_is_disabled = False  # Re-enable it at the end of the test

    def test_file_top_path(self):
        # Store settings
        check_before = self.s.configuration["project_check_enabled"]
        paths_before = list(self.s.configuration["project_paths"])
        disable_before = self.s.configuration["disable_database"]

        self.s.configuration["project_check_enabled"] = False
        self.assertIs(self.dbm.top_path(self.project_path + "/test"), None)

        self.s.configuration["project_check_enabled"] = True
        self.s.configuration["project_paths"] = [self.s.convert_path_to_abs_posix(getcwd())]
        self.s.configuration["disable_database"] = False  # Otherwise has the chance to override project_check_enabled..
        self.assertTrue(self.dbm.top_path(self.project_path + "/test") in self.project_path)

        # Put things back the way you found them
        self.s.configuration["project_check_enabled"] = check_before
        self.s.configuration["project_paths"] = paths_before
        self.s.configuration["disable_database"] = disable_before
