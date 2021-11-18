# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base._tests import TestWithProject
from pyiron_base import state
import os


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

        try:
            with self.subTest('disable project_check_enabled'):
                self.s.configuration["project_check_enabled"] = False
                self.assertIs(self.dbm.top_path(self.project_path + "/test"), None)

            with self.subTest('enable project_check_enabled'):
                new_root_path = self.s.convert_path_to_abs_posix(os.getcwd())
                self.s.configuration["project_check_enabled"] = True
                self.s.configuration["project_paths"] = [new_root_path]
                # Otherwise has the chance to override project_check_enabled... Thus:
                self.s.configuration["disable_database"] = False
                self.assertTrue(self.dbm.top_path(self.project_path + "/test") in self.project_path)

            with self.subTest("test Project.root_path and Project.project_path for a new sub-Project"):
                sub_pr = self.project.open('sub_project')
                self.assertEqual(sub_pr.root_path, new_root_path + '/')
                self.assertEqual(sub_pr.project_path,
                                 os.path.join(os.path.relpath(self.project_path, os.getcwd()),
                                              'sub_project').replace("\\", '/') + '/')
        finally:
            # Put things back the way you found them
            self.s.configuration["project_check_enabled"] = check_before
            self.s.configuration["project_paths"] = paths_before
            self.s.configuration["disable_database"] = disable_before

    def test_update_project_coupling(self):
        self.dbm.update()
        self.assertIs(
            self.dbm.database, self.project.db,
            msg="Expected the database access instance to stay coupled between state and project."
        )

