# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from os.path import abspath

from pyiron_base._tests import TestWithFilledProject

from pyiron_base.database.filetable import FileTable


class TestFileTable(TestWithFilledProject):

    def test_re_instantiation(self):
        pr = self.project
        sub_pr = self.project.open(self.project.list_groups()[0])

        ft0 = FileTable(project=pr.path)
        self.assertEqual(
            ft0._project,
            abspath(pr.path),
            msg="Path should be collected on instantiation"
        )

        ft1 = FileTable(project=sub_pr.path)
        self.assertEqual(
            ft1._project,
            abspath(sub_pr.path),
            msg="Path should be collected on instantiation"
        )
        self.assertNotEqual(
            ft0._project,
            ft1._project,
            msg="Separate instances should get their own paths"
        )
