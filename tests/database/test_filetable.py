# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from inspect import getfile
from os.path import abspath, dirname
from time import time

from pyiron_base._tests import PyironTestCase

from pyiron_base.database.filetable import FileTable


class TestFileTable(PyironTestCase):
    # Note: At time of writing, there are additional tests under the unintuitively-named
    #       sibling file `test_database_file.py`. These are ancient magic and I'm not
    #       touching them right now. -Liam Huber

    def test_re_initialization(self):
        here = dirname(abspath(__file__))
        there = dirname(getfile(FileTable))
        assert(here != there)  # Sanity check for the rest of the test to be valid

        start = time()
        ft = FileTable(here)
        first_initialization = time() - start

        start = time()
        ft_reinitialized = FileTable(here)
        second_initialization = time() - start

        self.assertTrue(
            ft is ft_reinitialized,
            msg="This is an implementation sanity check -- we are used a path-dependent"
                "singleton architecture, so reinitializations with the same path should"
                "be the same object"
        )
        self.assertTrue(
            second_initialization < 0.1 * first_initialization,
            msg=f"We promise re-initialization to be much faster, but got times "
                f"{first_initialization:.2e} for the first and "
                f"{second_initialization:.2e} for the second"
        )

        another_ft = FileTable(there)
        self.assertFalse(
            ft is another_ft,
            msg="New paths should create new FileTable instances"
        )

