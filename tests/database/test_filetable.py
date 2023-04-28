# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from os import mkdir, rmdir
from os.path import abspath, dirname, join
from time import time

from pyiron_base._tests import PyironTestCase

from pyiron_base.database.filetable import FileTable


class TestFileTable(PyironTestCase):
    # Note: At time of writing, there are additional tests under the unintuitively-named
    #       sibling file `test_database_file.py`. These are ancient magic and I'm not
    #       touching them right now. -Liam Huber

    def test_re_initialization(self):
        here = dirname(abspath(__file__))
        loc1 = join(here, "ft_test_loc1")
        loc2 = join(here, "ft_test_loc2")
        mkdir(loc1)
        mkdir(loc2)

        start = time()
        ft = FileTable(loc1)
        first_initialization = time() - start

        start = time()
        ft_reinitialized = FileTable(loc1)
        second_initialization = time() - start

        print([f"{t:.2e}" for t in [
            # master_init, m2,
            first_initialization, second_initialization
        ]])

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

        another_ft = FileTable(loc2)
        self.assertFalse(
            ft is another_ft,
            msg="New paths should create new FileTable instances"
        )

        rmdir(loc1)
        rmdir(loc2)
