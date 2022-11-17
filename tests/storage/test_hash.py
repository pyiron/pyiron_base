# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base import FileHDFio
import os
import unittest


class TestSum(unittest.TestCase):
    def test_sub_group(self):
        hdf_1 = FileHDFio(os.path.abspath("test_1"))
        hdf_1.create_group('generic')
        with hdf_1.open('generic') as hdf_generic:
            hdf_generic['a'] = 0
        hdf_2 = FileHDFio(os.path.abspath("test_2"))
        hdf_2['a'] = 0
        self.assertEqual(
            hdf_1['generic'].hexdigest(),
            hdf_2.hexdigest(),
            msg='hexdigest must be the same even in different levels as long as content is the same'
        )
        self.assertNotEqual(hdf_1.hexdigest(), hdf_2.hexdigest())
        hdf_1.remove_file()
        hdf_2.remove_file()

    def test_order(self):
        hdf_1 = FileHDFio(os.path.abspath("test_1"))
        hdf_1['a'] = 0
        hdf_1['b'] = 1
        hdf_2 = FileHDFio(os.path.abspath("test_2"))
        hdf_2['b'] = 1
        hdf_2['a'] = 0
        self.assertEqual(
            hdf_1.hexdigest(),
            hdf_2.hexdigest(),
            msg='Order must not matter'
        )
        hdf_1.remove_file()
        hdf_2.remove_file()


if __name__ == '__main__':
    unittest.main()
