# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base.storage.datacontainer import DataContainer
import os
import unittest


class TestSum(unittest.TestCase):
    def test_sub_group(self):
        hdf_1 = DataContainer()
        hdf_1.create_group('generic')
        hdf_1.generic['a'] = 0
        hdf_2 = DataContainer()
        hdf_2['a'] = 0
        self.assertEqual(
            hdf_1['generic'].get_hash(),
            hdf_2.get_hash(),
            msg='get_hash must be the same even in different levels as long as content is the same'
        )
        self.assertNotEqual(hdf_1.get_hash(), hdf_2.get_hash())

    def test_order(self):
        hdf_1 = DataContainer()
        hdf_1['a'] = 0
        hdf_1['b'] = 1
        hdf_2 = DataContainer()
        hdf_2['b'] = 1
        hdf_2['a'] = 0
        self.assertEqual(
            hdf_1.get_hash(sort_keys=True),
            hdf_2.get_hash(sort_keys=True),
            msg='Order must not matter'
        )
        self.assertNotEqual(
            hdf_1.get_hash(sort_keys=False),
            hdf_2.get_hash(sort_keys=False),
            msg='Order must matter'
        )


if __name__ == '__main__':
    unittest.main()
