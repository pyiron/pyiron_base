# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from pyiron_base.generic.factory import PyironFactory


class TestFactories(PyironTestCase):
    def test_pyiron_factory(self):
        factory = PyironFactory()
        factory.foo = "foo"
        self.assertEqual(factory.foo, factory['foo'])


if __name__ == "__main__":
    unittest.main()
