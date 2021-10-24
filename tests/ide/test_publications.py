# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from unittest import TestCase
from pyiron_base.ide.publications import publications


class TestPublications(TestCase):
    def setUp(self) -> None:
        publications.reset()

    def test_list(self):
        self.assertEqual(1, len(publications.list()), msg="Should only contain default pyiron publication")

    def test_show(self):
        self.assertEqual(1, len(publications.show()), msg="Should only contain default pyiron publication")

    def test_add(self):
        publications.add(publications.pyiron_publication)
        self.assertEqual(1, len(publications.list()), msg="Existing publication duplicated")
        new_pub = dict(publications.pyiron_publication)
        new_pub['a-new-name'] = dict(new_pub['pyiron'])
        new_pub.pop('pyiron')
        publications.add(new_pub)
        self.assertEqual(2, len(publications.list()), msg="New publication failed to add")

