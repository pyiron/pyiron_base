# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from unittest import TestCase
from pyiron_base.state.publications import publications
import pandas as pd

class TestPublicationsExt(TestCase):
    def setUp(self):
        publications.reset()
        self.publication_with_all_fields = {
            "my_code": {
                "my_pub": {
                    "author": ["Doe, John", "Doe, Jane"],
                    "title": "A fancy title",
                    "journal": "Journal of Fancy Things",
                    "volume": "1",
                    "issue": "2",
                    "number": "3",
                    "pages": "123-456",
                    "numpages": "333",
                    "year": "2024",
                    "month": "July",
                    "publisher": "Fancy Pants Publishing",
                    "url": "http://fancy.url",
                    "doi": "10.fancy/doi",
                    "issn": "1234-5678",
                }
            }
        }

    def test_show_dict(self):
        self.assertIsInstance(publications.show(bib_format="dict"), dict)

    def test_show_bibtex(self):
        publications.add(self.publication_with_all_fields)
        bibtex_str = publications.show(bib_format="bibtex")
        self.assertIsInstance(bibtex_str, str)
        self.assertIn("@article{my_pub,", bibtex_str)
        self.assertIn("author={Doe, John and Doe, Jane}", bibtex_str)
        self.assertIn("title={A fancy title}", bibtex_str)
        self.assertIn("journal={Journal of Fancy Things}", bibtex_str)
        self.assertIn("volume={1}", bibtex_str)
        self.assertIn("pages={123-456}", bibtex_str)
        self.assertIn("year={2024}", bibtex_str)
        self.assertIn("doi={10.fancy/doi}", bibtex_str)
        self.assertIn("url={http://fancy.url}", bibtex_str)
        self.assertIn("issn={1234-5678}", bibtex_str)
        self.assertIn("issue={2}", bibtex_str)
        self.assertIn("number={3}", bibtex_str)
        self.assertIn("numpages={333}", bibtex_str)
        self.assertIn("month={July}", bibtex_str)
        self.assertIn("publisher={Fancy Pants Publishing}", bibtex_str)

    def test_show_apa(self):
        publications.add(self.publication_with_all_fields)
        apa_str = publications.show(bib_format="apa")
        self.assertIsInstance(apa_str, str)
        self.assertIn("Doe, John & Doe, Jane (2024). A fancy title. Journal of Fancy Things, 1, 123-456. doi: 10.fancy/doi", apa_str)

    def test_show_invalid_format(self):
        with self.assertRaises(ValueError):
            publications.show(bib_format="invalid_format")

    def test_list_with_list_of_pubs(self):
        list_pub = {
            "another_code": [
                {"pub1": {"title": "pub1", "author": ["Ano Nymous"]}},
                {"pub2": {"title": "pub2", "author": ["Ano Nymous"]}}
            ]
        }
        publications.add(list_pub)
        # 1 from default pyiron + 2 from list_pub
        self.assertEqual(len(publications.show(bib_format="pandas")), 3)

    def test_reset(self):
        publications.add({"some_code": {"some_pub": {"title": "A"}}})
        self.assertEqual(len(publications.list()), 2)
        publications.reset()
        self.assertEqual(len(publications.list()), 1)
        self.assertEqual(publications.list(), [publications.pyiron_publication['pyiron']])

    def test_show_pandas(self):
        df = publications.show(bib_format="pandas")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)

    def test_apa_without_all_fields(self):
        publications.add({"some_code": {"some_pub": {"author": ["A", "B"], "title": "T"}}})
        apa_str = publications.show(bib_format="apa")
        self.assertIn("A & BT. ", apa_str)
