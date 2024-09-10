# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Wouldn't it be nice to automate your bibliography?
Well we can't, but that's the idea behind this alpha-stage feature.
The idea is that various pyiron submodules and objects will register their relevant publications and you can just ask
your project for a list of everything you should cite.
`Publications` is the way we work towards this goal.
"""

from typing import Dict, List, Union

import pandas
from pyiron_snippets.singleton import Singleton
from typing_extensions import Literal

__author__ = "Joerg Neugebauer, Jan Janssen"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


class Publications(metaclass=Singleton):
    def __init__(self):
        self._publications = {}
        self.add(self.pyiron_publication)

    @property
    def pyiron_publication(self) -> Dict:
        return {
            "pyiron": {
                "pyiron-paper": {
                    "author": [
                        "Jan Janssen",
                        "Sudarsan Surendralal",
                        "Yury Lysogorskiy",
                        "Mira Todorova",
                        "Tilmann Hickel",
                        "Ralf Drautz",
                        "Jörg Neugebauer",
                    ],
                    "title": "pyiron: An integrated development environment for computational "
                    "materials science",
                    "journal": "Computational Materials Science",
                    "volume": "161",
                    "pages": "24 - 36",
                    "issn": "0927-0256",
                    "doi": "https://doi.org/10.1016/j.commatsci.2018.07.043",
                    "url": "http://www.sciencedirect.com/science/article/pii/S0927025618304786",
                    "year": "2019",
                }
            }
        }

    def list(self) -> List[Dict]:
        """
        List of publications currently in use.

        Returns:
            list: list of publications
        """
        all_publications = []
        for v in self._publications.values():
            if isinstance(v, list):
                all_publications += v
            else:
                all_publications.append(v)
        return all_publications

    def add(self, pub_dict: Dict) -> None:
        """
        Add a publication to the list of publications.

        Args:
            pub_dict (dict): The key should be the name of the code used and the value a list of publications to cite.
        """
        for key, value in pub_dict.items():
            if key not in self._publications.keys():
                self._publications[key] = value

    def show(
        self, bib_format: Literal["pandas", "dict", "bibtex", "apa"] = "pandas"
    ) -> Union[Dict, pandas.DataFrame, str]:
        """
        List the publications used in this project.

        Args:
            bib_format ('pandas'|'dict'|'bibtex'|'apa'): Which format to use. Pandas (dataframe) and dict return the
                corresponding python object, while bibtex and apa give formatted strings.

        Returns:
            pandas.DataFrame|dict|str: Publication data.
        """

        def get_bibtex(k: str, v: str) -> str:
            total_keys = [
                "title",
                "journal",
                "volume",
                "issue",
                "number",
                "pages",
                "numpages",
                "year",
                "month",
                "publisher",
                "url",
                "doi",
                "issn",
            ]
            bibtex_str = (
                "@article{"
                + k
                + ",\n"
                + "    author={"
                + " and ".join(v["author"])
                + "},\n"
            )
            for kt in total_keys:
                if kt in value.keys():
                    bibtex_str += "    " + kt + "={" + v[kt] + "},\n"
            bibtex_str += "}\n"
            return bibtex_str

        def get_apa(v: dict) -> str:
            apa_str = " & ".join(v["author"])
            if "year" in v.keys():
                apa_str += " (" + v["year"] + "). "
            if "title" in v.keys():
                apa_str += v["title"] + ". "
            if "journal" in v.keys():
                apa_str += v["journal"] + ", "
            if "volume" in v.keys():
                apa_str += v["volume"] + ", "
            if "pages" in v.keys():
                apa_str += v["pages"] + ". "
            if "doi" in v.keys():
                apa_str += "doi: " + v["doi"] + "\n"
            return apa_str

        publication_dict = self.list()
        if bib_format.lower() == "dict":
            return self._publications
        elif bib_format.lower() == "pandas":
            publication_lst = []
            for p in publication_dict:
                for v in p.values():
                    publication_lst.append(v)
            return pandas.DataFrame(publication_lst)
        elif bib_format.lower() == "bibtex":
            total_str = ""
            for pub in publication_dict:
                for key, value in pub.items():
                    total_str += get_bibtex(k=key, v=value)
            return total_str
        elif bib_format.lower() == "apa":
            total_str = ""
            for pub in publication_dict:
                for key, value in pub.items():
                    total_str += get_apa(v=value)
            return total_str
        else:
            raise ValueError("Supported Bibformats are ['dict', 'bibtex', 'apa']")

    def reset(self) -> None:
        """Clean the publication list back to the default pyiron publication."""
        self.__init__()


publications = Publications()
