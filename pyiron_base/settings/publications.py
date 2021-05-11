# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import pandas

from pyiron_base.settings.generic import Settings

__author__ = "Joerg Neugebauer, Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"

s = Settings()


def list_publications(bib_format="pandas", category=0):
    """
    List the publications used in this project.

    Args:
        bib_format (str): ['pandas', 'dict', 'bibtex', 'apa']
        category (int): priority category

    Returns:
        pandas.DataFrame/ list: list of publications in chosen format.
    """

    def get_bibtex(k, v):
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

    def get_apa(v):
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

    category_name = {0: 'core', 1: 'general', 2: 'dependencies'}

    publication_dict = {}
    # Filter full publication dict:
    for key in [key for key in s.publication_dict.keys() if (key <= category or category < 0)]:
        publication_dict[category_name[key]] = s.publication_dict[key]

    # Convert dict into list with relevance stored as key inside its items
    publication_lst = []
    for cat in publication_dict.copy():
        for publication in publication_dict[cat]:
            pub = publication
            if category != 0:
                for pub_ in pub:
                    pub[pub_]["Relevance"] = cat
            publication_lst.append(pub)

    if bib_format.lower() == "dict":
        return publication_dict
    elif bib_format.lower() == "pandas":
        pub_list = []
        for pub_item in publication_lst:
            for p in pub_item.values():
                pub_list.append(p)
        df = pandas.DataFrame(pub_list)
        if category != 0:
            return df.set_index("Relevance", append=True)
        else:
            return df
    elif bib_format.lower() == "bibtex":
        total_str = ""
        for pub in publication_lst:
            for key, value in pub.items():
                total_str += get_bibtex(k=key, v=value)
        return total_str
    elif bib_format.lower() == "apa":
        total_str = ""
        for pub in publication_lst:
            for key, value in pub.items():
                total_str += get_apa(v=value)
        return total_str
    else:
        raise ValueError("Supported Bibformats are ['dict', 'pandas', 'bibtex', 'apa']")
