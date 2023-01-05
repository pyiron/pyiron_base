# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Create hash for a given dictionary
"""

import hashlib
import json
import numpy as np

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


def digest(h, sort_keys=True):
    return hashlib.md5(json.dumps(h, sort_keys=sort_keys).encode("utf-8")).hexdigest()


def get_hash(h, sort_keys=True):
    if hasattr(h, "items"):
        return digest({k: get_hash(v) for k, v in h.items()}, sort_keys=sort_keys)
    elif isinstance(h, list) or isinstance(h, np.ndarray):
        if isinstance(h, np.ndarray):
            h = h.tolist()
        try:
            return digest(h)
        except TypeError:
            return digest([get_hash(hh) for hh in h])
    else:
        try:
            return digest(h)
        except TypeError as e:
            hdf = PseudoHDF()
            if hasattr(h, "to_hdf"):
                h.to_hdf(hdf)
                return get_hash(hdf)
            raise TypeError(e)


class PseudoHDF(dict):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def open(self, group_name):
        self[group_name] = PseudoHDF()
        return self[group_name]
