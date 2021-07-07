"""
Convenience class to lazily read values from HDF.
"""

# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from abc import ABC, abstractmethod

__author__ = "Marvin Poul"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Marvin Poul"
__email__ = "poul@mpie.de"
__status__ = "production"
__date__ = "Apr 26, 2021"



class HDF5Stub:

    _realize_functions = {}

    def __init__(self, hdf, group_name):
        self._hdf = hdf
        self._group_name = group_name

    @classmethod
    def register(cls, type_name, realize):
        cls._realize_functions[type_name] = realize

    def realize(self):
        if self._group_name in self._hdf.list_nodes():
            return self._hdf[self._group_name]
        realize = self._realize_functions.get(
                self._hdf[self._group_name]['NAME'],
                lambda h, g: h[g].to_object()
        )
        return realize(self._hdf, self._group_name)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._hdf}, {self._group_name})"
