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



class HDF5Stub(ABC):

    def __init__(self, hdf, group_name):
        self._hdf = hdf
        self._group_name = group_name

    @abstractmethod
    def realize(self):
        pass

class SimpleStub(HDF5Stub):

    def realize(self):
        return self._hdf[self._group_name]

class ObjectStub(HDF5Stub):

    def realize(self):
        return self._hdf[self._group_name].to_object()

# exists to pass lazy=True, to make sure we can be recursively lazy!
class DataContainerStub(HDF5Stub):

    def realize(self):
        return self._hdf[self._group_name].to_object(lazy=True)

