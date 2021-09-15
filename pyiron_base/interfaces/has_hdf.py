# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""Interface for classes to serialize to HDF5."""

from abc import ABC, abstractmethod

__author__ = "Marvin Poul"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Marvin Poul"
__email__ = "poul@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2021"

class _WithHDF:
    __slots__ = ("_hdf", "_group_name")

    def __init__(self, hdf, group_name=None):
        self._hdf = hdf
        self._group_name = group_name

    def __enter__(self):
        if self._group_name is not None:
            self._hdf = self._hdf.open(self._group_name)

        return self._hdf

    def __exit__(self, *args):
        if self._group_name is not None:
            self._hdf.close()

class HasHDF(ABC):

    __hdf_version__ = "0.1.0"

    @abstractmethod
    def _from_hdf(self, hdf, version=None):
        pass

    @abstractmethod
    def _to_hdf(self, hdf):
        pass

    @abstractmethod
    def _get_hdf_group_name(self):
        pass

    @classmethod
    def from_hdf_args(cls, hdf):
        """
        Read arguments for instance creation from HDF5 file.

        Args:
            hdf (ProjectHDFio): HDF5 group object

        Returns:
            dict: arguments that can be **kwarg-passed to cls().
        """
        return {}

    def _type_to_hdf(self, hdf):
        hdf["NAME"] = self.__class__.__name__
        hdf["TYPE"] = str(type(self))
        hdf["OBJECT"] = hdf["NAME"] # unused alias
        if hasattr(self, "__version__"):
            hdf["VERSION"] = self.__version__
        hdf["HDF_VERSION"] = self.__hdf_version__

    def from_hdf(self, hdf, group_name=None):
        group_name = group_name or self._get_hdf_group_name()
        with _WithHDF(hdf, group_name) as hdf:
            version = hdf.get("HDF_VERSION", "0.1.0")
            self._from_hdf(hdf, version=version)

    def to_hdf(self, hdf, group_name=None):
        group_name = group_name or self._get_hdf_group_name()
        with _WithHDF(hdf, group_name) as hdf:
            if len(hdf.list_dirs()) > 0 and group_name is None:
                raise ValueError("HDF group must be empty when group_name is not set.")
            self._to_hdf(hdf)
            self._type_to_hdf(hdf)

    def rewrite_hdf(self, hdf, group_name=None):
        with _WithHDF(hdf, group_name) as hdf:
            obj = hdf.to_object()
            hdf.remove_group()
            obj.to_hdf(hdf)
