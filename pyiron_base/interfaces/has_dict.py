# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""Interface for classes to serialize to dictionary."""

from abc import ABC, abstractmethod

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2023, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Dec 20, 2023"


class HasDict(ABC):
    __dict_version__ = "0.1.0"

    @abstractmethod
    def from_dict(self, obj_dict: dict, version: str = None):
        pass

    @abstractmethod
    def to_dict(self):
        pass

    def _type_to_dict(self):
        type_dict = {
            "NAME": self.__class__.__name__,
            "TYPE": str(type(self)),
            "OBJECT": self.__class__.__name__,  # unused alias
            "DICT_VERSION": self.__dict_version__,
        }
        if hasattr(self, "__version__"):
            type_dict["VERSION"] = self.__version__
        return type_dict
