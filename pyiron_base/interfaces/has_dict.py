# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""Interface for classes to serialize to dictionary.

This also contains classes to ease the transition from HDF based storage to
dict based serialization.  Roughly we want to proceed as follows:
    1. Any new object directly implements :class:`.HasDict` and derives from
    :class:`.HasHDFfromDict` to be compatible to older code that still relies
    on the HDF interface.
    2. Any old object that doesn't yet directly implements :class:`.HasDict`
    can trivially derive from :class:`.HasDictfromHDF` to be compatible to
    newer code that uses the dict based serialization.
    3. Step by step we can transition old objects to directly implement
    :class:`.HasDict`.
"""

from abc import ABC, abstractmethod
from typing import Any

from pyiron_base.interfaces.has_hdf import HasHDF
from pyiron_base.storage.hdfio import (
    DummyHDFio,
    _extract_module_class_name,
    _import_class,
)

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


def create_from_dict(obj_dict):
    """
    Create and restores an object previously written as a dictionary.

    Args:
        obj_dict (dict): must be the output of HasDict.to_dict()

    Returns:
        object: restored object
    """
    if "TYPE" not in obj_dict:
        raise ValueError(
            "invalid obj_dict! must contain type information and be the output of HasDict.to_dict!"
        )
    type_field = obj_dict["TYPE"]
    module_path, class_name = _extract_module_class_name(type_field)
    class_object = _import_class(module_path, class_name)
    version = obj_dict.get("VERSION", None)
    obj = class_object.instantiate(obj_dict, version)
    obj.from_dict(obj_dict, version)
    return obj


class HasDict(ABC):
    __dict_version__ = "0.1.0"

    @classmethod
    def instantiate(cls, obj_dict: dict, version: str = None) -> "Self":
        return cls()

    def from_dict(self, obj_dict: dict, version: str = None):
        def load(inner_dict):
            if not isinstance(inner_dict, dict):
                return inner_dict
            if not all(
                k in inner_dict for k in ("NAME", "TYPE", "OBJECT", "DICT_VERSION")
            ):
                return {k: load(v) for k, v in inner_dict.items()}
            return create_from_dict(inner_dict)

        self._from_dict({k: load(v) for k, v in obj_dict.items()}, version)

    @abstractmethod
    def _from_dict(self, obj_dict: dict, version: str = None):
        pass

    @abstractmethod
    def _to_dict(self):
        pass

    def _type_to_dict(self):
        # Needed for the HasDictfromHDF/HasHDFfromDict classes.  When an object
        # derives from from both them and HasHDF/HasDict it will generally need
        # HDF_VERSION and DICT_VERSION defined for the version checking inside
        # from_dict/from_hdf to work properly.  So the code below tries to
        # escalate to super in case this is the case and falls back to {} if it
        # is not
        try:
            type_dict = super()._type_to_dict()
        except AttributeError:
            type_dict = {}
        type_dict |= {
            "NAME": self.__class__.__name__,
            "TYPE": str(type(self)),
            "OBJECT": self.__class__.__name__,  # unused alias
            "DICT_VERSION": self.__dict_version__,
        }
        if hasattr(self, "__version__"):
            type_dict["VERSION"] = self.__version__
        return type_dict

    def to_dict(self):
        type_dict = self._type_to_dict()
        data_dict = {}
        child_dict = {}
        for k, v in self._to_dict().items():
            if isinstance(v, HasDict):
                child_dict[k] = v.to_dict()
            elif isinstance(v, HasHDF):
                child_dict[k] = HasDictfromHDF.to_dict(v)
            else:
                data_dict[k] = v
        return data_dict | self._join_children_dict(child_dict) | type_dict

    @staticmethod
    def _join_children_dict(children: dict[str, dict[str, Any]]) -> dict[str, Any]:
        """
        Given a nested dictionary, flatten the first level.

        >>> d = {'a': {'a1': 3}, 'b': {'b1': 4, 'b2': {'c': 42}}}
        >>> _join_children_dict(d)
        {'a/a1': 3, 'b/b1': 4, 'b/b2': {'c': 42}}

        This is intended as a utility function for nested HasDict objects, that
        to_dict their children and then want to give a flattened dict for
        writing to ProjectHDFio.write_dict_to_hdf
        """
        return {
            "/".join((k1, k2)): v2
            for k1, v1 in children.items()
            for k2, v2 in v2.items()
        }


class HasHDFfromDict(HasHDF, HasDict):
    """
    Implements HasHDF in terms of HasDict.

    This class is intended for "new-style" objects that are used in a context
    that only assumes that they implements HasHDF.

    Implementors may still override :meth:`.HasHDF._get_hdf_group_name`.
    """

    def _from_hdf(self, hdf, version=None):
        self.from_dict(hdf.read_dict_from_hdf(recursive=True))

    def _to_hdf(self, hdf):
        hdf.write_dict_to_hdf(self.to_dict())


class HasDictfromHDF(HasDict, HasHDF):
    """
    Implements HasDict in terms of HasHDF.

    This class is intended for "old-style" objects that should be able to be
    used as children for objects that already implement HasDict and expect
    their children to implmement it.
    """

    @classmethod
    def instantiate(cls, obj_dict: dict, version: str = None) -> "Self":
        hdf = DummyHDFio(None, "/", obj_dict)
        return cls(**cls.from_hdf_args(hdf))

    def _from_dict(self, obj_dict: dict, version: str = None):
        # DummyHDFio(project=None) looks a bit weird, but it was added there
        # only to support saving/loading jobs which already use the HasDict
        # interface
        group_name = self._get_hdf_group_name()
        if group_name is not None:
            hdf = DummyHDFio(None, "/", {group_name: obj_dict})
        else:
            hdf = DummyHDFio(None, "/", obj_dict)
        self.from_hdf(hdf)

    def _to_dict(self):
        hdf = DummyHDFio(None, "/")
        self.to_hdf(hdf)
        group_name = self._get_hdf_group_name()
        data = hdf.to_dict()
        if group_name is not None:
            return data[group_name]
        else:
            return data
