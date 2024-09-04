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
from collections import defaultdict
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
    version = obj_dict.get("DICT_VERSION", None)
    obj = class_object.instantiate(obj_dict, version)
    obj.from_dict(obj_dict, version)
    return obj


def _join_children_dict(children: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """
    Given a nested dictionary, flatten the first level.

    >>> d = {'a': {'a1': 3}, 'b': {'b1': 4, 'b2': {'c': 42}}}
    >>> _join_children_dict(d)
    {'a/a1': 3, 'b/b1': 4, 'b/b2': {'c': 42}}

    This is intended as a utility function for nested HasDict objects, that
    to_dict their children and then want to give a flattened dict for
    writing to ProjectHDFio.write_dict_to_hdf.

    See also :func:`._split_children_dict`.
    """
    return {
        "/".join((k1, k2)): v2 for k1, v1 in children.items() for k2, v2 in v1.items()
    }


def _split_children_dict(obj_dict: dict[str, Any]) -> dict[str, Any | dict[str, Any]]:
    """
    Undoes _join_children_dict.

    Classes that use :func:`._join_children_dict` in their `_to_dict`, must
    call this function in their `_from_dict`.
    """
    subs = defaultdict(dict)
    plain = {}
    for k, v in obj_dict.items():
        if "/" not in k:
            plain[k] = v
            continue
        root, k = k.split("/", maxsplit=1)
        subs[root][k] = v
    # using update keeps type stability, i.e. we always return a plain dict
    plain.update(subs)
    return plain


def _from_dict_children(obj_dict: dict) -> dict:
    """
    Recurse through `obj_dict` and restore any objects with :class:`~.HasDict`.

    Args:
        obj_dict (dict): data previously returned from :meth:`.to_dict`
    """

    def load(inner_dict):
        # object is a not a dict, so nothing to do
        if not isinstance(inner_dict, dict):
            return inner_dict
        # if object is a dict but doesn't have type information, recurse through it to load any sub dicts that might
        if not all(k in inner_dict for k in ("NAME", "TYPE", "OBJECT", "DICT_VERSION")):
            return {k: load(v) for k, v in inner_dict.items()}
        # object has type info, so just load it
        return create_from_dict(inner_dict)

    return {k: load(v) for k, v in obj_dict.items()}


def _to_dict_children(obj_dict: dict) -> dict:
    """
    Call to_dict on any objects in the values that support it.

    Intended as a helper function for recursives object that want to to_dict
    their nested objects automatically.  It uses :func:`._join_children_dict`
    for any dictionaries returned from the children.

    The function only goes through the *first* layer of dictionary values and
    does *not* recurse through nested dictionaries.

    Args:
        obj_dict (dict): data previously returned from :meth:`._to_dict`

    Returns:
        obj_dict (dict): new dictionary with the obj_dict of the children
    """
    data_dict = {}
    child_dict = {}
    for k, v in obj_dict.items():
        if isinstance(v, HasDict):
            child_dict[k] = v.to_dict()
        elif isinstance(v, HasHDF):
            child_dict[k] = HasDictfromHDF.to_dict(v)
        else:
            data_dict[k] = v
    return data_dict | _join_children_dict(child_dict)


class HasDict(ABC):
    """
    Abstract interface to convert objects to dictionaries for storage.

    Subclasses must to implement :meth:`~._from_dict` and :meth:`~._to_dict` and may implement :meth:`.instantiate`.
    :meth:`._to_dict` is excepted to return a `dict` mapping string names to the values the object needs serialized.

    On recreating an object from scratch with :func:`.create_from_dict` first :meth:`.instantiate` is called and then
    :meth:`.from_dict` with the same `obj_dict`, i.e. it is roughly equivalent to

    >>> my_dict = dict(...)
    >>> my_object = MyType.instantiate(my_dict)
    >>> my_object.from_dict(my_dict)

    Implementations should make sure that calling `to_dict` after `from_dict` returns an equivalent dictionary even when
    the object was not obtained from :meth:`.instantiate`, such that

    >>> my_dict = dict(...)
    >>> my_object = MyType(...)
    >>> my_object.from_dict(my_dict)
    >>> my_object.to_dict() == my_dict
    True
    """

    __dict_version__ = "0.1.0"
    """A version string saved together with data returned from :meth:`._to_dict` and is passed back into
    :meth:`._from_dict`.  Implementations can use this change their representation and still read older data."""

    @classmethod
    def instantiate(cls, obj_dict: dict, version: str = None) -> "Self":
        """
        Create a blank instance of this class.

        This can be used when some values are already necessary for the objects `__init__`.

        Args:
            obj_dict (dict): data previously returned from :meth:`.to_dict`
            version (str): version tag written together with the data

        Returns:
            object: a blank instance of the object that is sufficiently initialized to call :meth:`._from_dict` on it
        """
        return cls()

    def from_dict(self, obj_dict: dict, version: str = None):
        """
        Populate the object from the serialized object.

        Args:
            obj_dict (dict): data previously returned from :meth:`.to_dict`
            version (str): version tag written together with the data
        """

        obj_dict = _split_children_dict(obj_dict)
        if version is None:
            version = obj_dict.get("DICT_VERSION", None)
        self._from_dict(obj_dict, version)

    @abstractmethod
    def _from_dict(self, obj_dict: dict, version: str = None):
        """
        Populate the object from the serialized object.

        Implementations must use :func:`._from_dict_children` if they use
        `._to_dict_children` in their implementation of :meth:`._to_dict`.

        Args:
            obj_dict (dict): data previously returned from :meth:`.to_dict`
            version (str): version tag written together with the data
        """
        pass

    def to_dict(self) -> dict:
        """
        Reduce the object to a dictionary.

        Returns:
            dict: serialized state of this object
        """
        type_dict = self._type_to_dict()
        return self._to_dict() | type_dict

    @abstractmethod
    def _to_dict(self) -> dict:
        """
        Reduce the object to a dictionary.

        Implementations may use :func:`._to_dict_children`, if they
        automatically want to call `to_dict` on any objects possible from their
        returned dictionary.

        Returns:
            dict: serialized state of this object
        """
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
