"""
Data structure for versatile data handling.
"""

# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import copy
import json
import warnings
from collections.abc import Sequence, Set, Mapping, MutableMapping

import numpy as np

from pyiron_base.interfaces.has_groups import HasGroups
from .fileio import read, write

__author__ = "Marvin Poul"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.1"
__maintainer__ = "Marvin Poul"
__email__ = "poul@mpie.de"
__status__ = "production"
__date__ = "Jun 17, 2020"


_internal_hdf_nodes = [
        "NAME",
        "TYPE",
        "OBJECT",
        "VERSION",
        "HDF_VERSION",
        "READ_ONLY"
]

def _normalize(key):
    if isinstance(key, str):
        if key.isdecimal():
            return int(key)
        elif "/" in key:
            return tuple(key.split("/"))

    elif isinstance(key, tuple) and len(key) == 1:
        return _normalize(key[0])

    return key


class DataContainer(MutableMapping, HasGroups):
    """
    Mutable sequence with optional keys.

    If no argument is given, the constructor creates a new empty DataContainer.  If
    specified init maybe a Sequence, Set or Mapping and all recursive
    occurrences of these are also wrapped by DataContainer.

    >>> pl = DataContainer([3, 2, 1, 0])
    >>> pm = DataContainer({"foo": 24, "bar": 42})

    Access can be like a normal list with integers or optionally with strings
    as keys.

    >>> pl[0]
    3
    >>> pl[2]
    1
    >>> pm["foo"]
    24

    Keys do not have to be present for all elements.

    >>> pl2 = DataContainer([1,2])
    >>> pl2["end"] = 3
    >>> pl2
    DataContainer({0: 1, 1: 2, 'end': 3})

    It is also allowed to set an item one past the length of the DataContainer,
    this is then equivalent to appending that element.  This allows to use the
    update method also with other DataContainers

    >>> pl[len(pl)] = -1
    >>> pl
    DataContainer([3, 2, 1, 0, -1])
    >>> pl.pop(-1)
    -1

    Where strings are used they may also be used as attributes.  Getting keys
    which clash with methods of DataContainer must be done with item access, but
    setting them works without overwriting the instance methods, but is not
    recommended for readability.

    >>> pm.foo
    24
    >>> pm.tail = 23
    >>> pm
    DataContainer({'foo': 24, 'bar': 42, 'tail': 23})

    Keys and indices can be tuples to traverse nested DataContainers.

    >>> pn = DataContainer({"foo": {"bar": [4, 2]}})
    >>> pn["foo", "bar"]
    DataContainer([4, 2])
    >>> pn["foo", "bar", 0]
    4

    Using keys with "/" in them is equivalent to the above after splitting the
    key.

    >>> pn["foo/bar"] == pn["foo", "bar"]
    True
    >>> pn["foo/bar/0"] == pn["foo", "bar", 0]
    True

    To make that work strings that are only decimal digits are automatically
    converted to integers before accessing the list and keys are restricted to
    not only contain digits on initialization.

    >>> pl["0"] == pl[0]
    True
    >>> DataContainer({1: 42})
    Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "datacontainer.py", line 126, in __init__
            raise ValueError(
    ValueError: keys in initializer must not be int or str of decimal digits or in correct order, is 1

    When initializing from a dict, it may not have integers or decimal strings
    as keys unless they match their position in the insertion order.  This is
    to avoid ambiguities in the final order of the DataContainer.

    >>> DataContainer({0: "foo", 1: "bar", 2: 42})
    DataContainer(['foo', 'bar', 42])
    >>> DataContainer({0: "foo", 2: 42, 1: "bar"})
    Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "datacontainer.py", line 132, in __init__
            raise ValueError(
    ValueError: keys in initializer must not be int or str of decimal digits or in correct order, is 2


    Using keys is completely optional, DataContainer can always be treated as a
    list, with the exception that `iter()` iterates of the keys and indices.
    This is to correctly implement the MutableMapping protocol, to convert to a
    normal list and discard the keys use `values()`.

    >>> pm[0]
    24
    >>> pn["0/0/1"]
    2
    >>> list(pl)
    [0, 1, 2, 3]
    >>> list(pl.values())
    [3, 2, 1, 0]
    >>> list(pl.keys())
    [0, 1, 2, 3]


    Implements :class:`.HasGroups`.  Groups are nested data containers and nodes are everything else.

    >>> p = DataContainer({"a": 42, "b": [0, 1, 2]})
    >>> p.list_groups()
    ['b']
    >>> p.list_nodes()
    ['a']

    .. attention:: Subclasses beware!

        DataContainer require some careful treatment when creating subclasses.

        1. Since DataContainers are expected to recursively instantiate themselves subclasses need to keep their
        `__init__ compatible to the base class.  That means being able to be instantiated without arguments, if
        arguments are given the first one (or `init`) has to accept a Mapping or Iterable.  Additional arguments may be
        added, but must be after `init` and must have a default.

        2. Creating new instance attributes that don't live in the container itself is possible, but you need to use
        `object.__setattr__` the first time you define that attribute.  Afterwards using normal assignment syntax works.

        3. Subclasses should always be thought of as general data structures, if you want to subclass to have access to
        the HDF5 functionality or the way the DataContainer is shown in jupyter notebooks, but only have a fixed number
        of attributes it is better to create a new class that has an DataContainer as an attribute and dispatch to the
        :meth:`DataContainer.from_hdf`, :meth:`DataContainer.to_hdf` and :meth:`DataContainer._repr_json_`
        methods.


    A few examples for subclasses

    >>> class ExtendedContainer(DataContainer):
    ...     def __init__(self, init=None, my_fancy_field=42, table_name=None):
    ...         super().__init__(init=init, table_name=table_name)
    ...         object.__setattr__(self, "my_fancy_field", my_fancy_field)

    After defining it once like this you can access my_fancy_field as a normal attribute, but it will not be stored in
    the container itself and will not be stored in HDF5.

    >>> e = ExtendedContainer({'foo': 1, 'bar': 5}, my_fancy_field=23)
    >>> e.my_fancy_field
    23
    >>> e
    ExtendedContainer({'foo': 1, 'bar': 5})
    >>> e.my_fancy_field = 42
    >>> e.my_fancy_field
    42
    >>> e
    ExtendedContainer({'foo': 1, 'bar': 5})

    Or a class that uses a DataContainer for storage, but doesn't derive from it.

    >>> class FancyClass:
    ...     def __init__(self, foo):
    ...         self.storage = DataContainer()
    ...         self.storage.foo = foo
    ...
    ...     @property
    ...     def foo(self):
    ...         return self.storage.foo
    ...
    ...     @foo.setter
    ...     def foo(self, val):
    ...         self.storage.foo = val
    ...
    ...     def from_hdf(self, hdf, group_name):
    ...         self.storage.from_hdf(hdf=hdf, group_name=group_name)
    ...
    ...     def to_hdf(self, hdf, group_name):
    ...         self.storage.to_hdf(hdf=hdf, group_name=group_name)
    ...
    ...     def _repr_json_(self):
    ...         return self.storage._repr_json_()

    """

    __version__ = "0.1.0"
    __hdf_version__ = "0.2.0"

    def __new__(cls, *args, **kwargs):

        instance = super().__new__(cls)
        # setting these immediately after object creation ensures that they are
        # always defined and attribute access works even before __init__ is
        # called.  This is relevant on deepcopy & pickling.
        object.__setattr__(instance, "_store", [])
        object.__setattr__(instance, "_indices", {})
        object.__setattr__(instance, "table_name", None)
        object.__setattr__(instance, "_read_only", False)

        return instance

    def __init__(self, init=None, table_name=None):
        self.table_name = table_name
        if init is not None:
            self.update(init, wrap=True)

    def __len__(self):
        return len(self._store)

    def __iter__(self):

        reverse_indices = {i: k for k, i in self._indices.items()}

        for i in range(len(self)):
            yield reverse_indices.get(i, i)

    def __getitem__(self, key):

        key = _normalize(key)

        if isinstance(key, tuple):
            return self[key[0]][key[1:]]

        elif isinstance(key, int):
            try:
                return self._store[key]
            except IndexError:
                raise IndexError("list index out of range") from None

        elif isinstance(key, str):
            try:
                return self._store[self._indices[key]]
            except KeyError:
                raise KeyError(repr(key)) from None

        else:
            raise ValueError(
                    "{} is not a valid key, must be str or int".format(key)
            )

    def __setitem__(self, key, val):

        if self.read_only:
            self._read_only_error()

        key = _normalize(key)

        if isinstance(key, tuple):
            if key[0] not in self.keys():
                self[key[0]] = type(self)()
            self[key[0]][key[1:]] = val
        elif isinstance(key, int):
            if key < len(self):
                self._store[key] = val
            elif key == len(self):
                self.append(val)
            else:
                raise IndexError("index out of range")
        elif isinstance(key, str):
            if key not in self._indices:
                self._indices[key] = len(self._store)
                self._store.append(val)
            else:
                self._store[self._indices[key]] = val
        else:
            raise ValueError(
                    "{} is not a valid key, must be str or int".format(key)
            )

    def __delitem__(self, key):

        if self.read_only:
            self._read_only_error()

        key = _normalize(key)

        if isinstance(key, tuple):
            del self[key[0]][key[1:]]
        elif isinstance(key, (str, int)):
            if isinstance(key, str):
                idx = self._indices[key]
                del self._indices[key]
            else:
                idx = key

            del self._store[idx]

            for k, i in self._indices.items():
                if i > idx:
                    self._indices[k] = i - 1
        else:
            raise ValueError(
                    "{} is not a valid key, must be str or int".format(key)
            )

    def __getattr__(self, name):
        # this is only called when python doesn't find name in the instance
        # or class variables, so we don't need to go through the same lengths
        # here as in __setattr__
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    @classmethod
    def _is_class_var(cls, name):
        return any(name in c.__dict__ for c in cls.__mro__)

    def __setattr__(self, name, val):
        # Search instance variables (self.__dict___) and class variables
        # (self.__class__.__dict__ + iterating over mro to find variables on
        #  all ancestors) first before we assign the value into our container.
        # If we find name refers to a instance/class variable, we let
        # object.__setattr__ do all the work for us.
        if name in self.__dict__ or self._is_class_var(name):
            object.__setattr__(self, name, val)
        else:
            self[name] = val

    def __delattr__(self, name):
        # see __setattr__
        if name in self.__dict__ or self._is_class_var(name):
            object.__delattr__(self, name)
        else:
            del self[name]

    def __array__(self):
        """Return bare list of values to play nice with numpy."""
        return np.array(self._store)

    def __dir__(self):
        return set(super().__dir__() + list(self._indices.keys()))

    def __repr__(self):
        name = self.__class__.__name__
        if self.has_keys():
            return name + "({" + ", ".join("{!r}: {!r}".format(k, v) for k, v in self.items()) + "})"
        else:
            return name + "([" + ", ".join("{!r}".format(v) for v in self._store) + "])"

    @classmethod
    def _wrap_val(cls, val):
        if isinstance(val, (tuple, list, dict)):
            return cls(val)
        else:
            return val

    @property
    def read_only(self):
        """
        bool: if set, raise warning when attempts are made to modify the container
        """
        return self._read_only

    @read_only.setter
    def read_only(self, val):
        # can't mark a read-only list as writeable
        if self._read_only and not val:
            self._read_only_error()
        else:
            self._read_only = bool(val)

    @classmethod
    def _read_only_error(cls):
        warnings.warn(
            "The input in {} changed, while the state of the job was already "
            "finished.".format(cls.__name__)
        )

    def to_builtin(self, stringify=False):
        """
        Convert the container back to builtin dict's and list's recursively.

        Args:
            stringify (bool, optional): convert all non-recursive elements to str
        """

        if self.has_keys():
            dd = {}
            for k, v in self.items():
                # force all string keys in output to work with h5io (it
                # requires all string keys when storing as json), since
                # _normalize calls int() on all digit string keys this is
                # transparent for the rest of the module
                k = str(k)
                if isinstance(v, DataContainer):
                    dd[k] = v.to_builtin(stringify=stringify)
                else:
                    dd[k] = repr(v) if stringify else v

            return dd
        elif stringify:
            return list(v.to_builtin(stringify=stringify)
                        if isinstance(v, DataContainer) else repr(v)
                        for v in self.values())
        else:
            return list(v.to_builtin(stringify=stringify)
                        if isinstance(v, DataContainer) else v
                        for v in self.values())

    # allows "nice" displays in jupyter lab
    def _repr_json_(self):
        return self.to_builtin(stringify=True)

    # allows 'nice' display in notebooks
    def _repr_html_(self):
        name = self.__class__.__name__
        plain = f"{name}({json.dumps(self.to_builtin(stringify=True), indent=2, default=str)})"
        return "<pre>" + plain + "</pre>"

    def get(self, key, default=None, create=False):
        """
        If ``key`` exists, behave as generic, if not call create_group.

        Args:
            key (str):               key to search
            default (optional):      return this instead if nothing found
            create (bool, optional): create empty container at key if nothing found

        Raise:
            IndexError: if key is not in the container and neither ``default`` not
            ``create`` are given

        Returns:
            object: element at ``key`` or new empty subcontainer
        """
        if create and key not in self:
            return self.create_group(key)
        else:
            return super().get(key, default=default)

    def update(self, init, wrap=False, **kwargs):
        """
        Add all elements or key-value pairs from init to this container.  If wrap is
        not given, behaves as the generic method.

        Args:
            init (Sequence, Set, Mapping): container to draw new elements from
            wrap (bool): if True wrap all encountered Sequences and Mappings in
                        DataContainers recursively
            **kwargs: update from this mapping as well
        """
        if wrap:
            if isinstance(init, (Sequence, Set)):
                for v in init:
                    self.append(self._wrap_val(v))

            elif isinstance(init, Mapping):
                for i, (k, v) in enumerate(init.items()):
                    k = _normalize(k)
                    v = self._wrap_val(v)
                    if isinstance(k, int):
                        if k == i:
                            self.append(v)
                        else:
                            raise ValueError(
                                "keys in initializer must not be int or str of "
                                "decimal digits or in correct order, "
                                "is {!r}".format(k))
                    else:
                        self[k] = v
            else:
                ValueError("init must be Sequence, Set or Mapping")

            for k in kwargs:
                self[k] = kwargs[k]
        else:
            super().update(init, **kwargs)

    def append(self, val):
        """
        Add new value to the container without a key.

        Args:
            val: new element
        """
        self._store.append(val)

    def extend(self, vals):
        """
        Append vals to the end of this DataContainer.

        Args:
            vals (Sequence): any python sequence to draw new elements from
        """

        for v in vals:
            self.append(v)

    def insert(self, index, val, key=None):
        """
        Add a new element to the container at the specified position, with an optional
        key.  If the key is already in the container it will be updated to point to
        the new element at the new index.  If index is larger than container, append
        instead.

        Args:
            index (int):            place val after this element
            val:                    new element to add
            key (str, optional):    optional key to mark the new element
        """
        if key is not None:
            for k, i in self._indices.items():
                if i >= index:
                    self._indices[k] = i + 1
            self._indices[key] = index

        self._store.insert(index, val)

    def mark(self, index, key):
        """
        Add a key to an existing item at index.  If key already exists, it is
        overwritten.

        Args:
            index (int):    index of the existing element to mark
            key (str):      key for the existing element

        Raises:
            IndexError: if index > len(self)

        >>> pl = DataContainer([42])
        >>> pl.mark(0, "head")
        >>> pl.head == 42
        True
        """
        if index >= len(self):
            raise IndexError("list index out of range")

        reverse_indices = {i: k for k, i in self._indices.items()}
        if index in reverse_indices:
            del self._indices[reverse_indices[index]]

        self._indices[key] = index

    def clear(self):
        """
        Remove all items from DataContainer.
        """
        self._store.clear()
        self._indices.clear()

    def create_group(self, name):
        """
        Add a new empty subcontainer under the given key.

        Args:
            name (str): key under which to store the new subcontainer in this container

        Returns:
            DataContainer: the newly created subcontainer

        >>> pl = DataContainer({})
        >>> pl.create_group("group_name")
        DataContainer([])
        >>> list(pl.group_name)
        []
        """
        self[name] = self.__class__()
        return self[name]

    def has_keys(self):
        """
        Check if the container has keys set or not.

        Returns:
            bool: True if there is at least one key set
        """
        return bool(self._indices)

    def __copy__(self):
        # by default copy.copy will use the same objects for _store and
        # _indices, which would cause the copied and the copiee to have the
        # same underlying data storage, so instead we have to do a shallow copy
        # of those manually
        copiee = type(self)()
        copiee._store = copy.copy(self._store)
        copiee._indices = copy.copy(self._indices)
        copiee.table_name = self.table_name
        return copiee

    def copy(self):
        """
        Returns deep copy of it self.  A shallow copy can be obtained via the
        copy module.

        Returns:
            DataContainer: deep copy of itself

        >>> pl = DataContainer([[1,2,3]])
        >>> pl.copy() == pl
        True
        >>> pl.copy() is pl
        False
        >>> all(a is not b for a, b in zip(pl.copy().values(), pl.values()))
        True
        """
        return copy.deepcopy(self)

    def to_hdf(self, hdf, group_name=None):
        """
        Store the DataContainer in an HDF5 file.  If ``group_name`` or
        *self.table_name* are not `None`, create a sub group in hdf prior to
        writing if not save directly to hdf.  group_name overrides
        self.table_name if both are not None.

        Args:
            hdf (ProjectHDFio): HDF5 group object
            group_name (str, optional): HDF5 subgroup name, overrides
                self.table_name
        """

        group_name = group_name or self.table_name
        if group_name:
            hdf = hdf.create_group(group_name)
        elif len(hdf.list_dirs()) > 0:
            raise ValueError(
                    "HDF group must be empty when neither table_name nor "
                    "group_name are set."
            )


        self._type_to_hdf(hdf)
        hdf["READ_ONLY"] = self.read_only
        for i, (k, v) in enumerate(self.items()):
            if isinstance(k, str) and "__index_" in k:
                raise ValueError("Key {} clashes with internal use!".format(k))

            k = "{}__index_{}".format(k if isinstance(k, str) else "", i)

            if hasattr(v, "to_hdf"):
                v.to_hdf(hdf=hdf, group_name=k)
            else:
                # if the value doesn't know how to serialize itself, assume
                # that h5py knows how to
                try:
                    hdf[k] = v
                except TypeError:
                    raise TypeError("Error saving {} (key {}): DataContainer doesn't support saving elements "
                                    "of type \"{}\" to HDF!".format(v, k, type(v))) from None

    def _type_to_hdf(self, hdf):
        """
        Internal helper function to save type and version in hdf root

        Args:
            hdf (ProjectHDFio): HDF5 group object
        """
        hdf["NAME"] = self.__class__.__name__
        hdf["TYPE"] = str(type(self))
        hdf["VERSION"] = self.__version__
        hdf["HDF_VERSION"] = self.__hdf_version__
        hdf["OBJECT"] = "DataContainer"

    def from_hdf(self, hdf, group_name=None):
        """
        Restore the DataContainer from an HDF5 file.  If group_name or
        self.table_name are not None, open a sub group in hdf prior to reading
        if not read directly from hdf.  group_name overrides self.table_name if
        both are not None.

        Args:
            hdf (ProjectHDFio): HDF5 group object
            group_name (str, optional): HDF5 subgroup name, overrides
                self.table_name
        """

        group_name = group_name or self.table_name
        if group_name:
            hdf = hdf.open(group_name)

        version = hdf.get("HDF_VERSION", "0.1.0")

        self.clear()

        if version == "0.1.0":
            self.update(hdf["data"], wrap=True)
            self.read_only = bool(hdf.get("read_only", False))
        else:
            def normalize_key(name):
                # split a dataset/group name into the position in the list and
                # the key
                k, i = name.split("__index_", maxsplit=1)
                i = int(i)
                if k == "":
                    return i, i
                else:
                    return i, k

            items = []
            for n in hdf.list_nodes():
                if n in _internal_hdf_nodes:
                    continue
                items.append( (*normalize_key(n), hdf[n]))
            for g in hdf.list_groups():
                items.append( (*normalize_key(g), hdf[g].to_object()))
            for _, k, v in sorted(items, key=lambda x: x[0]):
                self[k] = v

            self.read_only = bool(hdf.get("READ_ONLY", False))

    def nodes(self):
        """
        Iterator over keys to terminal nodes.

        Returns:
            :class:`list`: list of keys to normal values.
        """
        for k, v in self.items():
            if not isinstance(v, DataContainer):
                yield k

    def _list_nodes(self):
        return list(self.nodes())

    def groups(self):
        """
        Iterate over keys to nested containers.

        Returns:
            :class:`list`: list of all keys to elements of :class:`DataContainer`.
        """
        for k, v in self.items():
            if isinstance(v, DataContainer):
                yield k

    def _list_groups(self):
        return list(self.groups())

    def read(self, file_name, wrap=True):
        """
        Parse file as dictionary and add its keys to this container.

        For supported file types, see :func:`.fileio.read`.

        Errors during reading of the files generate a warning, but leave the container unchanged.

        Args:
            file_name(str): path to the input file
            wrap(bool), if set to true will wrap the inputed data itself as a datacontainer inside the datacontainer

        Raises:
            :class:`ValueError`: if file extension doesn't match one of the supported ones
        """
        self.update(read(file_name), wrap=wrap)

    def write(self, file_name):
        """
        Writes the DataContainer to a text file.

        For supported file types, see :func:`.fileio.write`.

        Args:
            file_name(str): the name of the file to be writen to.
        """
        write(self.to_builtin(), file_name)
