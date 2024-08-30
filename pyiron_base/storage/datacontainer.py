"""
Data structure for versatile data handling.
"""

# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import copy
import json
import warnings
from collections.abc import Mapping, MutableMapping, Sequence, Set

import numpy as np
import pandas

from pyiron_base.interfaces.has_dict import (
    HasDict,
    HasDictfromHDF,
    _from_dict_children,
    _to_dict_children,
)
from pyiron_base.interfaces.has_groups import HasGroups
from pyiron_base.interfaces.has_hdf import HasHDF
from pyiron_base.interfaces.lockable import Lockable, sentinel
from pyiron_base.storage.fileio import read, write
from pyiron_base.storage.hdfstub import HDFStub, to_object

__author__ = "Marvin Poul"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
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
    "DICT_VERSION",
    "READ_ONLY",
    "KEY_ORDER",
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


class DataContainerBase(MutableMapping, Lockable, HasGroups):
    """
    Mutable sequence with optional keys.

    If no argument is given, the constructor creates a new empty DataContainerBase.  If
    specified init maybe a Sequence, Set or Mapping and all recursive
    occurrences of these are also wrapped by DataContainerBase.

    >>> pl = DataContainerBase([3, 2, 1, 0])
    >>> pm = DataContainerBase({"foo": 24, "bar": 42})

    Access can be like a normal list with integers or optionally with strings
    as keys.

    >>> pl[0]
    3
    >>> pl[2]
    1
    >>> pm["foo"]
    24

    Keys do not have to be present for all elements.

    >>> pl2 = DataContainerBase([1,2])
    >>> pl2["end"] = 3
    >>> pl2
    DataContainerBase({0: 1, 1: 2, 'end': 3})

    It is also allowed to set an item one past the length of the DataContainerBase,
    this is then equivalent to appending that element.  This allows to use the
    update method also with other DataContainerBases

    >>> pl[len(pl)] = -1
    >>> pl
    DataContainerBase([3, 2, 1, 0, -1])
    >>> pl.pop(-1)
    -1

    Where strings are used they may also be used as attributes.  Getting keys
    which clash with methods of DataContainerBase must be done with item access, but
    setting them works without overwriting the instance methods, but is not
    recommended for readability.

    >>> pm.foo
    24
    >>> pm.tail = 23
    >>> pm
    DataContainerBase({'foo': 24, 'bar': 42, 'tail': 23})

    Keys and indices can be tuples to traverse nested DataContainerBases.

    >>> pn = DataContainerBase({"foo": {"bar": [4, 2]}})
    >>> pn["foo", "bar"]
    DataContainerBase([4, 2])
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
    >>> DataContainerBase({1: 42})
    Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "datacontainer.py", line 126, in __init__
            raise ValueError(
    ValueError: keys in initializer must not be int or str of decimal digits or in correct order, is 1

    When initializing from a dict, it may not have integers or decimal strings
    as keys unless they match their position in the insertion order.  This is
    to avoid ambiguities in the final order of the DataContainerBase.

    >>> DataContainerBase({0: "foo", 1: "bar", 2: 42})
    DataContainerBase(['foo', 'bar', 42])
    >>> DataContainerBase({0: "foo", 2: 42, 1: "bar"})
    Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "datacontainer.py", line 132, in __init__
            raise ValueError(
    ValueError: keys in initializer must not be int or str of decimal digits or in correct order, is 2


    Using keys is completely optional, DataContainerBase can always be treated as a
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

    >>> p = DataContainerBase({"a": 42, "b": [0, 1, 2]})
    >>> p.list_groups()
    ['b']
    >>> p.list_nodes()
    ['a']

    .. attention:: Subclasses beware!

        DataContainerBase require some careful treatment when creating subclasses.

        1. Since DataContainerBases are expected to recursively instantiate themselves subclasses need to keep their
        `__init__ compatible to the base class.  That means being able to be instantiated without arguments, if
        arguments are given the first one (or `init`) has to accept a Mapping or Iterable.  Additional arguments may be
        added, but must be after `init` and must have a default.

        2. Creating new instance attributes that don't live in the container itself is possible, but you need to use
        `object.__setattr__` the first time you define that attribute.  Afterwards using normal assignment syntax works.

        3. Subclasses should always be thought of as general data structures, if you want to subclass to have access to
        the HDF5 functionality or the way the DataContainerBase is shown in jupyter notebooks, but only have a fixed number
        of attributes it is better to create a new class that has an DataContainerBase as an attribute and dispatch to the
        :meth:`DataContainerBase.from_hdf`, :meth:`DataContainerBase.to_hdf` and :meth:`DataContainerBase._repr_json_`
        methods.


    A few examples for subclasses

    >>> class ExtendedContainer(DataContainerBase):
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

    Be aware the :class:`.DataContainerBase` and its subclasses are recursive data structures, i.e. your fancy attribute
    will be available also on sub groups.

    >>> g = e.create_group('sub')
    >>> g.fnord = 23
    >>> g.my_fancy_field
    42
    >>> e
    ExtendedContainer({'foo': 1, 'bar': 5, 'sub': ExtendedContainer({'fnord': 23})})

    For that reason most of time you'll actually want a class that uses a DataContainerBase for storage, but doesn't derive
    from it.

    >>> from pyiron_base.interfaces.object import HasStorage
    >>> class FancyClass(HasStorage):
    ...     def __init__(self, foo):
    ...         super().__init__()
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
    ...     def _repr_json_(self):
    ...         return self.storage._repr_json_()

    """

    __version__ = "0.1.0"

    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        # setting these immediately after object creation ensures that they are
        # always defined and attribute access works even before __init__ is
        # called.  This is relevant on deepcopy & pickling.
        object.__setattr__(instance, "_store", [])
        object.__setattr__(instance, "_indices", {})
        object.__setattr__(instance, "table_name", None)

        return instance

    def __init__(
        self,
        init=None,
        table_name=None,
        wrap_blacklist=(),
        lock_method="warning",
    ):
        """
        Create new container.

        Args:
            init (Sequence, Mapping): initial data for the container, nested occurances of Sequence and Mapping are
                                      translated to nested containers
            table_name (str): default name of the data container in HDF5
            wrap_blacklist (tuple of types): any values in `init` that are instances of the given types are *not*
                                             wrapped in :class:`.DataContainerBase`
        """
        super().__init__(lock_method=lock_method)
        self.table_name = table_name
        if init is not None:
            self.update(init, wrap=True, blacklist=wrap_blacklist)

    def __len__(self):
        return len(self._store)

    def __iter__(self):
        reverse_indices = {i: k for k, i in self._indices.items()}

        for i in range(len(self)):
            yield reverse_indices.get(i, i)

    def __getitem__(self, key):
        key = _normalize(key)

        if isinstance(key, tuple):
            if key[0] == "..." and len(key) > 1:
                res = self.search(key[1], False)
                return res if (len(key) == 2) else res[key[2:]]
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
            raise ValueError("{} is not a valid key, must be str or int".format(key))

    @sentinel
    def __setitem__(self, key, val):
        key = _normalize(key)

        if isinstance(key, tuple):
            if key[0] == "..." and len(key) > 1:
                res = self._search_parent(key[1], False)
                res[key[1:]] = val
                return
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
            raise ValueError("{} is not a valid key, must be str or int".format(key))

    @sentinel
    def __delitem__(self, key):
        key = _normalize(key)

        if isinstance(key, tuple):
            if key[0] == "..." and len(key) > 1:
                res = self._search_parent(key[1], False)
                del res[key[1:]]
                return
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
            raise ValueError("{} is not a valid key, must be str or int".format(key))

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

    @sentinel
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

    @sentinel
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
            # access _store and _indices directly to avoid forcing HDFStubs in
            # subclass
            index2key = {v: k for k, v in self._indices.items()}
            return (
                name
                + "({"
                + ", ".join(
                    "{!r}: {!r}".format(index2key.get(i, i), self._store[i])
                    for i in range(len(self))
                )
                + "})"
            )
        else:
            return name + "([" + ", ".join("{!r}".format(v) for v in self._store) + "])"

    def to_builtin(self, stringify=False):
        """
        Convert the container back to builtin dict's and list's recursively.

        Args:
            stringify (bool, optional): convert all non-recursive elements to str
        """

        def rec(v):
            if isinstance(v, DataContainerBase):
                return v.to_builtin(stringify=stringify)
            else:
                return repr(v) if stringify else v

        if self.has_keys():
            # force all string keys in output to work with h5io (it
            # requires all string keys when storing as json), since
            # _normalize calls int() on all digit string keys this is
            # transparent for the rest of the module
            return {str(k): rec(v) for k, v in self.items()}
        else:
            return [rec(v) for v in self.values()]

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

    def search(self, key, stop_on_first_hit=True):
        """
        Search for ``key`` in the Container hierarchy.

        This should be used if there is only one such item in the hierarchy.

        If stop_on_first_hit is True the first item found is taken.
        Otherwise, a ValueError is raised if the key appears multiple times.

        Args:
            key (str):                the key to look for
            stop_on_first_hit (bool): whether to stop on the first hit

        Raise:
            TypeError:  if key is not str
            KeyError:   if key is not found
            ValueError: if stop_on_first_hit is False and key is found twice

        Returns:
            object: element at ``key``
        """

        if not isinstance(key, str):
            raise TypeError("Cannot search for non-string key.")

        parent = self._search_parent(key, stop_on_first_hit)
        if parent is None:
            raise KeyError("Could not find any element '" + key + "' in tree.")

        return parent[key]

    def _search_parent(self, key, stop_on_first_hit=True):
        """
        Search for container in hierarchy which has ``key``

        This should be used if there is only one such item in the hierarchy.
        If stop_on_first_hit is True the first item found is taken.
        Otherwise, a ValueError is raised if the key appears multiple times.

        Args:
            key (str):                the key to look for
            stop_on_first_hit (bool): what to do if key is found
                                      True  => return
                                      False => continue to check that it is
                                               the only hit
        Raise:
            ValueError: if key is found twice and stop_on_first_hit is False

        Returns:
            DataContainerBase: container that has ``key``
        """
        # search within current level
        if key in self:
            if stop_on_first_hit:
                return self
            else:
                first_hit = self
        else:
            first_hit = None

        # descend into subgroups
        for it in self.groups():
            hit = self[it]._search_parent(key, stop_on_first_hit)
            if isinstance(hit, DataContainerBase):
                if stop_on_first_hit:
                    return hit
                else:
                    if isinstance(first_hit, DataContainerBase):
                        raise ValueError("'" + key + "' exists more than once!")
                first_hit = hit
        return first_hit

    @classmethod
    def _wrap_val(cls, val, blacklist):
        if isinstance(val, (Sequence, Set, Mapping)) and not isinstance(val, blacklist):
            return cls(val, wrap_blacklist=blacklist)
        else:
            return val

    @sentinel
    def update(self, init, wrap=False, blacklist=(), **kwargs):
        """
        Add all elements or key-value pairs from init to this container.  If wrap is
        not given, behaves as the generic method.

        Args:
            init (Sequence, Set, Mapping): container to draw new elements from
            wrap (bool): if True wrap all encountered Sequences and Mappings in
                         :class:`.DataContainerBase` recursively
            blacklist (list of types): when `wrap` is True, don't wrap these types even if they're instances of Sequence
                                       or Mapping
            **kwargs: update from this mapping as well
        """
        if str not in blacklist:
            blacklist += (str,)
        if wrap and (isinstance(wrap, bool) or not isinstance(init, blacklist)):
            if isinstance(init, (Sequence, Set)):
                for v in init:
                    self.append(self._wrap_val(v, blacklist))

            elif isinstance(init, Mapping):
                for i, (k, v) in enumerate(init.items()):
                    k = _normalize(k)
                    v = self._wrap_val(v, blacklist)
                    if isinstance(k, int):
                        if k == i:
                            self.append(v)
                        else:
                            raise ValueError(
                                "keys in initializer must not be int or str of "
                                "decimal digits or in correct order, "
                                "is {!r}".format(k)
                            )
                    else:
                        self[k] = v
            else:
                ValueError("init must be Sequence, Set or Mapping")

            for k in kwargs:
                self[k] = self._wrap_val(kwargs[k], blacklist)
        else:
            super().update(init, **kwargs)

    @sentinel
    def append(self, val):
        """
        Add new value to the container without a key.

        Args:
            val: new element
        """
        self._store.append(val)

    @sentinel
    def extend(self, vals):
        """
        Append vals to the end of this DataContainerBase.

        Args:
            vals (Sequence): any python sequence to draw new elements from
        """

        for v in vals:
            self.append(v)

    @sentinel
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

    @sentinel
    def mark(self, index, key):
        """
        Add a key to an existing item at index.  If key already exists, it is
        overwritten.

        Args:
            index (int):    index of the existing element to mark
            key (str):      key for the existing element

        Raises:
            IndexError: if index > len(self)

        >>> pl = DataContainerBase([42])
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

    @sentinel
    def clear(self):
        """
        Remove all items from DataContainerBase.
        """
        self._store.clear()
        self._indices.clear()

    @sentinel
    def create_group(self, name):
        """
        Add a new empty subcontainer under the given key.

        Args:
            name (str): key under which to store the new subcontainer in this container

        Returns:
            DataContainerBase: the newly created subcontainer

        Raises:
            ValueError: name already exists in container and is not a sub container

        >>> pl = DataContainerBase({})
        >>> pl.create_group("group_name")
        DataContainerBase([])
        >>> list(pl.group_name)
        []
        """
        if name not in self:
            self[name] = self.__class__()
            return self[name]
        else:
            v = self[name]
            if isinstance(v, self.__class__):
                return v
            else:
                raise ValueError(f"'{name}' already exists in DataContainerBase.")

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
            DataContainerBase: deep copy of itself

        >>> pl = DataContainerBase([[1,2,3]])
        >>> pl.copy() == pl
        True
        >>> pl.copy() is pl
        False
        >>> all(a is not b for a, b in zip(pl.copy().values(), pl.values()))
        True
        """
        return copy.deepcopy(self)

    def nodes(self):
        """
        Iterator over keys to terminal nodes.

        Returns:
            :class:`list`: list of keys to normal values.
        """
        for k, v in self.items():
            if not isinstance(v, DataContainerBase):
                yield k

    def _list_nodes(self):
        return list(self.nodes())

    def groups(self):
        """
        Iterate over keys to nested containers.

        Returns:
            :class:`list`: list of all keys to elements of :class:`DataContainerBase`.
        """
        for k, v in self.items():
            if isinstance(v, DataContainerBase):
                yield k

    def _list_groups(self):
        return list(self.groups())

    @sentinel
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
        Writes the DataContainerBase to a text file.

        For supported file types, see :func:`.fileio.write`.

        Args:
            file_name(str): the name of the file to be writen to.
        """
        write(self.to_builtin(), file_name)

    # Lockable overload
    def _on_unlock(self):
        from sys import version_info as python_version

        # a little dance to ensure that warning appear at the correct call
        # site, i.e. where someone either calls unlocked() or sets read_only
        if python_version[0] == 3 and python_version[1] >= 12:
            from pyiron_base.interfaces.lockable import __file__ as lock_file

            warnings.warn(
                "Unlock previously locked object!",
                skip_file_prefixes=(__file__, lock_file),
            )
        else:
            # stacklevel is so high, because _on_unlock is called after several
            # layers of Lockable and DataContainer.__setattr__ when used to set
            # read_only; when used with unlocked() a fixed stack level still
            # emits it at the wrong place, but we cannot do better without
            # Python 3.12
            warnings.warn("Unlock previously locked object!", stacklevel=5)
        super()._on_unlock()


class DataContainer(DataContainerBase, HasHDF, HasDict):
    __dict_version__ = "0.2.0"
    __doc__ = f"""{DataContainerBase.__doc__}

    If instantiated with the argument `lazy=True`, data read from HDF5 later via :method:`.from_hdf` are not actually
    read, but only earmarked to be read later when actually accessed via :class:`.HDFStub`.  This is largely
    transparent, i.e. when accessing an earmarked value it will automatically be loaded and this loaded value is stored
    in container.  The only difference is in the string representation of the container, values not read yet appear as
    'HDFStub(...)' in the output.

    .. attention:: Subclasses beware!
        1. To allow lazy loading sub classes must accept a `lazy` keyword argument and pass it to `super().__init__`.
    """
    __hdf_version__ = "0.2.0"

    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls, *args, **kwargs)
        object.__setattr__(instance, "_lazy", False)

        return instance

    def __init__(
        self,
        init=None,
        table_name=None,
        lazy=False,
        wrap_blacklist=(),
        lock_method="warning",
    ):
        """
        Create new container.

        Args:
            init (Sequence, Mapping): initial data for the container, nested occurances of Sequence and Mapping are
                                      translated to nested containers
            table_name (str): default name of the data container in HDF5
            lazy (bool): if True, use :class:`.HDFStub` to load values lazily from HDF5
            wrap_blacklist (tuple of types): any values in `init` that are instances of the given types are *not*
                                             wrapped in :class:`.DataContainerBase`
        """
        super().__init__(
            init=init,
            table_name=table_name,
            wrap_blacklist=wrap_blacklist,
            lock_method=lock_method,
        )
        self._lazy = lazy

    # HasHDF impl'

    def _get_hdf_group_name(self):
        return self.table_name

    def _to_hdf(self, hdf):
        hdf["READ_ONLY"] = self.read_only
        written_keys = _internal_hdf_nodes.copy()
        for i, (k, v) in enumerate(self.items()):
            if isinstance(k, str) and "__index_" in k:
                raise ValueError("Key {} clashes with internal use!".format(k))

            k = "{}__index_{}".format(k if isinstance(k, str) else "", i)
            written_keys.append(k)

            # pandas objects also have a to_hdf method that is entirely unrelated to ours
            if hasattr(v, "to_hdf") and not isinstance(
                v, (pandas.DataFrame, pandas.Series)
            ):
                # if v will be written as a group, but a node of the same name k exists already in the file, h5py will
                # complain, so delete it first
                if k in hdf.list_nodes():
                    del hdf[k]
                v.to_hdf(hdf=hdf, group_name=k)
            else:
                # if the value doesn't know how to serialize itself, assume
                # that h5py knows how to
                try:
                    hdf[k] = v
                except TypeError:
                    raise TypeError(
                        "Error saving {} (key {}): DataContainer doesn't support saving elements "
                        'of type "{}" to HDF!'.format(v, k, type(v))
                    ) from None
        for n in hdf.list_nodes() + hdf.list_groups():
            if n not in written_keys:
                del hdf[n]

    def _from_hdf(self, hdf, version=None):
        with self.unlocked():
            self.clear()

            if version == "0.1.0":
                self.update(hdf["data"], wrap=True)
                self.read_only = bool(hdf.get("read_only", False))
            else:

                def normalize_key(name):
                    # split a dataset/group name into the position in the list and
                    # the key
                    if "__index_" in name:
                        k, i = name.split("__index_", maxsplit=1)
                    else:
                        k = name
                        i = -1
                    i = int(i)
                    if k == "":
                        return i, i
                    else:
                        return i, k

                items = []
                for n in hdf.list_nodes():
                    if n in _internal_hdf_nodes:
                        continue
                    items.append(
                        (
                            *normalize_key(n),
                            hdf[n] if not self._lazy else HDFStub(hdf, n),
                        )
                    )
                for g in hdf.list_groups():
                    items.append(
                        (
                            *normalize_key(g),
                            to_object(hdf[g]) if not self._lazy else HDFStub(hdf, g),
                        )
                    )

                for _, k, v in sorted(items, key=lambda x: x[0]):
                    self[k] = v

                self.read_only = bool(hdf.get("READ_ONLY", False))

    # HDFStub compat
    def __getitem__(self, key):
        value = super().__getitem__(key)
        if not isinstance(value, HDFStub):
            return value
        else:
            value = self[key] = value.load()
            return value

    def _force_load(self, recursive=True):
        """
        Load all HDFStubs present in the data container.

        Args:
            recursive (bool): force also nested data containers, default True
        """
        if not self._lazy and not recursive:
            return

        # values are loaded from HDF once they are accessed via __getitem__, which is implicitly called by values()
        for v in self.values():
            if recursive and isinstance(v, DataContainer):
                v._force_load()

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
        self._force_load()
        return super().copy()

    def __init_subclass__(cls):
        # called whenever a subclass of DataContainer is defined, then register all subclasses with the same function
        # that the DataContainer is registered
        HDFStub.register(cls, lambda h, g: h[g].to_object(lazy=True))

    def to_builtin(self, stringify=False):
        data = super().to_builtin(stringify=stringify)

        def to(v):
            if isinstance(v, HasDict):
                return v.to_dict()
            elif isinstance(v, HasHDF):
                return HasDictfromHDF.to_dict(v)
            else:
                return v

        if not stringify:
            if isinstance(data, dict):
                data = {k: to(v) for k, v in data.items()}
            elif isinstance(data, list):
                data = [to(v) for v in data]
            else:
                assert False, "to_builtin returned neither list nor dict"

        return data

    def _to_dict(self):
        # stringify keys in case we are acting like a list
        data = {str(k): v for k, v in dict(self).items()}
        order = list(data)
        data["READ_ONLY"] = self.read_only
        data["KEY_ORDER"] = order
        return _to_dict_children(data)

    def _from_dict(self, obj_dict, version=None):
        obj_dict = _from_dict_children(obj_dict)
        if version == "0.2.0":
            order = obj_dict.pop("KEY_ORDER")
        else:
            order = None
        self.read_only = obj_dict.pop("READ_ONLY", False)
        for key in _internal_hdf_nodes:
            obj_dict.pop(key, None)
        with self.unlocked():
            self.clear()
            if order is not None:
                for key in order:
                    self[key] = obj_dict[key]
            else:
                self.update(obj_dict)


HDFStub.register(DataContainer, lambda h, g: h[g].to_object(lazy=True))
