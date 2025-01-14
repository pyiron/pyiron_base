"""
Efficient storage of ragged arrays in a flattened format.
"""

# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

__author__ = ("Marvin Poul", "Niklas Leimeroth")
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Marvin Poul"
__email__ = "poul@mpie.de"
__status__ = "production"
__date__ = "Jul 16, 2020"


import copy
import warnings
from typing import Any, Callable, Iterable, List, Tuple

import h5py
import numpy as np
import pandas as pd

from pyiron_base.interfaces.has_dict import HasDictfromHDF
from pyiron_base.interfaces.has_hdf import HasHDF
from pyiron_base.interfaces.lockable import Lockable, sentinel

_CHARSIZE = np.dtype("U1").itemsize


def _ensure_str_array_size(array, strlen):
    """
    Ensures that the given array can store at least string of length `strlen`.

    Args:
        array (ndarray): array of dtype <U
        strlen (int, ndarray): maximum length that should fit in it
    Returns:
        ndarray: either `array` or resized copy
    """
    current_length = array.itemsize // _CHARSIZE
    if isinstance(strlen, np.ndarray):
        strlen = strlen.itemsize // _CHARSIZE
    if current_length < strlen:
        return array.astype(f"{2 * strlen}U")
    else:
        return array


class FlattenedStorage(Lockable, HasDictfromHDF, HasHDF):
    """
    Efficient storage of ragged arrays in flattened arrays.

    This class stores multiple arrays at the same time.  Storage is organized in "chunks" that may be of any size, but
    all arrays within chunk are of the same size, e.g.

    >>> a = [ [1], [2, 3], [4,  5,  6] ]
    >>> b = [ [2], [4, 6], [8, 10, 12] ]

    are stored as in three chunks like

    >>> a_flat = [ 1,  2, 3,  4,  5,  6 ]
    >>> b_flat = [ 2,  4, 6,  8, 10, 12 ]

    with additional metadata to indicate where the boundaries of each chunk are.

    First add arrays and chunks like this

    >>> store = FlattenedStorage()
    >>> store.add_array("even", dtype=np.int64)
    >>> store.add_chunk(1, even=[2])
    >>> store.add_chunk(2, even=[4,  6])
    >>> store.add_chunk(3, even=[8, 10, 12])

    where the first argument indicates the length of each chunk.  You may retrieve stored values like this

    >>> store.get_array("even", 1)
    array([4, 6])
    >>> store.get_array("even", 0)
    array([2])

    where the second arguments are integer indices in the order of insertion.  After intial storage you may modify
    arrays.

    >>> store.set_array("even", 0, [0])
    >>> store.get_array("even", 0)
    array([0])

    As a shorthand you can use regular index syntax

    >>> store["even", 0] = [2]
    >>> store["even", 0]
    array([2])
    >>> store["even", 1]
    array([4, 6])
    >>> store["even"]
    array([2, 4, 6, 8, 10, 12])
    >>> store["even", 0] = [0]

    You can add arrays to the storage even after you added already other arrays and chunks.

    >>> store.add_array("odd", dtype=np.int64, fill=0)
    >>> store.get_array("odd", 1)
    array([0, 0])
    >>> store.set_array("odd", 0, [1])
    >>> store.set_array("odd", 1, [3, 5])
    >>> store.set_array("odd", 2, [7, 9, 11])
    >>> store.get_array("odd", 2)
    array([ 7,  9, 11])

    Because the second chunk is already known to be of length two and `fill` was specified the 'odd' array has been
    appropriatly allocated.

    Additionally arrays may also only have one value per chunk ("per chunk", previous examples are "per element").

    >>> store.add_array("sum", dtype=np.int64, per="chunk")
    >>> for i in range(len(store)):
    ...    store.set_array("sum", i, sum(store.get_array("even", i) + store.get_array("odd", i)))
    >>> store.get_array("sum", 0)
    1
    >>> store.get_array("sum", 1)
    18
    >>> store.get_array("sum", 2)
    57

    Finally you may add multiple arrays in one call to :meth:`.add_chunk` by using keyword arguments

    >>> store.add_chunk(4, even=[14, 16, 18, 20], odd=[13, 15, 17, 19], sum=119)
    >>> store.get_array("sum", 3)
    119
    >>> store.get_array("even", 3)
    array([14, 16, 18, 20])

    It is usually not necessary to call :meth:`.add_array` before :meth:`.add_chunk`, the type of the array will be
    inferred in this case.

    If you skip the `frame` argument to :meth:`.get_array` it will return a flat array of all the values for that array
    in storage.

    >>> store.get_array("sum")
    array([  1,  18,  57, 119])
    >>> store.get_array("even")
    array([ 0,  4,  6,  8, 10, 12, 14, 16, 18, 20])

    Arrays may be of more complicated shape, too, see :meth:`.add_array` for details.

    Use :meth:`.copy` to obtain a deep copy of the storage, for shallow copies using the builting `copy.copy` is
    sufficient.

    >>> copy = store.copy()
    >>> copy["even", 0]
    array([0])
    >>> copy["even", 1]
    array([4, 6])
    >>> copy["even"]
    array([0, 4, 6, 8, 10, 12])

    Storages can be :meth:`.split` and :meth:`.join` again as long as their internal chunk structure is consistent,
    i.e. same number of chunks and same chunk lengths.  If this is not the case a `ValueError` is raised.

    >>> even = store.split(["even"])
    >>> bool(even.has_array("even"))
    True
    >>> bool(even.has_array("odd"))
    False
    >>> odd = store.split(["odd"])

    :meth:`.join` adds new arrays to the storage it is called on in-place.  To leave it unchanged, simply call copy
    before join.
    >>> both = even.copy().join(odd)

    Chunks may be given string names, either by passing `identifier` to :meth:`.add_chunk` or by setting to the
    special per chunk array "identifier"

    >>> store.set_array("identifier", 1, "second")
    >>> all(store.get_array("even", "second") == store.get_array("even", 1))
    True

    When adding new arrays follow the convention that per-structure arrays should be named in singular and per-atom
    arrays should be named in plural.

    You may initialize flattened storage objects with a ragged lists or numpy arrays of dtype object

    >>> even = [ list(range(0, 2, 2)), list(range(2, 6, 2)), list(range(6, 12, 2)) ]
    >>> even
    [[0], [2, 4], [6, 8, 10]]

    >>> import numpy as np
    >>> odd = np.array([ np.arange(1, 2, 2), np.arange(3, 6, 2), np.arange(7, 12, 2) ], dtype=object)
    >>> odd
    array([array([1]), array([3, 5]), array([ 7,  9, 11])], dtype=object)

    >>> store = FlattenedStorage(even=even, odd=odd)
    >>> store.get_array("even", 1)
    array([2, 4])
    >>> store.get_array("odd", 2)
    array([ 7,  9, 11])
    >>> len(store)
    3

    You can set storages as read-only via methods defined on
    :class:`.Lockable`.

    >>> store.lock()
    >>> store.get_array("even", 0)
    array([0])
    >>> store.set_array("even", np.array([4]))
    >>> store.get_array("even", 0)
    array([0])
    >>> with store.unlocked():
    ...   store.set_array("even", np.array([4]))
    >>> store.get_array("even", 0)
    array([4])
    """

    __version__ = "0.2.0"
    __hdf_version__ = "0.3.0"
    _default_fill_values = {
        np.dtype("int8"): -1,
        np.dtype("int16"): -1,
        np.dtype("int32"): -1,
        np.dtype("int64"): -1,
        np.dtype("float16"): np.nan,
        np.dtype("float32"): np.nan,
        np.dtype("float64"): np.nan,
        np.dtype("object"): None,
        np.dtype("uint8"): 0,
        np.dtype("uint16"): 0,
        np.dtype("uint32"): 0,
        np.dtype("uint64"): 0,
        str: "_default",
    }

    def __init__(self, num_chunks=1, num_elements=1, lock_method="error", **kwargs):
        """
        Create new flattened storage.

        Args:
            num_chunks (int): pre-allocation for per chunk arrays
            num_elements (int): pre-allocation for per elements arrays
        """
        super().__init__(lock_method=lock_method)

        # tracks allocated versed as yet used number of chunks/elements
        self._num_chunks_alloc = num_chunks
        self._num_elements_alloc = num_elements
        self.num_chunks = 0
        self.num_elements = 0
        # store the starting index for properties with unknown length
        self.current_element_index = 0
        # store the index for properties of known size, stored at the same index as the chunk
        self.current_chunk_index = 0
        # Also store indices of chunk recently added
        self.prev_chunk_index = 0
        self.prev_element_index = 0
        self._fill_values = {}

        self._init_arrays()

        if len(kwargs) == 0:
            return

        if len(set(len(chunks) for chunks in kwargs.values())) != 1:
            raise ValueError("Not all initializers provide the same number of chunks!")
        keys = kwargs.keys()
        for chunk_list in zip(*kwargs.values()):
            chunk_length = len(chunk_list[0])
            # values in chunk_list may either be a sequence of chunk_length, scalars (see hasattr check) or a sequence of
            # length 1
            if any(
                hasattr(c, "__len__") and len(c) != chunk_length and len(c) != 1
                for c in chunk_list
            ):
                raise ValueError("Inconsistent chunk length in initializer!")
            self.add_chunk(chunk_length, **{k: c for k, c in zip(keys, chunk_list)})

    def _init_arrays(self):
        self._per_element_arrays = {}

        self._per_chunk_arrays = {
            "start_index": np.full(
                self._num_chunks_alloc, dtype=np.int32, fill_value=0
            ),
            "length": np.full(self._num_chunks_alloc, dtype=np.int32, fill_value=0),
            "identifier": np.empty(self._num_chunks_alloc, dtype=np.dtype("U20")),
        }

    def __len__(self):
        return self.current_chunk_index

    def _internal_arrays(self) -> Tuple[str, ...]:
        """
        Names of "internal" arrays, i.e. arrays needed for the correct inner
        working of the flattened storage and that not are not added by the
        user via :meth:`.add_array`.

        Subclasses can override this tuple, by calling `super()` and appending
        to it.

        This exists mostly to support :meth:`.to_pandas()`.
        """
        return (
            "start_index",
            "length",
        )

    def copy(self):
        """
        Return a deep copy of the storage.

        Returns:
            :class:`.FlattenedStorage`: copy of self
        """
        return copy.deepcopy(self)

    def find_chunk(self, identifier):
        """
        Return integer index for given identifier.

        Args:
            identifier (str): name of chunk previously passed to :meth:`.add_chunk`

        Returns:
            int: integer index for chunk

        Raises:
            KeyError: if identifier is not found in storage
        """
        for i, name in enumerate(self._per_chunk_arrays["identifier"]):
            if name == identifier:
                return i
        raise KeyError(f"No chunk named {identifier}")

    def _get_per_element_slice(self, frame):
        start = self._per_chunk_arrays["start_index"][frame]
        end = start + self._per_chunk_arrays["length"][frame]
        return slice(start, end, 1)

    @sentinel
    def _resize_elements(self, new):
        old_max = self._num_elements_alloc
        self._num_elements_alloc = new
        for k, a in self._per_element_arrays.items():
            new_shape = (new,) + a.shape[1:]
            try:
                a.resize(new_shape)
            except ValueError:
                self._per_element_arrays[k] = np.resize(a, new_shape)
        if old_max < new:
            for k in self._per_element_arrays.keys():
                if k in self._fill_values.keys():
                    self._per_element_arrays[k][old_max:] = self._fill_values[k]

    @sentinel
    def _resize_chunks(self, new):
        old_max = self._num_chunks_alloc
        self._num_chunks_alloc = new
        for k, a in self._per_chunk_arrays.items():
            new_shape = (new,) + a.shape[1:]
            try:
                a.resize(new_shape)
            except ValueError:
                self._per_chunk_arrays[k] = np.resize(a, new_shape)
        if old_max < new:
            for k in self._per_chunk_arrays.keys():
                if k in self._fill_values.keys():
                    self._per_chunk_arrays[k][old_max:] = self._fill_values[k]

    @sentinel
    def add_array(self, name, shape=(), dtype=np.float64, fill=None, per="element"):
        """
        Add a custom array to the container.

        When adding an array after some chunks have been added, specifying `fill` will be used as a default value
        for the value of the array for those chunks.

        Adding an array with the same name twice is ignored, if dtype and shape match, otherwise raises an exception.

        >>> store = FlattenedStorage()
        >>> store.add_chunk(1, "foo")
        >>> store.add_array("energy", shape=(), dtype=np.float64, fill=42, per="chunk")
        >>> store.get_array("energy", 0)
        42.0

        Args:
            name (str): name of the new array
            shape (tuple of int): shape of the new array per element or chunk; scalars can pass ()
            dtype (type): data type of the new array, string arrays can pass 'U$n' where $n is the length of the string
            fill (object): populate the new array with this value for existing chunk, if given; default `None`
            per (str): either "element" or "chunk"; denotes whether the new array should exist for every element in a
                       chunk or only once for every chunk; case-insensitive

        Raises:
            ValueError: if wrong value for `per` is given
            ValueError: if array with same name but different parameters exists already
        """

        if per == "structure":
            per = "chunk"
            warnings.warn(
                'per="structure" is deprecated, use pr="chunk"',
                category=DeprecationWarning,
                stacklevel=2,
            )
        if per == "atom":
            per = "element"
            warnings.warn(
                'per="atom" is deprecated, use pr="element"',
                category=DeprecationWarning,
                stacklevel=2,
            )

        if name in self._per_element_arrays:
            a = self._per_element_arrays[name]
            if (
                a.shape[1:] != shape
                or not np.can_cast(dtype, a.dtype)
                or per != "element"
            ):
                raise ValueError(
                    f"Array with name '{name}' exists with shape {a.shape[1:]} and dtype {a.dtype}."
                )
            else:
                return

        if name in self._per_chunk_arrays:
            a = self._per_chunk_arrays[name]
            if (
                a.shape[1:] != shape
                or not np.can_cast(dtype, a.dtype)
                or per != "chunk"
            ):
                raise ValueError(
                    f"Array with name '{name}' exists with shape {a.shape[1:]} and dtype {a.dtype}."
                )
            else:
                return

        per = per.lower()
        if per == "element":
            shape = (self._num_elements_alloc,) + shape
            store = self._per_element_arrays
        elif per == "chunk":
            shape = (self._num_chunks_alloc,) + shape
            store = self._per_chunk_arrays
        else:
            raise ValueError(f'per must "element" or "chunk", not {per}')

        if fill is None:
            store[name] = np.empty(shape=shape, dtype=dtype)
        else:
            store[name] = np.full(shape=shape, fill_value=fill, dtype=dtype)

        if fill is None and store[name].dtype in self._default_fill_values:
            fill = self._default_fill_values[store[name].dtype]
        if fill is not None:
            self._fill_values[name] = fill

    def get_array(self, name, frame=None):
        """
        Fetch array for given structure.

        Works for per atom and per arrays.

        Args:
            name (str): name of the array to fetch
            frame (int, str, optional): selects structure to fetch, as in :meth:`.get_structure()`, if not given
                                        return a flat array of all values for either all chunks or elements

        Returns:
            :class:`numpy.ndarray`: requested array

        Raises:
            `KeyError`: if array with name does not exists
        """

        if isinstance(frame, str):
            frame = self.find_chunk(frame)
        if name in self._per_element_arrays:
            if frame is not None:
                return self._per_element_arrays[name][
                    self._get_per_element_slice(frame)
                ]
            else:
                return self._per_element_arrays[name][: self.num_elements]
        elif name in self._per_chunk_arrays:
            if frame is not None:
                return self._per_chunk_arrays[name][frame]
            else:
                return self._per_chunk_arrays[name][: self.num_chunks]
        else:
            raise KeyError(f"no array named {name}")

    def get_array_ragged(self, name: str) -> np.ndarray:
        """
        Return elements of array `name` in all chunks.  Values are returned in a ragged array of dtype=object.

        If `name` specifies a per chunk array, there's nothing to pad and this method is equivalent to
        :meth:`.get_array`.

        Args:
            name (str): name of array to fetch

        Returns:
            numpy.ndarray, dtype=object: ragged arrray of all elements in all chunks
        """
        if name in self._per_chunk_arrays:
            return self.get_array(name)
        # pre-allocated as dtype=object, then setting individual elements makes sure that element arrays retain their
        # dtype
        result = np.empty(len(self), dtype=object)
        for i in range(len(self)):
            result[i] = self.get_array(name, i)
        return result

    def get_array_filled(self, name: str) -> np.ndarray:
        """
        Return elements of array `name` in all chunks.  Arrays are padded to be all of the same length.

        The padding value depends on the datatpye of the array or can be configured via the `fill` parameter of
        :meth:`.add_array`.

        If `name` specifies a per chunk array, there's nothing to pad and this method is equivalent to
        :meth:`.get_array`.

        Args:
            name (str): name of array to fetch

        Returns:
            numpy.ndarray: padded arrray of all elements in all chunks
        """
        if name in self._per_chunk_arrays:
            return self.get_array(name)
        values = self.get_array_ragged(name)
        max_len = self._per_chunk_arrays["length"].max()

        def resize_and_pad(v):
            value_len = len(v)
            per_shape = self._per_element_arrays[name].shape[1:]
            v = np.resize(v, max_len * np.prod(per_shape, dtype=int))
            v = v.reshape((max_len,) + per_shape)
            if name in self._fill_values:
                fill = self._fill_values[name]
            else:
                fill = np.zeros(1, dtype=self._per_element_arrays[name].dtype)[0]
            v[value_len:] = fill
            return v

        return np.array([resize_and_pad(v) for v in values])

    @sentinel
    def set_array(self, name, frame, value):
        """
        Add array for given structure.

        Works for per chunk and per element arrays.

        Args:
            name (str): name of array to set
            frame (int, str): selects structure to set, as in :meth:`.get_strucure()`
            value: value (for per chunk) or array of values (for per element); type and shape as per :meth:`.hasarray()`.

        Raises:
            `KeyError`: if array with name does not exists
        """

        if isinstance(frame, str):
            frame = self.find_chunk(frame)
        if name in self._per_element_arrays:
            if self._per_element_arrays[name].dtype.char == "U":
                self._per_element_arrays[name] = _ensure_str_array_size(
                    self._per_element_arrays[name], max(map(len, value))
                )
            self._per_element_arrays[name][self._get_per_element_slice(frame)] = value
        elif name in self._per_chunk_arrays:
            if self._per_chunk_arrays[name].dtype.char == "U":
                if isinstance(value, np.ndarray) and value.ndim == 0:
                    strlen = len(value.item())
                else:
                    strlen = len(value)
                self._per_chunk_arrays[name] = _ensure_str_array_size(
                    self._per_chunk_arrays[name], strlen
                )
            self._per_chunk_arrays[name][frame] = value
        else:
            raise KeyError(f"no array named {name}")

    @sentinel
    def del_array(self, name: str, ignore_missing: bool = False):
        """
        Remove an array.

        Works with both per chunk and per element arrays.

        Args:
            name (str): name of the array
            ignore_missing (bool): if given do not raise an error if no array
                                   of the given `name` exists

        Raises:
            KeyError: if no array with given `name` exists and `ignore_missing` is not given
        """
        if name in self._per_element_arrays:
            del self._per_element_arrays[name]
        elif name in self._per_chunk_arrays:
            del self._per_chunk_arrays[name]
        elif not ignore_missing:
            raise KeyError(name)

    def __getitem__(self, index):
        if isinstance(index, tuple) and len(index) == 2:
            return self.get_array(index[0], index[1])
        else:
            return self.get_array(index)

    @sentinel
    def __setitem__(self, index, value):
        if isinstance(index, tuple) and len(index) == 2:
            self.set_array(index[0], index[1], value)
        else:
            raise IndexError("Must specify chunk index.")

    @sentinel
    def __delitem__(self, index):
        self.del_array(index)

    def has_array(self, name):
        """
        Checks whether an array of the given name exists and returns meta data given to :meth:`.add_array()`.

        >>> container.has_array("energy")
        {'shape': (), 'dtype': np.float64, 'per': 'chunk'}
        >>> container.has_array("fnorble")
        None

        Args:
            name (str): name of the array to check

        Returns:
            None: if array does not exist
            dict: if array exists, keys corresponds to the shape, dtype and per arguments of :meth:`.add_array`
        """
        if name in self._per_element_arrays:
            a = self._per_element_arrays[name]
            per = "element"
        elif name in self._per_chunk_arrays:
            a = self._per_chunk_arrays[name]
            per = "chunk"
        else:
            return None
        return {"shape": a.shape[1:], "dtype": a.dtype, "per": per}

    def list_arrays(self, only_user=False) -> List[str]:
        """
        Return a list of names of arrays inside the storage.

        Args:
            only_user (bool): If `True` include only array names added by the
            user via :meth:`.add_array` and the `identifier` array.

        Returns:
            list of str: array names
        """
        arrays = list(self._per_chunk_arrays) + list(self._per_element_arrays)
        if only_user:
            arrays = [a for a in arrays if a not in self._internal_arrays()]
        return arrays

    def sample(
        self, selector: Callable[["FlattenedStorage", int], bool]
    ) -> "FlattenedStorage":
        """
        Create a new storage with chunks selected by given function.

        If called on a subclass this correctly returns an instance of that subclass instead.

        Args:
            select (callable): function that takes this storage as the first argument and the chunk index to sample as
                               the second argument; if it returns True it will be part of the new storage.

        Returns:
            :class:`.FlattenedStorage` or subclass: storage with the selected chunks
        """
        new = type(self)()
        for k, a in self._per_chunk_arrays.items():
            if k not in ("start_index", "length", "identifier"):
                new.add_array(k, shape=a.shape[1:], dtype=a.dtype, per="chunk")
        for k, a in self._per_element_arrays.items():
            new.add_array(k, shape=a.shape[1:], dtype=a.dtype, per="element")
        for i in range(len(self)):
            if selector(self, i):
                new.add_chunk(
                    self.get_array("length", i),
                    identifier=self.get_array("identifier", i),
                )
                for k in self._per_chunk_arrays:
                    if k not in ("start_index", "length", "identifier"):
                        new.set_array(k, len(new) - 1, self.get_array(k, i))
                for k in self._per_element_arrays:
                    new.set_array(k, len(new) - 1, self.get_array(k, i))
        return new

    def split(self, array_names: Iterable[str]) -> "FlattenedStorage":
        """
        Return a new storage with only the selected arrays present.

        Arrays are deep-copied from `self`.

        Args:
            array_names (list of str): names of the arrays to present in new storage

        Returns:
            :class:`.FlattenedStorage`: storage with split arrays
        """
        for k in array_names:
            if k not in self._per_element_arrays and k not in self._per_chunk_arrays:
                raise ValueError(f"Array name {k} not present in FlattenedStorage!")

        split = copy.copy(self)
        for k in list(split._per_element_arrays):
            if k not in array_names:
                del split._per_element_arrays[k]
            else:
                split._per_element_arrays[k] = np.copy(split._per_element_arrays[k])
        for k in list(split._per_chunk_arrays):
            if k not in array_names and k not in (
                "start_index",
                "length",
                "identifier",
            ):
                del split._per_chunk_arrays[k]
            else:
                split._per_chunk_arrays[k] = np.copy(split._per_chunk_arrays[k])
        return split

    @sentinel
    def join(
        self, store: "FlattenedStorage", lsuffix: str = "", rsuffix: str = ""
    ) -> "FlattenedStorage":
        """
        Merge given storage into this one.

        `self` and `store` may not share any arrays.  Arrays defined on `stores` are copied and then added to `self`.

        Args:
            store (:class:`.FlattenedStorage`): storage to join
            lsuffix, rsuffix (str, optional): if either are given rename *all* arrays by appending the suffices to the
                                              array name; `lsuffix` for arrays in this storage, `rsuffix` for arrays in
                                              the added storage; in this case arrays are no longer available under the
                                              old name

        Returns:
            :class:`.FlattenedStorage`: self

        Raise:
            ValueError: if the two stores do not have the same number of chunks
            ValueError: if the two stores do not have equal chunk lengths
            ValueError: if lsuffix and rsuffix are equal and different from ""
            ValueError: if the stores share array names but `lsuffix` and `rsuffix` are not given
        """
        if len(self) != len(store):
            raise ValueError(
                "FlattenedStorages to be joined have to be of the same length!"
            )
        if (self["length"] != store["length"]).any():
            raise ValueError(
                "FlattenedStorages to be joined have to have same length chunks everywhere!"
            )
        if lsuffix == rsuffix != "":
            raise ValueError("lsuffix and rsuffix may not be equal!")
        rename = lsuffix != "" or rsuffix != ""
        if not rename:
            shared_elements = set(self._per_element_arrays).intersection(
                store._per_element_arrays
            )
            shared_chunks = set(self._per_chunk_arrays).intersection(
                store._per_chunk_arrays
            )
            shared_chunks.remove("start_index")
            shared_chunks.remove("length")
            shared_chunks.remove("identifier")
            if len(shared_elements) > 0 or len(shared_chunks) > 0:
                raise ValueError(
                    "FlattenedStorages to be joined may have common arrays only if lsuffix or rsuffix are given!"
                )

        for k, a in store._per_element_arrays.items():
            if k in self._per_element_arrays and rename:
                self._per_element_arrays[k + lsuffix] = self._per_element_arrays[k]
                if lsuffix != "":
                    del self._per_element_arrays[k]
                k += rsuffix
            self._per_element_arrays[k] = a

        for k, a in store._per_chunk_arrays.items():
            if k not in ("start_index", "length", "identifier"):
                if k in self._per_chunk_arrays and rename:
                    self._per_chunk_arrays[k + lsuffix] = self._per_chunk_arrays[k]
                    if lsuffix != "":
                        del self._per_chunk_arrays[k]
                    k += rsuffix
                self._per_chunk_arrays[k] = a

        self._resize_elements(self._num_elements_alloc)
        self._resize_chunks(self._num_chunks_alloc)
        return self

    @sentinel
    def add_chunk(self, chunk_length, identifier=None, **arrays):
        """
        Add a new chunk to the storeage.

        Additional keyword arguments given specify arrays to store for the chunk.  If an array with the given keyword
        name does not exist yet, it will be added to the container.

        >>> container = FlattenedStorage()
        >>> container.add_chunk(2, identifier="A", energy=3.14)
        >>> container.get_array("energy", 0)
        3.14

        If the first axis of the extra array matches the length of the chunk, it will be added as an per element array,
        otherwise as an per chunk array.

        >>> container.add_chunk(2, identifier="B", forces=2 * [[0,0,0]])
        >>> len(container.get_array("forces", 1)) == 2
        True

        Reshaping the array to have the first axis be length 1 forces the array to be set as per chunk array.  That axis
        will then be stripped.

        >>> container.add_chunk(2, identifier="C", pressure=np.eye(3)[np.newaxis, :, :])
        >>> container.get_array("pressure", 2).shape
        (3, 3)

        .. attention:: Edge-case!

            This will not work when the chunk length is also 1 and the array does not exist yet!  In this case the array
            will be assumed to be per element and there is no way around explicitly calling :meth:`.add_array()`.


        Args:
            chunk_length (int): length of the new chunk
            identifier (str, optional): human-readable name for the chunk, if None use current chunk index as string
            **kwargs: additional arrays to store for the chunk
        """

        if identifier is None:
            identifier = str(self.num_chunks)

        n = chunk_length
        new_elements = self.current_element_index + n

        if new_elements > self._num_elements_alloc:
            self._resize_elements(max(new_elements, self._num_elements_alloc * 2))
        if self.current_chunk_index + 1 > self._num_chunks_alloc:
            self._resize_chunks(max(1, self._num_chunks_alloc * 2))

        if new_elements > self.num_elements:
            self.num_elements = new_elements
        if self.current_chunk_index + 1 > self.num_chunks:
            self.num_chunks += 1

        # len of chunk to index into the initialized arrays
        i = self.current_element_index + n

        chunk_ind = self.current_chunk_index
        el_ind = self.current_element_index
        self._per_chunk_arrays["start_index"][chunk_ind] = el_ind
        self._per_chunk_arrays["length"][self.current_chunk_index] = n
        self._per_chunk_arrays["identifier"] = _ensure_str_array_size(
            self._per_chunk_arrays["identifier"], len(identifier)
        )
        self._per_chunk_arrays["identifier"][chunk_ind] = identifier

        for k, a in arrays.items():
            a = np.asarray(a)
            if k not in self._per_element_arrays and k not in self._per_chunk_arrays:
                if len(a.shape) > 0 and a.shape[0] == n:
                    self.add_array(k, shape=a.shape[1:], dtype=a.dtype, per="element")
                else:
                    shape = a.shape
                    # if the first axis was added by the caller to force to add a per chunk array, remove it again here
                    if len(shape) > 0 and a.shape[0] == 1:
                        shape = shape[1:]
                    self.add_array(k, shape=shape, dtype=a.dtype, per="chunk")
            # same as above: if the first axis was added by the caller to force to add a per chunk array, remove it
            # again here
            if k in self._per_chunk_arrays and len(a.shape) > 0 and a.shape[0] == 1:
                a = a[0]
            self.set_array(k, self.current_chunk_index, a)

        self.prev_chunk_index = self.current_chunk_index
        self.prev_element_index = self.current_element_index

        # Set new current_element_index and increase current_chunk_index
        self.current_chunk_index += 1
        self.current_element_index = i
        # return last_chunk_index, last_element_index

    @sentinel
    def extend(self, other: "FlattenedStorage"):
        """
        Add chunks from `other` to this storage.

        Afterwards the number of chunks and elements are the sum of the respective previous values.

        If `other` defines new arrays or doesn't define some of the arrays they are padded by the fill values.

        Args:
            other (:class:`.FlattenedStorage`): other storage to add

        Raises:
            ValueError: if fill values between both storages are not compatible

        Returns:
            FlattenedStorage: return this storage
        """
        self._check_compatible_fill_values(other=other)

        combined_num_chunks = self.num_chunks + other.num_chunks
        combined_num_elements = self.num_elements + other.num_elements
        if combined_num_chunks > self._num_chunks_alloc:
            self._resize_chunks(combined_num_chunks)
        if combined_num_elements > self._num_elements_alloc:
            self._resize_elements(combined_num_elements)

        for k, a in other._per_chunk_arrays.items():
            # add start_index of last chunk to start_index of other for correct mapping
            if (
                k == "start_index" and len(self) > 0
            ):  # Check if len > 0 to ensure that no random values are accessed for length and start_index after empty init
                last = self.num_chunks - 1
                len_last = self._per_chunk_arrays["length"][last]
                a = (
                    a + self._per_chunk_arrays[k][last] + len_last
                )  # no += to prevent inplace mutation
            if k not in self._per_chunk_arrays.keys():
                dtype, fill = get_dtype_and_fill(storage=other, name=k)
                self.add_array(
                    name=k, dtype=dtype, shape=a.shape[1:], fill=fill, per="chunk"
                )
            elif a.dtype.char == "U":
                self._per_chunk_arrays[k] = _ensure_str_array_size(
                    self._per_chunk_arrays[k], a
                )
            self._per_chunk_arrays[k][self.num_chunks : combined_num_chunks] = a[
                0 : other.num_chunks
            ]

        for k, a in other._per_element_arrays.items():
            if k not in self._per_element_arrays.keys():
                dtype, fill = get_dtype_and_fill(storage=other, name=k)
                self.add_array(
                    name=k, shape=a.shape[1:], dtype=dtype, fill=fill, per="element"
                )
            elif a.dtype.char == "U":
                self._per_element_arrays[k] = _ensure_str_array_size(
                    self._per_element_arrays[k], a
                )
            self._per_element_arrays[k][self.num_elements : combined_num_elements] = a[
                0 : other.num_elements
            ]
        self.num_elements = combined_num_elements
        self.num_chunks = combined_num_chunks
        self.current_chunk_index = self.num_chunks
        self.current_element_index = self.num_elements

        return self

    def _check_compatible_fill_values(self, other: "FlattenedStorage"):
        """
        Check if fill values of 2 FlattenedStorages match to prevent errors due to wrong fill values,
        f.e. after extending to the storage.

        Args:
            other (FlattenedStorage): Another FlattenedStorage instance

        Raises:
            ValueError: Raises when the storages have different fill values for a key
        """
        for k in set(self._fill_values).intersection(other._fill_values):
            if np.isnan(self._fill_values[k]) and np.isnan(other._fill_values[k]):
                continue
            else:
                if self._fill_values[k] != other._fill_values[k]:
                    raise ValueError(
                        "Fill values for arrays in storages don't match, can't perform requested operation"
                    )

    def _get_hdf_group_name(self):
        return "flat_storage"

    def _to_hdf(self, hdf):
        def write_array(name, array, hdf):
            if array.dtype.char == "U":
                # numpy stores unicode data in UTF-32/UCS-4, but h5py wants UTF-8, so we manually encode them here
                # TODO: string arrays with shape != () not handled
                hdf[name] = np.array(
                    [s.encode("utf8") for s in array],
                    # each character in a utf8 string might be encoded in up to 4 bytes, so to
                    # make sure we can store any string of length n we tell h5py that the
                    # string will be 4 * n bytes; numpy's dtype does this calculation already
                    # in itemsize, so we don't need to repeat it here
                    # see also https://docs.h5py.org/en/stable/strings.html
                    dtype=h5py.string_dtype("utf8", array.dtype.itemsize),
                )
            else:
                hdf[name] = array

        # truncate arrays to necessary size before writing
        self._resize_elements(self.num_elements)
        self._resize_chunks(self.num_chunks)

        hdf["num_elements"] = self._num_elements_alloc
        hdf["num_chunks"] = self._num_chunks_alloc

        hdf_arrays = hdf.open("element_arrays")
        for k, a in self._per_element_arrays.items():
            write_array(k, a, hdf_arrays)

        hdf_arrays = hdf.open("chunk_arrays")
        for k, a in self._per_chunk_arrays.items():
            write_array(k, a, hdf_arrays)

        hdf["_fill_values"] = self._fill_values

    def _from_hdf(self, hdf, version=None):
        def read_array(name, hdf):
            a = np.asarray(hdf[name])
            if a.dtype.char == "S":
                # if saved as bytes, we wrote this as an encoded unicode string, so manually decode here
                # TODO: string arrays with shape != () not handled
                a = np.fromiter(
                    (s.decode("utf8") for s in a),
                    # itemsize of original a is four bytes per character, so divide by four to get
                    # length of the orignal stored unicode string; np.dtype('U1').itemsize is just a
                    # platform agnostic way of knowing how wide a unicode charater is for numpy
                    dtype=f"U{a.dtype.itemsize // np.dtype('U1').itemsize}",
                )
            return a

        try:
            num_chunks = hdf["num_chunks"]
            num_elements = hdf["num_elements"]
        except ValueError:
            num_chunks = hdf["num_structures"]
            num_elements = hdf["num_atoms"]

        self._num_chunks_alloc = self.num_chunks = self.current_chunk_index = num_chunks
        self._num_elements_alloc = self.num_elements = self.current_element_index = (
            num_elements
        )

        if version == "0.1.0":
            with hdf.open("arrays") as hdf_arrays:
                for k in hdf_arrays.list_nodes():
                    a = read_array(k, hdf_arrays)
                    if a.shape[0] == self._num_elements_alloc:
                        self._per_element_arrays[k] = a
                    elif a.shape[0] == self._num_chunks_alloc:
                        self._per_chunk_arrays[k] = a
        elif version == "0.2.0" or "0.3.0":
            with hdf.open("element_arrays") as hdf_arrays:
                for k in hdf_arrays.list_nodes():
                    self._per_element_arrays[k] = read_array(k, hdf_arrays)
            with hdf.open("chunk_arrays") as hdf_arrays:
                for k in hdf_arrays.list_nodes():
                    self._per_chunk_arrays[k] = read_array(k, hdf_arrays)
        else:
            raise RuntimeError(
                f"Unsupported HDF version {version}; use an older version of pyiron to load this job!"
            )

        for k, a in self._per_chunk_arrays.items():
            if a.shape[0] != self._num_chunks_alloc:
                raise RuntimeError(
                    f"per-chunk array {k} read inconsistently from HDF: "
                    f"shape {a.shape[0]} does not match global allocation {self._num_chunks_alloc}!"
                )
        for k, a in self._per_element_arrays.items():
            if a.shape[0] != self._num_elements_alloc:
                raise RuntimeError(
                    f"per-element array {k} read inconsistently from HDF: "
                    f"shape {a.shape[0]} does not match global allocation {self._num_elements_alloc}!"
                )

        if version >= "0.3.0":
            self._fill_values = hdf["_fill_values"]

    def to_pandas(self, explode=False, include_index=False) -> pd.DataFrame:
        """
        Convert arrays to pandas dataframe.

        Args:
            explode (bool): If `False` values of per element arrays are stored
                            in the dataframe as arrays, otherwise each row in the dataframe
                            corresponds to an element in the original storage.

        Returns:
            :class:`pandas.DataFrame`: table of array values
        """
        arrays = self.list_arrays(only_user=True)
        # convert to list for the case where shape!=(); in this case pandas
        # complains about multidimensional arrays
        df = pd.DataFrame({a: list(self.get_array_ragged(a)) for a in arrays})
        if explode:
            elem_arrays = [a for a in arrays if self.has_array(a)["per"] == "element"]
            df = (
                df.explode(elem_arrays)
                .infer_objects(copy=False)
                .reset_index(drop=not include_index)
            )
        return df


def get_dtype_and_fill(storage: FlattenedStorage, name: str) -> Tuple[np.generic, Any]:
    fill = None
    if name in storage._fill_values.keys():
        fill = storage._fill_values[name]
        dtype = type(fill)
    else:
        a = storage.get_array(name)
        if a.dtype.char == "U":
            dtype = str
        else:
            dtype = a.dtype
        try:
            fill = FlattenedStorage._default_fill_values[dtype]
        except KeyError:
            raise ValueError(
                f"Could not determine a default fill value for array {name}"
            )
    return dtype, fill
