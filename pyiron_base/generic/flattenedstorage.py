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


import numpy as np
import h5py

class FlattenedStorage:
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

    Finally you may add multiple arrays in one call to :method:`.add_chunk` by using keyword arguments

    >>> store.add_chunk(4, even=[14, 16, 18, 20], odd=[13, 15, 17, 19], sum=119)
    >>> store.get_array("sum", 3)
    119
    >>> store.get_array("even", 3)
    array([14, 16, 18, 20])

    Chunks may be given string names, either by passing `identifier` to :method:`.add_chunk` or by setting to the
    special per chunk array "identifier"

    >>> store.set_array("identifier", 1, "second")
    >>> all(store.get_array("even", "second") == store.get_array("even", 1))
    True

    It is usually not necessary to call :method:`.add_array` before :method:`.add_chunk`, the type of the array will be
    inferred in this case.

    Arrays may be of more complicated shape, too, see :method:`.add_array` for details.

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
    """

    __version__ = "0.1.0"
    __hdf_version__ = "0.2.0"

    def __init__(self, num_chunks=1, num_elements=1, **kwargs):
        """
        Create new flattened storage.

        Args:
            num_chunks (int): pre-allocation for per chunk arrays
            num_elements (int): pre-allocation for per elements arrays
        """
        # tracks allocated versed as yet used number of chunks/elements
        self._num_chunks_alloc = self.num_chunks = num_chunks
        self._num_elements_alloc = self.num_elements = num_elements
        # store the starting index for properties with unknown length
        self.current_element_index = 0
        # store the index for properties of known size, stored at the same index as the chunk
        self.current_chunk_index = 0
        # Also store indices of chunk recently added
        self.prev_chunk_index = 0
        self.prev_element_index = 0

        self._init_arrays()

        if len(kwargs) == 0: return

        if len(set(len(chunks) for chunks in kwargs.values())) != 1:
            raise ValueError("Not all initializers provide the same number of chunks!")
        keys = kwargs.keys()
        for chunk_list in zip(*kwargs.values()):
            chunk_length = len(chunk_list[0])
            # values in chunk_list may either be a sequence of chunk_length, scalars (see hasattr check) or a sequence of
            # length 1
            if any(hasattr(c, '__len__') and len(c) != chunk_length and len(c) != 1 for c in chunk_list):
                raise ValueError("Inconsistent chunk length in initializer!")
            self.add_chunk(chunk_length, **{k: c for k, c in zip(keys, chunk_list)})

    def _init_arrays(self):
        self._per_element_arrays = {}

        self._per_chunk_arrays = {
                "start_index": np.empty(self._num_chunks_alloc, dtype=np.int32),
                "length": np.empty(self._num_chunks_alloc, dtype=np.int32),
                "identifier": np.empty(self._num_chunks_alloc, dtype=np.dtype("U20"))
        }

    def __len__(self):
        return self.current_chunk_index

    def find_chunk(self, identifier):
        """
        Return integer index for given identifier.

        Args:
            identifier (str): name of chunk previously passed to :method:`.add_chunk`

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


    def _resize_elements(self, new):
        self._num_elements_alloc = new
        for k, a in self._per_element_arrays.items():
            new_shape = (new,) + a.shape[1:]
            try:
                a.resize(new_shape)
            except ValueError:
                self._per_element_arrays[k] = np.resize(a, new_shape)

    def _resize_chunks(self, new):
        self._num_chunks_alloc = new
        for k, a in self._per_chunk_arrays.items():
            new_shape = (new,) + a.shape[1:]
            try:
                a.resize(new_shape)
            except ValueError:
                self._per_chunk_arrays[k] = np.resize(a, new_shape)

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
            warnings.warn("per=\"structure\" is deprecated, use pr=\"chunk\"",
                          category=DeprecationWarning, stacklevel=2)
        if per == "atom":
            per = "element"
            warnings.warn("per=\"atom\" is deprecated, use pr=\"element\"",
                          category=DeprecationWarning, stacklevel=2)

        if name in self._per_element_arrays:
            a = self._per_element_arrays[name]
            if a.shape[1:] != shape or not np.can_cast(dtype, a.dtype) or per != "element":
                raise ValueError(f"Array with name '{name}' exists with shape {a.shape[1:]} and dtype {a.dtype}.")
            else:
                return

        if name in self._per_chunk_arrays:
            a = self._per_chunk_arrays[name]
            if a.shape[1:] != shape or not np.can_cast(dtype, a.dtype) or per != "chunk":
                raise ValueError(f"Array with name '{name}' exists with shape {a.shape[1:]} and dtype {a.dtype}.")
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
            raise ValueError(f"per must \"element\" or \"chunk\", not {per}")

        if fill is None:
            store[name] = np.empty(shape=shape, dtype=dtype)
        else:
            store[name] = np.full(shape=shape, fill_value=fill, dtype=dtype)

    def get_array(self, name, frame):
        """
        Fetch array for given structure.

        Works for per atom and per arrays.

        Args:
            name (str): name of the array to fetch
            frame (int, str): selects structure to fetch, as in :method:`.get_structure()`

        Returns:
            :class:`numpy.ndarray`: requested array

        Raises:
            `KeyError`: if array with name does not exists
        """

        if isinstance(frame, str):
            frame = self.find_chunk(frame)
        if name in self._per_element_arrays:
            return self._per_element_arrays[name][self._get_per_element_slice(frame)]
        elif name in self._per_chunk_arrays:
            return self._per_chunk_arrays[name][frame]
        else:
            raise KeyError(f"no array named {name}")

    def set_array(self, name, frame, value):
        """
        Add array for given structure.

        Works for per atom and per arrays.

        Args:
            name (str): name of array to set
            frame (int, str): selects structure to set, as in :method:`.get_strucure()`

        Raises:
            `KeyError`: if array with name does not exists
        """

        if isinstance(frame, str):
            frame = self.find_chunk(frame)
        if name in self._per_element_arrays:
            self._per_element_arrays[name][self._get_per_element_slice(frame)] = value
        elif name in self._per_chunk_arrays:
            self._per_chunk_arrays[name][frame] = value
        else:
            raise KeyError(f"no array named {name}")

    def has_array(self, name):
        """
        Checks whether an array of the given name exists and returns meta data given to :method:`.add_array()`.

        >>> container.has_array("energy")
        {'shape': (), 'dtype': np.float64, 'per': 'chunk'}
        >>> container.has_array("fnorble")
        None

        Args:
            name (str): name of the array to check

        Returns:
            None: if array does not exist
            dict: if array exists, keys corresponds to the shape, dtype and per arguments of :method:`.add_array`
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

        .. attention: Edge-case!

            This will not work when the chunk length is also 1 and the array does not exist yet!  In this case the array
            will be assumed to be per element and there is no way around explicitly calling :method:`.add_array()`.


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
            self._resize_chunks(self._num_chunks_alloc * 2)

        if new_elements > self.num_elements:
            self.num_elements = new_elements
        if self.current_chunk_index + 1 > self.num_chunks:
            self.num_chunks += 1

        # len of chunk to index into the initialized arrays
        i = self.current_element_index + n

        self._per_chunk_arrays["start_index"][self.current_chunk_index] = self.current_element_index
        self._per_chunk_arrays["length"][self.current_chunk_index] = n
        self._per_chunk_arrays["identifier"][self.current_chunk_index] = identifier

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
        #return last_chunk_index, last_element_index


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
        hdf["OBJECT"] = self.__class__.__name__

    def to_hdf(self, hdf, group_name="flat_storage"):

        def write_array(name, array, hdf):
            if array.dtype.char == "U":
                # numpy stores unicode data in UTF-32/UCS-4, but h5py wants UTF-8, so we manually encode them here
                # TODO: string arrays with shape != () not handled
                hdf[name] = np.array([s.encode("utf8") for s in array],
                                     # each character in a utf8 string might be encoded in up to 4 bytes, so to
                                     # make sure we can store any string of length n we tell h5py that the
                                     # string will be 4 * n bytes; numpy's dtype does this calculation already
                                     # in itemsize, so we don't need to repeat it here
                                     # see also https://docs.h5py.org/en/stable/strings.html
                                     dtype=h5py.string_dtype('utf8', array.dtype.itemsize))
            else:
                hdf[name] = array

        # truncate arrays to necessary size before writing
        self._resize_elements(self.num_elements)
        self._resize_chunks(self.num_chunks)

        with hdf.open(group_name) as hdf_s_lst:
            self._type_to_hdf(hdf_s_lst)
            hdf_s_lst["num_elements"] =  self._num_elements_alloc
            hdf_s_lst["num_chunks"] = self._num_chunks_alloc

            hdf_arrays = hdf_s_lst.open("element_arrays")
            for k, a in self._per_element_arrays.items():
                write_array(k, a, hdf_arrays)

            hdf_arrays = hdf_s_lst.open("chunk_arrays")
            for k, a in self._per_chunk_arrays.items():
                write_array(k, a, hdf_arrays)

    def from_hdf(self, hdf, group_name="flat_storage"):

        def read_array(name, hdf):
            a = np.array(hdf[name])
            if a.dtype.char == "S":
                # if saved as bytes, we wrote this as an encoded unicode string, so manually decode here
                # TODO: string arrays with shape != () not handled
                a = np.array([s.decode("utf8") for s in a],
                            # itemsize of original a is four bytes per character, so divide by four to get
                            # length of the orignal stored unicode string; np.dtype('U1').itemsize is just a
                            # platform agnostic way of knowing how wide a unicode charater is for numpy
                            dtype=f"U{a.dtype.itemsize//np.dtype('U1').itemsize}")
            return a

        with hdf.open(group_name) as hdf_s_lst:
            version = hdf_s_lst.get("HDF_VERSION", "0.0.0")
            try:
                num_chunks = hdf_s_lst["num_chunks"]
                num_elements = hdf_s_lst["num_elements"]
            except ValueError:
                num_chunks = hdf_s_lst["num_structures"]
                num_elements = hdf_s_lst["num_atoms"]

            self._num_chunks_alloc = self.num_chunks = self.current_chunk_index = num_chunks
            self._num_elements_alloc = self.num_elements = self.current_element_index = num_elements

            if version == "0.1.0":
                with hdf_s_lst.open("arrays") as hdf_arrays:
                    for k in hdf_arrays.list_nodes():
                        a = read_array(k, hdf_arrays)
                        if a.shape[0] == self._num_elements_alloc:
                            self._per_element_arrays[k] = a
                        elif a.shape[0] == self._num_chunks_alloc:
                            self._per_chunk_arrays[k] = a
            elif version == "0.2.0":
                with hdf_s_lst.open("element_arrays") as hdf_arrays:
                    for k in hdf_arrays.list_nodes():
                        self._per_element_arrays[k] = read_array(k, hdf_arrays)
                with hdf_s_lst.open("chunk_arrays") as hdf_arrays:
                    for k in hdf_arrays.list_nodes():
                        self._per_chunk_arrays[k] = read_array(k, hdf_arrays)
            else:
                raise RuntimeError(f"Unsupported HDF version {version}; use an older version of pyiron to load this job!")

            for k, a in self._per_chunk_arrays.items():
                if a.shape[0] != self._num_chunks_alloc:
                    raise RuntimeError(f"per-chunk array {k} read inconsistently from HDF: "
                                       f"shape {a.shape[0]} does not match global allocation {self._num_chunks_alloc}!")
            for k, a in self._per_element_arrays.items():
                if a.shape[0] != self._num_elements_alloc:
                    raise RuntimeError(f"per-element array {k} read inconsistently from HDF: "
                                       f"shape {a.shape[0]} does not match global allocation {self._num_elements_alloc}!")
