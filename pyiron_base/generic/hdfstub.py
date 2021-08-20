"""
Convenience class to lazily read values from HDF.
"""

# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

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


class HDFStub:
    """
    Provides lazy loading of data from HDF.

    Instead of accessing an HDF group directly

    >>> hdf[group_name]
    ...

    you can wrap this with this class

    >>> stub = HDFStub(hdf, group_name)

    and then later perform this lookup with :method:`.load`

    >>> stub.load() == hdf[group_name]
    True

    For simple datatypes there's not a big advantages to this, but :class:`.DataContainer` uses this to load its
    contents lazily and ensure that nested containers are also lazily loaded.  This is done by customizing what happend
    on :method:`.load` via :method:`.register`.  This class method adds a callback to the class that will be called
    when the specified type name is found in the hdf group that is to be loaded.

    >>> hdf['mytype/NAME']
    MyType
    >>> hdf['mytype/TYPE']
    <class 'my.module.MyType'>
    >>> HDFStub.register(MyType, lambda hdf, group: print(42) or hdf[group].to_object())
    >>> my = HDFStub(hdf, 'mytype').load()
    42
    >>> my
    MyType(...)

    This is intended to allow classes that want to be lazily loaded in a certain way to customize what arguments they
    pass `to_object()` (and therefore to their own initializers).
    """

    _load_functions = {}

    def __init__(self, hdf, group_name):
        """
        Create new stub.

        Args:
            hdf (:class:`.ProjectHDFio`): hdf object to load from
            group_name (str): node or group name to load from the hdf object
        """
        self._hdf = hdf
        self._group_name = group_name

    @classmethod
    def register(cls, type, load):
        """
        Register call back for a new type.

        Args:
            type (type): class to be registered
            load (function): callback that is called on :method:`.load` when the type matches `type_name`, must
                             accept `hdf` and `group_name` corresponding to the init parameters of this class and return
                             (lazily) loaded object
        """
        cls._load_functions[str(type)] = load

    def load(self):
        """
        Read value from HDF.

        If `group_name` is a node in HDF, simply its value will be returned.  If it is a group in HDF and the 'NAME'
        node matches any of the types registered with :method:`.register`, it will be loaded with the provided callback.
        Otherwise it will be loaded with :method:`.ProjectHDFio.to_object()`.
        """
        if self._group_name in self._hdf.list_nodes() or 'TYPE' not in self._hdf[self._group_name].list_nodes():
            return self._hdf[self._group_name]

        load = self._load_functions.get(
                self._hdf[self._group_name]['TYPE'],
                lambda h, g: h[g].to_object()
        )
        return load(self._hdf, self._group_name)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._hdf}, {self._group_name})"
