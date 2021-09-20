# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""Interface for classes to serialize to HDF5."""

from pyiron_base.generic.hdfio import ProjectHDFio

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
    """
    Mixin class for objects that can write themselves to HDF.

    Subclasses must implement :meth:`._from_hdf`, :meth:`._to_hdf` and :meth:`_get_hdf_group_name`.  They may implement
    :meth:`.from_hdf_args`.

    :meth:`from_hdf` and :meth:`to_hdf` shall respect the given `group_name` in the following way.  If either the
    argument or the method :meth:`_get_hdf_group_name` returns not `None` they shall create a new subgroup in the given
    HDF object and then call :meth:`_from_hdf` or :meth:`_to_hdf` with this subgroup and afterwards call
    :meth:`ProjectHDFio.close` on it.  If both are `None` it shall pass the given HDF object unchanged.

    Subclasses that need to read special arguments from HDF before an instance can be created, can overwrite
    :meth:`.from_hdf_args` and return the arguments in a `dict` that can be **kwargs-passed to the `__init__` of the
    subclass.  When loading an object with :class:`ProjectHDFio.to_object` this method is called internally, used to
    create an instance on which then :meth:`.from_hdf` is called.

    Subclasses may specify an :attr:`__hdf_version__` to signal changes in the layout of the data in HDF.
    :meth:`.from_hdf` will read this value and pass it verbatim to the subclasses :meth:`._from_hdf`.  No semantics are
    imposed on this value, but it is usually a three digit version number.

    Here's a toy class that enables writting `list`s to HDF.

    >>> class HDFList(list, HasHDF):
    ...     def _from_hdf(self, hdf, version=None):
    ...         values = []
    ...         for n in hdf.list_nodes():
    ...             if not n.startswith("__index_"): continue
    ...             values.append((int(n.split("__index_")[1]), hdf[n]))
    ...         values = sorted(values, key=lambda e: e[0])
    ...         self.clear()
    ...         self.extend(list(zip(*values))[1])
    ...     def _to_hdf(self, hdf):
    ...         for i, v in enumerate(self):
    ...             hdf[f"__index_{i}"] = v
    ...     def _get_hdf_group_name(self):
    ...         return "list"

    We can use this simply like any other list, but also call the new HDF methods on it after we get an HDF object.

    >>> l = HDFList([1,2,3,4])
    >>> from pyiron_base import Project
    >>> pr = Project('test_foo')
    >>> hdf = pr.create_hdf(pr.path, 'list')

    Since we return "list" in :meth:`._get_hdf_group_name` by default our list gets written into a group of the same
    name.

    >>> l.to_hdf(hdf)
    >>> hdf
    {'groups': ['list'], 'nodes': []}
    >>> hdf['list']
    {'groups': [], 'nodes': ['HDF_VERSION', 'NAME', 'OBJECT', 'TYPE', '__index_0', '__index_1', '__index_2', '__index_3']}

    (Since this is a docstring, actually calling :meth:`ProjectHDFio.to_object()` wont' work, so we'll simulate it.)

    >>> lcopy = HDFList()
    >>> lcopy.from_hdf(hdf)
    >>> lcopy
    [1, 2, 3, 4]

    We can also override the target group name by passing it
    >>> l.to_hdf(hdf, "my_group")
    >>> hdf
    {'groups': ['list', 'my_group'], 'nodes': []}

    >>> hdf.remove_file()
    >>> pr.remove(enable=True)

    .. document private methods
    .. automethod _from_hdf
    .. automethod _to_hdf
    .. automethod _get_hdf_group_name
    """

    __hdf_version__ = "0.1.0"

    @abstractmethod
    def _from_hdf(self, hdf: ProjectHDFio, version: str=None):
        pass

    @abstractmethod
    def _to_hdf(self, hdf: ProjectHDFio):
        pass

    def _get_hdf_group_name(self) -> str:
        return None

    @classmethod
    def from_hdf_args(cls, hdf: ProjectHDFio) -> dict:
        """
        Read arguments for instance creation from HDF5 file.

        Args:
            hdf (ProjectHDFio): HDF5 group object

        Returns:
            dict: arguments that can be **kwarg-passed to cls().
        """
        return {}

    def _store_type_to_hdf(self, hdf: ProjectHDFio):
        hdf["NAME"] = self.__class__.__name__
        hdf["TYPE"] = str(type(self))
        hdf["OBJECT"] = hdf["NAME"]  # unused alias
        if hasattr(self, "__version__"):
            hdf["VERSION"] = self.__version__
        hdf["HDF_VERSION"] = self.__hdf_version__

    def from_hdf(self, hdf: ProjectHDFio, group_name: str=None):
        """
        Read object to HDF.

        If group_name is given descend into subgroup in hdf first.

        Args:
            hdf (:class:`.ProjectHDFio`): HDF group to read from
            group_name (str, optional): name of subgroup
        """
        group_name = group_name if group_name is not None else self._get_hdf_group_name()
        with _WithHDF(hdf, group_name) as hdf:
            version = hdf.get("HDF_VERSION", "0.1.0")
            self._from_hdf(hdf, version=version)

    def to_hdf(self, hdf: ProjectHDFio, group_name: str=None):
        """
        Write object to HDF.

        If group_name is given create a subgroup in hdf first.

        Args:
            hdf (:class:`.ProjectHDFio`): HDF group to write to
            group_name (str, optional): name of subgroup
        """
        group_name = group_name if group_name is not None else self._get_hdf_group_name()
        with _WithHDF(hdf, group_name) as hdf:
            if len(hdf.list_dirs()) > 0 and group_name is None:
                raise ValueError("HDF group must be empty when group_name is not set.")
            self._to_hdf(hdf)
            self._store_type_to_hdf(hdf)

    def rewrite_hdf(self, hdf: ProjectHDFio, group_name: str=None):
        """
        Update the HDF representation.

        If an object is read from an older layout, this will remove the old data and rewrite it in the newest layout.

        Args:
            hdf (:class:`.ProjectHDFio`): HDF group to read/write
            group_name (str, optional): name of subgroup
        """
        with _WithHDF(hdf, group_name) as hdf:
            obj = hdf.to_object()
            hdf.remove_group()
            obj.to_hdf(hdf)
