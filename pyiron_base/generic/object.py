# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
The core class in pyiron, linking python to the database to file storage.
"""

from abc import ABC
from pyiron_base.generic.datacontainer import DataContainer
from pyiron_base.interfaces.has_hdf import HasHDF
from pyiron_base.generic.hdfio import ProjectHDFio
from pyiron_base.state import state

__author__ = "Liam Huber"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "0.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "development"
__date__ = "Mar 23, 2021"


class HasStorage(HasHDF, ABC):
    """
    A base class for objects that use HDF5 data serialization via the `DataContainer` class.
    """

    def __init__(self, *args, **kwargs):
        self._storage = DataContainer(table_name="storage")

    @property
    def storage(self) -> DataContainer:
        return self._storage

    def _to_hdf(self, hdf: ProjectHDFio):
        self.storage.to_hdf(hdf=hdf)

    def _from_hdf(self, hdf: ProjectHDFio, version: str = None):
        self.storage.from_hdf(hdf=hdf)


class HasDatabase(ABC):
    """
    A base class for objects that are registered in pyiron's database

    # TODO: Flesh this class out so it actually gives us a good link to the database!
    """

    def __init__(self, *args, **kwargs):
        if not state.database.database_is_disabled:
            state.database.open_connection()
            self._database = state.database.database
        else:
            raise NotImplementedError("WIP. For now only allowed with a database")

    @property
    def database(self):
        return self._database

    def save(self):
        raise NotImplementedError(
            "WIP. Saving should make sure you're registered with the database."
        )


class PyironObject(HasStorage, HasDatabase, ABC):
    """
    The fundamental pyiron object bringing together python objects, database identification, and data serialziation.
    """

    def __init__(self, *args, **kwargs):
        HasStorage.__init__(self)
        HasDatabase.__init__(self)
