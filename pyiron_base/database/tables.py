# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Classes defining the shape of pyiron's database tables.
"""

from abc import ABC, abstractmethod
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Table,
    MetaData
)
from typing import Type
# from pyiron_base.job.generic import GenericJob
from datetime import datetime


__author__ = "Murat Han Celik, Liam Huber"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH"
    " - Computational Materials Design (CM) Department"
)
__version__ = "0.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "development"
__date__ = "Sep, 2021"


class TableManager(ABC):
    """
    A parent class for pyiron database tables.
    """

    def __init__(self, table_name, metadata, extend_existing=True):
        self._table = self._create_table(table_name, metadata, extend_existing=extend_existing)

    def add(self, obj, settings):
        """Add data from an object to the database table."""
        par_dict = self._object_to_entry(obj, settings)
        par_dict = dict(
            (key.lower(), value) for key, value in par_dict.items()
        )
        return self._table.insert(par_dict)

    @property
    def table(self) -> Table:
        return self._table

    @abstractmethod
    def _create_table(self, table_name: str, metadata: MetaData, extend_existing: bool=True) -> Table:
        """Initialize a `Table` with all the columns you want."""
        pass

    @abstractmethod
    def _object_to_entry(self, obj, settings) -> dict:
        """Parse an object into a database entry."""
        pass


class HistoricalTable(TableManager):
    """The historical table."""

    def _create_table(self, table_name, metadata, extend_existing=True):
        return Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("parentid", Integer),
            Column("masterid", Integer),
            Column("projectpath", String(50)),
            Column("project", String(255)),
            Column("job", String(50)),
            Column("subjob", String(255)),
            Column("chemicalformula", String(50)),
            Column("status", String(20)),
            Column("hamilton", String(20)),
            Column("hamversion", String(50)),
            Column("username", String(20)),
            Column("computer", String(100)),
            Column("timestart", DateTime),
            Column("timestop", DateTime),
            Column("totalcputime", Float),
            extend_existing=extend_existing
        )

    def _object_to_entry(self, obj, settings) -> dict:  # : Type[GenericJob] circular import issues
        par_dict = {
            "username": settings.login_user,
            "projectpath": obj.project_hdf5.root_path,
            "project": obj.project_hdf5.project_path,
            "job": obj.job_name,
            "subjob": obj.project_hdf5.h5_path,
            "hamversion": obj.version,
            "hamilton": obj.__name__,
            "status": obj.status.string,
            "computer": obj._db_server_entry(),
            "timestart": datetime.now(),
            "masterid": obj.master_id,
            "parentid": obj.parent_id,
            "chemicalformula": obj.chemical_formula
        }
        return self._check_chem_formula_length(par_dict)

    def _check_chem_formula_length(self, par_dict):
        """
        performs a check whether the length of chemical formula exceeds the defined limit
        args:
        par_dict(dict): dictionary of the parameter
        limit(int): the limit for the length of checmical formular
        """
        key_limited = 'chemicalformula'
        if key_limited in par_dict.keys() and \
                par_dict[key_limited] is not None and \
                len(par_dict[key_limited]) > self.table.columns.chemicalformula.type.length:
            par_dict[key_limited] = "OVERFLOW_ERROR"
        return par_dict
