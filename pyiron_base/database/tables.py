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


class TooLongDBEntry(Exception):
    """
    Error: the value given as database exceeds the expected length
    args:
    key(str) : the column name which has the too long value
    limit(int): the expected limit in the number of characters
    val: the value given as the db entry
    """
    def __init__(self, key, limit, val):
        message = f"Error: the value for the {key} in the database exceed the limit of {limit}" \
                  f"the given value for the key: {val}"
        super(TooLongDBEntry, self).__init__(message)


class TableManager(ABC):
    """
    A parent class for pyiron database tables.
    """

    def __init__(self, table_name, metadata, extend_existing=True):
        self._char_limit = {}
        self._update_char_limit()
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
    def _update_char_limit(self) -> dict:
        pass

    @property
    @abstractmethod
    def _restrictively_limited_size_keys(self):
        pass

    @abstractmethod
    def _create_table(self, table_name: str, metadata: MetaData, extend_existing: bool=True) -> Table:
        """Initialize a `Table` with all the columns you want."""
        pass

    @abstractmethod
    def _object_to_entry(self, obj, settings) -> dict:
        """Parse an object into a database entry."""
        pass

    @abstractmethod
    def _check_char_limits(self, par_dict) -> dict:
        """ checks the character limits of each column"""
        pass


class HistoricalTable(TableManager):
    """The historical table."""
    def __init__(self, table_name, metadata, extend_existing=True):
        super().__init__(table_name, metadata, extend_existing=extend_existing)

    def _update_char_limit(self):
        self._char_limit.update({'projectpath': 50, 'project': 255, 'job': 50, 'subjob': 250,
                                 'chemicalformula': 50, 'status': 20, 'hamilton': 50, 'hamversion': 50,
                                 'username': 20, 'computer': 100})

    @property
    def _restrictively_limited_size_keys(self):
        return ['projectpath', 'project', 'job', 'subjob', 'hamilton', 'hamversion', 'username', 'computer']

    def _create_table(self, table_name, metadata, extend_existing=True):
        return Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("parentid", Integer),
            Column("masterid", Integer),
            Column("projectpath", String(self._char_limit["projectpath"])),
            Column("project", String(self._char_limit["project"])),
            Column("job", String(self._char_limit["job"])),
            Column("subjob", String(self._char_limit["subjob"])),
            Column("chemicalformula", String(self._char_limit["chemicalformula"])),
            Column("status", String(self._char_limit["status"])),
            Column("hamilton", String(self._char_limit["hamilton"])),
            Column("hamversion", String(self._char_limit["hamversion"])),
            Column("username", String(self._char_limit["username"])),
            Column("computer", String(self._char_limit["computer"])),
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
        return self._check_char_limits(par_dict)

    def _check_char_limits(self, par_dict):
        """
        performs a check whether the length of db entries do not exceed the defined limit
        args:
        par_dict(dict): dictionary of the parameter
        """
        for key in par_dict.keys():
            if par_dict[key] is not None and \
                    len(par_dict[key]) > self._char_limit[key.lower()]:
                if key.lower() not in self._restrictively_limited_size_keys:
                    par_dict[key] = "OVERFLOW_ERROR"
                else:
                    raise TooLongDBEntry(key=key, limit=self._char_limit[key.lower()], val=par_dict[key])
        return par_dict
