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


class ColumnManager(ABC):
    """
    A parent class for pyiron database columns
    """

    def __init__(
            self,
            column_name,
            column_type,
            object_to_entry_fnc,
            length_limit_is_critical=False,
            column_kwargs=None,
            type_kwargs=None
    ):
        column_kwargs = {} if column_kwargs is None else column_kwargs
        type_kwargs = {} if type_kwargs is None else type_kwargs
        self._name = column_name
        self._column = Column(column_name, column_type(**type_kwargs), **column_kwargs)
        self._object_to_entry = object_to_entry_fnc
        self._length_limit_is_critical = length_limit_is_critical

    @property
    def name(self):
        return self._name

    @property
    def column(self):
        return self._column

    def object_to_entry(self, obj, settings):
        entry = self._object_to_entry(obj, settings)
        return self._check_entry_length(entry)

    def _check_entry_length(self, entry):
        if hasattr(self.column, 'length') and len(entry) > self.column.length:
            if self._length_limit_is_critical:
                raise TooLongDBEntry(
                    key=self.name,
                    limit=self.column.length,
                    val=len(entry)
                )
            else:
                return "OVERFLOW_ERROR"
        else:
            return entry


class TableManager(ABC):
    """
    A parent class for pyiron database tables.
    """

    def __init__(self, table_name, metadata, extend_existing=True):
        self._table = self._create_table(table_name, metadata, extend_existing=extend_existing)

    @property
    @abstractmethod
    def _columns(self):
        """A list of `ColumnManager` objects"""
        pass

    def add(self, obj, settings):
        """Add data from an object to the database table."""
        return self._table.insert(
            {c.name.lower(): c.object_to_entry(obj, settings) for c in self._columns}
        )

    @property
    def table(self) -> Table:
        return self._table

    @property
    def columns(self):
        return [ColumnManager("id", Integer, column_kwargs={'primary_key': True, 'autoincrement': True})] + \
               self._columns

    def _create_table(self, table_name: str, metadata: MetaData, extend_existing: bool=True) -> Table:
        return Table(
            table_name,
            metadata,
            *self._columns,
            extend_existing=extend_existing
        )


def _attribute_or_none_decorator():
    raise NotImplementedError


class HistoricalTable(TableManager):
    """The historical table."""

    @property
    def _columns(self):
        return [
            ColumnManager("parentid", Integer, self._get_parentid),
            ColumnManager("masterid", Integer, self._get_masterid),
            ColumnManager("projectpath", String, self._get_projectpath,
                          type_kwargs={'length': 50}, length_limit_is_critical=True),
            ColumnManager("project", String, self._get_project,
                          type_kwargs={'length': 255}, length_limit_is_critical=True),
            ColumnManager("job", String, self._get_job,
                          type_kwargs={'length': 50}, length_limit_is_critical=True),
            ColumnManager("subjob", String, self._get_subjob,
                          type_kwargs={'length': 250}, length_limit_is_critical=True),
            ColumnManager("chemicalformula", String, self._get_chemicalformula,
                          type_kwargs={'length': 50}),
            ColumnManager("status", String, self._get_status,
                          type_kwargs={'length': 20}, length_limit_is_critical=True),
            ColumnManager("hamilton", String, self._get_hamilton,
                          type_kwargs={'length': 50}, length_limit_is_critical=True),
            ColumnManager("hamversion", String, self._get_hamversion,
                          type_kwargs={'length': 50}, length_limit_is_critical=True),
            ColumnManager("username", String, self._get_username,
                          type_kwargs={'length': 20}, length_limit_is_critical=True),
            ColumnManager("computer", String, self._get_computer,
                          type_kwargs={'length': 100}, length_limit_is_critical=True),
            ColumnManager("timestart", self._get_timestart, DateTime),
            ColumnManager("timestop", self._get_timestop, DateTime),
            ColumnManager("totalcputime", self._get_totalcputime, Float),
        ]

    @staticmethod
    def _get_parentid(obj, settings):
        return obj.parent_id

    @staticmethod
    def _get_masterid(obj, settings):
        return obj.chemical_formula

    @staticmethod
    def _get_username(obj, settings):
        return settings.login_user

    @staticmethod
    def _get_projectpath(obj, settings):
        return obj.project_hdf5.root_path

    @staticmethod
    def _get_project(obj, settings):
        return obj.project_hdf5.project_path

    @staticmethod
    def _get_job(obj, settings):
        return obj.job_name

    @staticmethod
    def _get_subjob(obj, settings):
        return obj.project_hdf5.

    @staticmethod
    def _get_chemicalformula(obj, settings):
        return obj.chemical_formula

    @staticmethod
    def _get_hamilton(obj, settings):
        return obj.__name__

    @staticmethod
    def _get_hamversion(obj, settings):
        return obj.version

    @staticmethod
    def _get_status(obj, settings):
        return obj.status.string

    @staticmethod
    def _get_computer(obj, settings):
        return obj._db_server_entry()

    @staticmethod
    def _get_timestart(obj, settings):
        return datetime.now()

    @staticmethod
    def _get_timestop(obj, settings):
        return obj._runtime()['timestop']

    @staticmethod
    def _get_totalcputime(obj, settings):
        return obj._runtime()['totalcputime']
