# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Classes defining the shape of pyiron's database tables.
"""

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Table,
)
from abc import ABC

__author__ = "Liam Huber, Murat Han Celik"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH"
    " - Computational Materials Design (CM) Department"
)
__version__ = "0.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "development"
__date__ = "Mar 24, 2021"


class PyironTable(ABC):
    def __init__(self, table_name, metadata):
        self._table = None

    @property
    def table(self):
        return self._table


class ObjectTable(PyironTable):
    """
    A table for storing pyiron objects. Only very primitive metadata is stored and more complex queries should use
    a `PyironTable`.
    """
    def __init__(self, table_name, metadata):
        super().__init__(table_name, metadata)
        self._table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),  # Globally unique
            Column("location", String(511)),  # How expensive would it be to make this even bigger???
            Column("name", String(50)),  # Unique within location
            # Note: location+name provides exact location of serialized data
            Column("pyiron", String(20)),  # e.g. pyiron_base, pyiron_contrib
            Column("version", String(20)),  # Which version of pyiron used
            Column("class", String(255)),  # Where to locate the class within pyiron
            # e.g.
            # pyiron=pyiron_base, class=ScriptJob; pyiron=pyiron_atomistics, class=not.a.thing.in.init.ClassName; etc.
            # pyiron+version+class provides all necessary information to re-instantiate the right object
            # (pyiron+version+class)+(location+name) gives all necessary information to fully recreate object instance
            Column("created", DateTime),  # When the object was first saved
            Column("status", String(20)),  # For data-like objects always simple like "created", for job-like is complex
            Column("username", String(20)),  # Who's doing this (for access filtering)
            extend_existing=True,
        )


class RelationTable(PyironTable):
    """
    A table for storing relational links between pyiron objects. Only the existence of the link is stored, the nature
    of the link must be found by examining the pyiron object itself.
    """
    def __init__(self, table_name, metadata):
        super().__init__(table_name, metadata)
        self._table = Table(
            table_name,
            metadata,
            Column("id1", Integer),
            Column("id2", Integer),
            extend_existing=True,
        )
