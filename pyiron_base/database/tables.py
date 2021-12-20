# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Classes defining the shape of pyiron's database tables.
"""

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Table,
)

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


class HistoricalTable(Table):
    """The historical table."""

    def _init(self, table_name, metadata, *args, extend_existing=True, **kwargs):
        super()._init(
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
            extend_existing=extend_existing,
        )
