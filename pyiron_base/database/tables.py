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
    MetaData,
    String,
    Table,
)

from pyiron_base.database.sqlcolumnlength import (
    CHEMICALFORMULA_STR_LENGTH,
    COMPUTER_STR_LENGTH,
    HAMILTON_STR_LENGTH,
    HAMVERSION_STR_LENGTH,
    JOB_STR_LENGTH,
    PROJECT_PATH_STR_LENGTH,
    PROJECT_STR_LENGTH,
    STATUS_STR_LENGTH,
    SUBJOB_STR_LENGTH,
    USERNAME_STR_LENGTH,
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


def get_historical_table(
    table_name: str, metadata: MetaData, extend_existing: bool = True
) -> Table:
    """The historical table."""
    return Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("parentid", Integer),
        Column("masterid", Integer),
        Column("projectpath", String(PROJECT_PATH_STR_LENGTH)),
        Column("project", String(PROJECT_STR_LENGTH)),
        Column("job", String(JOB_STR_LENGTH)),
        Column("subjob", String(SUBJOB_STR_LENGTH)),
        Column("chemicalformula", String(CHEMICALFORMULA_STR_LENGTH)),
        Column("status", String(STATUS_STR_LENGTH)),
        Column("hamilton", String(HAMILTON_STR_LENGTH)),
        Column("hamversion", String(HAMVERSION_STR_LENGTH)),
        Column("username", String(USERNAME_STR_LENGTH)),
        Column("computer", String(COMPUTER_STR_LENGTH)),
        Column("timestart", DateTime),
        Column("timestop", DateTime),
        Column("totalcputime", Float),
        extend_existing=extend_existing,
    )
