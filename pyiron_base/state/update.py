# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
Functions to update existing pyiron installations - mainly modify the database columns.
"""

from pyiron_base.database.sqlcolumnlength import (
    HAMVERSION_STR_LENGTH,
    JOB_STR_LENGTH,
    PROJECT_PATH_STR_LENGTH,
    SUBJOB_STR_LENGTH,
)
from pyiron_base.state import state

__author__ = "Joerg Neugebauer, Jan Janssen"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


def database():
    """
    Convenience function to update an existing (older) version of the database to the latest version, by modifying the
    database columns. This is only possible if no other pyiron session is accessing the database. Therefore the script
    might take some time to be executed successfully.
    """
    state.database.open_connection()
    db = state.database.database
    try:
        if "projectPath".lower() not in db.get_table_headings(db.table_name):
            print("add missing column: " + "projectPath")
            db.add_column(
                col_name="projectPath",
                col_type="varchar(" + PROJECT_PATH_STR_LENGTH + ")",
            )
        if "subJob".lower() not in db.get_table_headings(db.table_name):
            print("add missing column: " + "subJob")
            db.add_column(
                col_name="subJob", col_type="varchar(" + SUBJOB_STR_LENGTH + ")"
            )
        else:
            print("change data type of subJob")
            db.change_column_type(
                col_name="subJob", col_type="varchar(" + SUBJOB_STR_LENGTH + ")"
            )
        if "masterID".lower() not in db.get_table_headings(db.table_name):
            print("add missing column: " + "masterid")
            db.add_column(col_name="masterid", col_type="bigint")

        if "hamversion" in db.get_table_headings(db.table_name):
            print("change data type hamversion")
            db.change_column_type(
                col_name="hamversion", col_type="varchar(" + HAMVERSION_STR_LENGTH + ")"
            )

        if "job" in db.get_table_headings(db.table_name):
            print("change data type job")
            db.change_column_type(
                col_name="job", col_type="varchar(" + JOB_STR_LENGTH + ")"
            )
        print(db.table_name, " - database successful updated")
    except ValueError:
        print(db.table_name, " - database failed")

    print("database update done")


if __name__ == "__main__":
    database()
