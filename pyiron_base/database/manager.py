# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
A class for mediating connections to SQL databases.
"""

from pyiron_base.generic.util import Singleton
from pyiron_base.settings.generic import Settings
from pyiron_base.database.generic import DatabaseAccess
import os

s = Settings()

__author__ = "Jan Janssen, Liam Huber"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH"
    " - Computational Materials Design (CM) Department"
)
__version__ = "0.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "development"
__date__ = "Sep 24, 2021"


class DatabaseManager(metaclass=Singleton):
    def __init__(self):
        self._database = None
        self._use_local_database = False
        self._database_is_disabled = s.configuration["disable_database"]

    @property
    def database(self):
        return self._database

    @property
    def using_local_database(self):
        return self._use_local_database

    @property
    def database_is_disabled(self):
        return self._database_is_disabled

    @property
    def project_check_enabled(self):
        if self.database_is_disabled:
            return False
        else:
            return s.configuration["project_check_enabled"]

    @property
    def connection_timeout(self):
        """
        Get the connection timeout in seconds.  Zero means close the database after every connection.

        Returns:
            int: timeout in seconds
        """
        return s.configuration["connection_timeout"]

    @connection_timeout.setter
    def connection_timeout(self, val):
        s.configuration["connection_timeout"] = val

    def open_connection(self):
        """
        Internal function to open the connection to the database. Only after this function is called the database is
        accessable.
        """
        if self._database is None and not self.database_is_disabled:
            self._database = DatabaseAccess(
                s.configuration["sql_connection_string"],
                s.configuration["sql_table_name"],
                timeout=s.configuration["connection_timeout"]
            )

    def switch_to_local_database(self, file_name="pyiron.db", cwd=None):
        """
        Swtich to an local SQLite based database.

        Args:
            file_name (str): SQLite database file name
            cwd (str/None): directory where the SQLite database file is located in
        """
        if self.using_local_database:
            s.logger.log("Database is already in local mode or disabled!")
        else:
            if cwd is None and not os.path.isabs(file_name):
                file_name = os.path.join(os.path.abspath(os.path.curdir), file_name)
            elif cwd is not None:
                file_name = os.path.join(cwd, file_name)
            self.close_connection()
            self.open_local_sqlite_connection(connection_string="sqlite:///" + file_name)

    def open_local_sqlite_connection(self, connection_string):
        self._database = DatabaseAccess(connection_string, s.configuration["sql_table_name"])
        self._use_local_database = True
        self._database_is_disabled = False

    def switch_to_central_database(self):
        """
        Switch to central database
        """
        if self._use_local_database:
            self.close_connection()
            self._database_is_disabled = s.configuration["disable_database"]
            if self.database_is_disabled:
                self._database = None
            else:
                self._database = DatabaseAccess(
                    s.configuration["sql_connection_string"],
                    s.configuration["sql_table_name"],
                )

            self._use_local_database = False
        else:
            s.logger.log("Database is already in central mode or disabled!")

    def switch_to_viewer_mode(self):
        """
        Switch from user mode to viewer mode - if viewer_mode is enable pyiron has read only access to the database.
        """
        if s.configuration["sql_view_connection_string"] is not None and not self.database_is_disabled:
            if self._database.viewer_mode:
                s.logger.log("Database is already in viewer mode!")
            else:
                self.close_connection()
                self._database = DatabaseAccess(
                    s.configuration["sql_view_connection_string"],
                    s.configuration["sql_view_table_name"],
                )
                self._database.viewer_mode = True

        else:
            print("Viewer Mode is not available on this pyiron installation.")

    def switch_to_user_mode(self):
        """
        Switch from viewer mode to user mode - if viewer_mode is enable pyiron has read only access to the database.
        """
        if s.configuration["sql_view_connection_string"] is not None and not self.database_is_disabled:
            if self._database.viewer_mode:
                self.close_connection()
                self._database = DatabaseAccess(
                    s.configuration["sql_connection_string"],
                    s.configuration["sql_table_name"],
                )
                self._database.viewer_mode = True
            else:
                s.logger.log("Database is already in user mode!")
        else:
            print("Viewer Mode is not available on this pyiron installation.")

    def close_connection(self):
        """
        Internal function to close the connection to the database.
        """
        if self._database is not None:
            self._database.conn.close()
            self._database = None

    def top_path(self, full_path):
        """
        Validated that the full_path is a sub directory of one of the pyrion environments loaded.

        Args:
            full_path (str): path

        Returns:
            str: path
        """
        if full_path[-1] != "/":
            full_path += "/"

        if not self.project_check_enabled:
            return None

        for path in s.configuration["project_paths"]:
            if path in full_path:
                return path
        raise ValueError(
            "the current path {0} is not included in the .pyiron configuration. {1}".format(
                full_path, s.configuration["project_paths"]
            )
        )
