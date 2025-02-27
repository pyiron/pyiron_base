# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
A class for mediating connections to SQL databases.
"""

import os
from typing import Optional, Union
from urllib.parse import quote_plus

from pyiron_snippets.logger import logger
from pyiron_snippets.singleton import Singleton

from pyiron_base.state.settings import settings as s

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
        self.open_connection()

    @property
    def database(self):
        return self._database

    @property
    def using_local_database(self) -> bool:
        return self._use_local_database

    @property
    def database_is_disabled(self) -> bool:
        return self._database_is_disabled

    @property
    def project_check_enabled(self) -> bool:
        if self.database_is_disabled:
            return False
        else:
            return s.configuration["project_check_enabled"]

    @property
    def connection_timeout(self) -> int:
        """
        Get the connection timeout in seconds.  Zero means close the database after every connection.

        Returns:
            int: timeout in seconds
        """
        return s.configuration["connection_timeout"]

    @connection_timeout.setter
    def connection_timeout(self, val: int) -> None:
        s.configuration["connection_timeout"] = val

    @staticmethod
    def _sqlalchemy_string(prefix: str, user: str, key: str, host: str, database: str):
        key = quote_plus(key)
        return f"{prefix}://{user}:{key}@{host}/{database}"

    def _credentialed_sqalchemy_string(self, prefix: str):
        return self._sqlalchemy_string(
            prefix,
            s.configuration["user"],
            s.configuration["sql_user_key"],
            s.configuration["sql_host"],
            s.configuration["sql_database"],
        )

    @property
    def sql_connection_string(self) -> str:
        sql_type = s.configuration["sql_type"]
        if sql_type == "Postgres":
            return self._credentialed_sqalchemy_string("postgresql")
        elif sql_type == "MySQL":
            return self._credentialed_sqalchemy_string("mysql+pymysql")
        elif sql_type == "SQLalchemy":
            return s.configuration["sql_connection_string"]
        elif sql_type == "SQLite":
            return "sqlite:///" + s.configuration["sql_file"].replace("\\", "/")
        else:
            raise ValueError(
                f"Invalid SQL type {sql_type} -- This should have been caught at input processing, please contact the "
                f"developers"
            )

    @property
    def sql_view_connection_string(self) -> Union[str, None]:
        if s.configuration["sql_view_user"] is None:
            return None
        else:
            return self._sqlalchemy_string(
                "postgresql",
                s.configuration["sql_view_user"],
                s.configuration["sql_view_user_key"],
                s.configuration["sql_host"],
                s.configuration["sql_database"],
            )

    @property
    def sql_table_name(self) -> str:
        return s.configuration["sql_table_name"]

    def open_connection(self) -> None:
        """
        Internal function to open the connection to the database. Only after this function is called the database is
        accessable.
        """
        if self._database is None and not self.database_is_disabled:
            from pyiron_base.database.generic import DatabaseAccess

            self._database = DatabaseAccess(
                self.sql_connection_string,
                self.sql_table_name,
                timeout=self.connection_timeout,
            )

    def switch_to_local_database(
        self, file_name: str = "pyiron.db", cwd: Optional[str] = None
    ) -> None:
        """
        Swtich to an local SQLite based database.

        Args:
            file_name (str): SQLite database file name
            cwd (str/None): directory where the SQLite database file is located in
        """
        if self.using_local_database:
            logger.info("Database is already in local mode or disabled!")
        else:
            if cwd is None and not os.path.isabs(file_name):
                file_name = os.path.join(os.path.abspath(os.path.curdir), file_name)
            elif cwd is not None:
                file_name = os.path.join(cwd, file_name)
            self.close_connection()
            self.open_local_sqlite_connection(
                connection_string="sqlite:///" + file_name
            )

    def open_local_sqlite_connection(self, connection_string: str) -> None:
        from pyiron_base.database.generic import DatabaseAccess

        self._database = DatabaseAccess(connection_string, self.sql_table_name)
        self._use_local_database = True
        self._database_is_disabled = False

    def switch_to_central_database(self) -> None:
        """
        Switch to central database
        """
        if self.using_local_database:
            self.update()
        else:
            logger.info("Database is already in central mode or disabled!")

    def switch_to_viewer_mode(self) -> None:
        """
        Switch from user mode to viewer mode - if view_mode is enable pyiron has read only access to the database.
        """
        logger.info("Viewer Mode is not available any more!")
        raise DeprecationWarning("Viewer Mode is not available any more!")

    def switch_to_user_mode(self) -> None:
        """
        Switch from viewer mode to user mode - if view_mode is enable pyiron has read only access to the database.
        """
        logger.info("Database is already in user mode!")
        raise DeprecationWarning(
            "Viewer Mode is not available any more: already in user_mode!"
        )

    def close_connection(self) -> None:
        """
        Internal function to close the connection to the database.
        """
        if self._database is not None:
            self._database.conn.close()
            self._database = None

    def top_path(self, full_path: str) -> Union[str, None]:
        """
        Validated that the full_path is a sub directory of one of the pyrion environments loaded.

        Args:
            full_path (str): path

        Returns:
            str: path
        """
        full_path = full_path if full_path.endswith("/") else full_path + "/"

        for path in s.configuration["project_paths"]:
            if path in full_path:
                return path

        if self.project_check_enabled:
            raise ValueError(
                f"the current path {full_path} is not included in the .pyiron configuration 'project_paths': "
                f"{s.configuration['project_paths']}"
            )
        else:
            return None

    def update(self) -> None:
        """
        Warning: Database interaction does not have written spec. This method does a thing. It might not be the thing
                 you want.
        """
        self.close_connection()
        self._use_local_database = False
        self._database_is_disabled = s.configuration["disable_database"]
        self.open_connection()


database = DatabaseManager()
