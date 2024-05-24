# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
DatabaseAccess class deals with accessing the database
"""

import os
import re
import warnings
from datetime import datetime
from queue import Empty as QueueEmpty
from queue import SimpleQueue
from threading import Lock, Thread
from typing import List, Optional, Union

import numpy as np
import pandas
from pyiron_snippets.deprecate import deprecate
from pyiron_snippets.logger import logger
from pyiron_snippets.retry import retry
from sqlalchemy import (
    MetaData,
    Table,
    and_,
    create_engine,
    or_,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError, OperationalError
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select

from pyiron_base.database.interface import IsDatabase
from pyiron_base.database.sqlcolumnlength import CHEMICALFORMULA_STR_LENGTH
from pyiron_base.database.tables import get_historical_table

__author__ = "Murat Han Celik"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH"
    " - Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


class ConnectionWatchDog(Thread):
    """
    Helper class that closes idle connections after a given timeout.

    Initialize it with the connection to watch and a lock that protects it.  The lock prevents the watchdog from killing
    a connection that is currently used.  The timeout is in seconds.

    >>> conn = SqlConnection(...)
    >>> lock = threading.Lock()
    >>> dog = ConnectionWatchDog(conn, lock, timeout=60)

    After it is created, :method:`.kick()` the watchdog periodically before the timeout runs out.  It is important to
    acquire the lock when using the connection object.

    >>> dog.kick()
    >>> with lock:
    ...    conn.execute(...)
    >>> dog.kick()

    Once you want to finish the connection or want to make sure the watchdog quit, call :method:`.kill()` to shut it
    down.  This also causes the watch dog to try and close the connection.

    >>> dog.kill()
    """

    def __init__(self, conn, lock, timeout=60):
        """
        Create new watchdog.

        Args:
            conn: any python object with a `close()` method.
            lock (:class:`threading.Lock`): lock to protect conn
            timeout (int): time in seconds before the watchdog closes the connection.
        """
        super().__init__()
        self._queue = SimpleQueue()
        self._conn = conn
        self._lock = lock
        self._timeout = timeout

    def run(self) -> None:
        """
        Starts the watchdog.
        """
        while True:
            try:
                kicked = self._queue.get(timeout=self._timeout)
            except QueueEmpty:
                kicked = False
            if not kicked:
                with self._lock:
                    try:
                        self._conn.close()
                    except (DatabaseError, OperationalError):
                        pass
                    break

    def kick(self) -> None:
        """
        Restarts the timeout.
        """
        self._queue.put(True)

    def kill(self) -> None:
        """
        Stop the watchdog and close the connection.
        """
        self._queue.put(False)
        self.join()


class AutorestoredConnection:
    def __init__(self, engine: Engine, timeout: int = 60):
        self.engine = engine
        self._conn = None
        self._lock = Lock()
        self._watchdog = None
        self._logger = logger
        self._timeout = timeout

    def execute_once(self, *args, **kwargs):
        with self._lock:
            if self._conn is None or self._conn.closed:
                self._conn = self.engine.connect()
                if self._timeout > 0:
                    # only log reconnections when we keep the connection alive between requests otherwise we'll spam
                    # the log
                    if self._conn is None:
                        self._logger.info(
                            "Reconnecting to DB; connection did not exist."
                        )
                    else:
                        self._logger.info("Reconnecting to DB; connection was closed.")
                    if self._watchdog is not None:
                        # in case connection is dead, but watchdog is still up, something else killed the connection,
                        # make the watchdog quit, then making a new one
                        self._watchdog.kill()
                    self._watchdog = ConnectionWatchDog(
                        self._conn, self._lock, timeout=self._timeout
                    )
                    self._watchdog.start()
            if self._timeout > 0:
                self._watchdog.kick()
            return self._conn.execute(*args, **kwargs)

    def execute(self, *args, **kwargs):
        return retry(
            lambda: self.execute_once(*args, **kwargs),
            error=OperationalError,
            msg="Database connection failed with operational error.",
            delay=5,
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()

    def commit(self) -> None:
        if self._conn is not None:
            self._conn.commit()


class DatabaseAccess(IsDatabase):
    """
    A core element of PyIron, which generally deals with accessing the database: getting, sending, changing some data
    to the db.

    Args:
        connection_string (str): SQLalchemy connection string which specifies the database to connect to
                                 typical form: dialect+driver://username:password@host:port/database
                                 example: 'postgresql://scott:tiger@cmcent56.mpie.de/mdb'
        table_name (str): database table name, a simple string like: 'simulation'

    Murat Han Celik
    """

    def __init__(self, connection_string: str, table_name: str, timeout: int = 60):
        """
        Initialize the Database connection

        Args:
            connection_string (str): SQLalchemy connection string which specifies the database to connect to
                                     typical form: dialect+driver://username:password@host:port/database
                                     example: 'postgresql://scott:tiger@cmcent56.mpie.de/mdb'
            table_name (str): database table name, a simple string like: 'simulation'
            timeout (int): time in seconds before unused database connection are closed
        """
        self.table_name = table_name
        self._keep_connection = False
        self._timeout = timeout
        self._sql_lite = "sqlite" in connection_string
        try:
            if not self._sql_lite:
                self._engine = create_engine(
                    connection_string,
                    connect_args={"connect_timeout": 15},
                    poolclass=NullPool,
                    future=True,
                )
                self.conn = AutorestoredConnection(self._engine, timeout=self._timeout)
                self._keep_connection = self._timeout > 0
            else:
                self._engine = create_engine(connection_string, future=True)
                self.conn = self._engine.connect()
                self.conn.connection.create_function("like", 2, self.regexp)
                self._keep_connection = True
        except Exception as except_msg:
            raise ValueError("Connection to database failed: " + str(except_msg))

        self._chem_formula_lim_length = CHEMICALFORMULA_STR_LENGTH

        def _create_table() -> None:
            self.__reload_db()
            self.simulation_table = get_historical_table(
                table_name=str(table_name), metadata=self.metadata, extend_existing=True
            )
            self.metadata.create_all(bind=self._engine)

        # too many jobs trying to talk to the database can cause this to fail.
        retry(
            _create_table,
            error=OperationalError,
            msg="Database busy with too many connections.",
            at_most=10,
            delay=0.1,
            delay_factor=2,
        )

    def _job_dict(
        self,
        sql_query: str,
        user: str,
        project_path: str,
        recursive: bool,
        job: Optional[str] = None,
        sub_job_name: str = "%",
        element_lst: List[str] = None,
    ) -> List[dict]:
        """
        Internal function to access the database from the project directly.

        Args:
            sql_query (str): SQL query to enter a more specific request
            user (str): username of the user whoes user space should be searched
            project_path (str): root_path - this is in contrast to the project_path in GenericPath
            recursive (bool): search subprojects [True/False]
            job (str): job_name - by default None
            sub_job_name (str): path inside the HDF5 file - "%" by default to accept any path
            element_lst (list): list of elements required in the chemical formular - by default None

        Returns:
            list: the function returns a list of dicts, but it does not format datetime:
                 [{'chemicalformula': u'Ni108',
                  'computer': u'mapc157',
                  'hamilton': u'LAMMPS',
                  'hamversion': u'1.1',
                  'id': 24,
                  'job': u'DOF_1_0',
                  'parentid': 21L,
                  'project': u'lammps.phonons.Ni_fcc',
                  'projectpath': u'D:/PyIron/PyIron_data/projects',
                  'status': u'finished',
                  'timestart': datetime.datetime(2016, 6, 24, 10, 17, 3, 140000),
                  'timestop': datetime.datetime(2016, 6, 24, 10, 17, 3, 173000),
                  'totalcputime': 0.033,
                  'username': u'test'},
                 {'chemicalformula': u'Ni108',
                  'computer': u'mapc157',
                  'hamilton': u'LAMMPS',
                  'hamversion': u'1.1',
                  'id': 21,
                  'job': u'ref',
                  'parentid': 20L,
                  'project': u'lammps.phonons.Ni_fcc',
                  'projectpath': u'D:/PyIron/PyIron_data/projects',
                  'status': u'finished',
                  'timestart': datetime.datetime(2016, 6, 24, 10, 17, 2, 429000),
                  'timestop': datetime.datetime(2016, 6, 24, 10, 17, 2, 463000),
                  'totalcputime': 0.034,
                  'username': u'test'},.......]

        """
        if not self._sql_lite:

            def escape(s, escape_char="\\", special_chars="_%"):
                """Insert escape_char in front of special_chars, unless present.

                Handles the cases where s already contains escaped characters,
                including the escape character itself.

                Defaults for LIKE in SQL statements."""
                for c in special_chars:
                    if c in s:
                        s = s.replace(escape_char + c, c)
                    s = s.replace(c, escape_char + c)
                return s

        else:

            def escape(s, escape_char="\\", special_chars="_%"):
                return s

        dict_clause = {}
        # FOR GET_ITEMS_SQL: clause = []
        if user is not None:
            dict_clause["username"] = str(user)
            # FOR GET_ITEMS_SQL: clause.append("username = '" + self.user + "'")
        if sql_query is not None:
            # FOR GET_ITEMS_SQL: clause.append(self.sql_query)
            if "AND" in sql_query:
                cl_split = sql_query.split(" AND ")
            elif "and" in sql_query:
                cl_split = sql_query.split(" and ")
            else:
                cl_split = [sql_query]
            dict_clause.update(
                {str(element.split()[0]): element.split()[2] for element in cl_split}
            )
        if job is not None:
            dict_clause["job"] = str(job)

        if project_path == "./":
            project_path = ""
        if recursive:
            dict_clause["project"] = escape(str(project_path)) + "%"
        else:
            dict_clause["project"] = str(project_path)
        if sub_job_name is None:
            dict_clause["subjob"] = None
        elif sub_job_name != "%":
            dict_clause["subjob"] = str(sub_job_name)
        if element_lst is not None:
            dict_clause["element_lst"] = element_lst

        logger.debug("sql_query: %s", str(dict_clause))
        return self.get_items_dict(dict_clause)

    def _get_job_table(
        self,
        sql_query: str,
        user: str,
        project_path: str,
        recursive: bool = True,
        columns: List[str] = None,
        element_lst: List[str] = None,
    ) -> pandas.DataFrame:
        job_dict = self._job_dict(
            sql_query=sql_query,
            user=user,
            project_path=project_path,
            recursive=recursive,
            element_lst=element_lst,
        )
        return pandas.DataFrame(job_dict, columns=columns)

    # Internal functions
    def __del__(self) -> None:
        """
        Close database connection

        Returns:

        """
        if not self._keep_connection:
            self.conn.close()

    def __reload_db(self) -> None:
        """
        Reload database

        Returns:

        """
        self.metadata = MetaData()
        self.metadata.reflect(bind=self._engine)

    @staticmethod
    def regexp(expr: str, item: str) -> Union[str, None]:
        """
        Regex function for SQLite
        Args:
            expr: str, regex expression
            item: str, item which needs to be checked

        Returns:

        """
        expr = expr.replace("%", "(.)*")
        expr = expr.replace("_", ".")
        expr = "^" + expr
        if expr[-1] != "%":
            expr += "$"
        reg = re.compile(expr)
        if item is not None:
            return reg.search(item) is not None

    # Table functions
    def _get_table_headings(self, table_name: Optional[str] = None) -> List[str]:
        """
        Get column names

        Args:
            table_name (str): simple string of a table_name like: 'jobs_username'

        Returns:
            list: list of column names like:
                 ['id',
                 'parentid',
                 'masterid',
                 'projectpath',
                 'project',
                 'job',
                 'subjob',
                 'chemicalformula',
                 'status',
                 'hamilton',
                 'hamversion',
                 'username',
                 'computer',
                 'timestart',
                 'timestop',
                 'totalcputime']
        """
        if table_name is None:
            table_name = self.table_name
        self.__reload_db()
        try:
            simulation_list = Table(
                str(table_name),
                self.metadata,
                autoload_with=self._engine,
            )
        except Exception:
            raise ValueError(str(table_name) + " does not exist")
        return [column.name for column in iter(simulation_list.columns)]

    def add_column(
        self, col_name: Union[str, List[str]], col_type: Union[str, List[str]]
    ) -> None:
        """
        Add an additional column - required for modification on the database

        Args:
            col_name (str, list): name of the new column, normal string like: 'myColumn'
            col_type (str, list: SQL type of the new column, SQL type like: 'varchar(50)'

        Returns:

        """
        if isinstance(col_name, list):
            col_name = col_name[-1]
        if isinstance(col_type, list):
            col_type = col_type[-1]
        self.conn.execute(
            text(
                "ALTER TABLE %s ADD COLUMN %s %s"
                % (self.simulation_table.name, col_name, col_type)
            )
        )
        self.conn.commit()

    def change_column_type(
        self, col_name: Union[str, List[str]], col_type: Union[str, List[str]]
    ) -> None:
        """
        Modify data type of an existing column - required for modification on the database

        Args:
            col_name (str, list): name of the new column, normal string like: 'myColumn'
            col_type (str, list: SQL type of the new column, SQL type like: 'varchar(50)'

        Returns:

        """
        if isinstance(col_name, list):
            col_name = col_name[-1]
        if isinstance(col_type, list):
            col_type = col_type[-1]
        self.conn.execute(
            text(
                "ALTER TABLE %s ALTER COLUMN %s TYPE %s"
                % (self.simulation_table.name, col_name, col_type)
            )
        )
        self.conn.commit()

    def _check_chem_formula_length(self, par_dict: dict) -> dict:
        """
        performs a check whether the length of chemical formula exceeds the defined limit

        Args:
            par_dict(dict): dictionary of the parameters to be checked
        """
        key_limited = "ChemicalFormula"
        if (
            key_limited in par_dict.keys()
            and par_dict[key_limited] is not None
            and len(par_dict[key_limited]) > self._chem_formula_lim_length
        ):
            par_dict[key_limited] = "OVERFLOW_ERROR"
        return par_dict

    def _check_duplidates(self, par_dict: dict) -> bool:
        """
        Check for duplicates in the database

        Args:
            par_dict (dict): Dictionary with the item values and column names as keys
        """
        return (
            len(
                self.get_items_dict(
                    {"job": par_dict["job"], "project": par_dict["project"]}
                )
            )
            > 0
        )

    # Item functions
    def add_item_dict(self, par_dict: dict, check_duplicates: bool = False) -> int:
        """
        Create a new database item

        Args:
            par_dict (dict): Dictionary with the item values and column names as keys, like:
                              {'chemicalformula': 'BO',
                             'computer': 'localhost',
                             'hamilton': 'VAMPS',
                             'hamversion': '1.1',
                             'job': 'testing',
                             'subjob' : 'SubJob',
                             'parentid': 0L,
                             'myCol': 'Blubbablub',
                             'project': 'database.testing',
                             'projectpath': '/root/directory/tmp',
                             'status': 'KAAAA',
                             'timestart': datetime(2016, 5, 2, 11, 31, 4, 253377),
                             'timestop': datetime(2016, 5, 2, 11, 31, 4, 371165),
                             'totalcputime': 0.117788,
                             'username': 'Test'}
            check_duplicates (bool): Check for duplicate entries in the database

        Returns:
            int: Database ID of the item created as an int, like: 3
        """
        try:
            if check_duplicates and self._check_duplidates(par_dict):
                warnings.warn(f"Duplicate entry found in database: {par_dict}")
                return None
            par_dict = self._check_chem_formula_length(par_dict)
            par_dict = dict(
                (key.lower(), value) for key, value in par_dict.items()
            )  # make keys lowercase
            result = self.conn.execute(
                self.simulation_table.insert().values(**par_dict)
            ).inserted_primary_key[-1]
            self.conn.commit()
            if not self._keep_connection:
                self.conn.close()
            return result
        except Exception as except_msg:
            raise ValueError("Error occurred: " + str(except_msg))

    def __get_items(self, col_name: str, var: Union[str, int]) -> List[dict]:
        """
        Get multiple items from the database

        Args:
            col_name (str): column to query for, like :  'id'
            var (str, int): value of the specific column, like: '2'

        ----> __get_items('id', '2')

        Returns:
            dict: Dictionary where the key is the column name, like:
                    [{'chemicalformula': u'BO',
                      'computer': u'computer',
                      'hamilton': u'VAMPS',
                      'hamversion': u'1.1',
               ------>'id': 2,
                      'job': u'testing',
                      'parentid': 0,
                      'project': u'database.testing',
                      'projectpath': u'/root/directory/tmp',
                      'samucol': None,
                      'status': u'Testing',
                      'timestart': datetime.datetime(2016, 5, 2, 11, 31, 4, 253377),
                      'timestop': datetime.datetime(2016, 5, 2, 11, 31, 4, 371165),
                      'totalcputime': 0.117788,
                      'username': u'Test'}]
        """
        try:
            if isinstance(var, list):
                var = var[-1]
            query = select(self.simulation_table).where(
                self.simulation_table.c[str(col_name)] == var
            )
        except Exception:
            raise ValueError("There is no Column named: " + col_name)
        try:
            result = self.conn.execute(query)
        except (OperationalError, DatabaseError):
            if not self._sql_lite:
                self.conn = AutorestoredConnection(self._engine)
            else:
                self.conn = self._engine.connect()
                self.conn.connection.create_function("like", 2, self.regexp)
            result = self.conn.execute(query)
        row = result.fetchall()
        if not self._keep_connection:
            self.conn.close()
        return [dict(zip(col._mapping.keys(), col._mapping.values())) for col in row]

    def _item_update(self, par_dict: dict, item_id: int) -> None:
        """
        Modify Item in database

        Args:
            par_dict (dict): Dictionary of the parameters to be modified,, where the key is the column name.
                            {'job' : 'maximize',
                             'subjob' : 'testing',
                             ........}
            item_id (int, list): Database Item ID (Integer) - '38'  can also be [38]

        Returns:

        """
        if np.issubdtype(type(item_id), np.integer):
            item_id = int(item_id)
        # all items must be lower case, ensured here
        par_dict = dict((key.lower(), value) for key, value in par_dict.items())
        query = (
            self.simulation_table.update()
            .where(self.simulation_table.c["id"] == item_id)
            .values()
        )
        try:
            self.conn.execute(query, par_dict)
            self.conn.commit()
        except (OperationalError, DatabaseError):
            if not self._sql_lite:
                self.conn = AutorestoredConnection(self._engine)
            else:
                self.conn = self._engine.connect()
                self.conn.connection.create_function("like", 2, self.regexp)

            self.conn.execute(query, par_dict)
            self.conn.commit()
        if not self._keep_connection:
            self.conn.close()

    def delete_item(self, item_id: int) -> None:
        """
        Delete Item from database

        Args:
            item_id (int): Databse Item ID (Integer), like: 38

        Returns:

        """

        res = self.conn.execute(
            self.simulation_table.delete().where(
                self.simulation_table.c["id"] == int(item_id)
            )
        )
        if res.rowcount == 0:
            raise RuntimeError(f"Failed to delete job ({item_id}) from database!")
        self.conn.commit()

        if not self._keep_connection:
            self.conn.close()

    # IsDatabase impl'
    def _get_jobs(
        self,
        sql_query: str,
        user: str,
        project_path: str,
        recursive: bool = True,
        columns: Optional[List[str]] = None,
    ) -> List[dict]:
        df = self.job_table(
            sql_query=sql_query,
            user=user,
            project_path=project_path,
            recursive=recursive,
            columns=columns,
        )
        if len(df) == 0:
            return {key: list() for key in columns}
        return df.to_dict(orient="list")

    # Shortcut
    def get_item_by_id(self, item_id: int) -> dict:
        """
        Get item from database by searching for a specific item Id.

        Args:
            item_id (int): Databse Item ID (Integer), like: 38

        Returns:
            dict: Dictionary where the key is the column name, like:
                    {'chemicalformula': u'BO',
                     'computer': u'localhost',
                     'hamilton': u'VAMPS',
                     'hamversion': u'1.1',
                     'id': 1,
                     'job': u'testing',
                     'masterid': None,
                     'parentid': 0,
                     'project': u'database.testing',
                     'projectpath': u'/root/directory/tmp',
                     'status': u'KAAAA',
                     'subjob': u'SubJob',
                     'timestart': datetime.datetime(2016, 5, 2, 11, 31, 4, 253377),
                     'timestop': datetime.datetime(2016, 5, 2, 11, 31, 4, 371165),
                     'totalcputime': 0.117788,
                     'username': u'Test'}
        """
        # convert item_id to int type
        # needed since psycopg2 gives otherwise an error for np.int64 type (bigint in database)
        if item_id is None:
            return None
        if isinstance(item_id, (str, float)):
            item_id = int(item_id)
        if np.issubdtype(type(item_id), np.integer):
            try:
                return self.__get_items("id", int(item_id))[-1]
            except TypeError as except_msg:
                raise TypeError(
                    "Wrong data type given as parameter. item_id has to be Integer or String: ",
                    except_msg,
                )
            except IndexError as except_msg:
                raise IndexError(
                    "Error when trying to find elements by given Job ID: ", except_msg
                )
        else:
            raise TypeError("THE SQL database ID has to be an integer.")

    def query_for_element(self, element: str) -> Union[bool, str]:
        return or_(
            *[
                self.simulation_table.c["chemicalformula"].like(
                    "%" + element + "[ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789]%"
                ),
                self.simulation_table.c["chemicalformula"].like("%" + element),
            ]
        )

    def get_items_dict(
        self, item_dict: dict, return_all_columns: bool = True
    ) -> List[dict]:
        """

        Args:
            item_dict (dict): a dict type, which has a certain syntax for this function:
                              a normal dict like {'hamilton': 'VAMPE', 'hamversion': '1.1'} has similarities with a
                              simple query like
                                  select * from table_name where hamilton = 'VAMPE AND hamversion = '1.1'
                              as seen it puts an AND for every key, value combination in the dict and searches for it.

                              another syntax is for an OR statement, simply: {'hamilton': ['VAMPE', 'LAMMPS']}, the
                              query would be:
                                  select * from table_name where hamilton = 'VAMPE' OR hamilton = 'LAMMPS'

                              and lastly for a LIKE statement, simply: {'project': 'database.%'}, the query would be
                                  select * from table_name where project LIKE 'database.%'
                              that means you can simply add the syntax for a like statement like '%' and it will
                              automatically operate a like-search

                              of course you can also use a more complex select method, with everything in use:
                                  {'hamilton': ['VAMPE', 'LAMMPS'],
                                   'project': 'databse%',
                                   'hamversion': '1.1'}
                                  select * from table_name where (hamilton = 'VAMPE' Or hamilton = 'LAMMPS') AND
                                      (project LIKE 'database%') AND hamversion = '1.1'
            return_all_columns (bool): return all columns or only the 'id' - still the format stays the same.

        Returns:
            list: the function returns a list of dicts, but it does not format datetime:
                 [{'chemicalformula': u'Ni108',
                  'computer': u'mapc157',
                  'hamilton': u'LAMMPS',
                  'hamversion': u'1.1',
                  'id': 24,
                  'job': u'DOF_1_0',
                  'parentid': 21L,
                  'project': u'lammps.phonons.Ni_fcc',
                  'projectpath': u'D:/PyIron/PyIron_data/projects',
                  'status': u'finished',
                  'timestart': datetime.datetime(2016, 6, 24, 10, 17, 3, 140000),
                  'timestop': datetime.datetime(2016, 6, 24, 10, 17, 3, 173000),
                  'totalcputime': 0.033,
                  'username': u'test'},
                 {'chemicalformula': u'Ni108',
                  'computer': u'mapc157',
                  'hamilton': u'LAMMPS',
                  'hamversion': u'1.1',
                  'id': 21,
                  'job': u'ref',
                  'parentid': 20L,
                  'project': u'lammps.phonons.Ni_fcc',
                  'projectpath': u'D:/PyIron/PyIron_data/projects',
                  'status': u'finished',
                  'timestart': datetime.datetime(2016, 6, 24, 10, 17, 2, 429000),
                  'timestop': datetime.datetime(2016, 6, 24, 10, 17, 2, 463000),
                  'totalcputime': 0.034,
                  'username': u'test'},.......]
        """
        if not isinstance(item_dict, dict):
            raise TypeError("Wrong DataType! Only Dicts are usable!")
        and_statement = []  # list for the whole sqlalchemy statement
        # here we go through all keys and values of item_dict
        for key, value in item_dict.items():
            # if a value of item_dict is a list, we have to make an or statement of it
            if key == "element_lst":
                part_of_statement = [
                    self.query_for_element(element=element) for element in value
                ]
            elif isinstance(value, list):
                or_statement = [
                    (
                        self.simulation_table.c[str(key)] == element
                        if "%" not in element
                        else self.simulation_table.c[str(key)].like(element)
                    )
                    for element in value
                ]
                # here we wrap the given values in an sqlalchemy-type or_statement
                part_of_statement = [or_(*or_statement)]
            else:
                if "%" not in str(value):
                    part_of_statement = [self.simulation_table.c[str(key)] == value]
                else:
                    part_of_statement = [self.simulation_table.c[str(key)].like(value)]
            # here all statements are wrapped together for the and statement
            and_statement += part_of_statement
        if return_all_columns:
            query = select(self.simulation_table).where(and_(*and_statement))
        else:
            query = select(self.simulation_table.columns["id"]).where(
                and_(*and_statement)
            )
        try:
            result = self.conn.execute(query)
        except (OperationalError, DatabaseError):
            if not self._sql_lite:
                self.conn = AutorestoredConnection(self._engine)
            else:
                self.conn = self._engine.connect()
                self.conn.connection.create_function("like", 2, self.regexp)

            result = self.conn.execute(query)
        results = [row._asdict() for row in result.fetchall()]
        if not self._keep_connection:
            self.conn.close()
        return results

    def get_job_status(self, job_id: int) -> Union[str, None]:
        try:
            return self.get_item_by_id(item_id=job_id)["status"]
        except KeyError:
            return None

    def get_job_working_directory(self, job_id: int) -> Union[str, None]:
        try:
            db_entry = self.get_item_by_id(job_id)
            if db_entry:
                job_name = db_entry["subjob"][1:]
                return os.path.join(
                    db_entry["projectpath"],
                    db_entry["project"],
                    job_name + "_hdf5",
                    job_name,
                )
            else:
                return None
        except KeyError:
            return None
