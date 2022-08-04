# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
DatabaseAccess class deals with accessing the database
"""

from pyiron_base.state.logger import logger
from abc import ABC, abstractmethod
import warnings
import numpy as np
import re
import time
import os
from datetime import datetime
from pyiron_base.utils.deprecate import deprecate
import pandas
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    text,
    and_,
    or_,
)
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select
from sqlalchemy.exc import OperationalError, DatabaseError
from threading import Thread, Lock
from queue import SimpleQueue, Empty as QueueEmpty
from pyiron_base.database.tables import HistoricalTable

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


class IsDatabase(ABC):
    """
    Captures common interface for all database types in pyiron, e.g. SQL/SQLite/FileTable.
    """

    @abstractmethod
    def _get_view_mode(self):
        pass

    @property
    def view_mode(self):
        """
        Get view_mode - if view_moded is enable pyiron has read only access to the database.

        Some implementations do not allow to set this value.

        Returns:
            bool: True when view_mode is enabled
        """
        return self._get_view_mode()

    @property
    @deprecate("use view_mode")
    def viewer_mode(self):
        return self.view_mode

    viewer_mode.__doc__ = view_mode.__doc__

    @abstractmethod
    def _get_job_table(
        self,
        sql_query,
        user,
        project_path,
        recursive=True,
        columns=None,
        element_lst=None,
    ):
        pass

    @staticmethod
    def _get_filtered_job_table(
        df: pandas.DataFrame, **kwargs: dict
    ) -> pandas.DataFrame:
        """
        Get a job table in a project based on matching values from any column in the project database

        The values in `kwargs` can be wildcards, with the following special charaters:
            - !value matches in the inverse of value
            - *value matches anything that ends in value
            - value* matches anything that starts with value
            - *value* matches anything that contains value

        Args:
            df (pandas.DataFrame): DataFrame to be filtered
            **kwargs (dict): Optional arguments for filtering with keys matching the project database column name
                            (eg. status="finished")

        Returns:
            list: DataFrame containing filtered jobs
        """
        if len(kwargs) == 0 or df.empty:
            return df
        mask = np.ones_like(df.index, dtype=bool)
        for key in kwargs.keys():
            if key not in list(df.columns):
                raise ValueError(
                    f"Column name {key} does not exist in the project database!"
                )
        for key, val in kwargs.items():
            invert = False
            if val is not None and val[0] == "!":
                invert = True
                val = val[1:]
            if val is None:
                update = df[key].isnull()
            elif str(val).startswith("*") and str(val).endswith("*"):
                update = df[key].str.contains(str(val).replace("*", ""))
            elif str(val).endswith("*"):
                update = df[key].str.startswith(str(val).replace("*", ""))
            elif str(val).startswith("*"):
                update = df[key].str.endswith(str(val).replace("*", ""))
            else:
                update = df[key] == val
            if invert:
                update = ~update
            mask &= update
        return df[mask]

    def job_table(
        self,
        sql_query,
        user,
        project_path,
        recursive=True,
        columns=None,
        all_columns=False,
        sort_by="id",
        max_colwidth=200,
        full_table=False,
        element_lst=None,
        job_name_contains="",
        **kwargs,
    ):
        """
        Access the job_table.

        Args:
            sql_query (str): SQL query to enter a more specific request
            user (str): username of the user whoes user space should be searched
            project_path (str): root_path - this is in contrast to the project_path in GenericPath
            recursive (bool): search subprojects [True/False]
            columns (list): by default only the columns ['job', 'project', 'chemicalformula'] are selected, but the
                            user can select a subset of ['id', 'status', 'chemicalformula', 'job', 'subjob', 'project',
                            'projectpath', 'timestart', 'timestop', 'totalcputime', 'computer', 'hamilton', 'hamversion',
                            'parentid', 'masterid']
            all_columns (bool): Select all columns - this overwrites the columns option.
            sort_by (str): Sort by a specific column
            max_colwidth (int): set the column width
            full_table (bool): Whether to show the entire pandas table
            element_lst (list): list of elements required in the chemical formular - by default None
            job_name_contains (str): (deprecated) A string which should be contained in every job_name
            **kwargs (dict): Optional arguments for filtering with keys matching the project database column name
                            (eg. status="finished"). Asterisk can be used to denote a wildcard, for zero or more
                            instances of any character

        Returns:
            pandas.Dataframe: Return the result as a pandas.Dataframe object
        """
        if columns is None:
            columns = ["job", "project", "chemicalformula"]
        if all_columns:
            columns = [
                "id",
                "status",
                "chemicalformula",
                "job",
                "subjob",
                "projectpath",
                "project",
                "timestart",
                "timestop",
                "totalcputime",
                "computer",
                "hamilton",
                "hamversion",
                "parentid",
                "masterid",
            ]
        if sort_by not in columns:
            columns = list(columns) + [sort_by]
        if full_table:
            pandas.set_option("display.max_rows", None)
            pandas.set_option("display.max_columns", None)
        else:
            pandas.reset_option("display.max_rows")
            pandas.reset_option("display.max_columns")
        pandas.set_option("display.max_colwidth", max_colwidth)
        df = self._get_job_table(
            user=user,
            sql_query=sql_query,
            project_path=project_path,
            recursive=recursive,
            columns=columns,
        )
        if job_name_contains != "":
            warnings.warn(
                "`job_name_contains` is deprecated - use `job='*term*'` instead"
            )
            kwargs["job"] = "*{}*".format(job_name_contains)
        df = self._get_filtered_job_table(df, **kwargs)
        if sort_by is not None:
            return df.sort_values(by=sort_by)
        return df

    @abstractmethod
    def _get_table_headings(self, table_name=None):
        pass

    def get_table_headings(self, table_name=None):
        """
        Get column names; if given table_name can select one of multiple tables defined in the database, but subclasses
        may ignore it

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
        return self._get_table_headings(table_name=table_name)

    @deprecate("use get_table_headings()")
    def get_db_columns(self):
        """
        Get column names

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
        return self.get_table_headings()


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

    def run(self):
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
                    except:
                        pass
                    break

    def kick(self):
        """
        Restarts the timeout.
        """
        self._queue.put(True)

    def kill(self):
        """
        Stop the watchdog and close the connection.
        """
        self._queue.put(False)
        self.join()


class AutorestoredConnection:
    def __init__(self, engine, timeout=60):
        self.engine = engine
        self._conn = None
        self._lock = Lock()
        self._watchdog = None
        self._logger = logger
        self._timeout = timeout

    def execute(self, *args, **kwargs):
        while True:
            try:
                with self._lock:
                    if self._conn is None or self._conn.closed:
                        self._conn = self.engine.connect()
                        if self._timeout > 0:
                            # only log reconnections when we keep the connection alive between requests otherwise we'll spam
                            # the log
                            if self._conn is None:
                                self._logger.info(
                                    "Reconnecting to DB; connection not existing."
                                )
                            else:
                                self._logger.info(
                                    "Reconnecting to DB; connection closed."
                                )
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
                    result = self._conn.execute(*args, **kwargs)
                    break
            except OperationalError as e:
                print(
                    f"Database connection failed with operational error {e}, waiting 5s, then re-trying."
                )
                time.sleep(5)
        return result

    def close(self):
        if self._conn is not None:
            self._conn.close()


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

    def __init__(self, connection_string, table_name, timeout=60):
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
                )
                self.conn = AutorestoredConnection(self._engine, timeout=self._timeout)
                self._keep_connection = self._timeout > 0
            else:
                self._engine = create_engine(connection_string)
                self.conn = self._engine.connect()
                self.conn.connection.create_function("like", 2, self.regexp)
                self._keep_connection = True
        except Exception as except_msg:
            raise ValueError("Connection to database failed: " + str(except_msg))

        self._chem_formula_lim_length = 50
        self.__reload_db()
        self.simulation_table = HistoricalTable(str(table_name), self.metadata)
        self.metadata.create_all()
        self._view_mode = False

    def _get_view_mode(self):
        return self._view_mode

    @IsDatabase.view_mode.setter
    def view_mode(self, value):
        """
        Set view_mode - if view_mode is enable pyiron has read only access to the database.

        Args:
            value (bool): TRUE or FALSE
        """
        if isinstance(value, bool):
            self._view_mode = value
        else:
            raise TypeError("Viewmode can only be TRUE or FALSE.")

    @IsDatabase.viewer_mode.setter
    @deprecate("use view_mode")
    def viewer_mode(self, value):
        self.view_mode = value

    def _job_dict(
        self,
        sql_query,
        user,
        project_path,
        recursive,
        job=None,
        sub_job_name="%",
        element_lst=None,
    ):
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
            list: the function returns a list of dicts like get_items_sql, but it does not format datetime:
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
            dict_clause["project"] = str(project_path) + "%"
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
        sql_query,
        user,
        project_path,
        recursive=True,
        columns=None,
        element_lst=None,
    ):
        job_dict = self._job_dict(
            sql_query=sql_query,
            user=user,
            project_path=project_path,
            recursive=recursive,
            element_lst=element_lst,
        )
        return pandas.DataFrame(job_dict, columns=columns)

    # Internal functions
    def __del__(self):
        """
        Close database connection

        Returns:

        """
        if not self._keep_connection:
            self.conn.close()

    def __reload_db(self):
        """
        Reload database

        Returns:

        """
        self.metadata = MetaData(bind=self._engine)
        self.metadata.reflect(self._engine)

    @staticmethod
    def regexp(expr, item):
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
    def _get_table_headings(self, table_name=None):
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
                autoload=True,
                autoload_with=self._engine,
            )
        except Exception:
            raise ValueError(str(table_name) + " does not exist")
        return [column.name for column in iter(simulation_list.columns)]

    def add_column(self, col_name, col_type):
        """
        Add an additional column - required for modification on the database

        Args:
            col_name (str, list): name of the new column, normal string like: 'myColumn'
            col_type (str, list: SQL type of the new column, SQL type like: 'varchar(50)'

        Returns:

        """
        if not self._view_mode:
            if isinstance(col_name, list):
                col_name = col_name[-1]
            if isinstance(col_type, list):
                col_type = col_type[-1]
            self._engine.execute(
                "ALTER TABLE %s ADD COLUMN %s %s"
                % (self.simulation_table.name, col_name, col_type)
            )
        else:
            raise PermissionError("Not avilable in viewer mode.")

    def change_column_type(self, col_name, col_type):
        """
        Modify data type of an existing column - required for modification on the database

        Args:
            col_name (str, list): name of the new column, normal string like: 'myColumn'
            col_type (str, list: SQL type of the new column, SQL type like: 'varchar(50)'

        Returns:

        """
        if not self._view_mode:
            if isinstance(col_name, list):
                col_name = col_name[-1]
            if isinstance(col_type, list):
                col_type = col_type[-1]
            self._engine.execute(
                "ALTER TABLE %s ALTER COLUMN %s TYPE %s"
                % (self.simulation_table.name, col_name, col_type)
            )
        else:
            raise PermissionError("Not avilable in viewer mode.")

    def get_items_sql(self, where_condition=None, sql_statement=None):
        """
        Submit an SQL query to the database

        Args:
            where_condition (str): SQL where query, query like: "project LIKE 'lammps.phonons.Ni_fcc%'"
            sql_statement (str): general SQL query, normal SQL statement

        Returns:
            list: get a list of dictionaries, where each dictionary represents one item of the table like:
                 [{u'chemicalformula': u'BO',
                  u'computer': u'localhost',
                  u'hamilton': u'VAMPS',
                  u'hamversion': u'1.1',
                  u'id': 1,
                  u'job': u'testing',
                  u'masterid': None,
                  u'parentid': 0,
                  u'project': u'database.testing',
                  u'projectpath': u'/TESTING',
                  u'status': u'KAAAA',
                  u'subjob': u'testJob',
                  u'timestart': u'2016-05-02 11:31:04.253377',
                  u'timestop': u'2016-05-02 11:31:04.371165',
                  u'totalcputime': 0.117788,
                  u'username': u'User'},
                 {u'chemicalformula': u'BO',
                  u'computer': u'localhost',
                  u'hamilton': u'VAMPS',
                  u'hamversion': u'1.1',
                  u'id': 2,
                  u'job': u'testing',
                  u'masterid': 0,
                  u'parentid': 0,
                  u'project': u'database.testing',
                  u'projectpath': u'/TESTING',
                  u'status': u'KAAAA',
                  u'subjob': u'testJob',
                  u'timestart': u'2016-05-02 11:31:04.253377',
                  u'timestop': u'2016-05-02 11:31:04.371165',
                  u'totalcputime': 0.117788,
                  u'username': u'User'}.....]
        """

        if where_condition:
            where_condition = (
                where_condition.replace("like", "similar to")
                if self._engine.dialect.name == "postgresql"
                else where_condition
            )
            try:
                query = "select * from " + self.table_name + " where " + where_condition
                query.replace("%", "%%")
                result = self.conn.execute(text(query))
            except Exception as except_msg:
                print("EXCEPTION in get_items_sql: ", except_msg)
                raise ValueError("EXCEPTION in get_items_sql: ", except_msg)
        elif sql_statement:
            sql_statement = (
                sql_statement.replace("like", "similar to")
                if self._engine.dialect.name == "postgresql"
                else sql_statement
            )
            # TODO: make it save against SQL injection
            result = self.conn.execute(text(sql_statement))
        else:
            result = self.conn.execute(text("select * from " + self.table_name))
        row = result.fetchall()
        if not self._keep_connection:
            self.conn.close()

        # change the date of str datatype back into datetime object
        output_list = []
        for col in row:
            # ensures working with db entries, which are camel case
            timestop_index = [item.lower() for item in col.keys()].index("timestop")
            timestart_index = [item.lower() for item in col.keys()].index("timestart")
            tmp_values = col.values()
            if (
                col.values()[timestop_index] and col.values()[timestart_index]
            ) is not None:
                # changes values
                try:
                    tmp_values[timestop_index] = datetime.strptime(
                        str(tmp_values[timestop_index]), "%Y-%m-%d %H:%M:%S.%f"
                    )
                    tmp_values[timestart_index] = datetime.strptime(
                        str(tmp_values[timestart_index]), "%Y-%m-%d %H:%M:%S.%f"
                    )
                except ValueError:
                    print("error in: ", str(col))
            output_list += [dict(zip(col.keys(), tmp_values))]
        return output_list

    def _check_chem_formula_length(self, par_dict):
        """
        performs a check whether the length of chemical formula exceeds the defined limit
        args:
        par_dict(dict): dictionary of the parameter
        limit(int): the limit for the length of checmical formular
        """
        key_limited = "ChemicalFormula"
        if (
            key_limited in par_dict.keys()
            and par_dict[key_limited] is not None
            and len(par_dict[key_limited]) > self._chem_formula_lim_length
        ):
            par_dict[key_limited] = "OVERFLOW_ERROR"
        return par_dict

    # Item functions
    def add_item_dict(self, par_dict):
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

        Returns:
            int: Database ID of the item created as an int, like: 3
        """
        if not self._view_mode:
            try:
                par_dict = self._check_chem_formula_length(par_dict)
                par_dict = dict(
                    (key.lower(), value) for key, value in par_dict.items()
                )  # make keys lowercase
                result = self.conn.execute(
                    self.simulation_table.insert(par_dict)
                ).inserted_primary_key[-1]
                if not self._keep_connection:
                    self.conn.close()
                return result
            except Exception as except_msg:
                raise ValueError("Error occurred: " + str(except_msg))
        else:
            raise PermissionError("Not avilable in viewer mode.")

    def __get_items(self, col_name, var):
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
            if type(var) is list:
                var = var[-1]
            query = select(
                [self.simulation_table], self.simulation_table.c[str(col_name)] == var
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
        return [dict(zip(col.keys(), col._mapping.values())) for col in row]

    def item_update(self, par_dict, item_id):
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
        if not self._view_mode:
            if type(item_id) is list:
                item_id = item_id[-1]  # sometimes a list is given, make it int
            if np.issubdtype(type(item_id), np.integer):
                item_id = int(item_id)
            # all items must be lower case, ensured here
            par_dict = dict((key.lower(), value) for key, value in par_dict.items())
            query = self.simulation_table.update(
                self.simulation_table.c["id"] == item_id
            ).values()
            try:
                self.conn.execute(query, par_dict)
            except (OperationalError, DatabaseError):
                if not self._sql_lite:
                    self.conn = AutorestoredConnection(self._engine)
                else:
                    self.conn = self._engine.connect()
                    self.conn.connection.create_function("like", 2, self.regexp)

                self.conn.execute(query, par_dict)
            if not self._keep_connection:
                self.conn.close()
        else:
            raise PermissionError("Not avilable in viewer mode.")

    def delete_item(self, item_id):
        """
        Delete Item from database

        Args:
            item_id (int): Databse Item ID (Integer), like: 38

        Returns:

        """
        if not self._view_mode:
            self.conn.execute(
                self.simulation_table.delete(
                    self.simulation_table.c["id"] == int(item_id)
                )
            )
            if not self._keep_connection:
                self.conn.close()
        else:
            raise PermissionError("Not avilable in viewer mode.")

    # Shortcut
    def get_item_by_id(self, item_id):
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

    def query_for_element(self, element):
        return or_(
            *[
                self.simulation_table.c["chemicalformula"].like(
                    "%" + element + "[ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789]%"
                ),
                self.simulation_table.c["chemicalformula"].like("%" + element),
            ]
        )

    def get_items_dict(self, item_dict, return_all_columns=True):
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
            list: the function returns a list of dicts like get_items_sql, but it does not format datetime:
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
                    self.simulation_table.c[str(key)] == element
                    if "%" not in element
                    else self.simulation_table.c[str(key)].like(element)
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
            query = select([self.simulation_table], and_(*and_statement))
        else:
            query = select([self.simulation_table.columns["id"]], and_(*and_statement))
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
        return [dict(zip(col.keys(), col._mapping.values())) for col in row]

    def get_job_status(self, job_id):
        try:
            return self.get_item_by_id(item_id=job_id)["status"]
        except KeyError:
            return None

    def set_job_status(self, job_id, status):
        self.item_update(
            {"status": str(status)},
            job_id,
        )

    def get_job_working_directory(self, job_id):
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
