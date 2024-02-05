# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
DatabaseAccess class deals with accessing the database
"""

from pyiron_base.state.logger import logger
from abc import ABC, abstractmethod
from collections.abc import Iterable
import warnings
import numpy as np
import re
from pyiron_base.utils.deprecate import deprecate
import pandas
import typing
import fnmatch

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
        df: pandas.DataFrame,
        mode: typing.Literal["regex", "glob"] = "glob",
        **kwargs: dict,
    ) -> pandas.DataFrame:
        """
        Get a job table in a project based on matching values from any column in the project database

        The values in `kwargs` can be wildcards. The matches can be given
        either via "glob" or "regex".

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
            if mode == "regex":
                pattern = re.compile(str(val))
                update = df[key].apply(pattern.search).astype(bool)
            elif mode == "glob":
                if str(val).startswith("!"):
                    logger.warn(
                        "It looks like you are using an old pyiron convention."
                        " If you meant to exclude the term following '!', use"
                        " `mode='regex' and use a regex convention (such as"
                        " `^(?!term$)`)"
                    )
                arr = np.asarray(df[key]).astype(str)
                matches = fnmatch.filter(arr, str(val))
                update = np.array([k in matches for k in arr])
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
        mode: typing.Literal["regex", "glob"] = "glob",
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
            mode (str): search mode when kwargs are given.
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
        df = self._get_filtered_job_table(df, mode=mode, **kwargs)
        if sort_by is not None:
            return df.sort_values(by=sort_by)
        return df

    @abstractmethod
    def _get_table_headings(self, table_name=None):
        pass

    def item_update(self, par_dict, item_id):
        if isinstance(item_id, Iterable):
            return self._items_update(par_dict=par_dict, item_ids=item_id)
        return self._item_update(par_dict=par_dict, item_id=item_id)

    @abstractmethod
    def _item_update(self, par_dict, item_id):
        pass

    def _items_update(self, par_dict, item_ids):
        """
        For now simply loops over all item_ids to call item_update,
        but can be made more efficient.
        Should be made an asbtract method when defined in inheriting classes

        Args:
            par_dict (_type_): _description_
            item_ids (_type_): _description_
        """
        for i_id in item_ids:
            self._item_update(par_dict=par_dict, item_id=i_id)

    def set_job_status(self, status, job_id):
        """
        Set status of a job or multiple jobs if job_id is iterable.

        Args:
            status (str): status
            job_id (int, Iterable): job id
        """
        if isinstance(job_id, Iterable):
            return self._items_update(
                par_dict={"status": status},
                item_ids=job_id,
            )
        return self._item_update(
            par_dict={"status": status},
            item_id=job_id,
        )

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
