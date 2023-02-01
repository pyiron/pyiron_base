# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
File based database interface
"""

import datetime
from collections.abc import Iterable
import numpy as np
import os
import pandas
from pyfileindex import PyFileIndex
from pyiron_base.interfaces.singleton import Singleton
from pyiron_base.database.generic import IsDatabase
from pyiron_base.storage.helper_functions import read_hdf5, write_hdf5

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH"
    " - Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 8, 2020"


table_columns = {
    "id": None,
    "status": None,
    "chemicalformula": None,
    "job": None,
    "subjob": None,
    "projectpath": None,
    "project": None,
    "hamilton": None,
    "hamversion": None,
    "timestart": None,
    "computer": None,
    "parentid": None,
    "username": None,
    "timestop": None,
    "totalcputime": None,
    "masterid": None,
}


class FileTable(IsDatabase, metaclass=Singleton):
    def __init__(self, project):
        self._fileindex = None
        self._job_table = None
        self._project = os.path.abspath(project)
        self._columns = list(table_columns.keys())
        self.force_reset()

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
        par_dict = dict((key.lower(), value) for key, value in par_dict.items())
        if len(self._job_table) != 0:
            job_id = np.max(self._job_table.id.values) + 1
        else:
            job_id = 1
        default_values = {
            "id": job_id,
            "status": "initialized",
            "chemicalformula": None,
            "timestart": datetime.datetime.now(),
        }
        par_dict_merged = table_columns.copy()
        par_dict_merged.update(default_values)
        par_dict_merged.update(par_dict)
        self._job_table = pandas.concat(
            [self._job_table, pandas.DataFrame([par_dict_merged])[self._columns]]
        ).reset_index(drop=True)
        return int(par_dict_merged["id"])

    def delete_item(self, item_id):
        """
        Delete Item from database

        Args:
            item_id (int): Databse Item ID (Integer), like: 38
        """
        item_id = int(item_id)
        if item_id in [int(v) for v in self._job_table.id.values]:
            self._job_table = self._job_table[
                self._job_table.id != item_id
            ].reset_index(drop=True)
        else:
            raise ValueError

    def force_reset(self):
        """
        Reset cache of the FileTable object
        """
        self._fileindex = PyFileIndex(
            path=self._project, filter_function=filter_function
        )
        df = pandas.DataFrame(self.init_table(fileindex=self._fileindex.dataframe))
        if len(df) != 0:
            df.id = df.id.astype(int)
            self._job_table = df[np.array(self._columns)]
        else:
            self._job_table = pandas.DataFrame({k: [] for k in self._columns})

    def get_child_ids(self, job_specifier, project=None, status=None):
        """
        Get the childs for a specific job

        Args:
            job_specifier (str): name of the master job or the master jobs job ID
            project (str): project_path - this is in contrast to the project_path in GenericPath
            status (str): filter childs which match a specific status - None by default

        Returns:
            list: list of child IDs
        """
        if project is None:
            project = self._project
        self.update()
        id_master = self.get_job_id(project=project, job_specifier=job_specifier)
        if id_master is None:
            return []
        else:
            df_tmp = self._job_table[self._job_table.id == id_master]
            working_directory = (df_tmp["project"] + df_tmp["job"] + "_hdf5/").values[0]
            if status is not None:
                id_lst = self._job_table[
                    (self._job_table.project == working_directory)
                    & (self._job_table.status == status)
                ].id.values
            else:
                id_lst = self._job_table[
                    (self._job_table.project == working_directory)
                ].id.values
            return sorted(id_lst)

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
        item_id = int(item_id)
        return {
            k: list(v.values())[0]
            for k, v in self._job_table[self._job_table.id == item_id].to_dict().items()
        }

    def get_items_dict(self, item_dict, return_all_columns=True):
        """
        Get list of jobs which fulfills the query in the dictionary

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
        df = self._job_table
        if not isinstance(item_dict, dict):
            raise TypeError
        for k, v in item_dict.items():
            if k in ["id", "parentid", "masterid"]:
                df = df[df[k] == int(v)]
            elif "%" not in str(v):
                df = df[df[k] == v]
            else:
                df = df[df[k].str.contains(v.replace("%", ""))]
        df_dict = df.to_dict()
        if return_all_columns:
            return [{k: v[i] for k, v in df_dict.items()} for i in df_dict["id"].keys()]
        else:
            return [{"id": i} for i in df_dict["id"].values()]

    def get_jobs(self, project=None, recursive=True, columns=None):
        """
        Get jobs as dictionary from filetable

        Args:
            project (str/ None): path to the project
            recursive (boolean): recursively iterate over all sub projects
            columns (list/ None): list of columns to return

        Returns:
            dict: job entries as dictionary
        """
        if project is None:
            project = self._project
        if columns is None:
            columns = ["id", "project"]
        df = self.job_table(
            sql_query=None,
            user=None,
            project_path=project,
            recursive=recursive,
            columns=columns,
        )
        if len(df) == 0:
            dictionary = {}
            for key in columns:
                dictionary[key] = list()
            return dictionary
            # return {key: list() for key in columns}
        dictionary = {}
        for key in df.keys():
            dictionary[key] = df[
                key
            ].tolist()  # ToDo: Check difference of tolist and to_list
        return dictionary

    def get_job_ids(self, project=None, recursive=True):
        """
        Get job IDs from filetable

        Args:
            project (str/ None): path to the project
            recursive (boolean): recursively iterate over all sub projects

        Returns:
            list/ None: list of job IDs
        """
        return self.get_jobs(project=project, recursive=recursive, columns=["id"])["id"]

    def get_job_id(self, job_specifier, project=None):
        """
        Get job ID from filetable

        Args:
            job_specifier (str): Job ID or job name
            project (str/ None): project_path as string

        Returns:
            int/ None: job ID
        """
        if project is None:
            project = self._project
        if isinstance(job_specifier, (int, np.integer)):
            return job_specifier  # is id
        if len(self._job_table) == 0:
            return None
        job_specifier.replace(".", "_")
        job_id_lst = self._job_table[
            (self._job_table.project == project)
            & (self._job_table.job == job_specifier)
        ].id.values
        if len(job_id_lst) == 0:
            job_id_lst = self._job_table[
                self._job_table.project.str.contains(project)
                & (self._job_table.job == job_specifier)
            ].id.values
        if len(job_id_lst) == 0:
            return None
        elif len(job_id_lst) == 1:
            return int(job_id_lst[0])
        else:
            raise ValueError(
                "job name '{0}' in this project is not unique".format(job_specifier)
            )

    def get_job_status(self, job_id):
        """
        Get status of a given job selected by its job ID

        Args:
            job_id (int): job ID as integer

        Returns:
            str: status of the job
        """
        return self._job_table[self._job_table.id == job_id].status.values[0]

    def get_job_working_directory(self, job_id):
        """
        Get the working directory of a particular job

        Args:
            job_id (int): job ID as integer

        Returns:
            str: working directory as absolute path
        """
        try:
            db_entry = self.get_item_by_id(job_id)
            if db_entry and len(db_entry) > 0:
                job_name = db_entry["subjob"][1:]
                return os.path.join(
                    db_entry["project"],
                    job_name + "_hdf5",
                    job_name,
                )
            else:
                return None
        except KeyError:
            return None

    def init_table(self, fileindex, working_dir_lst=None):
        """
        Initialize the filetable class

        Args:
            fileindex (pandas.DataFrame): file system index for the current project path
            working_dir_lst (list/ None): list of working directories

        Returns:
            list: list of dictionaries
        """
        if working_dir_lst is None:
            working_dir_lst = []
        fileindex = fileindex[~fileindex.is_directory]
        fileindex = fileindex.iloc[fileindex.path.values.argsort()]
        job_lst = []
        for path, mtime in zip(fileindex.path, fileindex.mtime):
            try:  # Ignore HDF5 files which are not created by pyiron
                job_dict = self.get_extract(path, mtime)
            except (ValueError, OSError):
                pass
            else:
                job_dict["id"] = len(working_dir_lst) + 1
                working_dir_lst.append(
                    job_dict["project"][:-1] + job_dict["subjob"] + "_hdf5/"
                )
                if job_dict["project"] in working_dir_lst:
                    job_dict["masterid"] = (
                        working_dir_lst.index(job_dict["project"]) + 1
                    )
                else:
                    job_dict["masterid"] = None
                job_lst.append(job_dict)
        return job_lst

    def _item_update(self, par_dict, item_id):
        """
        Modify Item in database

        Args:
            par_dict (dict): Dictionary of the parameters to be modified,, where the key is the column name.
                            {'job' : 'maximize',
                             'subjob' : 'testing',
                             ........}
            item_id (int, list): Database Item ID (Integer) - '38'  can also be [38]
        """
        if isinstance(item_id, str):
            item_id = float(item_id)
        for k, v in par_dict.items():
            self._job_table.loc[self._job_table.id == int(item_id), k] = v

    def set_job_status(self, job_id, status):
        """
        Set job status

        Args:
            job_id (int): job ID as integer
            status (str): job status
        """
        super().set_job_status(job_id=job_id, status=status)
        self._update_hdf5_status(job_id=job_id, status=status)

    def _update_hdf5_status(self, job_id, status):
        if isinstance(job_id, Iterable):
            for j_id in job_id:
                db_entry = self.get_item_by_id(item_id=j_id)
                write_hdf5(
                    db_entry["project"] + db_entry["subjob"] + ".h5",
                    status,
                    title=db_entry["subjob"][1:] + "/status",
                    overwrite="update",
                )
        else:
            db_entry = self.get_item_by_id(item_id=job_id)
            write_hdf5(
                db_entry["project"] + db_entry["subjob"] + ".h5",
                status,
                title=db_entry["subjob"][1:] + "/status",
                overwrite="update",
            )

    def update(self):
        """
        Update the filetable cache
        """
        self._job_table.status = [
            self._get_job_status_from_hdf5(job_id)
            for job_id in self._job_table.id.values
        ]
        self._fileindex.update()
        if len(self._job_table) != 0:
            files_lst, working_dir_lst = zip(
                *[
                    [project + subjob[1:] + ".h5", project + subjob[1:] + "_hdf5"]
                    for project, subjob in zip(
                        self._job_table.project.values, self._job_table.subjob.values
                    )
                ]
            )
            df_new = self._fileindex.dataframe[
                ~self._fileindex.dataframe.is_directory
                & ~self._fileindex.dataframe.path.isin(files_lst)
            ]
        else:
            files_lst, working_dir_lst = [], []
            df_new = self._fileindex.dataframe[~self._fileindex.dataframe.is_directory]
        if len(df_new) > 0:
            job_lst = self.init_table(
                fileindex=df_new, working_dir_lst=list(working_dir_lst)
            )
            if len(job_lst) > 0:
                df = pandas.DataFrame(job_lst)[self._columns]
                if len(files_lst) != 0 and len(working_dir_lst) != 0:
                    self._job_table = pandas.concat([self._job_table, df]).reset_index(
                        drop=True
                    )
                else:
                    self._job_table = df

    @staticmethod
    def get_extract(path, mtime):
        basename = os.path.basename(path)
        job = os.path.splitext(basename)[0]
        time = datetime.datetime.fromtimestamp(mtime)
        return_dict = table_columns.copy()
        return_dict.update(
            {
                "status": get_job_status_from_file(hdf5_file=path, job_name=job),
                "job": job,
                "subjob": "/" + job,
                "project": os.path.dirname(path) + "/",
                "timestart": time,
                "timestop": time,
                "totalcputime": 0.0,
                "hamilton": get_hamilton_from_file(hdf5_file=path, job_name=job),
                "hamversion": get_hamilton_version_from_file(
                    hdf5_file=path, job_name=job
                ),
            }
        )
        del return_dict["id"]
        del return_dict["masterid"]
        return return_dict

    def _get_job_status_from_hdf5(self, job_id):
        db_entry = self.get_item_by_id(job_id)
        job_name = db_entry["subjob"][1:]
        return get_job_status_from_file(
            hdf5_file=os.path.join(db_entry["project"], job_name + ".h5"),
            job_name=job_name,
        )

    def _get_job_table(
        self,
        sql_query,
        user,
        project_path=None,
        recursive=True,
        columns=None,
        element_lst=None,
    ):
        self.update()
        if project_path is None:
            project_path = self._project
        if len(self._job_table) != 0:
            if recursive:
                return self._job_table[
                    self._job_table.project.str.contains(project_path)
                ]
            else:
                return self._job_table[self._job_table.project == project_path]
        else:
            return self._job_table

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
        return self._job_table.columns.values

    def _get_view_mode(self):
        return False


def filter_function(file_name):
    return ".h5" in file_name


def get_hamilton_from_file(hdf5_file, job_name):
    return read_hdf5(hdf5_file, job_name + "/TYPE").split(".")[-1].split("'")[0]


def get_hamilton_version_from_file(hdf5_file, job_name):
    return read_hdf5(hdf5_file, job_name + "/VERSION")


def get_job_status_from_file(hdf5_file, job_name):
    if os.path.exists(hdf5_file):
        return read_hdf5(hdf5_file, job_name + "/status")
    else:
        return None
