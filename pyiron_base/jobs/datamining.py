# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import codecs
from datetime import datetime
import dill as pickle
import json
import numpy as np
import os
import pandas
from pandas.errors import EmptyDataError
from tqdm.auto import tqdm
import types
from typing import List, Tuple

from pyiron_base.utils.deprecate import deprecate
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.jobs.job.extension import jobstatus
from pyiron_base.storage.hdfio import FileHDFio
from pyiron_base.jobs.master.generic import get_function_from_string


__author__ = "Uday Gajera, Jan Janssen, Joerg Neugebauer"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "0.0.1"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "development"
__date__ = "Sep 1, 2018"


def _to_pickle(hdf, key, value):
    hdf[key] = codecs.encode(pickle.dumps(value), "base64").decode()


def _from_pickle(hdf, key):
    return pickle.loads(codecs.decode(hdf[key].encode(), "base64"))


def get_job_id(job):
    return {"job_id": job.job_id}


class FunctionContainer(object):
    """
    Class which is able to append, store and retreive a set of functions.

    """

    def __init__(self, system_function_lst=None):
        if system_function_lst is None:
            system_function_lst = []
        self._user_function_dict = {}
        self._system_function_lst = system_function_lst
        self._system_function_dict = {
            func.__name__: False for func in self._system_function_lst
        }
        self._system_function_dict["get_job_id"] = True

    @property
    def _function_lst(self):
        return [
            funct
            for funct in self._system_function_lst
            if funct.__name__ in self._system_function_dict.keys()
            and self._system_function_dict[funct.__name__]
        ] + list(self._user_function_dict.values())

    def _to_hdf(self, hdf):
        _to_pickle(hdf=hdf, key="user_function_dict", value=self._user_function_dict)
        _to_pickle(
            hdf=hdf, key="system_function_dict", value=self._system_function_dict
        )

    def _from_hdf(self, hdf):
        self._user_function_dict = _from_pickle(hdf=hdf, key="user_function_dict")
        self._system_function_dict = _from_pickle(hdf=hdf, key="system_function_dict")

    def __setitem__(self, key, item):
        if isinstance(item, str):
            self._user_function_dict[key] = eval(
                'lambda job: {"' + key + '":' + item + "}"
            )
        elif isinstance(item, types.FunctionType):
            self._user_function_dict[key] = lambda job: {key: item(job)}
        else:
            raise TypeError("unsupported function type!")

    def __getitem__(self, key):
        return self._user_function_dict[key]

    def __getattr__(self, name):
        if name in list(self._system_function_dict.keys()):
            self._system_function_dict[name] = True
            return self._system_function_dict[name]
        else:
            super(FunctionContainer, self).__getattr__(name)

    def __dir__(self):
        return list(self._system_function_dict.keys())


class JobFilters(object):
    """
    Certain predefined job filters

    """

    @staticmethod
    def job_type(job_type):
        def filter_job_type(job):
            return job.__name__ == job_type

        return filter_job_type

    @staticmethod
    def job_name_contains(job_name_segment):
        def filter_job_name_segment(job):
            return job_name_segment in job.job_name

        return filter_job_name_segment


class PyironTable:
    """
    Class for easy, efficient, and pythonic analysis of data from pyiron projects

    Args:
        project (pyiron.project.Project/None): The project to analyze
        name (str): Name of the pyiron table
        system_function_lst (list/ None): List of built-in functions
    """

    def __init__(
        self, project, name=None, system_function_lst=None, csv_file_name=None
    ):
        self._project = project
        self._df = pandas.DataFrame({})
        self.convert_to_object = False
        self._name = name
        self._db_filter_function = always_true_pandas
        self._filter_function = always_true
        self._filter = JobFilters()
        self._system_function_lst = system_function_lst
        self.add = FunctionContainer(system_function_lst=self._system_function_lst)
        self._csv_file = csv_file_name

    @property
    def filter(self):
        """
        Object containing pre-defined filter functions

        Returns:
            pyiron.table.datamining.JobFilters: The object containing the filters

        """
        return self._filter

    @property
    def name(self):
        """
        Name of the table. Takes the project name if not specified

        Returns:
            str: Name of the table

        """
        if self._name is None:
            return self._project.name
        return self._name

    @property
    def db_filter_function(self):
        """
        Function to filter the a project database table before job specific functions are applied.

        The function must take a pyiron project table in the pandas.DataFrame format (project.job_table()) and return a
        boolean pandas.DataSeries with the same number of rows as the project table

        Example:

        >>> def job_filter_function(df):
        >>>    return (df["chemicalformula"=="H2"]) & (df["hamilton"=="Vasp"])

        >>> table.db_filter_function = job_filter_function
        """
        return self._db_filter_function

    @db_filter_function.setter
    def db_filter_function(self, funct):
        self._db_filter_function = funct

    @property
    def filter_function(self):
        """
        Function to filter each job before more expensive functions are applied

        Example:

        >>> def job_filter_function(job):
        >>>     return (job.status == "finished") & ("murn" in job.job_name)

        >>> table.filter_function = job_filter_function
        """
        return self._filter_function

    @filter_function.setter
    def filter_function(self, funct):
        self._filter_function = funct

    def _get_new_functions(self, file: FileHDFio) -> Tuple[List, List]:
        try:
            (
                temp_user_function_dict,
                temp_system_function_dict,
            ) = self._get_data_from_hdf5(hdf=file)
            new_user_functions = [
                key
                for key in self.add._user_function_dict.keys()
                if key not in temp_user_function_dict.keys()
            ]
            new_system_functions = [
                k
                for k, v in self.add._system_function_dict.items()
                if v and not temp_system_function_dict[k]
            ]
        except:
            new_user_functions = []
            new_system_functions = []
        return new_user_functions, new_system_functions

    def create_table(self, file, job_status_list, enforce_update=False):
        """
        Create or update the table.

        If this method has been called before and there are new functions added to :attr:`.add`, apply them on the
        previously analyzed jobs.
        If this method has been called before and there are new jobs added to :attr:`.analysis_project`, apply all
        functions to them.

        The result is available via :meth:`.get_dataframe`.

        Args:
            file (FileHDFio): HDF were the previous state of the table is stored
            job_status_list (list of str): only consider jobs with these statuses
            enforce_update (bool): if True always regenerate the table completely.
        """
        # if there's new keys, apply the *new* functions to the old jobs and name the resulting table `df_new_keys`
        # if there's new jobs, apply *all* functions to them and name the resulting table `df_new_ids`

        # if enforce_update is given we recalculate the whole table below anyway, no need to patch up new keys
        if not enforce_update:
            new_user_functions, new_system_functions = self._get_new_functions(file)

            if len(new_user_functions) > 0 or len(new_system_functions) > 0:
                function_lst = [
                    self.add._user_function_dict[k] for k in new_user_functions
                ] + [
                    funct
                    for funct in self.add._system_function_lst
                    if funct.__name__ in new_system_functions
                ]
                df_new_keys = self._iterate_over_job_lst(
                    job_lst=map(self._project.inspect, self._get_job_ids()),
                    function_lst=function_lst,
                )
                if len(df_new_keys) > 0:
                    self._df = pandas.concat([self._df, df_new_keys], axis="columns")

        new_jobs = self._collect_job_update_lst(
            job_status_list=job_status_list,
            job_stored_ids=self._get_job_ids() if not enforce_update else None,
        )
        if len(new_jobs) > 0:
            df_new_ids = self._iterate_over_job_lst(
                job_lst=new_jobs, function_lst=self.add._function_lst
            )
            if len(df_new_ids) > 0:
                self._df = pandas.concat([self._df, df_new_ids], ignore_index=True)

    def get_dataframe(self):
        return self._df

    def _list_nodes(self):
        return list(self._df.columns)

    def __getitem__(self, item):
        if item in self.list_nodes():
            return np.array(self._df[item])
        return None

    def __str__(self):
        return self._df.__str__()

    def __repr__(self):
        """
        Human readable string representation

        Returns:
            str: pandas Dataframe structure as string
        """
        return self._df.__repr__()

    @property
    def _file_name_csv(self):
        if self._csv_file is None:
            return self._project.path + self.name + ".csv"
        else:
            return self._csv_file

    def _load_csv(self):
        # Legacy method to read tables written to csv
        self._df = pandas.read_csv(self._file_name_csv)

    @staticmethod
    def _get_data_from_hdf5(hdf):
        temp_user_function_dict = _from_pickle(hdf=hdf, key="user_function_dict")
        temp_system_function_dict = _from_pickle(hdf=hdf, key="system_function_dict")
        return temp_user_function_dict, temp_system_function_dict

    def _get_job_ids(self):
        if len(self._df) > 0:
            return self._df.job_id.values
        else:
            return np.array([])

    def _get_filtered_job_ids_from_project(self, recursive=True):
        project_table = self._project.job_table(recursive=recursive)
        filter_funct = self.db_filter_function
        return project_table[filter_funct(project_table)]["id"].tolist()

    @staticmethod
    def _apply_function_on_job(funct, job):
        try:
            return funct(job)
        except (ValueError, TypeError):
            return {}

    def _apply_list_of_functions_on_job(self, job, function_lst):
        diff_dict = {}
        for funct in function_lst:
            funct_dict = self._apply_function_on_job(funct, job)
            for key, value in funct_dict.items():
                diff_dict[key] = value
        return diff_dict

    def _iterate_over_job_lst(self, job_lst: List, function_lst: List) -> List[dict]:
        """
        Apply functions to job.

        Any functions that raise an error are set to `None` in the final list.

        Args:
            job_lst (list of JobPath): all jobs to analyze
            function_lst (list of functions): all functions to apply on jobs.  Must return a dictionary.

        Returns:
            list of dict: a list of the merged dicts from all functions for each job
        """
        diff_dict_lst = []
        for job_inspect in tqdm(job_lst, desc="Processing jobs"):
            if self.convert_to_object:
                job = job_inspect.to_object()
            else:
                job = job_inspect
            diff_dict = self._apply_list_of_functions_on_job(
                job=job, function_lst=function_lst
            )
            diff_dict_lst.append(diff_dict)
        self.refill_dict(diff_dict_lst)
        return pandas.DataFrame(diff_dict_lst)

    @staticmethod
    def total_lst_of_keys(diff_dict_lst):
        """
        Get unique list of all keys occuring in list.
        """
        total_key_lst = []
        for sub_dict in diff_dict_lst:
            for key in sub_dict.keys():
                total_key_lst.append(key)
        return set(total_key_lst)

    def refill_dict(self, diff_dict_lst):
        """
        Ensure that all dictionaries in the list have the same keys.

        Keys that are not in a dict are set to `None`.
        """
        total_key_lst = self.total_lst_of_keys(diff_dict_lst)
        for sub_dict in diff_dict_lst:
            for key in total_key_lst:
                if key not in sub_dict.keys():
                    sub_dict[key] = None

    def _collect_job_update_lst(self, job_status_list, job_stored_ids=None):
        """
        Collect jobs to update the pyiron table.

        Jobs in `job_stored_ids` are ignored.

        Args:
            job_status_list (list): List of job status to consider
            job_stored_ids (list/ None): List of already analysed job ids

        Returns:
            list: List of JobCore objects
        """
        if job_stored_ids is not None:
            job_id_lst = [
                job_id
                for job_id in self._get_filtered_job_ids_from_project()
                if job_id not in job_stored_ids
            ]
        else:
            job_id_lst = self._get_filtered_job_ids_from_project()

        job_update_lst = []
        for job_id in tqdm(job_id_lst, desc="Loading and filtering jobs"):
            try:
                job = self._project.inspect(job_id)
            except (
                IndexError
            ):  # In case the job was deleted while the pyiron table is running
                job = None
            if (
                job is not None
                and job.status in job_status_list
                and self.filter_function(job)
            ):
                job_update_lst.append(job)
        return job_update_lst

    def _repr_html_(self):
        """
        Internal helper function to represent the GenericParameters object within the Jupyter Framework

        Returns:
            HTML: Jupyter HTML object
        """
        return self._df._repr_html_()


class TableJob(GenericJob):
    """

    Since a project can have a large number of jobs, it is often necessary
    to “filter” the data to extract useful information. PyironTable is a tool
    that allows the user to do this efficiently.

    Example:

    >>> # Prepare random data
    >>> for T in T_range:
    >>>     lmp = pr.create.job.Lammps(('lmp', T))
    >>>     lmp.structure = pr.create.structure.bulk('Ni', cubic=True).repeat(5)
    >>>     lmp.calc_md(temperature=T)
    >>>     lmp.run()

    >>> def db_filter_function(job_table):
    >>>     return (job_table.status == "finished") & (job_table.hamilton == "Lammps")

    >>> def get_energy(job):
    >>>     return job["output/generic/energy_pot"][-1]

    >>> def get_temperature(job):
    >>>     return job['output/generic/temperature'][-1]

    >>> table.db_filter_function = db_filter_function

    >>> table.add["energy"] = get_energy
    >>> table.add["temperature"] = get_temperature
    >>> table.run()
    >>> table.get_dataframe()

    This returns a dataframe containing job-id, energy and temperature.

    Alternatively, the filter function can be applied on the job

    >>> def job_filter_function(job):
    >>>     return (job.status == "finished") & ("lmp" in job.job_name)

    >>> table.filter_function = job_filter_function

    """

    _system_function_lst = [get_job_id]

    def __init__(self, project, job_name):
        super(TableJob, self).__init__(project, job_name)
        self.__version__ = "0.1"
        self.__hdf_version__ = "0.3.0"
        self._analysis_project = None
        self._pyiron_table = PyironTable(
            project=None,
            system_function_lst=self._system_function_lst,
            csv_file_name=os.path.join(self.working_directory, "pyirontable.csv"),
        )
        self._enforce_update = False
        self._job_status = ["finished"]
        self._python_only_job = True
        self.analysis_project = project.project

    @property
    def filter(self):
        return self._pyiron_table.filter

    @property
    def db_filter_function(self):
        """
        function: database level filter function

        The function should accept a dataframe, the job table of :attr:`~.analysis_project` and return a bool index into
        it.  Jobs where the index is `False` are excluced from the analysis.
        """
        return self._pyiron_table.db_filter_function

    @db_filter_function.setter
    def db_filter_function(self, funct):
        self._pyiron_table.db_filter_function = funct

    @property
    def filter_function(self):
        """
        function: job level filter function

        The function should accept a GenericJob or JobCore object and return a bool, if it returns `False` the job is
        excluced from the analysis.
        """
        return self._pyiron_table.filter_function

    @filter_function.setter
    def filter_function(self, funct):
        self._pyiron_table.filter_function = funct

    @property
    def job_status(self):
        """
        list of str: only jobs with status in this list are included in the table.
        """
        return self._job_status

    @job_status.setter
    def job_status(self, status):
        if isinstance(status, str):
            status = [status]
        for s in status:
            valid = jobstatus.job_status_lst
            if s not in valid:
                raise ValueError(
                    f"'{s}' not a valid job status! Must be one of {valid}."
                )
        self._job_status = status

    @property
    def pyiron_table(self):
        return self._pyiron_table

    @property
    @deprecate("Use analysis_project instead!")
    def ref_project(self):
        return self.analysis_project

    @ref_project.setter
    def ref_project(self, project):
        self.analysis_project = project

    @property
    def analysis_project(self):
        """
        :class:`.Project`: which pyiron project should be searched for jobs

        WARNING: setting this resets any previously added analysis and filter functions
        """
        return self._analysis_project

    @analysis_project.setter
    def analysis_project(self, project):
        self._analysis_project = project
        self._pyiron_table = PyironTable(
            project=self._analysis_project,
            system_function_lst=self._system_function_lst,
            csv_file_name=os.path.join(self.working_directory, "pyirontable.csv"),
        )

    @property
    def add(self):
        """
        Add a function to analyse job data

        Example:

        >>> def get_energy(job):
        >>>     return job["output/generic/energy_pot"][-1]

        >>> table.add["energy"] = get_energy
        """
        return self._pyiron_table.add

    @property
    def convert_to_object(self):
        """
        bool: if `True` convert fully load jobs before passing them to functions, if `False` use inspect mode.
        """
        return self._pyiron_table.convert_to_object

    @convert_to_object.setter
    def convert_to_object(self, conv_to_obj):
        self._pyiron_table.convert_to_object = conv_to_obj

    @property
    def enforce_update(self):
        """
        bool: if `True` re-evaluate all function on all jobs when :meth:`.update_table` is called.
        """
        return self._enforce_update

    @enforce_update.setter
    def enforce_update(self, enforce):
        if isinstance(enforce, bool):
            if enforce:
                self._enforce_update = True
                if self.status.finished:
                    self.status.created = True
            else:
                self._enforce_update = False
        else:
            raise TypeError()

    def _save_output(self):
        with self.project_hdf5.open("output") as hdf5_output:
            self.pyiron_table._df.to_hdf(
                hdf5_output.file_name, hdf5_output.h5_path + "/table"
            )

    def to_hdf(self, hdf=None, group_name=None):
        """
        Store pyiron table job in HDF5

        Args:
            hdf:
            group_name:

        """
        super(TableJob, self).to_hdf(hdf=hdf, group_name=group_name)
        with self.project_hdf5.open("input") as hdf5_input:
            hdf5_input["bool_dict"] = {
                "enforce_update": self._enforce_update,
                "convert_to_object": self._pyiron_table.convert_to_object,
            }
            self._pyiron_table.add._to_hdf(hdf5_input)
            if self._analysis_project is not None:
                hdf5_input["project"] = {
                    "path": self._analysis_project.path,
                    "user": self._analysis_project.user,
                    "sql_query": self._analysis_project.sql_query,
                    "filter": self._analysis_project._filter,
                    "inspect_mode": self._analysis_project._inspect_mode,
                }
            if self.pyiron_table._filter_function is not None:
                _to_pickle(hdf5_input, "filter", self.pyiron_table._filter_function)
            if self.pyiron_table._db_filter_function is not None:
                _to_pickle(
                    hdf5_input, "db_filter", self.pyiron_table._db_filter_function
                )
        if len(self.pyiron_table._df) != 0:
            self._save_output()

    def from_hdf(self, hdf=None, group_name=None):
        """
        Restore pyiron table job from HDF5

        Args:
            hdf:
            group_name:
        """
        super(TableJob, self).from_hdf(hdf=hdf, group_name=group_name)
        hdf_version = self.project_hdf5.get("HDF_VERSION", "0.1.0")
        with self.project_hdf5.open("input") as hdf5_input:
            if "project" in hdf5_input.list_nodes():
                project_dict = hdf5_input["project"]
                project = self.project.__class__(
                    path=project_dict["path"],
                    user=project_dict["user"],
                    sql_query=project_dict["sql_query"],
                )
                project._filter = project_dict["filter"]
                project._inspect_mode = project_dict["inspect_mode"]
                self.analysis_project = project
            if "filter" in hdf5_input.list_nodes():
                if hdf_version == "0.1.0":
                    self.pyiron_table._filter_function_str = hdf5_input["filter"]
                    self.pyiron_table.filter_function = get_function_from_string(
                        hdf5_input["filter"]
                    )
                else:
                    self.pyiron_table.filter_function = _from_pickle(
                        hdf5_input, "filter"
                    )
            if "db_filter" in hdf5_input.list_nodes():
                if hdf_version == "0.1.0":
                    self.pyiron_table._db_filter_function_str = hdf5_input["db_filter"]
                    self.pyiron_table.db_filter_function = get_function_from_string(
                        hdf5_input["db_filter"]
                    )
                else:
                    self.pyiron_table.db_filter_function = _from_pickle(
                        hdf5_input, "db_filter"
                    )
            bool_dict = hdf5_input["bool_dict"]
            self._enforce_update = bool_dict["enforce_update"]
            self._pyiron_table.convert_to_object = bool_dict["convert_to_object"]
            self._pyiron_table.add._from_hdf(hdf5_input)
        if hdf_version == "0.3.0":
            with self.project_hdf5.open("output") as hdf5_output:
                if "table" in hdf5_output.list_groups():
                    self._pyiron_table._df = pandas.read_hdf(
                        hdf5_output.file_name, hdf5_output.h5_path + "/table"
                    )
        else:
            pyiron_table = os.path.join(self.working_directory, "pyirontable.csv")
            if os.path.exists(pyiron_table):
                try:
                    self._pyiron_table._df = pandas.read_csv(pyiron_table)
                    self._pyiron_table._csv_file = pyiron_table
                except EmptyDataError:
                    pass
            else:
                with self.project_hdf5.open("output") as hdf5_output:
                    if "table" in hdf5_output.list_nodes():
                        self._pyiron_table._df = pandas.DataFrame(
                            json.loads(hdf5_output["table"])
                        )

    def validate_ready_to_run(self):
        if self._analysis_project is None:
            raise ValueError("Analysis project not defined!")

    def run_static(self):
        self._create_working_directory()
        self.status.running = True
        self.update_table()
        self.status.finished = True

    @deprecate(job_status_list="Use TableJob.job_status instead!")
    def update_table(self, job_status_list=None):
        """
        Update the pyiron table object, add new columns if a new function was added or add new rows for new jobs.

        By default this function does not recompute already evaluated functions on already existing jobs.  To force a
        complete re-evaluation set :attr:`~.enforce_update` to `True`.

        Args:
            job_status_list (list/None): List of job status which are added to the table by default ["finished"].
                                         Deprecated, use :attr:`.job_status` instead!
        """
        if job_status_list is None:
            job_status_list = self.job_status
        if self.job_id is not None:
            self.project.db.item_update({"timestart": datetime.now()}, self.job_id)
        with self.project_hdf5.open("input") as hdf5_input:
            self._pyiron_table.create_table(
                file=hdf5_input,
                job_status_list=job_status_list,
                enforce_update=self._enforce_update,
            )
        self.to_hdf()
        self._pyiron_table._df.to_csv(
            os.path.join(self.working_directory, "pyirontable.csv"), index=False
        )
        self._save_output()
        self.run_time_to_db()

    def get_dataframe(self):
        """
        Returns aggregated results over all jobs.

        Returns:
            pandas.Dataframe
        """
        return self.pyiron_table._df


def always_true_pandas(job_table):
    """
    A function which returns a pandas Series with all True values based on the size of the input pandas dataframe
    Args:
        job_table (pandas.DataFrame): Input dataframe

    Returns:
        pandas.Series: A series of True values

    """
    from pandas import Series

    return Series([True] * len(job_table), index=job_table.index)


def always_true(_):
    """
    A function that always returns True no matter what!

    Returns:
        bool: True

    """
    return True
