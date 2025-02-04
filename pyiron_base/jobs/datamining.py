# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import codecs
import concurrent.futures
import json
import os
import types
from datetime import datetime
from typing import Any, List, Optional, Tuple, Union

import cloudpickle
import numpy as np
import pandas
from pandas.errors import EmptyDataError
from pyiron_snippets.deprecate import deprecate
from tqdm.auto import tqdm

from pyiron_base.jobs.job.extension import jobstatus
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.storage.hdfio import FileHDFio

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


def _to_pickle(hdf: FileHDFio, key: str, value: Any) -> None:
    """
    Pickle and store an object in an HDF file.

    Args:
        hdf (FileHDFio): The HDF file object.
        key (str): The key to store the object under.
        value (Any): The object to be pickled and stored.
    """
    hdf[key] = codecs.encode(
        cloudpickle.dumps(obj=value, protocol=5, buffer_callback=None), "base64"
    ).decode()


def _from_pickle(hdf: FileHDFio, key: str) -> Any:
    """
    Load and unpickle an object from an HDF file.

    Args:
        hdf (FileHDFio): The HDF file object.
        key (str): The key of the object in the HDF file.

    Returns:
        Any: The unpickled object.
    """
    try:
        return cloudpickle.loads(codecs.decode(hdf[key].encode(), "base64"))
    except ModuleNotFoundError:
        import dill

        return dill.loads(codecs.decode(hdf[key].encode(), "base64"))


def get_job_id(job):
    """
    Get the job ID.

    Args:
        job: The job object.

    Returns:
        dict: A dictionary containing the job ID.

    """
    return {"job_id": job.job_id}


class FunctionContainer(object):
    """
    Class which is able to append, store and retreive a set of functions.

    """

    def __init__(self, system_function_lst: Optional[List[callable]] = None):
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

    def _to_hdf(self, hdf: FileHDFio) -> None:
        """
        Store the user and system function dictionaries in an HDF file.

        Args:
            hdf (FileHDFio): The HDF file object.
        """
        _to_pickle(hdf=hdf, key="user_function_dict", value=self._user_function_dict)
        _to_pickle(
            hdf=hdf, key="system_function_dict", value=self._system_function_dict
        )

    def _from_hdf(self, hdf: FileHDFio) -> None:
        """
        Load data from an HDF file.

        Args:
            hdf (str): The path to the HDF file.

        Returns:
            None
        """
        self._user_function_dict = _from_pickle(hdf=hdf, key="user_function_dict")
        self._system_function_dict = _from_pickle(hdf=hdf, key="system_function_dict")

    def __setitem__(self, key: str, item: Union[str, types.FunctionType]) -> None:
        if isinstance(item, str):
            self._user_function_dict[key] = eval(
                'lambda job: {"' + key + '":' + item + "}"
            )
        elif isinstance(item, types.FunctionType):
            self._user_function_dict[key] = lambda job: {key: item(job)}
        else:
            raise TypeError("unsupported function type!")

    def __getitem__(self, key: str) -> callable:
        return self._user_function_dict[key]

    def __getattr__(self, name: str) -> callable:
        if name in list(self._system_function_dict.keys()):
            self._system_function_dict[name] = True
            return self._system_function_dict[name]
        else:
            super(FunctionContainer, self).__getattr__(name)

    def __dir__(self) -> list:
        return list(self._system_function_dict.keys())


class JobFilters(object):
    """
    Certain predefined job filters

    """

    @staticmethod
    def job_type(job_type: str) -> callable:
        def filter_job_type(job):
            return job.__name__ == job_type

        return filter_job_type

    @staticmethod
    def job_name_contains(job_name_segment: str) -> callable:
        def filter_job_name_segment(job):
            return job_name_segment in job.job_name

        return filter_job_name_segment


class PyironTable:
    """
    Class for easy, efficient, and pythonic analysis of data from pyiron projects

    Args:
        project (pyiron_base.project.generic.Project): The project to analyze
        name (str): Name of the pyiron table
        system_function_lst (list/ None): List of built-in functions
    """

    def __init__(
        self,
        project: "pyiron_base.project.generic.Project",
        name: Optional[str] = None,
        system_function_lst: List[callable] = None,
        csv_file_name: Optional[str] = None,
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
    def filter(self) -> JobFilters:
        """
        Object containing pre-defined filter functions

        Returns:
            pyiron.table.datamining.JobFilters: The object containing the filters

        """
        return self._filter

    @property
    def name(self) -> str:
        """
        Name of the table. Takes the project name if not specified

        Returns:
            str: Name of the table

        """
        if self._name is None:
            return self._project.name
        return self._name

    @property
    def db_filter_function(self) -> Union[callable, None]:
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
    def db_filter_function(self, funct: callable) -> None:
        self._db_filter_function = funct

    @property
    def filter_function(self) -> Union[callable, None]:
        """
        Function to filter each job before more expensive functions are applied

        Example:

        >>> def job_filter_function(job):
        >>>     return (job.status == "finished") & ("murn" in job.job_name)

        >>> table.filter_function = job_filter_function
        """
        return self._filter_function

    @filter_function.setter
    def filter_function(self, funct: callable):
        self._filter_function = funct

    def _get_new_functions(self, file: FileHDFio) -> Tuple[List, List]:
        """
        Get new user-defined and system functions from an HDF5 file.

        Args:
            file (FileHDFio): The HDF5 file to extract data from.

        Returns:
            Tuple[List, List]: A tuple containing two lists:
                - new_user_functions (List): A list of new user-defined functions.
                - new_system_functions (List): A list of new system functions.

        Raises:
            IndexError: If an index is out of range.
            ValueError: If a value is not valid.
            TypeError: If a type is incorrect.
        """
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
        except (IndexError, ValueError, TypeError):
            new_user_functions = []
            new_system_functions = []
        return new_user_functions, new_system_functions

    def create_table(
        self,
        file: FileHDFio,
        job_status_list: List[str],
        executor: Optional["concurrent.futures.Executor"] = None,
        enforce_update: bool = False,
    ):
        """
        Create or update the table.

        If this method has been called before and there are new functions added to :attr:`.add`, apply them on the
        previously analyzed jobs.
        If this method has been called before and there are new jobs added to :attr:`.analysis_project`, apply all
        functions to them.

        The result is available via :meth:`.get_dataframe`.

        .. warning::
            The executor, if given, must not naively pickle the mapped functions or
            arguments, as PyironTable relies on lambda functions internally.  Use
            with executors that rely on dill or cloudpickle instead.  Pyiron
            provides such executors in the `executorlib` sub packages.

        Args:
            file (FileHDFio): HDF were the previous state of the table is stored
            job_status_list (list of str): only consider jobs with these statuses
            executor (concurrent.futures.Executor): executor for parallel execution
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
                    job_id_lst=self._get_job_ids(),
                    function_lst=function_lst,
                    executor=executor,
                )
                if len(df_new_keys) > 0:
                    self._df = pandas.concat([self._df, df_new_keys], axis="columns")

        new_jobs = self._collect_job_update_lst(
            job_status_list=job_status_list,
            job_stored_ids=self._get_job_ids() if not enforce_update else None,
        )
        if len(new_jobs) > 0:
            df_new_ids = self._iterate_over_job_lst(
                job_id_lst=new_jobs,
                function_lst=self.add._function_lst,
                executor=executor,
            )
            if len(df_new_ids) > 0:
                self._df = pandas.concat([self._df, df_new_ids], ignore_index=True)

    def get_dataframe(self) -> pandas.DataFrame:
        return self._df

    def _list_nodes(self) -> list:
        return list(self._df.columns)

    def __getitem__(self, item: str) -> Union[np.ndarray, None]:
        if item in self.list_nodes():
            return np.array(self._df[item])
        return None

    def __str__(self) -> str:
        return self._df.__str__()

    def __repr__(self) -> str:
        """
        Human readable string representation

        Returns:
            str: pandas Dataframe structure as string
        """
        return self._df.__repr__()

    @property
    def _file_name_csv(self) -> str:
        """
        Get the file name of the CSV file.

        Returns:
            str: The file name of the CSV file.
        """
        if self._csv_file is None:
            return self._project.path + self.name + ".csv"
        else:
            return self._csv_file

    def _load_csv(self) -> None:
        """
        Load the table from a CSV file.

        Returns:
            None
        """
        self._df = pandas.read_csv(self._file_name_csv)

    @staticmethod
    def _get_data_from_hdf5(hdf: FileHDFio) -> Tuple[dict, dict]:
        """
        Load user-defined and system function dictionaries from an HDF file.

        Args:
            hdf (FileHDFio): The HDF file object.

        Returns:
            Tuple[dict, dict]: A tuple containing two dictionaries:
                - temp_user_function_dict (dict): The user-defined function dictionary.
                - temp_system_function_dict (dict): The system function dictionary.
        """
        temp_user_function_dict = _from_pickle(hdf=hdf, key="user_function_dict")
        temp_system_function_dict = _from_pickle(hdf=hdf, key="system_function_dict")
        return temp_user_function_dict, temp_system_function_dict

    def _get_job_ids(self) -> np.ndarray:
        """
        Get the job IDs from the dataframe.

        Returns:
            np.ndarray: An array of job IDs.
        """
        if len(self._df) > 0:
            return self._df.job_id.values
        else:
            return np.array([])

    def _get_filtered_job_ids_from_project(self, recursive: bool = True) -> List[int]:
        """
        Get the filtered job IDs from the project.

        Args:
            recursive (bool): Flag to indicate whether to include jobs from subprojects (default is True).

        Returns:
            List[int]: A list of filtered job IDs.
        """
        project_table = self._project.job_table(recursive=recursive)
        filter_funct = self.db_filter_function
        return project_table[filter_funct(project_table)]["id"].tolist()

    def _iterate_over_job_lst(
        self,
        job_id_lst: List,
        function_lst: List,
        executor: concurrent.futures.Executor = None,
    ) -> List[dict]:
        """
        Apply functions to job.

        Any functions that raise an error are set to `None` in the final list.

        Args:
            job_id_lst (list of int): all job ids to analyze
            function_lst (list of functions): all functions to apply on jobs. Must return a dictionary.
            executor (concurrent.futures.Executor): executor for parallel execution

        Returns:
            list of dict: a list of the merged dicts from all functions for each job
        """
        job_to_analyse_lst = [
            [
                self._project.db.get_item_by_id(job_id),
                function_lst,
                self.convert_to_object,
            ]
            for job_id in job_id_lst
        ]
        if executor is not None:
            diff_dict_lst = list(
                tqdm(
                    executor.map(_apply_list_of_functions_on_job, job_to_analyse_lst),
                    total=len(job_to_analyse_lst),
                )
            )
        else:
            diff_dict_lst = list(
                tqdm(
                    map(_apply_list_of_functions_on_job, job_to_analyse_lst),
                    total=len(job_to_analyse_lst),
                )
            )
        self.refill_dict(diff_dict_lst)
        return pandas.DataFrame(diff_dict_lst)

    @staticmethod
    def total_lst_of_keys(diff_dict_lst: List[dict]) -> set:
        """
        Get unique list of all keys occuring in list.
        """
        total_key_lst = []
        for sub_dict in diff_dict_lst:
            for key in sub_dict.keys():
                total_key_lst.append(key)
        return set(total_key_lst)

    def refill_dict(self, diff_dict_lst: List) -> None:
        """
        Ensure that all dictionaries in the list have the same keys.

        Keys that are not in a dict are set to `None`.
        """
        total_key_lst = self.total_lst_of_keys(diff_dict_lst)
        for sub_dict in diff_dict_lst:
            for key in total_key_lst:
                if key not in sub_dict.keys():
                    sub_dict[key] = None

    def _collect_job_update_lst(
        self, job_status_list: List, job_stored_ids: Optional[List] = None
    ) -> List:
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
                job_update_lst.append(job_id)
        return job_update_lst

    def _repr_html_(self) -> str:
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
        self._job_with_calculate_function = True
        self.analysis_project = project.project

    @property
    def filter(self) -> JobFilters:
        return self._pyiron_table.filter

    @property
    def db_filter_function(self) -> Union[callable, None]:
        """
        function: database level filter function

        The function should accept a dataframe, the job table of :attr:`~.analysis_project` and return a bool index into
        it.  Jobs where the index is `False` are excluced from the analysis.
        """
        return self._pyiron_table.db_filter_function

    @db_filter_function.setter
    def db_filter_function(self, funct: callable) -> None:
        self._pyiron_table.db_filter_function = funct

    @property
    def filter_function(self) -> Union[callable, None]:
        """
        function: job level filter function

        The function should accept a GenericJob or JobCore object and return a bool, if it returns `False` the job is
        excluced from the analysis.
        """
        return self._pyiron_table.filter_function

    @filter_function.setter
    def filter_function(self, funct: callable) -> None:
        self._pyiron_table.filter_function = funct

    @property
    def job_status(self) -> List[str]:
        """
        list of str: only jobs with status in this list are included in the table.
        """
        return self._job_status

    @job_status.setter
    def job_status(self, status: Union[str, List[str]]) -> None:
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
    def pyiron_table(self) -> PyironTable:
        return self._pyiron_table

    @property
    @deprecate("Use analysis_project instead!")
    def ref_project(self) -> "pyiron_base.project.generic.Project":
        return self.analysis_project

    @ref_project.setter
    def ref_project(self, project: "pyiron_base.project.generic.Project") -> None:
        self.analysis_project = project

    @property
    def analysis_project(self) -> "pyiron_base.project.generic.Project":
        """
        :class:`.Project`: which pyiron project should be searched for jobs

        WARNING: setting this resets any previously added analysis and filter functions
        """
        return self._analysis_project

    @analysis_project.setter
    def analysis_project(self, project: "pyiron_base.project.generic.Project") -> None:
        self._analysis_project = project
        self._pyiron_table = PyironTable(
            project=self._analysis_project,
            system_function_lst=self._system_function_lst,
            csv_file_name=os.path.join(self.working_directory, "pyirontable.csv"),
        )

    @property
    def add(self) -> FunctionContainer:
        """
        Add a function to analyse job data

        Example:

        >>> def get_energy(job):
        >>>     return job["output/generic/energy_pot"][-1]

        >>> table.add["energy"] = get_energy
        """
        return self._pyiron_table.add

    @property
    def convert_to_object(self) -> bool:
        """
        bool: if `True` convert fully load jobs before passing them to functions, if `False` use inspect mode.
        """
        return self._pyiron_table.convert_to_object

    @convert_to_object.setter
    def convert_to_object(self, conv_to_obj: bool) -> None:
        self._pyiron_table.convert_to_object = conv_to_obj

    @property
    def enforce_update(self) -> bool:
        """
        bool: if `True` re-evaluate all function on all jobs when :meth:`.update_table` is called.
        """
        return self._enforce_update

    @enforce_update.setter
    def enforce_update(self, enforce: bool) -> None:
        """
        Set the enforce_update property.

        Args:
            enforce (bool): If True, re-evaluate all functions on all jobs when update_table is called.
        """
        if isinstance(enforce, bool):
            if enforce:
                self._enforce_update = True
                if self.status.finished:
                    self.status.created = True
            else:
                self._enforce_update = False
        else:
            raise TypeError("enforce must be a boolean")

    def _save_output(self) -> None:
        """
        Save the pyiron table dataframe to the HDF5 output file.

        Returns:
            None
        """
        with self.project_hdf5.open("output") as hdf5_output:
            self.pyiron_table._df.to_hdf(
                hdf5_output.file_name, key=hdf5_output.h5_path + "/table"
            )

    def _to_dict(self) -> dict:
        """
        Convert the TableJob object to a dictionary.

        Returns:
            dict: The TableJob object represented as a dictionary.
        """
        job_dict = super()._to_dict()
        job_dict["input/bool_dict"] = {
            "enforce_update": self._enforce_update,
            "convert_to_object": self._pyiron_table.convert_to_object,
        }
        if self._analysis_project is not None:
            job_dict["input/project"] = {
                "path": self._analysis_project.path,
                "user": self._analysis_project.user,
                "sql_query": self._analysis_project.sql_query,
                "filter": self._analysis_project._filter,
                "inspect_mode": self._analysis_project._inspect_mode,
            }
        add_dict = {}
        self._pyiron_table.add._to_hdf(add_dict)
        for k, v in add_dict.items():
            job_dict["input/" + k] = v
        if self.pyiron_table._filter_function is not None:
            _to_pickle(job_dict, "input/filter", self.pyiron_table._filter_function)
        if self.pyiron_table._db_filter_function is not None:
            _to_pickle(
                job_dict, "input/db_filter", self.pyiron_table._db_filter_function
            )
        return job_dict

    def _from_dict(self, obj_dict: dict, version: str = None):
        """
        Restore the TableJob object from a dictionary.

        Args:
            obj_dict (dict): The TableJob object represented as a dictionary.
            version (str): The version of the object.

        Returns:
            None
        """
        super()._from_dict(obj_dict=obj_dict, version=version)
        if "project" in obj_dict["input"].keys():
            project_dict = obj_dict["input"]["project"]
            if os.path.exists(project_dict["path"]):
                project = self.project.__class__(
                    path=project_dict["path"],
                    user=project_dict["user"],
                    sql_query=project_dict["sql_query"],
                )
                project._filter = project_dict["filter"]
                project._inspect_mode = project_dict["inspect_mode"]
                self.analysis_project = project
            else:
                self._logger.warning(
                    f"Could not instantiate analysis_project, no such path {project_dict['path']}."
                )
        if "filter" in obj_dict["input"].keys():
            self.pyiron_table.filter_function = _from_pickle(
                obj_dict["input"], "filter"
            )
        if "db_filter" in obj_dict["input"].keys():
            self.pyiron_table.db_filter_function = _from_pickle(
                obj_dict["input"], "db_filter"
            )
        bool_dict = obj_dict["input"]["bool_dict"]
        self._enforce_update = bool_dict["enforce_update"]
        self._pyiron_table.convert_to_object = bool_dict["convert_to_object"]
        self._pyiron_table.add._from_hdf(obj_dict["input"])

    def to_hdf(
        self,
        hdf: Optional["pyiron_base.storage.hdfio.ProjectHDFio"] = None,
        group_name: Optional[str] = None,
    ) -> None:
        """
        Store pyiron table job in HDF5

        Args:
            hdf (Optional[ProjectHDFio]): The HDF5 file object.
            group_name (Optional[str]): The name of the group in the HDF5 file.

        Returns:
            None
        """
        super(TableJob, self).to_hdf(hdf=hdf, group_name=group_name)
        if len(self.pyiron_table._df) != 0:
            self._save_output()

    def from_hdf(
        self,
        hdf: Optional["pyiron_base.storage.hdfio.ProjectHDFio"] = None,
        group_name: Optional[str] = None,
    ) -> None:
        """
        Restore pyiron table job from HDF5

        Args:
            hdf (Optional[ProjectHDFio]): The HDF5 file object.
            group_name (Optional[str]): The name of the group in the HDF5 file.

        Returns:
            None
        """
        super(TableJob, self).from_hdf(hdf=hdf, group_name=group_name)
        hdf_version = self.project_hdf5.get("HDF_VERSION", "0.1.0")
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

    def validate_ready_to_run(self) -> None:
        """
        Validate if the job is ready to run.

        Raises:
            ValueError: If the analysis project is not defined.
        """
        if self._analysis_project is None:
            raise ValueError("Analysis project not defined!")

    def run_static(self) -> None:
        """
        Run the static analysis job.

        This method creates the working directory, updates the table, and sets the job status to finished.
        """
        self._create_working_directory()
        self.status.running = True
        self.update_table()
        self.status.finished = True

    @deprecate(job_status_list="Use TableJob.job_status instead!")
    def update_table(self, job_status_list: Optional[List[str]] = None) -> None:
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
            if self._executor_type is None and self.server.cores > 1:
                self._executor_type = "executorlib.SingleNodeExecutor"
            if self._executor_type is not None:
                with self._get_executor(max_workers=self.server.cores) as exe:
                    self._pyiron_table.create_table(
                        file=hdf5_input,
                        job_status_list=job_status_list,
                        enforce_update=self._enforce_update,
                        executor=exe,
                    )
            else:
                self._pyiron_table.create_table(
                    file=hdf5_input,
                    job_status_list=job_status_list,
                    enforce_update=self._enforce_update,
                    executor=None,
                )
        self.to_hdf()
        self._pyiron_table._df.to_csv(
            os.path.join(self.working_directory, "pyirontable.csv"), index=False
        )
        self._save_output()
        self.run_time_to_db()

    def get_dataframe(self) -> pandas.DataFrame:
        """
        Returns aggregated results over all jobs.

        Returns:
            pandas.Dataframe
        """
        return self.pyiron_table._df


def always_true_pandas(job_table) -> "pandas.Series":
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


def _apply_list_of_functions_on_job(input_parameters: Tuple) -> dict:
    from pyiron_snippets.logger import logger

    from pyiron_base.jobs.job.path import JobPath

    db_entry, function_lst, convert_to_object = input_parameters
    job = JobPath.from_db_entry(db_entry)
    if convert_to_object:
        job = job.to_object()
        job.set_input_to_read_only()
    diff_dict = {}
    for funct in function_lst:
        try:
            diff_dict.update(funct(job))
        except Exception as e:
            logger.warn(f"Caught exception '{e}' when called on job {job.id}!")
    return diff_dict
