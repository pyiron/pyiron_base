# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The project object is the central import point of pyiron - all other objects can be created from this one
"""

from __future__ import annotations

import os
import posixpath
import shutil
import stat
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Literal, Optional, Union

import cloudpickle
import numpy as np
import pandas
from pyiron_snippets.deprecate import deprecate
from tqdm.auto import tqdm

from pyiron_base.database.filetable import FileTable
from pyiron_base.database.jobtable import (
    get_child_ids,
    get_job_id,
    get_job_status,
    get_job_working_directory,
    set_job_status,
)
from pyiron_base.interfaces.has_groups import HasGroups
from pyiron_base.jobs.flex.factory import create_job_factory
from pyiron_base.jobs.job.extension.server.generic import Server
from pyiron_base.jobs.job.extension.server.queuestatus import (
    queue_check_job_is_waiting_or_running,
    queue_delete_job,
    queue_enable_reservation,
    queue_is_empty,
    queue_table,
    update_from_remote,
    wait_for_job,
    wait_for_jobs,
)
from pyiron_base.jobs.job.jobtype import (
    JOB_CLASS_DICT,
    JobFactory,
    JobType,
    JobTypeChoice,
)
from pyiron_base.jobs.job.util import _get_safe_job_name, _special_symbol_replacements
from pyiron_base.project.archiving import export_archive, import_archive
from pyiron_base.project.data import ProjectData
from pyiron_base.project.delayed import DelayedObject, get_hash
from pyiron_base.project.external import Notebook
from pyiron_base.project.jobloader import JobInspector, JobLoader
from pyiron_base.project.path import ProjectPath
from pyiron_base.state import State, state
from pyiron_base.storage.hdfio import ProjectHDFio

if TYPE_CHECKING:
    pass

__author__ = "Joerg Neugebauer, Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


class Project(ProjectPath, HasGroups):
    """
    The project is the central class in pyiron, all other objects can be created from the project object.

    Implements :class:`.HasGroups`.  Groups are sub directories in the project, nodes are jobs inside the project.

    Args:
        path (GenericPath, str): path of the project defined by GenericPath, absolute or relative (with respect to
                                     current working directory) path
        user (str): current pyiron user
        sql_query (str): SQL query to only select a subset of the existing jobs within the current project
        default_working_directory (bool): Access default working directory, for ScriptJobs this equals the project
                                    directory of the ScriptJob for regular projects it falls back to the current
                                    directory.

    Attributes:
        root_path (): The pyiron user directory, defined in the .pyiron configuration.
        project_path (): The relative path of the current project / folder starting from the root path of the pyiron
                            user directory
        path (): The absolute path of the current project / folder.
        base_name (): The name of the current project / folder.
        history (): Previously opened projects / folders.
        parent_group (): Parent project - one level above the current project.
        user (): Current unix/linux/windows user who is running pyiron
        sql_query (): An SQL query to limit the jobs within the project to a subset which matches the SQL query.
        db (): Connection to the SQL database.
        job_type (): Job Type object with all the available job types: ['ExampleJob', 'ParallelMaster',
                        'ScriptJob', 'ListMaster'].
        data (pyiron_base.project.data.ProjectData): A storage container for project-level data.

    Examples:

        Storing data:
            >>> pr = Project('example')
            >>> pr.data.foo = 42
            >>> pr.data.write()
            Some time later or in a different notebook, but in the same file location...
            >>> other_pr_instance = Project('example')
            >>> print(pr.data)
            {'foo': 42}
    """

    def __init__(
        self,
        path: str = "",
        user: Optional[str] = None,
        sql_query: Optional[str] = None,
        default_working_directory: bool = False,
    ):
        if default_working_directory and path == "":
            inputdict = Notebook.get_custom_dict()
            if inputdict is not None and "project_dir" in inputdict.keys():
                path = inputdict["project_dir"]
            else:
                path = "."

        super(Project, self).__init__(path=path)

        self.user = user
        self.sql_query = sql_query
        self._filter = ["groups", "nodes", "objects"]
        self._inspect_mode = False
        self._data = None
        self._creator = Creator(project=self)
        self._loader = JobLoader(project=self)
        self._inspector = JobInspector(project=self)

        self.job_type = JobTypeChoice()

        self._maintenance = None

    @property
    def state(self) -> State:
        return state

    @property
    def db(self) -> Union["DatabaseAccess", FileTable]:
        if not state.database.database_is_disabled:
            return state.database.database
        else:
            return FileTable(index_from=self.path)

    @property
    def maintenance(self) -> "Maintenance":
        if self._maintenance is None:
            from pyiron_base.maintenance.generic import Maintenance

            self._maintenance = Maintenance(self)
        return self._maintenance

    @property
    def parent_group(self) -> "Project":
        """
        Get the parent group of the current project

        Returns:
            Project: parent project
        """
        return self.create_group("..")

    @property
    def name(self) -> str:
        """
        The name of the current project folder

        Returns:
            str: name of the current project folder
        """
        return self.base_name

    @property
    def create(self) -> Creator:
        return self._creator

    @property
    def data(self) -> ProjectData:
        if self._data is None:
            self._data = ProjectData(project=self, table_name="data")
            try:
                self._data.read()
            except KeyError:
                pass
        return self._data

    @property
    def size(self) -> float:
        """
        Get the size of the project
        """
        from pyiron_base.project.size import get_folder_size

        return get_folder_size(path=self.path)

    @property
    def conda_environment(self) -> "CondaEnvironment":
        try:
            from pyiron_base.project.condaenv import CondaEnvironment
        except ImportError:
            raise ImportError(
                "You need to have the conda python package installed to access conda environments."
            ) from None
        return CondaEnvironment(env_path=os.path.join(self.path, "conda"))

    def copy(self) -> "Project":
        """
        Copy the project object - copying just the Python object but maintaining the same pyiron path

        Returns:
            Project: copy of the project object
        """
        new = self.__class__(path=self.path, user=self.user, sql_query=self.sql_query)
        new._filter = self._filter
        new._inspect_mode = self._inspect_mode
        return new

    def copy_to(
        self, destination: "Project", delete_original_data: bool = False
    ) -> "Project":
        """
        Copy the project object to a different pyiron path - including the content of the project (all jobs).
        In order to move individual jobs, use `copy_to` from the job objects.

        Args:
            destination (Project): project path to copy the project content to
            delete_original_data (bool): delete the original data after copying - default=False

        Returns:
            Project: pointing to the new project path
        """
        if not isinstance(destination, Project):
            raise TypeError("A project can only be copied to another project.")
        for sub_project_name in tqdm(self.list_groups(), desc="Copying sub-projects"):
            if "_hdf5" not in sub_project_name:
                sub_project = self.open(sub_project_name)
                destination_sub_project = destination.open(sub_project_name)
                sub_project.copy_to(
                    destination_sub_project, delete_original_data=delete_original_data
                )
        for job_id in tqdm(self.get_job_ids(recursive=False), desc="Copying jobs"):
            ham = self.load(job_id)
            if delete_original_data:
                ham.move_to(destination)
            else:
                ham.copy_to(project=destination)
        if delete_original_data:
            for file in tqdm(self.list_files(), desc="Moving files"):
                shutil.move(os.path.join(self.path, file), destination.path)
            self.removedirs()
        else:
            for file in tqdm(self.list_files(), desc="Copying files"):
                if ".h5" not in file:
                    shutil.copy(os.path.join(self.path, file), destination.path)
                if self._data is not None:
                    shutil.copy(
                        os.path.join(self.path, "project_data.h5"), destination.path
                    )
        return destination

    def create_from_job(self, job_old: "GenericJob", new_job_name: str) -> "GenericJob":
        """
        Create a new job from an existing pyiron job

        Args:
            job_old (GenericJob): Job to copy
            new_job_name (str): New job name

        Returns:
            GenericJob: New job with the new job name.
        """
        job_id = self.get_job_id(new_job_name)
        if job_id is not None:
            state.logger.info(
                f"create_from_job: {new_job_name} has already job_id {job_id}!"
            )
            return None

        print("job_old: ", job_old.status)
        job_new = job_old.copy_to(
            project=self,
            new_job_name=new_job_name,
            input_only=False,
            new_database_entry=True,
        )
        state.logger.debug(
            "create_job:: {} {} from id {}".format(
                self.path, new_job_name, job_old.job_id
            )
        )
        return job_new

    def create_group(self, group: str) -> "Project":
        """
        Create a new subproject/ group/ folder

        Args:
            group (str): name of the new project

        Returns:
            Project: New subproject
        """
        new = self.copy()
        return new.open(group, history=False)

    @staticmethod
    def create_job_class(
        class_name: str,
        executable_str: str,
        write_input_funct: Optional[callable] = None,
        collect_output_funct: Optional[callable] = None,
        default_input_dict: Optional[dict] = None,
    ) -> None:
        """
        Create a new job class based on pre-defined write_input() and collect_output() function plus a dictionary of
        default inputs and an executable string.

        Args:
            class_name (str): A name for the newly created job class, so it is accessible via pr.create.job.<class_name>
            executable_str (str): Call to an external executable
            write_input_funct (callable): The write input function write_input(input_dict, working_directory)
            collect_output_funct (callable): The collect output function collect_output(working_directory)
            default_input_dict (dict): Default input for the newly created job class

        Example:

        >>> def write_input(input_dict, working_directory="."):
        >>>     with open(os.path.join(working_directory, "input_file"), "w") as f:
        >>>         f.write(str(input_dict["energy"]))
        >>>
        >>>
        >>> def collect_output(working_directory="."):
        >>>     with open(os.path.join(working_directory, "output_file"), "r") as f:
        >>>         return {"energy": float(f.readline())}
        >>>
        >>>
        >>> from pyiron_base import Project
        >>> pr = Project("test")
        >>> pr.create_job_class(
        >>>     class_name="CatJob",
        >>>     write_input_funct=write_input,
        >>>     collect_output_funct=collect_output,
        >>>     default_input_dict={"energy": 1.0},
        >>>     executable_str="cat input_file > output_file",
        >>> )
        >>> job = pr.create.job.CatJob(job_name="job_test")
        >>> job.input["energy"] = 2.0
        >>> job.run()
        >>> job.output
        """
        JOB_CLASS_DICT[class_name] = create_job_factory(
            write_input_funct=write_input_funct,
            collect_output_funct=collect_output_funct,
            default_input_dict=default_input_dict,
            executable_str=executable_str,
        )

    def wrap_executable(
        self,
        executable_str: str,
        job_name: Optional[str] = None,
        write_input_funct: Optional[callable] = None,
        collect_output_funct: Optional[callable] = None,
        input_dict: Optional[dict] = None,
        conda_environment_path: Optional[str] = None,
        conda_environment_name: Optional[str] = None,
        input_file_lst: Optional[list] = None,
        automatically_rename: bool = False,
        execute_job: bool = False,
        delayed: bool = False,
        output_file_lst: list = [],
        output_key_lst: list = [],
    ) -> "ExecutableContainerJob":
        """
        Wrap any executable into a pyiron job object using the ExecutableContainerJob.

        Args:
            executable_str (str): call to an external executable
            job_name (str): name of the new job object
            write_input_funct (callable): The write input function write_input(input_dict, working_directory)
            collect_output_funct (callable): The collect output function collect_output(working_directory)
            input_dict (dict): Default input for the newly created job class
            conda_environment_path (str): path of the conda environment
            conda_environment_name (str): name of the conda environment
            input_file_lst (list): list of files to be copied to the working directory before executing it\
            execute_job (boolean): automatically call run() on the job object - default false
            automatically_rename (bool): Whether to automatically rename the job at
                save-time to append a string based on the input values. (Default is
                False.)
            delayed (bool): delayed execution
            output_file_lst (list):
            output_key_lst (list):

        Example:

        >>> def write_input(input_dict, working_directory="."):
        >>>     with open(os.path.join(working_directory, "input_file"), "w") as f:
        >>>         f.write(str(input_dict["energy"]))
        >>>
        >>>
        >>> def collect_output(working_directory="."):
        >>>     with open(os.path.join(working_directory, "output_file"), "r") as f:
        >>>         return {"energy": float(f.readline())}
        >>>
        >>>
        >>> from pyiron_base import Project
        >>> pr = Project("test")
        >>> job = pr.wrap_executable(
        >>>     job_name="Cat_Job_energy_1_0",
        >>>     write_input_funct=write_input,
        >>>     collect_output_funct=collect_output,
        >>>     input_dict={"energy": 1.0},
        >>>     executable_str="cat input_file > output_file",
        >>>     execute_job=True,
        >>> )
        >>> print(job.output)

        Returns:
            pyiron_base.jobs.flex.ExecutableContainerJob: pyiron job object
        """

        def generate_job_hash(
            project,
            input_internal_dict,
            executable_internal_str,
            internal_file_lst,
            internal_job_name=None,
        ):
            job = create_job_factory(
                write_input_funct=write_input_funct,
                collect_output_funct=collect_output_funct,
                default_input_dict=input_internal_dict,
                executable_str=executable_internal_str,
            )(project=project, job_name=internal_job_name)
            if internal_file_lst is not None and len(internal_file_lst) > 0:
                for file in internal_file_lst:
                    job.restart_file_list.append(file)
            return (
                internal_job_name
                + "_"
                + get_hash(
                    binary=cloudpickle.dumps(
                        {
                            "write_input": write_input_funct,
                            "collect_output": collect_output_funct,
                            "kwargs": job.calculate_kwargs,
                        }
                    )
                )
            )

        def create_executable_job(
            project: Project,
            input_internal_dict: Dict[str, any],
            executable_internal_str: str,
            internal_file_lst: List[str],
            internal_job_name: Optional[str] = None,
            internal_execute_job: bool = True,
            internal_auto_rename: bool = False,
            _server_obj: Server = None,
        ) -> Project:
            """
            Create an executable job.

            Args:
                project (Project): The project object.
                input_internal_dict (Dict[str, any]): The input dictionary for the job.
                executable_internal_str (str): The executable string.
                internal_file_lst (List[str]): The list of files to be copied to the working directory.
                internal_job_name (str, optional): The name of the job. Defaults to None.
                internal_execute_job (bool, optional): Whether to execute the job. Defaults to True.
                internal_auto_rename (bool, optional): Whether to automatically rename the job. Defaults to False.
                _server_obj (Server): Server object to define the resource requirements for the executable

            Returns:
                Project: The project object.

            """
            if internal_job_name is None:
                internal_job_name = "exe"
                internal_auto_rename = True
            if internal_auto_rename:
                internal_job_name = generate_job_hash(
                    project=project,
                    input_internal_dict=input_internal_dict,
                    executable_internal_str=executable_internal_str,
                    internal_file_lst=internal_file_lst,
                    internal_job_name=internal_job_name,
                )
            job_id = get_job_id(
                database=project.db,
                sql_query=project.sql_query,
                user=project.user,
                project_path=project.project_path,
                job_specifier=internal_job_name,
            )
            if job_id is None:
                job = create_job_factory(
                    write_input_funct=write_input_funct,
                    collect_output_funct=collect_output_funct,
                    default_input_dict=input_internal_dict,
                    executable_str=executable_internal_str,
                )(project=project, job_name=internal_job_name)
            else:
                return project.load(job_specifier=job_id)
            if _server_obj is not None:
                job.server = _server_obj
            if conda_environment_path is not None:
                job.server.conda_environment_path = conda_environment_path
            elif conda_environment_name is not None:
                job.server.conda_environment_name = conda_environment_name
            if internal_file_lst is not None and len(internal_file_lst) > 0:
                for file in internal_file_lst:
                    job.restart_file_list.append(file)
            if internal_execute_job:
                job.run()
            return job

        if delayed:
            return DelayedObject(
                function=create_executable_job,
                output_key=None,
                output_file=None,
                output_file_lst=[f.replace(".", "_") for f in output_file_lst],
                output_key_lst=output_key_lst,
                project=self,
                input_internal_dict=input_dict,
                executable_internal_str=executable_str,
                internal_file_lst=input_file_lst,
                internal_job_name=job_name,
                internal_auto_rename=automatically_rename,
                internal_execute_job=True,
            )
        else:
            return create_executable_job(
                project=self,
                input_internal_dict=input_dict,
                executable_internal_str=executable_str,
                internal_file_lst=input_file_lst,
                internal_job_name=job_name,
                internal_auto_rename=automatically_rename,
                internal_execute_job=execute_job,
            )

    def create_job(
        self, job_type: str, job_name: str, delete_existing_job: bool = False
    ) -> "GenericJob":
        """
        Create one of the following jobs:
        - 'ExampleJob': example job just generating random number
        - 'ParallelMaster': series of jobs run in parallel
        - 'ScriptJob': Python script or jupyter notebook job container
        - 'ListMaster': list of jobs

        Args:
            job_type (str): job type can be ['ExampleJob', 'ParallelMaster', 'ScriptJob', 'ListMaster']
            job_name (str): name of the job
            delete_existing_job (bool): delete an existing job - default false

        Returns:
            GenericJob: job object depending on the job_type selected
        """
        job_name = _get_safe_job_name(name=job_name)
        job = JobType(
            job_type,
            project=ProjectHDFio(project=self.copy(), file_name=job_name),
            job_name=job_name,
            job_class_dict=self.job_type.job_class_dict,
            delete_existing_job=delete_existing_job,
        )
        if self.user is not None:
            job.user = self.user
        return job

    def create_table(
        self, job_name: str = "table", delete_existing_job: bool = False
    ) -> "TableJob":
        """
        Create pyiron table

        Args:
            job_name (str): job name of the pyiron table job
            delete_existing_job (bool): Delete the existing table and run the analysis again.

        Returns:
            pyiron.table.datamining.TableJob
        """
        table = self.create_job(
            job_type=self.job_type.TableJob,
            job_name=job_name,
            delete_existing_job=delete_existing_job,
        )
        table.analysis_project = self
        return table

    def wrap_python_function(
        self,
        python_function: callable,
        *args,
        job_name: Optional[str] = None,
        automatically_rename: bool = True,
        execute_job: bool = False,
        delayed: bool = False,
        output_file_lst: list = [],
        output_key_lst: list = [],
        **kwargs,
    ) -> "PythonFunctionContainerJob":
        """
        Create a pyiron job object from any python function

        Args:
            python_function (callable): python function to create a job object from
            *args: Arguments for the user-defined python function
            job_name (str | None): The name for the created job. (Default is None, use
                the name of the function.)
            automatically_rename (bool): Whether to automatically rename the job at
                save-time to append a string based on the input values. (Default is
                True.)
            delayed (bool): delayed execution
            execute_job (boolean): automatically call run() on the job object - default false
            **kwargs: Keyword-arguments for the user-defined python function

        Returns:
            pyiron_base.jobs.flex.pythonfunctioncontainer.PythonFunctionContainerJob: pyiron job object

        Example:

        >>> def test_function(a, b=8):
        >>>     return a+b
        >>>
        >>> from pyiron_base import Project
        >>> pr = Project("test")
        >>> job = pr.wrap_python_function(test_function)
        >>> job.input["a"] = 4
        >>> job.input["b"] = 5
        >>> job.run()
        >>> job.output
        >>>
        >>> test_function_wrapped = pr.wrap_python_function(test_function)
        >>> test_function_wrapped(4, b=6)

        """

        def create_function_job(
            *args, _server_obj=None, _return_job_object=False, **kwargs
        ):
            job = self.create.job.PythonFunctionContainerJob(
                job_name=python_function.__name__ if job_name is None else job_name
            )
            job._automatically_rename_on_save_using_input = automatically_rename
            job.python_function = python_function
            if _server_obj is not None:
                job.server = _server_obj
            if _return_job_object:
                job.set_input(*args, **kwargs)
                return job
            else:
                return job(*args, **kwargs)

        if delayed:
            return DelayedObject(
                function=create_function_job,
                *args,
                output_key=None,
                output_file=None,
                output_file_lst=output_file_lst,
                output_key_lst=output_key_lst,
                input_prefix_key="kwargs",
                **kwargs,
            )
        else:
            job = self.create.job.PythonFunctionContainerJob(
                job_name=python_function.__name__ if job_name is None else job_name
            )
            job._automatically_rename_on_save_using_input = automatically_rename
            job.python_function = python_function
            if args or len(kwargs) != 0:
                job.set_input(*args, **kwargs)
            if execute_job:
                job.run()
                return job.output["result"]
            else:
                return job

    def get_child_ids(
        self, job_specifier: Union[str, int], project: Optional["Project"] = None
    ) -> List[int]:
        """
        Get the childs for a specific job

        Args:
            job_specifier (str, int): name of the job or job ID
            project (Project): Project the job is located in - optional

        Returns:
            list: list of child IDs
        """
        if project is None:
            project = self.project_path
        return get_child_ids(
            database=self.db,
            sql_query=self.sql_query,
            user=self.user,
            project_path=project,
            job_specifier=job_specifier,
        )

    def get_db_columns(self) -> List[str]:
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
        return self.db.get_table_headings()

    def get_jobs(
        self, recursive: bool = True, columns: Optional[List[str]] = None
    ) -> dict:
        """
        Internal function to return the jobs as dictionary rather than a pandas.Dataframe

        Args:
            recursive (bool): search subprojects [True/False]
            columns (list): by default only the columns ['id', 'project'] are selected, but the user can select a subset
                            of ['id', 'status', 'chemicalformula', 'job', 'subjob', 'project', 'projectpath',
                            'timestart', 'timestop', 'totalcputime', 'computer', 'hamilton', 'hamversion', 'parentid',
                            'masterid']

        Returns:
            dict: columns are used as keys and point to a list of the corresponding values
        """
        return self.db.get_jobs(
            sql_query=self.sql_query,
            user=self.user,
            project_path=self.project_path,
            recursive=recursive,
            columns=columns,
        )

    def get_job_ids(self, recursive: bool = True) -> List[int]:
        """
        Return the job IDs matching a specific query

        Args:
            recursive (bool): search subprojects [True/False]

        Returns:
            list: a list of job IDs
        """
        return self.db.get_job_ids(
            sql_query=self.sql_query,
            user=self.user,
            project_path=self.project_path,
            recursive=recursive,
        )

    def get_job_id(self, job_specifier: Union[str, int]) -> int:
        """
        get the job_id for job named job_name in the local project path from database

        Args:
            job_specifier (str, int): name of the job or job ID

        Returns:
            int: job ID of the job
        """
        return get_job_id(
            database=self.db,
            sql_query=self.sql_query,
            user=self.user,
            project_path=self.project_path,
            job_specifier=job_specifier,
        )

    def get_job_status(
        self, job_specifier: Union[str, int], project: Optional["Project"] = None
    ) -> str:
        """
        Get the status of a particular job

        Args:
            job_specifier (str, int): name of the job or job ID
            project (Project): Project the job is located in - optional

        Returns:
            str: job status can be one of the following ['initialized', 'appended', 'created', 'submitted', 'running',
                 'aborted', 'collect', 'suspended', 'refresh', 'busy', 'finished']
        """
        if project is None:
            project = self.project_path
        return get_job_status(
            database=self.db,
            sql_query=self.sql_query,
            user=self.user,
            project_path=project,
            job_specifier=job_specifier,
        )

    def get_job_working_directory(
        self, job_specifier: Union[str, int], project: Optional["Project"] = None
    ) -> str:
        """
        Get the working directory of a particular job

        Args:
            job_specifier (str, int): name of the job or job ID
            project (Project): Project the job is located in - optional

        Returns:
            str: working directory as absolute path
        """
        if project is None:
            project = self.project_path
        return get_job_working_directory(
            sql_query=self.sql_query,
            user=self.user,
            project_path=project,
            database=self.db,
            job_specifier=job_specifier,
        )

    @deprecate("use self.size instead.")
    def get_project_size(self) -> float:
        """
        Get the size of the project.

        Returns:
            float: project size
        """
        return self.size

    @deprecate("use maintenance.get_repository_status() instead.")
    def get_repository_status(self) -> pandas.DataFrame:
        return self.maintenance.get_repository_status()

    def groups(self):
        """
        Filter project by groups

        Returns:
            Project: a project which is filtered by groups
        """
        new = self.copy()
        new._filter = ["groups"]
        return new

    @property
    def inspect(self) -> JobInspector:
        return self._inspector

    def iter_jobs(
        self,
        path: str = None,
        recursive: bool = True,
        convert_to_object: bool = True,
        progress: bool = True,
        **kwargs: dict,
    ) -> Generator:
        """
        Iterate over the jobs within the current project and it is sub projects

        Args:
            path (str): HDF5 path inside each job object. (Default is None, which just uses the top level of the job's
                HDF5 path.)
            recursive (bool): search subprojects. (Default is True.)
            convert_to_object (bool): load the full GenericJob object, else just return the HDF5 / JobCore object.
                                     (Default is True, convert everything to the full python object.)
            progress (bool): add an interactive progress bar to the iteration. (Default is True, show the bar.)
            **kwargs (dict): Optional arguments for filtering with keys matching the project database column name
                            (eg. status="finished"). Asterisk can be used to denote a wildcard, for zero or more
                            instances of any character

        Returns:
            yield: Yield of GenericJob or JobCore

        Note:
            The default behavior of converting to object can cause **significant** slowdown in larger projects. In this
            case, you may seriously wish to consider setting `convert_to_object=False` and access only the HDF5/JobCore
            representation of the jobs instead.
        """
        job_table = self.job_table(recursive=recursive, **kwargs)
        if not isinstance(self.db, FileTable):
            job_lst = [[job_id, None] for job_id in job_table["id"]]
        else:
            # From all the possible database columns, the following ones are removed:
            # ["id", "chemicalformula", "timestart", "computer", "parentid",
            #  "username", "timestop", "totalcputime", "masterid"]
            # because those are not used when running without database and can lead errors.
            table_columns = [
                "job",
                "subjob",
                "projectpath",
                "project",
                "status",
                "hamilton",
                "hamversion",
            ]
            job_lst = [
                [None, {column: db_entry[column] for column in table_columns}]
                for db_entry in [row[1].to_dict() for row in job_table.iterrows()]
            ]

        if progress:
            job_lst = tqdm(job_lst)
        for job_id, db_entry in job_lst:
            if path is not None:
                yield self.load_from_jobpath(
                    job_id=job_id,
                    db_entry=db_entry,
                    convert_to_object=False,
                )[path]
            else:  # Backwards compatibility - in future the option convert_to_object should be removed
                yield self.load_from_jobpath(
                    job_id=job_id,
                    db_entry=db_entry,
                    convert_to_object=convert_to_object,
                )

    def iter_output(self, recursive: bool = True) -> Generator:
        """
        Iterate over the output of jobs within the current project and it is sub projects

        Args:
            recursive (bool): search subprojects [True/False] - True by default

        Returns:
            yield: Yield of GenericJob or JobCore
        """
        return self.iter_jobs(path="output", recursive=recursive)

    def iter_groups(self, progress: bool = True) -> Generator:
        """
        Iterate over the groups within the current project

        Args:
            progress (bool): Display a progress bar during the iteration

        Yields:
            :class:`.Project`: sub projects/ groups/ folders
        """
        groups = self.list_groups()
        if progress:
            groups = tqdm(groups)
        for group in groups:
            if progress:
                groups.set_postfix(group=group)
            yield self[group]

    def items(self) -> list:
        """
        All items in the current project - this includes jobs, sub projects/ groups/ folders and any kind of files

        Returns:
            list: items in the project
        """
        return [(key, self[key]) for key in self.keys()]

    def update_from_remote(
        self,
        recursive: bool = True,
        ignore_exceptions: bool = False,
        try_collecting: bool = False,
    ):
        """
        Update jobs from the remote server

        Args:
            recursive (bool): search subprojects [True/False] - default=True
            ignore_exceptions (bool): ignore eventual exceptions when retrieving jobs - default=False

        Returns:
            returns None if ignore_exceptions is False or when no error occured.
            returns a list with job ids when errors occured, but were ignored

        """
        return update_from_remote(
            project=self,
            recursive=recursive,
            ignore_exceptions=ignore_exceptions,
            try_collecting=try_collecting,
        )

    def job_table(
        self,
        recursive: bool = True,
        columns: Optional[List[str]] = None,
        all_columns: bool = True,
        sort_by: str = "id",
        full_table: bool = False,
        element_lst: Optional[List[str]] = None,
        job_name_contains: str = "",
        auto_refresh_job_status: bool = False,
        mode: Literal["regex", "glob"] = "glob",
        **kwargs: dict,
    ):
        """
        auto_refresh_job_status (bool): will automatically reload job status by calling refresh_job_status() upon calling job_table
        """
        if not isinstance(self.db, FileTable) and auto_refresh_job_status:
            self.refresh_job_status()
        job_table = self.db.job_table(
            sql_query=self.sql_query,
            user=self.user,
            project_path=self.project_path,
            recursive=recursive,
            columns=columns,
            all_columns=all_columns,
            sort_by=sort_by,
            full_table=full_table,
            element_lst=element_lst,
            mode=mode,
            **kwargs,
        )
        if not isinstance(self.db, FileTable) or not auto_refresh_job_status:
            return job_table
        else:
            return self._refresh_job_status_file_table(df=job_table)

    job_table.__doc__ = "\n".join(
        [
            ll
            for ll in FileTable.job_table.__doc__.split("\n")
            if not any(
                [
                    item in ll
                    for item in ["sql_query (str)", "user (str)", "project_path (str)"]
                ]
            )
        ]
    )

    def get_jobs_status(self, recursive: bool = True, **kwargs) -> pandas.Series:
        """
        Gives a overview of all jobs status.

        Args:
            recursive (bool): search subprojects [True/False] - default=True
            kwargs: passed directly to :method:`.job_table` and can be used to filter jobs you want to have the status
            for

        Returns:
            pandas.Series: prints an overview of the job status.
        """
        df = self.job_table(recursive=recursive, all_columns=True, **kwargs)
        return df["status"].value_counts()

    def keys(self) -> list:
        """
        List of file-, folder- and objectnames

        Returns:
            list: list of the names of project directories and project nodes
        """
        return self.list_dirs() + self.list_nodes()

    def _list_all(self) -> dict:
        """
        Combination of list_groups(), list_nodes() and list_files() all in one dictionary with the corresponding keys:
        - 'groups': Subprojects/ -folder/ -groups.
        - 'nodes': Jobs or pyiron objects
        - 'files': Files inside a project which do not belong to any pyiron object

        Returns:
            dict: dictionary with all items in the project
        """
        return {
            "groups": self.list_groups(),
            "nodes": self.list_nodes(),
            "files": self.list_files(),
        }

    def list_dirs(self, skip_hdf5: bool = True) -> list:
        """
        List directories inside the project

        Args:
            skip_hdf5 (bool): Skip directories which belong to a pyiron object/ pyiron job - default=True

        Returns:
            list: list of directory names
        """
        if "groups" not in self._filter:
            return []
        files = set(next(os.walk(self.path))[2])
        dirs = set(os.listdir(self.path)) - files
        dirs = sorted([direct for direct in dirs if not (direct[0] == ".")])
        if skip_hdf5:
            return [d for d in dirs if not self._is_hdf5_dir(d)]
        return dirs

    def list_files(self, extension: Optional[str] = None) -> list:
        """
        List files inside the project

        Args:
            extension (str): filter by a specific extension

        Returns:
            list: list of file names
        """
        if "nodes" not in self._filter:
            return []
        try:
            files = next(os.walk(self.path))[2]
            if extension is None:
                return files
            return [
                ".".join(f.split(".")[:-1])
                for f in files
                if f.split(".")[-1] in extension
            ]
        except StopIteration:
            return []

    _list_groups = list_dirs

    def _list_nodes(self, recursive: bool = False) -> list:
        """
        List nodes/ jobs/ pyiron objects inside the project

        Args:
            recursive (bool): search subprojects [True/False] - default=False

        Returns:
            list: list of nodes/ jobs/ pyiron objects inside the project
        """
        if "nodes" not in self._filter:
            return []
        return self.get_jobs(recursive=recursive, columns=["job"])["job"]

    @property
    def load(self) -> JobLoader:
        return self._loader

    load.__doc__ = JobLoader.__doc__

    def load_from_jobpath(
        self,
        job_id: Optional[int] = None,
        db_entry: Optional[dict] = None,
        convert_to_object: bool = True,
    ) -> Union["GenricJob", "JobCore"]:
        """
        Internal function to load an existing job either based on the job ID or based on the database entry dictionary.

        Args:
            job_id (int/ None): Job ID - optional, but either the job_id or the db_entry is required.
            db_entry (dict): database entry dictionary - optional, but either the job_id or the db_entry is required.
            convert_to_object (bool): convert the object to an pyiron object or only access the HDF5 file - default=True
                                      accessing only the HDF5 file is about an order of magnitude faster, but only
                                      provides limited functionality. Compare the GenericJob object to JobCore object.

        Returns:
            GenericJob, JobCore: Either the full GenericJob object or just a reduced JobCore object
        """
        from pyiron_base.jobs.job.path import JobPath

        if job_id is not None:
            job = JobPath.from_job_id(db=self.db, job_id=job_id)
            if convert_to_object:
                job = job.to_object()
                job.reset_job_id(job_id=job_id)
                job.set_input_to_read_only()
            return job
        elif db_entry is not None:
            job = JobPath.from_db_entry(db_entry)
            if convert_to_object:
                job = job.to_object()
                job.set_input_to_read_only()
            return job
        else:
            raise ValueError("Either a job ID or an database entry has to be provided.")

    def move_to(self, destination: "Project") -> None:
        """Same as copy_to() but deletes the original project after copying"""
        self.copy_to(destination=destination, delete_original_data=True)

    def nodes(self) -> "Project":
        """
        Filter project by nodes

        Returns:
            Project: a project which is filtered by nodes
        """
        new = self.copy()
        new._filter = ["nodes"]
        return new

    def queue_table(
        self,
        project_only: bool = True,
        recursive: bool = True,
        full_table: bool = False,
    ) -> pandas.DataFrame:
        """
        Display the queuing system table as pandas.Dataframe

        Args:
            project_only (bool): Query only for jobs within the current project - True by default
            recursive (bool): Include jobs from sub projects
            full_table (bool): Whether to show the entire pandas table

        Returns:
            pandas.DataFrame: Output from the queuing system - optimized for the Sun grid engine
        """
        if not isinstance(self.db, FileTable):
            return queue_table(
                job_ids=self.get_job_ids(recursive=recursive),
                project_only=project_only,
                full_table=full_table,
            )
        else:
            return queue_table(
                project_only=project_only,
                full_table=full_table,
                working_directory_lst=[self.path],
            )

    def queue_table_global(self, full_table: bool = False) -> pandas.DataFrame:
        """
        Display the queuing system table as pandas.Dataframe

        Args:
            full_table (bool): Whether to show the entire pandas table

        Returns:
            pandas.DataFrame: Output from the queuing system - optimized for the Sun grid engine
        """
        df = queue_table(job_ids=[], project_only=False, full_table=full_table)
        if len(df) != 0 and self.db is not None:
            if not isinstance(self.db, FileTable):
                return pandas.DataFrame(
                    [
                        self.db.get_item_by_id(
                            int(str(queue_ID).replace("pi_", "").replace(".sh", ""))
                        )
                        for queue_ID in df["jobname"]
                        if str(queue_ID).startswith("pi_")
                    ]
                )
            else:

                def get_id_from_job_table(
                    job_table: pandas.DataFrame, job_path: str
                ) -> int:
                    job_dir = "_hdf5".join(job_path.split("_hdf5")[:-1])
                    job_name = os.path.basename(job_dir)
                    project = os.path.dirname(job_dir) + "/"
                    return job_table[
                        (job_table.job == job_name) & (job_table.project == project)
                    ].id.values[0]

                job_table_df = self.job_table()

                return pandas.DataFrame(
                    [
                        self.db.get_item_by_id(
                            int(
                                get_id_from_job_table(
                                    job_table=job_table_df, job_path=working_directory
                                )
                            )
                        )
                        for queue_ID, working_directory in zip(
                            df["jobname"], df["working_directory"]
                        )
                        if str(queue_ID).startswith("pi_")
                    ]
                )
        else:
            return None

    def refresh_job_status(
        self, *jobs, by_status: List[str] = ["running", "submitted"]
    ) -> None:
        """
        Check if job is still running or crashed on the cluster node.

        If `jobs` is not given, check for all jobs listed as running in the current project.

        Args:
            *jobs (str, int): name of the job or job ID, any number of them
            by_status (iterable of str): if not jobs are given, select all jobs
                with the given status in this project
        """
        if len(jobs) == 0:
            df = self.job_table()
            jobs = df[df.status.isin(by_status)].id
        if self.db is not None:
            for job_specifier in jobs:
                if isinstance(job_specifier, str):
                    job_id = get_job_id(
                        database=self.db,
                        sql_query=self.sql_query,
                        user=self.user,
                        project_path=self.project_path,
                        job_specifier=job_specifier,
                    )
                else:
                    job_id = job_specifier
                self.refresh_job_status_based_on_job_id(job_id)
        else:
            raise ValueError("Must have established database connection!")

    @deprecate("use refresh_job_status()")
    def refresh_job_status_based_on_queue_status(
        self, job_specifier: Union[str, int], status: str = "running"
    ) -> None:
        """
        Check if the job is still listed as running, while it is no longer listed in the queue.

        Args:
            job_specifier (str, int): name of the job or job ID
            status (str): Currently only the jobstatus of 'running' jobs can be refreshed - default='running'
        """
        if status != "running":
            raise NotImplementedError()
        self.refresh_job_status(job_specifier)

    def refresh_job_status_based_on_job_id(
        self, job_id: int, que_mode: bool = True
    ) -> None:
        """
        Internal function to check if a job is still listed 'running' in the job_table while it is no longer listed in
        the queuing system. In this case update the entry in the job_table to 'aborted'.

        Args:
            job_id (int): job ID
            que_mode (bool): [True/False] - default=True
        """
        if job_id and self.db is not None:
            if (
                not que_mode
                and self.db.get_item_by_id(job_id)["status"] not in ["finished"]
            ) or (
                que_mode
                and self.db.get_item_by_id(job_id)["status"] in ["running", "submitted"]
            ):
                job = self.inspect(job_id)
                # a job can be in status running or submitted without being on
                # the queue, if the run mode is worker or non_modal.  In this
                # case we do not want to check the queue status, so we just
                # short circuit here.
                if job["server"]["run_mode"] in ["worker", "non_modal"]:
                    return
                if not self.queue_check_job_is_waiting_or_running(job):
                    self.db.set_job_status(job_id=job_id, status="aborted")

    @staticmethod
    def _refresh_job_status_file_table(df: pandas.DataFrame) -> pandas.DataFrame:
        """
        Internal function to refresh the job table and update the job table with the status from the queuing system.

        Args:
            df (pandas.DataFrame): job table from the file based database

        Returns:
            pandas.DataFrame: updated job table with status from the queuing system
        """

        def convert_queue_status(queue_status: str) -> str:
            return {"pending": "submitted"}.get(queue_status, default=queue_status)

        df_queue = state.queue_adapter.get_status_of_my_jobs()

        status_lst = df.status.values.tolist()
        working_dir_lst = df.project + df.job + "_hdf5/" + df.job
        for i, [working_dir, status] in enumerate(
            zip(working_dir_lst, status_lst.copy())
        ):
            if status == "initialized":
                df_tmp = df_queue[df_queue.working_directory == working_dir]
                if len(df_tmp) > 0:
                    status_lst[i] = convert_queue_status(
                        queue_status=df_tmp.status.values[0]
                    )
        df["status"] = status_lst

        return df

    def remove_file(self, file_name: str) -> None:
        """
        Remove a file (same as unlink()) - copied from os.remove()

        If dir_fd is not None, it should be a file descriptor open to a directory,
          and path should be relative; path will then be relative to that directory.
        dir_fd may not be implemented on your platform.
          If it is unavailable, using it will raise a NotImplementedError.

        Args:
            file_name (str): name of the file
        """
        os.remove(posixpath.join(self.path, file_name))

    def remove_job(
        self, job_specifier: Union[str, int], _unprotect: bool = False
    ) -> None:
        """
        Remove a single job from the project based on its job_specifier - see also remove_jobs()

        Args:
            job_specifier (str, int): name of the job or job ID
            _unprotect (bool): [True/False] delete the job without validating the dependencies to other jobs
                               - default=False
        """
        if isinstance(job_specifier, (list, np.ndarray)):
            for job_id in job_specifier:
                self.remove_job(job_specifier=job_id, _unprotect=_unprotect)
            return
        job = self.inspect(job_specifier=job_specifier)
        if job is None:
            state.logger.warning(
                "Job '%s' does not exist and could not be removed",
                str(job_specifier),
            )
            return
        try:
            if _unprotect:
                job.remove_child()
            else:
                job.remove()
        except IOError as _:
            state.logger.debug(
                "hdf file does not exist. Removal from database will be attempted."
            )
            self.db.delete_item(job.id)

    def remove_jobs(
        self, recursive: bool = False, progress: bool = True, silently: bool = False
    ) -> None:
        """
        Remove all jobs in the current project and in all subprojects if recursive=True is selected - see also
        remove_job().

        For safety, the user is asked via input() to confirm the removal. To bypass this
        interactive interruption, use `remove_jobs(silently=True)`.

        Args:
            recursive (bool): [True/False] delete all jobs in all subprojects - default=False
            progress (bool): if True (default), add an interactive progress bar to the iteration
            silently (bool): if True the safety check is disabled - default=False
        """
        if not isinstance(recursive, bool):
            raise ValueError("recursive must be a boolean")
        if silently:
            confirmed = "y"
        else:
            confirmed = None
        while confirmed not in ["y", "n"]:
            if confirmed is None:
                confirmed = input(
                    "Are you sure you want to delete all jobs from "
                    + f"'{self.base_name}'? y/(n)"
                ).lower()
            else:
                confirmed = input(
                    "Invalid response. Please enter 'y' (yes) or 'n' (no): "
                ).lower()
        if confirmed == "y":
            self._remove_jobs_helper(recursive=recursive, progress=progress)
        else:
            print(f"No jobs removed from '{self.base_name}'.")

    @deprecate(
        message="Use pr.remove_jobs(silently=True) rather than pr.remove_jobs_silently()."
    )
    def remove_jobs_silently(
        self, recursive: bool = False, progress: bool = True
    ) -> None:
        self.remove_jobs(recursive=recursive, progress=progress, silently=True)

    def compress_jobs(self, recursive: bool = False) -> None:
        """
        Compress all finished jobs in the current project and in all subprojects if recursive=True is selected.

        Args:
            recursive (bool): [True/False] compress all jobs in all subprojects - default=False
        """
        for job_id in self.get_job_ids(recursive=recursive):
            job = self.inspect(job_id)
            if job.status == "finished":
                job.compress()

    def delete_output_files_jobs(self, recursive: bool = False) -> None:
        """
        Delete the output files of all finished jobs in the current project and in all subprojects if recursive=True is
        selected.

        Args:
            recursive (bool): [True/False] delete the output files of all jobs in all subprojects - default=False
        """
        for job_id in self.get_job_ids(recursive=recursive):
            job = self.inspect(job_id)
            if job.status == "finished":
                for file in job.files.list():
                    fullname = os.path.join(job.working_directory, file)
                    if os.path.isfile(fullname) and ".h5" not in fullname:
                        os.remove(fullname)
                    elif os.path.isdir(fullname):
                        os.removedirs(fullname)

    def remove(self, enable: bool = False, enforce: bool = False) -> None:
        """
        Delete all the whole project including all jobs in the project and its subprojects

        Args:
            enforce (bool): [True/False] delete jobs even though they are used in other projects - default=False
            enable (bool): [True/False] enable this command.
        """
        if enable is not True:
            raise ValueError(
                "To prevent users from accidentally deleting files - enable has to be set to True."
            )
        self._remove_jobs_helper(recursive=True)
        for file in self.list_files():
            os.remove(os.path.join(self.path, file))
        if enforce:
            print("remove directory: {}".format(self.path))
            shutil.rmtree(self.path, ignore_errors=True)
        else:
            for root, *_ in os.walk(self.path, topdown=False):
                # dirs and files return values of the iterator are not updated when removing files, so we need to
                # manually call listdir
                if len(os.listdir(root)) == 0:
                    root = root.rstrip(os.sep)
                    # the project was symlinked before being deleted
                    if os.path.islink(root):
                        os.rmdir(os.readlink(root))
                        os.remove(root)
                    else:
                        os.rmdir(root)

    def set_job_status(
        self, job_specifier: Union[str, int], status: str, project: "Project" = None
    ) -> None:
        """
        Set the status of a particular job

        Args:
            job_specifier (str, int): name of the job or job ID
            status (str): job status can be one of the following ['initialized', 'appended', 'created', 'submitted',
                         'running', 'aborted', 'collect', 'suspended', 'refresh', 'busy', 'finished']
            project (str): project path
        """
        if project is None:
            project = self.project_path
        set_job_status(
            database=self.db,
            sql_query=self.sql_query,
            user=self.user,
            project_path=project,
            job_specifier=job_specifier,
            status=status,
        )

    def values(self) -> list:
        """
        All items in the current project - this includes jobs, sub projects/ groups/ folders and any kind of files

        Returns:
            list: items in the project
        """
        return [self[key] for key in self.keys()]

    @deprecate(
        "The viewer mode is not used any longer. The functionality is already present in user mode. Doing nothing"
    )
    def switch_to_viewer_mode(self) -> None:
        """
        Switch from user mode to viewer mode - if viewer_mode is enable pyiron has read only access to the database.
        """
        pass

    @deprecate("Not doing anything any more (always in user mode).")
    def switch_to_user_mode(self) -> None:
        """
        Switch from viewer mode to user mode - if viewer_mode is enable pyiron has read only access to the database.
        """
        pass

    def switch_to_local_database(
        self, file_name: str = "pyiron.db", cwd: Optional[str] = None
    ) -> None:
        """
        Switch from central mode to local mode - if local_mode is enable pyiron is using a local database.

        Args:
            file_name (str): file name or file path for the local database
            cwd (str): directory where the local database is located
        """
        cwd = self.path if cwd is None else cwd
        state.database.switch_to_local_database(file_name=file_name, cwd=cwd)

    def switch_to_central_database(self) -> None:
        """
        Switch from local mode to central mode - if local_mode is enable pyiron is using a local database.
        """
        state.database.switch_to_central_database()

    def queue_delete_job(self, item: Union[int, "GenericJob"]) -> None:
        """
        Delete a job from the queuing system

        Args:
            item (int, GenericJob): Provide either the job_ID or the full hamiltonian

        Returns:
            str: Output from the queuing system as string - optimized for the Sun grid engine
        """
        return queue_delete_job(item)

    @staticmethod
    def create_hdf(path, job_name: str) -> ProjectHDFio:
        """
        Create an ProjectHDFio object to store project related information - for example aggregated data

        Args:
            path (str): absolute path
            job_name (str): name of the HDF5 container

        Returns:
            ProjectHDFio: HDF5 object
        """
        return ProjectHDFio(
            project=Project(path), file_name=job_name, h5_path="/" + job_name
        )

    @staticmethod
    def load_from_jobpath_string(
        job_path: str, convert_to_object: bool = True
    ) -> "JobPath":
        """
        Internal function to load an existing job either based on the job ID or based on the database entry dictionary.

        Args:
            job_path (str): string to reload the job from an HDF5 file - '/root_path/project_path/filename.h5/h5_path'
            convert_to_object (bool): convert the object to an pyiron object or only access the HDF5 file - default=True
                                      accessing only the HDF5 file is about an order of magnitude faster, but only
                                      provides limited functionality. Compare the GenericJob object to JobCore object.

        Returns:
            GenericJob, JobCore: Either the full GenericJob object or just a reduced JobCore object
        """
        from pyiron_base.jobs.job.path import JobPath

        job = JobPath(job_path)
        if convert_to_object:
            job = job.to_object()
        job.set_input_to_read_only()
        return job

    @staticmethod
    def get_external_input() -> dict:
        """
        Get external input either from the HDF5 file of the ScriptJob object which executes the Jupyter notebook
        or from an input.json file located in the same directory as the Jupyter notebook.

        Returns:
            dict: Dictionary with external input
        """
        inputdict = Notebook.get_custom_dict()
        if inputdict is None:
            raise ValueError(
                "No input found, either there is an issue with your ScriptJob, "
                + "or your input.json file is not located in the same directory "
                + "as your Jupyter Notebook."
            )
        return inputdict

    @staticmethod
    def list_publications(bib_format: str = "pandas") -> pandas.DataFrame:
        """
        List the publications used in this project.

        Args:
            bib_format (str): ['pandas', 'dict', 'bibtex', 'apa']

        Returns:
            pandas.DataFrame/ list: list of publications in Bibtex format.
        """
        return state.publications.show(bib_format=bib_format)

    @staticmethod
    def queue_is_empty() -> bool:
        """
        Check if the queue table is currently empty - no more jobs to wait for.

        Returns:
            bool: True if the table is empty, else False - optimized for the Sun grid engine
        """
        return queue_is_empty()

    @staticmethod
    def queue_enable_reservation(item: Union[int, "GenericJob"]) -> str:
        """
        Enable a reservation for a particular job within the queuing system

        Args:
            item (int, GenericJob): Provide either the job_ID or the full hamiltonian

        Returns:
            str: Output from the queuing system as string - optimized for the Sun grid engine
        """
        return queue_enable_reservation(item)

    @staticmethod
    def queue_check_job_is_waiting_or_running(item: Union[int, "GenericJob"]) -> bool:
        """
        Check if a job is still listed in the queue system as either waiting or running.

        Args:
            item (int, GenericJob): Provide either the job_ID or the full hamiltonian

        Returns:
            bool: [True/False]
        """
        return queue_check_job_is_waiting_or_running(item)

    @staticmethod
    def wait_for_job(
        job: "GenericJob", interval_in_s: int = 5, max_iterations: int = 100
    ) -> None:
        """
        Sleep until the job is finished but maximum interval_in_s * max_iterations seconds.

        Args:
            job (GenericJob): Job to wait for
            interval_in_s (int): interval when the job status is queried from the database - default 5 sec.
            max_iterations (int): maximum number of iterations - default 100

        Raises:
            ValueError: max_iterations reached, job still running
        """
        if isinstance(job, DelayedObject):
            wait_for_job(
                job=job._job, interval_in_s=interval_in_s, max_iterations=max_iterations
            )

        else:
            wait_for_job(
                job=job, interval_in_s=interval_in_s, max_iterations=max_iterations
            )

    def wait_for_jobs(
        self,
        interval_in_s: int = 5,
        max_iterations: int = 100,
        recursive: bool = True,
        ignore_exceptions: bool = False,
    ) -> None:
        """
        Wait for the calculation in the project to be finished

        Args:
            interval_in_s (int): interval when the job status is queried from the database - default 5 sec.
            max_iterations (int): maximum number of iterations - default 100
            recursive (bool): search subprojects [True/False] - default=True
            ignore_exceptions (bool): ignore eventual exceptions when retrieving jobs - default=False

        Raises:
            ValueError: max_iterations reached, but jobs still running
        """
        wait_for_jobs(
            project=self,
            interval_in_s=interval_in_s,
            max_iterations=max_iterations,
            recursive=recursive,
            ignore_exceptions=ignore_exceptions,
        )

    @staticmethod
    @deprecate(message="Use state.logger.set_logging_level instead.")
    def set_logging_level(level: str, channel: Optional[int] = None) -> None:
        """
        Set level for logger

        Args:
            level (str): 'DEBUG, INFO, WARN'
            channel (int): 0: file_log, 1: stream, None: both
        """
        state.logger.set_logging_level(level=level, channel=channel)

    @staticmethod
    def list_clusters() -> list:
        """
        List available computing clusters for remote submission

        Returns:
            list: List of computing clusters
        """
        return state.queue_adapter.list_clusters()

    @staticmethod
    def switch_cluster(cluster_name: str) -> None:
        """
        Switch to a different computing cluster

        Args:
            cluster_name (str): name of the computing cluster
        """
        state.queue_adapter.switch_cluster(cluster_name=cluster_name)

    @staticmethod
    def _is_hdf5_dir(item: str) -> bool:
        """
        Static internal function to check if the current project directory belongs to an pyiron object

        Args:
            item (str): folder/ project name

        Returns:
            bool: [True/False]
        """
        it = item.split("_")
        if len(it) > 1:
            if "hdf5" in it[-1]:
                return True
        return False

    def __getitem__(self, item: Union[str, int]) -> Any:
        """
        Get item from project

        Args:
            item (str, int): key

        Returns:
            Project, GenericJob, JobCore, dict, list, float: basically any kind of item inside the project.
        """
        if isinstance(item, slice):
            if not (item.start or item.stop or item.step):
                return self.values()
            print("slice: ", item)
            raise NotImplementedError("Implement if needed, e.g. for [:]")
        else:
            item_lst = item.split("/")
            if len(item_lst) > 1:
                try:
                    return self._get_item_helper(
                        item=item_lst[0], convert_to_object=False
                    ).__getitem__("/".join(item_lst[1:]))
                except ValueError:
                    return self._get_item_helper(
                        item=item_lst[0], convert_to_object=True
                    ).__getitem__("/".join(item_lst[1:]))
        return self._get_item_helper(item=item, convert_to_object=True)

    def __repr__(self) -> str:
        """
        Human readable string representation of the project object

        Returns:
            str: string representation
        """
        return str(
            {"groups": self.list_dirs(skip_hdf5=True), "nodes": self.list_nodes()}
        )

    def __getstate__(self) -> dict:
        state_dict = super().__getstate__()
        state_dict.update(
            {
                "user": self.user,
                "sql_query": self.sql_query,
                "filter": self._filter,
                "inspect_mode": self._inspect_mode,
            }
        )
        return state_dict

    def __setstate__(self, state: dict) -> None:
        super().__setstate__(state)
        self.user = state["user"]
        self.sql_query = state["sql_query"]
        self._filter = state["filter"]
        self._inspect_mode = state["inspect_mode"]
        self._data = None
        self._creator = Creator(project=self)
        self._loader = JobLoader(project=self)
        self._inspector = JobInspector(project=self)
        self.job_type = JobTypeChoice()
        self._maintenance = None

    def _get_item_helper(
        self, item: Union[str, int], convert_to_object: bool = True
    ) -> Any:
        """
        Internal helper function to get item from project

        Args:
            item (str, int): key
            convert_to_object (bool): convert the object to an pyiron object or only access the HDF5 file - default=True
                                      accessing only the HDF5 file is about an order of magnitude faster, but only
                                      provides limited functionality. Compare the GenericJob object to JobCore object.

        Returns:
            Project, GenericJob, JobCore, dict, list, float: basically any kind of item inside the project.
        """
        if item == "..":
            return self.parent_group
        try:
            item_save = _get_safe_job_name(name=item)
        except ValueError:
            item_save = None
        if item in self.list_nodes() or item_save in self.list_nodes():
            if self._inspect_mode or not convert_to_object:
                return self.inspect(item)
            return self.load(item)
        if item in self.list_files(extension="h5"):
            file_name = posixpath.join(self.path, "{}.h5".format(item))
            return ProjectHDFio(project=self, file_name=file_name)
        if item in self.list_files():
            file_name = posixpath.join(self.path, "{}".format(item))
            from pyiron_base.storage.filedata import load_file

            return load_file(file_name, project=self)
        if item in self.list_dirs():
            with self.open(item) as new_item:
                return new_item.copy()
        if item in os.listdir(self.path) and os.path.isdir(
            os.path.join(self.path, item)
        ):
            return self.open(item)
        raise ValueError("Unknown item: {}".format(item))

    def _remove_jobs_helper(
        self, recursive: bool = False, progress: bool = True
    ) -> None:
        """
        Remove all jobs in the current project and in all subprojects if recursive=True is selected - see also
        remove_job()

        Args:
            recursive (bool): [True/False] delete all jobs in all subprojects - default=False
            progress (bool): if True (default), add an interactive progress bar to the iteration
        """
        if not isinstance(recursive, bool):
            raise ValueError("recursive must be a boolean")
        job_id_lst = self.get_job_ids(recursive=recursive)
        job_id_progress = tqdm(job_id_lst) if progress else job_id_lst
        for job_id in job_id_progress:
            try:
                self.remove_job(job_specifier=job_id)
                state.logger.debug("Remove job with ID {0} ".format(job_id))
            except (IndexError, Exception):
                state.logger.warning("Could not remove job with ID {0} ".format(job_id))

    def _remove_files(self, pattern: str = "*") -> None:
        """
        Remove files within the current project

        Args:
            pattern (str): glob pattern - default="*"
        """
        import glob

        pattern = posixpath.join(self.path, pattern)
        for f in glob.glob(pattern):
            state.logger.info("remove file {}".format(posixpath.basename(f)))
            os.remove(f)

    def _update_jobs_in_old_database_format(self, job_name: str) -> None:
        """

        Args:
            job_name (str):
        """
        if self.db is not None:
            db_entry_in_old_format = self.db.get_items_dict(
                {"job": job_name, "project": self.project_path[:-1]}
            )
            if db_entry_in_old_format and len(db_entry_in_old_format) == 1:
                self.db.item_update(
                    {"project": self.project_path}, db_entry_in_old_format[0]["id"]
                )
            elif db_entry_in_old_format:
                for entry in db_entry_in_old_format:
                    self.db.item_update({"project": self.project_path}, entry["id"])

    def pack(
        self,
        destination_path: Optional[str] = None,
        compress: bool = True,
        copy_all_files: bool = False,
        **kwargs,
    ) -> None:
        """
        Export job table to a csv file and copy (and optionally compress) the project directory.

        Args:
            destination_path (str): gives the relative path, in which the project folder is copied and compressed
            compress (bool): if true, the function will compress the destination_path to a tar.gz file.
            copy_all_files (bool):
        """
        if "csv_file_name" in kwargs and kwargs["csv_file_name"] != "export.csv":
            raise ValueError(
                "csv_file_name is not supported anymore. Rename"
                " {} to export.csv.".format(kwargs["csv_file_name"])
            )
        if destination_path is None:
            destination_path = self.path
        if ".tar.gz" in destination_path:
            destination_path = destination_path.split(".tar.gz")[0]
            compress = True
        destination_path_abs = os.path.abspath(destination_path)
        directory_to_transfer = os.path.abspath(self.path)
        assert not destination_path_abs.endswith(".tar")
        assert not destination_path_abs.endswith(".gz")
        if destination_path_abs == directory_to_transfer and not compress:
            raise ValueError(
                "destination_path cannot have the same name as the project."
            )
        export_archive.copy_files_to_archive(
            directory_to_transfer=directory_to_transfer,
            archive_directory=destination_path_abs,
            compress=compress,
            copy_all_files=copy_all_files,
            arcname=os.path.relpath(self.path, os.getcwd()),
            df=export_archive.export_database(self.job_table()),
        )

    @staticmethod
    def unpack_csv(tar_path: str, csv_file: str = "export.csv") -> pandas.DataFrame:
        """
        Import job table from a csv file and copy the content of a project
        directory from a given path.

        Args:
            tar_path (str): the relative path of a directory from which the
                project directory is copied.
            csv_file (str): the name of the csv file.

        Returns:
            pandas.DataFrame: job table
        """
        return import_archive.inspect_csv(tar_path=tar_path, csv_file=csv_file)

    def unpack(self, origin_path: str, **kwargs) -> None:
        """
        by this function, job table is imported from a given csv file,
        and also the content of project directory is copied from a given path

        Args:
            origin_path (str): the relative path of a directory from which
               the project directory is copied.
        """
        if "csv_file_name" in kwargs and kwargs["csv_file_name"] != "export.csv":
            raise ValueError(
                "csv_file_name is not supported anymore. Rename"
                " {} to export.csv.".format(kwargs["csv_file_name"])
            )
        if "compress" in kwargs and kwargs["compress"] is (
            ".tar.gz" not in origin_path
        ):
            raise ValueError(
                "compress is not supported anymore. Use the full file name"
            )
        import_archive.import_jobs(self, archive_directory=origin_path)

    @classmethod
    def register_tools(cls, name: str, tools) -> None:
        """
        Add a new creator to the project class.

        Example)

        >>> from pyiron_base import Project, Toolkit
        >>> class MyTools(Toolkit):
        ...     @property
        ...     def foo(self):
        ...         return 'foo'
        >>>
        >>> Project.register_tools('my_tools', MyTools)
        >>> pr = Project('scratch')
        >>> print(pr.my_tools.foo)
        'foo'

        The intent is then that pyiron submodules (e.g. `pyiron_atomistics`) define a new creator and in their
        `__init__.py` file only need to invoke `Project.register_creator('pyiron_submodule', SubmoduleCreator)`.
        Then whenever `pyiron_submodule` gets imported, all its functionality is available on the project.

        Args:
            name (str): The name for the newly registered property.
            tools (Toolkit): The tools to register.
        """
        if hasattr(cls, name):
            raise AttributeError(
                f"{cls.__name__} already has an attribute {name}. Please use a new name for registration."
            )
        setattr(cls, name, property(lambda self: tools(self)))

    def symlink(self, target_dir: str) -> None:
        """
        Move underlying project folder to target and create a symlink to it.

        The project itself does not change and is not updated in the database.  Instead the project folder is moved into
        a subdirectory of target_dir with the same name as the project and a symlink is placed in the previous project path
        pointing to the newly created one.

        If self.path is already a symlink pointing inside target_dir, this method will silently return.

        Args:
            target_dir (str): new parent folder for the project

        Raises:
            OSError: when calling this method on non-unix systems
            RuntimeError: the project path is already a symlink to somewhere else
            RuntimeError: the project path has submitted or running jobs inside it, wait until after they are finished
            RuntimeError: target already contains a subdirectory with the project name and it is not empty
        """
        target = os.path.join(target_dir, self.name)
        destination = self.path
        if destination[-1] == "/":
            destination = destination[:-1]
        if stat.S_ISLNK(os.lstat(destination).st_mode):
            if os.readlink(destination) == target:
                return
            raise RuntimeError(
                "Refusing to symlink and move a project that is already symlinked!"
            )
        if os.name != "posix":
            raise OSError("Symlinking projects is only supported on unix systems!")
        if len(self.job_table().query('status.isin(["submitted", "running"])')) > 0:
            raise RuntimeError(
                "Refusing to symlink and move a project that has submitted or running jobs!"
            )
        os.makedirs(target_dir, exist_ok=True)
        if os.path.exists(target):
            if len(os.listdir(target)) > 0:
                raise RuntimeError(
                    "Refusing to symlink and move a project to non-empty directory!"
                )
            else:
                os.rmdir(target)
        shutil.move(self.path, target_dir)
        os.symlink(target, destination)

    def unlink(self) -> None:
        """
        If the project folder is symlinked somewhere else remove the link and restore the original folder.

        If it is not symlinked, silently return.
        """
        path = self.path.rstrip(os.sep)
        if not stat.S_ISLNK(os.lstat(path).st_mode):
            return

        target = os.readlink(path)
        os.unlink(path)
        shutil.move(target, path)


class Creator:
    def __init__(self, project: Project):
        self._job_factory = JobFactory(project=project)
        self._project = project

    @property
    def job(self) -> JobFactory:
        return self._job_factory

    @staticmethod
    def job_name(
        job_name: str,
        ndigits: Union[int, None] = 8,
        special_symbols: Union[Dict, None] = None,
    ) -> str:
        """
        Creation of job names with special symbol replacement and rounding of floating numbers

        Args:
            job_name (str/list): Job name
            ndigits (int/None): Decimal digits to round floats to a given precision. `None` if
                no rounding should be performed.
            special_symbols (dict): Replacement of special symbols.

        Returns:
            (str): Job name

        Default `special_symbols`: default_special_symbols_to_be_replaced
        """
        return _get_safe_job_name(
            name=job_name, ndigits=ndigits, special_symbols=special_symbols
        )

    job_name.__doc__ = job_name.__doc__.replace(
        "default_special_symbols_to_be_replaced", str(_special_symbol_replacements)
    )

    def table(
        self, job_name: str = "table", delete_existing_job: bool = False
    ) -> "TableJob":
        """
        Create pyiron table

        Args:
            job_name (str): job name of the pyiron table job
            delete_existing_job (bool): Delete the existing table and run the analysis again.

        Returns:
            pyiron_base.table.datamining.TableJob
        """
        table = self.job.TableJob(
            job_name=job_name, delete_existing_job=delete_existing_job
        )
        table.analysis_project = self._project
        return table
