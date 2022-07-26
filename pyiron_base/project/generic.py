# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The project object is the central import point of pyiron - all other objects can be created from this one
"""

import os
import posixpath
import shutil
from tqdm.auto import tqdm
import pandas
import pint
import importlib
import math
import numpy as np

from pyiron_base.project.maintenance import Maintenance
from pyiron_base.project.path import ProjectPath
from pyiron_base.database.filetable import FileTable
from pyiron_base.state import state
from pyiron_base.database.jobtable import (
    get_job_ids,
    get_job_id,
    get_jobs,
    set_job_status,
    get_child_ids,
    get_job_working_directory,
    get_job_status,
)
from pyiron_base.storage.hdfio import ProjectHDFio
from pyiron_base.storage.filedata import load_file
from pyiron_base.utils.deprecate import deprecate
from pyiron_base.jobs.job.util import _special_symbol_replacements, _get_safe_job_name
from pyiron_base.interfaces.has_groups import HasGroups
from pyiron_base.jobs.job.jobtype import JobType, JobTypeChoice, JobFactory
from pyiron_base.jobs.job.extension.server.queuestatus import (
    queue_delete_job,
    queue_is_empty,
    queue_table,
    wait_for_job,
    wait_for_jobs,
    update_from_remote,
    queue_enable_reservation,
    queue_check_job_is_waiting_or_running,
)
from pyiron_base.project.external import Notebook
from pyiron_base.project.data import ProjectData
from pyiron_base.project.archiving import export_archive, import_archive
from typing import Generator, Union, Dict

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
        job_type (): Job Type object with all the available job types: ['ExampleJob', 'SerialMaster', 'ParallelMaster',
                        'ScriptJob', 'ListMaster'].
        view_mode (): If viewer_mode is enable pyiron has read only access to the database.
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
        self, path="", user=None, sql_query=None, default_working_directory=False
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

        self.job_type = JobTypeChoice()

        self._maintenance = None

    @property
    def state(self):
        return state

    @property
    def db(self):
        if not state.database.database_is_disabled:
            return state.database.database
        else:
            return FileTable(project=self.path)

    @property
    def maintenance(self):
        if self._maintenance is None:
            self._maintenance = Maintenance(self)
        return self._maintenance

    @property
    def parent_group(self):
        """
        Get the parent group of the current project

        Returns:
            Project: parent project
        """
        return self.create_group("..")

    @property
    @deprecate("use db.view_mode")
    def view_mode(self):
        """
        Get viewer_mode - if viewer_mode is enable pyiron has read only access to the database.

        Change it via
        `Project('my_project').switch_to_viewer_mode()`
        and
        `Project('my_project').switch_to_user_mode()`

        Returns:
            bool: returns TRUE when viewer_mode is enabled
        """
        return self.db.view_mode

    @property
    def name(self):
        """
        The name of the current project folder

        Returns:
            str: name of the current project folder
        """
        return self.base_name

    @property
    def create(self):
        return self._creator

    @property
    def data(self):
        if self._data is None:
            self._data = ProjectData(project=self, table_name="data")
            try:
                self._data.read()
            except KeyError:
                pass
        return self._data

    @property
    def size(self):
        """
        Get the size of the project
        """
        size = (
            sum(
                [
                    sum([os.path.getsize(os.path.join(path, f)) for f in files])
                    for path, dirs, files in os.walk(self.path)
                ]
            )
            * pint.UnitRegistry().byte
        )
        return self._size_conversion(size)

    @staticmethod
    def _size_conversion(size: pint.Quantity):
        sign_prefactor = 1
        if size < 0:
            sign_prefactor = -1
            size *= -1
        elif size == 0:
            return size

        prefix_index = math.floor(math.log2(size) / 10) - 1
        prefix = ["Ki", "Mi", "Gi", "Ti", "Pi"]

        size *= sign_prefactor
        if prefix_index < 0:
            return size
        elif prefix_index < 5:
            return size.to(f"{prefix[prefix_index]}byte")
        else:
            return size.to(f"{prefix[-1]}byte")

    def copy(self):
        """
        Copy the project object - copying just the Python object but maintaining the same pyiron path

        Returns:
            Project: copy of the project object
        """
        new = self.__class__(path=self.path, user=self.user, sql_query=self.sql_query)
        new._filter = self._filter
        new._inspect_mode = self._inspect_mode
        return new

    def copy_to(self, destination):
        """
        Copy the project object to a different pyiron path - including the content of the project (all jobs).
        In order to move individual jobs, use `copy_to` from the job objects.

        Args:
            destination (Project): project path to copy the project content to

        Returns:
            Project: pointing to the new project path
        """
        if not self.view_mode:
            if not isinstance(destination, Project):
                raise TypeError("A project can only be copied to another project.")
            for sub_project_name in self.list_groups():
                if "_hdf5" not in sub_project_name:
                    sub_project = self.open(sub_project_name)
                    destination_sub_project = destination.open(sub_project_name)
                    sub_project.copy_to(destination_sub_project)
            for job_id in self.get_job_ids(recursive=False):
                ham = self.load(job_id)
                ham.copy_to(project=destination)
            for file in self.list_files():
                if ".h5" not in file:
                    shutil.copy(os.path.join(self.path, file), destination.path)
            return destination
        else:
            raise EnvironmentError("copy_to: is not available in Viewermode !")

    def create_from_job(self, job_old, new_job_name):
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

    def create_group(self, group):
        """
        Create a new subproject/ group/ folder

        Args:
            group (str): name of the new project

        Returns:
            Project: New subproject
        """
        new = self.copy()
        return new.open(group, history=False)

    def create_job(self, job_type, job_name, delete_existing_job=False):
        """
        Create one of the following jobs:
        - 'ExampleJob': example job just generating random number
        - 'SerialMaster': series of jobs run in serial
        - 'ParallelMaster': series of jobs run in parallel
        - 'ScriptJob': Python script or jupyter notebook job container
        - 'ListMaster': list of jobs

        Args:
            job_type (str): job type can be ['ExampleJob', 'SerialMaster', 'ParallelMaster', 'ScriptJob', 'ListMaster']
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

    def create_table(self, job_name="table", delete_existing_job=False):
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

    def get_child_ids(self, job_specifier, project=None):
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
        return self.db.get_table_headings()

    def get_jobs(self, recursive=True, columns=None):
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
        return get_jobs(
            database=self.db,
            sql_query=self.sql_query,
            user=self.user,
            project_path=self.project_path,
            recursive=recursive,
            columns=columns,
        )

    def get_job_ids(self, recursive=True):
        """
        Return the job IDs matching a specific query

        Args:
            recursive (bool): search subprojects [True/False]

        Returns:
            list: a list of job IDs
        """
        return get_job_ids(
            database=self.db,
            sql_query=self.sql_query,
            user=self.user,
            project_path=self.project_path,
            recursive=recursive,
        )

    def get_job_id(self, job_specifier):
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

    def get_job_status(self, job_specifier, project=None):
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

    def get_job_working_directory(self, job_specifier, project=None):
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
    def get_project_size(self):
        """
        Get the size of the project.

        Returns:
            float: project size
        """
        return self.size

    @deprecate("use maintenance.get_repository_status() instead.")
    def get_repository_status(self):
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

    def inspect(self, job_specifier):
        """
        Inspect an existing pyiron object - most commonly a job - from the database

        Args:
            job_specifier (str, int): name of the job or job ID

        Returns:
            JobCore: Access to the HDF5 object - not a GenericJob object - use load() instead.
        """
        return self.load(job_specifier=job_specifier, convert_to_object=False)

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
            path (str): HDF5 path inside each job object
            recursive (bool): search subprojects [True/False] - True by default
            convert_to_object (bool): load the full GenericJob object (default) or just the HDF5 / JobCore object
            progress (bool): if True (default), add an interactive progress bar to the iteration
            **kwargs (dict): Optional arguments for filtering with keys matching the project database column name
                            (eg. status="finished"). Asterisk can be used to denote a wildcard, for zero or more
                            instances of any character

        Returns:
            yield: Yield of GenericJob or JobCore
        """
        job_id_lst = self.job_table(recursive=recursive, **kwargs)["id"]
        if progress:
            job_id_lst = tqdm(job_id_lst)
        for job_id in job_id_lst:
            if path is not None:
                yield self.load(job_id, convert_to_object=False)[path]
            else:  # Backwards compatibility - in future the option convert_to_object should be removed
                yield self.load(job_id, convert_to_object=convert_to_object)

    def iter_output(self, recursive=True):
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

    def items(self):
        """
        All items in the current project - this includes jobs, sub projects/ groups/ folders and any kind of files

        Returns:
            list: items in the project
        """
        return [(key, self[key]) for key in self.keys()]

    def update_from_remote(self, recursive=True, ignore_exceptions=False):
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
            project=self, recursive=recursive, ignore_exceptions=ignore_exceptions
        )

    def job_table(
        self,
        recursive=True,
        columns=None,
        all_columns=True,
        sort_by="id",
        full_table=False,
        element_lst=None,
        job_name_contains="",
        **kwargs: dict,
    ):
        return self.db.job_table(
            sql_query=self.sql_query,
            user=self.user,
            project_path=self.project_path,
            recursive=recursive,
            columns=columns,
            all_columns=all_columns,
            sort_by=sort_by,
            full_table=full_table,
            element_lst=element_lst,
            **kwargs,
        )

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

    def get_jobs_status(self, recursive=True, **kwargs):
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

    def keys(self):
        """
        List of file-, folder- and objectnames

        Returns:
            list: list of the names of project directories and project nodes
        """
        return self.list_dirs() + self.list_nodes()

    def _list_all(self):
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

    def list_dirs(self, skip_hdf5=True):
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

    def list_files(self, extension=None):
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

    def _list_nodes(self, recursive=False):
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

    def load(self, job_specifier, convert_to_object=True):
        """
        Load an existing pyiron object - most commonly a job - from the database

        Args:
            job_specifier (str, int): name of the job or job ID
            convert_to_object (bool): convert the object to an pyiron object or only access the HDF5 file - default=True
                                      accessing only the HDF5 file is about an order of magnitude faster, but only
                                      provides limited functionality. Compare the GenericJob object to JobCore object.

        Returns:
            GenericJob, JobCore: Either the full GenericJob object or just a reduced JobCore object
        """
        if self.sql_query is not None:
            state.logger.warning(
                "SQL filter '%s' is active (may exclude job) ", self.sql_query
            )
        if not isinstance(job_specifier, (int, np.integer)):
            job_specifier = _get_safe_job_name(name=job_specifier)
        job_id = self.get_job_id(job_specifier=job_specifier)
        if job_id is None:
            state.logger.warning(
                "Job '%s' does not exist and cannot be loaded", job_specifier
            )
            return None
        return self.load_from_jobpath(
            job_id=job_id, convert_to_object=convert_to_object
        )

    def load_from_jobpath(self, job_id=None, db_entry=None, convert_to_object=True):
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
        jobpath = getattr(
            importlib.import_module("pyiron_base.jobs.job.path"), "JobPath"
        )
        if job_id is not None:
            job = jobpath(db=self.db, job_id=job_id, user=self.user)
            if convert_to_object:
                job = job.to_object()
                job.reset_job_id(job_id=job_id)
                job.set_input_to_read_only()
            return job
        elif db_entry is not None:
            job = jobpath(db=self.db, db_entry=db_entry)
            if convert_to_object:
                job = job.to_object()
                job.set_input_to_read_only()
            return job
        else:
            raise ValueError("Either a job ID or an database entry has to be provided.")

    def move_to(self, destination):
        """
        Similar to the copy_to() function move the project object to a different pyiron path - including the content of
        the project (all jobs). In order to move individual jobs, use `move_to` from the job objects.

        Args:
            destination (Project): project path to move the project content to

        Returns:
            Project: pointing to the new project path
        """
        if not self.view_mode:
            if not isinstance(destination, Project):
                raise TypeError("A project can only be copied to another project.")
            for sub_project_name in self.list_groups():
                if "_hdf5" not in sub_project_name:
                    sub_project = self.open(sub_project_name)
                    destination_sub_project = destination.open(sub_project_name)
                    sub_project.move_to(destination_sub_project)
            for job_id in self.get_job_ids(recursive=False):
                ham = self.load(job_id)
                ham.move_to(destination)
            for file in self.list_files():
                shutil.move(os.path.join(self.path, file), destination.path)
        else:
            raise EnvironmentError("move_to: is not available in Viewermode !")

    def nodes(self):
        """
        Filter project by nodes

        Returns:
            Project: a project which is filtered by nodes
        """
        new = self.copy()
        new._filter = ["nodes"]
        return new

    def queue_table(self, project_only=True, recursive=True, full_table=False):
        """
        Display the queuing system table as pandas.Dataframe

        Args:
            project_only (bool): Query only for jobs within the current project - True by default
            recursive (bool): Include jobs from sub projects
            full_table (bool): Whether to show the entire pandas table

        Returns:
            pandas.DataFrame: Output from the queuing system - optimized for the Sun grid engine
        """
        return queue_table(
            job_ids=self.get_job_ids(recursive=recursive),
            project_only=project_only,
            full_table=full_table,
        )

    def queue_table_global(self, full_table=False):
        """
        Display the queuing system table as pandas.Dataframe

        Args:
            full_table (bool): Whether to show the entire pandas table

        Returns:
            pandas.DataFrame: Output from the queuing system - optimized for the Sun grid engine
        """
        df = queue_table(job_ids=[], project_only=False, full_table=full_table)
        if len(df) != 0 and self.db is not None:
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
            return None

    def refresh_job_status(self, *jobs):
        """
        Check if job is still running or crashed on the cluster node.

        If `jobs` is not given, check for all jobs listed as running in the current project.

        Args:
            *jobs (str, int): name of the job or job ID, any number of them
        """
        if len(jobs) == 0:
            jobs = self.job_table(status="running").id
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
    def refresh_job_status_based_on_queue_status(self, job_specifier, status="running"):
        """
        Check if the job is still listed as running, while it is no longer listed in the queue.

        Args:
            job_specifier (str, int): name of the job or job ID
            status (str): Currently only the jobstatus of 'running' jobs can be refreshed - default='running'
        """
        if status != "running":
            raise NotImplementedError()
        self.refresh_job_status(job_specifier)

    def refresh_job_status_based_on_job_id(self, job_id, que_mode=True):
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
                if not self.queue_check_job_is_waiting_or_running(self.inspect(job_id)):
                    self.db.set_job_status(job_id=job_id, status="aborted")

    def remove_file(self, file_name):
        """
        Remove a file (same as unlink()) - copied from os.remove()

        If dir_fd is not None, it should be a file descriptor open to a directory,
          and path should be relative; path will then be relative to that directory.
        dir_fd may not be implemented on your platform.
          If it is unavailable, using it will raise a NotImplementedError.

        Args:
            file_name (str): name of the file
        """
        if not self.view_mode:
            os.remove(posixpath.join(self.path, file_name))
        else:
            raise EnvironmentError("copy_to: is not available in Viewermode !")

    def remove_job(self, job_specifier, _unprotect=False):
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
        else:
            if not self.db.view_mode:
                try:
                    job = self.load(
                        job_specifier=job_specifier, convert_to_object=False
                    )
                    if job is None:
                        state.logger.warning(
                            "Job '%s' does not exist and could not be removed",
                            str(job_specifier),
                        )
                    elif _unprotect:
                        job.remove_child()
                    else:
                        job.remove()
                except IOError as _:
                    state.logger.debug(
                        "hdf file does not exist. Removal from database will be attempted."
                    )
                    job_id = self.get_job_id(job_specifier)
                    self.db.delete_item(job_id)
            else:
                raise EnvironmentError("copy_to: is not available in Viewermode !")

    def remove_jobs(self, recursive=False, progress=True, silently=False):
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
    def remove_jobs_silently(self, recursive=False, progress=True):
        self.remove_jobs(recursive=recursive, progress=progress, silently=True)

    def compress_jobs(self, recursive=False):
        """
        Compress all finished jobs in the current project and in all subprojects if recursive=True is selected.

        Args:
            recursive (bool): [True/False] compress all jobs in all subprojects - default=False
        """
        for job_id in self.get_job_ids(recursive=recursive):
            job = self.inspect(job_id)
            if job.status == "finished":
                job.compress()

    def delete_output_files_jobs(self, recursive=False):
        """
        Delete the output files of all finished jobs in the current project and in all subprojects if recursive=True is
        selected.

        Args:
            recursive (bool): [True/False] delete the output files of all jobs in all subprojects - default=False
        """
        for job_id in self.get_job_ids(recursive=recursive):
            job = self.inspect(job_id)
            if job.status == "finished":
                for file in job.list_files():
                    fullname = os.path.join(job.working_directory, file)
                    if os.path.isfile(fullname) and ".h5" not in fullname:
                        os.remove(fullname)
                    elif os.path.isdir(fullname):
                        os.removedirs(fullname)

    def remove(self, enable=False, enforce=False):
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
        if not self.db.view_mode:
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
                        os.rmdir(root)
        else:
            raise EnvironmentError("remove() is not available in view_mode!")

    def set_job_status(self, job_specifier, status, project=None):
        """
        Set the status of a particular job

        Args:
            job_specifier (str): name of the job or job ID
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

    def values(self):
        """
        All items in the current project - this includes jobs, sub projects/ groups/ folders and any kind of files

        Returns:
            list: items in the project
        """
        return [self[key] for key in self.keys()]

    def switch_to_viewer_mode(self):
        """
        Switch from user mode to viewer mode - if viewer_mode is enable pyiron has read only access to the database.
        """
        if not isinstance(self.db, FileTable):
            state.database.switch_to_viewer_mode()

    def switch_to_user_mode(self):
        """
        Switch from viewer mode to user mode - if viewer_mode is enable pyiron has read only access to the database.
        """
        if not isinstance(self.db, FileTable):
            state.database.switch_to_user_mode()

    def switch_to_local_database(self, file_name="pyiron.db", cwd=None):
        """
        Switch from central mode to local mode - if local_mode is enable pyiron is using a local database.

        Args:
            file_name (str): file name or file path for the local database
            cwd (str): directory where the local database is located
        """
        cwd = self.path if cwd is None else cwd
        state.database.switch_to_local_database(file_name=file_name, cwd=cwd)

    def switch_to_central_database(self):
        """
        Switch from local mode to central mode - if local_mode is enable pyiron is using a local database.
        """
        state.database.switch_to_central_database()

    def queue_delete_job(self, item):
        """
        Delete a job from the queuing system

        Args:
            item (int, GenericJob): Provide either the job_ID or the full hamiltonian

        Returns:
            str: Output from the queuing system as string - optimized for the Sun grid engine
        """
        if not self.view_mode:
            return queue_delete_job(item)
        else:
            raise EnvironmentError("copy_to: is not available in Viewermode !")

    @staticmethod
    def create_hdf(path, job_name):
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
    def load_from_jobpath_string(job_path, convert_to_object=True):
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
        job = getattr(
            importlib.import_module("pyiron_base.jobs.job.path"), "JobPathBase"
        )(job_path=job_path)
        if convert_to_object:
            job = job.to_object()
        job.set_input_to_read_only()
        return job

    @staticmethod
    def get_external_input():
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
    def list_publications(bib_format="pandas"):
        """
        List the publications used in this project.

        Args:
            bib_format (str): ['pandas', 'dict', 'bibtex', 'apa']

        Returns:
            pandas.DataFrame/ list: list of publications in Bibtex format.
        """
        return state.publications.show(bib_format=bib_format)

    @staticmethod
    def queue_is_empty():
        """
        Check if the queue table is currently empty - no more jobs to wait for.

        Returns:
            bool: True if the table is empty, else False - optimized for the Sun grid engine
        """
        return queue_is_empty()

    @staticmethod
    def queue_enable_reservation(item):
        """
        Enable a reservation for a particular job within the queuing system

        Args:
            item (int, GenericJob): Provide either the job_ID or the full hamiltonian

        Returns:
            str: Output from the queuing system as string - optimized for the Sun grid engine
        """
        return queue_enable_reservation(item)

    @staticmethod
    def queue_check_job_is_waiting_or_running(item):
        """
        Check if a job is still listed in the queue system as either waiting or running.

        Args:
            item (int, GenericJob): Provide either the job_ID or the full hamiltonian

        Returns:
            bool: [True/False]
        """
        return queue_check_job_is_waiting_or_running(item)

    @staticmethod
    def wait_for_job(job, interval_in_s=5, max_iterations=100):
        """
        Sleep until the job is finished but maximum interval_in_s * max_iterations seconds.

        Args:
            job (GenericJob): Job to wait for
            interval_in_s (int): interval when the job status is queried from the database - default 5 sec.
            max_iterations (int): maximum number of iterations - default 100

        Raises:
            ValueError: max_iterations reached, job still running
        """
        wait_for_job(
            job=job, interval_in_s=interval_in_s, max_iterations=max_iterations
        )

    def wait_for_jobs(
        self,
        interval_in_s=5,
        max_iterations=100,
        recursive=True,
        ignore_exceptions=False,
    ):
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
    def set_logging_level(level, channel=None):
        """
        Set level for logger

        Args:
            level (str): 'DEBUG, INFO, WARN'
            channel (int): 0: file_log, 1: stream, None: both
        """
        state.logger.set_logging_level(level=level, channel=channel)

    @staticmethod
    def list_clusters():
        """
        List available computing clusters for remote submission

        Returns:
            list: List of computing clusters
        """
        return state.queue_adapter.list_clusters()

    @staticmethod
    def switch_cluster(cluster_name):
        """
        Switch to a different computing cluster

        Args:
            cluster_name (str): name of the computing cluster
        """
        state.queue_adapter.switch_cluster(cluster_name=cluster_name)

    @staticmethod
    def _is_hdf5_dir(item):
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

    def __getitem__(self, item):
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
            item_lst = [sub_item.replace(" ", "") for sub_item in item.split("/")]
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

    def __repr__(self):
        """
        Human readable string representation of the project object

        Returns:
            str: string representation
        """
        return str(
            {"groups": self.list_dirs(skip_hdf5=True), "nodes": self.list_nodes()}
        )

    def _get_item_helper(self, item, convert_to_object=True):
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
        if item in self.list_nodes():
            if self._inspect_mode or not convert_to_object:
                return self.inspect(item)
            return self.load(item)
        if item in self.list_files(extension="h5"):
            file_name = posixpath.join(self.path, "{}.h5".format(item))
            return ProjectHDFio(project=self, file_name=file_name)
        if item in self.list_files():
            file_name = posixpath.join(self.path, "{}".format(item))
            return load_file(file_name, project=self)
        if item in self.list_dirs():
            with self.open(item) as new_item:
                return new_item.copy()
        if item in os.listdir(self.path) and os.path.isdir(
            os.path.join(self.path, item)
        ):
            return self.open(item)
        raise ValueError("Unknown item: {}".format(item))

    def _remove_jobs_helper(self, recursive=False, progress=True):
        """
        Remove all jobs in the current project and in all subprojects if recursive=True is selected - see also
        remove_job()

        Args:
            recursive (bool): [True/False] delete all jobs in all subprojects - default=False
            progress (bool): if True (default), add an interactive progress bar to the iteration
        """
        if not isinstance(recursive, bool):
            raise ValueError("recursive must be a boolean")
        if not self.db.view_mode:
            job_id_lst = self.get_job_ids(recursive=recursive)
            if progress and len(job_id_lst) > 0:
                job_id_lst = tqdm(job_id_lst)
            for job_id in job_id_lst:
                if job_id not in self.get_job_ids(recursive=recursive):
                    continue
                else:
                    try:
                        self.remove_job(job_specifier=job_id)
                        state.logger.debug("Remove job with ID {0} ".format(job_id))
                    except (IndexError, Exception):
                        state.logger.debug(
                            "Could not remove job with ID {0} ".format(job_id)
                        )
        else:
            raise EnvironmentError("copy_to: is not available in Viewermode !")

    def _remove_files(self, pattern="*"):
        """
        Remove files within the current project

        Args:
            pattern (str): glob pattern - default="*"
        """
        if not self.view_mode:
            import glob

            pattern = posixpath.join(self.path, pattern)
            for f in glob.glob(pattern):
                state.logger.info("remove file {}".format(posixpath.basename(f)))
                os.remove(f)
        else:
            raise EnvironmentError("copy_to: is not available in Viewermode !")

    def _update_jobs_in_old_database_format(self, job_name):
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
        destination_path,
        csv_file_name="export.csv",
        compress=True,
        copy_all_files=False,
    ):
        """
        Export job table to a csv file and copy (and optionally compress) the project directory.

        Args:
            destination_path (str): gives the relative path, in which the project folder is copied and compressed
            csv_file_name (str): is the name of the csv file used to store the project table.
            compress (bool): if true, the function will compress the destination_path to a tar.gz file.
            copy_all_files (bool):
        """
        directory_to_transfer = os.path.basename(self.path[:-1])
        export_archive.copy_files_to_archive(
            directory_to_transfer,
            destination_path,
            compressed=compress,
            copy_all_files=copy_all_files,
        )
        df = export_archive.export_database(
            self, directory_to_transfer, destination_path
        )
        df.to_csv(csv_file_name)

    def unpack(self, origin_path, csv_file_name="export.csv", compress=True):
        """
        by this function, job table is imported from a given csv file,
        and also the content of project directory is copied from a given path

        Args:
            origin_path (str): the relative path of a directory (or a compressed file without the tar.gz exention)
                            from which the project directory is copied.
            csv_file_name (str): the csv file from which the job_table is copied to the current project
            compress (bool): if True, it looks for a compressed file
        """
        csv_path = csv_file_name
        df = pandas.read_csv(csv_path, index_col=0)
        import_archive.import_jobs(
            self, archive_directory=origin_path, df=df, compressed=compress
        )

    @classmethod
    def register_tools(cls, name: str, tools):
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


class Creator:
    def __init__(self, project):
        self._job_factory = JobFactory(project=project)
        self._project = project

    @property
    def job(self):
        return self._job_factory

    @staticmethod
    def job_name(
        job_name: str,
        ndigits: Union[int, None] = 8,
        special_symbols: Union[Dict, None] = None,
    ):
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

    def table(self, job_name="table", delete_existing_job=False):
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
