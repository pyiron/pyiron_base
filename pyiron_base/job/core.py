# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The JobCore the most fundamental pyiron job class.
"""

import copy
import os
import posixpath
import math
from pyiron_base.settings.generic import Settings
from pyiron_base.generic.template import PyironObject
from pyiron_base.generic.util import static_isinstance
from pyiron_base.job.util import \
    _get_project_for_copy, \
    _copy_database_entry, \
    _copy_to_delete_existing, \
    _rename_job, \
    _is_valid_job_name, \
    _job_is_archived, \
    _job_archive, \
    _job_unarchive, \
    _job_is_compressed, \
    _job_compress, \
    _job_decompress, \
    _job_delete_files, \
    _job_delete_hdf, \
    _job_remove_folder
from tables import NoSuchNodeError
import shutil

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"

s = Settings()


class JobCore(PyironObject):
    """
    The JobCore the most fundamental pyiron job class. From this class the GenericJob as well as the reduced JobPath
    class are derived. While JobPath only provides access to the HDF5 file it is about one order faster.

    Args:
        project (ProjectHDFio): ProjectHDFio instance which points to the HDF5 file the job is stored in
        job_name (str): name of the job, which has to be unique within the project

    Attributes:

        .. attribute:: job_name

            name of the job, which has to be unique within the project

        .. attribute:: status

            execution status of the job, can be one of the following [initialized, appended, created, submitted, running,
                                                                      aborted, collect, suspended, refresh, busy, finished]

        .. attribute:: job_id

            unique id to identify the job in the pyiron database

        .. attribute:: parent_id

            job id of the predecessor job - the job which was executed before the current one in the current job series

        .. attribute:: master_id

            job id of the master job - a meta job which groups a series of jobs, which are executed either in parallel or in
            serial.

        .. attribute:: child_ids

            list of child job ids - only meta jobs have child jobs - jobs which list the meta job as their master

        .. attribute:: project

            Project instance the jobs is located in

        .. attribute:: project_hdf5

            ProjectHDFio instance which points to the HDF5 file the job is stored in

        .. attribute:: job_info_str

            short string to describe the job by it is job_name and job ID - mainly used for logging

        .. attribute:: working_directory

            working directory of the job is executed in - outside the HDF5 file

        .. attribute:: path

            path to the job as a combination of absolute file system path and path within the HDF5 file.
    """

    def __init__(self, project, job_name):
        _is_valid_job_name(job_name)
        self._name = job_name
        self._hdf5 = project.open(self._name)
        self._job_id = None
        self._parent_id = None
        self._master_id = None
        self._status = None
        self._import_directory = None
        self._database_property = DatabaseProperties()
        self._hdf5_content = HDF5Content(project_hdf5=self._hdf5)

    @property
    def content(self):
        return self._hdf5_content

    @property
    def job_name(self):
        """
        Get name of the job, which has to be unique within the project

        Returns:
            str: job name
        """
        return self.name

    @job_name.setter
    def job_name(self, new_job_name):
        """
        Set name of the job, which has to be unique within the project. When changing the job_name this also moves the
        HDF5 file as the name of the HDF5 file is the job_name plus the extension *.h5

        Args:
            new_job_name (str): new job name
        """
        self.name = new_job_name

    @property
    def name(self):
        """
        Get name of the job, which has to be unique within the project

        Returns:
            str: job name
        """
        return self._name

    @name.setter
    def name(self, new_job_name):
        """
        Set name of the job, which has to be unique within the project. When changing the job_name this also moves the
        HDF5 file as the name of the HDF5 file is the job_name plus the extension *.h5

        Args:
            new_job_name (str): new job name
        """
        _rename_job(job=self, new_job_name=new_job_name)

    @property
    def status(self):
        """
        Execution status of the job, can be one of the following [initialized, appended, created, submitted, running,
                                                                  aborted, collect, suspended, refresh, busy, finished]

        Returns:
            (str/pyiron_base.job.jobstatus.JobStatus): status
        """
        return self._status

    @property
    def job_id(self):
        """
        Unique id to identify the job in the pyiron database

        Returns:
            int: job id
        """
        if self._job_id is None:
            self._job_id = self.get_job_id()
        return self._job_id

    @property
    def id(self):
        """
        Unique id to identify the job in the pyiron database - use self.job_id instead

        Returns:
            int: job id
        """
        return self.job_id

    @property
    def database_entry(self):
        if not bool(self._database_property):
            self._database_property = DatabaseProperties(
                job_dict=self.project.db.get_item_by_id(self.job_id)
            )
        return self._database_property

    @property
    def parent_id(self):
        """
        Get job id of the predecessor job - the job which was executed before the current one in the current job series

        Returns:
            int: parent id
        """
        if self._parent_id:
            return self._parent_id
        elif self.job_id:
            return self.project.db.get_item_by_id(self.job_id)["parentid"]
        else:
            return None

    @parent_id.setter
    def parent_id(self, parent_id):
        """
        Set job id of the predecessor job - the job which was executed before the current one in the current job series

        Args:
            parent_id (int): parent id
        """
        if self.job_id:
            self.project.db.item_update({"parentid": parent_id}, self.job_id)
        self._parent_id = parent_id

    @property
    def master_id(self):
        """
        Get job id of the master job - a meta job which groups a series of jobs, which are executed either in parallel
        or in serial.

        Returns:
            int: master id
        """
        if self._master_id:
            return self._master_id
        elif self.job_id:
            return self.project.db.get_item_by_id(self.job_id)["masterid"]
        else:
            return None

    @master_id.setter
    def master_id(self, master_id):
        """
        Set job id of the master job - a meta job which groups a series of jobs, which are executed either in parallel
        or in serial.

        Args:
            master_id (int): master id
        """
        if self.job_id:
            self.project.db.item_update({"masterid": master_id}, self.job_id)
        self._master_id = master_id

    @property
    def child_ids(self):
        """
        list of child job ids - only meta jobs have child jobs - jobs which list the meta job as their master

        Returns:
            list: list of child job ids
        """
        id_master = self.job_id
        if id_master is None:
            return []
        else:
            id_l = self.project.db.get_items_dict(
                {"masterid": str(id_master)}, return_all_columns=False
            )
            return sorted([job["id"] for job in id_l])

    @property
    def project_hdf5(self):
        """
        Get the ProjectHDFio instance which points to the HDF5 file the job is stored in

        Returns:
            ProjectHDFio: HDF5 project
        """
        return self._hdf5

    @project_hdf5.setter
    def project_hdf5(self, project):
        """
        Set the ProjectHDFio instance which points to the HDF5 file the job is stored in

        Args:
            project (ProjectHDFio): HDF5 project
        """
        self._hdf5 = project.copy()

    @property
    def project(self):
        """
        Project instance the jobs is located in

        Returns:
            Project: project the job is located in
        """
        return self._hdf5.project

    @property
    def job_info_str(self):
        """
        Short string to describe the job by it is job_name and job ID - mainly used for logging

        Returns:
            str: job info string
        """
        return "job: {0} id: {1}".format(self._name, self.job_id)

    @property
    def working_directory(self):
        """
        working directory of the job is executed in - outside the HDF5 file

        Returns:
            str: working directory
        """
        return self.project_hdf5.working_directory

    @property
    def path(self):
        """
        Absolute path of the HDF5 group starting from the system root - combination of the absolute system path plus the
        absolute path inside the HDF5 file starting from the root group.

        Returns:
            str: absolute path
        """
        return self.project_hdf5.path

    def check_if_job_exists(self, job_name=None, project=None):
        """
        Check if a job already exists in an specific project.

        Args:
            job_name (str): Job name (optional)
            project (ProjectHDFio, Project): Project path (optional)

        Returns:
            (bool): True / False
        """
        if not job_name:
            job_name = self.job_name
        if not project:
            project = self._hdf5

        where_dict = {
            "job": str(job_name),
            "project": str(project.project_path),
            "subjob": str(project.h5_path),
        }
        if self.project.db.get_items_dict(where_dict, return_all_columns=False):
            return True
        else:
            return False

    def show_hdf(self):
        """
        Iterating over the HDF5 datastructure and generating a human readable graph.
        """
        self.project_hdf5.show_hdf()

    def get_from_table(self, path, name):
        """
        Get a specific value from a pandas.Dataframe

        Args:
            path (str): relative path to the data object
            name (str): parameter key

        Returns:
            dict, list, float, int: the value associated to the specific parameter key
        """
        return self.project_hdf5.get_from_table(path, name)

    def get_pandas(self, name):
        """
        Load a dictionary from the HDF5 file and display the dictionary as pandas Dataframe

        Args:
            name (str): HDF5 node name

        Returns:
            pandas.Dataframe: The dictionary is returned as pandas.Dataframe object
        """
        return self.project_hdf5.get_pandas(name)

    def remove(self, _protect_childs=True):
        """
        Remove the job - this removes the HDF5 file, all data stored in the HDF5 file an the corresponding database entry.

        Args:
            _protect_childs (bool): [True/False] by default child jobs can not be deleted, to maintain the consistency
                                    - default=True
        """
        # When the Job is a GenericMaster, try to delete its children first.
        if len(self.child_ids) > 0:
            if _protect_childs:
                if self._master_id is not None and not math.isnan(self._master_id):
                    s.logger.error(
                        "Job {0} is a child of a master job and cannot be deleted!".format(
                            str(self.job_id)
                        )
                    )
                    raise ValueError("Child jobs are protected and cannot be deleted!")
            for job_id in self.child_ids:
                job = self.project.load(job_id, convert_to_object=False)
                if len(job.child_ids) > 0:
                    job.remove(_protect_childs=False)
                else:
                    self.project_hdf5.remove_job(job_id, _unprotect=True)

        # After all children are deleted, remove the job itself.
        self.remove_child()

    def remove_child(self):
        """
        internal function to remove command that removes also child jobs.
        Do never use this command, since it will destroy the integrity of your project.
        """
        # Delete job from HPC-computing-queue if it is still running.
        job_status = str(self.status)
        if (
            job_status in ["submitted", "running", "collect"]
            and "server" in self.project_hdf5.list_nodes()
        ):
            server_hdf_dict = self.project_hdf5["server"]
            if (
                "qid" in server_hdf_dict.keys()
                and server_hdf_dict["qid"] is not None
            ):
                self.project.queue_delete_job(server_hdf_dict["qid"])

        # Delete working directory:
        _job_delete_files(job=self)

        # Delete HDF5 file
        with self.project_hdf5.open("..") as hdf_parent:
            hdf_groups = hdf_parent.list_groups()

        if self.job_name in hdf_groups and len(hdf_groups) < 2:
            _job_delete_hdf(job=self)
        else:
            with self.project_hdf5.open("..") as hdf_parent:
                try:
                    del hdf_parent[self.job_name]
                except (NoSuchNodeError, KeyError, OSError):
                    print(
                        "This group does not exist in the HDF5 file {}".format(
                            self.job_name
                        )
                    )

        _job_remove_folder(job=self)

        # Delete database entry
        if self.job_id:
            self.project.db.delete_item(self.job_id)

    def to_object(self, object_type=None, **qwargs):
        """
        Load the full pyiron object from an HDF5 file

        Args:
            object_type: if the 'TYPE' node is not available in the HDF5 file a manual object type can be set - optional
            **qwargs: optional parameters ['job_name', 'project'] - to specify the location of the HDF5 path

        Returns:
            GenericJob: pyiron object
        """
        if self.project_hdf5.is_empty:
            raise ValueError(
                "The HDF5 file of this job with the job_name: \""
                + self.job_name
                + "\" is empty, so it can not be loaded."
            )
        return self.project_hdf5.to_object(object_type, **qwargs)

    def get(self, name):
        """
        Internal wrapper function for __getitem__() - self[name]

        Args:
            key (str, slice): path to the data or key of the data object

        Returns:
            dict, list, float, int: data or data object
        """
        return self.__getitem__(name)

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
        return self.project.load(
            job_specifier=job_specifier,
            convert_to_object=convert_to_object
        )

    def inspect(self, job_specifier):
        """
        Inspect an existing pyiron object - most commonly a job - from the database

        Args:
            job_specifier (str, int): name of the job or job ID

        Returns:
            JobCore: Access to the HDF5 object - not a GenericJob object - use load() instead.
        """
        return self.project.load(
            job_specifier=job_specifier,
            convert_to_object=False
        )

    def load_object(self, convert_to_object=True, project=None):
        """
        Load object to convert a JobPath to an GenericJob object.

        Args:
            convert_to_object (bool): convert the object to an pyiron object or only access the HDF5 file - default=True
                                      accessing only the HDF5 file is about an order of magnitude faster, but only
                                      provides limited functionality. Compare the GenericJob object to JobCore object.
            project (ProjectHDFio): ProjectHDFio to load the object with - optional

        Returns:
            GenericJob, JobPath: depending on convert_to_object
        """
        if not project:
            project = self.project_hdf5.copy()
        if convert_to_object:
            with project.open("..") as job_dir:
                job_dir._mode = "a"
                return self.to_object(project=job_dir, job_name=self._name)
        return self

    def is_master_id(self, job_id):
        """
        Check if the job ID job_id is the master ID for any child job

        Args:
            job_id (int): job ID of the master job

        Returns:
            bool: [True/False]
        """
        return (
            len(
                [
                    job["id"]
                    for job in self.project.db.get_items_dict(
                        {"masterid": str(job_id)}, return_all_columns=False
                    )
                ]
            )
            > 0
        )

    def get_job_id(self, job_specifier=None):
        """
        get the job_id for job named job_name in the local project path from database

        Args:
            job_specifier (str, int): name of the job or job ID

        Returns:
            int: job ID of the job
        """
        if job_specifier:
            return self.project.get_job_id(
                job_specifier
            )  # , sub_job_name=self.project_hdf5.h5_path)
        else:
            where_dict = {
                "job": str(self._name),
                "project": str(self.project_hdf5.project_path),
                "subjob": str(self.project_hdf5.h5_path),
            }
            response = self.project.db.get_items_dict(
                where_dict, return_all_columns=False
            )
            if len(response) > 0:
                return response[-1]["id"]
            else:
                return None

    def list_files(self):
        """
        List files inside the working directory

        Args:
            extension (str): filter by a specific extension

        Returns:
            list: list of file names
        """
        if os.path.isdir(self.working_directory):
            return os.listdir(self.working_directory)
        return []

    def list_childs(self):
        """
        List child jobs as JobPath objects - not loading the full GenericJob objects for each child

        Returns:
            list: list of child jobs
        """
        return [
            self.project.load(child_id, convert_to_object=False).job_name
            for child_id in self.child_ids
        ]

    def list_groups(self):
        """
        equivalent to os.listdirs (consider groups as equivalent to dirs)

        Returns:
            (list): list of groups in pytables for the path self.h5_path
        """
        return self.project_hdf5.list_groups() + self._list_ext_childs()

    def list_nodes(self):
        """
        List all groups and nodes of the HDF5 file

        Returns:
            list: list of nodes
        """
        return self.project_hdf5.list_nodes()

    def list_all(self):
        """
        List all groups and nodes of the HDF5 file - where groups are equivalent to directories and nodes to files.

        Returns:
            dict: {'groups': [list of groups], 'nodes': [list of nodes]}
        """
        h5_dict = self.project_hdf5.list_all()
        h5_dict["groups"] += self._list_ext_childs()
        return h5_dict

    def copy(self):
        """
        Copy the JobCore object which links to the HDF5 file

        Returns:
            JobCore: New FileHDFio object pointing to the same HDF5 file
        """
        copied_self = copy.copy(self)
        copied_self.reset_job_id()
        return copied_self

    def _internal_copy_to(self, project=None, new_job_name=None, new_database_entry=True,
                          copy_files=True, delete_existing_job=False):
        """
        Internal helper function for copy_to() which returns more

        Args:
            project (JobCore/ProjectHDFio/Project/None): The project to copy the job to.
                (Default is None, use the same project.)
            new_job_name (str): The new name to assign the duplicate job. Required if the project is `None` or the same
                project as the copied job. (Default is None, try to keep the same name.)
            new_database_entry (bool): [True/False] to create a new database entry - default True
            copy_files (bool): [True/False] copy the files inside the working directory - default True
            delete_existing_job (bool): [True/False] Delete existing job in case it exists already (Default is False.)

        """
        # Check either a new project, a new job_name or both were specified.
        if project is None and new_job_name is None:
            raise ValueError("copy_to requires either a new project or a new_job_name.")

        # Set the new job name
        new_job_name = new_job_name or self.job_name

        # The project variable can be JobCore/ProjectHDFio/Project,
        # get a Project and a ProjectHDFio object.
        file_project, hdf5_project = _get_project_for_copy(
            job=self,
            project=project,
            new_job_name=new_job_name
        )

        # Check if the job exists already and either delete it or return it
        job_return = _copy_to_delete_existing(
            project_class=file_project,
            job_name=new_job_name,
            delete_job=delete_existing_job
        )
        if job_return is not None:
            return job_return, file_project, hdf5_project, True

        # Create a new job by copying the current python object, move the content
        # of the HDF5 file and then attach the new HDF5 link to the new python object.
        new_job_core = self.copy()
        new_job_core._name = new_job_name
        new_job_core._hdf5 = hdf5_project
        new_job_core._master_id = self._master_id
        new_job_core._parent_id = self._parent_id
        new_job_core._master_id = self._master_id
        new_job_core._status = self._status
        if new_job_name == self.job_name:
            self.project_hdf5.copy_to(destination=hdf5_project.open(".."))
        else:
            self.project_hdf5.copy_to(destination=hdf5_project, maintain_name=False)

        # Update the database entry
        if self.job_id:
            _copy_database_entry(
                new_job_core=new_job_core,
                job_copied_id=self.job_id,
                new_database_entry=new_database_entry
            )

        # Copy files outside the HDF5 file
        if copy_files and os.path.exists(self.working_directory):
            shutil.copytree(
                self.working_directory,
                new_job_core.working_directory,
            )
        return new_job_core, file_project, hdf5_project, False

    def copy_to(self, project, new_job_name=None, new_database_entry=True, copy_files=True):
        """
        Copy the content of the job including the HDF5 file to a new location

        Args:
            project (JobCore/ProjectHDFio/Project): project to copy the job to
            new_database_entry (bool): [True/False] to create a new database entry - default True
            copy_files (bool): [True/False] copy the files inside the working directory - default True

        Returns:
            JobCore: JobCore object pointing to the new location.
        """
        new_job_core, _, _, _ = self._internal_copy_to(
            project=project,
            new_job_name=new_job_name,
            new_database_entry=new_database_entry,
            copy_files=copy_files
        )
        return new_job_core

    def move_to(self, project):
        """
        Move the content of the job including the HDF5 file to a new location

        Args:
            project (ProjectHDFio): project to move the job to

        Returns:
            JobCore: JobCore object pointing to the new location.
        """
        delete_hdf5_after_copy = False
        old_working_directory = self.working_directory
        if not self.project_hdf5.file_exists:
            delete_hdf5_after_copy = True
        new_job = self.copy_to(
            project=project,
            new_database_entry=False
        )
        if self.project_hdf5.file_exists:
            if len(self.project_hdf5.h5_path.split("/")) == 2:
                self.project_hdf5.remove_file()
            else:
                self.project_hdf5.remove_group()
        self.project_hdf5 = new_job.project_hdf5.copy()
        if self._job_id:
            self.project.db.item_update(
                {
                    "subjob": self.project_hdf5.h5_path,
                    "projectpath": self.project_hdf5.root_path,
                    "project": self.project_hdf5.project_path,
                },
                self._job_id,
            )
        if delete_hdf5_after_copy:
            if len(self.project_hdf5.h5_path.split("/")) == 2:
                self.project_hdf5.remove_file()
            else:
                self.project_hdf5.remove_group()
        if os.path.exists(old_working_directory):
            shutil.rmtree(old_working_directory)
            os.rmdir("/".join(old_working_directory.split("/")[:-1]))

    def rename(self, new_job_name):
        """
        Rename the job - by changing the job name

        Args:
            new_job_name (str): new job name
        """
        self.job_name = new_job_name

    def reset_job_id(self, job_id=None):
        """
        The reset_job_id function has to be implemented by the derived classes - usually the GenericJob class

        Args:
            job_id (int/ None):

        """
        self._job_id = job_id

    def save(self):
        """
        The save function has to be implemented by the derived classes - usually the GenericJob class
        """
        raise NotImplementedError("save() should be implemented in the derived class")

    def to_hdf(self, hdf=None, group_name="group"):
        """
        Store object in hdf5 format - The function has to be implemented by the derived classes
        - usually the GenericJob class

        Args:
            hdf (ProjectHDFio): Optional hdf5 file, otherwise self is used.
            group_name (str): Optional hdf5 group in the hdf5 file.
        """
        raise NotImplementedError("to_hdf() should be implemented in the derived class")

    def from_hdf(self, hdf=None, group_name="group"):
        """
        Restore object from hdf5 format - The function has to be implemented by the derived classes
        - usually the GenericJob class

        Args:
            hdf (ProjectHDFio): Optional hdf5 file, otherwise self is used.
            group_name (str): Optional hdf5 group in the hdf5 file.
        """
        raise NotImplementedError(
            "from_hdf() should be implemented in the derived class"
        )

    def __del__(self):
        """
        The delete function is just implemented for compatibilty
        """
        del self._name
        del self._hdf5
        del self._job_id
        del self._parent_id
        del self._master_id
        del self._status

    def __getitem__(self, item):
        """
        Get/read data from the HDF5 file or access log files.

        If the job is :method:`~.decompress`ed, item can also be a file name to
        access the raw output file of that name of the job.  See available file
        with :method:`~.list_files()`.

        Args:
            item (str, slice): path to the data or key of the data object

        Returns:
            dict, list, float, int: data or data object
        """
        name_lst = item.split("/")
        item_obj = name_lst[0]
        if item_obj in self._list_ext_childs():
            # ToDo: Murn['strain_0.9'] - sucht im HDF5 file, dort gibt es aber die entsprechenden Gruppen noch nicht.
            child = self._hdf5[self._name + "_hdf5/" + item_obj]
            print("job get: ", self._name + "_jobs")
            if len(name_lst) == 1:
                return child
            else:
                return child["/".join(name_lst[1:])]

        try:
            hdf5_item = self._hdf5[item]
        except ValueError:
            hdf5_item = None
        if hdf5_item is not None:
            return hdf5_item

        if name_lst[0] in self.list_files():
            file_name = posixpath.join(self.working_directory, "{}".format(item_obj))
            with open(file_name) as f:
                return f.readlines()
        return None

    def __setitem__(self, key, value):
        """
        Stores data

        Args:
            key (str): key to store in hdf (full path)
            value (anything): value to store
        """
        if not key.startswith('user/'):
            raise ValueError("user defined paths+values must begin with user/, e.g. job['user/key'] = value")
        self._hdf5[key] = value

    def __delitem__(self, key):
        """
        Delete item from the HDF5 file

        Args:
            key (str): key of the item to delete
        """
        del self.project_hdf5[posixpath.join(self.project_hdf5.h5_path, key)]

    def __repr__(self):
        """
        Human readable string representation

        Returns:
            str: list all nodes and groups as string
        """
        return str(self.list_all())

    def _create_working_directory(self):
        """
        internal function to create the working directory on the file system if it does not exist already.
        """
        self.project_hdf5.create_working_directory()

    def _list_ext_childs(self):
        """
        internal function to list nodes excluding childs

        Returns:
            list: list of nodes without childs
        """
        nodes = self.list_nodes()
        childs = self.list_childs()
        return list(set(childs) - set(nodes))

    def compress(self, files_to_compress=None):
        """
        Compress the output files of a job object.

        Args:
            files_to_compress (list):
        """
        _job_compress(job=self, files_to_compress=files_to_compress)

    def decompress(self):
        """
        Decompress the output files of a compressed job object.
        """
        _job_decompress(job=self)

    def is_compressed(self):
        """
        Check if the job is already compressed or not.

        Returns:
            bool: [True/False]
        """
        return _job_is_compressed(job=self)

    def self_archive(self):
        """
        Compress HDF5 file of the job object to tar-archive
        """
        _job_archive(job=self)

    def self_unarchive(self):
        """
        Decompress HDF5 file of the job object from tar-archive
        """
        _job_unarchive(job=self)

    def is_self_archived(self):
        """
        Check if the HDF5 file of the Job is compressed as tar-archive

        Returns:
            bool: [True/False]
        """
        return _job_is_archived(job=self)


class DatabaseProperties(object):
    """
    Access the database entry of the job
    """

    def __init__(self, job_dict=None):
        self._job_dict = job_dict

    def __bool__(self):
        return self._job_dict is not None

    def __nonzero__(self):  # __bool__() for Python 2.7
        return self._job_dict is not None

    def __dir__(self):
        return list(self._job_dict.keys())

    def __getattr__(self, name):
        if name in self._job_dict.keys():
            return self._job_dict[name]
        else:
            raise AttributeError


class HDF5Content(object):
    """
    Access the HDF5 file of the job
    """

    def __init__(self, project_hdf5):
        self._project_hdf5 = project_hdf5

    def __getattr__(self, name):
        if name in self._project_hdf5.list_nodes():
            return self._project_hdf5.__getitem__(name)
        elif name in self._project_hdf5.list_groups():
            return HDF5Content(self._project_hdf5.__getitem__(name))
        else:
            raise AttributeError

    def __dir__(self):
        return self._project_hdf5.list_nodes() + self._project_hdf5.list_groups()

    def __repr__(self):
        return self._project_hdf5.__repr__()
