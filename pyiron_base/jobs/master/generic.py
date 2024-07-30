# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The GenericMaster is the template class for all meta jobs
"""

import inspect
import textwrap
from functools import wraps
from typing import Union

from pyiron_snippets.deprecate import deprecate

from pyiron_base.interfaces.object import HasStorage
from pyiron_base.jobs.job.core import _doc_str_job_core_args
from pyiron_base.jobs.job.extension.jobstatus import job_status_finished_lst
from pyiron_base.jobs.job.generic import GenericJob, _doc_str_generic_job_attr
from pyiron_base.storage.datacontainer import DataContainer
from pyiron_base.storage.parameters import GenericParameters

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


# Modular Docstrings
_doc_str_generic_master_attr = (
    _doc_str_generic_job_attr
    + "\n"
    + """\
        .. attribute:: child_names

            Dictionary matching the child ID to the child job name.
"""
)


class GenericMaster(GenericJob):
    __doc__ = (
        """
    The GenericMaster is the template class for all meta jobs - meaning all jobs which contain multiple other jobs. It
    defines the shared functionality of the different kind of job series.

"""
        + "\n"
        + _doc_str_job_core_args
        + "\n"
        + _doc_str_generic_master_attr
    )

    def __init__(self, project, job_name):
        super(GenericMaster, self).__init__(project, job_name=job_name)
        self._input = GenericParameters("parameters")
        self._job_name_lst = []
        self._job_object_dict = {}
        self._child_id_func = None
        self._child_id_func_str = None
        self._job_with_calculate_function = True

    @property
    def child_names(self):
        """
        Dictionary matching the child ID to the child job name

        Returns:
            dict: {child_id: child job name }
        """
        return {
            child_id: self.project.db.get_item_by_id(child_id)["job"]
            for child_id in self.child_ids
        }

    @property
    def child_ids(self):
        """
        list of child job ids - only meta jobs have child jobs - jobs which list the meta job as their master

        Returns:
            list: list of child job ids
        """
        if self._child_id_func is not None:
            return self._child_id_func(self)
        else:
            return super(GenericMaster, self).child_ids

    @property
    def child_project(self):
        """
        :class:`.Project`: project which holds the created child jobs
        """
        if not self.server.new_hdf:
            return self.project
        else:
            return self.project.open(self.job_name + "_hdf5")

    @property
    def input(self):
        return self._input

    @input.setter
    def input(self, new_input: Union[DataContainer, GenericParameters, HasStorage]):
        if isinstance(new_input, (DataContainer, GenericParameters, HasStorage)):
            self._input = new_input
        else:
            raise TypeError(
                f"Expected a DataContainer, GenericParameters or HasStorage object but got {new_input.__class__}"
            )

    def child_hdf(self, job_name):
        """
        Find correct HDF for new children.  Depending on `self.server.new_hdf` this creates a new hdf file or creates
        the group in the file of this job.

        Args:
            job_name (str): name of the new job

        Returns:
            :class:`.ProjectHDFio`: HDF file for new child job, can be assigned to its
            :attr:`~.Generic.project_hdf5`
        """
        if self.server.new_hdf:
            return self.project_hdf5.create_hdf(
                path=self.child_project.path, job_name=job_name
            )
        else:
            return self.project_hdf5.open(job_name)

    @property
    def job_object_dict(self):
        """
        internal cache of currently loaded jobs

        Returns:
            dict: Dictionary of currently loaded jobs
        """
        return self._job_object_dict

    @wraps(GenericJob.set_input_to_read_only)
    def set_input_to_read_only(self):
        super().set_input_to_read_only()
        self._input.read_only = True

    def first_child_name(self):
        """
        Get the name of the first child job

        Returns:
            str: name of the first child job
        """
        return self.project.db.get_item_by_id(self.child_ids[0])["job"]

    def append(self, job):
        """
        Append a job to the GenericMaster - just like you would append an element to a list.

        Args:
            job (GenericJob): job to append
        """
        if self.status.initialized and not job.status.initialized:
            raise ValueError(
                "GenericMaster requires reference jobs to have status initialized, rather than ",
                job.status.string,
            )
        if job.server.cores >= self.server.cores:
            self.server.cores = job.server.cores
        if job.job_name not in self._job_name_lst:
            self._job_name_lst.append(job.job_name)
            self._child_job_update_hdf(parent_job=self, child_job=job)

    def pop(self, i=-1):
        """
        Pop a job from the GenericMaster - just like you would pop an element from a list

        Args:
            i (int): position of the job. (Default is last element, -1.)

        Returns:
            GenericJob: job
        """
        job_name_to_return = self._job_name_lst[i]
        job_to_return = self._load_all_child_jobs(
            self._load_job_from_cache(job_name_to_return)
        )
        del self._job_name_lst[i]
        with self.project_hdf5.open("input") as hdf5_input:
            hdf5_input["job_list"] = self._job_name_lst
        job_to_return.relocate_hdf5()
        if isinstance(job_to_return, GenericMaster):
            for sub_job in job_to_return._job_object_dict.values():
                self._child_job_update_hdf(parent_job=job_to_return, child_job=sub_job)
        job_to_return.status.initialized = True
        return job_to_return

    def move_to(self, project):
        """
        Move the content of the job including the HDF5 file to a new location

        Args:
            project (ProjectHDFio): project to move the job to

        Returns:
            JobCore: JobCore object pointing to the new location.
        """
        if self._job_id is not None:
            for child_id in self.child_ids:
                child = self.project.load(child_id)
                child.move_to(project.open(self.job_name + "_hdf5"))
        super(GenericMaster, self).move_to(project)

    def _after_generic_copy_to(self, original, new_database_entry, reloaded):
        if reloaded:
            return

        if (
            self.job_id is not None
            and new_database_entry
            and original._job_id is not None
        ):
            for child_id in original.child_ids:
                child = original.project.load(child_id)
                new_child = child.copy_to(
                    project=self.project.open(self.job_name + "_hdf5"),
                    new_database_entry=new_database_entry,
                )
                if new_database_entry and child.parent_id:
                    new_child.parent_id = self.job_id
                if new_database_entry and child.master_id:
                    new_child.master_id = self.job_id

    def update_master(self, force_update=True):
        super().update_master(force_update=force_update)

    update_master.__doc__ = GenericJob.update_master.__doc__

    def to_hdf(self, hdf=None, group_name=None):
        """
        Store the GenericMaster in an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """
        super(GenericMaster, self).to_hdf(hdf=hdf, group_name=group_name)
        with self.project_hdf5.open("input") as hdf5_input:
            self.input.to_hdf(hdf5_input)
            hdf5_input["job_list"] = self._job_name_lst
            self._to_hdf_child_function(hdf=hdf5_input)
        for job in self._job_object_dict.values():
            job.to_hdf()

    def from_hdf(self, hdf=None, group_name=None):
        """
        Restore the GenericMaster from an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """
        super(GenericMaster, self).from_hdf(hdf=hdf, group_name=group_name)
        with self.project_hdf5.open("input") as hdf5_input:
            self.input.from_hdf(hdf5_input)
            job_list_tmp = hdf5_input["job_list"]
            self._from_hdf_child_function(hdf=hdf5_input)
            self._job_name_lst = job_list_tmp
            self._job_object_dict = {
                job_name: self._load_job_from_cache(job_name=job_name)
                for job_name in job_list_tmp
            }

    def set_child_id_func(self, child_id_func):
        """
        Add an external function to derive a list of child IDs - experimental feature

        Args:
            child_id_func (Function): Python function which returns the list of child IDs
        """
        self._child_id_func = child_id_func
        self.save()
        self.status.finished = True

    def get_child_cores(self):
        """
        Calculate the currently active number of cores, by summarizing all childs which are neither finished nor
        aborted.

        Returns:
            (int): number of cores used
        """
        return sum(
            [
                int(db_entry["computer"].split("#")[1])
                for db_entry in self.project.db.get_items_dict(
                    {"masterid": self.job_id}
                )
                if db_entry["status"] not in job_status_finished_lst
            ]
        )

    def __len__(self):
        """
        Length of the GenericMaster equal the number of childs appended.

        Returns:
            int: length of the GenericMaster
        """
        return len(self._job_name_lst)

    @deprecate(
        "Use job.output for results, job.files to access files; job.content to access HDF storage and "
        "job.child_project to access children of master jobs."
    )
    def __getitem__(self, item):
        """
        Get/ read data from the GenericMaster

        Args:
            item (str, slice): path to the data or key of the data object

        Returns:
            dict, list, float, int: data or data object
        """
        child_id_lst = self.child_ids
        child_name_lst = [
            self.project.db.get_item_by_id(child_id)["job"]
            for child_id in self.child_ids
        ]
        if isinstance(item, int):
            total_lst = self._job_name_lst + child_name_lst
            item = total_lst[item]
        return self._get_item_when_str(
            item=item, child_id_lst=child_id_lst, child_name_lst=child_name_lst
        )

    def __getattr__(self, item):
        """
        CHeck if a job with the specific name exists

        Args:
            item (str): name of the job

        Returns:

        """
        item_from_get_item = self.__getitem__(item=item)
        if item_from_get_item is not None:
            return item_from_get_item
        else:
            raise AttributeError(
                "{} tried to find child job {}, but getattr failed to find the item.".format(
                    self.job_name, item
                )
            )

    def interactive_close(self):
        """Not implemented for MetaJobs."""
        pass

    def interactive_fetch(self):
        """Not implemented for MetaJobs."""
        pass

    def interactive_flush(self, path="generic", include_last_step=True):
        """Not implemented for MetaJobs."""
        pass

    def run_if_interactive_non_modal(self):
        """Not implemented for MetaJobs."""
        pass

    def _run_if_busy(self):
        """Not implemented for MetaJobs."""
        pass

    def _load_all_child_jobs(self, job_to_load):
        """
        Helper function to load all child jobs to memory - like it was done in the previous implementation

        Args:
            job_to_load (GenericJob): job to be reloaded

        Returns:
            GenericJob: job to be reloaded - including all the child jobs and their child jobs
        """
        if isinstance(job_to_load, GenericMaster):
            for sub_job_name in job_to_load._job_name_lst:
                job_to_load._job_object_dict[sub_job_name] = self._load_all_child_jobs(
                    job_to_load._load_job_from_cache(sub_job_name)
                )
        return job_to_load

    def _load_job_from_cache(self, job_name):
        """
        Helper funcction to load a job either from the _job_object_dict or from the HDF5 file

        Args:
            job_name (str): name of the job

        Returns:
            GenericJob: the reloaded job
        """
        if job_name in self._job_object_dict.keys():
            return self._job_object_dict[job_name]
        else:
            ham_obj = self.project_hdf5[job_name].to_object(
                project=self.project_hdf5,
                job_name=job_name,
            )
            return ham_obj

    def _to_hdf_child_function(self, hdf):
        """
        Helper function to store the child function in HDF5

        Args:
            hdf: HDF5 file object
        """
        hdf["job_list"] = self._job_name_lst
        if self._child_id_func is not None:
            try:
                hdf["child_id_func"] = inspect.getsource(self._child_id_func)
            except IOError:
                hdf["child_id_func"] = self._child_id_func_str
        else:
            hdf["child_id_func"] = "None"

    def _from_hdf_child_function(self, hdf):
        """
        Helper function to load the child function from HDF5

        Args:
            hdf: HDF5 file object
        """
        try:
            child_id_func_str = hdf["child_id_func"]
        except ValueError:
            child_id_func_str = "None"
        if child_id_func_str == "None":
            self._child_id_func = None
        else:
            self._child_id_func_str = child_id_func_str
            self._child_id_func = get_function_from_string(child_id_func_str)

    def _get_item_when_str(self, item, child_id_lst, child_name_lst):
        """
        Helper function for __get_item__ when item is type string

        Args:
            item (str):
            child_id_lst (list): a list containing all child job ids
            child_name_lst (list): a list containing the names of all child jobs

        Returns:
            anything
        """
        name_lst = item.split("/")
        item_obj = name_lst[0]
        if item_obj in child_name_lst:
            child_id = child_id_lst[child_name_lst.index(item_obj)]
            if len(name_lst) > 1:
                return self.project.inspect(child_id)["/".join(name_lst[1:])]
            else:
                return self.project.load(child_id)
        elif item_obj in self._job_name_lst:
            child = self._load_job_from_cache(job_name=item_obj)
            if len(name_lst) == 1:
                return child
            else:
                return child["/".join(name_lst[1:])]
        else:
            return super(GenericMaster, self).__getitem__(item)

    def _child_job_update_hdf(self, parent_job, child_job):
        """

        Args:
            parent_job:
            child_job:
        """
        child_job.project_hdf5.file_name = parent_job.project_hdf5.file_name
        child_job.project_hdf5.h5_path = (
            parent_job.project_hdf5.h5_path + "/" + child_job.job_name
        )
        if isinstance(child_job, GenericMaster):
            for sub_job_name in child_job._job_name_lst:
                self._child_job_update_hdf(
                    parent_job=child_job,
                    child_job=child_job._load_job_from_cache(sub_job_name),
                )
        parent_job.job_object_dict[child_job.job_name] = child_job

    def _executable_activate_mpi(self):
        """
        Internal helper function to switch the executable to MPI mode
        """
        pass

    def _init_child_job(self, parent):
        """
        Update our reference job.

        Args:
            parent (:class:`.GenericJob`): job instance that this job was created from
        """
        self.ref_job = parent


def get_function_from_string(function_str):
    """
    Convert a string of source code to a function

    Args:
        function_str: function source code

    Returns:
        function:
    """
    function_dedent_str = textwrap.dedent(function_str)
    exec(function_dedent_str)
    return eval(function_dedent_str.split("(")[0][4:])
