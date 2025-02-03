# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The Flexible master uses a list of functions to connect multiple jobs in a series.
"""

import inspect

from pyiron_base.jobs.job.core import _doc_str_job_core_args
from pyiron_base.jobs.job.extension.jobstatus import job_status_finished_lst
from pyiron_base.jobs.master.generic import GenericMaster, _doc_str_generic_master_attr

__author__ = "Jan Janssen, Liam Huber"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "development"
__date__ = "Mar 24, 2019"


class FlexibleMaster(GenericMaster):
    __doc__ = (
        """
    The FlexibleMaster uses a list of functions to connect multiple jobs in a series.
"""
        + "\n"
        + _doc_str_job_core_args
        + "\n"
        + _doc_str_generic_master_attr
    )

    def __init__(self, project, job_name):
        super(FlexibleMaster, self).__init__(project, job_name=job_name)
        self.__version__ = "0.1"
        self._step_function_lst = []

    @property
    def function_lst(self):
        return self._step_function_lst

    def validate_ready_to_run(self):
        """
        Checks that the number of job names is matching the number of given step functions.

        Raises:
            ValueError: if number of names is not matching number of functions
        """
        super(FlexibleMaster, self).validate_ready_to_run()
        if len(self._job_name_lst) < len(self._step_function_lst) + 1:
            raise ValueError("Not enough job names set.")
        elif len(self._job_name_lst) > len(self._step_function_lst) + 1:
            raise ValueError("Not enough step functions set.")

    def is_finished(self):
        """
        Check if the ParallelMaster job is finished - by checking the job status and the submission status.

        Returns:
            bool: [True/False]
        """
        if self.status.finished:
            return True
        if len(self._job_name_lst) > 0:
            return False
        return self.check_all_childs_finished()

    def check_all_childs_finished(self):
        return set(
            [
                self.project.db.get_job_status(job_id=child_id)
                for child_id in self.child_ids
            ]
        ) < set(job_status_finished_lst)

    def run_static(self):
        """
        The FlexibleMaster uses functions to connect multiple Jobs.
        """
        self.status.running = True
        max_steps = len(self.child_ids + self._job_name_lst)
        ind = max_steps - 1
        if self.check_all_childs_finished():
            for ind in range(len(self.child_ids), max_steps):
                job = self.pop(0)
                job._master_id = self.job_id
                if ind != 0:
                    prev_job = self[
                        self.project.db.get_item_by_id(self.child_ids[-1])["job"]
                    ]
                    if ind < max_steps:
                        mod_funct = self._step_function_lst[ind - 1]
                        mod_funct(prev_job, job)
                    job._parent_id = prev_job.job_id
                job.run()
                if job.server.run_mode.interactive and not isinstance(
                    job, GenericMaster
                ):
                    job.interactive_close()
                if self.server.run_mode.non_modal and job.server.run_mode.non_modal:
                    break
                if job.server.run_mode.queue:
                    break
        if ind == max_steps - 1 and self.is_finished():
            self.status.finished = True
            self.run_time_to_db()
        else:
            self.status.suspended = True

    def run_if_refresh(self):
        """
        Internal helper function the run if refresh function is called when the job status is 'refresh'. If the job was
        suspended previously, the job is going to be started again, to be continued.
        """
        if self.is_finished():
            self.status.collect = True
            self.run()
        elif (
            self.server.run_mode.non_modal
            or self.server.run_mode.queue
            or self.server.run_mode.modal
        ):
            self.run_static()
        else:
            self.refresh_job_status()
            if self.status.refresh:
                self.status.suspended = True
            if self.status.busy:
                self.status.refresh = True
                self.run_if_refresh()

    def collect_output(self):
        """
        Collect output is not implemented for FlexibleMaster jobs
        """
        pass

    def run_if_interactive(self):
        """
        run_if_interactive() is not implemented for FlexibleMaster jobs
        """
        pass

    def to_hdf(self, hdf=None, group_name=None):
        """
        Store the FlexibleMaster in an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """
        super(FlexibleMaster, self).to_hdf(hdf=hdf, group_name=group_name)
        with self.project_hdf5.open("input") as hdf5_input:
            if self._step_function_lst != []:
                try:
                    hdf5_input["funct_lst"] = [
                        inspect.getsource(funct) for funct in self._step_function_lst
                    ]
                except IOError:
                    pass

    def from_hdf(self, hdf=None, group_name=None):
        """
        Restore the FlexibleMaster from an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """
        super(FlexibleMaster, self).from_hdf(hdf=hdf, group_name=group_name)
        with self.project_hdf5.open("input") as hdf5_input:
            if "funct_lst" in hdf5_input.list_nodes() and self._step_function_lst == []:
                funct_str_lst = hdf5_input["funct_lst"]
                for funct_str in funct_str_lst:
                    exec(funct_str)
                    self._step_function_lst.append(eval(funct_str.split("(")[0][4:]))
