# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The serial master class is a metajob consisting of a dynamic list of jobs which are executed in serial mode.
"""

from collections import OrderedDict
import inspect
import time
import numpy as np
from pyiron_base.jobs.job.core import _doc_str_job_core_args
from pyiron_base.jobs.master.generic import (
    GenericMaster,
    get_function_from_string,
    _doc_str_generic_master_attr,
)

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
_doc_str_serial_master_base_attr = (
    _doc_str_generic_master_attr
    + "\n"
    + """\
        .. attribute:: start_job

            The first job of the series.

        .. attribute:: input

            The input of the start job - the first job of the series.
"""
)


class SerialMasterBase(GenericMaster):
    __doc__ = (
        """
    The serial master class is a metajob consisting of a dynamic list of jobs which are executed in serial mode. The job
    is derived from the GenericMaster.
"""
        + "\n"
        + _doc_str_job_core_args
        + "\n"
        + _doc_str_serial_master_base_attr
    )

    def __init__(self, project, job_name):
        super(SerialMasterBase, self).__init__(project, job_name=job_name)
        self.__version__ = "0.3"
        self._output = GenericOutput()
        self._max_iterations = 100
        self._start_job = None
        self._convergence_goal = None
        self._convergence_goal_qwargs = {}
        self._convergence_goal_str = None

    @property
    def start_job(self):
        """
        Get the first job of the series.

        Returns:
            GenericJob: start job
        """
        if self._start_job is not None:
            return self._start_job
        elif len(self) > 0:
            self._start_job = self[-1]
            return self._start_job
        else:
            return None

    @start_job.setter
    def start_job(self, job):
        """
        Set the first job of the series - that is the same like appending the job.

        Args:
            job (GenericJob): start job
        """
        self.append(job)

    @property
    def ref_job(self):
        return self.start_job

    @ref_job.setter
    def ref_job(self, job):
        self.append(job)

    def get_initial_child_name(self):
        """
        Get name of the initial child.

        Returns:
            str: name of the initial child
        """
        return self.project.db.get_item_by_id(self.child_ids[0])["job"]

    def create_next(self, job_name=None):
        """
        Create the next job in the series by duplicating the previous job.

        Args:
            job_name (str): name of the new job - optional - default='job_<index>'

        Returns:
            GenericJob: next job
        """
        if len(self) == 0:
            raise ValueError("No job available in job list, please append a job first.")
        if len(self._job_name_lst) > len(self.child_ids):
            return self.pop(-1)
        ham_old = self.project.load(self.child_ids[-1], convert_to_object=True)

        if ham_old.status.aborted:
            ham_old.status.created = True
            return ham_old
        elif not ham_old.status.finished:
            return None
        if job_name is None:
            job_name = "_".join(
                ham_old.job_name.split("_")[:-1] + [str(len(self.child_ids))]
            )
        new_job = ham_old.restart(job_name=job_name)
        new_job.server.cores = self.server.cores
        return new_job

    def collect_output(self):
        """
        Collect the output files of the individual jobs and set the output of the last job to be the output of the
        SerialMaster - so the SerialMaster contains the same output as its last child.
        """
        ham_lst = [self.project_hdf5.inspect(child_id) for child_id in self.child_ids]
        if (
            "output" in ham_lst[0].list_groups()
            and "generic" in ham_lst[0]["output"].list_groups()
        ):
            nodes = ham_lst[0]["output/generic"].list_nodes()
            with self.project_hdf5.open("output/generic") as hh:
                for node in nodes:
                    if ham_lst[0]["output/generic/{}".format(node)] is not None:
                        hh[node] = np.concatenate(
                            [ham["output/generic/{}".format(node)] for ham in ham_lst],
                            axis=0,
                        )
                    else:
                        hh[node] = None

    def collect_logfiles(self):
        """
        The collect logfiles function is required by the GenericJob class, therefore we use an empty template here.
        """
        pass

    def copy(self):
        """
        Copy the GenericJob object which links to the job and its HDF5 file

        Returns:
            GenericJob: New GenericJob object pointing to the same job
        """
        new_job = super(SerialMasterBase, self).copy()
        new_job.start_job = self.start_job
        return new_job

    def from_hdf(self, hdf=None, group_name=None):
        """
        Restore the SerialMaster from an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """
        super(SerialMasterBase, self).from_hdf(hdf=hdf, group_name=group_name)
        with self.project_hdf5.open("input") as hdf5_input:
            convergence_goal_str = hdf5_input["convergence_goal"]
            if convergence_goal_str == "None":
                self._convergence_goal = None
            else:
                self._convergence_goal_str = convergence_goal_str
                self._convergence_goal = get_function_from_string(convergence_goal_str)
                self._convergence_goal_qwargs = hdf5_input["convergence_goal_qwargs"]

    def get_from_childs(self, path):
        """
        Extract the output from all child jobs and appending it to a list

        Args:
            path (str): path inside the HDF5 files of the individual jobs like 'output/generic/volume'

        Returns:
            list: list of output from the child jobs
        """
        var_lst = []
        for child_id in self.child_ids:
            ham = self.project.load(child_id, convert_to_object=False)
            var = ham.__getitem__(path)
            var_lst.append(var)
        return np.array(var_lst)

    def iter_jobs(self, convert_to_object=True):
        """
        Iterate over the jobs within the SerialMaster

        Args:
            convert_to_object (bool): load the full GenericJob object (default) or just the HDF5 / JobCore object

        Returns:
            yield: Yield of GenericJob or JobCore
        """
        for job_id in self.child_ids:
            yield self.project.load(job_id, convert_to_object=convert_to_object)

    def run_if_interactive(self):
        pass

    def _get_job_template(self):
        self._logger.info("run serial master {}".format(self.job_info_str))
        job = self.pop(-1)
        job._master_id = self.job_id
        job._hdf5 = self.child_hdf(job.job_name)
        self._logger.info("SerialMaster: run job {}".format(job.job_name))
        return job

    @staticmethod
    def _run_child_job(job):
        job.run()

    def _run_if_master_queue(self, job):
        job.server.run_mode.modal = True
        job.run()
        self.run_if_refresh()

    def _run_if_master_non_modal_child_non_modal(self, job):
        job.run()
        if self.master_id is not None:
            del self

    def _run_if_master_modal_child_modal(self, job):
        job.run()
        self.run_if_refresh()

    def _run_if_master_modal_child_non_modal(self, job):
        job.run()
        while not job.status.finished and not job.status.aborted:
            job.refresh_job_status()
            time.sleep(5)
        self.run_if_refresh()

    def run_static(self, **qwargs):
        self.status.running = True
        if len(self) > len(self.child_ids):
            job = self._get_job_template()
            self.status.suspended = True
            if self.server.run_mode.queue:
                self._run_if_master_queue(job)
            elif self.server.run_mode.non_modal and job.server.run_mode.non_modal:
                self._run_if_master_non_modal_child_non_modal(job)
            elif self.server.run_mode.modal and job.server.run_mode.modal:
                self._run_if_master_modal_child_modal(job)
            elif self.server.run_mode.modal and job.server.run_mode.non_modal:
                self._run_if_master_modal_child_non_modal(job)
            else:
                raise TypeError()
        else:
            self.status.collect = True
            self.run()

    def set_goal(self, convergence_goal, **qwargs):
        """
        Set a convergence goal for the SerialMaster - this is necessary to stop the series.

        Args:
            convergence_goal (Function): the convergence goal can be any Python function, but if external packages are
                                         used like numpy they have to be imported within the function.
            **qwargs: arguments of the convergence goal function.
        """
        self._convergence_goal = convergence_goal
        self._convergence_goal_qwargs = qwargs
        self._convergence_goal_str = inspect.getsource(convergence_goal)
        if self.project_hdf5.file_exists:
            self.to_hdf()

    def show(self):
        """
        list all jobs in the SerialMaster

        Returns:
            list: list of jobs ['job', <index>, <GenericJob>]
        """
        return [["job", str(i), str(job)] for i, job in enumerate(self)]

    def to_hdf(self, hdf=None, group_name=None):
        """
        Store the SerialMaster in an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """
        super(SerialMasterBase, self).to_hdf(hdf=hdf, group_name=group_name)
        with self.project_hdf5.open("input") as hdf5_input:
            if self._convergence_goal is not None:
                try:
                    hdf5_input["convergence_goal"] = inspect.getsource(
                        self._convergence_goal
                    )
                except IOError:
                    hdf5_input["convergence_goal"] = self._convergence_goal_str

                hdf5_input["convergence_goal_qwargs"] = self._convergence_goal_qwargs
            else:
                hdf5_input["convergence_goal"] = "None"

    def __len__(self):
        """
        Length of the SerialMaster equal the number of childs appended.

        Returns:
            int: length of the SerialMaster
        """
        return len(self.child_ids + self._job_name_lst)

    def run_if_refresh(self):
        """
        Internal helper function the run if refresh function is called when the job status is 'refresh'. If the job was
        suspended previously, the job is going to be started again, to be continued.
        """
        conv_goal_exists = bool(self._convergence_goal)
        self._logger.info("Does the convergence goal exit: {}".format(conv_goal_exists))
        if not conv_goal_exists:
            self.status.collect = True
            self.run()
        else:
            subjobs_statuses = set(
                [
                    self.project.db.get_job_status(job_id=child_id)
                    for child_id in self.child_ids
                ]
            )
            if len(subjobs_statuses) == 0 or subjobs_statuses == {"finished"}:
                ham = self._convergence_goal(self, **self._convergence_goal_qwargs)
                if ham is not True:
                    self.append(ham)
                    self.to_hdf()
                    self.run_static()
                else:
                    self.status.collect = True
                    self.run()

    def _init_child_job(self, parent):
        """
        Update our reference job and copy their run mode.

        Args:
            parent (:class:`.GenericJob`): job instance that this job was created from
        """
        super()._init_child_job(parent)
        if parent.server.run_mode.non_modal:
            self.server.run_mode.non_modal = True
        elif (
            parent.server.run_mode.interactive
            or parent.server.run_mode.interactive_non_modal
        ):
            self.server.run_mode.interactive = True


class GenericOutput(OrderedDict):
    """
    Generic Output just a place holder to store the output of the last child directly in the SerialMaster.
    """

    def __init__(self):
        super(GenericOutput, self).__init__()
