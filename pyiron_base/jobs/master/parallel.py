# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The parallel master class is a metajob consisting of a list of jobs which are executed in parallel.
"""

import importlib
import multiprocessing
from collections import OrderedDict
from datetime import datetime
from typing import Union

import numpy as np
import pandas
from pyiron_snippets.deprecate import deprecate

from pyiron_base.jobs.job.base import _doc_str_job_core_args
from pyiron_base.jobs.job.extension.jobstatus import JobStatus
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.jobs.job.util import _get_safe_job_name
from pyiron_base.jobs.job.wrapper import job_wrapper_function
from pyiron_base.jobs.master.generic import GenericMaster, _doc_str_generic_master_attr
from pyiron_base.jobs.master.submissionstatus import SubmissionStatus
from pyiron_base.state import state

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


# Modular Docstrings
_doc_str_parallel_master_attr = (
    _doc_str_generic_master_attr
    + "\n"
    + """\
        .. attribute:: ref_job

            Reference job template from which all jobs within the ParallelMaster are generated.

        .. attribute:: number_jobs_total

            Total number of jobs
"""
)


class ParallelMaster(GenericMaster):
    __doc__ = (
        """
    MasterJob that handles the creation and analysis of several parallel jobs (including master and
    continuation jobs), Examples are Murnaghan or Phonon calculations.

    Subclasses *must* implement :meth:`.collect_output()`.  Additionally :attr:`._job_generator` must be
    initialized with an instance of :class:`.JobGenerator` in the subclasses' `__init__`.
"""
        + "\n"
        + _doc_str_job_core_args
        + "\n"
        + _doc_str_parallel_master_attr
    )

    def __init__(self, project, job_name):
        super(ParallelMaster, self).__init__(project, job_name=job_name)
        self.__version__ = "0.3"
        self._ref_job = None
        self._output = GenericOutput()
        self._job_generator = None
        self.submission_status = SubmissionStatus(db=project.db, job_id=self.job_id)
        self.refresh_submission_status()

    @property
    def ref_job(self):
        """
        Get the reference job template from which all jobs within the ParallelMaster are generated.

        Returns:
            GenericJob: reference job
        """
        if self._ref_job:
            return self._ref_job
        try:
            ref_job = self[0]
            if isinstance(ref_job, GenericJob):
                self._ref_job = ref_job
                self._ref_job._job_id = None
                self._ref_job._status = JobStatus(db=self.project.db)
                return self._ref_job
            else:
                return None
        except IndexError:
            return None

    @ref_job.setter
    def ref_job(self, ref_job):
        """
        Set the reference job template from which all jobs within the ParallelMaster are generated.

        Args:
            ref_job (GenericJob): reference job
        """
        self.append(ref_job)

    @property
    def number_jobs_total(self):
        """
        Get number of total jobs

        Returns:
            int: number of total jobs
        """
        return self.submission_status.total_jobs

    @number_jobs_total.setter
    def number_jobs_total(self, num_jobs):
        """
        Set number of total jobs (optional: default = None)

        Args:
            num_jobs (int): number of submitted jobs
        """
        self.submission_status.total_jobs = num_jobs

    def reset_job_id(self, job_id=None):
        """
        Reset the job id sets the job_id to None as well as all connected modules like JobStatus and SubmissionStatus.
        """
        super(ParallelMaster, self).reset_job_id(job_id=job_id)
        if job_id is not None:
            self.submission_status = SubmissionStatus(db=self.project.db, job_id=job_id)
        else:
            self.submission_status = SubmissionStatus(
                db=self.project.db, job_id=self.job_id
            )

    def collect_output(self):
        """
        Collect the output files of the external executable and store the information in the HDF5 file. This method has
        to be implemented in the individual meta jobs derived from the ParallelMaster.
        """
        raise NotImplementedError("Implement in derived class")

    def collect_logfiles(self):
        """
        Collect the log files of the external executable and store the information in the HDF5 file. This method is
        currently not implemented for the ParallelMaster.
        """
        pass

    def output_to_pandas(self, sort_by=None, h5_path="output"):
        """
        Convert output of all child jobs to a pandas Dataframe object.

        Args:
            sort_by (str): sort the output using pandas.DataFrame.sort_values(by=sort_by)
            h5_path (str): select child output to include - default='output'

        Returns:
            pandas.Dataframe: output as dataframe
        """
        # TODO: The output to pandas function should no longer be required
        with self.project_hdf5.open(h5_path) as hdf:
            for key in hdf.list_nodes():
                self._output[key] = hdf[key]
        df = pandas.DataFrame(self._output)
        if sort_by is not None:
            df = df.sort_values(by=sort_by)
        return df

    # TODO: make it more general and move it then into genericJob
    def show_hdf(self):
        """
        Display the output of the child jobs in a human readable print out
        """
        try:
            display = getattr(importlib.import_module("IPython"), "display")
        except ModuleNotFoundError:
            print("show_hdf() requires IPython to be installed.")
        else:
            for nn in self.project_hdf5.list_groups():
                with self.project_hdf5.open(nn) as hdf_dir:
                    display.display(nn)
                    if nn.strip() == "output":
                        display.display(self.output_to_pandas(h5_path=nn))
                        continue
                    for n in hdf_dir.list_groups():
                        display.display("-->" + n)
                        try:
                            display.display(hdf_dir.get_pandas(n))
                        except Exception as e:
                            print(e)
                            print("Not a pandas object")

    def save(self):
        """
        Save the object, by writing the content to the HDF5 file and storing an entry in the database.

        Returns:
            (int): Job ID stored in the database
        """
        job_id = super(ParallelMaster, self).save()
        self.refresh_submission_status()
        return job_id

    def refresh_submission_status(self):
        """
        Refresh the submission status - if a job ID job_id is set then the submission status is loaded from the
        database.
        """
        if self.job_id:
            self.submission_status = SubmissionStatus(
                db=self.project.db, job_id=self.job_id
            )
            self.submission_status.refresh()

    def interactive_ref_job_initialize(self):
        """
        To execute the reference job in interactive mode it is necessary to initialize it.
        """
        if len(self._job_name_lst) > 0:
            self._ref_job = self.pop(-1)
            job_name = self.job_name + "_" + self._ref_job.job_name
            self._ref_job.job_name = job_name
            self._ref_job.project_hdf5 = self.child_hdf(job_name)
            if self._job_id is not None and self._ref_job._master_id is None:
                self._ref_job.master_id = self.job_id

    def copy(self):
        """
        Copy the GenericJob object which links to the job and its HDF5 file

        Returns:
            :class:`.GenericJob`: New object pointing to the same job
        """
        new_job = super(ParallelMaster, self).copy()
        if self.ref_job is not None:
            new_job.ref_job = self.ref_job
        return new_job

    def _after_generic_copy_to(self, original, new_database_entry, reloaded):
        super()._after_generic_copy_to(original, new_database_entry, reloaded)
        self.submission_status = SubmissionStatus(
            db=self._hdf5.project.db, job_id=self.job_id
        )

    def is_finished(self):
        """
        Check if the ParallelMaster job is finished - by checking the job status and the submission status.

        Returns:
            bool: [True/False]
        """
        if self.status.finished:
            return True
        if len(self.child_ids) < len(self._job_generator):
            return False
        return set(
            [self.project.db.get_job_status(child_id) for child_id in self.child_ids]
        ) < {"finished", "busy", "refresh", "aborted", "not_converged"}

    def iter_jobs(self, convert_to_object=True):
        """
        Iterate over the jobs within the ListMaster

        Args:
            convert_to_object (bool): load the full GenericJob object (default) or just the HDF5 / JobCore object

        Returns:
            yield: Yield of GenericJob or JobCore
        """
        project_working_directory = self.project.open(self.job_name + "_hdf5")
        if state.database.database_is_disabled:
            project_working_directory.db.update()
        for job_id in self._get_jobs_sorted():
            yield project_working_directory.load(
                job_id, convert_to_object=convert_to_object
            )

    def _get_jobs_sorted(self):
        job_names = self.child_names.values()
        return [
            j
            for j in [
                self._job_generator.job_name(p)
                for p in self._job_generator.parameter_list
            ]
            if j in job_names
        ]

    def __len__(self):
        """
        Length of the ListMaster equal the number of childs appended.

        Returns:
            int: length of the ListMaster
        """
        return len(self.child_ids)

    def run_if_refresh(self):
        """
        Internal helper function the run if refresh function is called when the job status is 'refresh'. If the job was
        suspended previously, the job is going to be started again, to be continued.
        """
        log_str = "{}, status: {}, finished: {} parallel master refresh".format(
            self.job_info_str, self.status, self.is_finished()
        )
        self._logger.info(log_str)
        if self.is_finished():
            self.status.collect = True
            self.run()
        elif (
            self.server.run_mode.non_modal or self.server.run_mode.queue
        ) and not self.submission_status.finished:
            self.run_static()
        else:
            self.refresh_job_status()
            if self.status.refresh:
                self.status.suspended = True
            if self.status.busy:
                self.status.refresh = True
                self.run_if_refresh()

    def _run_if_collect(self):
        """
        Internal helper function the run if collect function is called when the job status is 'collect'. It collects
        the simulation output using the standardized functions collect_output() and collect_logfiles(). Afterwards the
        status is set to 'finished'.
        """
        self._logger.info(
            "{}, status: {}, finished".format(self.job_info_str, self.status)
        )
        self.collect_output()

        job_id = self.get_job_id()
        db_dict = {}
        start_time = self.project.db.get_item_by_id(job_id)["timestart"]
        db_dict["timestop"] = datetime.now()
        db_dict["totalcputime"] = (db_dict["timestop"] - start_time).seconds
        self.project.db.item_update(db_dict, job_id)
        if not self.convergence_check():
            self.status.not_converged = True
        else:
            self.status.finished = True
        self._hdf5["status"] = self.status.string
        self._logger.info(
            "{}, status: {}, parallel master".format(self.job_info_str, self.status)
        )
        self.update_master()

    def _run_if_new(self, debug=False):
        """
        Internal helper function the run if new function is called when the job status is 'initialized'. It prepares
        the hdf5 file and the corresponding directory structure.

        Args:
            debug (bool): Debug Mode
        """
        self.submission_status.submitted_jobs = 0
        super()._run_if_new(debug=debug)

    def convergence_check(self) -> bool:
        """
        Check if and all child jobs of the calculation are converged. May need be extended in the base classes depending
        on the specific application

        Returns:
             (bool): If the calculation is converged
        """
        for job in self.iter_jobs(convert_to_object=False):
            if job.status not in ["finished", "warning"]:
                return False
        return True

    def _validate_cores(self, job, cores_for_session):
        """
        Check if enough cores are available to start the next child job.

        Args:
            job (GenericJob): child job to be started
            cores_for_session (list): list of currently active cores - list of integers

        Returns:
            bool: [True/False]
        """
        return (
            self.get_child_cores() + job.server.cores + sum(cores_for_session)
            > self.server.cores
        )

    def _next_job_series(self, job):
        """
        Generate a list of child jobs to be executed in the next iteration.

        Args:
            job (GenericJob): child job to be started

        Returns:
            list: list of GenericJob objects
        """
        job_to_be_run_lst, cores_for_session = [], []
        while job is not None:
            self._logger.debug("create job: %s %s", job.job_info_str, job.master_id)
            if not job.status.finished:
                self.submission_status.submit_next()
                job_to_be_run_lst.append(job)
                cores_for_session.append(job.server.cores)
                self._logger.info(
                    "{}: finished job {}".format(self.job_name, job.job_name)
                )
            job = next(self._job_generator, None)
            if job is not None and self._validate_cores(job, cores_for_session):
                job = None
        return job_to_be_run_lst

    def _run_if_child_queue(self, job):
        """
        run function which is executed when the child jobs are submitted to the queue. In this case all child jobs are
        submitted at the same time without considering the number of cores specified for the Parallelmaster.

        Args:
            job (GenericJob): child job to be started
        """
        while job is not None:
            self._logger.debug("create job: %s %s", job.job_info_str, job.master_id)
            if not job.status.finished:
                job.run()
                self._logger.info(
                    "{}: submitted job {}".format(self.job_name, job.job_name)
                )
            job = next(self._job_generator, None)
        self.submission_status.submitted_jobs = self.submission_status.total_jobs
        self.status.suspended = True
        if self.is_finished():
            self.status.collect = True
            self.run()

    def _run_if_master_non_modal_child_non_modal(self, job):
        """
        run function which is executed when the Parallelmaster as well as its childs are running in non modal mode.

        Args:
            job (GenericJob): child job to be started
        """
        job_to_be_run_lst = self._next_job_series(job)
        if self.project.db.get_job_status(job_id=self.job_id) != "busy":
            self.status.suspended = True
            for job in job_to_be_run_lst:
                job.run()
            if self.master_id:
                del self
        else:
            self.run_static()

    def _run_if_master_modal_child_modal(self, job):
        """
        run function which is executed when the Parallelmaster as well as its childs are running in modal mode.

        Args:
            job (GenericJob): child job to be started
        """
        while job is not None:
            self._logger.debug("create job: %s %s", job.job_info_str, job.master_id)
            if not job.status.finished:
                self.submission_status.submit_next()
                job.run()
                self._logger.info(
                    "{}: finished job {}".format(self.job_name, job.job_name)
                )
            job = next(self._job_generator, None)
        if self.is_finished():
            self.status.collect = True
            self.run()
        elif self.status.busy:
            self.status.refresh = True
            self.run_if_refresh()
        else:
            self.status.suspended = True

    def _run_if_master_modal_child_non_modal(self, job):
        """
        run function which is executed when the Parallelmaster is running in modal mode and its childs are running in
        non modal mode.

        Args:
            job (GenericJob): child job to be started
        """
        pool = multiprocessing.Pool(self.server.cores)
        job_lst = []
        for i, p in enumerate(self._job_generator.parameter_list):
            if hasattr(self._job_generator, "job_name"):
                job = self.create_child_job(self._job_generator.job_name(parameter=p))
            else:
                job = self.create_child_job(self.ref_job.job_name + "_" + str(i))
            job = self._job_generator.modify_job(job=job, parameter=p)
            job.server.run_mode.modal = True
            job.save()
            job.project_hdf5.create_working_directory()
            job.write_input()
            if state.database.database_is_disabled or (
                state.queue_adapter is not None and state.queue_adapter.remote_flag
            ):
                job_lst.append(
                    (
                        job.project.path,
                        None,
                        job.project_hdf5.file_name + job.project_hdf5.h5_path,
                        False,
                        False,
                    )
                )
            else:
                job_lst.append((job.project.path, job.job_id, None, False, False))
        pool.starmap(job_wrapper_function, job_lst)
        if state.database.database_is_disabled:
            self.project.db.update()
        self.status.collect = True
        self.run()

    def run_static(self):
        """
        The run_static function is executed within the GenericJob class and depending on the run_mode of the
        Parallelmaster and its child jobs a more specific run function is selected.
        """
        self._logger.info("{} run parallel master (modal)".format(self.job_info_str))
        self.status.running = True
        self.submission_status.total_jobs = len(self._job_generator)
        self.submission_status.submitted_jobs = 0
        if (
            self.job_id or state.database.database_is_disabled
        ) and not self.is_finished():
            self._logger.debug(
                "{} child project {}".format(self.job_name, self.project.__str__())
            )
            job = next(self._job_generator, None)
            if job is not None:
                if (
                    self.server.run_mode.non_modal
                    or self.server.run_mode.queue
                    or self.server.run_mode.modal
                ) and job.server.run_mode.interactive:
                    self.run_if_interactive()
                elif self.server.run_mode.queue:
                    self._run_if_master_modal_child_non_modal(job=job)
                elif job.server.run_mode.queue:
                    self._run_if_child_queue(job)
                elif self.server.run_mode.non_modal and job.server.run_mode.non_modal:
                    self._run_if_master_non_modal_child_non_modal(job)
                elif (self.server.run_mode.modal and job.server.run_mode.modal) or (
                    self.server.run_mode.interactive and job.server.run_mode.interactive
                ):
                    self._run_if_master_modal_child_modal(job)
                elif self.server.run_mode.modal and job.server.run_mode.non_modal:
                    self._run_if_master_modal_child_non_modal(job)
                elif job.server.run_mode.executor:
                    raise NotImplementedError(
                        "Currently ParallelMaster jobs do not support child jobs with job.server.run_mode.executor."
                    )
                else:
                    raise TypeError()
        else:
            self.status.collect = True
            self.run()

    def run_if_interactive(self):
        if not (
            self.ref_job.server.run_mode.interactive
            or self.ref_job.server.run_mode.interactive_non_modal
        ):
            raise ValueError(
                "The child job has to be run_mode interactive or interactive_non_modal."
            )
        if isinstance(self.ref_job, GenericMaster):
            self.run_static()
        elif self.server.cores == 1:
            self.interactive_ref_job_initialize()
            for parameter in self._job_generator.parameter_list:
                self._job_generator.modify_job(job=self.ref_job, parameter=parameter)
                self.ref_job.run()
            self.ref_job.interactive_close()
        else:
            if self.server.cores > len(self._job_generator.parameter_list):
                number_of_jobs = len(self._job_generator.parameter_list)
            else:
                number_of_jobs = self.server.cores
            max_tasks_per_job = (
                int(len(self._job_generator.parameter_list) // number_of_jobs) + 1
            )
            parameters_sub_lst = [
                self._job_generator.parameter_list[i : i + max_tasks_per_job]
                for i in range(
                    0, len(self._job_generator.parameter_list), max_tasks_per_job
                )
            ]
            list_of_sub_jobs = [
                self.create_child_job("job_" + str(i)) for i in range(number_of_jobs)
            ]
            primary_job = list_of_sub_jobs[0]
            if not primary_job.server.run_mode.interactive_non_modal:
                raise ValueError(
                    "The child job has to be run_mode interactive_non_modal."
                )
            if primary_job.server.cores != 1:
                raise ValueError("The child job can only use a single core.")
            for iteration in range(len(parameters_sub_lst[0])):
                for job_ind, job in enumerate(list_of_sub_jobs):
                    if iteration < len(parameters_sub_lst[job_ind]):
                        self._job_generator.modify_job(
                            job=job, parameter=parameters_sub_lst[job_ind][iteration]
                        )
                        job.run()
                for job_ind, job in enumerate(list_of_sub_jobs):
                    if iteration < len(parameters_sub_lst[job_ind]):
                        job.interactive_fetch()
            for job in list_of_sub_jobs:
                job.interactive_close()
            self.interactive_ref_job_initialize()
            self.ref_job.run()
            for key in primary_job.interactive_cache.keys():
                output_sum = []
                for job in list_of_sub_jobs:
                    output = job["output/interactive/" + key]
                    if isinstance(output, np.ndarray):
                        output = output.tolist()
                    if isinstance(output, list):
                        output_sum += output
                    else:
                        raise TypeError(
                            "output should be list or numpy.ndarray but it is ",
                            type(output),
                        )
                self.ref_job.interactive_cache[key] = output_sum
            interactive_cache_backup = self.ref_job.interactive_cache.copy()
            self.ref_job.interactive_flush(path="generic", include_last_step=True)
            self.ref_job.interactive_cache = interactive_cache_backup
            self.ref_job.interactive_close()
        self.status.collect = True
        self.run()

    def create_child_job(self, job_name):
        """
        Internal helper function to create the next child job from the reference job template - usually this is called
        as part of the create_jobs() function.

        Args:
            job_name (str): name of the next job

        Returns:
            GenericJob: next job
        """
        project = self.child_project
        if not self.server.new_hdf:
            where_dict = {
                "job": str(job_name),
                "project": str(self.project_hdf5.project_path),
                "subjob": str(self.project_hdf5.h5_path + "/" + job_name),
            }
            response = self.project.db.get_items_dict(
                where_dict, return_all_columns=False
            )
            if len(response) > 0:
                job_id = response[-1]["id"]
            else:
                job_id = None
        else:
            job_id = project.get_job_id(job_specifier=job_name)
        if job_id is not None:
            ham = project.load(job_id)
            self._logger.debug("job {} found, status: {}".format(job_name, ham.status))
            if ham.server.run_mode.queue:
                self.project.refresh_job_status_based_on_job_id(job_id, que_mode=True)
            else:
                self.project.refresh_job_status_based_on_job_id(job_id, que_mode=False)
            if ham.status.aborted:
                ham.status.created = True

            self._logger.debug("job - status: {}".format(ham.status))
            return ham

        job = self.ref_job.copy()
        job = self._load_all_child_jobs(job_to_load=job)
        job.project_hdf5 = self.child_hdf(job_name)
        if isinstance(job, GenericMaster):
            for sub_job in job._job_object_dict.values():
                self._child_job_update_hdf(parent_job=job, child_job=sub_job)
        self._logger.debug(
            "create_job:: {} {} {} {}".format(
                self.project_hdf5.path,
                self._name,
                self.project_hdf5.h5_path,
                str(self.get_job_id()),
            )
        )
        job._name = job_name
        job.master_id = self.get_job_id()
        job.status.initialized = True
        if self.server.run_mode.non_modal and job.server.run_mode.modal:
            job.server.run_mode.non_modal = True
        elif self.server.run_mode.queue:
            job.server.run_mode.thread = True
        self._logger.info("{}: run job {}".format(self.job_name, job.job_name))
        return job

    def _db_server_entry(self):
        """
        connect all the info regarding the server into a single word that can be used e.g. as entry in a database

        Returns:
            (str): server info as single word

        """
        db_entry = super(ParallelMaster, self)._db_server_entry()
        if self.submission_status.total_jobs:
            return (
                db_entry
                + "#"
                + str(self.submission_status.submitted_jobs)
                + "/"
                + str(self.submission_status.total_jobs)
            )
        else:
            return db_entry + "#" + str(self.submission_status.submitted_jobs)

    def _run_if_repair(self):
        """
        Internal helper function the run if repair function is called when the run() function is called with the
        'repair' parameter.
        """
        reload_self = self.to_object()
        reload_self._run_if_created()

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
    Generic Output just a place holder to store the output of the last child directly in the ParallelMaster.
    """

    def __init__(self):
        super(GenericOutput, self).__init__()


class JobGenerator(object):
    """
    Implements the functions to generate the parameter list, modify the individual jobs according to the parameter list
    and generate the new job names according to the parameter list.

    Subclasses have to override :meth:`.parameter_list()` to provide a list of (arbitrary) parameter objects and
    :meth:`.modify_job()` and may override :meth:`.job_name()` to provide custom job names.

    The generated jobs are created as child job from the given master.
    """

    def __init__(self, master):
        """
        Args:
            master (:class:`.ParallelMaster`): master job from which child jobs are created with
            :meth:`.ParallelMaster.create_child_job()`.
        """
        self._master = master
        self._childcounter = 0
        self._parameter_lst_cached = []

    @property
    def master(self):
        """
        :class:`.ParallelMaster`: the parallel master job with which this generator was initialized
        """
        return self._master

    @property
    @deprecate("use self.master instead")
    def _job(self):
        return self.master

    @property
    def parameter_list_cached(self):
        if len(self._parameter_lst_cached) == 0:
            self._parameter_lst_cached = self.parameter_list
        return self._parameter_lst_cached

    @property
    def parameter_list(self):
        """
        list:
            parameter objects passed to :meth:`.modify_job()` when the next
            job is requested.
        """
        raise NotImplementedError("Implement in derived class")

    @staticmethod
    def modify_job(job, parameter):
        """
        Modify next job with the parameter object.  job is already the newly
        created job object cloned from the template job, so this function has
        to return the same instance, but may (and should) modify it.

        Args:
            job (:class:`.GenericJob`):
                new job instance
            parameter (type):
                current parameter object drawn from :attr:`.parameter_list`.

        Returns:
            :class:`.GenericJob`: must be the given job
        """
        raise NotImplementedError("Implement in derived class")

    def job_name(self, parameter) -> Union[str, tuple]:
        """
        Return new job name from parameter object.  The next child job created
        will have this name.  Subclasses may override this to give custom job
        names.

        Args:
            parameter (type):
                current parameter object drawn from :attr:`.parameter_list`.

        Returns:
            str: job name for the next child job
            tuple: construct the job name via :func:`_get_safe_job_name`;
                   allows any object that can be coerced to str inside the tuple
        """
        return self._master.ref_job.job_name + "_" + str(self._childcounter)

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def __len__(self):
        return len(self.parameter_list_cached)

    def next(self):
        """
        Iterate over the child jobs

        Returns:
            :class:`~.GenericJob`: new job object
        """
        if len(self.parameter_list_cached) > self._childcounter:
            current_paramenter = self.parameter_list_cached[self._childcounter]
            job = self._master.create_child_job(
                _get_safe_job_name(self.job_name(parameter=current_paramenter))
            )
            if job is not None:
                self._childcounter += 1
                job = self.modify_job(job=job, parameter=current_paramenter)
                return job
            else:
                raise StopIteration()
        else:
            self._master.refresh_job_status()
            raise StopIteration()
