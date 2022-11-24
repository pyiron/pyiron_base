# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The GenericMaster is the template class for all meta jobs
"""

import inspect
import textwrap

from pyiron_base.interfaces.object import HasStorage
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.jobs.job.extension.jobstatus import job_status_finished_lst
from pyiron_base.jobs.job.jobtype import JobType

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


class GenericMaster(GenericJob, HasStorage):
    """
    The GenericMaster is the template class for all meta jobs - meaning all jobs which contain multiple other jobs. It
    defines the shared functionality of the different kind of job series.

    Args:
        project (ProjectHDFio): ProjectHDFio instance which points to the HDF5 file the job is stored in
        job_name (str): name of the job, which has to be unique within the project

    Attributes:

        .. attribute:: job_name

            name of the job, which has to be unique within the project

        .. attribute:: status

            execution status of the job, can be one of the following [initialized, appended, created, submitted,
                                                                      running, aborted, collect, suspended, refresh,
                                                                      busy, finished]

        .. attribute:: job_id

            unique id to identify the job in the pyiron database

        .. attribute:: parent_id

            job id of the predecessor job - the job which was executed before the current one in the current job series

        .. attribute:: master_id

            job id of the master job - a meta job which groups a series of jobs, which are executed either in parallel
            or in serial.

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

        .. attribute:: version

            Version of the hamiltonian, which is also the version of the executable unless a custom executable is used.

        .. attribute:: executable

            Executable used to run the job - usually the path to an external executable.

        .. attribute:: library_activated

            For job types which offer a Python library pyiron can use the python library instead of an external
            executable.

        .. attribute:: server

            Server object to handle the execution environment for the job.

        .. attribute:: queue_id

            the ID returned from the queuing system - it is most likely not the same as the job ID.

        .. attribute:: logger

            logger object to monitor the external execution and internal pyiron warnings.

        .. attribute:: restart_file_list

            list of files which are used to restart the calculation from these files.

        .. attribute:: job_type

            Job type object with all the available job types: ['ExampleJob', 'SerialMaster', 'ParallelMaster',
                                                               'ScriptJob', 'ListMaster']

        .. attribute:: child_names

            Dictionary matching the child ID to the child job name.
    """

    def __init__(self, project, job_name):
        super(GenericMaster, self).__init__(project, job_name=job_name)
        self._job_name_lst = []
        self._job_object_dict = {}
        self._child_id_func = None
        self._child_id_func_str = None
        HasStorage.__init__(self, group_name="")
        self.storage.create_group("input")
        self.storage.input.create_group("user")
        self.storage.input.create_group("child")
        self.storage.create_group("output")
        self._ref_job = None
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

    def set_input_to_read_only(self):
        """
        This function enforces read-only mode for the input classes, but it has to be implement in the individual
        classes.
        """
        self.input.read_only = True

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
            self._ref_job.job_name = self.job_name + "_" + self._ref_job.job_name
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

    def __getitem__(self, item):
        """
        Get/ read data from the HDF5 file

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

    def run_if_refresh(self):
        """
        Internal helper function the run if refresh function is called when the job status is 'refresh'. If the job was
        suspended previously, the job is going to be started again, to be continued.
        """
        self._logger.info(
            "{}, status: {}, finished: {} parallel master "
            "refresh".format(self.job_info_str, self.status, self.is_finished())
        )
        if self.is_finished():
            self.status.collect = True
            self.run()  # self.run_if_collect()
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
        # self.send_to_database()

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
        self.run()  # self.run_if_collect()

    def run_static(self):
        """
        The run_static function is executed within the GenericJob class and depending on the run_mode of the
        Parallelmaster and its child jobs a more specific run function is selected.
        """
        self._logger.info("{} run parallel master (modal)".format(self.job_info_str))
        self.status.running = True
        self.submission_status.total_jobs = len(self._job_generator)
        self.submission_status.submitted_jobs = 0
        if self.job_id and not self.is_finished():
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

    @property
    def job_generator(self):
        raise NotImplementedError('job_generator must be defind in the child class')

    @property
    def input(self):
        return self.storage.input.user

    @property
    def output(self):
        return self.storage.output

    @property
    def child_names(self):
        """
        Dictionary matching the child ID to the child job name

        Returns:
            dict: {child_id: child job name }
        """
        child_dict = {}
        for child_id in self.child_ids:
            child_dict[child_id] = self.project.db.get_item_by_id(child_id)["job"]
        return child_dict

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
        GenericJob.to_hdf(self, hdf=hdf, group_name=group_name)
        HasStorage.to_hdf(self, hdf=self.project_hdf5)
        with self.project_hdf5.open("input") as hdf5_input:
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
        GenericJob.from_hdf(self, hdf=hdf, group_name=group_name)
        HasStorage.from_hdf(self, hdf=self.project_hdf5)
        with self.project_hdf5.open("input") as hdf5_input:
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
            item = self._job_name_lst[item]
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
                return self.project.load(child_id, convert_to_object=True)
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

    def job_name(self, parameter):
        """
        Return new job name from parameter object.  The next child job created
        will have this name.  Subclasses may override this to give custom job
        names.

        Args:
            parameter (type):
                current parameter object drawn from :attr:`.parameter_list`.

        Returns:
            str: job name for the next child job
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
                self.job_name(parameter=current_paramenter)
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
