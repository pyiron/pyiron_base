# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Worker Class to execute calculation in an asynchronous way
"""

import os
import time
from datetime import datetime
from multiprocessing import Pool
from typing import Tuple

import numpy as np
import psutil

from pyiron_base.jobs.job.template import PythonTemplateJob
from pyiron_base.state import state

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Nov 5, 2021"


def worker_function(args: list) -> None:
    """
    The worker function is executed inside an aproc processing pool.

    Args:
        args (list): A list of arguments

    Arguments inside the argument list:
        working_directory (str): working directory of the job
        job_id (int/ None): job ID
        hdf5_file (str): path to the HDF5 file of the job
        h5_path (str): path inside the HDF5 file to load the job
        submit_on_remote (bool): submit to queuing system on remote host
        debug (bool): enable debug mode [True/False] (optional)
    """
    import subprocess

    working_directory, job_link = args
    if isinstance(job_link, int) or str(job_link).isdigit():
        executable = [
            "python",
            "-m",
            "pyiron_base.cli",
            "wrapper",
            "-p",
            working_directory,
            "-j",
            str(job_link),
        ]
    else:
        executable = [
            "python",
            "-m",
            "pyiron_base.cli",
            "wrapper",
            "-p",
            working_directory,
            "-f",
            job_link,
        ]
    try:
        _ = subprocess.run(
            executable,
            cwd=working_directory,
            shell=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            universal_newlines=True,
            env=os.environ.copy(),
        )
    except subprocess.CalledProcessError:
        pass


class WorkerJob(PythonTemplateJob):
    """
    The WorkerJob executes jobs linked to its master id.

    The worker can either be in the same project as the calculation it should execute
    or a different project. For the example two projects are created:

    >>> from pyiron_base import Project
    >>> pr_worker = Project("worker")
    >>> pr_calc = Project("calc")

    The worker is configured to be executed in the background using the non_modal mode,
    with the number of cores defining the total number avaiable to the worker and the
    cores_per_job definitng the per job allocation. It is recommended to use the same
    number of cores for each task the worker executes to optimise the load balancing.

    >>> job_worker = pr_worker.create.job.WorkerJob("runner")
    >>> job_worker.server.run_mode.non_modal = True
    >>> job_worker.server.cores = 4
    >>> job_worker.input.cores_per_job = 2
    >>> job_worker.run()

    The calculation are assinged to the worker by setting the run_mode to worker and
    assigning the job_id of the worker as master_id of each job. In this example a total
    of ten toyjobs are attached to the worker, with each toyjob using two cores.

    >>> for i in range(10):
    >>>     job = pr_calc.create.job.ToyJob("toy_" + str(i))
    >>>     job.server.run_mode.worker = True
    >>>     job.server.cores = 2
    >>>     job.master_id = job_worker.job_id
    >>>     job.run()

    The execution can be monitored using the job_table of the calculation object:

    >>> pr_calc.job_table()

    Finally after all calculation are finished the status of the worker is set to collect,
    which internally stops the execution of the worker and afterwards updates the job status
    to finished:

    >>> pr_calc.wait_for_jobs()
    >>> job_worker.status.collect = True

    """

    def __init__(self, project: "pyiron_base.project.generic.Project", job_name: str):
        super(WorkerJob, self).__init__(project, job_name)
        if not state.database.database_is_disabled:
            self.input.project = project.path
        else:
            self.input.project = self.working_directory
        self.input.cores_per_job = 1
        self.input.sleep_interval = 10
        self.input.child_runtime = 0
        self.input.queue_limit_factor = 2
        self.input.maxtasksperchild = 1
        self._job_with_calculate_function = True

    @property
    def project_to_watch(self) -> "pyiron_base.project.generic.Project":
        rel_path = os.path.relpath(self.input.project, self.project.path)
        return self.project.open(rel_path)

    @project_to_watch.setter
    def project_to_watch(self, pr: "pyiron_base.project.generic.Project") -> None:
        self.input.project = pr.path

    @property
    def cores_per_job(self) -> int:
        return self.input.cores_per_job

    @cores_per_job.setter
    def cores_per_job(self, cores: int) -> None:
        self.input.cores_per_job = int(cores)

    @property
    def queue_limit_factor(self) -> int:
        return self.input.queue_limit_factor

    @queue_limit_factor.setter
    def queue_limit_factor(self, limit_factor: int) -> None:
        self.input.queue_limit_factor = limit_factor

    @property
    def child_runtime(self) -> int:  # in seconds:
        return self.input.child_runtime

    @child_runtime.setter
    def child_runtime(self, time_in_sec: int) -> None:
        self.input.child_runtime = time_in_sec

    @property
    def sleep_interval(self) -> int:  # in seconds:
        return self.input.sleep_interval

    @sleep_interval.setter
    def sleep_interval(self, interval: int) -> None:
        self.input.sleep_interval = int(interval)

    # This function is executed
    def run_static(self) -> None:
        """
        Run the worker job in static mode.

        If the database is enabled, the worker job will execute calculations by querying the database for jobs to
        execute. If the database is disabled, the worker job will execute calculations by scanning the working
        directory for HDF5 files.

        Returns:
            None
        """
        if not state.database.database_is_disabled:
            self.run_static_with_database()
        else:
            self.run_static_without_database()

    def run_static_with_database(self) -> None:
        """
        Run the worker job in static mode with database.

        This method executes calculations by querying the database for jobs to execute.

        Returns:
            None
        """
        self.status.running = True
        master_id = self.job_id
        pr = self.project_to_watch
        self.project_hdf5.create_working_directory()
        log_file = os.path.join(self.working_directory, "worker.log")
        active_job_ids, res_lst = [], []
        process = psutil.Process(os.getpid())
        number_tasks = int(self.server.cores / self.cores_per_job)
        with Pool(
            processes=number_tasks, maxtasksperchild=self.input.maxtasksperchild
        ) as pool:
            while True:
                # Check the database if there are more calculation to execute
                df = pr.job_table()
                df_sub = df[
                    (df["status"] == "submitted")
                    & (df["masterid"] == master_id)
                    & (~df["id"].isin(active_job_ids))
                ]
                if (
                    len(df_sub) > 0
                    and sum([i for r, i in res_lst if not r.ready()])
                    < number_tasks * self.input.queue_limit_factor
                ):  # Check if there are jobs to execute
                    path_lst = [
                        [pp, p, job_id]
                        for pp, p, job_id in zip(
                            df_sub["projectpath"].values,
                            df_sub["project"].values,
                            df_sub["id"].values,
                        )
                        if job_id not in active_job_ids
                    ]
                    job_lst = [
                        [p, job_id] if pp is None else [os.path.join(pp, p), job_id]
                        for pp, p, job_id in path_lst
                    ]
                    active_job_ids += [j[1] for j in job_lst]
                    result = pool.map_async(worker_function, job_lst)
                    res_lst.append([result, len(job_lst)])
                elif self.status.collect or self.status.aborted or self.status.finished:
                    if self.status.collect:
                        while sum([i for r, i in res_lst if not r.ready()]) > 0:
                            time.sleep(self.input.sleep_interval)
                            if self.status.aborted or self.status.finished:
                                break
                    break  # The infinite loop can be stopped by setting the job status to collect.
                else:  # The sleep interval can be set as part of the input
                    if self.input.child_runtime > 0:
                        df_run = df[
                            (df["status"] == "running") & (df["masterid"] == master_id)
                        ]
                        if len(df_run) > 0:
                            for job_id in df_run[
                                (
                                    np.array(datetime.now(), dtype="datetime64[ns]")
                                    - df_run.timestart.values
                                ).astype("timedelta64[s]")
                                > np.array(self.input.child_runtime).astype(
                                    "timedelta64[s]"
                                )
                            ].id.values:
                                self.project.db.set_job_status(
                                    job_id=job_id, status="aborted"
                                )
                    time.sleep(self.input.sleep_interval)

                # job submission
                with open(log_file, "a") as f:
                    f.write(
                        str(datetime.today())
                        + " "
                        + str(len(active_job_ids))
                        + " "
                        + str(len(df))
                        + " "
                        + str(len(df_sub))
                        + " "
                        + str(process.memory_info().rss / 1024 / 1024 / 1024)
                        + "GB"
                        + "\n"
                    )

        # The job is finished
        self.status.finished = True

    @staticmethod
    def _get_working_directory_and_h5path(path: str) -> Tuple[str, str]:
        """
        Get the working directory and h5path from the given path.

        Args:
            path (str): The path to the h5 file.

        Returns:
            Tuple[str, str]: The working directory and h5path.

        Example:
            >>> _get_working_directory_and_h5path("/path/to/job.h5")
            ("/path/to/job_hdf5/job", "/path/to/job.h5/job")
        """
        path_split = path.split("/")
        job_name = path_split[-1].split(".h5")[0]
        parent_dir = "/".join(path_split[:-1])
        working_directory = parent_dir + "/" + job_name + "_hdf5/" + job_name
        h5path = path + "/" + job_name
        return working_directory, h5path

    def run_static_without_database(self) -> None:
        """
        Run the worker job in static mode without a database.

        This method executes calculations by scanning the working directory for HDF5 files.

        Returns:
            None
        """
        self.project_hdf5.create_working_directory()
        working_directory = self.working_directory
        log_file = os.path.join(working_directory, "worker.log")
        file_memory_lst, res_lst = [], []
        process = psutil.Process(os.getpid())
        number_tasks = int(self.server.cores / self.cores_per_job)
        with Pool(number_tasks) as pool:
            while True:
                file_lst = [
                    os.path.join(working_directory, f)
                    for f in os.listdir(working_directory)
                    if f.endswith(".h5")
                ]
                file_vec = ~np.isin(file_lst, file_memory_lst)
                file_lst = np.array(file_lst)[file_vec].tolist()
                if (
                    len(file_lst) > 0
                    and sum([i for r, i in res_lst if not r.ready()])
                    < number_tasks * self.input.queue_limit_factor
                ):
                    job_submit_lst = [
                        self._get_working_directory_and_h5path(path=f) for f in file_lst
                    ]
                    file_memory_lst += file_lst
                    result = pool.map_async(worker_function, job_submit_lst)
                    res_lst.append([result, len(job_submit_lst)])
                elif self.project_hdf5["status"] in ["collect", "aborted", "finished"]:
                    if self.project_hdf5["status"] == "collect":
                        while sum([i for r, i in res_lst if not r.ready()]) > 0:
                            time.sleep(self.input.sleep_interval)
                            if self.project_hdf5["status"] in ["aborted", "finished"]:
                                break
                    break
                time.sleep(self.input.sleep_interval)

                with open(log_file, "a") as f:
                    f.write(
                        str(datetime.today())
                        + " "
                        + str(len(file_memory_lst))
                        + " "
                        + str(len(file_lst))
                        + " "
                        + str(process.memory_info().rss / 1024 / 1024 / 1024)
                        + "GB"
                        + "\n"
                    )

        # The job is finished
        self.status.finished = True

    def wait_for_worker(
        self, interval_in_s: int = 60, max_iterations: int = 10
    ) -> None:
        """
        Wait for the workerjob to finish the execution of all jobs. If no job is in status running or submitted the
        workerjob shuts down automatically after 10 minutes.

        Args:
            interval_in_s (int): interval when the job status is queried from the database - default 60 sec.
            max_iterations (int): maximum number of iterations - default 10
        """
        finished = False
        j = 0
        log_file = os.path.join(self.working_directory, "process.log")
        if not state.database.database_is_disabled:
            pr = self.project_to_watch
            master_id = self.job_id
        else:
            pr = self.project.open(self.working_directory)
            master_id = None
        while not finished:
            df = pr.job_table()
            if master_id is not None:
                df_sub = df[
                    ((df["status"] == "submitted") | (df.status == "running"))
                    & (df["masterid"] == master_id)
                ]
            else:
                df_sub = df[((df["status"] == "submitted") | (df.status == "running"))]
            if len(df_sub) == 0:
                j += 1
                if j > max_iterations:
                    finished = True
            else:
                j = 0
            with open(log_file, "a") as f:
                log_str = str(datetime.today()) + " j: " + str(j)
                for status in ["submitted", "running", "finished", "aborted"]:
                    log_str += (
                        "   " + status + " : " + str(len(df[df.status == status]))
                    )
                log_str += "\n"
                f.write(log_str)
            if (
                not state.database.database_is_disabled
                and state.database.get_job_status(job_id=self.job_id) == "aborted"
            ):
                raise ValueError("The worker job was aborted.")
            time.sleep(interval_in_s)
        self.status.collect = True
