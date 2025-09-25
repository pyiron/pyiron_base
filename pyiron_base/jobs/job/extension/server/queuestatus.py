# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Set of functions to interact with the queuing system directly from within pyiron - optimized for the Sun grid engine.
"""

import time
from concurrent.futures import Future
from typing import List, Optional, Union

import numpy as np
import pandas

from pyiron_base.jobs.job.extension.jobstatus import job_status_finished_lst
from pyiron_base.state import state
from pyiron_base.utils.instance import static_isinstance

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

QUEUE_SCRIPT_PREFIX = "pi_"


def queue_table(
    job_ids: Optional[List[int]] = None,
    working_directory_lst: Optional[List[str]] = None,
    project_only: bool = True,
    full_table: bool = False,
) -> pandas.DataFrame:
    """
    Display the queuing system table as pandas.Dataframe

    Args:
        job_ids (list): check for a specific list of job IDs - empty list by default
        working_directory_lst (list): list of working directories to include - empty list by default
        project_only (bool): Query only for jobs within the current project - True by default
        full_table (bool): Return all entries from the queuing system without filtering - False by default

    Returns:
        pandas.DataFrame: Output from the queuing system - optimized for the Sun grid engine
    """
    job_ids = [] if job_ids is None else job_ids
    working_directory_lst = (
        [] if working_directory_lst is None else working_directory_lst
    )
    if project_only and not job_ids and not working_directory_lst:
        return []
    if state.queue_adapter is not None:
        if full_table:
            pandas.set_option("display.max_rows", None)
            pandas.set_option("display.max_columns", None)
        else:
            pandas.reset_option("display.max_rows")
            pandas.reset_option("display.max_columns")
        df = state.queue_adapter.get_status_of_my_jobs()
        if not project_only:
            return df[
                [
                    True if QUEUE_SCRIPT_PREFIX in job_name else False
                    for job_name in list(df.jobname)
                ]
            ]
        else:
            if len(job_ids) > len(working_directory_lst):
                job_name_lst = [QUEUE_SCRIPT_PREFIX + str(job_id) for job_id in job_ids]
                return df[
                    [
                        True if job_name in job_name_lst else False
                        for job_name in list(df.jobname)
                    ]
                ]
            else:
                if len(df) > 0 and "working_directory" in df.columns:
                    return df[
                        [
                            any(
                                [
                                    working_dir.startswith(p)
                                    for p in working_directory_lst
                                ]
                            )
                            for working_dir in list(df.working_directory)
                        ]
                    ]
                else:
                    return df
    else:
        return None


def queue_check_job_is_waiting_or_running(
    item: Union[int, "pyiron_base.jobs.job.generic.GenericJob"],
) -> Union[bool, None]:
    """
    Check if a job is still listed in the queue system as either waiting or running.

    Args:
        item (int, GenericJob): Provide either the job_ID or the full hamiltonian

    Returns:
        bool: [True/False]
    """
    que_id = validate_que_request(item)
    if state.queue_adapter is not None:
        return state.queue_adapter.get_status_of_job(process_id=que_id) in [
            "pending",
            "running",
        ]
    else:
        return None


def queue_info_by_job_id(job_id: int) -> dict:
    """
    Display the queuing system info of job by qstat | grep  shell command
    as dictionary

    Args:
        job_id (int): query for a specific job_id

    Returns:
        dict: Dictionary with the output from the queuing system - optimized for the Sun grid engine
    """
    if state.queue_adapter is not None:
        return state.queue_adapter.get_status_of_job(process_id=job_id)
    else:
        return None


def queue_is_empty() -> bool:
    """
    Check if the queue table is currently empty - no more jobs to wait for.

    Returns:
        bool: True if the table is empty, else False - optimized for the Sun grid engine
    """
    if state.queue_adapter is not None:
        return len(state.queue_adapter.get_status_of_my_jobs()) == 0
    else:
        return True


def queue_delete_job(
    item: Union[int, "pyiron_base.jobs.job.generic.GenericJob"],
) -> Union[str, None]:
    """
    Delete a job from the queuing system

    Args:
        item (int, pyiron_base.jobs.job.generic.GenericJob): Provide either the job_ID or the full hamiltonian

    Returns:
        str: Output from the queuing system as string - optimized for the Sun grid engine
    """
    que_id = validate_que_request(item)
    if state.queue_adapter is not None:
        return state.queue_adapter.delete_job(process_id=que_id)
    else:
        return None


def queue_enable_reservation(
    item: Union[int, "pyiron_base.jobs.job.generic.GenericJob"],
) -> Union[str, None]:
    """
    Enable a reservation for a particular job within the queuing system

    Args:
        item (int, pyiron_base.jobs.job.generic.GenericJob): Provide either the job_ID or the full hamiltonian

    Returns:
        str: Output from the queuing system as string - optimized for the Sun grid engine
    """
    que_id = validate_que_request(item)
    if state.queue_adapter is not None:
        if isinstance(que_id, list):
            return [
                state.queue_adapter.enable_reservation(process_id=q) for q in que_id
            ]
        else:
            return state.queue_adapter.enable_reservation(process_id=que_id)
    else:
        return None


def wait_for_job(
    job: "pyiron_base.jobs.job.generic.GenericJob",
    interval_in_s: int = 5,
    max_iterations: int = 100,
) -> None:
    """
    Sleep until the job is finished but maximum interval_in_s * max_iterations seconds.

    Args:
        job (pyiron_base.job.utils.GenericJob): Job to wait for
        interval_in_s (int): interval when the job status is queried from the database - default 5 sec.
        max_iterations (int): maximum number of iterations - default 100

    Raises:
        ValueError: max_iterations reached, job still running
    """
    if job.status.string not in job_status_finished_lst:
        if (
            state.queue_adapter is not None
            and state.queue_adapter.remote_flag
            and job.server.queue is not None
        ):
            finished = False
            for _ in range(max_iterations):
                if not queue_check_job_is_waiting_or_running(item=job):
                    state.queue_adapter.transfer_file_to_remote(
                        file=job.project_hdf5.file_name,
                        transfer_back=True,
                        delete_file_on_remote=False,
                    )
                    status_hdf5 = job.project_hdf5["status"]
                    job.status.string = status_hdf5
                else:
                    status_hdf5 = job.status.string
                if status_hdf5 in job_status_finished_lst:
                    job.transfer_from_remote()
                    finished = True
                    break
                time.sleep(interval_in_s)
            if not finished:
                raise ValueError(
                    "Maximum iterations reached, but the job was not finished."
                )
        else:
            finished = False
            for _ in range(max_iterations):
                if state.database.database_is_disabled:
                    job.project.db.update()
                job.refresh_job_status()
                if job.status.string in job_status_finished_lst:
                    finished = True
                    break
                elif isinstance(job.server.future, Future):
                    try:
                        job.server.future.result(timeout=interval_in_s)
                    except TimeoutError:
                        pass
                    else:
                        finished = job.server.future.done()
                        break
                else:
                    time.sleep(interval_in_s)
            if not finished:
                raise ValueError(
                    "Maximum iterations reached, but the job was not finished."
                )


def wait_for_jobs(
    project: "pyiron_base.project.generic.Project",
    interval_in_s: int = 5,
    max_iterations: int = 100,
    recursive: bool = True,
    ignore_exceptions: bool = False,
    try_collecting: bool = False,
) -> None:
    """
    Wait for the calculation in the project to be finished

    Args:
        project: Project instance the jobs is located in
        interval_in_s (int): interval when the job status is queried from the database - default 5 sec.
        max_iterations (int): maximum number of iterations - default 100
        recursive (bool): search subprojects [True/False] - default=True
        ignore_exceptions (bool): ignore eventual exceptions when retrieving jobs - default=False
        try_collecting (bool): try to run collect for fetched jobs that don't have a status counting as finished - default=False

    Raises:
        ValueError: max_iterations reached, but jobs still running
    """
    finished = False
    for _ in range(max_iterations):
        project.update_from_remote(recursive=True, ignore_exceptions=ignore_exceptions)
        project.refresh_job_status()
        df = project.job_table(recursive=recursive)
        if all(df.status.isin(job_status_finished_lst)):
            finished = True
            break
        time.sleep(interval_in_s)
    if not finished:
        raise ValueError("Maximum iterations reached, but the job was not finished.")


def update_from_remote(
    project: "pyiron_base.project.generic.Project",
    recursive: bool = True,
    ignore_exceptions: bool = False,
    try_collecting: bool = False,
) -> None:
    """
    Update jobs from the remote server

    Args:
        project: Project instance the jobs is located in
        recursive (bool): search subprojects [True/False] - default=True
        ignore_exceptions (bool): ignore eventual exceptions when retrieving jobs - default=False
        try_collecting (bool): try to collect jobs that don't have a status counting as finished - default=False

    Returns:
        returns None if ignore_exceptions is False or when no error occured.
        returns a list with job ids when errors occured, but were ignored
    """
    if state.queue_adapter is not None and state.queue_adapter.remote_flag:
        df_project = project.job_table(recursive=recursive)
        df_submitted = df_project[df_project.status == "submitted"]
        df_combined = df_project[df_project.status.isin(["running", "submitted"])]
        df_queue = state.queue_adapter.get_status_of_my_jobs()
        if (
            len(df_queue) > 0
            and len(df_queue[df_queue.jobname.str.contains(QUEUE_SCRIPT_PREFIX)]) > 0
        ):
            df_queue = df_queue[df_queue.jobname.str.contains(QUEUE_SCRIPT_PREFIX)]
            df_queue["pyiron_id"] = df_queue.apply(
                lambda x: int(x["jobname"].split(QUEUE_SCRIPT_PREFIX)[-1]), axis=1
            )
            queue_running = df_queue[df_queue.status == "running"].pyiron_id.values
            jobs_now_running_lst = df_submitted.id.values[
                np.isin(df_submitted.id.values, queue_running)
            ]
            project.db.set_job_status(status="running", job_id=jobs_now_running_lst)

            fetch_ids = df_combined.id.values[
                np.isin(df_combined.id.values, df_queue.pyiron_id.values, invert=True)
            ]
        else:  # handle empty pyiron queue case for fetching
            fetch_ids = df_combined.id.values

        failed_jobs = []
        for job_id in fetch_ids:
            try:
                job = project.load(job_id)
                retrieve_job(job, try_collecting=try_collecting)
            except Exception as e:
                if ignore_exceptions:
                    state.logger.warning(
                        f"An error occurred while trying to retrieve job {job_id}\n"
                        f"Error message: \n{e}"
                    )
                    failed_jobs.append(job_id)
                else:
                    raise e

        if len(failed_jobs) > 0:
            return failed_jobs


def retrieve_job(
    job: "pyiron_base.jobs.job.generic.GenericJob", try_collecting: bool = False
) -> None:
    """
    Retrieve a job from remote server and check if it has a "finished status".
    Optionally try to collect its output.

    Args:
        job: pyiron job
        try_collecting (bool): whether to run collect if not finished - default=False

    Returns:
        returns None
    """
    job.transfer_from_remote()
    if job.status in job_status_finished_lst:
        return

    if try_collecting:
        job.status.collect = True
        job.run()


def validate_que_request(
    item: Union[int, "pyiron_base.jobs.job.generic.GenericJob"],
) -> int:
    """
    Internal function to convert the job_ID or hamiltonian to the queuing system ID.

    Args:
        item (int, pyiron_base.jobs.job.generic.GenericJob): Provide either the job_ID or the full hamiltonian

    Returns:
        int: queuing system ID
    """

    if isinstance(item, int):
        que_id = item
    elif static_isinstance(
        item.__class__, "pyiron_base.jobs.master.generic.GenericMaster"
    ):
        if item.server.queue_id:
            que_id = item.server.queue_id
        else:
            queue_id_lst = [
                item.project.load(child_id).server.queue_id
                for child_id in item.child_ids
            ]
            que_id = [queue_id for queue_id in queue_id_lst if queue_id is not None]
            if len(que_id) == 0:
                raise ValueError("This job does not have a queue ID.")
    elif static_isinstance(item.__class__, "pyiron_base.jobs.job.generic.GenericJob"):
        if item.server.queue_id:
            que_id = item.server.queue_id
        else:
            raise ValueError("This job does not have a queue ID.")
    elif static_isinstance(item.__class__, "pyiron_base.jobs.job.base.JobCore"):
        if "server" in item.project_hdf5.list_nodes():
            server_hdf_dict = item.project_hdf5["server"]
            if "qid" in server_hdf_dict.keys():
                que_id = server_hdf_dict["qid"]
            else:
                raise ValueError("This job does not have a queue ID.")
        else:
            raise ValueError("This job does not have a queue ID.")
    else:
        raise TypeError(
            "The queue can either query for IDs or for pyiron GenericJobObjects."
        )
    return que_id
