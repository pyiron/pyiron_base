# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The job wrapper is called from the run_job.py script, it restores the job from hdf5 and executes it.
"""

import logging
import os
from typing import Optional

from pyiron_base.database.filetable import (
    get_hamilton_from_file,
    get_hamilton_version_from_file,
    get_job_status_from_file,
)
from pyiron_base.project.generic import Project
from pyiron_base.state import state
from pyiron_base.state.signal import catch_signals

__author__ = "Joerg Neugebauer"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


class JobWrapper(object):
    """
    The job wrapper is called from the run_job.py script, it restores the job from hdf5 and executes it.

    Args:
        working_directory (str): working directory of the job
        job_id (int/ None): job ID
        hdf5_file (str): path to the HDF5 file of the job
        h5_path (str): path inside the HDF5 file to load the job
        submit_on_remote (bool): submit to queuing system on remote host
        debug (bool): enable debug mode [True/False] (optional)
    """

    def __init__(
        self,
        working_directory: str,
        job_id: Optional[int] = None,
        hdf5_file: Optional["pyiron_base.storage.hdfio.ProjectHDFio"] = None,
        h5_path: Optional[str] = None,
        submit_on_remote: bool = False,
        debug: bool = False,
        connection_string: Optional[str] = None,
        collect: bool = False,
    ):
        self.working_directory = working_directory
        self._remote_flag = submit_on_remote
        self._collect = collect
        if connection_string is not None:
            state.database.open_local_sqlite_connection(
                connection_string=connection_string
            )
        pr = Project(path=os.path.join(working_directory, "..", ".."))
        if job_id is not None:
            self.job = pr.load(int(job_id))
        else:
            projectpath = state.database.top_path(hdf5_file)
            if projectpath is None:
                project = os.path.dirname(hdf5_file)
            else:
                project = os.path.relpath(os.path.dirname(hdf5_file), projectpath)
            job_name = h5_path[1:]
            self.job = pr.load_from_jobpath(
                job_id=None,
                db_entry={
                    "job": job_name,
                    "subjob": h5_path,
                    "projectpath": projectpath,
                    "project": project + "/",
                    "status": get_job_status_from_file(
                        hdf5_file=hdf5_file, job_name=job_name
                    ),
                    "hamilton": get_hamilton_from_file(
                        hdf5_file=hdf5_file, job_name=job_name
                    ),
                    "hamversion": get_hamilton_version_from_file(
                        hdf5_file=hdf5_file, job_name=job_name
                    ),
                },
                convert_to_object=True,
            )

        # setup logger
        self._logger = self.setup_logger(debug=debug)

    @staticmethod
    def setup_logger(debug: bool = False) -> logging.Logger:
        """
        Setup the error logger

        Args:
            debug (bool): the level of logging, enable debug mode [True/False] (optional)

        Returns:
            logger: logger object instance
        """
        logger = logging.getLogger("pyiron_log")
        logger.setLevel(logging.INFO)
        if debug:
            logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def run(self) -> None:
        """
        The job wrapper run command, sets the job status to 'running' and executes run_if_modal().
        """
        if self._remote_flag and self.job.server.queue is not None:
            self.job.run_if_scheduler()
            self.job.status.submitted = True
        elif self._collect:
            self.job.status.collect = True
            self.job.run()
        else:
            with catch_signals(self.job.signal_intercept):
                self.job.run_static()


def job_wrapper_function(
    working_directory: str,
    job_id: Optional[int] = None,
    file_path: Optional[str] = None,
    submit_on_remote: bool = False,
    debug: bool = False,
    collect: bool = False,
):
    """
    Job Wrapper function - creates a JobWrapper object and calls run() on that object

    Args:
        working_directory (str): directory where the HDF5 file of the job is located
        job_id (int/ None): job id
        file_path (str): path to the HDF5 file
        debug (bool): enable debug mode
        submit_on_remote (bool): submit to queuing system on remote host
        collect (bool): collect output of calculation
    """

    # always close the database connection in calculations on the cluster to avoid high number of concurrent
    # connections.
    state.database.close_connection()
    state.database.connection_timeout = 0
    state.database.open_connection()

    if job_id is not None:
        job = JobWrapper(
            working_directory=working_directory,
            job_id=job_id,
            submit_on_remote=submit_on_remote,
            debug=debug,
            collect=collect,
        )
    elif file_path is not None:
        hdf5_file = (
            ".".join(file_path.split(".")[:-1])
            + "."
            + file_path.split(".")[-1].split("/")[0]
        )
        h5_path = "/".join(file_path.split(".")[-1].split("/")[1:])
        job = JobWrapper(
            working_directory,
            job_id=None,
            hdf5_file=hdf5_file,
            h5_path="/" + h5_path,
            submit_on_remote=submit_on_remote,
            debug=debug,
            collect=collect,
        )
    else:
        raise ValueError("Either job_id or file_path have to be not None.")
    job.run()
