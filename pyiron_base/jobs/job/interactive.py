# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
InteractiveBase class extends the Generic Job class with all the functionality to run the job object interactivley.
"""

from typing import Any, Optional

import numpy as np

from pyiron_base.database.filetable import FileTable
from pyiron_base.jobs.job.generic import GenericJob

__author__ = "Osamu Waseda, Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2018"


class InteractiveBase(GenericJob):
    """
    InteractiveBase class extends the Generic Job class with all the functionality to run the job object interactively.
    From this class all interactive Hamiltonians are derived. Therefore it should contain the properties/routines common
    to all interactive jobs. The functions in this module should be as generic as possible.

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

        .. attribute:: version

            Version of the hamiltonian, which is also the version of the executable unless a custom executable is used.

        .. attribute:: executable

            Executable used to run the job - usually the path to an external executable.

        .. attribute:: library_activated

            For job types which offer a Python library pyiron can use the python library instead of an external executable.

        .. attribute:: server

            Server object to handle the execution environment for the job.

        .. attribute:: queue_id

            the ID returned from the queuing system - it is most likely not the same as the job ID.

        .. attribute:: logger

            logger object to monitor the external execution and internal pyiron warnings.

        .. attribute:: restart_file_list

            list of files which are used to restart the calculation from these files.

        .. attribute:: job_type

            Job type object with all the available job types: ['ExampleJob', 'ParallelMaster', 'ScriptJob',
                                                               'ListMaster']

    Examples:
        In the default 'modal' mode calculation jobs can only be executed ones:

        >>> job.run()

        Still if you want to execute multiple similar calculations, you can execute them in interactive mode:

        >>> with job.interactive_open() as job_int:
        >>>     # Do something with job_int
        >>>     job_int.run()

    """

    def __init__(
        self, project: "pyiron_base.storage.hdfio.ProjectHDFio", job_name: str
    ):
        super(InteractiveBase, self).__init__(project=project, job_name=job_name)
        self._interactive_library = None
        self._interactive_write_input_files = False
        self._interactive_flush_frequency = 10000
        self._interactive_write_frequency = 1
        self.interactive_cache = {}

    @property
    def interactive_flush_frequency(self) -> int:
        return self._interactive_flush_frequency

    @interactive_flush_frequency.setter
    def interactive_flush_frequency(self, frequency: int) -> None:
        if not isinstance(frequency, int) or frequency < 1:
            raise AssertionError("interactive_flush_frequency must be an integer>0")
        if frequency < self._interactive_write_frequency:
            raise ValueError(
                "interactive_flush_frequency must be larger or equal to interactive_write_frequency"
            )
        self._interactive_flush_frequency = frequency

    @property
    def interactive_write_frequency(self) -> int:
        return self._interactive_write_frequency

    @interactive_write_frequency.setter
    def interactive_write_frequency(self, frequency: int) -> None:
        if not isinstance(frequency, int) or frequency < 1:
            raise AssertionError("interactive_write_frequency must be an integer>0")
        if self._interactive_flush_frequency < frequency:
            self.interactive_flush_frequency = frequency
        self._interactive_write_frequency = frequency

    def validate_ready_to_run(self) -> None:
        """
        This should work but doesn't...
        """
        if self._interactive_flush_frequency < self._interactive_write_frequency:
            raise ValueError(
                "interactive_write_frequency must be smaller or equal to interactive_flush_frequency"
            )

    def _run_if_running(self) -> None:
        """
        Run the job if it is in the running state.

        Returns:
            None
        """
        if self.server.run_mode.interactive:
            self.run_if_interactive()
        elif self.server.run_mode.interactive_non_modal:
            self.run_if_interactive_non_modal()
        else:
            super(InteractiveBase, self)._run_if_running()

    def _check_if_input_should_be_written(self) -> bool:
        """
        Check if the input should be written.

        Returns:
            bool: True if the input should be written, False otherwise.
        """
        return (
            super(InteractiveBase, self)._check_if_input_should_be_written()
            or self._interactive_write_input_files
        )

    def interactive_is_activated(self) -> bool:
        """
        Check if the interactive library is activated.

        Returns:
            bool: True if the interactive library is activated, False otherwise.
        """
        if self._interactive_library is None:
            return False
        else:
            return True

    @staticmethod
    def _extend_hdf(
        h5: "pyiron_base.storage.hdfio.ProjectHDFio", path: str, key: str, data: Any
    ) -> None:
        """
        Extend an existing HDF5 dataset with new data.

        Args:
            h5 (pyiron_base.storage.hdfio.ProjectHDFio): HDF5 file object.
            path (str): Path to the dataset within the HDF5 file.
            key (str): Name of the dataset.
            data (Union[list, np.ndarray]): Data to be added to the dataset.

        Returns:
            None
        """
        if path in h5.list_groups() and key in h5[path].list_nodes():
            current_hdf = h5[path + "/" + key]
            if isinstance(data, list):
                entry = current_hdf.tolist() + data
            else:
                entry = current_hdf.tolist() + data.tolist()
            data = np.array(entry)
        h5[path + "/" + key] = data

    @staticmethod
    def _include_last_step(
        array: np.ndarray, step: int = 1, include_last: bool = False
    ) -> np.ndarray:
        """
        Returns a new array with elements selected at a given step size.

        Args:
            array (np.ndarray): The input array.
            step (int, optional): The step size for selecting elements. Defaults to 1.
            include_last (bool, optional): Whether to include the last element in the new array. Defaults to False.

        Returns:
            np.ndarray: The new array with selected elements.
        """
        if step == 1:
            return array
        if len(array) > 0:
            if len(array) > step:
                new_array = array[::step]
                index_lst = list(range(len(array)))
                if include_last and index_lst[-1] != index_lst[::step][-1]:
                    new_array.append(array[-1])
                return new_array
            else:
                if include_last:
                    return [array[-1]]
                else:
                    return []
        return []

    def interactive_flush(
        self, path: str = "interactive", include_last_step: bool = False
    ) -> None:
        """
        Flushes the interactive cache to the HDF5 file.

        Args:
            path (str): The path within the HDF5 file to store the flushed data.
            include_last_step (bool): Whether to include the last step of the cache in the flushed data.

        Returns:
            None
        """
        with self.project_hdf5.open("output") as h5:
            for key in self.interactive_cache.keys():
                if len(self.interactive_cache[key]) == 0:
                    continue
                data = self._include_last_step(
                    array=self.interactive_cache[key],
                    step=self.interactive_write_frequency,
                    include_last=include_last_step,
                )
                try:
                    if (
                        len(data) > 0
                        and isinstance(data[0], list)
                        and len(np.shape(data)) == 1
                    ):
                        self._extend_hdf(h5=h5, path=path, key=key, data=data)
                    elif np.array(data).dtype == np.dtype("O"):
                        self._extend_hdf(h5=h5, path=path, key=key, data=data)
                    else:
                        self._extend_hdf(h5=h5, path=path, key=key, data=np.array(data))
                except ValueError:
                    self._extend_hdf(
                        h5=h5, path=path, key=key, data=np.array(data, dtype="object")
                    )
                self.interactive_cache[key] = []

    def interactive_open(self) -> "pyiron_base.jobs.job.interactive.InteractiveBase":
        """
        Set the run mode to interactive.

        This is the same as setting :attr:`.server.run_mode.interactive`.

        Must be called before :meth:`.run()` is called.
        """
        self.server.run_mode.interactive = True
        return _WithInteractiveOpen(self)

    def interactive_close(self) -> None:
        """
        Stop interactive execution and sync interactive output cache.

        Sets the job status to :attr:`~.JobStatus.finished`, :meth:`.run()` cannot be called after this.
        """
        if (
            len(list(self.interactive_cache.keys())) > 0
            and len(self.interactive_cache[list(self.interactive_cache.keys())[0]]) != 0
        ):
            self.interactive_flush(path="interactive", include_last_step=True)
        self.project_hdf5.rewrite_hdf5()
        self.status.finished = True
        if not isinstance(self.project.db, FileTable):
            self.run_time_to_db()
        else:
            self._hdf5["status"] = self.status.string
        self._interactive_library = None
        for key in self.interactive_cache.keys():
            self.interactive_cache[key] = []

    def interactive_store_in_cache(self, key: str, value: Any) -> None:
        """
        Store a value in the interactive cache.

        Args:
            key (str): The key to store the value under.
            value (Any): The value to be stored.

        Returns:
            None
        """
        self.interactive_cache[key] = value

    def run_if_interactive(self) -> None:
        raise NotImplementedError("run_if_interactive() is not implemented!")

    def run_if_interactive_non_modal(self) -> None:
        raise NotImplementedError("run_if_interactive_non_modal() is not implemented!")

    def to_hdf(
        self,
        hdf: Optional["pyiron_base.storage.hdfio.ProjectHDFio"] = None,
        group_name: Optional[str] = None,
    ):
        """
        Store the InteractiveBase object in the HDF5 File

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """
        super(InteractiveBase, self).to_hdf(hdf=hdf, group_name=group_name)
        with self.project_hdf5.open("input") as hdf5_input:
            hdf5_input["interactive"] = {
                "interactive_flush_frequency": self._interactive_flush_frequency,
                "interactive_write_frequency": self._interactive_write_frequency,
            }

    def from_hdf(
        self,
        hdf: Optional["pyiron_base.storage.hdfio.ProjectHDFio"] = None,
        group_name: Optional[str] = None,
    ):
        """
        Restore the InteractiveBase object in the HDF5 File

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """
        super(InteractiveBase, self).from_hdf(hdf=hdf, group_name=group_name)
        with self.project_hdf5.open("input") as hdf5_input:
            if "interactive" in hdf5_input.list_nodes():
                interactive_dict = hdf5_input["interactive"]
                self._interactive_flush_frequency = interactive_dict[
                    "interactive_flush_frequency"
                ]
                if "interactive_write_frequency" in interactive_dict.keys():
                    self._interactive_write_frequency = interactive_dict[
                        "interactive_write_frequency"
                    ]
                else:
                    self._interactive_write_frequency = 1


class _WithInteractiveOpen:
    def __init__(self, job: InteractiveBase):
        self._job = job

    def __repr__(self) -> str:
        return "Interactive ready"

    def __enter__(self) -> InteractiveBase:
        return self._job

    def __exit__(self, exc_type, exc_val, exc_tb):
        job_status = self._job.status.string
        job_closed = self._job.interactive_close()
        if job_status in ["aborted"]:
            self._job.status.string = job_status
        return job_closed

    def __getattr__(self, attr):
        error_message = (
            "Syntax:\n"
            + "`your_job.interactive_open()`\n"
            + "`your_job.run()`\n"
            + "Alternatively you can use the `with`-statement:\n"
            + "`with your_job.interactive_open() as job_int:`\n"
            + "`    job_int.run()`\n"
        )
        raise ValueError(error_message)
