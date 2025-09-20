# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Server object class which is connected to each job containing the technical details how the job is executed.
"""

import numbers
import socket
from concurrent.futures import Executor, Future
from dataclasses import asdict, fields
from typing import Optional, Union

import pandas
from pyiron_dataclasses.v1.jobs.generic import Server as ServerDataClass
from pyiron_snippets.deprecate import deprecate

from pyiron_base.interfaces.has_dict import HasDict
from pyiron_base.interfaces.lockable import Lockable, sentinel
from pyiron_base.jobs.job.extension.server.runmode import Runmode
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


class Server(
    Lockable, HasDict
):  # add the option to return the job id and the hold id to the server object
    """
    Generic Server object to handle the execution environment for the job

    Args:
        host (str): hostname of the local machine
        queue (str): queue name of the currently selected queue
        cores (int): number of cores
        run_mode (pyiron_base.server.runmode.Runmode): mode of the job ['modal', 'non_modal', 'queue', 'manual']
        new_hdf (bool): create a new HDF5 file [True/False] - default=True
        accept_crash (bool): ignore execution errors raised by external executables - default False
        run_time (int): run time limit in seconds for the job to finish - required for HPC job schedulers
        memory_limit (str): memory required
        qid (int): Queuing system ID - ID received from the HPC job scheduler
        additional_arguments (dict): Additional arguments for the HPC job scheduler
        conda_environment_name (str): Name of the conda environment
        conda_environment_path (str): Path to the conda environment

    Attributes:

        .. attribute:: host

            the hostname of the current system.

        .. attribute:: queue

            the queue selected for a current simulation.

        .. attribute:: cores

            the number of cores selected for the current simulation.

        .. attribute:: run_time

            the run time in seconds selected for the current simulation.

        .. attribute:: run_mode

            the run mode of the job ['modal', 'non_modal', 'queue', 'manual']

        .. attribute:: memory_limit

            the maximum amount of RAM allocated for the calculation in GB

        .. attribute:: new_hdf

            defines whether a subjob should be stored in the same HDF5 file or in a new one.

        .. attribute:: executor

            the executor can be used to execute the job object

        .. attribute:: future

            the concurrent.futures.Future object for monitoring the execution of the job object
    """

    def __init__(
        self,
        host: Optional[str] = None,
        queue: Optional[str] = None,
        cores: int = 1,
        threads: int = 1,
        gpus: Optional[int] = None,
        run_mode: str = "modal",
        new_hdf: bool = True,
        accept_crash: bool = False,
        run_time: Optional[int] = None,
        memory_limit: Optional[str] = None,
        qid: Optional[int] = None,
        additional_arguments: dict = {},
        conda_environment_name: Optional[str] = None,
        conda_environment_path: Optional[str] = None,
    ):
        super().__init__()
        self._data = ServerDataClass(
            user=state.settings.login_user,
            host=self._init_host(host=host),
            run_mode=run_mode,
            cores=cores,
            gpus=gpus,
            threads=threads,
            new_hdf=new_hdf,
            accept_crash=accept_crash,
            run_time=run_time,
            memory_limit=memory_limit,
            queue=queue,
            qid=qid,
            additional_arguments=additional_arguments,
            conda_environment_name=conda_environment_name,
            conda_environment_path=conda_environment_path,
        )
        self._run_mode = Runmode()
        self._executor: Union[Executor, None] = None
        self._future: Union[Future, None] = None

        self.queue = queue
        self.run_mode = run_mode

    @property
    def accept_crash(self) -> bool:
        return self._data.accept_crash

    @accept_crash.setter
    @sentinel
    def accept_crash(self, accept: bool) -> None:
        self._data.accept_crash = accept

    @property
    def additional_arguments(self) -> dict:
        return self._data.additional_arguments

    @additional_arguments.setter
    @sentinel
    def additional_arguments(self, additional_arguments: dict) -> None:
        self._data.additional_arguments = additional_arguments

    @property
    def queue(self) -> Union[str, None]:
        """
        The que selected for a current simulation

        Returns:
            (str): schedulers_name
        """
        return self._data.queue

    @queue.setter
    @sentinel
    def queue(self, new_scheduler: Union[str, None]) -> None:
        """
        Set a queue for the current simulation, by choosing one of the options
        listed in :attribute:`~.queue_list`.

        Args:
            new_scheduler (str/None): scheduler name, None resets to default
                                      run_mode modal
        """
        if new_scheduler is None:
            self._data.queue = None
            self.run_mode.modal = True
            self.cores = 1
            self.threads = 1
            self._data.run_time = None
            self.memory_limit = None
        else:
            if state.queue_adapter is not None:
                (
                    cores,
                    run_time_max,
                    memory_max,
                ) = state.queue_adapter.check_queue_parameters(
                    queue=new_scheduler,
                    cores=self.cores,
                    run_time_max=self.run_time,
                    memory_max=self.memory_limit,
                )
                if self.cores is not None and cores != self.cores:
                    self._data.cores = cores
                    state.logger.warning(f"Updated the number of cores to: {cores}")
                if self.run_time is not None and run_time_max != self.run_time:
                    self._data.run_time = run_time_max
                    state.logger.warning(
                        f"Updated the run time limit to: {run_time_max}"
                    )
                if self.memory_limit is not None and memory_max != self.memory_limit:
                    self._data.memory_limit = memory_max
                    state.logger.warning(f"Updated the memory limit to: {memory_max}")
                self._data.queue = new_scheduler
                self.run_mode = "queue"
            else:
                raise TypeError(
                    "No queue adapter defined. Have you "
                    "configured in $PYIRONRESOURCES_PATHS/queues?"
                )

    @property
    def queue_id(self) -> int:
        """
        Get the queue ID - the ID in the queuing system is most likely not the same as the job ID.

        Returns:
            int: queue ID
        """
        return self._data.qid

    @queue_id.setter
    @sentinel
    def queue_id(self, qid: int) -> None:
        """
        Set the queue ID

        Args:
            qid (int): queue ID
        """
        self._data.qid = int(qid)

    @property
    def threads(self) -> int:
        """
        The number of threads selected for the current simulation

        Returns:
            (int): number of threads
        """
        return self._data.threads

    @threads.setter
    @sentinel
    def threads(self, number_of_threads: int) -> None:
        """
        The number of threads selected for the current simulation

        Args:
            number_of_threads (int): number of threads
        """
        self._data.threads = number_of_threads

    @property
    def gpus(self) -> Union[int, None]:
        """
        Total number of GPUs to use for this calculation.

        Returns:
            int: Total number of GPUs to use for this calculation.
        """
        return self._data.gpus

    @gpus.setter
    @sentinel
    def gpus(self, number_of_gpus: int) -> None:
        """
        Total number of GPUs to use for this calculation.

        Args:
            number_of_gpus (int): Total number of GPUs to use for this calculation.
        """
        self._data.gpus = number_of_gpus

    @property
    def cores(self) -> int:
        """
        The number of cores selected for the current simulation

        Returns:
            (int): number of cores
        """
        return self._data.cores

    @cores.setter
    @sentinel
    def cores(self, new_cores: int) -> None:
        """
        The number of cores selected for the current simulation

        Args:
            new_cores (int): number of cores
        """
        if not isinstance(new_cores, numbers.Integral):
            converted = int(new_cores)
            # if the conversion truncated the number, raise error otherwise silently accept it
            if new_cores != converted:
                raise ValueError(f"cores must be an integer number, not {new_cores}!")
            new_cores = converted
        if state.queue_adapter is not None and self._data.queue is not None:
            cores = state.queue_adapter.check_queue_parameters(
                queue=self.queue,
                cores=new_cores,
                run_time_max=self.run_time,
                memory_max=self.memory_limit,
            )[0]
            if cores != new_cores:
                self._data.cores = cores
                state.logger.warning(f"Updated the number of cores to: {cores}")
            else:
                self._data.cores = new_cores
        else:
            self._data.cores = new_cores

    @property
    def run_time(self) -> int:
        """
        The run time in seconds selected for the current simulation

        Returns:
            (int): run time in seconds
        """
        return self._data.run_time

    @run_time.setter
    @sentinel
    def run_time(self, new_run_time: int) -> None:
        """
        The run time in seconds selected for the current simulation

        Args:
            new_run_time (int): run time in seconds

        Raises:
            ValueError: if new_run_time cannot be converted to int
        """
        new_run_time = int(new_run_time)
        if state.queue_adapter is not None and self._data.queue is not None:
            run_time_max = state.queue_adapter.check_queue_parameters(
                queue=self.queue,
                cores=self.cores,
                run_time_max=new_run_time,
                memory_max=self.memory_limit,
            )[1]
            if run_time_max != new_run_time:
                self._data.run_time = run_time_max
                state.logger.warning(f"Updated the run time limit to: {run_time_max}")
            else:
                self._data.run_time = new_run_time
        else:
            self._data.run_time = new_run_time

    @property
    def memory_limit(self) -> str:
        return self._data.memory_limit

    @memory_limit.setter
    @sentinel
    def memory_limit(self, limit: str) -> None:
        if state.queue_adapter is not None and self._data.queue is not None:
            memory_max = state.queue_adapter.check_queue_parameters(
                queue=self.queue,
                cores=self.cores,
                run_time_max=self.run_time,
                memory_max=limit,
            )[2]
            if memory_max != limit:
                self._data.memory_limit = memory_max
                state.logger.warning(f"Updated the memory limit to: {memory_max}")
            else:
                self._data.memory_limit = limit
        else:
            self._data.memory_limit = limit

    @property
    def run_mode(self) -> str:
        """
        Get the run mode of the job

        Returns:
            (str/pyiron_base.server.runmode.Runmode): ['modal', 'non_modal', 'queue', 'manual', 'thread', 'worker',
                        'interactive', 'interactive_non_modal', 'srun', 'executor']
        """
        return self._run_mode

    @run_mode.setter
    @sentinel
    def run_mode(self, new_mode: str) -> None:
        """
        Set the run mode of the job

        Args:
            new_mode (str): ['modal', 'non_modal', 'queue', 'manual', 'thread', 'worker', 'interactive',
                        'interactive_non_modal', 'srun', 'executor']
        """
        self._run_mode.mode = new_mode
        if new_mode == "queue":
            if state.queue_adapter is None:
                raise TypeError("No queue adapter defined.")
            if self._data.queue is None:
                self.queue = state.queue_adapter.config["queue_primary"]

    @property
    def new_hdf(self) -> bool:
        """
        New_hdf5 defines whether a subjob should be stored in the same HDF5 file or in a new one.

        Returns:
            (bool): [True / False]

        """
        return self._data.new_hdf

    @new_hdf.setter
    @sentinel
    def new_hdf(self, new_hdf_bool: bool) -> None:
        """
        New_hdf5 defines whether a subjob should be stored in the same HDF5 file or in a new one.

        Args:
            new_hdf_bool (bool): [True / False]
        """
        if isinstance(new_hdf_bool, bool):
            self._data.new_hdf = new_hdf_bool
        else:
            raise TypeError(
                "The new_hdf5 is a boolean property, defining whether subjobs are stored in the same file."
            )

    @property
    def queue_list(self) -> list:
        """
        List the available Job scheduler provided by the system.

        Returns:
            (list)
        """
        return self.list_queues()

    @property
    def queue_view(self) -> pandas.DataFrame:
        """
        List the available Job scheduler provided by the system.

        Returns:
            (pandas.DataFrame)
        """
        return self.view_queues()

    @property
    def executor(self) -> Union[Executor, None]:
        """
        Executor to execute the job object this server object is attached to in the background.

        Returns:
            concurrent.futures.Executor:
        """
        if not self.run_mode.executor and self._executor is not None:
            self._executor = None
        return self._executor

    @executor.setter
    @sentinel
    def executor(self, exe: Union[Executor, None]):
        """
        Executor to execute the job object this server object is attached to in the background.

        Args:
            exe (concurrent.futures.Executor):
        """
        if isinstance(exe, Executor):
            self.run_mode.executor = True
        elif static_isinstance(exe, "flux.job.executor.FluxExecutor"):
            self.run_mode.executor = True
        elif exe is None and self.run_mode.executor:
            self.run_mode.modal = True
        elif exe is not None:
            raise TypeError(
                "The executor has to be derived from the concurrent.futures.Executor class."
            )
        self._executor = exe

    @property
    def future(self) -> Union[Future, None]:
        """
        Python concurrent.futures.Future object to track the status of the execution of the job this server object is
        attached to. This is an internal pyiron feature and most users never have to interact with the future object
        directly.

        Returns:
             concurrent.futures.Future: future object to track the status of the execution
        """
        return self._future

    # We don't wrap future in sentinel, to allow it later to be dropped to
    # None, once execution is finished
    @future.setter
    def future(self, future_obj: Future) -> None:
        """
        Set a python concurrent.futures.Future object to track the status of the execution of the job this server object
        is attached to. This is an internal pyiron feature and most users never have to interact with the future object
        directly.

        Args:
            future_obj (concurrent.futures.Future): future object to track the status of the execution
        """
        self._future = future_obj

    @property
    def conda_environment_name(self) -> str:
        """
        Get name of the conda environment the job should be executed in.

        Returns:
            str: name of the conda environment
        """
        return self._data.conda_environment_name

    @conda_environment_name.setter
    @sentinel
    def conda_environment_name(self, environment_name: str) -> None:
        """
        Set name of the conda environment the job should be executed in.

        Args:
            environment_name (str): name of the conda environment
        """
        self._data.conda_environment_name = environment_name

    @property
    def conda_environment_path(self) -> str:
        """
        Get path of the conda environment the job should be executed in.

        Returns:
            str: path of the conda environment
        """
        return self._data.conda_environment_path

    @conda_environment_path.setter
    @sentinel
    def conda_environment_path(self, environment_path: str) -> None:
        """
        Set path of the conda environment the job should be executed in.

        Args:
            environment_path (str): path of the conda environment
        """
        self._data.conda_environment_path = environment_path

    @staticmethod
    def list_queues() -> list:
        """
        List the available Job scheduler provided by the system.

        Returns:
            (list)
        """
        if state.queue_adapter is not None:
            return state.queue_adapter.queue_list
        else:
            return None

    @staticmethod
    def view_queues() -> pandas.DataFrame:
        """
        List the available Job scheduler provided by the system.

        Returns:
            (pandas.DataFrame)
        """
        if state.queue_adapter is not None:
            return state.queue_adapter.queue_view
        else:
            return None

    def _to_dict(self) -> dict:
        """
        Convert the Server object to a dictionary.

        Returns:
            dict: The Server object as a dictionary.
        """
        self._data.run_mode = self._run_mode.mode
        return asdict(self._data)

    def _from_dict(self, obj_dict: dict, version: Optional[str] = None) -> None:
        """
        Load the Server object from a dictionary.

        Args:
            obj_dict (dict): The dictionary containing the Server object data.
            version (str, optional): The version of the Server object. Defaults to None.
        """
        # backwards compatibility
        if "new_h5" in obj_dict.keys():
            obj_dict["new_hdf"] = obj_dict.pop("new_h5") == 1
        for key in ["conda_environment_name", "conda_environment_path", "qid"]:
            if key not in obj_dict.keys():
                obj_dict[key] = None
        if "accept_crash" not in obj_dict.keys():
            obj_dict["accept_crash"] = False
        if "additional_arguments" not in obj_dict.keys():
            obj_dict["additional_arguments"] = {}

        # Reload dataclass and discard unknown keys
        server_fields = tuple(f.name for f in fields(ServerDataClass))
        # force tuple otherwise dict complains about changing size
        for key in tuple(obj_dict):
            if key not in server_fields:
                del obj_dict[key]
        self._data = ServerDataClass(**obj_dict)
        self._run_mode = Runmode(mode=self._data.run_mode)

    @deprecate(message="Use job.server.to_dict() instead of to_hdf()", version=0.9)
    def to_hdf(
        self,
        hdf: "pyiron_base.storage.hdfio.ProjectHDFio",
        group_name: Optional[str] = None,
    ) -> None:
        """
        Store Server object in HDF5 file
        Args:
            hdf: HDF5 object
            group_name (str): node name in the HDF5 file
        """
        if group_name is not None:
            with hdf.open(group_name) as hdf_group:
                hdf_group["server"] = self.to_dict()
        else:
            hdf["server"] = self.to_dict()

    @deprecate(message="Use job.server.from_dict() instead of from_hdf()", version=0.9)
    def from_hdf(
        self,
        hdf: "pyiron_base.storage.hdfio.ProjectHDFio",
        group_name: Optional[str] = None,
    ) -> None:
        """
        Recover Server object in HDF5 file
        Args:
            hdf: HDF5 object
            group_name: node name in the HDF5 file
        """
        if group_name is not None:
            with hdf.open(group_name) as hdf_group:
                self.from_dict(obj_dict=hdf_group["server"])
        else:
            self.from_dict(obj_dict=hdf["server"])

    def db_entry(self) -> str:
        """
        connect all the info regarding the server into a single word that can be used e.g. as entry in a database

        Returns:
            (str): server info as single word

        """
        if self.run_mode.queue:
            server_lst = [self._data.host, str(self.cores), self.queue]
        else:
            server_lst = [self._data.host, str(self.cores)]
        return self._data.user + "@" + "#".join(server_lst)

    def __del__(self) -> None:
        """
        Delete the Server object from memory
        """
        del self._run_mode
        del self._data

    @staticmethod
    def _init_host(host) -> Union[str, None]:
        if host is None:
            return socket.gethostname()
        else:
            return host
