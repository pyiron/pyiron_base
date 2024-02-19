# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Server object class which is connected to each job containing the technical details how the job is executed.
"""

from concurrent.futures import Executor, Future
from collections import OrderedDict
import numbers
import socket
from typing import Union

from pyiron_base.state import state
from pyiron_base.interfaces.lockable import Lockable, sentinel
from pyiron_base.utils.instance import static_isinstance
from pyiron_base.jobs.job.extension.server.runmode import Runmode

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
    Lockable
):  # add the option to return the job id and the hold id to the server object
    """
    Generic Server object to handle the execution environment for the job

    Args:
        host (str): hostname of the local machine
        queue (str): queue name of the currently selected queue
        cores (int): number of cores
        run_mode (pyiron_base.server.runmode.Runmode): mode of the job ['modal', 'non_modal', 'queue', 'manual']
        new_hdf (bool): create a new HDF5 file [True/False] - default=True

    Attributes:

        .. attribute:: send_to_db

            boolean option to decide which jobs should be store in the external/public database.

        .. attribute:: structure_id

            the structure ID to be linked to an external/public database.

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
        host=None,
        queue=None,
        cores=1,
        threads=1,
        gpus=None,
        run_mode="modal",
        new_hdf=True,
    ):
        super().__init__()
        self._cores = cores
        self._threads = threads
        self._active_queue = None
        self._gpus = gpus
        self._run_time = None
        self._memory_limit = None
        self._host = self._init_host(host=host)
        self._run_mode = Runmode()
        self._executor: Union[Executor, None] = None
        self._future: Union[Future, None] = None

        self.queue = queue

        self._user = state.settings.login_user
        self.run_mode = run_mode

        self._queue_id = None

        self._new_hdf = new_hdf
        self._send_to_db = False
        self._structure_id = None
        self._accept_crash = False
        self._environment_name = None
        self._environment_path = None
        self.additional_arguments = {}

    @property
    def send_to_db(self):
        """
        Get the boolean option to decide which jobs should be store in the external/public database

        Returns:
            bool: [True/False]
        """
        return self._send_to_db

    @send_to_db.setter
    @sentinel
    def send_to_db(self, send):
        """
        Set the boolean option to decide which jobs should be store in the external/public database

        Args:
            send (bool): [True/False]
        """
        self._send_to_db = send

    @property
    def accept_crash(self):
        return self._accept_crash

    @accept_crash.setter
    @sentinel
    def accept_crash(self, accept):
        self._accept_crash = accept

    @property
    def structure_id(self):
        """
        Get the structure ID to be linked to an external/public database

        Returns:
            int: structure ID
        """
        return self._structure_id

    @structure_id.setter
    @sentinel
    def structure_id(self, structure_id):
        """
        Set the structure ID to be linked to an external/public database

        Args:
            structure_id (int): structure ID
        """
        self._structure_id = structure_id

    @property
    def queue(self):
        """
        The que selected for a current simulation

        Returns:
            (str): schedulers_name
        """
        return self._active_queue

    @queue.setter
    @sentinel
    def queue(self, new_scheduler):
        """
        Set a queue for the current simulation, by choosing one of the options
        listed in :attribute:`~.queue_list`.

        Args:
            new_scheduler (str/None): scheduler name, None resets to default
                                      run_mode modal
        """
        if new_scheduler is None:
            self._active_queue = None
            self.run_mode.modal = True
            self.cores = 1
            self.threads = 1
            self._run_time = None
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
                    self._cores = cores
                    state.logger.warning(f"Updated the number of cores to: {cores}")
                if self.run_time is not None and run_time_max != self.run_time:
                    self._run_time = run_time_max
                    state.logger.warning(
                        f"Updated the run time limit to: {run_time_max}"
                    )
                if self.memory_limit is not None and memory_max != self.memory_limit:
                    self._memory_limit = memory_max
                    state.logger.warning(f"Updated the memory limit to: {memory_max}")
                self._active_queue = new_scheduler
                self.run_mode = "queue"
            else:
                raise TypeError(
                    "No queue adapter defined. Have you "
                    "configured in $PYIRONRESOURCES_PATHS/queues?"
                )

    @property
    def queue_id(self):
        """
        Get the queue ID - the ID in the queuing system is most likely not the same as the job ID.

        Returns:
            int: queue ID
        """
        return self._queue_id

    @queue_id.setter
    @sentinel
    def queue_id(self, qid):
        """
        Set the queue ID

        Args:
            qid (int): queue ID
        """
        self._queue_id = int(qid)

    @property
    def threads(self):
        """
        The number of threads selected for the current simulation

        Returns:
            (int): number of threads
        """
        return self._threads

    @threads.setter
    @sentinel
    def threads(self, number_of_threads):
        """
        The number of threads selected for the current simulation

        Args:
            number_of_threads (int): number of threads
        """
        self._threads = number_of_threads

    @property
    def gpus(self):
        """
        Total number of GPUs to use for this calculation.

        Returns:
            int: Total number of GPUs to use for this calculation.
        """
        return self._gpus

    @gpus.setter
    @sentinel
    def gpus(self, number_of_gpus):
        """
        Total number of GPUs to use for this calculation.

        Args:
            number_of_gpus (int): Total number of GPUs to use for this calculation.
        """
        self._gpus = number_of_gpus

    @property
    def cores(self):
        """
        The number of cores selected for the current simulation

        Returns:
            (int): number of cores
        """
        return self._cores

    @cores.setter
    @sentinel
    def cores(self, new_cores):
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
        if state.queue_adapter is not None and self._active_queue is not None:
            cores = state.queue_adapter.check_queue_parameters(
                queue=self.queue,
                cores=new_cores,
                run_time_max=self.run_time,
                memory_max=self.memory_limit,
            )[0]
            if cores != new_cores:
                self._cores = cores
                state.logger.warning(f"Updated the number of cores to: {cores}")
            else:
                self._cores = new_cores
        else:
            self._cores = new_cores

    @property
    def run_time(self):
        """
        The run time in seconds selected for the current simulation

        Returns:
            (int): run time in seconds
        """
        return self._run_time

    @run_time.setter
    @sentinel
    def run_time(self, new_run_time):
        """
        The run time in seconds selected for the current simulation

        Args:
            new_run_time (int): run time in seconds

        Raises:
            ValueError: if new_run_time cannot be converted to int
        """
        new_run_time = int(new_run_time)
        if state.queue_adapter is not None and self._active_queue is not None:
            run_time_max = state.queue_adapter.check_queue_parameters(
                queue=self.queue,
                cores=self.cores,
                run_time_max=new_run_time,
                memory_max=self.memory_limit,
            )[1]
            if run_time_max != new_run_time:
                self._run_time = run_time_max
                state.logger.warning(f"Updated the run time limit to: {run_time_max}")
            else:
                self._run_time = new_run_time
        else:
            self._run_time = new_run_time

    @property
    def memory_limit(self):
        return self._memory_limit

    @memory_limit.setter
    @sentinel
    def memory_limit(self, limit):
        if state.queue_adapter is not None and self._active_queue is not None:
            memory_max = state.queue_adapter.check_queue_parameters(
                queue=self.queue,
                cores=self.cores,
                run_time_max=self.run_time,
                memory_max=limit,
            )[2]
            if memory_max != limit:
                self._memory_limit = memory_max
                state.logger.warning(f"Updated the memory limit to: {memory_max}")
            else:
                self._memory_limit = limit
        else:
            self._memory_limit = limit

    @property
    def run_mode(self):
        """
        Get the run mode of the job

        Returns:
            (str/pyiron_base.server.runmode.Runmode): ['modal', 'non_modal', 'queue', 'manual', 'thread', 'worker',
                        'interactive', 'interactive_non_modal', 'srun', 'executor']
        """
        return self._run_mode

    @run_mode.setter
    @sentinel
    def run_mode(self, new_mode):
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
            if self._active_queue is None:
                self.queue = state.queue_adapter.config["queue_primary"]

    @property
    def new_hdf(self):
        """
        New_hdf5 defines whether a subjob should be stored in the same HDF5 file or in a new one.

        Returns:
            (bool): [True / False]

        """
        return self._new_hdf

    @new_hdf.setter
    @sentinel
    def new_hdf(self, new_hdf_bool):
        """
        New_hdf5 defines whether a subjob should be stored in the same HDF5 file or in a new one.

        Args:
            new_hdf_bool (bool): [True / False]
        """
        if isinstance(new_hdf_bool, bool):
            self._new_hdf = new_hdf_bool
        else:
            raise TypeError(
                "The new_hdf5 is a boolean property, defining whether subjobs are stored in the same file."
            )

    @property
    def queue_list(self):
        """
        List the available Job scheduler provided by the system.

        Returns:
            (list)
        """
        return self.list_queues()

    @property
    def queue_view(self):
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
    def future(self, future_obj: Future):
        """
        Set a python concurrent.futures.Future object to track the status of the execution of the job this server object
        is attached to. This is an internal pyiron feature and most users never have to interact with the future object
        directly.

        Args:
            future_obj (concurrent.futures.Future): future object to track the status of the execution
        """
        self._future = future_obj

    @property
    def conda_environment_name(self):
        """
        Get name of the conda environment the job should be executed in.

        Returns:
            str: name of the conda environment
        """
        return self._environment_name

    @conda_environment_name.setter
    @sentinel
    def conda_environment_name(self, environment_name):
        """
        Set name of the conda environment the job should be executed in.

        Args:
            environment_name (str): name of the conda environment
        """
        self._environment_name = environment_name

    @property
    def conda_environment_path(self):
        """
        Get path of the conda environment the job should be executed in.

        Returns:
            str: path of the conda environment
        """
        return self._environment_path

    @conda_environment_path.setter
    @sentinel
    def conda_environment_path(self, environment_path):
        """
        Set path of the conda environment the job should be executed in.

        Args:
            environment_path (str): path of the conda environment
        """
        self._environment_path = environment_path

    @staticmethod
    def list_queues():
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
    def view_queues():
        """
        List the available Job scheduler provided by the system.

        Returns:
            (pandas.DataFrame)
        """
        if state.queue_adapter is not None:
            return state.queue_adapter.queue_view
        else:
            return None

    def to_dict(self):
        server_dict = OrderedDict(
            {
                "user": self._user,
                "host": self._host,
                "run_mode": self.run_mode.mode,
                "queue": self.queue,
                "qid": self._queue_id,
                "cores": self.cores,
                "threads": self.threads,
                "new_h5": self.new_hdf,
                "structure_id": self.structure_id,
                "run_time": self.run_time,
                "memory_limit": self.memory_limit,
                "accept_crash": self.accept_crash,
            }
        )
        if len(self.additional_arguments) > 0:
            server_dict["additional_arguments"] = self.additional_arguments
        if self._gpus is not None:
            server_dict["gpus"] = self._gpus
        if self._environment_name is not None:
            server_dict["conda_environment_name"] = self._environment_name
        if self._environment_path is not None:
            server_dict["conda_environment_path"] = self._environment_path
        return server_dict

    def from_dict(self, server_dict):
        self._user = server_dict["user"]
        self._host = server_dict["host"]
        self._run_mode.mode = server_dict["run_mode"]
        if self.run_mode.queue:
            self._active_queue = server_dict["queue"]
            if "qid" in server_dict.keys():
                self._queue_id = server_dict["qid"]
            else:
                self._queue_id = None
        self._cores = server_dict["cores"]
        h5_mapping = {
            "structure_id": "_structure_id",
            "run_time": "_run_time",
            "memory_limit": "_memory_limit",
            "threads": "_threads",
            "additional_arguments": "additional_arguments",
            "gpus": "_gpus",
            "conda_environment_name": "_environment_name",
            "conda_environment_path": "_environment_path",
        }
        for h5_key, obj_attr in h5_mapping.items():
            if h5_key in server_dict.keys():
                setattr(self, obj_attr, server_dict[h5_key])

        if "accept_crash" in server_dict.keys():
            self._accept_crash = server_dict["accept_crash"] == 1
        self._new_hdf = server_dict["new_h5"] == 1

    def to_hdf(self, hdf, group_name=None):
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

    def from_hdf(self, hdf, group_name=None):
        """
        Recover Server object in HDF5 file

        Args:
            hdf: HDF5 object
            group_name: node name in the HDF5 file

        """
        if group_name is not None:
            with hdf.open(group_name) as hdf_group:
                self.from_dict(server_dict=hdf_group["server"])
        else:
            self.from_dict(server_dict=hdf["server"])

    def db_entry(self):
        """
        connect all the info regarding the server into a single word that can be used e.g. as entry in a database

        Returns:
            (str): server info as single word

        """
        if self.run_mode.queue:
            server_lst = [self._host, str(self.cores), self.queue]
        else:
            server_lst = [self._host, str(self.cores)]
        return self._user + "@" + "#".join(server_lst)

    def __del__(self):
        """
        Delete the Server object from memory
        """
        del self._cores
        del self._threads
        del self._run_time
        del self._memory_limit
        del self._host
        del self._active_queue
        del self._user
        del self._run_mode
        del self._queue_id
        del self._new_hdf
        del self._send_to_db
        del self._structure_id
        del self._accept_crash

    @staticmethod
    def _init_host(host):
        if host is None:
            return socket.gethostname()
        else:
            return host
