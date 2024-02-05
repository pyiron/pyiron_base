# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Generic Job class extends the JobCore class with all the functionality to run the job object.
"""

from concurrent.futures import Future
from datetime import datetime
import os
import posixpath
import signal
import warnings

from h5io_browser.base import _read_hdf, _write_hdf

from pyiron_base.state import state
from pyiron_base.state.signal import catch_signals
from pyiron_base.jobs.job.extension.executable import Executable
from pyiron_base.jobs.job.extension.jobstatus import JobStatus
from pyiron_base.jobs.job.core import (
    JobCore,
    _doc_str_job_core_args,
    _doc_str_job_core_attr,
)
from pyiron_base.jobs.job.runfunction import (
    run_job_with_parameter_repair,
    run_job_with_status_initialized,
    run_job_with_status_created,
    run_job_with_status_submitted,
    run_job_with_status_running,
    run_job_with_status_refresh,
    run_job_with_status_busy,
    run_job_with_status_collect,
    run_job_with_status_suspended,
    run_job_with_status_finished,
    run_job_with_runmode_modal,
    run_job_with_runmode_queue,
    execute_job_with_external_executable,
)
from pyiron_base.jobs.job.util import (
    _copy_restart_files,
    _kill_child,
    _job_store_before_copy,
    _job_reload_after_copy,
)
from pyiron_base.utils.instance import static_isinstance
from pyiron_base.utils.deprecate import deprecate
from pyiron_base.jobs.job.extension.server.generic import Server
from pyiron_base.database.filetable import FileTable
from pyiron_base.interfaces.has_dict import HasDict

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
_doc_str_generic_job_attr = (
    _doc_str_job_core_attr
    + "\n"
    + """\
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

        .. attribute:: exclude_nodes_hdf

            list of nodes which are excluded from storing in the hdf5 file.

        .. attribute:: exclude_groups_hdf

            list of groups which are excluded from storing in the hdf5 file.

        .. attribute:: job_type

            Job type object with all the available job types: ['ExampleJob', 'SerialMaster', 'ParallelMaster',
                                                               'ScriptJob', 'ListMaster']
"""
)


class GenericJob(JobCore, HasDict):
    __doc__ = (
        """
    Generic Job class extends the JobCore class with all the functionality to run the job object. From this class
    all specific job types are derived. Therefore it should contain the properties/routines common to all jobs.
    The functions in this module should be as generic as possible.

    Sub classes that need to add special behavior after :method:`.copy_to()` can override
    :method:`._after_generic_copy_to()`.
"""
        + "\n"
        + _doc_str_job_core_args
        + "\n"
        + _doc_str_generic_job_attr
    )

    def __init__(self, project, job_name):
        super(GenericJob, self).__init__(project, job_name)
        self.__name__ = type(self).__name__
        self.__version__ = "0.4"
        self.__hdf_version__ = "0.1.0"
        self._server = Server()
        self._logger = state.logger
        self._executable = None
        if not state.database.database_is_disabled:
            self._status = JobStatus(db=project.db, job_id=self.job_id)
            self.refresh_job_status()
        elif os.path.exists(self.project_hdf5.file_name):
            initial_status = _read_hdf(
                # in most cases self.project_hdf5.h5_path == / + self.job_name but not for child jobs of GenericMasters
                self.project_hdf5.file_name,
                self.project_hdf5.h5_path + "/status",
            )
            self._status = JobStatus(initial_status=initial_status)
            if "job_id" in self.list_nodes():
                self._job_id = _read_hdf(
                    # in most cases self.project_hdf5.h5_path == / + self.job_name but not for child jobs of GenericMasters
                    self.project_hdf5.file_name,
                    self.project_hdf5.h5_path + "/job_id",
                )
        else:
            self._status = JobStatus()
        self._restart_file_list = list()
        self._restart_file_dict = dict()
        self._exclude_nodes_hdf = list()
        self._exclude_groups_hdf = list()
        self._process = None
        self._compress_by_default = False
        self._python_only_job = False
        self._write_work_dir_warnings = True
        self.interactive_cache = None
        self.error = GenericError(job=self)

    @property
    def version(self):
        """
        Get the version of the hamiltonian, which is also the version of the executable unless a custom executable is
        used.

        Returns:
            str: version number
        """
        if self.__version__:
            return self.__version__
        else:
            self._executable_activate()
            if self._executable is not None:
                return self._executable.version
            else:
                return None

    @version.setter
    def version(self, new_version):
        """
        Set the version of the hamiltonian, which is also the version of the executable unless a custom executable is
        used.

        Args:
            new_version (str): version
        """
        self._executable_activate()
        self._executable.version = new_version

    @property
    def executable(self):
        """
        Get the executable used to run the job - usually the path to an external executable.

        Returns:
            (str/pyiron_base.job.executable.Executable): exectuable path
        """
        self._executable_activate()
        return self._executable

    @executable.setter
    def executable(self, exe):
        """
        Set the executable used to run the job - usually the path to an external executable.

        Args:
            exe (str): executable path, if no valid path is provided an executable is chosen based on version.
        """
        self._executable_activate()
        self._executable.executable_path = exe

    @property
    def server(self):
        """
        Get the server object to handle the execution environment for the job.

        Returns:
            Server: server object
        """
        return self._server

    @server.setter
    def server(self, server):
        """
        Set the server object to handle the execution environment for the job.
        Args:
            server (Server): server object
        """
        self._server = server

    @property
    def queue_id(self):
        """
        Get the queue ID, the ID returned from the queuing system - it is most likely not the same as the job ID.

        Returns:
            int: queue ID
        """
        return self.server.queue_id

    @queue_id.setter
    def queue_id(self, qid):
        """
        Set the queue ID, the ID returned from the queuing system - it is most likely not the same as the job ID.

        Args:
            qid (int): queue ID
        """
        self.server.queue_id = qid

    @property
    def logger(self):
        """
        Get the logger object to monitor the external execution and internal pyiron warnings.

        Returns:
            logging.getLogger(): logger object
        """
        return self._logger

    @property
    def restart_file_list(self):
        """
        Get the list of files which are used to restart the calculation from these files.

        Returns:
            list: list of files
        """
        return self._restart_file_list

    @restart_file_list.setter
    def restart_file_list(self, filenames):
        """
        Append new files to the restart file list - the list of files which are used to restart the calculation from.

        Args:
            filenames (list):
        """
        for f in filenames:
            if not (os.path.isfile(f)):
                raise IOError("File: {} does not exist".format(f))
            self.restart_file_list.append(f)

    @property
    def restart_file_dict(self):
        """
        A dictionary of the new name of the copied restart files
        """
        for actual_name in [os.path.basename(f) for f in self._restart_file_list]:
            if actual_name not in self._restart_file_dict.keys():
                self._restart_file_dict[actual_name] = actual_name
        return self._restart_file_dict

    @restart_file_dict.setter
    def restart_file_dict(self, val):
        if not isinstance(val, dict):
            raise ValueError("restart_file_dict should be a dictionary!")
        else:
            self._restart_file_dict = val

    @property
    def exclude_nodes_hdf(self):
        """
        Get the list of nodes which are excluded from storing in the hdf5 file

        Returns:
            nodes(list)
        """
        return self._exclude_nodes_hdf

    @exclude_nodes_hdf.setter
    def exclude_nodes_hdf(self, val):
        if isinstance(val, str):
            val = [val]
        elif not hasattr(val, "__len__"):
            raise ValueError("Wrong type of variable.")
        self._exclude_nodes_hdf = val

    @property
    def exclude_groups_hdf(self):
        """
        Get the list of groups which are excluded from storing in the hdf5 file

        Returns:
            groups(list)
        """
        return self._exclude_groups_hdf

    @exclude_groups_hdf.setter
    def exclude_groups_hdf(self, val):
        if isinstance(val, str):
            val = [val]
        elif not hasattr(val, "__len__"):
            raise ValueError("Wrong type of variable.")
        self._exclude_groups_hdf = val

    @property
    def job_type(self):
        """
        Job type object with all the available job types: ['ExampleJob', 'SerialMaster', 'ParallelMaster', 'ScriptJob',
                                                           'ListMaster']
        Returns:
            JobTypeChoice: Job type object
        """
        return self.project.job_type

    @property
    def working_directory(self):
        """
        Get the working directory of the job is executed in - outside the HDF5 file. The working directory equals the
        path but it is represented by the filesystem:
            /absolute/path/to/the/file.h5/path/inside/the/hdf5/file
        becomes:
            /absolute/path/to/the/file_hdf5/path/inside/the/hdf5/file

        Returns:
            str: absolute path to the working directory
        """
        if self._import_directory is not None:
            return self._import_directory
        elif not self.project_hdf5.working_directory:
            self._create_working_directory()
        return self.project_hdf5.working_directory

    def collect_logfiles(self):
        """
        Collect the log files of the external executable and store the information in the HDF5 file. This method has
        to be implemented in the individual hamiltonians.
        """
        pass

    def write_input(self):
        """
        Write the input files for the external executable. This method has to be implemented in the individual
        hamiltonians.
        """
        if (
            state.settings.configuration["write_work_dir_warnings"]
            and self._write_work_dir_warnings
            and not self._python_only_job
        ):
            with open(
                os.path.join(self.working_directory, "WARNING_pyiron_modified_content"),
                "w",
            ) as f:
                f.write(
                    "Files in this directory are intended to be written and read by pyiron. \n\n"
                    "pyiron may transform user input to enhance performance, thus, use these files with care!\n"
                    "Consult the log and/or the documentation to gain further information.\n\n"
                    "To disable writing these warning files, specify \n"
                    "WRITE_WORK_DIR_WARNINGS=False in the .pyiron configuration file (or set the "
                    "PYIRONWRITEWORKDIRWARNINGS environment variable accordingly)."
                )

    def collect_output(self):
        """
        Collect the output files of the external executable and store the information in the HDF5 file. This method has
        to be implemented in the individual hamiltonians.
        """
        raise NotImplementedError(
            "read procedure must be defined for derived Hamilton!"
        )

    def suspend(self):
        """
        Suspend the job by storing the object and its state persistently in HDF5 file and exit it.
        """
        self.to_hdf()
        self.status.suspended = True
        self._logger.info(
            "{}, status: {}, job has been suspended".format(
                self.job_info_str, self.status
            )
        )
        self.clear_job()

    def refresh_job_status(self):
        """
        Refresh job status by updating the job status with the status from the database if a job ID is available.
        """
        if self.job_id:
            self._status = JobStatus(
                initial_status=self.project.db.get_job_status(self.job_id),
                db=self.project.db,
                job_id=self.job_id,
            )
        elif state.database.database_is_disabled:
            self._status = JobStatus(
                initial_status=_read_hdf(
                    self.project_hdf5.file_name, self.job_name + "/status"
                )
            )
        if (
            isinstance(self.server.future, Future)
            and not self.status.finished
            and self.server.future.done()
        ):
            if self.server.future.cancelled():
                self.status.aborted = True
            else:
                self.status.finished = True

    def clear_job(self):
        """
        Convenience function to clear job info after suspend. Mimics deletion of all the job info after suspend in a
        local test environment.
        """
        del self.__name__
        del self.__version__
        del self._executable
        del self._server
        del self._logger
        del self._import_directory
        del self._status
        del self._restart_file_list
        del self._restart_file_dict

    def copy(self):
        """
        Copy the GenericJob object which links to the job and its HDF5 file

        Returns:
            GenericJob: New GenericJob object pointing to the same job
        """
        # Store all job arguments in the HDF5 file
        delete_file_after_copy = _job_store_before_copy(job=self)

        # Copy Python object - super().copy() causes recursion error for serial master
        copied_self = self.__class__(
            job_name=self.job_name, project=self.project_hdf5.open("..")
        )
        copied_self.reset_job_id()

        # Reload object from HDF5 file
        _job_reload_after_copy(
            job=copied_self, delete_file_after_copy=delete_file_after_copy
        )

        # Copy executor - it cannot be copied and is just linked instead
        if self.server.executor is not None:
            copied_self.server.executor = self.server.executor
        if self.server.future is not None and not self.server.future.done():
            raise RuntimeError(
                "Jobs whose server has executor and future attributes cannot be copied unless the future is `done()`"
            )
        return copied_self

    def _internal_copy_to(
        self,
        project=None,
        new_job_name=None,
        new_database_entry=True,
        copy_files=True,
        delete_existing_job=False,
    ):
        # Store all job arguments in the HDF5 file
        delete_file_after_copy = _job_store_before_copy(job=self)

        # Call the copy_to() function defined in the JobCore
        new_job_core, file_project, hdf5_project, reloaded = super(
            GenericJob, self
        )._internal_copy_to(
            project=project,
            new_job_name=new_job_name,
            new_database_entry=new_database_entry,
            copy_files=copy_files,
            delete_existing_job=delete_existing_job,
        )
        if reloaded:
            return new_job_core, file_project, hdf5_project, reloaded

        # Reload object from HDF5 file
        if not static_isinstance(
            obj=project.__class__, obj_type="pyiron_base.jobs.job.core.JobCore"
        ):
            _job_reload_after_copy(
                job=new_job_core, delete_file_after_copy=delete_file_after_copy
            )
        if delete_file_after_copy:
            self.project_hdf5.remove_file()
        return new_job_core, file_project, hdf5_project, reloaded

    def copy_to(
        self,
        project=None,
        new_job_name=None,
        input_only=False,
        new_database_entry=True,
        delete_existing_job=False,
        copy_files=True,
    ):
        """
        Copy the content of the job including the HDF5 file to a new location.

        Args:
            project (JobCore/ProjectHDFio/Project/None): The project to copy the job to.
                (Default is None, use the same project.)
            new_job_name (str): The new name to assign the duplicate job. Required if the project is `None` or the same
                project as the copied job. (Default is None, try to keep the same name.)
            input_only (bool): [True/False] Whether to copy only the input. (Default is False.)
            new_database_entry (bool): [True/False] Whether to create a new database entry. If input_only is True then
                new_database_entry is False. (Default is True.)
            delete_existing_job (bool): [True/False] Delete existing job in case it exists already (Default is False.)
            copy_files (bool): If True copy all files the working directory of the job, too

        Returns:
            GenericJob: GenericJob object pointing to the new location.
        """
        # Update flags
        if input_only and new_database_entry:
            warnings.warn(
                "input_only conflicts new_database_entry; setting new_database_entry=False"
            )
            new_database_entry = False

        # Call the copy_to() function defined in the JobCore
        new_job_core, file_project, hdf5_project, reloaded = self._internal_copy_to(
            project=project,
            new_job_name=new_job_name,
            new_database_entry=new_database_entry,
            copy_files=copy_files,
            delete_existing_job=delete_existing_job,
        )

        # Remove output if it should not be copied
        if input_only:
            for group in new_job_core.project_hdf5.list_groups():
                if "output" in group:
                    del new_job_core.project_hdf5[
                        posixpath.join(new_job_core.project_hdf5.h5_path, group)
                    ]
            new_job_core.status.initialized = True
        new_job_core._after_generic_copy_to(
            self, new_database_entry=new_database_entry, reloaded=reloaded
        )
        return new_job_core

    def _after_generic_copy_to(self, original, new_database_entry, reloaded):
        """
        Called in :method:`.copy_to()` after :method`._internal_copy_to()` to allow sub classes to modify copy behavior.

        Args:
            original (:class:`.GenericJob`): job that this job was copied from
            new_database_entry (bool): Whether to create a new database entry was created.
            reloaded (bool): True if this job was reloaded instead of copied.
        """
        pass

    def copy_file_to_working_directory(self, file):
        """
        Copy a specific file to the working directory before the job is executed.

        Args:
            file (str): path of the file to be copied.
        """
        if os.path.isabs(file):
            self.restart_file_list.append(file)
        else:
            self.restart_file_list.append(os.path.abspath(file))

    def copy_template(self, project=None, new_job_name=None):
        """
        Copy the content of the job including the HDF5 file but without the output data to a new location

        Args:
            project (JobCore/ProjectHDFio/Project/None): The project to copy the job to.
                (Default is None, use the same project.)
            new_job_name (str): The new name to assign the duplicate job. Required if the project is `None` or the same
                project as the copied job. (Default is None, try to keep the same name.)

        Returns:
            GenericJob: GenericJob object pointing to the new location.
        """
        return self.copy_to(
            project=project,
            new_job_name=new_job_name,
            input_only=True,
            new_database_entry=False,
        )

    def remove(self, _protect_childs=True):
        """
        Remove the job - this removes the HDF5 file, all data stored in the HDF5 file an the corresponding database entry.

        Args:
            _protect_childs (bool): [True/False] by default child jobs can not be deleted, to maintain the consistency
                                    - default=True
        """
        if isinstance(self.server.future, Future) and not self.server.future.done():
            self.server.future.cancel()
        super().remove(_protect_childs=_protect_childs)

    def remove_child(self):
        """
        internal function to remove command that removes also child jobs.
        Do never use this command, since it will destroy the integrity of your project.
        """
        _kill_child(job=self)
        super(GenericJob, self).remove_child()

    def remove_and_reset_id(self, _protect_childs=True):
        if self.job_id is not None:
            master_id, parent_id = self.master_id, self.parent_id
            self.remove(_protect_childs=_protect_childs)
            self.reset_job_id()
            self.master_id, self.parent_id = master_id, parent_id
        else:
            self.remove(_protect_childs=_protect_childs)

    def kill(self):
        if self.status.running or self.status.submitted:
            self.remove_and_reset_id()
        else:
            raise ValueError(
                "The kill() function is only available during the execution of the job."
            )

    def validate_ready_to_run(self):
        """
        Validate that the calculation is ready to be executed. By default no generic checks are performed, but one could
        check that the input information is complete or validate the consistency of the input at this point.

        Raises:
            ValueError: if ready check is unsuccessful
        """
        pass

    def check_setup(self):
        """
        Checks whether certain parameters (such as plane wave cutoff radius in DFT) are changed from the pyiron standard
        values to allow for a physically meaningful results. This function is called manually or only when the job is
        submitted to the queueing system.
        """
        pass

    def reset_job_id(self, job_id=None):
        """
        Reset the job id sets the job_id to None in the GenericJob as well as all connected modules like JobStatus.
        """
        super().reset_job_id(job_id=job_id)
        self._status = JobStatus(db=self.project.db, job_id=self._job_id)

    @deprecate(
        run_again="Either delete the job via job.remove() or use delete_existing_job=True.",
        version="0.4.0",
    )
    def run(
        self,
        delete_existing_job=False,
        repair=False,
        debug=False,
        run_mode=None,
        run_again=False,
    ):
        """
        This is the main run function, depending on the job status ['initialized', 'created', 'submitted', 'running',
        'collect','finished', 'refresh', 'suspended'] the corresponding run mode is chosen.

        Args:
            delete_existing_job (bool): Delete the existing job and run the simulation again.
            repair (bool): Set the job status to created and run the simulation again.
            debug (bool): Debug Mode - defines the log level of the subprocess the job is executed in.
            run_mode (str): ['modal', 'non_modal', 'queue', 'manual'] overwrites self.server.run_mode
            run_again (bool): Same as delete_existing_job (deprecated)
        """
        with catch_signals(self.signal_intercept):
            if run_again:
                delete_existing_job = True
            try:
                self._logger.info(
                    "run {}, status: {}".format(self.job_info_str, self.status)
                )
                status = self.status.string
                if run_mode is not None:
                    self.server.run_mode = run_mode
                if delete_existing_job:
                    status = "initialized"
                    self.remove_and_reset_id(_protect_childs=False)
                if repair and self.job_id and not self.status.finished:
                    self._run_if_repair()
                elif status == "initialized":
                    self._run_if_new(debug=debug)
                elif status == "created":
                    self._run_if_created()
                elif status == "submitted":
                    run_job_with_status_submitted(job=self)
                elif status == "running":
                    self._run_if_running()
                elif status == "collect":
                    self._run_if_collect()
                elif status == "suspend":
                    self._run_if_suspended()
                elif status == "refresh":
                    self.run_if_refresh()
                elif status == "busy":
                    self._run_if_busy()
                elif status == "finished":
                    run_job_with_status_finished(job=self)
                elif status == "aborted":
                    raise ValueError(
                        "Running an aborted job with `delete_existing_job=False` is meaningless."
                    )
            except Exception:
                self.drop_status_to_aborted()
                raise

    def run_if_modal(self):
        """
        The run if modal function is called by run to execute the simulation, while waiting for the output. For this we
        use subprocess.check_output()
        """
        run_job_with_runmode_modal(job=self)

    def run_static(self):
        """
        The run static function is called by run to execute the simulation.
        """
        execute_job_with_external_executable(job=self)

    def run_if_scheduler(self):
        """
        The run if queue function is called by run if the user decides to submit the job to and queing system. The job
        is submitted to the queuing system using subprocess.Popen()
        Returns:
            int: Returns the queue ID for the job.
        """
        return run_job_with_runmode_queue(job=self)

    def transfer_from_remote(self):
        state.queue_adapter.get_job_from_remote(
            working_directory="/".join(self.working_directory.split("/")[:-1]),
        )
        state.queue_adapter.transfer_file_to_remote(
            file=self.project_hdf5.file_name,
            transfer_back=True,
            delete_file_on_remote=True,
        )
        if state.database.database_is_disabled:
            self.project.db.update()
        else:
            ft = FileTable(index_from=self.project_hdf5.path + "_hdf5/")
            df = ft.job_table(
                sql_query=None,
                user=state.settings.login_user,
                project_path=None,
                all_columns=True,
            )
            db_dict_lst = []
            for j, st, sj, p, h, hv, c, ts, tp, tc in zip(
                df.job.values,
                df.status.values,
                df.subjob.values,
                df.project.values,
                df.hamilton.values,
                df.hamversion.values,
                df.computer.values,
                df.timestart.values,
                df.timestop.values,
                df.totalcputime.values,
            ):
                gp = self.project._convert_str_to_generic_path(p)
                db_dict_lst.append(
                    {
                        "username": state.settings.login_user,
                        "projectpath": gp.root_path,
                        "project": gp.project_path,
                        "job": j,
                        "subjob": sj,
                        "hamversion": hv,
                        "hamilton": h,
                        "status": st,
                        "computer": c,
                        "timestart": datetime.utcfromtimestamp(ts.tolist() / 1e9),
                        "timestop": datetime.utcfromtimestamp(tp.tolist() / 1e9),
                        "totalcputime": tc,
                        "masterid": self.master_id,
                        "parentid": None,
                    }
                )
            _ = [self.project.db.add_item_dict(d) for d in db_dict_lst]
        self.status.string = self.project_hdf5["status"]
        if self.master_id is not None:
            self._reload_update_master(project=self.project, master_id=self.master_id)

    def run_if_interactive(self):
        """
        For jobs which executables are available as Python library, those can also be executed with a library call
        instead of calling an external executable. This is usually faster than a single core python job.
        """
        raise NotImplementedError(
            "This function needs to be implemented in the specific class."
        )

    def run_if_interactive_non_modal(self):
        """
        For jobs which executables are available as Python library, those can also be executed with a library call
        instead of calling an external executable. This is usually faster than a single core python job.
        """
        raise NotImplementedError(
            "This function needs to be implemented in the specific class."
        )

    def interactive_close(self):
        """
        For jobs which executables are available as Python library, those can also be executed with a library call
        instead of calling an external executable. This is usually faster than a single core python job. After the
        interactive execution, the job can be closed using the interactive_close function.
        """
        raise NotImplementedError(
            "This function needs to be implemented in the specific class."
        )

    def interactive_fetch(self):
        """
        For jobs which executables are available as Python library, those can also be executed with a library call
        instead of calling an external executable. This is usually faster than a single core python job. To access the
        output data during the execution the interactive_fetch function is used.
        """
        raise NotImplementedError(
            "This function needs to be implemented in the specific class."
        )

    def interactive_flush(self, path="generic", include_last_step=True):
        """
        For jobs which executables are available as Python library, those can also be executed with a library call
        instead of calling an external executable. This is usually faster than a single core python job. To write the
        interactive cache to the HDF5 file the interactive flush function is used.
        """
        raise NotImplementedError(
            "This function needs to be implemented in the specific class."
        )

    def send_to_database(self):
        """
        if the jobs should be store in the external/public database this could be implemented here, but currently it is
        just a placeholder.
        """
        if self.server.send_to_db:
            pass

    def _init_child_job(self, parent):
        """
        Finalize job initialization when job instance is created as a child from another one.

        Master jobs use this to set their own reference job, when created from that reference job.

        Args:
            parent (:class:`.GenericJob`): job instance that this job was created from
        """
        pass

    def create_job(self, job_type, job_name, delete_existing_job=False):
        """
        Create one of the following jobs:
        - 'StructureContainer’:
        - ‘StructurePipeline’:
        - ‘AtomisticExampleJob’: example job just generating random number
        - ‘ExampleJob’: example job just generating random number
        - ‘Lammps’:
        - ‘KMC’:
        - ‘Sphinx’:
        - ‘Vasp’:
        - ‘GenericMaster’:
        - ‘SerialMaster’: series of jobs run in serial
        - ‘AtomisticSerialMaster’:
        - ‘ParallelMaster’: series of jobs run in parallel
        - ‘KmcMaster’:
        - ‘ThermoLambdaMaster’:
        - ‘RandomSeedMaster’:
        - ‘MeamFit’:
        - ‘Murnaghan’:
        - ‘MinimizeMurnaghan’:
        - ‘ElasticMatrix’:
        - ‘ConvergenceVolume’:
        - ‘ConvergenceEncutParallel’:
        - ‘ConvergenceKpointParallel’:
        - ’PhonopyMaster’:
        - ‘DefectFormationEnergy’:
        - ‘LammpsASE’:
        - ‘PipelineMaster’:
        - ’TransformationPath’:
        - ‘ThermoIntEamQh’:
        - ‘ThermoIntDftEam’:
        - ‘ScriptJob’: Python script or jupyter notebook job container
        - ‘ListMaster': list of jobs

        Args:
            job_type (str): job type can be ['StructureContainer’, ‘StructurePipeline’, ‘AtomisticExampleJob’,
                                             ‘ExampleJob’, ‘Lammps’, ‘KMC’, ‘Sphinx’, ‘Vasp’, ‘GenericMaster’,
                                             ‘SerialMaster’, ‘AtomisticSerialMaster’, ‘ParallelMaster’, ‘KmcMaster’,
                                             ‘ThermoLambdaMaster’, ‘RandomSeedMaster’, ‘MeamFit’, ‘Murnaghan’,
                                             ‘MinimizeMurnaghan’, ‘ElasticMatrix’, ‘ConvergenceVolume’,
                                             ‘ConvergenceEncutParallel’, ‘ConvergenceKpointParallel’, ’PhonopyMaster’,
                                             ‘DefectFormationEnergy’, ‘LammpsASE’, ‘PipelineMaster’,
                                             ’TransformationPath’, ‘ThermoIntEamQh’, ‘ThermoIntDftEam’, ‘ScriptJob’,
                                             ‘ListMaster']
            job_name (str): name of the job
            delete_existing_job (bool): delete an existing job - default false

        Returns:
            GenericJob: job object depending on the job_type selected
        """
        job = self.project.create_job(
            job_type=job_type,
            job_name=job_name,
            delete_existing_job=delete_existing_job,
        )
        job._init_child_job(self)
        return job

    def update_master(self, force_update=False):
        """
        After a job is finished it checks whether it is linked to any metajob - meaning the master ID is pointing to
        this jobs job ID. If this is the case and the master job is in status suspended - the child wakes up the master
        job, sets the status to refresh and execute run on the master job. During the execution the master job is set to
        status refresh. If another child calls update_master, while the master is in refresh the status of the master is
        set to busy and if the master is in status busy at the end of the update_master process another update is
        triggered.

        Args:
            force_update (bool): Whether to check run mode for updating master
        """
        if not state.database.database_is_disabled:
            master_id = self.master_id
            project = self.project
            self._logger.info(
                "update master: {} {} {}".format(
                    master_id, self.get_job_id(), self.server.run_mode
                )
            )
            if master_id is not None and (
                force_update
                or not (
                    self.server.run_mode.thread
                    or self.server.run_mode.modal
                    or self.server.run_mode.interactive
                    or self.server.run_mode.worker
                )
            ):
                self._reload_update_master(project=project, master_id=master_id)

    def job_file_name(self, file_name, cwd=None):
        """
        combine the file name file_name with the path of the current working directory

        Args:
            file_name (str): name of the file
            cwd (str): current working directory - this overwrites self.project_hdf5.working_directory - optional

        Returns:
            str: absolute path to the file in the current working directory
        """
        if cwd is None:
            cwd = self.project_hdf5.working_directory
        return posixpath.join(cwd, file_name)

    def _set_hdf(self, hdf=None, group_name=None):
        if hdf is not None:
            self._hdf5 = hdf
        if group_name is not None and self._hdf5 is not None:
            self._hdf5 = self._hdf5.open(group_name)

    def to_dict(self):
        data_dict = self._type_to_dict()
        data_dict["status"] = self.status.string
        data_dict["input/generic_dict"] = {
            "restart_file_list": self._restart_file_list,
            "restart_file_dict": self._restart_file_dict,
            "exclude_nodes_hdf": self._exclude_nodes_hdf,
            "exclude_groups_hdf": self._exclude_groups_hdf,
        }
        data_dict["server"] = self._server.to_dict()
        if self._import_directory is not None:
            data_dict["import_directory"] = self._import_directory
        return data_dict

    def from_dict(self, job_dict):
        self._type_from_dict(type_dict=job_dict)
        if "import_directory" in job_dict.keys():
            self._import_directory = job_dict["import_directory"]
        self._server.from_dict(server_dict=job_dict["server"])
        input_dict = job_dict["input"]
        if "generic_dict" in input_dict.keys():
            generic_dict = input_dict["generic_dict"]
            self._restart_file_list = generic_dict["restart_file_list"]
            self._restart_file_dict = generic_dict["restart_file_dict"]
            self._exclude_nodes_hdf = generic_dict["exclude_nodes_hdf"]
            self._exclude_groups_hdf = generic_dict["exclude_groups_hdf"]
        # Backwards compatbility
        if "restart_file_list" in input_dict.keys():
            self._restart_file_list = input_dict["restart_file_list"]
        if "restart_file_dict" in input_dict.keys():
            self._restart_file_dict = input_dict["restart_file_dict"]
        if "exclude_nodes_hdf" in input_dict.keys():
            self._exclude_nodes_hdf = input_dict["exclude_nodes_hdf"]
        if "exclude_groups_hdf" in input_dict.keys():
            self._exclude_groups_hdf = input_dict["exclude_groups_hdf"]

    def to_hdf(self, hdf=None, group_name=None):
        """
        Store the GenericJob in an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """

        self._set_hdf(hdf=hdf, group_name=group_name)
        self._executable_activate_mpi()

        # Write combined dictionary to HDF5
        self._hdf5.write_dict(data_dict=self.to_dict())

        # Write remaining objects to HDF5
        if self._executable is not None:
            self.executable.to_hdf(self._hdf5)

    @classmethod
    def from_hdf_args(cls, hdf):
        """
        Read arguments for instance creation from HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object
        """
        job_name = posixpath.splitext(posixpath.basename(hdf.file_name))[0]
        project_hdf5 = type(hdf)(
            project=hdf.create_project_from_hdf5(), file_name=job_name
        )
        return {"job_name": job_name, "project": project_hdf5}

    def from_hdf(self, hdf=None, group_name=None):
        """
        Restore the GenericJob from an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """
        self._set_hdf(hdf=hdf, group_name=group_name)
        job_dict = self._hdf5.read_dict_from_hdf()
        with self._hdf5.open("input") as hdf5_input:
            job_dict["input"] = hdf5_input.read_dict_from_hdf(recursive=True)
        self.from_dict(job_dict=job_dict)

        if "executable" in self._hdf5.list_groups():
            self.executable.from_hdf(self._hdf5)

    def save(self):
        """
        Save the object, by writing the content to the HDF5 file and storing an entry in the database.

        Returns:
            (int): Job ID stored in the database
        """
        self.to_hdf()
        if not state.database.database_is_disabled:
            job_id = self.project.db.add_item_dict(self.db_entry())
            self._job_id = job_id
            _write_hdf(
                hdf_filehandle=self.project_hdf5.file_name,
                data=job_id,
                h5_path=self.job_name + "/job_id",
                overwrite="update",
            )
            self.refresh_job_status()
        else:
            job_id = self.job_name
        if self._check_if_input_should_be_written():
            self.project_hdf5.create_working_directory()
            self.write_input()
            _copy_restart_files(job=self)
        self.status.created = True
        self._calculate_predecessor()
        print(
            "The job "
            + self.job_name
            + " was saved and received the ID: "
            + str(job_id)
        )
        return job_id

    def convergence_check(self):
        """
        Validate the convergence of the calculation.

        Returns:
             (bool): If the calculation is converged
        """
        return True

    def db_entry(self):
        """
        Generate the initial database entry for the current GenericJob

        Returns:
            (dict): database dictionary {"username", "projectpath", "project", "job", "subjob", "hamversion",
                                         "hamilton", "status", "computer", "timestart", "masterid", "parentid"}
        """
        db_dict = {
            "username": state.settings.login_user,
            "projectpath": self.project_hdf5.root_path,
            "project": self.project_hdf5.project_path,
            "job": self.job_name,
            "subjob": self.project_hdf5.h5_path,
            "hamversion": self.version,
            "hamilton": self.__name__,
            "status": self.status.string,
            "computer": self._db_server_entry(),
            "timestart": datetime.now(),
            "masterid": self.master_id,
            "parentid": self.parent_id,
        }
        return db_dict

    def restart(self, job_name=None, job_type=None):
        """
        Create an restart calculation from the current calculation - in the GenericJob this is the same as create_job().
        A restart is only possible after the current job has finished. If you want to run the same job again with
        different input parameters use job.run(delete_existing_job=True) instead.

        Args:
            job_name (str): job name of the new calculation - default=<job_name>_restart
            job_type (str): job type of the new calculation - default is the same type as the exeisting calculation

        Returns:

        """
        if self.job_id is None:
            self.save()
        if job_name is None:
            job_name = "{}_restart".format(self.job_name)
        if job_type is None:
            job_type = self.__name__
        if job_type == self.__name__ and job_name not in self.project.list_nodes():
            new_ham = self.copy_to(
                new_job_name=job_name,
                new_database_entry=False,
                input_only=True,
                copy_files=False,
            )
        else:
            new_ham = self.create_job(job_type, job_name)
        new_ham.parent_id = self.job_id
        # ensuring that the new job does not inherit the restart_file_list from the old job
        new_ham._restart_file_list = list()
        new_ham._restart_file_dict = dict()
        return new_ham

    def _list_all(self):
        """
        List all groups and nodes of the HDF5 file - where groups are equivalent to directories and nodes to files.

        Returns:
            dict: {'groups': [list of groups], 'nodes': [list of nodes]}
        """
        h5_dict = self.project_hdf5.list_all()
        if self.server.new_hdf:
            h5_dict["groups"] += self._list_ext_childs()
        return h5_dict

    def signal_intercept(self, sig):
        """
        Abort the job and log signal that caused it.

        Expected to be called from
        :func:`pyiron_base.state.signal.catch_signals`.

        Args:
            sig (int): the signal that triggered the abort
        """
        try:
            self._logger.info(
                "Job {} intercept signal {}, job is shutting down".format(
                    self._job_id, sig
                )
            )
            self.drop_status_to_aborted()
        except:
            raise

    def drop_status_to_aborted(self):
        """
        Change the job status to aborted when the job was intercepted.
        """
        self.refresh_job_status()
        if not (self.status.finished or self.status.suspended):
            self.status.aborted = True

    def _run_if_new(self, debug=False):
        """
        Internal helper function the run if new function is called when the job status is 'initialized'. It prepares
        the hdf5 file and the corresponding directory structure.

        Args:
            debug (bool): Debug Mode
        """
        run_job_with_status_initialized(job=self, debug=debug)

    def _run_if_created(self):
        """
        Internal helper function the run if created function is called when the job status is 'created'. It executes
        the simulation, either in modal mode, meaning waiting for the simulation to finish, manually, or submits the
        simulation to the que.

        Returns:
            int: Queue ID - if the job was send to the queue
        """
        return run_job_with_status_created(job=self)

    def _run_if_repair(self):
        """
        Internal helper function the run if repair function is called when the run() function is called with the
        'repair' parameter.
        """
        run_job_with_parameter_repair(job=self)

    def _run_if_running(self):
        """
        Internal helper function the run if running function is called when the job status is 'running'. It allows the
        user to interact with the simulation while it is running.
        """
        run_job_with_status_running(job=self)

    def run_if_refresh(self):
        """
        Internal helper function the run if refresh function is called when the job status is 'refresh'. If the job was
        suspended previously, the job is going to be started again, to be continued.
        """
        run_job_with_status_refresh(job=self)

    def set_input_to_read_only(self):
        """
        This function enforces read-only mode for the input classes, but it has to be implemented in the individual
        classes.
        """
        self.server.lock()

    def _run_if_busy(self):
        """
        Internal helper function the run if busy function is called when the job status is 'busy'.
        """
        run_job_with_status_busy(job=self)

    def _run_if_collect(self):
        """
        Internal helper function the run if collect function is called when the job status is 'collect'. It collects
        the simulation output using the standardized functions collect_output() and collect_logfiles(). Afterwards the
        status is set to 'finished'
        """
        run_job_with_status_collect(job=self)

    def _run_if_suspended(self):
        """
        Internal helper function the run if suspended function is called when the job status is 'suspended'. It
        restarts the job by calling the run if refresh function after setting the status to 'refresh'.
        """
        run_job_with_status_suspended(job=self)

    def _executable_activate(self, enforce=False, codename=None):
        """
        Internal helper function to koad the executable object, if it was not loaded already.

        Args:
            enforce (bool): Force the executable module to reinitialize
            codename (str): Name of the resource directory and run script.
        """
        if self._executable is None or enforce:
            if codename is not None:
                self._executable = Executable(
                    codename=codename,
                    module=codename,
                    path_binary_codes=None,
                )
            elif len(self.__module__.split(".")) > 1:
                self._executable = Executable(
                    codename=self.__name__,
                    module=self.__module__.split(".")[-2],
                    path_binary_codes=None,
                )
            elif self.__module__ == "__main__":
                # Special case when the job classes defined in Jupyter notebooks
                parent_class = self.__class__.__bases__[0]
                self._executable = Executable(
                    codename=parent_class.__name__,
                    module=parent_class.__module__.split(".")[-2],
                    path_binary_codes=None,
                )
            else:
                self._executable = Executable(
                    codename=self.__name__,
                    path_binary_codes=None,
                )

    def _type_to_dict(self):
        """
        Internal helper function to save type and version in HDF5 file root
        """
        data_dict = super()._type_to_dict()
        if self._executable:  # overwrite version - default self.__version__
            data_dict["VERSION"] = self.executable.version
        if hasattr(self, "__hdf_version__"):
            data_dict["HDF_VERSION"] = self.__hdf_version__
        return data_dict

    def _type_from_dict(self, type_dict):
        self.__obj_type__ = type_dict["TYPE"]
        if self._executable is None:
            self.__obj_version__ = type_dict["VERSION"]

    def _type_from_hdf(self):
        """
        Internal helper function to load type and version from HDF5 file root
        """
        self._type_from_dict(
            type_dict={
                "TYPE": self._hdf5["TYPE"],
                "VERSION": self._hdf5["VERSION"],
            }
        )

    def run_time_to_db(self):
        """
        Internal helper function to store the run_time in the database
        """
        if not state.database.database_is_disabled and self.job_id is not None:
            self.project.db.item_update(self._runtime(), self.job_id)

    def _runtime(self):
        """
        Internal helper function to calculate runtime by substracting the starttime, from the stoptime.

        Returns:
            (dict): Database dictionary db_dict
        """
        start_time = self.project.db.get_item_by_id(self.job_id)["timestart"]
        stop_time = datetime.now()
        return {
            "timestop": stop_time,
            "totalcputime": int((stop_time - start_time).total_seconds()),
        }

    def _db_server_entry(self):
        """
        Internal helper function to connect all the info regarding the server into a single word that can be used
        e.g. as entry in a database

        Returns:
            (str): server info as single word

        """
        return self._server.db_entry()

    def _executable_activate_mpi(self):
        """
        Internal helper function to switch the executable to MPI mode
        """
        try:
            if self.server.cores > 1:
                self.executable.mpi = True
        except ValueError:
            self.server.cores = 1
            warnings.warn(
                "No multi core executable found falling back to the single core executable.",
                RuntimeWarning,
            )

    def _calculate_predecessor(self):
        """
        Internal helper function to calculate the predecessor of the current job if it was not calculated before. This
        function is used to execute a series of jobs based on their parent relationship - marked by the parent ID.
        Mainly used by the ListMaster job type.
        """
        parent_id = self.parent_id
        if parent_id is not None:
            if self._hdf5.db.get_item_by_id(parent_id)["status"] in [
                "initialized",
                "created",
            ]:
                self.status.suspended = True
                parent_job = self._hdf5.load(parent_id)
                parent_job.run()

    def _calculate_successor(self):
        """
        Internal helper function to calculate the successor of the current job. This function is used to execute a
        series of jobs based on their parent relationship - marked by the parent ID. Mainly used by the ListMaster job
        type.
        """
        for child_id in sorted(
            [
                job["id"]
                for job in self.project.db.get_items_dict(
                    {"parentid": str(self.job_id)}, return_all_columns=False
                )
            ]
        ):
            if self._hdf5.db.get_item_by_id(child_id)["status"] in ["suspended"]:
                child = self._hdf5.load(child_id)
                child.status.created = True
                self._before_successor_calc(child)
                child.run()

    @deprecate("Use job.save()")
    def _create_job_structure(self, debug=False):
        """
        Internal helper function to create the input directories, save the job in the database and write the wrapper.

        Args:
            debug (bool): Debug Mode
        """
        self.save()

    def _check_if_input_should_be_written(self):
        if self._python_only_job:
            return False
        else:
            return not (
                self.server.run_mode.interactive
                or self.server.run_mode.interactive_non_modal
            )

    def _before_successor_calc(self, ham):
        """
        Internal helper function which is executed based on the hamiltonian of the successor job, before it is executed.
        This function is used to execute a series of jobs based on their parent relationship - marked by the parent ID.
        Mainly used by the ListMaster job type.
        """
        pass

    def _reload_update_master(self, project, master_id):
        queue_flag = self.server.run_mode.queue
        master_db_entry = project.db.get_item_by_id(master_id)
        if master_db_entry["status"] == "suspended":
            project.db.set_job_status(job_id=master_id, status="refresh")
            self._logger.info("run_if_refresh() called")
            del self
            master_inspect = project.inspect(master_id)
            if master_inspect["server"]["run_mode"] == "non_modal" or (
                master_inspect["server"]["run_mode"] == "modal" and queue_flag
            ):
                master = project.load(master_id)
                master.run_if_refresh()
        elif master_db_entry["status"] == "refresh":
            project.db.set_job_status(job_id=master_id, status="busy")
            self._logger.info("busy master: {} {}".format(master_id, self.get_job_id()))
            del self


class GenericError(object):
    def __init__(self, job):
        self._job = job

    def __repr__(self):
        all_messages = ""
        for message in [self.print_message(), self.print_queue()]:
            if message is True:
                all_messages += message
        if len(all_messages) == 0:
            all_messages = "There is no error/warning"
        return all_messages

    def print_message(self, string=""):
        return self._print_error(file_name="error.msg", string=string)

    def print_queue(self, string=""):
        return self._print_error(file_name="error.out", string=string)

    def _print_error(self, file_name, string="", print_yes=True):
        if self._job[file_name] is None:
            return ""
        elif print_yes:
            return string.join(self._job[file_name])
