# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
from datetime import datetime
import multiprocessing
import os
import posixpath
import subprocess

from pyiron_base.utils.deprecate import deprecate
from pyiron_base.jobs.job.wrapper import JobWrapper
from pyiron_base.state import state


"""
The function job.run() inside pyiron is executed differently depending on the status of the job object. This module 
introduces the most general run functions and how they are selected. 

If an additional parameter is provided, then a specific run function is executed: 
    repair: run_job_with_parameter_repair

If no explicit parameter is provided the first implicit parameter is the job.status: 
    initialized: run_job_with_status_initialized
    created: run_job_with_status_created
    submitted: run_job_with_status_submitted
    running: run_job_with_status_running
    refresh: run_job_with_status_refresh
    busy: run_job_with_status_busy
    collect: run_job_with_status_collect
    suspended: run_job_with_status_suspended
    finished: run_job_with_status_finished
    
Afterwards inside the run_job_with_status_created() function the job is executed differently depending on the run mode 
of the server object attached to the job object: job.server.run_mode
    manually: run_job_with_runmode_manually
    modal: run_job_with_runmode_modal
    non_modal: run_job_with_runmode_non_modal
    interactive: run_job_with_runmode_interactive
    interactive_non_modal: run_job_with_runmode_interactive_non_modal
    queue: run_job_with_runmode_queue
    
Finally for jobs which call an external executable the execution is implemented in an function as well: 
    execute_job_with_external_executable
"""


# Parameter
def run_job_with_parameter_repair(job):
    """
    Internal helper function the run if repair function is called when the run() function is called with the
    'repair' parameter.

    Args:
        job (GenericJob): pyiron job object
    """
    job._run_if_created()


# Job Status
def run_job_with_status_initialized(job, debug=False):
    """
    Internal helper function the run if new function is called when the job status is 'initialized'. It prepares
    the hdf5 file and the corresponding directory structure.

    Args:
        job (GenericJob): pyiron job object
        debug (bool): Debug Mode
    """
    job.validate_ready_to_run()
    if job.server.run_mode.queue:
        job.check_setup()
    if job.check_if_job_exists():
        print("job exists already and therefore was not created!")
    else:
        job.save()
        job.run()


def run_job_with_status_created(job):
    """
    Internal helper function the run if created function is called when the job status is 'created'. It executes
    the simulation, either in modal mode, meaning waiting for the simulation to finish, manually, or submits the
    simulation to the que.

    Args:
        job (GenericJob): pyiron job object

    Returns:
        int: Queue ID - if the job was send to the queue
    """
    job.status.submitted = True

    # Different run modes
    if job.server.run_mode.manual:
        job.run_if_manually()
    elif job.server.run_mode.worker:
        job.run_if_manually(_manually_print=False)
    elif job.server.run_mode.modal:
        job.run_static()
    elif (
        job.server.run_mode.non_modal
        or job.server.run_mode.thread
        or job.server.run_mode.worker
    ):
        job.run_if_non_modal()
    elif job.server.run_mode.queue:
        job.run_if_scheduler()
    elif job.server.run_mode.interactive:
        job.run_if_interactive()
    elif job.server.run_mode.interactive_non_modal:
        job.run_if_interactive_non_modal()


def run_job_with_status_submitted(
    job,
):  # Submitted jobs are handled by the job wrapper!
    """
    Internal helper function the run if submitted function is called when the job status is 'submitted'. It means
    the job is waiting in the queue. ToDo: Display a list of the users jobs in the queue.

    Args:
        job (GenericJob): pyiron job object
    """
    if (
        job.server.run_mode.queue
        and not job.project.queue_check_job_is_waiting_or_running(job)
    ):
        if not state.queue_adapter.remote_flag:
            job.run(delete_existing_job=True)
        else:
            job.transfer_from_remote()
    else:
        print("Job " + str(job.job_id) + " is waiting in the que!")


def run_job_with_status_running(job):
    """
    Internal helper function the run if running function is called when the job status is 'running'. It allows the
    user to interact with the simulation while it is running.

    Args:
        job (GenericJob): pyiron job object
    """
    if (
        job.server.run_mode.queue
        and not job.project.queue_check_job_is_waiting_or_running(job)
    ):
        job.run(delete_existing_job=True)
    elif job.server.run_mode.interactive:
        job.run_if_interactive()
    elif job.server.run_mode.interactive_non_modal:
        job.run_if_interactive_non_modal()
    else:
        print("Job " + str(job.job_id) + " is running!")


def run_job_with_status_refresh(job):
    """
    Internal helper function the run if refresh function is called when the job status is 'refresh'. If the job was
    suspended previously, the job is going to be started again, to be continued.

    Args:
        job (GenericJob): pyiron job object
    """
    raise NotImplementedError(
        "Refresh is not supported for this job type for job  " + str(job.job_id)
    )


def run_job_with_status_busy(job):
    """
    Internal helper function the run if busy function is called when the job status is 'busy'.

    Args:
        job (GenericJob): pyiron job object
    """
    raise NotImplementedError(
        "Refresh is not supported for this job type for job  " + str(job.job_id)
    )


def run_job_with_status_collect(job):
    """
    Internal helper function the run if collect function is called when the job status is 'collect'. It collects
    the simulation output using the standardized functions collect_output() and collect_logfiles(). Afterwards the
    status is set to 'finished'

    Args:
        job (GenericJob): pyiron job object
    """
    job.collect_output()
    job.collect_logfiles()
    if job.job_id is not None:
        job.project.db.item_update(job._runtime(), job.job_id)
    if job.status.collect:
        if not job.convergence_check():
            job.status.not_converged = True
        else:
            if job._compress_by_default:
                job.compress()
            job.status.finished = True
        job._hdf5["status"] = job.status.string
    if job.job_id is not None:
        job._calculate_successor()
    job.send_to_database()
    job.update_master()


def run_job_with_status_suspended(job):
    """
    Internal helper function the run if suspended function is called when the job status is 'suspended'. It
    restarts the job by calling the run if refresh function after setting the status to 'refresh'.

    Args:
        job (GenericJob): pyiron job object
    """
    job.status.refresh = True
    job.run()


@deprecate(
    run_again="Either delete the job via job.remove() or use delete_existing_job=True.",
    version="0.4.0",
)
def run_job_with_status_finished(job, delete_existing_job=False, run_again=False):
    """
    Internal helper function the run if finished function is called when the job status is 'finished'. It loads
    the existing job.

    Args:
        job (GenericJob): pyiron job object
        delete_existing_job (bool): Delete the existing job and run the simulation again.
        run_again (bool): Same as delete_existing_job (deprecated)
    """
    if run_again:
        delete_existing_job = True
    if delete_existing_job:
        parent_id = job.parent_id
        job.parent_id = None
        job.remove()
        job._job_id = None
        job.status.initialized = True
        job.parent_id = parent_id
        job.run()
    else:
        job.logger.warning(
            "The job {} is being loaded instead of running. To re-run use the argument "
            "'delete_existing_job=True in create_job'".format(job.job_name)
        )
        job.from_hdf()


# Run Modes
def run_job_with_runmode_manually(job, _manually_print=True):
    """
    Internal helper function to run a job manually.

    Args:
        job (GenericJob): pyiron job object
        _manually_print (bool): [True/False] print command for execution - default=True
    """
    if _manually_print:
        abs_working = posixpath.abspath(job.project_hdf5.working_directory)
        print(
            "You have selected to start the job manually. "
            + "To run it, go into the working directory {} and ".format(abs_working)
            + "call 'python -m pyiron_base.cli wrapper -p {}".format(abs_working)
            + " -j {} ' ".format(job.job_id)
        )


def run_job_with_runmode_modal(job):
    """
    The run if modal function is called by run to execute the simulation, while waiting for the output. For this we
    use subprocess.check_output()

    Args:
        job (GenericJob): pyiron job object
    """
    job.run_static()


def run_job_with_runmode_interactive(job):
    """
    For jobs which executables are available as Python library, those can also be executed with a library call
    instead of calling an external executable. This is usually faster than a single core python job.

    Args:
        job (GenericJob): pyiron job object
    """
    raise NotImplementedError(
        "This function needs to be implemented in the specific class."
    )


def run_job_with_runmode_interactive_non_modal(job):
    """
    For jobs which executables are available as Python library, those can also be executed with a library call
    instead of calling an external executable. This is usually faster than a single core python job.

    Args:
        job (GenericJob): pyiron job object
    """
    raise NotImplementedError(
        "This function needs to be implemented in the specific class."
    )


def run_job_with_runmode_non_modal(job):
    """
    The run if non modal function is called by run to execute the simulation in the background. For this we use
    multiprocessing.Process()

    Args:
        job (GenericJob): pyiron job object
    """
    if not state.database.database_is_disabled:
        if not state.database.using_local_database:
            args = (job.job_id, job.project_hdf5.working_directory, False, None)
        else:
            args = (
                job.job_id,
                job.project_hdf5.working_directory,
                False,
                str(job.project.db.conn.engine.url),
            )

        p = multiprocessing.Process(
            target=multiprocess_wrapper,
            args=args,
        )

        if job.master_id and job.server.run_mode.non_modal:
            del job
            p.start()
        else:
            if job.server.run_mode.non_modal:
                p.start()
            else:
                job._process = p
                job._process.start()
    else:
        command = (
            "python -m pyiron_base.cli wrapper -p "
            + job.working_directory
            + " -f "
            + job.project_hdf5.file_name
            + job.project_hdf5.h5_path
        )
        working_directory = job.project_hdf5.working_directory
        if not os.path.exists(working_directory):
            os.makedirs(working_directory)
        del job
        subprocess.Popen(
            command,
            cwd=working_directory,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )


def run_job_with_runmode_queue(job):
    """
    The run if queue function is called by run if the user decides to submit the job to and queing system. The job
    is submitted to the queuing system using subprocess.Popen()

    Args:
        job (GenericJob): pyiron job object

    Returns:
        int: Returns the queue ID for the job.
    """
    if state.queue_adapter is None:
        raise TypeError("No queue adapter defined.")
    if state.queue_adapter.remote_flag:
        filename = state.queue_adapter.convert_path_to_remote(
            path=job.project_hdf5.file_name
        )
        working_directory = state.queue_adapter.convert_path_to_remote(
            path=job.working_directory
        )
        command = (
            "python -m pyiron_base.cli wrapper -p "
            + working_directory
            + " -f "
            + filename
            + job.project_hdf5.h5_path
            + " --submit"
        )
        state.queue_adapter.transfer_file_to_remote(
            file=job.project_hdf5.file_name, transfer_back=False
        )
    elif state.database.database_is_disabled:
        command = (
            "python -m pyiron_base.cli wrapper -p "
            + job.working_directory
            + " -f "
            + job.project_hdf5.file_name
            + job.project_hdf5.h5_path
        )
    else:
        command = (
            "python -m pyiron_base.cli wrapper -p "
            + job.working_directory
            + " -j "
            + str(job.job_id)
        )
    que_id = state.queue_adapter.submit_job(
        queue=job.server.queue,
        job_name="pi_" + str(job.job_id),
        working_directory=job.project_hdf5.working_directory,
        cores=job.server.cores,
        run_time_max=job.server.run_time,
        memory_max=job.server.memory_limit,
        command=command,
    )
    if que_id is not None:
        job.server.queue_id = que_id
        job._server.to_hdf(job._hdf5)
        print("Queue system id: ", que_id)
    else:
        job._logger.warning("Job aborted")
        job.status.aborted = True
        raise ValueError("run_queue.sh crashed")
    state.logger.debug("submitted %s", job.job_name)
    job._logger.debug("job status: %s", job.status)
    job._logger.info(
        "{}, status: {}, submitted: queue id {}".format(
            job.job_info_str, job.status, que_id
        )
    )


def execute_job_with_external_executable(job):
    """
    The run static function is called by run to execute the simulation.

    Args:
        job (GenericJob): pyiron job object
    """
    job._logger.info(
        "{}, status: {}, run job (modal)".format(job.job_info_str, job.status)
    )
    if job.executable.executable_path == "":
        job.status.aborted = True
        raise ValueError("No executable set!")
    job.status.running = True
    if job.job_id is not None:
        job.project.db.item_update({"timestart": datetime.now()}, job.job_id)
    job_crashed, out = False, None
    if job.server.cores == 1 or not job.executable.mpi:
        executable = str(job.executable)
        shell = True
    elif isinstance(job.executable.executable_path, list):
        executable = job.executable.executable_path[:] + [
            str(job.server.cores),
            str(job.server.threads),
        ]
        shell = False
    else:
        executable = [
            job.executable.executable_path,
            str(job.server.cores),
            str(job.server.threads),
        ]
        shell = False
    try:
        out = subprocess.run(
            executable,
            cwd=job.project_hdf5.working_directory,
            shell=shell,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            check=True,
        ).stdout
    except subprocess.CalledProcessError as e:
        out = e.output
        if e.returncode in job.executable.accepted_return_codes:
            pass
        elif not job.server.accept_crash:
            job._logger.warning("Job aborted")
            job._logger.warning(e.output)
            job.status.aborted = True
            if job.job_id is not None:
                job.project.db.item_update(job._runtime(), job.job_id)
            error_file = posixpath.join(job.project_hdf5.working_directory, "error.msg")
            with open(error_file, "w") as f:
                f.write(e.output)
            if job.server.run_mode.non_modal:
                state.database.close_connection()
            raise RuntimeError("Job aborted")
        else:
            job_crashed = True

    with open(
        posixpath.join(job.project_hdf5.working_directory, "error.out"), mode="w"
    ) as f_err:
        f_err.write(out)

    job.set_input_to_read_only()
    job.status.collect = True
    job._logger.info(
        "{}, status: {}, output: {}".format(job.job_info_str, job.status, out)
    )
    job.run()
    if job_crashed:
        job.status.aborted = True


def multiprocess_wrapper(job_id, working_dir, debug=False, connection_string=None):
    job_wrap = JobWrapper(
        working_directory=str(working_dir),
        job_id=int(job_id),
        debug=debug,
        connection_string=connection_string,
    )
    job_wrap.job.run_static()
