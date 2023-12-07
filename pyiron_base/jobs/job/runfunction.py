# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import multiprocessing
import os
import posixpath
import subprocess

from jinja2 import Template

from pyiron_base.utils.deprecate import deprecate
from pyiron_base.jobs.job.wrapper import JobWrapper
from pyiron_base.state import state
from pyiron_base.utils.instance import static_isinstance


try:
    import flux.job

    flux_available = True
except ImportError:
    flux_available = False

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
    manual: run_job_with_runmode_manually
    modal: run_job_with_runmode_modal
    non_modal: run_job_with_runmode_non_modal
    interactive: run_job_with_runmode_interactive
    interactive_non_modal: run_job_with_runmode_interactive_non_modal
    queue: run_job_with_runmode_queue
    srun: run_job_with_runmode_srun
    executor: run_job_with_runmode_executor
    thread: only affects children of a GenericMaster 
    worker: only affects children of a GenericMaster 
    
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
        run_job_with_runmode_manually(job=job, _manually_print=True)
    elif job.server.run_mode.worker:
        run_job_with_runmode_manually(job=job, _manually_print=True)
    elif job.server.run_mode.modal:
        job.run_static()
    elif job.server.run_mode.srun:
        run_job_with_runmode_srun(job=job)
    elif job.server.run_mode.executor:
        if job.server.gpus is not None:
            gpus_per_slot = int(job.server.gpus / job.server.cores)
            if gpus_per_slot < 0:
                raise ValueError(
                    "Both job.server.gpus and job.server.cores have to be greater than zero."
                )
        else:
            gpus_per_slot = None
        run_job_with_runmode_executor(
            job=job,
            executor=job.server.executor,
            gpus_per_slot=gpus_per_slot,
        )
    elif (
        job.server.run_mode.non_modal
        or job.server.run_mode.thread
        or job.server.run_mode.worker
    ):
        run_job_with_runmode_non_modal(job=job)
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
    job.run_time_to_db()
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
def run_job_with_status_finished(job):
    """
    Internal helper function the run if finished function is called when the job status is 'finished'. It loads
    the existing job.

    Args:
        job (GenericJob): pyiron job object
    """
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
        if not state.database.database_is_disabled:
            print(
                "You have selected to start the job manually. "
                + "To run it, go into the working directory {} and ".format(abs_working)
                + "call 'python -m pyiron_base.cli wrapper -p {}".format(abs_working)
                + " -j {} ' ".format(job.job_id)
            )
        else:
            print(
                "You have selected to start the job manually. "
                + "To run it, go into the working directory {} and ".format(abs_working)
                + "call 'python -m pyiron_base.cli wrapper -p {}".format(abs_working)
                + " -f {} ' ".format(
                    job.project_hdf5.file_name + job.project_hdf5.h5_path
                )
            )


def run_job_with_runmode_modal(job):
    """
    The run if modal function is called by run to execute the simulation, while waiting for the output. For this we
    use subprocess.check_output()

    Args:
        job (GenericJob): pyiron job object
    """
    job.run_static()


def run_job_with_runmode_non_modal(job):
    """
    The run if non modal function is called by run to execute the simulation in the background. For this we use
    multiprocessing.Process()

    Args:
        job (GenericJob): pyiron job object
    """
    if not state.database.database_is_disabled:
        if not state.database.using_local_database:
            args = (job.project_hdf5.working_directory, job.job_id, None, False, None)
        else:
            args = (
                job.project_hdf5.working_directory,
                job.job_id,
                None,
                False,
                str(job.project.db.conn.engine.url),
            )
    else:
        args = (
            job.project_hdf5.working_directory,
            None,
            job.project_hdf5.file_name + job.project_hdf5.h5_path,
            False,
            None,
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
        **job.server.additional_arguments,
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


def run_job_with_runmode_srun(job):
    working_directory = job.project_hdf5.working_directory
    if not state.database.database_is_disabled:
        if not state.database.using_local_database:
            command = (
                "srun python -m pyiron_base.cli wrapper -p "
                + working_directory
                + "- j "
                + job.job_id
            )
        else:
            raise ValueError(
                "run_job_with_runmode_srun() does not support local databases."
            )
    else:
        command = (
            "srun python -m pyiron_base.cli wrapper -p "
            + working_directory
            + " -f "
            + job.project_hdf5.file_name
            + job.project_hdf5.h5_path
        )
    os.makedirs(working_directory, exist_ok=True)
    del job
    subprocess.Popen(
        command,
        cwd=working_directory,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )


def run_job_with_runmode_executor(job, executor, gpus_per_slot=None):
    """
    Introduced in Python 3.2 the concurrent.futures interface enables the asynchronous execution of python programs.
    A function is submitted to the executor and a future object is returned. The future object is updated in the
    background once the executor finished executing the function. The job.server.run_mode.executor implements the same
    functionality for pyiron jobs. An executor is set as an attribute to the server object:

    >>> job.server.executor = concurrent.futures.Executor()
    >>> job.run()
    >>> job.server.future.done()
    False
    >>> job.server.future.result()
    >>> job.server.future.done()
    True

    When the job is executed by calling the run() function a future object is returned. The job is then executed in the
    background and the user can use the future object to check the status of the job.

    Args:
        job (GenericJob): pyiron job object
        executor (concurrent.futures.Executor): executor class which implements the executor interface defined in the
                                                python concurrent.futures.Executor class.
        gpus_per_slot (int): number of GPUs per MPI rank, typically 1
    """

    if static_isinstance(
        obj=job,
        obj_type="pyiron_base.jobs.master.generic.GenericMaster"
        # The static check is used to avoid a circular import:
        # runfunction -> GenericJob -> GenericMaster -> runfunction
        # This smells a bit, so if a better architecture is found in the future, use it
        # to avoid string-based specifications
    ):
        raise NotImplementedError(
            "Currently job.server.run_mode.executor does not support GenericMaster jobs."
        )
    if flux_available and isinstance(executor, flux.job.FluxExecutor):
        run_job_with_runmode_executor_flux(
            job=job, executor=executor, gpus_per_slot=gpus_per_slot
        )
    elif isinstance(executor, ProcessPoolExecutor):
        run_job_with_runmode_executor_futures(job=job, executor=executor)
    else:
        raise NotImplementedError(
            "Currently only flux.job.FluxExecutor and concurrent.futures.ProcessPoolExecutor are supported."
        )


def run_job_with_runmode_executor_futures(job, executor):
    """
    Interface for the ProcessPoolExecutor implemented in the python standard library as part of the concurrent.futures
    module. The ProcessPoolExecutor does not provide any resource management, so the user is responsible to keep track of
    the number of compute cores in use, as over-subscription can lead to low performance.

    The [ProcessPoolExecutor docs](https://docs.python.org/3/library/concurrent.futures.html#processpoolexecutor) state: "The __main__ module must be importable by worker subprocesses. This means that ProcessPoolExecutor will not work in the interactive interpreter." (i.e. Jupyter notebooks). For standard usage this is a non-issue, but for the edge case of job classes defined in-notebook (e.g. children of `PythonTemplateJob`), the using the ProcessPoolExecutor will result in errors. To resolve this, relocate such classes to an importable .py file.

    >>> from concurrent.futures import ProcessPoolExecutor
    >>> job.server.executor = ProcessPoolExecutor()
    >>> job.server.future.done()
    False
    >>> job.server.future.result()
    >>> job.server.future.done()
    True

    Args:
        job (GenericJob): pyiron job object
        executor (concurrent.futures.Executor): executor class which implements the executor interface defined in the
                                                python concurrent.futures.Executor class.
    """
    if state.database.database_is_disabled:
        file_path = job.project_hdf5.file_name + job.project_hdf5.h5_path
        connection_string = None
    else:
        file_path = None
        if state.database.using_local_database:
            connection_string = str(job.project.db.conn.engine.url)
        else:
            connection_string = None

    job.server.future = executor.submit(
        multiprocess_wrapper,
        working_directory=job.project_hdf5.working_directory,
        job_id=job.job_id,
        file_path=file_path,
        debug=False,
        connection_string=connection_string,
    )


def run_job_with_runmode_executor_flux(job, executor, gpus_per_slot=None):
    """
    Interface for the flux.job.FluxExecutor executor. Flux is a hierarchical resource management. It can either be used to
    replace queuing systems like SLURM or be used as a user specific queuing system within an existing allocation.
    pyiron provides two interfaces to flux, this executor interface as well as a traditional queuing system interface
    via pysqa. This executor interface is designed for the development of asynchronous simulation protocols, while the
    traditional queuing system interface simplifies the transition from other queuing systems like SLURM. The usuage
    is analog to the concurrent.futures.Executor interface:

    >>> from flux.job import FluxExecutor
    >>> job.server.executor = FluxExecutor()
    >>> job.run()
    >>> job.server.future.done()
    False
    >>> job.server.future.result()
    >>> job.server.future.done()
    True

    A word of caution - flux is currently only available on Linux, for all other operation systems the ProcessPoolExecutor
    from the python standard library concurrent.futures is recommended. The advantage of flux over the ProcessPoolExecutor
    is that flux takes over the resource management, like monitoring how many cores are available while with the
    ProcessPoolExecutor this is left to the user.

    Args:
        job (GenericJob): pyiron job object
        executor (flux.job.FluxExecutor): flux executor class which implements the executor interface defined in the
                                      python concurrent.futures.Executor class.
        gpus_per_slot (int): number of GPUs per MPI rank, typically 1

     Returns:
         concurrent.futures.Future: future object to develop asynchronous simulation protocols
    """
    if not flux_available:
        raise ModuleNotFoundError(
            "No module named 'flux'. Running in flux mode is only available on Linux;"
            "For CPU jobs, please use `conda install -c conda-forge flux-core`; for "
            "GPU support you will additionally need "
            "`conda install -c conda-forge flux-sched libhwloc=*=cuda*`"
        )
    executable_str, job_name = _generate_flux_execute_string(
        job=job, database_is_disabled=state.database.database_is_disabled
    )
    jobspec = flux.job.JobspecV1.from_batch_command(
        jobname=job_name,
        script=executable_str,
        num_nodes=1,
        cores_per_slot=1,
        gpus_per_slot=gpus_per_slot,
        num_slots=job.server.cores,
    )
    jobspec.cwd = job.project_hdf5.working_directory
    jobspec.environment = dict(os.environ)
    job.server.future = executor.submit(jobspec)


def run_time_decorator(func):
    def wrapper(job):
        if not state.database.database_is_disabled and job.job_id is not None:
            job.project.db.item_update({"timestart": datetime.now()}, job.job_id)
            func(job)
            job.project.db.item_update(job._runtime(), job.job_id)
        else:
            func(job)

    return wrapper


@run_time_decorator
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
    executable, shell = job.executable.get_input_for_subprocess_call(
        cores=job.server.cores, threads=job.server.threads, gpus=job.server.gpus
    )
    job_crashed, out = False, None
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
        out, job_crashed = handle_failed_job(job=job, error=e)

    job._logger.info(
        "{}, status: {}, output: {}".format(job.job_info_str, job.status, out)
    )
    with open(
        posixpath.join(job.project_hdf5.working_directory, "error.out"), mode="w"
    ) as f_err:
        f_err.write(out)
    handle_finished_job(job=job, job_crashed=job_crashed, collect_output=True)


def handle_finished_job(job, job_crashed=False, collect_output=True):
    """
    Handle finished jobs, collect the calculation output and set the status to aborted if the job crashed

    Args:
        job (GenericJob): pyiron job object
        job_crashed (boolean): flag to indicate failed jobs
        collect_output (boolean): flag to indicate if the collect_output() function should be called
    """
    job.set_input_to_read_only()
    if collect_output:
        job.status.collect = True
        job.run()
    if job_crashed:
        job.status.aborted = True
        job._hdf5["status"] = job.status.string


def handle_failed_job(job, error):
    """
    Handle failed jobs write error message to text file and update database

    Args:
        job (GenericJob): pyiron job object
        error (subprocess.SubprocessError): error of the subprocess executing the job

    Returns:
        boolean, str: job crashed and error message
    """
    out = error.output
    if error.returncode in job.executable.accepted_return_codes:
        return False, out
    elif not job.server.accept_crash:
        job._logger.warning("Job aborted")
        job._logger.warning(error.output)
        job.status.aborted = True
        job.run_time_to_db()
        error_file = posixpath.join(job.project_hdf5.working_directory, "error.msg")
        with open(error_file, "w") as f:
            f.write(error.output)
        if job.server.run_mode.non_modal:
            state.database.close_connection()
        raise RuntimeError("Job aborted")
    else:
        return True, out


def multiprocess_wrapper(
    working_directory, job_id=None, file_path=None, debug=False, connection_string=None
):
    if job_id is not None:
        job_wrap = JobWrapper(
            working_directory=str(working_directory),
            job_id=int(job_id),
            debug=debug,
            connection_string=connection_string,
        )
    elif file_path is not None:
        hdf5_file = (
            ".".join(file_path.split(".")[:-1])
            + "."
            + file_path.split(".")[-1].split("/")[0]
        )
        h5_path = "/".join(file_path.split(".")[-1].split("/")[1:])
        job_wrap = JobWrapper(
            working_directory,
            job_id=None,
            hdf5_file=hdf5_file,
            h5_path="/" + h5_path,
            debug=debug,
            connection_string=connection_string,
        )
    else:
        raise ValueError("Either job_id or file_path have to be not None.")
    job_wrap.job.run_static()


def _generate_flux_execute_string(job, database_is_disabled):
    if not database_is_disabled:
        executable_template = Template(
            "#!/bin/bash\n"
            + "python -m pyiron_base.cli wrapper -p {{working_directory}} -j {{job_id}}"
        )
        executable_str = executable_template.render(
            working_directory=job.working_directory,
            job_id=str(job.job_id),
        )
        job_name = "pi_" + str(job.job_id)
    else:
        executable_template = Template(
            "#!/bin/bash\n"
            + "python -m pyiron_base.cli wrapper -p {{working_directory}} -f {{file_name}}{{h5_path}}"
        )
        executable_str = executable_template.render(
            working_directory=job.working_directory,
            file_name=job.project_hdf5.file_name,
            h5_path=job.project_hdf5.h5_path,
        )
        job_name = "pi_" + job.job_name
    return executable_str, job_name
