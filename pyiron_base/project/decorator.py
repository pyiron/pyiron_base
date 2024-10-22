import inspect
from typing import Optional

from pyiron_base.jobs.job.extension.server.generic import Server
from pyiron_base.project.generic import Project


# The combined decorator
def pyiron_job(
    funct: Optional[callable] = None,
    *,
    host: Optional[str] = None,
    queue: Optional[str] = None,
    cores: int = 1,
    threads: int = 1,
    gpus: Optional[int] = None,
    run_mode: str = "modal",
    new_hdf: bool = True,
    output_file_lst: list = [],
    output_key_lst: list = [],
    list_length: Optional[int] = None,
):
    """
    Decorator to create a pyiron job object from any python function

    Args:
        funct (callable): python function to create a job object from
        host (str): the hostname of the current system.
        queue (str): the queue selected for a current simulation.
        cores (int): the number of cores selected for the current simulation.
        threads (int): the number of threads selected for the current simulation.
        gpus (int): the number of gpus selected for the current simulation.
        run_mode (str): the run mode of the job ['modal', 'non_modal', 'queue', 'manual']
        new_hdf (bool): defines whether a subjob should be stored in the same HDF5 file or in a new one.
        output_file_lst (list):
        output_key_lst (list):

    Returns:
        callable: The decorated functions

    Example:
        >>> from pyiron_base import pyiron_job, Project
        >>>
        >>> @pyiron_job
        >>> def my_function_a(a, b=8):
        >>>     return a + b
        >>>
        >>> @pyiron_job(cores=2)
        >>> def my_function_b(a, b=8):
        >>>     return a + b
        >>>
        >>> pr = Project("test")
        >>> c = my_function_a(a=1, b=2, pyiron_project=pr)
        >>> d = my_function_b(a=c, b=3, pyiron_project=pr)
        >>> print(d.pull())

        Output: 6
    """

    def get_delayed_object(
        *args,
        pyiron_project: Project,
        python_function: callable,
        pyiron_resource_dict: dict,
        resource_default_dict: dict,
        **kwargs,
    ):
        for k, v in resource_default_dict.items():
            if k not in pyiron_resource_dict:
                pyiron_resource_dict[k] = v
        delayed_job_object = pyiron_project.wrap_python_function(
            python_function=python_function,
            *args,
            job_name=None,
            automatically_rename=True,
            execute_job=False,
            delayed=True,
            output_file_lst=output_file_lst,
            output_key_lst=output_key_lst,
            list_length=list_length,
            **kwargs,
        )
        delayed_job_object._server = Server(**pyiron_resource_dict)
        return delayed_job_object

    # This is the actual decorator function that applies to the decorated function
    def pyiron_job_function(f) -> callable:
        def function(
            *args, pyiron_project: Project, pyiron_resource_dict: dict = {}, **kwargs
        ):
            resource_default_dict = {
                "host": host,
                "queue": queue,
                "cores": cores,
                "threads": threads,
                "gpus": gpus,
                "run_mode": run_mode,
                "new_hdf": new_hdf,
            }
            return get_delayed_object(
                *args,
                python_function=f,
                pyiron_project=pyiron_project,
                pyiron_resource_dict=pyiron_resource_dict,
                resource_default_dict=resource_default_dict,
                **kwargs,
            )

        return function

    # If funct is None, it means the decorator is called with arguments (like @pyiron_job(...))
    if funct is None:
        return pyiron_job_function

    # If funct is not None, it means the decorator is called without parentheses (like @pyiron_job)
    else:
        # Assume this usage and handle the decorator like `pyiron_job_simple`
        def function(
            *args,
            pyiron_project: Project,
            pyiron_resource_dict: dict = {},
            **kwargs,
        ):
            resource_default_dict = {
                k: v.default for k, v in inspect.signature(Server).parameters.items()
            }
            return get_delayed_object(
                *args,
                python_function=funct,
                pyiron_project=pyiron_project,
                pyiron_resource_dict=pyiron_resource_dict,
                resource_default_dict=resource_default_dict,
                **kwargs,
            )

        return function
