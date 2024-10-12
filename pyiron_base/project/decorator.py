import inspect
from typing import Optional
from pyiron_base.project.generic import Project
from pyiron_base.jobs.job.extension.server.generic import Server


def pyiron_job(
    project: Project = Project("."),
    host: Optional[str] = None,
    queue: Optional[str] = None,
    cores: int = 1,
    threads: int = 1,
    gpus: Optional[int] = None,
    run_mode: str = "modal",
    new_hdf: bool = True,
    output_file_lst: list = [],
    output_key_lst: list = [],
):
    def pyiron_job_function(funct) -> callable:
        def function(*args, **kwargs):
            delayed_job_object = project.wrap_python_function(
                python_function=funct,
                *args,
                job_name=None,
                automatically_rename=True,
                execute_job=False,
                delayed=True,
                output_file_lst=output_file_lst,
                output_key_lst=output_key_lst,
                **kwargs,
            )
            delayed_job_object._server = Server(
                host=host,
                queue=queue,
                cores=cores,
                threads=threads,
                gpus=gpus,
                run_mode=run_mode,
                new_hdf=new_hdf,
            )
            return delayed_job_object
        return function
    return pyiron_job_function


def pyiron_job_simple(funct) -> callable:
    def function(
        *args,
        project: Project = Project("."),
        resource_dict: dict = {},
        output_file_lst: list = [],
        output_key_lst: list = [],
        **kwargs
    ):
        resource_default_dict = {
            k: v.default for k, v in inspect.signature(Server).parameters.items()
        }
        resource_dict.update(
            {
                k:v for k, v in resource_default_dict.items()
                if k not in resource_dict.keys()
            }
        )
        delayed_job_object = project.wrap_python_function(
            python_function=funct,
            *args,
            job_name=None,
            automatically_rename=True,
            execute_job=False,
            delayed=True,
            output_file_lst=output_file_lst,
            output_key_lst=output_key_lst,
            **kwargs,
        )
        delayed_job_object._server = Server(**resource_dict)
        return delayed_job_object
    return function