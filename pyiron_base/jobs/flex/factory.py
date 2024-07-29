from typing import Callable, Dict, Optional

from pyiron_base.utils.instance import static_isinstance


def create_job_factory(
    executable_str: str,
    write_input_funct: Optional[Callable] = None,
    collect_output_funct: Optional[Callable] = None,
    default_input_dict: Optional[Dict] = None,
) -> callable:
    """
    Create a new job class based on pre-defined write_input() and collect_output() function plus a dictionary of
    default inputs and an executable string.

    Args:
        executable_str (str): Call to an external executable
        write_input_funct (callable): The write input function write_input(input_dict, working_directory)
        collect_output_funct (callable): The collect output function collect_output(working_directory)
        default_input_dict (dict/None): Default input for the newly created job class

    Example:

    >>> def write_input(input_dict, working_directory="."):
    >>>     with open(os.path.join(working_directory, "input_file"), "w") as f:
    >>>         f.write(str(input_dict["energy"]))
    >>>
    >>>
    >>> def collect_output(working_directory="."):
    >>>     with open(os.path.join(working_directory, "output_file"), "r") as f:
    >>>         return {"energy": float(f.readline())}
    >>>
    >>>
    >>> from pyiron_base import Project, create_job_factory
    >>> pr = Project("test")
    >>> create_catjob = create_job_factory(
    >>>     write_input_funct=write_input,
    >>>     collect_output_funct=collect_output,
    >>>     default_input_dict={"energy": 1.0},
    >>>     executable_str="cat input_file > output_file",
    >>> )
    >>> job = create_catjob(project=pr, job_name="job_test")
    >>> job.input["energy"] = 2.0
    >>> job.run()
    >>> job.output
    """

    def job_factory(project, job_name):
        """
        Create a job based on the previously defined write_input(), collect_output() and the executable string.

        Args:
            project (ProjectHDFio/ Project): ProjectHDFio instance which points to the HDF5 file the job is stored in
            job_name (str): name of the job, which has to be unique within the project

        Returns:
            pyiron_base.jobs.flex.executablecontainer.ExecutableContainerJob: pyiron job object
        """
        if static_isinstance(project, "pyiron_base.project.generic.Project"):
            job = project.create.job.ExecutableContainerJob(job_name=job_name)
        elif static_isinstance(project, "pyiron_base.storage.hdfio.ProjectHDFio"):
            job = project.project.create.job.ExecutableContainerJob(job_name=job_name)
        else:
            raise TypeError(
                "Expected ProjectHDFio/ Project but recieved", type(project)
            )
        job.set_job_type(
            write_input_funct=write_input_funct,
            collect_output_funct=collect_output_funct,
            default_input_dict=default_input_dict,
            executable_str=executable_str,
        )
        return job

    return job_factory
