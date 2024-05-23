import cloudpickle
import numpy as np
from pyiron_base.jobs.job.template import TemplateJob


class ExecutableContainerJob(TemplateJob):
    """
    The ExecutableContainerJob is designed to wrap any kind of external executable into a pyiron job object by providing
    a write_input(input_dict, working_directory) and a collect_output(working_directory) function.

    Example:

    >>> import os
    >>>
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
    >>> from pyiron_base import Project
    >>> pr = Project("test")
    >>> pr.create_job_class(
    >>>     class_name="CatJob",
    >>>     write_input_funct=write_input,
    >>>     collect_output_funct=collect_output,
    >>>     default_input_dict={"energy": 1.0},
    >>>     executable_str="cat input_file > output_file",
    >>> )
    >>> job = pr.create.job.CatJob(job_name="job_test")
    >>> job.input["energy"] = 2.0
    >>> job.run()
    >>> print(job.output)
    DataContainer({'energy': 2.0})

    """

    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self._write_input_funct = None
        self._collect_output_funct = None

    def set_job_type(
        self,
        executable_str,
        write_input_funct=None,
        collect_output_funct=None,
        default_input_dict=None,
    ):
        """
        Set the pre-defined write_input() and collect_output() function plus a dictionary of default inputs and an
        executable string.

        Args:
            executable_str (str): Call to an external executable
            write_input_funct (callable): The write input function write_input(input_dict, working_directory)
            collect_output_funct (callable): The collect output function collect_output(working_directory)
            default_input_dict (dict/None): Default input for the newly created job class

        Returns:
            callable: Function which requires a project and a job_name as input and returns a job object
        """
        self.executable = executable_str
        if write_input_funct is not None:
            self._write_input_funct = write_input_funct
        if collect_output_funct is not None:
            self._collect_output_funct = collect_output_funct
        if default_input_dict is not None:
            self.input.update(default_input_dict)

    def write_input(self):
        if self._write_input_funct is not None:
            self._write_input_funct(
                input_dict=self.input.to_builtin(),
                working_directory=self.working_directory,
            )

    def run_static(self):
        self.storage.output.stdout = super().run_static()

    def collect_output(self):
        if self._collect_output_funct is not None:
            self.output.update(
                self._collect_output_funct(working_directory=self.working_directory)
            )
            self.to_hdf()

    def to_dict(self):
        job_dict = super().to_dict()
        if self._write_input_funct is not None:
            job_dict["write_input_function"] = np.void(
                cloudpickle.dumps(self._write_input_funct)
            )
        if self._collect_output_funct is not None:
            job_dict["collect_output_function"] = np.void(
                cloudpickle.dumps(self._collect_output_funct)
            )
        return job_dict

    def from_dict(self, job_dict):
        super().from_dict(job_dict=job_dict)
        if "write_input_function" in job_dict.keys():
            self._write_input_funct = cloudpickle.loads(
                job_dict["write_input_function"]
            )
        if "write_input_function" in job_dict.keys():
            self._collect_output_funct = cloudpickle.loads(
                job_dict["collect_output_function"]
            )
