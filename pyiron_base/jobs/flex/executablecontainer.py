import cloudpickle
import numpy as np

from pyiron_base.jobs.job.runfunction import (
    CalculateFunctionCaller,
    write_input_files_from_input_dict,
)
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
        # Set job_with_calculate_function flag to true to use run_static() to execute the python function generated by
        # the job with its arguments job.get_calculate_function(**job.calculate_kwargs) without calling the old
        # interface with write_input() and collect_output(). Finally, the output dictionary is stored in the HDF5 file
        # using self.save_output(output_dict, shell_output)
        self._job_with_calculate_function = True

    @property
    def calculate_kwargs(self) -> dict:
        """
        Generate keyword arguments for the calculate() function.

        Example:

        >>> calculate_function = job.get_calculate_function()
        >>> shell_output, parsed_output, job_crashed = calculate_function(**job.calculate_kwargs)
        >>> job.save_output(output_dict=parsed_output, shell_output=shell_output)

        Returns:
            dict: keyword arguments for the calculate() function
        """
        kwargs = super().calculate_kwargs
        kwargs.update(
            {
                "input_parameter_dict": self.input.to_builtin(),
                "executable_script": self.executable.executable_path,
                "shell_parameter": True,
            }
        )
        return kwargs

    def set_job_type(
        self,
        executable_str: str,
        write_input_funct: callable = None,
        collect_output_funct: callable = None,
        default_input_dict: dict = None,
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

    def get_calculate_function(self) -> callable:
        """
        Generate calculate() function

        Example:

        >>> calculate_function = job.get_calculate_function()
        >>> shell_output, parsed_output, job_crashed = calculate_function(**job.calculate_kwargs)
        >>> job.save_output(output_dict=parsed_output, shell_output=shell_output)

        Returns:
            callable: calculate() functione
        """

        def get_combined_write_input_funct(input_job_dict, write_input_funct=None):
            def write_input_combo_funct(working_directory, input_dict):
                write_input_files_from_input_dict(
                    input_dict=input_job_dict,
                    working_directory=working_directory,
                )
                if write_input_funct is not None:
                    write_input_funct(
                        working_directory=working_directory,
                        input_dict=input_dict,
                    )

            return write_input_combo_funct

        return CalculateFunctionCaller(
            write_input_funct=get_combined_write_input_funct(
                input_job_dict=self.get_input_parameter_dict(),
                write_input_funct=self._write_input_funct,
            ),
            collect_output_funct=self._collect_output_funct,
        )

    def to_dict(self) -> dict:
        """
        Convert the job object to a dictionary representation.

        Returns:
            dict: A dictionary representation of the job object.
        """
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

    def from_dict(self, job_dict: dict):
        """
        Load the job attributes from a dictionary representation.

        Args:
            job_dict (dict): A dictionary containing the job attributes.

        """
        super().from_dict(job_dict=job_dict)
        if "write_input_function" in job_dict.keys():
            self._write_input_funct = cloudpickle.loads(
                job_dict["write_input_function"]
            )
        if "write_input_function" in job_dict.keys():
            self._collect_output_funct = cloudpickle.loads(
                job_dict["collect_output_function"]
            )
