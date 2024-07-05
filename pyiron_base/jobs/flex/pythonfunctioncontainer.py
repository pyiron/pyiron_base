import inspect
from typing import Tuple

import cloudpickle
import numpy as np

from pyiron_base.jobs.job.generic import get_executor
from pyiron_base.jobs.job.template import PythonTemplateJob
from pyiron_base.project.delayed import get_function_parameter_dict, get_hash


class PythonCalculateFunctionCaller:
    __slots__ = ("_function", "_executor_type", "_cores")

    def __init__(
        self,
        funct: callable = None,
        executor_type: str = None,
        cores: int = 1,
    ):
        self._function = funct
        self._executor_type = executor_type
        self._cores = cores

    def __call__(
        self,
        *args,
        **kwargs,
    ) -> Tuple[str, dict, bool]:
        """
        Generic calculate function, which writes the input files into the working_directory, executes the
        executable_script and parses the output using the output_parameter_dict.

        Args:
            Arguments of the user defined function

        Returns:
            str, dict, bool: Tuple consisting of the shell output (str), the parsed output (dict) and a boolean flag if
                             the execution raised an accepted error.
        """
        if (
            self._executor_type is not None
            and "executor" in inspect.signature(self._function).parameters.keys()
        ):
            if "executor" in kwargs.keys():
                del kwargs["executor"]
            with get_executor(
                executor_type=self._executor_type, max_workers=self._cores
            ) as exe:
                result = self._function(*args, executor=exe, **kwargs)
        else:
            result = self._function(*args, **kwargs)
        return None, {"result": result}, False


class PythonFunctionContainerJob(PythonTemplateJob):
    """
    The PythonFunctionContainerJob is designed to wrap any kind of python function into a pyiron job object

    Example:

    >>> def test_function(a, b=8):
    >>>     return a+b
    >>>
    >>> from pyiron_base import Project
    >>> pr = Project("test")
    >>> job = pr.wrap_python_function(test_function)
    >>> job.input["a"] = 4
    >>> job.input["b"] = 5
    >>> job.run()
    >>> job.output
    >>>
    >>> test_function_wrapped = pr.wrap_python_function(test_function)
    >>> test_function_wrapped(4, b=6)
    """

    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self._function = None
        self._executor_type = None
        self._automatically_rename_on_save_using_input = False
        # Automatically rename job using function and input values at save time
        # This is useful for the edge case where these jobs are created from a wrapper
        # and automatically assigned a name based on the function name, but multiple
        # jobs are created from the same function (and thus distinguished only by their
        # input)

    @property
    def python_function(self):
        return self._function

    @python_function.setter
    def python_function(self, funct):
        self.input.update(get_function_parameter_dict(funct=funct))
        self._function = funct

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
        return self.input.to_builtin()

    def get_calculate_function(self):
        """
        Generate calculate() function

        Example:

        >>> calculate_function = job.get_calculate_function()
        >>> shell_output, parsed_output, job_crashed = calculate_function(**job.calculate_kwargs)
        >>> job.save_output(output_dict=parsed_output, shell_output=shell_output)

        Returns:
            callable: calculate() functione
        """
        return PythonCalculateFunctionCaller(
            funct=self._function,
            executor_type=self._executor_type,
            cores=self.server.cores,
        )

    def set_input(self, *args, **kwargs):
        self.input.update(
            inspect.signature(self._function).bind(*args, **kwargs).arguments
        )

    def __call__(self, *args, **kwargs):
        self.set_input(*args, **kwargs)
        self.run()
        return self.output["result"]

    def to_dict(self):
        job_dict = super().to_dict()
        job_dict["function"] = np.void(cloudpickle.dumps(self._function))
        job_dict["_automatically_rename_on_save_using_input"] = (
            self._automatically_rename_on_save_using_input
        )
        return job_dict

    def from_dict(self, job_dict):
        super().from_dict(job_dict=job_dict)
        self._function = cloudpickle.loads(job_dict["function"])
        self._automatically_rename_on_save_using_input = bool(
            job_dict["_automatically_rename_on_save_using_input"]
        )

    def save(self):
        """
        Automatically rename job using function and input values at save time. This is useful for the edge case where
        these jobs are created from a wrapper and automatically assigned a name based on the function name, but multiple
        jobs are created from the same function (and thus distinguished only by their input).
        """
        if self._automatically_rename_on_save_using_input:
            self.job_name = self.job_name + get_hash(
                binary=cloudpickle.dumps(
                    {"fn": self._function, "kwargs": self.input.to_builtin()}
                )
            )

        if self.job_name in self.project.list_nodes():
            self.from_hdf()
            self.status.finished = True
            return  # Without saving
        super().save()
