import inspect
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import cloudpickle
import numpy as np

from pyiron_base.jobs.job.template import PythonTemplateJob
from pyiron_base.project.delayed import get_function_parameter_dict, get_hash
from pyiron_base.state import state
from pyiron_base.storage.datacontainer import DataContainer


@contextmanager
def set_directory(path: Path):
    """Sets the cwd within the context

    Args:
        path (Path): The path to the cwd

    Yields:
        None
    """

    origin = Path().absolute()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(origin)


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
        self._execute_in_working_directory = False
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
    def python_function(self, funct: callable) -> None:
        """
        Set the python function for the job and update the input dictionary.

        Args:
            funct (callable): The python function to be wrapped.
        """
        self.input.update(get_function_parameter_dict(funct=funct))
        self._function = funct

    @property
    def execute_in_working_directory(self) -> bool:
        return self._execute_in_working_directory

    @execute_in_working_directory.setter
    def execute_in_working_directory(self, val: bool) -> None:
        self._execute_in_working_directory = bool(val)

    def set_input(self, *args, **kwargs):
        """
        Sets the input arguments for the job.python_function function.

        Args:
            *args: Positional arguments to be passed to the function.
            **kwargs: Keyword arguments to be passed to the function.
        """
        self.input.update(
            DataContainer(
                inspect.signature(self._function).bind(*args, **kwargs).arguments
            ).to_builtin()
        )

    def __call__(self, *args, **kwargs):
        self.set_input(*args, **kwargs)
        self.run()
        return self.output["result"]

    def _to_dict(self) -> dict:
        """
        Convert the job object to a dictionary representation.

        Returns:
            dict: The dictionary representation of the job object.
        """
        job_dict = super()._to_dict()
        job_dict["function"] = np.void(cloudpickle.dumps(self._function))
        job_dict["execute_in_working_directory"] = self._execute_in_working_directory
        job_dict["_automatically_rename_on_save_using_input"] = (
            self._automatically_rename_on_save_using_input
        )
        return job_dict

    def _from_dict(self, obj_dict: dict, version: Optional[str] = None) -> None:
        """
        Load the job object from a dictionary representation.

        Args:
            obj_dict (dict): The dictionary representation of the job object.
            version (str): The version of the job object.
        """
        super()._from_dict(obj_dict=obj_dict)
        self._function = cloudpickle.loads(obj_dict["function"])
        self._automatically_rename_on_save_using_input = bool(
            obj_dict["_automatically_rename_on_save_using_input"]
        )
        if "execute_in_working_directory" in obj_dict:
            self._execute_in_working_directory = bool(
                obj_dict["execute_in_working_directory"]
            )

    def save(self) -> None:
        """
        Save the job to the project.

        If `self._automatically_rename_on_save_using_input` is True, the job name will be automatically renamed by appending
        a hash generated from the function and input arguments.

        If the job name already exists in the project, the job will be loaded from the HDF5 file and marked as finished without saving.

        Returns:
            None
        """
        if self._automatically_rename_on_save_using_input:
            self.job_name = (
                self.job_name
                + "_"
                + get_hash(
                    binary=cloudpickle.dumps(
                        {"fn": self._function, "kwargs": self.input.to_builtin()}
                    )
                )
            )

        if self.job_name in self.project.list_nodes():
            self.from_hdf()
            self.status.finished = True
            return  # Without saving
        super().save()

    def run_static(self) -> None:
        """
        Run the static function.

        If an executor is specified and the function signature contains an 'executor' parameter,
        the function is executed using the specified executor. Otherwise, the function is executed
        without an executor.
        """
        self.status.running = True
        try:
            if (
                self._executor_type is not None
                and "executor" in inspect.signature(self._function).parameters.keys()
            ):
                input_dict = self.input.to_builtin()
                del input_dict["executor"]
                with self._get_executor(max_workers=self.server.cores) as exe:
                    output = self._function(**input_dict, executor=exe)
            else:
                if self._execute_in_working_directory:
                    self._create_working_directory()
                    with set_directory(Path(self.working_directory)):
                        output = self._function(**self.input)
                else:
                    output = self._function(**self.input)
        except Exception as e:
            self._logger.warning("Job aborted")
            self.status.aborted = True
            self._hdf5["status"] = self.status.string
            self.run_time_to_db()
            if self.server.run_mode.non_modal:
                state.database.close_connection()
            self.output.update({"error": e})
            self.to_hdf()
        else:
            self.output.update(DataContainer({"result": output}).to_builtin())
            self.to_hdf()
            self.status.finished = True
