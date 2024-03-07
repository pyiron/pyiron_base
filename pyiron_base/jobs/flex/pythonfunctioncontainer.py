import inspect
import hashlib
import re
import cloudpickle
import numpy as np
from pyiron_base.jobs.job.template import PythonTemplateJob


def get_function_parameter_dict(funct):
    return {
        k: None if v.default == inspect._empty else v.default
        for k, v in inspect.signature(funct).parameters.items()
    }


def get_hash(binary):
    # Remove specification of jupyter kernel from hash to be deterministic
    binary_no_ipykernel = re.sub(b"(?<=/ipykernel_)(.*)(?=/)", b"", binary)
    return str(hashlib.md5(binary_no_ipykernel).hexdigest())


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

    def __call__(self, *args, **kwargs):
        self.input.update(
            inspect.signature(self._function).bind(*args, **kwargs).arguments
        )
        self.run()
        return self.output["result"]

    def to_hdf(self, hdf=None, group_name=None):
        super().to_hdf(hdf=hdf, group_name=group_name)
        self.project_hdf5["function"] = np.void(cloudpickle.dumps(self._function))
        self.project_hdf5["_automatically_rename_on_save_using_input"] = (
            self._automatically_rename_on_save_using_input
        )

    def from_hdf(self, hdf=None, group_name=None):
        super().from_hdf(hdf=hdf, group_name=group_name)
        self._function = cloudpickle.loads(self.project_hdf5["function"])
        self._automatically_rename_on_save_using_input = bool(
            self.project_hdf5["_automatically_rename_on_save_using_input"]
        )

    def save(self):
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

    def run_static(self):
        if (
            self._executor_type is not None
            and "executor" in inspect.signature(self._function).parameters.keys()
        ):
            input_dict = self.input.to_builtin()
            del input_dict["executor"]
            output = self._function(
                **input_dict, executor=self._get_executor(max_workers=self.server.cores)
            )
        else:
            output = self._function(**self.input.to_builtin())
        self.output.update({"result": output})
        self.to_hdf()
        self.status.finished = True
