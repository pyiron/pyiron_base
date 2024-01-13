import cloudpickle
import numpy as np
from pyiron_base.jobs.job.template import TemplateJob


class ExecutableContainerJob(TemplateJob):
    """
    The ExecutableContainerJob is designed to wrap any kind of external executable into a pyiron job object by providing
    a write_input(input_dict, working_directory) and a collect_output(working_directory) function.

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
    >>> from pyiron_base import Project
    >>> pr = Project("test")
    >>> pr.create_job_class(
    >>>     class_name="CatJob",
    >>>     write_input_funct=write_input,
    >>>     collect_output_funct=collect_output,
    >>>     default_input_dict={"energy": 1.0},
    >>>     executable_str="cat input_file > output_file",
    >>> )
    >>> job = self.project.create.job.CatJob(job_name="job_test")
    >>> job.input["energy"] = 2.0
    >>> job.run()
    >>> job.output

    """

    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self._write_input_funct = None
        self._collect_output_funct = None

    def set_job_type(
        self,
        write_input_funct,
        collect_output_funct,
        default_input_dict,
        executable_str,
    ):
        """
        Set the pre-defined write_input() and collect_output() function plus a dictionary of default inputs and an
        executable string.

        Args:
            write_input_funct (callable): The write input function write_input(input_dict, working_directory)
            collect_output_funct (callable): The collect output function collect_output(working_directory)
            default_input_dict (dict): Default input for the newly created job class
            executable_str (str): Call to an external executable
        """
        self.input.update(default_input_dict)
        self._write_input_funct = write_input_funct
        self._collect_output_funct = collect_output_funct
        self.executable = executable_str

    def write_input(self):
        self._write_input_funct(
            input_dict=self.input.to_builtin(), working_directory=self.working_directory
        )

    def collect_output(self):
        self.output.update(
            self._collect_output_funct(working_directory=self.working_directory)
        )
        self.to_hdf()

    def to_hdf(self, hdf=None, group_name=None):
        super().to_hdf(hdf=hdf, group_name=group_name)
        if self._write_input_funct is not None:
            self.project_hdf5["write_input_function"] = np.void(
                cloudpickle.dumps(self._write_input_funct)
            )
        if self._collect_output_funct is not None:
            self.project_hdf5["collect_output_function"] = np.void(
                cloudpickle.dumps(self._collect_output_funct)
            )

    def from_hdf(self, hdf=None, group_name=None):
        super().from_hdf(hdf=hdf, group_name=group_name)
        self._write_input_funct = cloudpickle.loads(
            self.project_hdf5["write_input_function"]
        )
        self._collect_output_funct = cloudpickle.loads(
            self.project_hdf5["collect_output_function"]
        )
