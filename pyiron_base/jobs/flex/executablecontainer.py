import cloudpickle
import numpy as np
from pyiron_base.jobs.job.template import TemplateJob


class ExecutableJobContainer(TemplateJob):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self._write_input_funct = None
        self._collect_output_funct = None

    def set_job_type(
        self,
        class_name,
        write_input_funct,
        collect_output_funct,
        default_input_dict,
        executable_str,
    ):
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
