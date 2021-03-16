
from pyiron_base import PythonTemplateJob


class ToyJob(PythonTemplateJob):
    def __init__(self, project, job_name):
        """A toyjob which can be run() to test functionalities."""
        super(ToyJob, self).__init__(project, job_name)
        self.input['input_energy'] = 100

    # This function is executed
    def run_static(self):
        with self.project_hdf5.open("output/generic") as h5out:
            h5out["energy_tot"] = self.input["input_energy"]
        self.status.finished = True