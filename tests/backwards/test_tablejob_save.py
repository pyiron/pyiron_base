from pyiron_base import Project, PythonTemplateJob, __version__

class ToyJob(PythonTemplateJob):
    def __init__(self, project, job_name):
        super(ToyJob, self).__init__(project, job_name)
        self.input['input_energy'] = 100

    def run_static(self):
        with self.project_hdf5.open("output/generic") as h5out:
            h5out["energy_tot"] = self.input["input_energy"]
        self.status.finished = True

pr = Project(
            "tests/static/backwards/V{}".format(__version__).replace(".", "_")
)
job = pr.create_job(pr.job_type.ToyJob, "toy1")
job.input["input_energy"] = 42
job.run()
job = pr.create_job(pr.job_type.ToyJob, "toy2")
job.input["input_energy"] = 23
job.run()
tab = pr.create_table("toy_table")
tab.add['name'] = lambda j: j.name
tab.add['array'] = lambda j: np.arange(8)
tab.run()
