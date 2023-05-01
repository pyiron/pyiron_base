from pyiron_base.jobs.job.generic import GenericJob


class FunctionContainer(GenericJob):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self._python_only_job = True

    def run_static(self):
        for kwargs in self.input:
            self.function(**kwargs)

    def to_hdf(self, hdf=None, group_name=None):
        pass