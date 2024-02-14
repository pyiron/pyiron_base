from pyiron_base.jobs.job.generic import GenericJob


class JobWithExecutor(GenericJob):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self._executor_type = None

    @property
    def executor_type(self):
        return self._executor_type

    @executor_type.setter
    def executor_type(self, exe):
        self._executor_type = exe

    def _get_executor(self, max_workers=None):
        if self._executor_type is None:
            raise ValueError(
                "No executor type defined - Please set self.executor_type."
            )
        elif (
            isinstance(self._executor_type, str)
            and self.executor_type == "ProcessPoolExecutor"
        ):
            from concurrent.futures import ProcessPoolExecutor

            return ProcessPoolExecutor(max_workers=max_workers)
        elif (
            isinstance(self._executor_type, str)
            and self.executor_type == "ThreadPoolExecutor"
        ):
            from concurrent.futures import ThreadPoolExecutor

            return ThreadPoolExecutor(max_workers=max_workers)
        elif isinstance(self._executor_type, str):
            raise TypeError(
                "Unknown Executor Type: Please select either ProcessPoolExecutor or ThreadPoolExecutor."
            )
        else:
            raise TypeError("The self.executor_type has to be a string.")

    def _executor_type_to_hdf(self):
        if self._executor_type is not None:
            self.project_hdf5["executor_type"] = self._executor_type

    def _executor_type_from_hdf(self):
        if "executor_type" in self.project_hdf5.list_nodes():
            self._executor_type = self.project_hdf5["executor_type"]

    def to_hdf(self, hdf=None, group_name=None):
        super().to_hdf(hdf=hdf, group_name=group_name)
        self._executor_type_to_hdf()

    def from_hdf(self, hdf=None, group_name=None):
        super().from_hdf(hdf=hdf, group_name=group_name)
        self._executor_type_from_hdf()
