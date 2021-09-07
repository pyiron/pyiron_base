from abc import ABC

from pyiron_base.job.factory import JobFactory


class Toolkit(ABC):
    def __init__(self, project):
        self._project = project


class BaseTools(Toolkit):
    def __init__(self, project):
        super().__init__(project)
        self._job = JobFactory(project)

    @property
    def job(self):
        return self._job
