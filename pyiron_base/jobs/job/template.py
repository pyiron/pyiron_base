# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Template class to define jobs
"""

from typing import Optional

from pyiron_base.interfaces.object import HasStorage
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.storage.datacontainer import DataContainer

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "development"
__date__ = "May 15, 2020"


class TemplateJob(GenericJob, HasStorage):
    """
    pyiron template job class for codes with an external executable.

    Example:

    >>> from pyiron_base import TemplateJob, Project

    >>> class MyJob(TemplateJob):
    >>>     def __init__(self, project, job_name):
    >>>         super().__init__(project, job_name)
    >>>         job.input.message = "hello!"
    >>>         self.executable = "cat input.dat > output.dat"

    >>>     def write_input(self):
    >>>         with open(self.working_directory + "/input.dat", "w") as f:
    >>>             f.write(job.input.message)

    >>>     def collect_output(self):
    >>>         with open(self.working_directory + "/output.dat", "w") as f:
    >>>             job.output.message = f.read()
    >>>         job.to_hdf()

    >>> pr = Project("my_project")
    >>> job = pr.create_job(MyJob, "my_job")
    >>> job.run()

    You can store information you need in `job.input` (or `self.input`) and
    `job.output` (or `self.output`). The information assigned there will be
    automatically stored in the database after a successful run. You can write
    everything inside `run_static`, but optionally you can use the functions
    `def write_input(self)` and `def collect_output(self)`, which are called
    before and after `run_static`, respectively.

    Important: The job runs in the working directory of the pyiron job. In the
    example above, it is placed under `my_project/my_job_hdf5/my_job/`. It is
    therefore important to use the absolute path, or `self.working_directory`
    to make sure that the files are found correctly

    If you have a code which requires an executable, take a look at
    :class:`~.TemplateJob` instead.

    """

    def __init__(
        self, project: "pyiron_base.storage.hdfio.ProjectHDFio", job_name: str
    ):
        GenericJob.__init__(self, project, job_name)
        HasStorage.__init__(self, group_name="")
        self.storage.create_group("input")
        self.storage.create_group("output")

    @property
    def input(self) -> DataContainer:
        return self.storage.input

    @property
    def output(self) -> DataContainer:
        return self.storage.output

    def _to_dict(self) -> dict:
        """
        Convert the job object to a dictionary.

        Returns:
            dict: A dictionary representation of the job object.
        """
        job_dict = super()._to_dict()
        job_dict["input/data"] = self.storage.input.to_dict()
        return job_dict

    def _from_dict(self, obj_dict: dict, version: str = None):
        """
        Update the object attributes from a dictionary representation.

        Args:
            obj_dict (dict): The dictionary containing the object attributes.
            version (str, optional): The version of the dictionary format. Defaults to None.
        """
        super()._from_dict(obj_dict=obj_dict, version=version)
        input_dict = obj_dict["input"]
        if "data" in input_dict.keys():
            self.storage.input.update(input_dict["data"])

    def to_hdf(
        self,
        hdf: Optional["pyiron_base.storage.hdfio.ProjectHDFio"] = None,
        group_name: Optional[str] = None,
    ) -> None:
        GenericJob.to_hdf(self=self, hdf=hdf, group_name=group_name)
        HasStorage.to_hdf(self=self, hdf=self.project_hdf5)

    def from_hdf(
        self,
        hdf: Optional["pyiron_base.storage.hdfio.ProjectHDFio"] = None,
        group_name: Optional[str] = None,
    ) -> None:
        GenericJob.from_hdf(self=self, hdf=hdf, group_name=group_name)
        HasStorage.from_hdf(self=self, hdf=self.project_hdf5)


class PythonTemplateJob(TemplateJob):
    """
    pyiron template job class for python codes.

    Example:

    >>> from pyiron_base import PythonTemplateJob, Project

    >>> class ToyJob(PythonTemplateJob):  # Create a custom job class
    >>>     def __init__(self, project, job_name):
    >>>         super().__init__(project, job_name)
    >>>         self.input.energy = 100  # Define default input

    >>>     def run_static(self):  # Call a python function and store stuff in the output
    >>>         self.output.double = self.input.energy * 2
    >>>         self.status.finished = True
    >>>         self.to_hdf()

    >>> job = pr.create_job(job_type=ToyJob, job_name="toy")  # Create job instance
    >>> job.run()  # Execute Custom job class

    You can store information you need in `job.input` (or `self.input`) and
    `job.output` (or `self.output`). The information assigned there will be
    automatically stored in the database after a successful run. You can write
    everything inside `run_static`, but optionally you can use the functions
    `def write_input(self)` and `def collect_output(self)`, whichare called
    before and after `run_static`, respectively.

    If you have a code which requires an executable, take a look at
    `TemplateJob` instead.

    """

    def __init__(
        self, project: "pyiron_base.storeage.hdfio.ProjectHDFio", job_name: str
    ):
        super().__init__(project, job_name)
        self._job_with_calculate_function = True
        self._write_work_dir_warnings = False
