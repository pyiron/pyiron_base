# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Template class to define jobs
"""

from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.interfaces.object import HasStorage

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
    pyiron template job class for python codes.

    Example:

    >>> from pyiron_base import TemplateJob

    >>> class MyJob(TemplateJob):
    >>>     def __init__(self, project, job_name):
    >>>         super().__init__(project, job_name)
    >>>         self.executable = "touch test.dat"

    >>>     def write_input(self):
    >>>         pass

    >>>     def collect_output(self):
    >>>         pass

    You can store information you need in `job.input` (or `self.input`) and
    `job.output` (or `self.output`). The information assigned there will be
    automatically stored in the database after a successful run. You can write
    everything inside `run_static`, but optionally you can use the functions
    `def write_input(self)` and `def collect_output(self)`, whichare called
    before and after `run_static`, respectively.

    If you have a code which requires an executable, take a look at
    `TemplateJob` instead.

    """
    def __init__(self, project, job_name):
        GenericJob.__init__(self, project, job_name)
        HasStorage.__init__(self, group_name="")
        self.storage.create_group("input")
        self.storage.create_group("output")

    @property
    def input(self):
        return self.storage.input

    @property
    def output(self):
        return self.storage.output

    def to_hdf(self, hdf=None, group_name=None):
        GenericJob.to_hdf(self, hdf=hdf, group_name=group_name)
        HasStorage.to_hdf(self, hdf=self.project_hdf5)

    def from_hdf(self, hdf=None, group_name=None):
        GenericJob.from_hdf(self, hdf=hdf, group_name=group_name)
        HasStorage.from_hdf(self, hdf=self.project_hdf5)


class PythonTemplateJob(TemplateJob):
    """
    pyiron template job class for python codes.

    Example:

    >>> from pyiron_base import PythonTemplateJob

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
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self._python_only_job = True
        self._write_work_dir_warnings = False
