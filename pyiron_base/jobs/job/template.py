# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Template class to define jobs
"""

from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.interfaces.object import HasStorage
from pyiron_base.jobs.job.jobtype import JobType

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
    def __init__(self, project, job_name):
        GenericJob.__init__(self, project, job_name)
        HasStorage.__init__(self)
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
        HasStorage.to_hdf(self, hdf=self.project_hdf5, group_name="")

    def from_hdf(self, hdf=None, group_name=None):
        GenericJob.from_hdf(self, hdf=hdf, group_name=group_name)
        HasStorage.from_hdf(self, hdf=self.project_hdf5, group_name="")


class PythonTemplateJob(TemplateJob):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self._python_only_job = True

    def _check_if_input_should_be_written(self):
        return False
