# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Template class to define jobs
"""

from pyiron_base.job.generic import GenericJob
from pyiron_base.generic.datacontainer import DataContainer

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


class TemplateJob(GenericJob):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self._data = DataContainer(table_name="data")
        self._data.create_group('input')
        self._data.create_group('output')

    @property
    def input(self):
        return self._data.input

    @property
    def output(self):
        return self._data.output

    def to_hdf(self, hdf=None, group_name=None):
        super().to_hdf(
            hdf=hdf,
            group_name=group_name
        )
        self._data.to_hdf(hdf=self.project_hdf5, group_name=None)

    def from_hdf(self, hdf=None, group_name=None):
        super().from_hdf(
            hdf=hdf,
            group_name=group_name
        )
        self._data.from_hdf(hdf=self.project_hdf5, group_name=None)


class PythonTemplateJob(TemplateJob):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self._python_only_job = True

    def _check_if_input_should_be_written(self):
        return False
