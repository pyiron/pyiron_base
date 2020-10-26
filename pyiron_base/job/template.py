# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
from pyiron_base.job.generic import GenericJob
from pyiron_base.generic.parameters import GenericParameters
from pyiron_base.generic.inputlist import InputList

"""
Template class to define jobs
"""

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
        self._input = InputList(table_name="input")

    @property
    def input(self):
        return self._input

    @input.setter
    def input(self, input):
        self._input = InputList(input, table_name="input")

    def to_hdf(self, hdf=None, group_name=None):
        super().to_hdf(
            hdf=hdf,
            group_name=group_name
        )
        self._input.to_hdf(hdf=self._hdf5, group_name=None)

    def from_hdf(self, hdf=None, group_name=None):
        super().from_hdf(
            hdf=hdf,
            group_name=group_name
        )
        self._input.from_hdf(hdf=self._hdf5, group_name=None)


class PythonTemplateJob(TemplateJob):
    def __init__(self, project, job_name):
        super().__init__(project, job_name)
        self._output = InputList(table_name="output")
        self._python_only_job = True

    @property
    def output(self):
        return self._output

    @output.setter
    def output(self, output):
        self._output = InputList(output, table_name="output")

    def from_hdf(self, hdf=None, group_name=None):
        super().from_hdf(
            hdf=hdf,
            group_name=group_name
        )
        self._output.from_hdf(hdf=self._hdf5, group_name=None)

    def to_hdf(self, hdf=None, group_name=None):
        super().to_hdf(
            hdf=hdf,
            group_name=group_name
        )
        self._output.to_hdf(hdf=self._hdf5, group_name=None)

    def save_output(self):
        self._output.to_hdf(hdf=self._hdf5, group_name=None)
        self.status.finished = True

    def _check_if_input_should_be_written(self):
        return False
