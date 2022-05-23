# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Template class to define jobs
"""

from pyiron_base.job.generic import GenericJob
from pyiron_base.generic.object import HasStorage
from pyiron_base.generic.datacontainer import DataContainer
from typing import Type

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
        self.storage.input = self._input_class(table_name="input")
        self.storage.output = self._output_class(table_name="output")

    @property
    def _input_class(self) -> Type[DataContainer]:
        """Children can overwrite this method to return some other child of `DataContainer` for custom behaviour"""
        return DataContainer

    @property
    def _output_class(self) -> Type[DataContainer]:
        """Children can overwrite this method to return some other child of `DataContainer` for custom behaviour"""
        return DataContainer

    @property
    def input(self) -> Type[DataContainer]:
        return self.storage.input

    @input.setter
    def input(self, new_input):
        raise AttributeError(
            """
            Input cannot be overwritten; to get custom input behaviour, override the `_input_class` property to return 
            your own custom class (which inherits from `DataContainer`).
            
            Example:
            >>> class MyInput(DataContainer):
            >>>     def __init__(self, init=None, table_name=None, lazy=False, wrap_blacklist=()):
            >>>         super().__init__(init=init, table_name=table_name, lazy=lazy, wrap_blacklist=wrap_blacklist)
            >>>         self.foo = 42
            >>>
            >>> class MyJob(TemplateJob):
            >>>     @property
            >>>     def _input_class(self) -> MyInput:
            >>>         return MyInput
            """
        )

    @property
    def output(self) -> Type[DataContainer]:
        return self.storage.output

    @output.setter
    def output(self, new_output):
        raise AttributeError(
            """
            Output cannot be overwritten; to get custom output behaviour, override the `_output_class` property to return 
            your own custom class (which inherits from `DataContainer`).

            Example:
            >>> class MyOutput(DataContainer):
            >>>     def __init__(self, init=None, table_name=None, lazy=False, wrap_blacklist=()):
            >>>         super().__init__(init=init, table_name=table_name, lazy=lazy, wrap_blacklist=wrap_blacklist)
            >>>         self.bar = 'towel'
            >>>
            >>> class MyJob(TemplateJob):
            >>>     @property
            >>>     def _output_class(self) -> MyOutput:
            >>>         return MyOutput
            """
        )

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
