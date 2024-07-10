# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Builtin tools that come with pyiron base.
"""

from abc import ABC

from pyiron_base.jobs.job.factory import JobFactory

__author__ = "Liam Huber"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "production"
__date__ = "Sep 7, 2021"


class Toolkit(ABC):
    def __init__(self, project):
        self._project = project


class BaseTools(Toolkit):
    def __init__(self, project):
        super().__init__(project)
        self._job = JobFactory(project)

    @property
    def job(self) -> JobFactory:
        return self._job
