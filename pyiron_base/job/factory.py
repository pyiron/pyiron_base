# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Factories for creating jobs, which may already exist in the database/storage.
"""

from pyiron_base.project.generic import Project
from pyiron_base.generic.factory import PyironFactory
from abc import ABC, abstractmethod
from pyiron_base.job.jobtype import JobType
from pyiron_base.job.generic import GenericJob
from typing import Type

from pyiron_base.master.flexible import FlexibleMaster
from pyiron_base.job.script import ScriptJob
from pyiron_base.master.serial import SerialMasterBase
from pyiron_base.table.datamining import TableJob
from pyiron_base.generic.hdfio import ProjectHDFio
from pyiron_base.settings.generic import Settings
s = Settings()

__author__ = "Liam Huber, Jan Janssen"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "production"
__date__ = "Sep 7, 2021"


class JobFactoryCore(PyironFactory, ABC):
    def __init__(self, project: Project):
        self._project = project

    @property
    @abstractmethod
    def _job_class_dict(self):
        pass

    def __dir__(self):
        """
        Enable autocompletion by overwriting the __dir__() function.
        """
        return list(self._job_class_dict.keys())

    def __getattr__(self, name):
        if name in self._job_class_dict.keys():
            def wrapper(job_name, delete_existing_job=False, delete_aborted_job=False) -> Type[GenericJob]:
                """
                Create a job.

                Args:
                    job_name (str): name of the job
                    delete_existing_job (bool): delete an existing job - default false
                    delete_aborted_job (bool): delete an existing and aborted job - default false

                Returns:
                    GenericJob: job object depending on the job_type selected
                """
                return JobType(
                    class_name=self._job_class_dict[name],  # Pass the class directly, JobType can handle that
                    project=ProjectHDFio(project=self._project.copy(), file_name=job_name),
                    job_name=job_name,
                    job_class_dict=self._job_class_dict,
                    delete_existing_job=delete_existing_job,
                    delete_aborted_job=delete_aborted_job
                )
            return wrapper
        else:
            raise AttributeError("no job class named '{}' defined".format(name))

    def __call__(self, job_type, job_name, delete_existing_job=False, delete_aborted_job=False):
        """
        Create a job.

        Args:
            job_type (str): The job class to be instantiated.
            job_name (str): name of the job
            delete_existing_job (bool): delete an existing job - default false
            delete_aborted_job (bool): delete an existing and aborted job - default false

        Returns:
            GenericJob: job object depending on the job_type selected
        """
        job_name = job_name.replace(".", "_")
        job = JobType(
            class_name=self._job_class_dict[job_type],  # Pass the class directly, JobType can handle that
            project=ProjectHDFio(project=self._project.copy(), file_name=job_name),
            job_name=job_name,
            job_class_dict=self._job_class_dict,
            delete_existing_job=delete_existing_job,
            delete_aborted_job=delete_aborted_job
        )
        if s.login_user is not None:
            job.user = s.login_user
        return job


class JobFactory(JobFactoryCore):
    @property
    def _job_class_dict(self) -> dict:
        return {
            "FlexibleMaster": FlexibleMaster,
            "ScriptJob": ScriptJob,
            "SerialMasterBase": SerialMasterBase,
            "TableJob": TableJob
        }
