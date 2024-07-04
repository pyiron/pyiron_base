# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Factories for creating jobs, which may already exist in the database/storage.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Type, Union

from pyiron_base.interfaces.factory import PyironFactory
from pyiron_base.jobs.datamining import TableJob
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.jobs.job.jobtype import JobType
from pyiron_base.jobs.master.flexible import FlexibleMaster
from pyiron_base.jobs.script import ScriptJob
from pyiron_base.project.generic import Project
from pyiron_base.state import state
from pyiron_base.storage.hdfio import ProjectHDFio

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
    def _job_class_dict(self) -> Dict:
        pass

    def __dir__(self) -> List:
        """
        Enable autocompletion by overwriting the __dir__() function.
        """
        return list(self._job_class_dict.keys())

    def __getattr__(self, name) -> Type[GenericJob]:
        if name in self._job_class_dict.keys():

            def wrapper(
                job_name: str,
                delete_existing_job: bool = False,
                delete_aborted_job: bool = False,
            ) -> GenericJob:
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
                    class_name=name,  # Pass the class directly, JobType can handle that
                    project=ProjectHDFio(
                        project=self._project.copy(), file_name=job_name
                    ),
                    job_name=job_name,
                    job_class_dict=self._job_class_dict,
                    delete_existing_job=delete_existing_job,
                    delete_aborted_job=delete_aborted_job,
                )

            return wrapper
        else:
            raise AttributeError("no job class named '{}' defined".format(name))

    def __call__(
        self,
        job_type: Union[str, Type[GenericJob]],
        job_name: str,
        delete_existing_job: bool = False,
        delete_aborted_job: bool = False,
    ) -> GenericJob:
        """
        Create a job.

        Args:
            job_type (str|Type[GenericJob]): The job class to be instantiated, either the string from a known class, or
                an actual class, e.g. in the case of custom user-made jobs.
            job_name (str): name of the job.
            delete_existing_job (bool): delete an existing job. (Default is False.)
            delete_aborted_job (bool): delete an existing and aborted job. (Default is False.)

        Returns:
            GenericJob: job object depending on the job_type selected
        """
        job = JobType(
            class_name=job_type,  # Pass the class directly, JobType can handle that
            project=ProjectHDFio(project=self._project.copy(), file_name=job_name),
            job_name=job_name,
            job_class_dict=self._job_class_dict,
            delete_existing_job=delete_existing_job,
            delete_aborted_job=delete_aborted_job,
        )
        if state.settings.login_user is not None:
            job.user = state.settings.login_user
        return job


class JobFactory(JobFactoryCore):
    @property
    def _job_class_dict(self) -> Dict:
        return {
            "FlexibleMaster": FlexibleMaster,
            "ScriptJob": ScriptJob,
            "TableJob": TableJob,
        }
