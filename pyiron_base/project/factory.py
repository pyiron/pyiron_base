# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Creator to create pyiron objects from existing objects
"""
from pyiron_base.job.jobtype import JobType, JobTypeChoice

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Nov 17, 2020"


class Creator(object):
    def __init__(self, project):
        self._job_creator = JobCreator(project=project)

    @property
    def job(self):
        return self._job_creator


class JobCreator(object):
    def __init__(self, project):
        self._job_type = JobTypeChoice()
        self._project = project

    def __dir__(self):
        return dir(self._job_type)

    def __getattr__(self, name):
        if name in self._job_type.job_class_dict.keys():
            def wrapper(job_name, delete_existing_job=False):
                """
                Create one of the following jobs:
                - 'ExampleJob': example job just generating random number
                - 'SerialMaster': series of jobs run in serial
                - 'ParallelMaster': series of jobs run in parallel
                - 'ScriptJob': Python script or jupyter notebook job container
                - 'ListMaster': list of jobs

                Args:
                    job_name (str): name of the job
                    delete_existing_job (bool): delete an existing job - default false

                Returns:
                    GenericJob: job object depending on the job_type selected
                """
                job = JobClass(
                    class_name=name,
                    project=self._project,
                    job_class_dict=self._job_type.job_class_dict
                )
                return job.create(job_name=job_name, delete_existing_job=delete_existing_job)
            return wrapper
        else:
            raise AttributeError("no job class named '{}' defined".format(name))


class JobClass(object):
    def __init__(self, class_name, project, job_class_dict):
        self._class_name = class_name
        self._project = project
        self._job_class_dict = job_class_dict

    def create(self, job_name, delete_existing_job=False):
        return JobType(
            class_name=self._class_name,
            project=self._project,
            job_name=job_name,
            job_class_dict=self._job_class_dict,
            delete_existing_job=delete_existing_job
        )
