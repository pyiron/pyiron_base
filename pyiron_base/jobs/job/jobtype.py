# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Jobtype class to create GenericJob type objects
"""

import importlib
import inspect
import os
from typing import Optional, Union

from pyiron_snippets.singleton import Singleton

from pyiron_base.interfaces.factory import PyironFactory
from pyiron_base.jobs.job.extension.jobstatus import job_status_finished_lst
from pyiron_base.jobs.job.util import _get_safe_job_name
from pyiron_base.storage.hdfio import ProjectHDFio

__author__ = "Joerg Neugebauer, Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


JOB_CLASS_DICT = {
    "FlexibleMaster": "pyiron_base.jobs.master.flexible",
    "ScriptJob": "pyiron_base.jobs.script",
    "TableJob": "pyiron_base.jobs.datamining",
    "WorkerJob": "pyiron_base.jobs.worker",
    "ExecutableContainerJob": "pyiron_base.jobs.flex.executablecontainer",
    "PythonFunctionContainerJob": "pyiron_base.jobs.flex.pythonfunctioncontainer",
}


class JobType:
    """
    The JobTypeBase class creates a new object of a given class type.
    """

    _job_class_dict = JOB_CLASS_DICT

    def __new__(
        cls,
        class_name: Union[type, str],
        project: ProjectHDFio,
        job_name: str,
        job_class_dict: Optional[dict] = None,
        delete_existing_job: bool = False,
        delete_aborted_job: bool = False,
    ):
        """
        The __new__() method allows to create objects from other classes - the class selected by class_name

        Args:
            class_name (str/Type('GenericJob')): The specific class name of the class this object belongs to.
            project (Project): Project object (defines path where job will be created and stored)
            job_name (str): name of the job (must be unique within this project path)
            job_class_dict (dict): dictionary with the jobtypes to choose from.
            delete_existing_job (bool): delete an existing job - default false
            delete_aborted_job (bool): delete an existing and aborted job - default false

        Returns:
            GenericJob: object of type class_name
        """
        if not isinstance(delete_existing_job, bool):
            raise ValueError(
                f"We got delete_existing_job = {delete_existing_job}. If you"
                " meant to delete the job, set delete_existing_job = True"
            )
        job_name = _get_safe_job_name(job_name)
        cls.job_class_dict = job_class_dict or cls._job_class_dict
        if isinstance(class_name, str):
            job_class = cls.convert_str_to_class(
                job_class_dict=cls.job_class_dict, class_name=class_name
            )
        elif inspect.isclass(class_name):
            if (
                class_name.__name__ not in cls._job_class_dict
                or cls._convert_pyiron_to_pyiron_atomistics_module(
                    class_name.__module__
                )
                == cls._job_class_dict[class_name.__name__]
            ):
                job_class = class_name
            else:
                raise ValueError(
                    f"You are trying to instantiate a job with the class {class_name} named {class_name.__name__} "
                    f"from module {class_name.__module__}, however, this name has already been registered with the"
                    f" module {cls._job_class_dict[class_name.__name__]}."
                )
        else:
            raise TypeError()
        job = job_class(project, job_name)
        if job.job_id is not None and not os.path.exists(job.project_hdf5.file_name):
            job.logger.warning(
                "No HDF5 file found - remove database entry and create new job! {}".format(
                    job.job_name
                )
            )
            delete_existing_job = True
        if delete_existing_job or (job.status.aborted and delete_aborted_job):
            job.remove()
            job = job_class(project, job_name)
        if job.status.aborted:
            job.logger.warning(
                "Job aborted - please remove it and run again! {}".format(job.job_name)
            )
        if not job.status.initialized:
            job.from_hdf()
        if job.status.string in job_status_finished_lst:
            job.set_input_to_read_only()
        return job

    @classmethod
    def unregister(cls, job_name_or_class: Union[str, type]) -> Optional[type]:
        """Unregister job type from the exposed list of available job types

        Args:
            job_name_or_class(str/type): name of the job or job class

        Returns:
            None if a str is provided as job_name_or_class
            job_name_or_class if a class is provided. Therefore, this method can be used as class decorator.
        """
        _cls = None
        if isinstance(job_name_or_class, type):
            _cls = job_name_or_class
            job_name_or_class = job_name_or_class.__name__
        if job_name_or_class in cls._job_class_dict:
            del cls._job_class_dict[job_name_or_class]
        else:
            raise KeyError(f"No JobType with name '{job_name_or_class}' found.")
        return _cls

    @staticmethod
    def _convert_pyiron_to_pyiron_atomistics_module(cls_module_str: str) -> str:
        if cls_module_str.startswith("pyiron."):
            # Currently, we set all sub-modules of pyiron_atomistics to be sub-modules of pyiron. Thus, any class
            # pyiron.submodule.PyironClass is identical to pyiron_atomistics_submodule.PyironClass:
            cls_module_str = cls_module_str.replace("pyiron.", "pyiron_atomistics.")
        return cls_module_str

    @classmethod
    def register(
        cls,
        job_class_or_module_str: Union[type, str],
        job_name: Optional[str] = None,
        overwrite: bool = False,
    ) -> None:
        """Register job type from the exposed list of available job types

        Args:
            job_class_or_module_str(type/str/None): job class itself, string representation of the job class module as
                provided by cls.__module__, or None for do not register.
            job_name(str/None): Name of the job to register. Must match cls.__name__. Can be omitted for class input.
            overwrite(bool): If True, overwrite existing job type.
        """
        if job_class_or_module_str is None:
            return
        elif isinstance(job_class_or_module_str, type):
            cls_module_str = cls._convert_pyiron_to_pyiron_atomistics_module(
                job_class_or_module_str.__module__
            )
            if job_name is not None and job_class_or_module_str.__name__ != job_name:
                raise NotImplementedError(
                    "Currently, the given name has to match the class name."
                )
            else:
                job_name = job_class_or_module_str.__name__
        elif job_name is not None:
            cls_module_str = cls._convert_pyiron_to_pyiron_atomistics_module(
                job_class_or_module_str
            )
        else:
            raise ValueError(
                "The job_name needs to be provided if a job_module_string is provided."
            )

        if (
            not overwrite
            and job_name in cls._job_class_dict
            and cls_module_str != cls._job_class_dict[job_name]
        ):
            ValueError(
                f"A JobType with name '{job_name}' is already defined! New class = '{cls_module_str}', "
                f"already registered class = '{cls._job_class_dict[job_name]}'."
            )
        else:
            cls._job_class_dict[job_name] = cls_module_str

    @staticmethod
    def convert_str_to_class(job_class_dict: dict, class_name: str) -> type:
        """
        convert the name of a class to the corresponding class object - only for pyiron internal classes.

        Args:
            job_class_dict (dict):
            class_name (str):

        Returns:
            (class):
        """
        job_type_lst = class_name.split(".")
        if len(job_type_lst) > 1:
            class_name = class_name.split()[-1][1:-2]
            job_type = class_name.split(".")[-1]
        else:
            job_type = job_type_lst[-1]

        if job_type in job_class_dict.keys():
            job_class = job_class_dict[job_type]
            if isinstance(job_class, str):
                job_module = importlib.import_module(job_class)
                job_class = getattr(job_module, job_type)
            return job_class
        raise ValueError(
            "Unknown job type: ",
            class_name,
            [job for job in list(job_class_dict.keys())],
        )


class JobFactory(PyironFactory):
    """
    The job creator is used to create job objects using pr.create.job.Code() where Code can be any external code
    which is wrapped as pyiron job type.
    """

    def __init__(self, project: "pyiron_base.project.generic.Project"):
        self._job_class_dict = JOB_CLASS_DICT
        self._project = project

    def __dir__(self) -> list:
        """
        Enable autocompletion by overwriting the __dir__() function.
        """
        return list(self._job_class_dict.keys())

    def __getattr__(self, name: str) -> callable:
        if name in self._job_class_dict:

            def wrapper(job_name, delete_existing_job=False, delete_aborted_job=False):
                """
                Create one of the following jobs:
                - 'ExampleJob': example job just generating random number
                - 'ParallelMaster': series of jobs run in parallel
                - 'ScriptJob': Python script or jupyter notebook job container
                - 'ListMaster': list of jobs

                Args:
                    job_name (str): name of the job
                    delete_existing_job (bool): delete an existing job - default false
                    delete_aborted_job (bool): delete an existing and aborted job - default false

                Returns:
                    GenericJob: job object depending on the job_type selected
                """
                job_name = _get_safe_job_name(job_name)
                return JobType(
                    class_name=name,
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


class JobTypeChoice(metaclass=Singleton):
    """
    Helper class to choose the job type directly from the project, autocompletion is enabled by overwriting the
    __dir__() function. This class is only required for pr.job_type.Code which is only used in pr.create_job().
    As a consequence this class can be removed once the pr.create_job() function is replaced by pr.create.job.Code().
    """

    def __init__(self):
        self._job_class_dict = None
        self.job_class_dict = JOB_CLASS_DICT

    @property
    def job_class_dict(self) -> dict:
        return self._job_class_dict

    @job_class_dict.setter
    def job_class_dict(self, job_class_dict: dict) -> None:
        self._job_class_dict = job_class_dict

    def __getattr__(self, name: str) -> str:
        if name in self._job_class_dict.keys():
            return name
        else:
            raise AttributeError("no job class named '{}' defined".format(name))

    def __dir__(self) -> list:
        """
        Enable autocompletion by overwriting the __dir__() function.
        """
        return list(self.job_class_dict.keys())
