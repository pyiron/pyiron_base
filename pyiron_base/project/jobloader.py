# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
A helper class to be assigned to the project, which facilitates tab-completion when
loading jobs.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import numpy as np

from pyiron_base.state import state
from pyiron_base.database.jobtable import get_job_id
from pyiron_base.jobs.job.util import _get_safe_job_name

if TYPE_CHECKING:
    from pyiron_base.jobs.job.generic import GenericJob
    from pyiron_base.jobs.job.path import JobPath
    from pyiron_base.project.generic import Project


class _JobByAttribute(ABC):
    """
    A parent class for accessing project jobs by a call and a job specifier, or by tab
    completion.
    """

    def __init__(self, project: Project):
        self._project = project

    @property
    def _job_table(self):
        return self._project.job_table(columns=["job"])

    @property
    def _job_names(self):
        return self._job_table["job"].values

    def __dir__(self):
        return self._job_names

    def _id_from_name(self, name):
        return self._job_table.loc[self._job_names == name, "id"].values[0]

    def __getattr__(self, item):
        return self._project.load_from_jobpath(
            job_id=self._id_from_name(item), convert_to_object=self.convert_to_object
        )

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __call__(self, job_specifier=None, db_entry=None, convert_to_object=None):
        if (
            (job_specifier is None and db_entry is None)
            or (job_specifier is not None and db_entry is not None)
        ):
            raise TypeError(
                f"Exactly one of job_specifier or db_entry must be None, but got "
                f"{job_specifier} and {db_entry}, respectively"
            )
        if self._project.sql_query is not None:
            state.logger.warning(
                f"SQL filter '{self._project.sql_query}' is active (may exclude job)"
            )

        convert_to_object = convert_to_object if convert_to_object is not None \
            else self.convert_to_object

        if job_specifier is not None:
            return self._from_job_specifier(job_specifier, convert_to_object)
        else:
            return self._from_db_entry(db_entry, convert_to_object)

    def _from_job_specifier(self, job_specifier, convert_to_object):
        if not isinstance(job_specifier, (int, np.integer)):
            job_specifier = _get_safe_job_name(name=job_specifier)
        job_id = get_job_id(
            database=self._project.db,
            sql_query=self._project.sql_query,
            user=self._project.user,
            project_path=self._project.project_path,
            job_specifier=job_specifier,
        )
        if job_id is None:
            state.logger.warning(
                f"Job '{job_specifier}' does not exist and cannot be loaded"
            )
            return None
        return self._project.load_from_jobpath(
            job_id=job_id,
            convert_to_object=convert_to_object,
        )

    def _from_db_entry(self, db_entry, convert_to_object):
        return self._project.load_from_jobpath(
            db_entry=db_entry,
            convert_to_object=convert_to_object,
        )

    @property
    @abstractmethod
    def convert_to_object(self):
        pass


class JobLoader(_JobByAttribute):
    """
    Load an existing pyiron object - most commonly a job - from the database

    Args:
        job_specifier (str, int): name of the job or job ID

    Returns:
        GenericJob, JobCore: Either the full GenericJob object or just a reduced JobCore object
    """

    convert_to_object = True

    def __call__(
        self,
        job_specifier=None,
        db_entry=None,
        convert_to_object=None
    ) -> GenericJob:
        return super().__call__(
            job_specifier=job_specifier,
            db_entry=db_entry,
            convert_to_object=convert_to_object
        )


class JobInspector(_JobByAttribute):
    """
    Inspect an existing pyiron object - most commonly a job - from the database

    Args:
        job_specifier (str, int): name of the job or job ID

    Returns:
        JobCore: Access to the HDF5 object - not a GenericJob object - use :meth:`~.Project.load()`
            instead.
    """

    convert_to_object = False

    def __call__(self, job_specifier=None, db_entry=None) -> JobPath:
        return super().__call__(job_specifier=job_specifier, db_entry=db_entry)
