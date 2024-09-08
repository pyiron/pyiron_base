# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The Jobtable module provides a set of top level functions to interact with the database.
"""

from typing import List, Optional, Union

import numpy as np

from pyiron_base.database.filetable import FileTable
from pyiron_base.database.generic import DatabaseAccess

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


def get_child_ids(
    database: Union[FileTable, DatabaseAccess],
    sql_query: str,
    user: str,
    project_path: str,
    job_specifier: str,
    status: Optional[str] = None,
) -> List[dict]:
    """
    Get the childs for a specific job

    Args:
        database (DatabaseAccess): Database object
        sql_query (str): SQL query to enter a more specific request
        user (str): username of the user whoes user space should be searched
        project_path (str): root_path - this is in contrast to the project_path in GenericPath
        job_specifier (str): name of the master job or the master jobs job ID
        status (str): filter childs which match a specific status - None by default

    Returns:
        list: list of child IDs
    """
    if not isinstance(database, FileTable):
        id_master = get_job_id(database, sql_query, user, project_path, job_specifier)
        if id_master is None:
            return []
        else:
            search_dict = {"masterid": str(id_master)}
            if status is not None:
                search_dict["status"] = status
            return sorted(
                [
                    job["id"]
                    for job in database.get_items_dict(
                        search_dict, return_all_columns=False
                    )
                ]
            )
    else:
        return database.get_child_ids(job_specifier=job_specifier, project=project_path)


def get_job_id(
    database: Union[FileTable, DatabaseAccess],
    sql_query: str,
    user: str,
    project_path: str,
    job_specifier: str,
) -> Union[int, None]:
    """
    get the job_id for job named job_name in the local project path from database

    Args:
        database (DatabaseAccess): Database object
        sql_query (str): SQL query to enter a more specific request
        user (str): username of the user whoes user space should be searched
        project_path (str): root_path - this is in contrast to the project_path in GenericPath
        job_specifier (str): name of the job or job ID

    Returns:
        int: job ID of the job
    """
    if not isinstance(database, FileTable):
        if isinstance(job_specifier, (int, np.integer)):
            return job_specifier  # is id

        job_dict = database._job_dict(
            sql_query=sql_query,
            user=user,
            project_path=project_path,
            recursive=False,
            job=job_specifier,
        )
        if len(job_dict) == 0:
            job_dict = database._job_dict(
                sql_query=sql_query,
                user=user,
                project_path=project_path,
                recursive=True,
                job=job_specifier,
            )
        if len(job_dict) == 0:
            return None
        elif len(job_dict) == 1:
            return job_dict[0]["id"]
        else:
            raise ValueError(
                "job name '{0}' in this project '{1}' is not unique '{2}".format(
                    job_specifier, project_path, job_dict
                )
            )
    else:
        return database.get_job_id(job_specifier=job_specifier, project=project_path)


def set_job_status(
    database: Union[FileTable, DatabaseAccess],
    sql_query: str,
    user: str,
    project_path: str,
    job_specifier: Union[str, int],
    status: str,
) -> None:
    """
    Set the status of a particular job

    Args:
        database (DatabaseAccess/ FileTable): Database object
        sql_query (str): SQL query to enter a more specific request
        user (str): username of the user whoes user space should be searched
        project_path (str): root_path - this is in contrast to the project_path in GenericPath
        job_specifier (str): name of the job or job ID
        status (str): job status can be one of the following ['initialized', 'appended', 'created', 'submitted',
                     'running', 'aborted', 'collect', 'suspended', 'refresh', 'busy', 'finished']

    """
    database.set_job_status(
        job_id=get_job_id(
            database=database,
            sql_query=sql_query,
            user=user,
            project_path=project_path,
            job_specifier=job_specifier,
        ),
        status=status,
    )


def get_job_status(
    database: Union[FileTable, DatabaseAccess],
    sql_query: str,
    user: str,
    project_path: str,
    job_specifier: Union[str, int],
) -> str:
    """
    Get the status of a particular job

    Args:
        database (DatabaseAccess): Database object
        sql_query (str): SQL query to enter a more specific request
        user (str): username of the user whoes user space should be searched
        project_path (str): root_path - this is in contrast to the project_path in GenericPath
        job_specifier (str): name of the job or job ID

    Returns:
        str: job status can be one of the following ['initialized', 'appended', 'created', 'submitted', 'running',
             'aborted', 'collect', 'suspended', 'refresh', 'busy', 'finished']
    """

    return database.get_job_status(
        job_id=get_job_id(
            database=database,
            sql_query=sql_query,
            user=user,
            project_path=project_path,
            job_specifier=job_specifier,
        )
    )


def get_job_working_directory(
    database: Union[FileTable, DatabaseAccess],
    sql_query: str,
    user: str,
    project_path: str,
    job_specifier: Union[str, int],
) -> str:
    """
    Get the working directory of a particular job

    Args:
        database (DatabaseAccess): Database object
        sql_query (str): SQL query to enter a more specific request
        user (str): username of the user whoes user space should be searched
        project_path (str): root_path - this is in contrast to the project_path in GenericPath
        job_specifier (str): name of the job or job ID

    Returns:
        str: working directory as absolute path
    """
    return database.get_job_working_directory(
        job_id=get_job_id(
            database=database,
            sql_query=sql_query,
            user=user,
            project_path=project_path,
            job_specifier=job_specifier,
        )
    )
