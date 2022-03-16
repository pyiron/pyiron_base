# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The Jobtable module provides a set of top level functions to interact with the database.
"""

import numpy as np
from pyiron_base.database.filetable import FileTable

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


def get_jobs(database, sql_query, user, project_path, recursive=True, columns=None):
    """
    Internal function to return the jobs as dictionary rather than a pandas.Dataframe

    Args:
        database (DatabaseAccess): Database object
        sql_query (str): SQL query to enter a more specific request
        user (str): username of the user whoes user space should be searched
        project_path (str): root_path - this is in contrast to the project_path in GenericPath
        recursive (bool): search subprojects [True/False]
        columns (list): by default only the columns ['id', 'project'] are selected, but the user can select a subset
                        of ['id', 'status', 'chemicalformula', 'job', 'subjob', 'project', 'projectpath', 'timestart',
                        'timestop', 'totalcputime', 'computer', 'hamilton', 'hamversion', 'parentid', 'masterid']

    Returns:
        dict: columns are used as keys and point to a list of the corresponding values
    """
    if not isinstance(database, FileTable):
        if columns is None:
            columns = ["id", "project"]
        df = database.job_table(
            sql_query=sql_query,
            user=user,
            project_path=project_path,
            recursive=recursive,
            columns=columns,
        )
        if len(df) == 0:
            return {key: list() for key in columns}
        return df.to_dict(orient="list")
    else:
        return database.get_jobs(
            project=project_path, recursive=recursive, columns=columns
        )


def get_job_ids(database, sql_query, user, project_path, recursive=True):
    """
    Return the job IDs matching a specific query

    Args:
        database (DatabaseAccess): Database object
        sql_query (str): SQL query to enter a more specific request
        user (str): username of the user whoes user space should be searched
        project_path (str): root_path - this is in contrast to the project_path in GenericPath
        recursive (bool): search subprojects [True/False]

    Returns:
        list: a list of job IDs
    """
    if not isinstance(database, FileTable):
        return get_jobs(
            database=database,
            sql_query=sql_query,
            user=user,
            project_path=project_path,
            recursive=recursive,
        )["id"]
    else:
        return database.get_job_ids(project=project_path, recursive=recursive)


def get_child_ids(database, sql_query, user, project_path, job_specifier, status=None):
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


def get_job_id(database, sql_query, user, project_path, job_specifier):
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


def set_job_status(database, sql_query, user, project_path, job_specifier, status):
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


def get_job_status(database, sql_query, user, project_path, job_specifier):
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


def get_job_working_directory(database, sql_query, user, project_path, job_specifier):
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
