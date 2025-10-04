# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Helper functions for the JobCore and GenericJob objects
"""

import os
import posixpath
import shutil
import stat
import tarfile
from itertools import islice
from typing import Optional, Tuple, Union

import monty.io
import psutil
from pyiron_snippets.logger import logger

from pyiron_base.database.sqlcolumnlength import JOB_STR_LENGTH
from pyiron_base.utils.instance import static_isinstance
from pyiron_base.utils.safetar import safe_extract

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Nov 28, 2020"


_special_symbol_replacements = {
    ".": "d",
    "-": "m",
    "+": "p",
    ",": "c",
    " ": "_",
}


def _copy_database_entry(
    new_job_core: "pyiron_base.jobs.job.generic.GenericJob",
    job_copied_id: int,
    username: Optional[str] = None,
) -> None:
    """
    Copy database entry from previous job

    Args:
        new_job_core (GenericJob): Copy of the job object
        job_copied_id (int): Job id of the copied job
        username (str): Optional name of the user to copy the job to
    """
    db_entry = new_job_core.project.db.get_item_by_id(job_copied_id)
    if db_entry is not None:
        db_entry["job"] = new_job_core.job_name
        db_entry["subjob"] = new_job_core.project_hdf5.h5_path
        db_entry["project"] = new_job_core.project_hdf5.project_path
        db_entry["projectpath"] = new_job_core.project_hdf5.root_path
        if username is not None:
            db_entry["username"] = username
        del db_entry["id"]
        job_id = new_job_core.project.db.add_item_dict(db_entry)
        new_job_core.reset_job_id(job_id=job_id)


def _copy_to_delete_existing(
    project_class: "pyiron_base.project.generic.Project",
    job_name: str,
    delete_job: bool,
):
    """
    Check if the job exists already in the project, if that is the case either
    delete it or reload it depending on the setting of delete_job

    Args:
        project_class (Project): The project to copy the job to.
            (Default is None, use the same project.)
        job_name (str): The new name to assign the duplicate job. Required if the project is `None` or the same
            project as the copied job. (Default is None, try to keep the same name.)
        delete_job (bool): Delete job if it exists already

    Returns:
        GenericJob/ None
    """
    job_table = project_class.job_table(recursive=False)
    if len(job_table) > 0 and job_name in job_table.job.values:
        if not delete_job:
            return project_class.load(job_name)
        else:
            project_class.remove_job(job_name)
            return None


def _get_project_for_copy(
    job: "pyiron_base.jobs.job.base.JobCore",
    project: Optional[
        Union[
            "pyiron_base.project.generic.Project",
            "pyiron_base.jobs.job.base.JobCore",
            "pyiron_base.storage.hdfio.ProjectHDFio",
        ]
    ],
    new_job_name: str,
) -> Tuple[
    "pyiron_base.project.generic.Project", "pyiron_base.storage.hdfio.ProjectHDFio"
]:
    """
    Internal helper function to generate a project and hdf5 project for copying

    Args:
        job (JobCore): Job object used for comparison
        project (JobCore/ProjectHDFio/Project/None): The project to copy the job to.
            (Default is None, use the same project.)
        new_job_name (str): The new name to assign the duplicate job. Required if the
            project is `None` or the same project as the copied job. (Default is None,
            try to keep the same name.)

    Returns:
        Project, ProjectHDFio
    """
    if static_isinstance(
        obj=project.__class__, obj_type="pyiron_base.jobs.job.base.JobCore"
    ):
        file_project = project.project
        hdf5_project = project.project_hdf5.open(new_job_name)
    elif isinstance(job.project, project.__class__):
        file_project = project
        hdf5_project = job.project_hdf5.__class__(
            project=project, file_name=new_job_name, h5_path="/" + new_job_name
        )
    elif isinstance(job.project_hdf5, project.__class__):
        file_project = project.project
        hdf5_project = project.open(new_job_name)
    elif project is None:
        file_project = job.project
        hdf5_project = job.project_hdf5.__class__(
            project=file_project, file_name=new_job_name, h5_path="/" + new_job_name
        )
    else:
        raise ValueError("Project should be JobCore/ProjectHDFio/Project/None")
    return file_project, hdf5_project


def _get_safe_job_name(
    name: Union[str, tuple],
    ndigits: Optional[int] = 8,
    special_symbols: Optional[dict] = None,
) -> str:
    """
    Sanitize a job name, optionally appending numeric values.

    Args:
        name (str|tuple): The name to sanitize, or a tuple of the name and any number
            of numeric values to append with '_' in between.
        ndigits (int|None): How many digits to round any floating point values in a
            `name` tuple to. (Default is 8; to not round at all use None.)
        special_symbols (dict|None): Conversions of special symbols to apply. This will
            be applied to the default conversion dict, which contains:
            DEFAULT_CONV_DICT

    Returns:
        (str): The sanitized (and possibly rounded) name.
    """
    d_special_symbols = _special_symbol_replacements.copy()
    if special_symbols is not None:
        d_special_symbols.update(special_symbols)

    def round_(value, ndigits=ndigits):
        if isinstance(value, float) and ndigits is not None:
            return round(value, ndigits=ndigits)
        return value

    if not isinstance(name, str):
        name_rounded = [round_(nn) for nn in name]
        job_name = "_".join([str(nn) for nn in name_rounded])
    else:
        job_name = name
    for k, v in d_special_symbols.items():
        job_name = job_name.replace(k, v)
    _is_valid_job_name(job_name=job_name)
    return job_name


_get_safe_job_name.__doc__ = _get_safe_job_name.__doc__.replace(
    "DEFAULT_CONV_DICT", f"{_special_symbol_replacements}"
)


def _rename_job(
    job: Union[
        "pyiron_base.jobs.job.generic.GenericJob", "pyiron_base.jobs.job.base.JobCore"
    ],
    new_job_name: str,
) -> None:
    """ """
    new_job_name = _get_safe_job_name(new_job_name)
    child_ids = job.child_ids
    if child_ids:
        for child_id in child_ids:
            ham = job.project.load(child_id)
            ham.move_to(job.project.open(new_job_name + "_hdf5"))
    old_working_directory = job.working_directory
    if len(job.project_hdf5.h5_path.split("/")) > 2:
        new_location = job.project_hdf5.open("../" + new_job_name)
    else:
        new_location = job.project_hdf5.__class__(
            job.project, new_job_name, h5_path="/" + new_job_name
        )
    if job.job_id:
        job.project.db.item_update(
            {"job": new_job_name, "subjob": new_location.h5_path}, job.job_id
        )
    old_job_name = job.name
    job._name = new_job_name
    job.project_hdf5.copy_to(destination=new_location, maintain_name=False)
    job.project_hdf5.remove_file()
    job.project_hdf5 = new_location
    if os.path.exists(old_working_directory):
        shutil.move(old_working_directory, job.working_directory)
        os.rmdir("/".join(old_working_directory.split("/")[:-1]))
    if os.path.exists(os.path.join(job.working_directory, old_job_name + ".tar.bz2")):
        os.rename(
            os.path.join(job.working_directory, old_job_name + ".tar.bz2"),
            os.path.join(job.working_directory, job.job_name + ".tar.bz2"),
        )


def _is_valid_job_name(job_name: str) -> None:
    """
    internal function to validate the job_name - only available in Python 3.4 <

    Args:
        job_name (str): job name
    """
    if not job_name.isidentifier():
        raise ValueError(
            f'Invalid name for a pyiron object, must be letters, digits (not as first character) and "_" only, not {job_name}'
        )
    if len(job_name) > JOB_STR_LENGTH:
        raise ValueError(
            f"Invalid name for a PyIron object: must be less then or equal to {JOB_STR_LENGTH} characters"
        )


def _get_restart_copy_dict(job: "pyiron_base.jobs.job.generic.GenericJob") -> dict:
    copy_dict = {}
    for i, actual_name in enumerate(
        [os.path.basename(f) for f in job.restart_file_list]
    ):
        if actual_name in job.restart_file_dict.keys():
            new_name = job.restart_file_dict[actual_name]
        else:
            new_name = os.path.basename(job.restart_file_list[i])
        copy_dict[new_name] = job.restart_file_list[i]
    return copy_dict


def _copy_restart_files(job: "pyiron_base.jobs.job.generic.GenericJob") -> None:
    """
    Internal helper function to copy the files required for the restart job.
    """
    if not (os.path.isdir(job.working_directory)):
        raise ValueError(
            "The working directory is not yet available to copy restart files"
        )
    for file_name, source in _get_restart_copy_dict(job=job).items():
        shutil.copy(
            source,
            posixpath.join(job.working_directory, file_name),
        )


def _kill_child(job: "pyiron_base.jobs.job.base.JobCore") -> None:
    """
    Internal helper function to kill a child process.

    Args:
        job (JobCore): job object to decompress
    """
    if (
        static_isinstance(
            obj=job.__class__, obj_type="pyiron_base.jobs.master.GenericMaster"
        )
        and not job.server.run_mode.queue
        and (job.status.running or job.status.submitted)
    ):
        for proc in psutil.process_iter():
            try:
                pinfo = proc.as_dict(attrs=["pid", "cwd"])
            except psutil.NoSuchProcess:
                pass
            else:
                if pinfo["cwd"] is not None and pinfo["cwd"].startswith(
                    job.working_directory
                ):
                    job_process = psutil.Process(pinfo["pid"])
                    job_process.kill()


def _job_compressed_name(job: "pyiron_base.jobs.job.base.JobCore") -> str:
    """Return the canonical file name of a compressed job."""
    return _get_compressed_job_name(working_directory=job.working_directory)


def _get_compressed_job_name(
    working_directory: "pyiron_base.jobs.job.base.JobCore",
) -> str:
    """Return the canonical file name of a compressed job from the working directory."""
    return os.path.join(
        working_directory, os.path.basename(working_directory) + ".tar.bz2"
    )


def _job_compress(
    job: "pyiron_base.jobs.job.base.JobCore",
    files_to_compress: list = [],
    files_to_remove: list = [],
) -> None:
    """
    Compress the output files of a job object.

    Args:
        job (JobCore): job object to compress
        files_to_compress (list): list of files to compress
        files_to_remove (list): list of files to remove
    """

    def delete_file_or_folder(fullname):
        if os.path.isfile(fullname):
            os.remove(fullname)
        elif os.path.isdir(fullname):
            shutil.rmtree(fullname)

    if not _job_is_compressed(job):
        for name in files_to_remove:
            delete_file_or_folder(fullname=os.path.join(job.working_directory, name))
        if len(files_to_compress) == 0:
            return
        cwd = os.getcwd()
        try:
            os.chdir(job.working_directory)
            with tarfile.open(_job_compressed_name(job), "w:bz2") as tar:
                for name in files_to_compress:
                    if "tar" not in name and not stat.S_ISFIFO(os.stat(name).st_mode):
                        tar.add(name)
            for name in files_to_compress:
                if name != _job_compressed_name(job):
                    delete_file_or_folder(
                        fullname=os.path.join(job.working_directory, name)
                    )
        finally:
            os.chdir(cwd)
    else:
        logger.info("The files are already compressed!")


def _job_decompress(job: "pyiron_base.jobs.job.base.JobCore") -> None:
    """
    Decompress the output files of a compressed job object.

    Args:
        job (JobCore): job object to decompress
    """
    tar_file_name = _job_compressed_name(job)
    try:
        with tarfile.open(tar_file_name, "r:bz2") as tar:
            safe_extract(tar, job.working_directory)
        os.remove(tar_file_name)
    except IOError:
        pass


def _working_directory_is_compressed(working_directory: str) -> bool:
    """
    Check if the working directory of a given job is already compressed or not.

    Args:
        working_directory (str): working directory of the job object

    Returns:
        bool: [True/False]
    """
    compressed_name = os.path.basename(
        _get_compressed_job_name(working_directory=working_directory)
    )
    return compressed_name in os.listdir(working_directory)


def _job_is_compressed(job: "pyiron_base.jobs.job.base.JobCore") -> bool:
    """
    Check if the job is already compressed or not.

    Args:
        job (JobCore): job object to check

    Returns:
        bool: [True/False]
    """
    return _working_directory_is_compressed(working_directory=job.working_directory)


def _working_directory_list_files(
    working_directory: str, include_archive: bool = True
) -> list:
    """
    Returns list of files in the jobs working directory.

    If the working directory is compressed, return a list of files in the archive.

    Args:
        working_directory (str): working directory of the job object to inspect files in
        include_archive (bool): include files in the .tag.gz archive

    Returns:
        list of str: file names
    """
    if os.path.isdir(working_directory):
        uncompressed_files_lst = os.listdir(working_directory)
        if include_archive and _working_directory_is_compressed(
            working_directory=working_directory
        ):
            compressed_job_name = _get_compressed_job_name(
                working_directory=working_directory
            )
            with tarfile.open(compressed_job_name, "r") as tar:
                compressed_files_lst = [
                    member.name for member in tar.getmembers() if member.isfile()
                ]
                uncompressed_files_lst.remove(os.path.basename(compressed_job_name))
                return uncompressed_files_lst + compressed_files_lst
        else:
            return uncompressed_files_lst
    return []


def _job_list_files(job: "pyiron_base.jobs.job.base.JobCore") -> list:
    """
    Returns list of files in the jobs working directory.

    If the job is compressed, return a list of files in the archive.

    Args:
        job (JobCore): job object to inspect files in

    Returns:
        list of str: file names
    """
    return _working_directory_list_files(working_directory=job.working_directory)


def _working_directory_read_file(
    working_directory: str, file_name: str, tail: Optional[int] = None
) -> list:
    """
    Return list of lines of the given file.

    Transparently decompresses the file if working directory is compressed.

    If `tail` is given and job is decompressed, only read the last lines
    instead of traversing the full file.

    Args:
        working_directory (str): working directory of the job object
        file_name (str): the file to print
        tail (int, optional): only return the last lines

    Raises:
        FileNotFoundError: if the given file name does not exist in the job folder
    """
    if file_name not in _working_directory_list_files(
        working_directory=working_directory
    ):
        raise FileNotFoundError(file_name)

    if _working_directory_is_compressed(
        working_directory=working_directory
    ) and file_name not in os.listdir(working_directory):
        with tarfile.open(
            _get_compressed_job_name(working_directory=working_directory),
            encoding="utf8",
        ) as f:
            lines = [
                line.decode("utf8") for line in f.extractfile(file_name).readlines()
            ]
            if tail is None:
                return lines
            else:
                return lines[-tail:]
    else:
        file_name = posixpath.join(working_directory, file_name)
        if tail is None:
            with open(file_name) as f:
                return f.readlines()
        else:
            lines = list(
                reversed(
                    [
                        line + os.linesep
                        for line in islice(monty.io.reverse_readfile(file_name), tail)
                    ]
                )
            )
            # compatibility with the other methods
            # monty strips all newlines, where as reading the other ways does
            # not.  So if a file does not end with a newline (as most text
            # files) adding it to every line like above adds an additional one.
            if len(lines) > 0:
                lines[-1] = lines[-1].rstrip(os.linesep)
            return lines


def _job_read_file(
    job: "pyiron_base.jobs.job.base.JobCore", file_name: str, tail: Optional[int] = None
) -> list:
    """
    Return list of lines of the given file.

    Transparently decompresses the file if job is compressed.

    If `tail` is given and job is decompressed, only read the last lines
    instead of traversing the full file.

    Args:
        file_name (str): the file to print
        tail (int, optional): only return the last lines

    Raises:
        FileNotFoundError: if the given file name does not exist in the job folder
    """
    return _working_directory_read_file(
        working_directory=job.working_directory, file_name=file_name, tail=tail
    )


def _job_archive(job: "pyiron_base.jobs.job.base.JobCore") -> None:
    """
    Compress HDF5 file of the job object to tar-archive

    Args:
        job (JobCore): job object to archive
    """
    fpath = job.project_hdf5.file_path
    jname = job.job_name
    h5_dir_name = jname + "_hdf5"
    h5_file_name = jname + ".h5"
    cwd = os.getcwd()
    try:
        os.chdir(fpath)
        with tarfile.open(
            os.path.join(fpath, job.job_name + ".tar.bz2"), "w:bz2"
        ) as tar:
            for name in [h5_dir_name, h5_file_name]:
                tar.add(name)
        for name in [h5_dir_name, h5_file_name]:
            fullname = os.path.join(fpath, name)
            if os.path.isfile(fullname):
                os.remove(fullname)
            elif os.path.isdir(fullname):
                shutil.rmtree(fullname)
    finally:
        os.chdir(cwd)


def _job_unarchive(job: "pyiron_base.jobs.job.base.JobCore") -> None:
    """
    Decompress HDF5 file of the job object from tar-archive

    Args:
        job (JobCore): job object to unarchive
    """
    fpath = job.project_hdf5.file_path
    try:
        tar_name = os.path.join(fpath, job.job_name + ".tar.bz2")
        with tarfile.open(tar_name, "r:bz2") as tar:
            safe_extract(tar, fpath)
        os.remove(tar_name)
    finally:
        pass


def _job_is_archived(job: "pyiron_base.jobs.job.base.JobCore") -> bool:
    """
    Check if the HDF5 file of the Job is compressed as tar-archive

    Args:
        job (JobCore): job object to check

    Returns:
        bool: [True/False]
    """
    return os.path.isfile(
        os.path.join(job.project_hdf5.file_path, job.job_name + ".tar.bz2")
    )


def _job_delete_hdf(job: "pyiron_base.jobs.job.base.JobCore") -> None:
    """
    Delete HDF5 file of job object

    Args:
        job (JobCore): job object to delete
    """
    if os.path.isfile(job.project_hdf5.file_name):
        os.remove(job.project_hdf5.file_name)


def _job_delete_files(job: "pyiron_base.jobs.job.base.JobCore") -> None:
    """
    Delete files in the working directory of job object

    Args:
        job (JobCore): job object to delete
    """
    working_directory = str(job.working_directory)
    if job._import_directory is None and os.path.exists(working_directory):
        shutil.rmtree(working_directory)
    else:
        job._import_directory = None


def _job_remove_folder(job: "pyiron_base.jobs.job.base.JobCore") -> None:
    """
    Delete the working directory of the job object

    Args:
        job (JobCore): job object to delete
    """
    working_directory = os.path.abspath(os.path.join(str(job.working_directory), ".."))
    if os.path.exists(working_directory) and len(os.listdir(working_directory)) == 0:
        shutil.rmtree(working_directory)


def _job_store_before_copy(job: "pyiron_base.jobs.job.base.JobCore") -> bool:
    """
    Store job in HDF5 file for copying

    Args:
        job (GenericJob): job object to copy

    Returns:
        bool: [True/False] if the HDF5 file of the job exists already
    """
    if not job.project_hdf5.file_exists:
        delete_file_after_copy = True
    else:
        delete_file_after_copy = False
    job.to_hdf()
    return delete_file_after_copy


def _job_reload_after_copy(
    job: "pyiron_base.jobs.job.base.JobCore", delete_file_after_copy: bool
) -> None:
    """
    Reload job from HDF5 file after copying

    Args:
        job (GenericJob): copied job object
        delete_file_after_copy (bool): delete HDF5 file after reload
    """
    job.from_hdf()
    if delete_file_after_copy:
        job.project_hdf5.remove_file()
