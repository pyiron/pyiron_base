# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Helper functions for the JobCore and GenericJob objects
"""
import os
import posixpath
import psutil
from pyiron_base.generic.util import static_isinstance
import tarfile
import stat
import shutil
from typing import Union, Dict

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


def _copy_database_entry(new_job_core, job_copied_id, new_database_entry=True):
    """
    Copy database entry from previous job

    Args:
        new_job_core (GenericJob): Copy of the job object
        job_copied_id (int): Job id of the copied job
        new_database_entry (bool): [True/False] to create a new database entry - default True
    """
    if new_database_entry:
        db_entry = new_job_core.project.db.get_item_by_id(job_copied_id)
        if db_entry is not None:
            db_entry["project"] = new_job_core.project_hdf5.project_path
            db_entry["projectpath"] = new_job_core.project_hdf5.root_path
            db_entry["subjob"] = new_job_core.project_hdf5.h5_path
            del db_entry["id"]
            job_id = new_job_core.project.db.add_item_dict(db_entry)
            new_job_core.reset_job_id(job_id=job_id)
    else:
        new_job_core.reset_job_id(job_id=None)


def _copy_to_delete_existing(project_class, job_name, delete_job):
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


def _get_project_for_copy(job, project, new_job_name):
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
        obj=project.__class__, obj_type="pyiron_base.job.core.JobCore"
    ):
        file_project = project.project
        hdf5_project = project.project_hdf5.open(new_job_name)
    elif isinstance(project, job.project.__class__):
        file_project = project
        hdf5_project = job.project_hdf5.__class__(
            project=project, file_name=new_job_name, h5_path="/" + new_job_name
        )
    elif isinstance(project, job.project_hdf5.__class__):
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


_special_symbol_replacements = {
    ".": "d",
    "-": "m",
    "+": "p",
    ",": "c",
    " ": "_",
}


def _get_safe_job_name(
    name: str, ndigits: Union[int, None] = 8, special_symbols: Union[Dict, None] = None
):
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


def _rename_job(job, new_job_name):
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


def _is_valid_job_name(job_name):
    """
    internal function to validate the job_name - only available in Python 3.4 <

    Args:
        job_name (str): job name
    """
    if not job_name.isidentifier():
        raise ValueError(
            f'Invalid name for a pyiron object, must be letters, digits (not as first character) and "_" only, not {job_name}'
        )
    if len(job_name) > 50:
        raise ValueError(
            "Invalid name for a PyIron object: must be less then or "
            "equal to 50 characters"
        )


def _copy_restart_files(job):
    """
    Internal helper function to copy the files required for the restart job.
    """
    if not (os.path.isdir(job.working_directory)):
        raise ValueError(
            "The working directory is not yet available to copy restart files"
        )
    for i, actual_name in enumerate(
        [os.path.basename(f) for f in job.restart_file_list]
    ):
        if actual_name in job.restart_file_dict.keys():
            new_name = job.restart_file_dict[actual_name]
            shutil.copy(
                job.restart_file_list[i],
                posixpath.join(job.working_directory, new_name),
            )
        else:
            shutil.copy(job.restart_file_list[i], job.working_directory)


def _kill_child(job):
    """
    Internal helper function to kill a child process.

    Args:
        job (JobCore): job object to decompress
    """
    if (
        static_isinstance(
            obj=job.__class__, obj_type="pyiron_base.master.GenericMaster"
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


def _job_compress(job, files_to_compress=None):
    """
    Compress the output files of a job object.

    Args:
        job (JobCore): job object to compress
        files_to_compress (list): list of files to compress
    """
    if not _job_is_compressed(job):
        if files_to_compress is None:
            files_to_compress = list(job.list_files())
        cwd = os.getcwd()
        try:
            os.chdir(job.working_directory)
            with tarfile.open(
                os.path.join(job.working_directory, job.job_name + ".tar.bz2"),
                "w:bz2",
            ) as tar:
                for name in files_to_compress:
                    if "tar" not in name and not stat.S_ISFIFO(os.stat(name).st_mode):
                        tar.add(name)
            for name in files_to_compress:
                if "tar" not in name:
                    fullname = os.path.join(job.working_directory, name)
                    if os.path.isfile(fullname):
                        os.remove(fullname)
                    elif os.path.isdir(fullname):
                        shutil.rmtree(fullname)
        finally:
            os.chdir(cwd)
    else:
        print("The files are already compressed!")


def _job_decompress(job):
    """
    Decompress the output files of a compressed job object.

    Args:
        job (JobCore): job object to decompress
    """
    try:
        tar_file_name = os.path.join(job.working_directory, job.job_name + ".tar.bz2")
        with tarfile.open(tar_file_name, "r:bz2") as tar:
            tar.extractall(job.working_directory)
        os.remove(tar_file_name)
    except IOError:
        pass


def _job_is_compressed(job):
    """
    Check if the job is already compressed or not.

    Args:
        job (JobCore): job object to check

    Returns:
        bool: [True/False]
    """
    compressed_name = job.job_name + ".tar.bz2"
    for name in job.list_files():
        if compressed_name in name:
            return True
    return False


def _job_archive(job):
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


def _job_unarchive(job):
    """
    Decompress HDF5 file of the job object from tar-archive

    Args:
        job (JobCore): job object to unarchive
    """
    fpath = job.project_hdf5.file_path
    try:
        tar_name = os.path.join(fpath, job.job_name + ".tar.bz2")
        with tarfile.open(tar_name, "r:bz2") as tar:
            tar.extractall(fpath)
        os.remove(tar_name)
    finally:
        pass


def _job_is_archived(job):
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


def _job_delete_hdf(job):
    """
    Delete HDF5 file of job object

    Args:
        job (JobCore): job object to delete
    """
    if os.path.isfile(job.project_hdf5.file_name):
        os.remove(job.project_hdf5.file_name)


def _job_delete_files(job):
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


def _job_remove_folder(job):
    """
    Delete the working directory of the job object

    Args:
        job (JobCore): job object to delete
    """
    working_directory = os.path.abspath(os.path.join(str(job.working_directory), ".."))
    if os.path.exists(working_directory) and len(os.listdir(working_directory)) == 0:
        shutil.rmtree(working_directory)


def _job_store_before_copy(job):
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


def _job_reload_after_copy(job, delete_file_after_copy):
    """
    Reload job from HDF5 file after copying

    Args:
        job (GenericJob): copied job object
        delete_file_after_copy (bool): delete HDF5 file after reload
    """
    job.from_hdf()
    if delete_file_after_copy:
        job.project_hdf5.remove_file()
