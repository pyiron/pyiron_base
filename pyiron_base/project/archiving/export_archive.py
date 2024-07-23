import os
import tarfile
from shutil import copytree, rmtree

import numpy as np
from pyfileindex import PyFileIndex

from pyiron_base.project.archiving.shared import getdir
from pyiron_base.utils.instance import static_isinstance


def new_job_id(job_id, job_translate_dict):
    if isinstance(job_id, float) and not np.isnan(job_id):
        job_id = int(job_id)
    if isinstance(job_id, int):
        return job_translate_dict[job_id]
    else:
        return None


def update_project(project_instance, directory_to_transfer, archive_directory, df):
    directory_to_transfer = os.path.basename(directory_to_transfer)
    pr_transfer = project_instance.open(os.curdir)
    dir_name_transfer = getdir(path=directory_to_transfer)
    dir_name_archive = getdir(path=archive_directory)
    path_rel_lst = [
        os.path.relpath(p, pr_transfer.project_path) for p in df["project"].values
    ]
    return [
        (
            os.path.join(dir_name_archive, dir_name_transfer, p)
            if p != "."
            else os.path.join(dir_name_archive, dir_name_transfer)
        )
        for p in path_rel_lst
    ]


def generate_list_of_directories(df_files, directory_to_transfer, archive_directory):
    path_rel_lst = [
        os.path.relpath(d, directory_to_transfer) for d in df_files.dirname.unique()
    ]
    dir_name_transfer = getdir(path=directory_to_transfer)
    return [
        (
            os.path.join(archive_directory, dir_name_transfer, p)
            if p != "."
            else os.path.join(archive_directory, dir_name_transfer)
        )
        for p in path_rel_lst
    ]


def compress_dir(archive_directory):
    arch_comp_name = archive_directory + ".tar.gz"
    with tarfile.open(arch_comp_name, "w:gz") as tar:
        tar.add(os.path.relpath(archive_directory, os.getcwd()))
    rmtree(archive_directory)
    return arch_comp_name


def copy_files_to_archive(
    directory_to_transfer, archive_directory, compressed=True, copy_all_files=False
):
    """
    Create an archive of jobs in directory_to_transfer.

    Args:
        directory_to_transfer (str): project directory with jobs to export
        archive_directory (str): name of the final archive; if no file ending is given .tar.gz is added automatically when needed
        compressed (bool): if True compress archive_directory as a tarball; default True
        copy_all_files (bool): if True include job output files in archive, otherwise just include .h5 files; default False
    """

    assert isinstance(archive_directory, str) and ".tar.gz" not in archive_directory
    # print("directory to transfer: "+directory_to_transfer)
    dst = os.path.join(archive_directory, getdir(path=directory_to_transfer))
    if copy_all_files:
        copytree(directory_to_transfer, dst)
    else:
        copytree(directory_to_transfer, dst, ignore=ignore_non_h5_files)
    if compressed:
        compress_dir(archive_directory)


def ignore_non_h5_files(dir, files):
    return [f for f in files if not f.endswith(".h5")]


def export_database(pr, directory_to_transfer, archive_directory):
    # here we first check wether the archive directory is a path
    # or a project object
    assert isinstance(archive_directory, str) and ".tar.gz" not in archive_directory
    directory_to_transfer = os.path.basename(directory_to_transfer)
    df = pr.job_table()
    job_ids_sorted = sorted(df.id.values)
    new_job_ids = list(range(len(job_ids_sorted)))
    job_translate_dict = {j: n for j, n in zip(job_ids_sorted, new_job_ids)}
    df["id"] = [
        new_job_id(job_id=job_id, job_translate_dict=job_translate_dict)
        for job_id in df.id
    ]
    df["masterid"] = [
        new_job_id(job_id=job_id, job_translate_dict=job_translate_dict)
        for job_id in df.masterid
    ]
    df["parentid"] = [
        new_job_id(job_id=job_id, job_translate_dict=job_translate_dict)
        for job_id in df.parentid
    ]
    df["project"] = update_project(
        project_instance=pr,
        directory_to_transfer=directory_to_transfer,
        archive_directory=archive_directory,
        df=df,
    )
    del df["projectpath"]
    return df
