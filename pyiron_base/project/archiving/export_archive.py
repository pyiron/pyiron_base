import os
import tarfile
from shutil import copyfile, copytree, rmtree

import numpy as np
from pyfileindex import PyFileIndex

from pyiron_base.project.archiving.shared import getdir


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


def filter_function(file_name):
    return ".h5" in file_name


def generate_list_of_directories(df_files, directory_to_transfer):
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


def compress_dir(directory_to_transfer, archive_directory):
    arch_comp_name = archive_directory + ".tar.gz"
    with tarfile.open(arch_comp_name, "w:gz") as tar:
        tar.add(archive_directory, arcname=os.path.basename(directory_to_transfer))
    rmtree(archive_directory)
    return arch_comp_name


def copy_files_to_archive(
    project,
    directory_to_transfer,
    archive_directory,
    compressed=True,
    copy_all_files=False,
):
    """
    Create an archive of jobs in directory_to_transfer.

    Args:
        directory_to_transfer (str): project directory with jobs to export
        archive_directory (str): name of the final archive; if no file ending is given .tar.gz is added automatically when needed
        compressed (bool): if True compress archive_directory as a tarball; default True
        copy_all_files (bool): if True include job output files in archive, otherwise just include .h5 files; default False
    """
    if archive_directory[-7:] == ".tar.gz":
        archive_directory = archive_directory[:-7]
        if not compressed:
            compressed = True
    directory_to_transfer = os.path.normpath(directory_to_transfer)
    archive_directory = os.path.normpath(archive_directory)

    tempdir = export_files(
        directory_to_transfer, compressed, copy_all_files=copy_all_files
    )
    df = export_database(project, directory_to_transfer, archive_directory)
    csv_file_name = os.path.join(tempdir.name, "export.csv")
    df.to_csv(csv_file_name)

    if compressed:
        archived_file = compress_dir(directory_to_transfer, tempdir.name)
        copyfile(
            archived_file,
            os.path.join(
                os.path.dirname(os.path.abspath(archive_directory)),
                f"{os.path.basename(directory_to_transfer)}.tar.gz",
            ),
        )
    else:
        if os.path.exists(archive_directory):
            raise ValueError("Folder exists, give different name or allow compression")
        # now copy the whole set of folders
        copytree(tempdir.name, archive_directory)


def export_files(directory_to_transfer, compressed, copy_all_files=False):
    if not copy_all_files:
        pfi = PyFileIndex(path=directory_to_transfer, filter_function=filter_function)
    else:
        pfi = PyFileIndex(path=directory_to_transfer)
    df_files = pfi.dataframe[~pfi.dataframe.is_directory]

    # create a temporary folder for archiving
    tempdir = tempfile.TemporaryDirectory()

    # Create directories
    dir_lst = generate_list_of_directories(
        df_files=df_files, directory_to_transfer=directory_to_transfer
    )

    # now make these directories
    for d in dir_lst:
        os.makedirs(d.replace(directory_to_transfer, tempdir.name), exist_ok=True)

    # copy files
    for f in df_files.path.values:
        copyfile(
            f,
            os.path.join(
                tempdir.name,
                os.path.relpath(f, directory_to_transfer),
            ),
        )

    return tempdir


def export_database(pr, directory_to_transfer, archive_directory):
    # here we first check wether the archive directory is a path
    # or a project object

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

    # figure if we need to update archive names
    path_rel_lst = [os.path.relpath(os.path.normpath(p)) for p in df["project"].values]
    if os.path.basename(directory_to_transfer) != os.path.basename(archive_directory):
        # we need to update the project name
        path_rel_lst = [
            p.replace(
                os.path.basename(directory_to_transfer),
                os.path.basename(archive_directory),
            )
            for p in path_rel_lst
        ]

    df["project"] = path_rel_lst
    del df["projectpath"]

    return df
