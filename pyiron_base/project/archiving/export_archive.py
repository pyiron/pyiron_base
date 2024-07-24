import os
import tarfile
from shutil import copytree, rmtree

import numpy as np
from pyfileindex import PyFileIndex


def new_job_id(job_id, job_translate_dict):
    """
    Translate a job ID using a provided dictionary.

    Args:
        job_id (float or int): The job ID to be translated. If it is a float, it will be converted to an integer.
        job_translate_dict (dict): Dictionary mapping original job IDs to new job IDs.

    Returns:
        int or None: The translated job ID if it exists in the dictionary, otherwise None.
    """
    if isinstance(job_id, float) and not np.isnan(job_id):
        job_id = int(job_id)
    return job_translate_dict.get(job_id) if isinstance(job_id, int) else None


def update_project(project_instance, directory_to_transfer, archive_directory, df):
    """
    Update the project paths in a DataFrame to reflect the new archive location.

    Args:
        project_instance (Project): The project instance for accessing project properties.
        directory_to_transfer (str): The directory containing the jobs to be transferred.
        archive_directory (str): The base directory for the archive.
        df (DataFrame): DataFrame containing job information, including project paths.

    Returns:
        list: List of updated project paths reflecting the new archive location.
    """
    dir_name_transfer = os.path.basename(directory_to_transfer) or os.path.basename(
        os.path.dirname(directory_to_transfer)
    )
    dir_name_archive = os.path.basename(archive_directory) or os.path.basename(
        os.path.dirname(archive_directory)
    )

    pr_transfer = project_instance.open(os.curdir)
    path_rel_lst = [
        os.path.relpath(p, pr_transfer.project_path) for p in df["project"].values
    ]

    return [
        os.path.join(dir_name_archive, dir_name_transfer, p)
        if p != "."
        else os.path.join(dir_name_archive, dir_name_transfer)
        for p in path_rel_lst
    ]


def compress_dir(archive_directory):
    """
    Compress a directory into a tar.gz archive and remove the original directory.

    Args:
        archive_directory (str): The directory to be compressed.

    Returns:
        str: The name of the compressed tar.gz archive.
    """
    arch_comp_name = f"{archive_directory}.tar.gz"
    with tarfile.open(arch_comp_name, "w:gz") as tar:
        tar.add(os.path.relpath(archive_directory, os.getcwd()))
    rmtree(archive_directory)
    return arch_comp_name


def copy_files_to_archive(
    directory_to_transfer, archive_directory, compressed=True, copy_all_files=False
):
    """
    Copy files from a directory to an archive, optionally compressing the archive.

    Args:
        directory_to_transfer (str): The directory containing the files to transfer.
        archive_directory (str): The destination directory for the archive.
        compressed (bool): If True, compress the archive directory into a tarball. Default is True.
        copy_all_files (bool): If True, include all files in the transfer, otherwise only .h5 files. Default is False.

    Returns:
        None
    """
    assert isinstance(archive_directory, str) and ".tar.gz" not in archive_directory
    dir_name_transfer = os.path.basename(directory_to_transfer) or os.path.basename(
        os.path.dirname(directory_to_transfer)
    )
    dst = os.path.join(archive_directory, dir_name_transfer)

    ignore = None if copy_all_files else ignore_non_h5_files
    copytree(directory_to_transfer, dst, ignore=ignore, dirs_exist_ok=True)

    if compressed:
        compress_dir(archive_directory)


def ignore_non_h5_files(dir, files):
    """
    Ignore files that do not have a .h5 extension.

    Args:
        dir (str): The directory containing the files.
        files (list): List of file names in the directory.

    Returns:
        list: List of file names that do not have a .h5 extension.
    """
    return [f for f in files if not f.endswith(".h5")]


def export_database(pr, directory_to_transfer, archive_directory):
    """
    Export the project database to an archive directory.

    Args:
        pr (Project): The project instance containing the jobs.
        directory_to_transfer (str): The directory containing the jobs to transfer.
        archive_directory (str): The destination directory for the archive.

    Returns:
        DataFrame: DataFrame containing updated job information with new IDs and project paths.
    """
    assert isinstance(archive_directory, str) and ".tar.gz" not in archive_directory
    directory_to_transfer = os.path.basename(directory_to_transfer)

    df = pr.job_table()
    job_translate_dict = {
        old_id: new_id for new_id, old_id in enumerate(sorted(df.id.values))
    }

    df["id"] = df["id"].map(job_translate_dict)
    df["masterid"] = df["masterid"].map(job_translate_dict)
    df["parentid"] = df["parentid"].map(job_translate_dict)
    df["project"] = update_project(pr, directory_to_transfer, archive_directory, df)

    df.drop(columns=["projectpath"], inplace=True)
    return df
