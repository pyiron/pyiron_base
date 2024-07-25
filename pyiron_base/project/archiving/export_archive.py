import os
import shutil
import tarfile

import numpy as np

from pyiron_base.project.archiving.shared import getdir


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
    directory_to_transfer = os.path.basename(directory_to_transfer)
    dir_name_transfer = getdir(path=directory_to_transfer)
    dir_name_archive = getdir(path=archive_directory)

    pr_transfer = project_instance.open(os.curdir)
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
    shutil.rmtree(archive_directory)
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
        copy_all_files (bool): If True, include all files in the archive, otherwise only .h5 files. Default is False.

    """
    assert isinstance(archive_directory, str) and ".tar.gz" not in archive_directory
    dir_name_transfer = getdir(path=directory_to_transfer)
    dst = os.path.join(archive_directory, dir_name_transfer)
    if copy_all_files:
        shutil.copytree(directory_to_transfer, dst, dirs_exist_ok=True)
    else:
        copy_h5_files(directory_to_transfer, dst)
    if compressed:
        compress_dir(archive_directory)


def copy_h5_files(src, dst):
    """
    Copies all .h5 files from the source directory to the destination directory,
    preserving the directory structure.

    Args:
        src (str): The source directory from which .h5 files will be copied.
        dst (str): The destination directory where .h5 files will be copied to.

    This function traverses the source directory tree, identifies files with a .h5
    extension, and copies them to the destination directory while maintaining the
    same directory structure. Non-.h5 files are ignored.
    """

    for root, dirs, files in os.walk(src):
        for file in files:
            if file.endswith(".h5"):
                src_file = os.path.join(root, file)
                rel_path = os.path.relpath(root, src)
                dst_dir = os.path.join(dst, rel_path)
                os.makedirs(dst_dir, exist_ok=True)
                shutil.copy2(src_file, os.path.join(dst_dir, file))


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
