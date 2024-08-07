import os
import shutil
import tarfile
import tempfile

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


def copy_files_to_archive(
    directory_to_transfer,
    archive_directory,
    compress=True,
    copy_all_files=False,
    arcname=None,
):
    """
    Copy files from a directory to an archive, optionally compressing the archive.

    Args:
        directory_to_transfer (str): The directory containing the files to transfer.
        archive_directory (str): The destination directory for the archive.
        compress (bool): If True, compress the archive directory into a tarball. Default is True.
        copy_all_files (bool): If True, include all files in the archive, otherwise only .h5 files. Default is False.

    """

    def copy_files(origin, destination, copy_all_files=copy_all_files):
        """
        Copy files from the origin directory to the destination directory.

        Args:
            origin (str): The origin directory containing the files to copy.
            destination (str): The destination directory for the copied files.
            copy_all_files (bool): If True, include all files in the archive,
                otherwise only .h5 files. Default is False.
        """
        if copy_all_files:
            shutil.copytree(origin, destination, dirs_exist_ok=True)
        else:
            copy_h5_files(origin, destination)

    assert isinstance(archive_directory, str) and ".tar.gz" not in archive_directory
    if arcname is None:
        arcname = os.path.relpath(os.path.abspath(archive_directory), os.getcwd())
    dir_name_transfer = getdir(path=directory_to_transfer)
    if not compress:
        copy_files(
            directory_to_transfer, os.path.join(archive_directory, dir_name_transfer)
        )
    else:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Copy files to the temporary directory
            copy_files(directory_to_transfer, os.path.join(temp_dir, dir_name_transfer))

            # Compress the temporary directory into a tar.gz archive
            with tarfile.open(f"{archive_directory}.tar.gz", "w:gz") as tar:
                tar.add(
                    temp_dir,
                    arcname=arcname,
                )


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
