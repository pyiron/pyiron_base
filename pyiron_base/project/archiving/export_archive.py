import os
import shutil
import tarfile
import tempfile
from typing import Optional

import pandas


def copy_files_to_archive(
    directory_to_transfer: str,
    archive_directory: str,
    compress: bool = True,
    copy_all_files: bool = False,
    arcname: Optional[str] = None,
    df: Optional[pandas.DataFrame] = None,
):
    """
    Copy files from a directory to an archive, optionally compressing the archive.

    Args:
        directory_to_transfer (str): The directory containing the files to transfer.
        archive_directory (str): The destination directory for the archive.
        compress (bool): If True, compress the archive directory into a tarball. Default is True.
        copy_all_files (bool): If True, include all files in the archive, otherwise only .h5 files. Default is False.
        arcname (str): The name of the archive directory. Default is the name of the directory to transfer.
        df (DataFrame): DataFrame containing updated job information with new IDs and project paths.

    """

    def copy_files(
        origin: str, destination: str, copy_all_files: bool = copy_all_files
    ):
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
    if df is not None:
        df.to_csv(os.path.join(directory_to_transfer, "export.csv"))
    if not compress:
        copy_files(directory_to_transfer, os.path.join(archive_directory, arcname))
    elif compress and copy_all_files:
        with tarfile.open(f"{archive_directory}.tar.gz", "w:gz") as tar:
            tar.add(directory_to_transfer, arcname=arcname)
    else:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Copy files to the temporary directory
            dest = os.path.join(
                temp_dir, os.path.basename(directory_to_transfer.rstrip("/\\"))
            )
            copy_files(directory_to_transfer, dest)
            # Compress the temporary directory into a tar.gz archive
            with tarfile.open(f"{archive_directory}.tar.gz", "w:gz") as tar:
                tar.add(dest, arcname=arcname)
    if df is not None:
        os.remove(os.path.join(directory_to_transfer, "export.csv"))


def copy_h5_files(src: str, dst: str) -> None:
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
            if file.endswith(".h5") or file == "export.csv":
                src_file = os.path.join(root, file)
                rel_path = os.path.relpath(root, src)
                dst_dir = os.path.join(dst, rel_path)
                os.makedirs(dst_dir, exist_ok=True)
                shutil.copy2(src_file, os.path.join(dst_dir, file))


def export_database(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    Export the project database to an archive directory.

    Args:
        df (DataFrame): pyiron job table containing job information.

    Returns:
        DataFrame: DataFrame containing updated job information with new IDs
            and project paths.
    """

    job_translate_dict = {
        old_id: new_id for new_id, old_id in enumerate(sorted(df.id.values))
    }

    df["id"] = df["id"].map(job_translate_dict)
    df["masterid"] = df["masterid"].map(job_translate_dict)
    df["parentid"] = df["parentid"].map(job_translate_dict)
    df["project"] = df["project"].map(lambda x: os.path.relpath(x, os.getcwd()))

    df.drop(columns=["projectpath"], inplace=True)
    return df
