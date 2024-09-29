import io
import os
import posixpath
import tarfile
import tempfile
from shutil import copytree
from typing import Tuple

import numpy as np
import pandas

from pyiron_base.state import state
from pyiron_base.utils.instance import static_isinstance


def update_id_lst(record_lst: list, job_id_lst: list) -> list:
    """
    Update the list of master IDs based on the record list and job ID list.

    Args:
        record_lst (list): List of master IDs.
        job_id_lst (list): List of job IDs.

    Returns:
        list: Updated list of master IDs.
    """
    masterid_lst = []
    for masterid in record_lst:
        if masterid is None or np.isnan(masterid):
            masterid_lst.append(None)
        elif isinstance(masterid, int) or isinstance(masterid, float):
            masterid = int(masterid)
            masterid_lst.append(job_id_lst[masterid])
    return masterid_lst


def import_jobs(
    project_instance: "pyiron_base.project.generic.Project", archive_directory: str
):
    """
    Import jobs from an archive directory to a pyiron project.

    Args:
        project_instance (pyiron_base.project.generic.Project): Pyiron project instance.
        archive_directory (str): Path to the archive directory.
    """
    # Copy HDF5 files
    # if the archive_directory is a path(string)/name of the compressed file
    if static_isinstance(
        obj=archive_directory.__class__,
        obj_type=[
            "pyiron_base.project.generic.Project",
        ],
    ):
        archive_directory = archive_directory.path
    elif not isinstance(archive_directory, str):
        raise RuntimeError(
            "The given path for importing from, does not have the correct"
            " format paths as string or pyiron Project objects are expected"
        )
    if archive_directory.endswith(".tar.gz"):
        with tempfile.TemporaryDirectory() as temp_dir:
            with tarfile.open(archive_directory, "r:gz") as tar:
                tar.extractall(path=temp_dir)
            df, common_path = transfer_files(
                origin_path=temp_dir, project_path=project_instance.path
            )
    else:
        df, common_path = transfer_files(
            origin_path=archive_directory, project_path=project_instance.path
        )

    pr_import = project_instance.open(os.curdir)
    df["project"] = [
        posixpath.normpath(
            posixpath.join(pr_import.project_path, posixpath.relpath(p, common_path))
        )
        + "/"
        for p in df["project"].values
    ]
    df["projectpath"] = len(df) * [pr_import.root_path]
    # Add jobs to database
    job_id_lst = []

    for entry in df.dropna(axis=1).to_dict(orient="records"):
        for tag in ["id", "parentid", "masterid"]:
            if tag in entry:
                del entry[tag]
        if "timestart" in entry:
            entry["timestart"] = pandas.to_datetime(entry["timestart"])
        if "timestop" in entry:
            entry["timestop"] = pandas.to_datetime(entry["timestop"])
        if "username" not in entry:
            entry["username"] = state.settings.login_user
        job_id = pr_import.db.add_item_dict(par_dict=entry, check_duplicates=True)
        job_id_lst.append(job_id)

    # Update parent and master ids
    for job_id, masterid, parentid in zip(
        job_id_lst,
        update_id_lst(record_lst=df["masterid"].values, job_id_lst=job_id_lst),
        update_id_lst(record_lst=df["parentid"].values, job_id_lst=job_id_lst),
    ):
        if masterid is not None or parentid is not None:
            pr_import.db.item_update(
                item_id=job_id, par_dict={"parentid": parentid, "masterid": masterid}
            )


def transfer_files(origin_path: str, project_path: str) -> Tuple[pandas.DataFrame, str]:
    """
    Transfer files from the origin path to the project path.

    Args:
        origin_path (str): Path to the origin directory.
        project_path (str): Path to the project directory.

    Returns:
        Tuple[pandas.DataFrame, str]: A tuple containing the job table and the common path.
    """
    df = get_dataframe(origin_path=origin_path)
    common_path = posixpath.commonpath(list(df["project"]))
    copytree(posixpath.join(origin_path, common_path), project_path, dirs_exist_ok=True)
    return df, common_path


def get_dataframe(
    origin_path: str, csv_file_name: str = "export.csv"
) -> pandas.DataFrame:
    """
    Get the job table from the csv file.

    Args:
        origin_path (str): Path to the origin directory.
        csv_file_name (str): Name of the csv file.

    Returns:
        pandas.DataFrame: Job table.
    """
    # This line looks for the csv file outside of the archive directory to
    # guarantee backward compatibility with old archives.
    if os.path.exists(csv_file_name):
        return pandas.read_csv(csv_file_name, index_col=0)
    for root, dirs, files in os.walk(origin_path):
        if csv_file_name in files:
            return pandas.read_csv(os.path.join(root, csv_file_name), index_col=0)
    raise FileNotFoundError(f"File: {csv_file_name} was not found.")


def inspect_csv(tar_path: str, csv_file: str = "export.csv") -> None:
    """
    Inspect the csv file inside a tar archive.

    Args:
        tar_path (str): Path to the tar archive.
        csv_file (str): Name of the csv file.

    Returns:
        pandas.DataFrame: Job table.
    """
    with tarfile.open(tar_path, mode="r:gz") as tar:
        for member in tar.getmembers():
            # Check if the member is a file and ends with the desired csv file name
            if member.isfile() and member.name.endswith(f"/{csv_file}"):
                # Extract the file object
                extracted_file = tar.extractfile(member)

                if extracted_file:
                    # Read the file content
                    return pandas.read_csv(
                        io.StringIO(extracted_file.read().decode("utf-8")), index_col=0
                    )
        raise FileNotFoundError(f"File: {csv_file} in {tar_path} was not found.")
