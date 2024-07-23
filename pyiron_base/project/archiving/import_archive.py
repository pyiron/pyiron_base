import os
import tarfile
from shutil import copytree, rmtree

import numpy as np
import pandas

from pyiron_base.project.archiving.shared import getdir
from pyiron_base.state import state
from pyiron_base.utils.instance import static_isinstance


def update_id_lst(record_lst, job_id_lst):
    masterid_lst = []
    for masterid in record_lst:
        if masterid is None or np.isnan(masterid):
            masterid_lst.append(None)
        elif isinstance(masterid, int) or isinstance(masterid, float):
            masterid = int(masterid)
            masterid_lst.append(job_id_lst[masterid])
    return masterid_lst


def extract_archive(archive_directory):
    arch_comp_name = archive_directory + ".tar.gz"
    with tarfile.open(arch_comp_name, "r:gz") as tar:
        tar.extractall()


def import_jobs_to_new_project(cls, archive_directory, compressed=True):
    pass


def import_jobs_to_existing_project(pr, archive_directory, compressed=True):
    pass


def prepare_path(pr, archive_directory):
    if archive_directory[-7:] == ".tar.gz":
        archive_directory = archive_directory[:-7]
    elif not os.path.exists(archive_directory + ".tar.gz"):
        raise FileNotFoundError("Cannot find archive")

    arch_comp_name = archive_directory + ".tar.gz"
    with tarfile.open(arch_comp_name, "r:gz") as tar:
        target_folder = os.path.join(
            os.path.dirname(archive_directory), os.path.basename(tar.members[0].name)
        )
    if os.path.exists(target_folder):
        raise ValueError("Cannot extract to existing folder")

    return target_folder, archive_directory


def import_jobs(pr, archive_directory):
    # now open and extract archive
    extract_archive(archive_directory)

    # read csv
    csv_file_name = os.path.join(pr.path, "export.csv")
    df = pandas.read_csv(csv_file_name, index_col=0)
    df["project"] = [
        os.path.join(pr.project_path, os.path.relpath(p, pr.project_path)) + "/"
        for p in df["project"].values
    ]
    df["projectpath"] = len(df) * [pr.root_path]
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
        job_id = pr.db.add_item_dict(par_dict=entry)
        job_id_lst.append(job_id)

    # print(job_id_lst)
    # Update parent and master ids
    for job_id, masterid, parentid in zip(
        job_id_lst,
        update_id_lst(record_lst=df["masterid"].values, job_id_lst=job_id_lst),
        update_id_lst(record_lst=df["parentid"].values, job_id_lst=job_id_lst),
    ):
        if masterid is not None or parentid is not None:
            pr.db.item_update(
                item_id=job_id, par_dict={"parentid": parentid, "masterid": masterid}
            )
