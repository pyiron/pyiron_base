import os
import pandas
import numpy as np
from shutil import rmtree
from distutils.dir_util import copy_tree
import tarfile
from pyiron_base.project.archiving.shared import getdir
from pyiron_base.utils.instance import static_isinstance
from pyiron_base.state import state


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


def import_jobs(cls, archive_directory, compressed=True):
    if archive_directory[-7:] == ".tar.gz":
        archive_directory = archive_directory[:-7]
        if not compressed:
            compressed = True

    if compressed:
        arch_comp_name = archive_directory + ".tar.gz"
        with tarfile.open(arch_comp_name, "r:gz") as tar:
            target_folder = os.path.join(os.path.dirname(archive_directory), os.path.basename(tar.members[0].name))
    else:
        target_folder = archive_directory

    if os.path.exists(target_folder):
        raise ValueError("Cannot extract to existing folder")

    #otherise all ok, create project
    pr = cls(path=target_folder)

    #now open and extract archive
    extract_archive(archive_directory)

    #read csv
    csv_file_name = os.path.join(target_folder, "export.csv")
    df = pandas.read_csv(csv_file_name, index_col=0)

    df["projectpath"] = len(df) * [pr.root_path]
    # Add jobs to database
    job_id_lst = []
    for entry in df.dropna(axis=1).to_dict(orient="records"):
        if "id" in entry:
            del entry["id"]
        if "parentid" in entry:
            del entry["parentid"]
        if "masterid" in entry:
            del entry["masterid"]
        if "timestart" in entry:
            entry["timestart"] = pandas.to_datetime(entry["timestart"])
        if "timestop" in entry:
            entry["timestop"] = pandas.to_datetime(entry["timestop"])
        if "username" not in entry:
            entry["username"] = state.settings.login_user
        job_id = pr.db.add_item_dict(par_dict=entry)
        job_id_lst.append(job_id)

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

    return pr
