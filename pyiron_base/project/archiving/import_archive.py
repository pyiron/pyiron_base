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
    fname = archive_directory + ".tar.gz"
    tar = tarfile.open(fname, "r:gz")
    tar.extractall()
    tar.close()


def import_jobs(project_instance, archive_directory, df, compressed=True):
    if len(set(df["job"]) & set(project_instance.job_table().job)) > 0:
        raise ValueError("Overlapping", df["job"], project_instance.job_table().job)
    # Copy HDF5 files
    # if the archive_directory is a path(string)/name of the compressed file
    if static_isinstance(
        obj=archive_directory.__class__,
        obj_type=[
            "pyiron_base.project.generic.Project",
        ],
    ):
        archive_directory = archive_directory.path
    elif isinstance(archive_directory, str):
        if archive_directory[-7:] == ".tar.gz":
            archive_directory = archive_directory[:-7]
            compressed = True
    else:
        raise RuntimeError(
            """the given path for importing from,
            does not have the correct format paths
            as string or pyiron Project objects are expected"""
        )
    if compressed:
        extract_archive(os.path.relpath(archive_directory, os.getcwd()))

    # source folder; archive folder
    src = os.path.abspath(archive_directory)
    copy_tree(src, project_instance.path)
    if compressed:
        rmtree(src)

    # # Update Database
    pr_import = project_instance.open(os.curdir)

    df["project"] = [
        os.path.join(
            pr_import.project_path, os.path.relpath(p, getdir(path=archive_directory))
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
        job_id = pr_import.db.add_item_dict(par_dict=entry)
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
