import os
import pandas
import numpy as np
from shutil import rmtree
from distutils.dir_util import copy_tree
import tarfile
from pyiron_base.generic.util import static_isinstance
from pyiron_base.settings.generic import Settings


s = Settings()


def getdir(path):
    path_base_name = os.path.basename(path)
    if path_base_name == "":
        return os.path.basename(os.path.dirname(path))
    else:
        return path_base_name


def update_id_lst(record_lst, job_id_lst):
    masterid_lst = []
    for masterid in record_lst:
        if np.isnan(masterid):
            masterid_lst.append(masterid)
        elif isinstance(masterid, int) or isinstance(masterid, float):
            masterid = int(masterid)
            masterid_lst.append(job_id_lst[masterid])
    return masterid_lst


def extract_archive(archive_directory):
    fname = archive_directory+".tar.gz"
    tar = tarfile.open(fname, "r:gz")
    tar.extractall()
    tar.close()


def import_jobs(
    project_instance, directory_to_import_to, archive_directory,
    df, compressed=True
):
    # Copy HDF5 files
    # if the archive_directory is a path(string)/name of the compressed file
    if isinstance(archive_directory, str):
        archive_directory = os.path.basename(archive_directory)
    # if the archive_directory is a project
    elif static_isinstance(
        obj=archive_directory.__class__,
        obj_type=[
            "pyiron_base.project.generic.Project",
        ]
    ):
        archive_directory = archive_directory.path
    else:
        raise RuntimeError(
            """the given path for importing from,
            does not have the correct format paths
            as string or pyiron Project objects are expected"""
        )
    if compressed:
        extract_archive(archive_directory)
    archive_name = getdir(path=archive_directory)
    if directory_to_import_to[-1] != '/':
        directory_to_import_to = os.path.basename(directory_to_import_to)
    else:
        directory_to_import_to = os.path.basename(directory_to_import_to[:-1])
    # destination folder
    des = os.path.abspath(os.path.join(os.curdir, directory_to_import_to))
    # source folder; archive folder
    src = os.path.abspath(os.path.join(os.curdir, archive_directory))
    copy_tree(src, des)
    if compressed:
        rmtree(archive_directory)

    # Update Database
    pr_import = project_instance.open(os.curdir)
    df["project"] = [os.path.join(
        pr_import.project_path, os.path.relpath(p, archive_name)) + "/"
        for p in df["project"].values
    ]
    df['projectpath'] = len(df) * [pr_import.root_path]
    # Add jobs to database
    job_id_lst = []
    for entry in df.dropna(axis=1).to_dict(orient="records"):
        if 'id' in entry:
            del entry['id']
        if 'parentid' in entry:
            del entry['parentid']
        if 'masterid' in entry:
            del entry['masterid']
        if 'timestart' in entry:
            entry["timestart"] = pandas.to_datetime(entry["timestart"])
        if 'timestop' in entry:
            entry["timestop"] = pandas.to_datetime(entry["timestop"])
        if 'username' not in entry: 
            entry["username"] = s.login_user
        job_id = pr_import.db.add_item_dict(par_dict=entry)
        job_id_lst.append(job_id)

    # Update parent and master ids
    for job_id, masterid, parentid in zip(
        job_id_lst,
        update_id_lst(record_lst=df["masterid"].values, job_id_lst=job_id_lst),
        update_id_lst(record_lst=df["parentid"].values, job_id_lst=job_id_lst),
    ):
        if not np.isnan(masterid) or not np.isnan(parentid):
            pr_import.db.item_update(
                item_id=job_id,
                par_dict={
                    "parentid": parentid, "masterid": masterid
                }
            )
