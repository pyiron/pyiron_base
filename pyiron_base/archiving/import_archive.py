import os
import pandas 
import numpy as np
from shutil import copytree
#from pyiron_base import Project

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

def import_jobs(project_instance, directory_to_import_to, archive_directory, df):
    # Copy HDF5 files
    archive_name = getdir(path=archive_directory)
    cwd = os.getcwd()
    directory_to_import_to = directory_to_import_to.split("/")[1]
    src = cwd+"/"+archive_directory
    des = cwd+"/"+directory_to_import_to
    copytree(src, des, dirs_exist_ok=True)

    # Update Database
    pr_import = project_instance.open('.')
    df["project"] = [os.path.join(pr_import.project_path, os.path.relpath(p, archive_name)) for p in df["project"].values]
    df['projectpath'] = len(df) * [pr_import.root_path]
    print(df)
    # Add jobs to database 
    job_id_lst = []
    for entry in df.to_dict(orient="records"):
        del entry['id']
        del entry['parentid']
        del entry['masterid']
        entry["timestart"] = pandas.to_datetime(entry["timestart"])
        entry["timestop"] = pandas.to_datetime(entry["timestop"])
        job_id = pr_import.db.add_item_dict(par_dict=entry)
        job_id_lst.append(job_id)
        
    # Update parent and master ids 
    for job_id, masterid, parentid in zip(job_id_lst, update_id_lst(record_lst=df['masterid'].values, job_id_lst=job_id_lst), update_id_lst(record_lst=df['parentid'].values, job_id_lst=job_id_lst)):
        if not np.isnan(masterid) or not np.isnan(parentid):
            pr_import.db.item_update(item_id=job_id, par_dict={'parentid': parentid, 'masterid': masterid})
