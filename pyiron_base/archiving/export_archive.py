import os
import numpy as np
from shutil import copyfile
from pyfileindex import PyFileIndex 
#from pyiron_base import Project

def new_job_id(job_id, job_translate_dict):
    if isinstance(job_id, float) and not np.isnan(job_id):
        job_id = int(job_id)
    if isinstance(job_id, int):
        return job_translate_dict[job_id]
    else:
        return None

def getdir(path): 
    path_base_name = os.path.basename(path)
    if path_base_name == "":
        return os.path.basename(os.path.dirname(path))
    else: 
        return path_base_name

def update_project(project_instance, directory_to_transfer, archive_directory, df):
    pr_transfer = project_instance.open(directory_to_transfer)
    #pr_transfer = Project(directory_to_transfer)
    dir_name_transfer = getdir(path=directory_to_transfer)
    dir_name_archive = getdir(path=archive_directory)
    path_rel_lst = [os.path.relpath(p, pr_transfer.project_path) for p in df["project"].values]
    return [os.path.join(dir_name_archive, dir_name_transfer, p) if p != "." else os.path.join(dir_name_archive, dir_name_transfer) for p in path_rel_lst]

def filter_function(file_name):
    return '.h5' in file_name

def generate_list_of_directories(df_files, directory_to_transfer, archive_directory):
    path_rel_lst = [os.path.relpath(d, directory_to_transfer) for d in df_files.dirname.unique()]
    dir_name_transfer = getdir(path=directory_to_transfer)
    return [os.path.join(archive_directory, dir_name_transfer, p) if p != "." else os.path.join(archive_directory, dir_name_transfer) for p in path_rel_lst]

def copy_files_to_archive(directory_to_transfer, archive_directory):
    pfi = PyFileIndex(path=directory_to_transfer, filter_function=filter_function)
    df_files = pfi.dataframe[~pfi.dataframe.is_directory]
    
    # Create directories 
    dir_lst = generate_list_of_directories(df_files=df_files, directory_to_transfer=directory_to_transfer, archive_directory=archive_directory)
    for d in dir_lst: 
        os.makedirs(d, exist_ok=True)
    
    # Copy files 
    dir_name_transfer = getdir(path=directory_to_transfer)
    for f in df_files.path.values:
        copyfile(f, os.path.join(archive_directory, dir_name_transfer, os.path.relpath(f, directory_to_transfer)))

def export_database(directory_to_transfer, archive_directory):
    pr = Project(directory_to_transfer)
    df = pr.job_table()
    job_ids_sorted = sorted(df.id.values)
    new_job_ids = list(range(len(job_ids_sorted)))
    job_translate_dict = {j:n for j, n in zip(job_ids_sorted, new_job_ids)}
    df['id'] = [new_job_id(job_id=job_id, job_translate_dict=job_translate_dict) for job_id in df.id]
    df['masterid'] = [new_job_id(job_id=job_id, job_translate_dict=job_translate_dict) for job_id in df.masterid]
    df['parentid'] = [new_job_id(job_id=job_id, job_translate_dict=job_translate_dict) for job_id in df.parentid]
    df['project'] = update_project(directory_to_transfer=directory_to_transfer, archive_directory=archive_directory, df=df)
    del df["projectpath"]
    return df

