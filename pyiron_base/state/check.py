import os
import stat
import glob
import pandas

try:
    from conda.cli import python_api
    conda_imported_successful = True
except ImportError:
    conda_imported_successful = False

from pyiron_base import state

conda_package_dict = {
    "mlip": "mlip",
    "atomicrex": "atomicrex",
    "damask": "damask",
    "runner": "runner",
    "randspg": "randspg",
    "sphinx": "sphinxdft",
    "lammps": "lammps"
}


def check_for_conda_package(name):
    lst = [l for l in python_api.run_command("list")[0].split("\n") if name + " " in l]
    if len(lst) == 0:
        return False
    else:
        return True


def check_executable_bit(resource_paths):
    def check_bit(script_path):
        filemode = os.stat(script_path).st_mode
        return bool(filemode & stat.S_IXUSR or filemode & stat.S_IXGRP or filemode & stat.S_IXOTH)

    path_lst = []
    for res_path in resource_paths:
        path_lst += glob.glob(res_path + "/*/*/*.sh")
    return {p: check_bit(script_path=p) for p in path_lst}


def check_executables_status():
    executables_dict = check_executable_bit(
        resource_paths=state.settings.configuration['resource_paths']
    )
    if conda_imported_successful:
        conda_lst = [
            check_for_conda_package(name=conda_package_dict[f])
            if f is not None and f in conda_package_dict.keys()
            else False
            for f in [
                p.split("/")[-3]
                if "/share/pyiron/" in p else None
                for p in executables_dict.keys()
            ]
        ]
        return pandas.DataFrame({
            "name": [p.split("/")[-3] for p in executables_dict.keys()],
            "path": list(executables_dict.keys()),
            "executable_bit": list(executables_dict.values()),
            "conda_package_installed": conda_lst,
        })
    else:
        return pandas.DataFrame({
            "name": [p.split("/")[-3] for p in executables_dict.keys()],
            "path": list(executables_dict.keys()),
            "executable_bit": list(executables_dict.values()),
        })
