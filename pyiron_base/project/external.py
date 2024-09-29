# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Load input parameters for jupyter notebooks from external HDF5 or JSON file
"""

import json
import warnings
from pathlib import Path

from pyiron_base.storage.datacontainer import DataContainer
from pyiron_base.storage.hdfio import FileHDFio

__author__ = "Osamu Waseda"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2019"


class Notebook(object):
    """
    class for pyiron notebook objects

    # TODO: Extract JSON functionality over to Project.data.read/write and remove this file.
    """

    @staticmethod
    def get_custom_dict():
        return load()

    @staticmethod
    def store_custom_output_dict(output_dict):
        dump(output_dict=output_dict)


def load() -> DataContainer:
    """
    Load input parameters from HDF5 or JSON file.

    Returns:
        DataContainer: The loaded input parameters.
    """
    folder = Path(".").cwd().parts[-1]
    project_folder = Path(".").cwd().parents[1]
    hdf_file = project_folder / folder
    hdf_file = str(hdf_file).replace("\\", "/") + ".h5"
    if Path(hdf_file).exists():
        obj = DataContainer()
        hdf_file_obj = FileHDFio(hdf_file)
        hdf_input = hdf_file_obj[folder + "/input"]
        if "custom_dict" in hdf_input.list_nodes():
            obj.update(hdf_file_obj[folder + "/input/custom_dict"])
        else:  # Backwards compatibility
            obj.from_hdf(hdf=hdf_file_obj, group_name=folder + "/input/custom_dict")
        obj["project_dir"] = str(project_folder)
        return obj
    elif Path("input.json").exists():
        with open("input.json") as f:
            return json.load(f)
    else:
        warnings.warn("{} not found".format(hdf_file))
        return None


def dump(output_dict: dict) -> None:
    """
    Dump output dictionary to HDF5 file.

    Args:
        output_dict (dict): The output dictionary to be dumped.
    """
    folder = Path(".").cwd().parts[-1]
    hdf_file = Path(".").cwd().parents[1] / folder
    hdf_file = str(hdf_file) + ".h5"
    hdf = FileHDFio(hdf_file)
    hdf[folder].create_group("output")
    obj = DataContainer(output_dict)
    obj.to_hdf(hdf[folder + "/output"])
