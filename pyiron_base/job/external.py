# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Load input parameters for jupyter notebooks from external HDF5 or JSON file
"""

import json
from pathlib2 import Path
import warnings
from pyiron_base.generic.hdfio import FileHDFio
from pyiron_base.generic.datacontainer import DataContainer

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
        folder = Path(".").cwd().parts[-1]
        project_folder = Path(".").cwd().parents[1]
        hdf_file = project_folder / folder
        hdf_file = str(hdf_file).replace("\\", "/") + ".h5"
        if Path(hdf_file).exists():
            obj = DataContainer()
            obj.from_hdf(
                hdf=FileHDFio(hdf_file), group_name=folder + "/input/custom_dict"
            )
            obj["project_dir"] = str(project_folder)
            return obj
        elif Path("input.json").exists():
            with open("input.json") as f:
                return json.load(f)
        else:
            warnings.warn("{} not found".format(hdf_file))
            return None

    @staticmethod
    def store_custom_output_dict(output_dict):
        folder = Path(".").cwd().parts[-1]
        hdf_file = Path(".").cwd().parents[1] / folder
        hdf_file = str(hdf_file) + ".h5"
        hdf = FileHDFio(hdf_file)
        hdf[folder].create_group("output")
        obj = DataContainer(output_dict)
        obj.to_hdf(hdf[folder + "/output"])
