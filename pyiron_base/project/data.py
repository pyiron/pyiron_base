# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
A modified data container for storing associating data with a project.

Spec:
    The `Project` class should have data associated with it which can be stored to-file in our supported format(s)
    (right now just hdf5). This should meet the following requirements:

    - Data storage is immediately accessible, i.e. appearing in the project tab-completion module
    - Data stored there should be readable and writeable with a single parameter-free call
    - When instantiated, new projects should automatically read any available data (not *quite* satisfied right now!)
    - The `Project` tab completion menu should not get cluttered (i.e. nest everything under `Project.foo`)
"""

from pyiron_base.storage.datacontainer import DataContainer
from pyiron_base.storage.hdfio import ProjectHDFio

__author__ = "Liam Huber"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "0.1"
__maintainer__ = "Liam Huber"
__email__ = "huber@mpie.de"
__status__ = "production"
__date__ = "Feb 19, 2021"


class ProjectData(DataContainer):
    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls, *args, **kwargs)
        object.__setattr__(instance, "_project", None)
        return instance

    def __init__(
        self, *args, project: "pyiron_base.Project" = None, lazy: bool = True, **kwargs
    ):
        """
        A data storage container which can store itself to/retrieve itself from file at the project level.

        Args:
            project (pyiron_base.Project): The project instance the storage is attached to.
            lazy (bool): If True, the data is loaded lazily. Default is True.
        """
        super().__init__(*args, lazy=lazy, **kwargs)
        self._project = project

    def read(self) -> None:
        """Read existing data from project-level storage."""
        hdf = ProjectHDFio(self._project, file_name="project_data")
        if self.table_name not in hdf.list_groups():
            raise KeyError(
                f"Table name {self.table_name} was not found -- Project data is empty."
            )
        self.from_hdf(hdf=hdf)

    def write(self) -> None:
        """Write data to project-level storage."""
        self.to_hdf(ProjectHDFio(self._project, file_name="project_data"))
