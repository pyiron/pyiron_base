# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
from dataclasses import asdict, fields
from typing import List, Optional, Tuple, Union

from pyiron_dataclasses.v1.jobs.generic import Executable as ExecutableDataClass
from pyiron_snippets.resources import ExecutableResolver

from pyiron_base.interfaces.has_dict import HasDict
from pyiron_base.state import state

"""
Executable class loading executables from static/bin/<code>/
"""

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


class Executable(HasDict):
    __hdf_version__ = "0.3.0"

    def __init__(
        self,
        path_binary_codes: Optional[List[str]] = None,
        codename: Optional[str] = None,
        module: Optional[str] = None,
        overwrite_nt_flag: bool = False,
    ):
        """
        Handle the path to the executable, as well as the version selection.

        Args:
            codename (str): name of the code str
            path_binary_codes (list): path to the binary codes as an absolute path
            overwrite_nt_flag (bool):
        """
        super().__init__()
        if overwrite_nt_flag:
            operation_system_nt = False
        else:
            operation_system_nt = os.name == "nt"

        self.storage = ExecutableDataClass(
            version=None,
            name=codename.lower(),
            operation_system_nt=operation_system_nt,
            executable=None,
            mpi=False,
            accepted_return_codes=[0],
        )

        if path_binary_codes is None:
            path_binary_codes = state.settings.resource_paths
        if module is None:
            module = self.storage.name
        self._module = module
        self.path_bin = path_binary_codes
        self.executable_lst = self._executable_versions_list()
        self._executable_path = None
        if self.executable_lst:
            self.version = self.default_version

    @property
    def accepted_return_codes(self) -> List[int]:
        """
        list of int: accept all of the return codes in this list as the result of a successful run
        """
        return self.storage.accepted_return_codes

    @accepted_return_codes.setter
    def accepted_return_codes(self, value: List[int]) -> None:
        if not isinstance(value, list) or any(
            not isinstance(c, int) or c > 255 for c in value
        ):
            raise ValueError("accepted_return_codes must be a list of integers <= 255!")
        self.storage.accepted_return_codes = value

    @property
    def version(self) -> str:
        """
        Version of the Executable

        Returns:
            str: version
        """
        return self.storage.version

    @property
    def default_version(self) -> str:
        """
        Default Version of the Available Executables
        i.e. specifically defined

        Returns:
            str: default_version
        """
        for executable in self.executable_lst.keys():
            if "default" in executable and "mpi" not in executable:
                return executable
        return sorted(self.executable_lst.keys())[0]

    @version.setter
    def version(self, new_version: str) -> None:
        """
        Version of the Executable

        Args:
            new_version (str): version
        """
        if new_version in self.executable_lst.keys():
            self.storage.version = new_version
            if "mpi" in new_version:
                self.storage.mpi = True
            self._executable_path = None
        else:
            raise ValueError(
                "Version  [%s] is not supported, please choose one of the following versions: "
                % new_version,
                str(self.available_versions),
            )

    @property
    def mpi(self) -> bool:
        """
        Check if the message processing interface is activated.

        Returns:
            bool: [True/False]
        """
        if not self.storage.mpi and self.version and "_mpi" in self.version:
            self.storage.mpi = True
        return self.storage.mpi

    @mpi.setter
    def mpi(self, mpi_bool: bool) -> None:
        """
        Activate the message processing interface.

        Args:
            mpi_bool (bool): [True/False]
        """
        if not isinstance(mpi_bool, bool):
            raise TypeError("MPI can either be enabled or disabled: [True/False]")
        if self.version and "_mpi" not in self.version:
            self.version += "_mpi"
        if self.version is None and self.executable_path is None:
            raise ValueError("No executable set!")

    @property
    def available_versions(self) -> List[str]:
        """
        List all available exectuables in the path_binary_codes for the specified codename.

        Returns:
            list: list of the available version
        """
        return self.list_executables()

    def list_executables(self) -> List[str]:
        """
        List all available exectuables in the path_binary_codes for the specified codename.

        Returns:
            list: list of the available version
        """
        return sorted(list(self.executable_lst.keys()))

    @property
    def executable_path(self) -> str:
        """
        Get the executable path

        Returns:
            str: absolute path
        """
        if self._executable_path is not None:
            if os.name == "nt":
                return self._executable_path.replace("\\", "/")
            else:
                return self._executable_path
        return self._executable_select()

    @executable_path.setter
    def executable_path(self, new_path: str) -> None:
        """
        Set the executable path

        Args:
            new_path: absolute path
        """
        self.storage.version = new_path
        self._executable_path = new_path
        if new_path and "mpi" in new_path:
            self.storage.mpi = True
        else:
            self.storage.mpi = False

    @classmethod
    def instantiate(cls, obj_dict: dict, version: str = None) -> "Executable":
        try:
            codename = obj_dict["name"]
        except KeyError:
            codename = obj_dict["executable"]["name"]
        return cls(codename=codename)

    def _to_dict(self) -> dict:
        """
        Convert the object to a dictionary.

        Returns:
            dict: A dictionary representation of the object.
        """
        return asdict(self.storage)

    def _from_dict(self, obj_dict: dict, version: Optional[str] = None) -> None:
        """
        Load the object from a dictionary representation.

        Args:
            obj_dict (dict): A dictionary representation of the object.
            version (str, optional): The version of the object. Defaults to None.
        """
        data_container_keys = tuple(f.name for f in fields(ExecutableDataClass))
        executable_class_dict = {}
        # Backwards compatibility; dict state used to be nested one level deeper
        if "executable" in obj_dict.keys() and isinstance(obj_dict["executable"], dict):
            obj_dict = obj_dict["executable"]
        for key in data_container_keys:
            executable_class_dict[key] = obj_dict.get(key, None)
        self.storage = ExecutableDataClass(**executable_class_dict)

    def get_input_for_subprocess_call(
        self, cores: int, threads: int, gpus: Optional[int] = None
    ) -> Tuple[Union[str, List[str]], bool]:
        """
        Get the input parameters for the subprocess call to execute the job

        Args:
            cores (int): number of cores
            threads (int): number of threads
            gpus (int/None): number of gpus

        Returns:
            str/ list, boolean:  executable and shell variables
        """
        if cores == 1 or not self.mpi:
            executable = self.__str__()
            shell = True
        else:
            if isinstance(self.executable_path, list):
                executable = self.executable_path[:]
            else:
                executable = [self.executable_path]
            executable += [str(cores), str(threads)]
            if gpus is not None:
                executable += [str(gpus)]
            shell = False
        return executable, shell

    def __repr__(self) -> str:
        """
        Executable path
        """
        return repr(self.executable_path)

    def __str__(self) -> str:
        """
        Executable path
        """
        return str(self.executable_path)

    def _executable_versions_list(self) -> dict:
        """
        Internal function to list all available exectuables in the path_binary_codes for the specified codename.

        Returns:
            dict: list of the available version
        """
        return ExecutableResolver(
            resource_paths=self.path_bin,
            code=self.storage.name,
            module=self._module,
        ).dict()

    def _executable_select(self) -> str:
        """
        Internal function to select an executable based on the codename and the version.

        Returns:
            str: absolute executable path
        """
        try:
            return self.executable_lst[self.version]
        except KeyError:
            if isinstance(self.version, str):
                return self.version
            else:
                return ""

    def _get_hdf_group_name(self) -> str:
        return "executable"
