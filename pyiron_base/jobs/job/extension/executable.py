# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
from pyiron_base.interfaces.object import HasStorage
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


class Executable(HasStorage):
    def __init__(
        self,
        path_binary_codes=None,
        codename=None,
        module=None,
        code=None,
        overwrite_nt_flag=False,
    ):
        """
        Handle the path to the executable, as well as the version selection.

        Args:
            codename (str): name of the code str
            path_binary_codes (list): path to the binary codes as an absolute path
            overwrite_nt_flag (bool):
        """
        super().__init__()
        self.storage.table_name = "executable"

        if path_binary_codes is None:
            path_binary_codes = state.settings.resource_paths
        self.storage.version = None
        if code is not None:  # Backwards compatibility
            if not isinstance(code.__name__, str):
                raise TypeError("The codename should be a string.")
            codename = code.__name__
            module = code.__module__.split(".")[1]
        if codename is not None and module is not None:
            self.storage.name = codename.lower()
            code_path_lst = [
                os.path.join(path, module, "bin") for path in path_binary_codes
            ]
            backwards_compatible_path_lst = [
                os.path.join(path, self.storage.name) for path in path_binary_codes
            ]
            self.path_bin = [
                exe_path
                for exe_path in (code_path_lst + backwards_compatible_path_lst)
                if os.path.exists(exe_path)
            ]
        else:  # Backwards compatibility
            self.storage.name = codename.lower()
            self.path_bin = [
                os.path.join(path, self.storage.name)
                for path in path_binary_codes
                if os.path.exists(os.path.join(path, self.storage.name))
            ]
        if overwrite_nt_flag:
            self.storage.operation_system_nt = False
        else:
            self.storage.operation_system_nt = os.name == "nt"
        self.executable_lst = self._executable_versions_list()
        self.storage.executable = None
        self._executable_path = None
        self.storage.mpi = False
        if self.executable_lst:
            self.version = self.default_version
        self.storage.accepted_return_codes = [0]

    @property
    def accepted_return_codes(self):
        """
        list of int: accept all of the return codes in this list as the result of a successful run
        """
        return self.storage.accepted_return_codes

    @accepted_return_codes.setter
    def accepted_return_codes(self, value):
        if not isinstance(value, list) or any(
            not isinstance(c, int) or c > 255 for c in value
        ):
            raise ValueError("accepted_return_codes must be a list of integers <= 255!")
        self.storage.accepted_return_codes = value

    @property
    def version(self):
        """
        Version of the Executable

        Returns:
            str: version
        """
        return self.storage.version

    @property
    def default_version(self):
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
    def version(self, new_version):
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
    def mpi(self):
        """
        Check if the message processing interface is activated.

        Returns:
            bool: [True/False]
        """
        if not self.storage.mpi and self.version and "_mpi" in self.version:
            self.storage.mpi = True
        return self.storage.mpi

    @mpi.setter
    def mpi(self, mpi_bool):
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
    def available_versions(self):
        """
        List all available exectuables in the path_binary_codes for the specified codename.

        Returns:
            list: list of the available version
        """
        return self.list_executables()

    def list_executables(self):
        """
        List all available exectuables in the path_binary_codes for the specified codename.

        Returns:
            list: list of the available version
        """
        return sorted(list(self.executable_lst.keys()))

    @property
    def executable_path(self):
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
    def executable_path(self, new_path):
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

    def get_input_for_subprocess_call(self, cores, threads, gpus=None):
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

    def __repr__(self):
        """
        Executable path
        """
        return repr(self.executable_path)

    def __str__(self):
        """
        Executable path
        """
        return str(self.executable_path)

    def _executable_versions_list(self):
        """
        Internal function to list all available exectuables in the path_binary_codes for the specified codename.

        Returns:
            dict: list of the available version
        """
        if self.storage.operation_system_nt:
            extension = ".bat"
        else:
            extension = ".sh"
        try:
            executable_dict = {}
            for path in self.path_bin:
                for executable in os.listdir(path):
                    if (
                        executable.startswith("run_" + self.storage.name + "_")
                        & executable.endswith(extension)
                        and executable[
                            len("run_" + self.storage.name) + 1 : -len(extension)
                        ]
                        not in executable_dict.keys()
                    ):
                        executable_dict[
                            executable[
                                len("run_" + self.storage.name) + 1 : -len(extension)
                            ]
                        ] = os.path.join(path, executable).replace("\\", "/")
            return executable_dict
        except (
            OSError
        ):  # No executable exists - This is the case for GenericJob and other abstract job classes.
            return dict()

    def _executable_select(self):
        """
        Internal function to select an executable based on the codename and the version.

        Returns:
            str: absolute executable path
        """
        try:
            return self.executable_lst[self.version]
        except KeyError:
            return ""

    def _get_hdf_group_name(self):
        return "executable"
