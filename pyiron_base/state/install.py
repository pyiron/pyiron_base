# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os
import tarfile
from shutil import copytree, rmtree
import tempfile
import stat
import urllib.request as urllib2
from pyiron_base.utils.safetar import safe_extract

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


def _download_resources(
    zip_file="resources.tar.gz",
    resource_directory="~/pyiron/resources",
    giturl_for_zip_file="https://github.com/pyiron/pyiron-resources/releases/latest/download/resources.tar.gz",
    git_folder_name="resources",
):
    """
    Download pyiron resources from Github

    Args:
        zip_file (str): name of the compressed file
        resource_directory (str): directory where to extract the resources - the users resource directory
        giturl_for_zip_file (str): url for the zipped resources file on github
        git_folder_name (str): name of the extracted folder

    """
    user_directory = os.path.normpath(
        os.path.abspath(os.path.expanduser(resource_directory))
    )
    if os.path.exists(user_directory) and not os.listdir(user_directory):
        os.rmdir(user_directory)
    temp_directory = tempfile.gettempdir()
    temp_zip_file = os.path.join(temp_directory, zip_file)
    temp_extract_folder = os.path.join(temp_directory, git_folder_name)
    urllib2.urlretrieve(giturl_for_zip_file, temp_zip_file)
    if os.path.exists(user_directory):
        raise ValueError(
            "The resource directory exists already, therefore it can not be created: ",
            user_directory,
        )
    with tarfile.open(temp_zip_file, "r:gz") as tar:
        safe_extract(tar, temp_directory)
    copytree(temp_extract_folder, user_directory)
    if os.name != "nt":  #
        for root, dirs, files in os.walk(user_directory):
            for file in files:
                if ".sh" in file:
                    st = os.stat(os.path.join(root, file))
                    os.chmod(os.path.join(root, file), st.st_mode | stat.S_IEXEC)
    os.remove(temp_zip_file)
    rmtree(temp_extract_folder)


def _write_config_file(
    file_name="~/.pyiron",
    project_path="~/pyiron/projects",
    resource_path="~/pyiron/resources",
):
    """
    Write configuration file and create the corresponding project path.

    Args:
        file_name (str): configuration file name - usually ~/.pyiron
        project_path (str): the location where pyiron is going to store the pyiron projects
        resource_path (str): the location where the resouces (executables, potentials, ...) for pyiron are stored.
    """
    config_file = os.path.normpath(os.path.abspath(os.path.expanduser(file_name)))
    if not os.path.isfile(config_file):
        with open(config_file, "w") as cf:
            cf.writelines(
                [
                    "[DEFAULT]\n",
                    "PROJECT_PATHS = " + project_path + "\n",
                    "RESOURCE_PATHS = " + resource_path + "\n",
                ]
            )
        project_path = os.path.normpath(
            os.path.abspath(os.path.expanduser(project_path))
        )
        os.makedirs(project_path, exist_ok=True)


def install_dialog(silently=False):
    if not silently:
        user_input = None
    else:
        user_input = "yes"

    if "PYIRONCONFIG" in os.environ.keys():
        config_file = os.environ["PYIRONCONFIG"]
    else:
        config_file = "~/.pyiron"
    if not os.path.exists(os.path.expanduser(config_file)):
        while user_input not in ["yes", "no"]:
            user_input = input(
                "It appears that pyiron is not yet configured, do you want to create a default start configuration "
                "(recommended: yes). [yes/no]:"
            )
        if user_input.lower() == "yes" or user_input.lower() == "y":
            install_pyiron(
                config_file_name="~/.pyiron",
                zip_file="resources.tar.gz",
                resource_directory="~/pyiron/resources",
                giturl_for_zip_file="https://github.com/pyiron/pyiron-resources/releases/latest/download/resources.tar.gz",
                git_folder_name="resources",
            )
            print(
                "pyiron resources installed - restart your server for the changes to be in effect"
            )
        else:
            raise ValueError("pyiron was not installed!")
    else:
        print("pyiron is already installed.")


def install_pyiron(
    config_file_name="~/.pyiron",
    zip_file="resources.tar.gz",
    project_path="~/pyiron/projects",
    resource_directory="~/pyiron/resources",
    giturl_for_zip_file="https://github.com/pyiron/pyiron-resources/releases/latest/download/resources.tar.gz",
    git_folder_name="resources",
):
    """
    Function to configure the pyiron installation.

    Args:
        config_file_name (str): configuration file name - usually ~/.pyiron
        zip_file (str): name of the compressed file
        project_path (str): the location where pyiron is going to store the pyiron projects
        resource_directory (str): the location where the resouces (executables, potentials, ...) for pyiron are stored.
        giturl_for_zip_file (str/None): url for the zipped resources file on github.
            (Default points to pyiron's github resource repository. If None, leaves the
            resources directory *empty*.)
        git_folder_name (str): name of the extracted folder
    """
    _write_config_file(
        file_name=config_file_name,
        project_path=project_path,
        resource_path=resource_directory,
    )
    if giturl_for_zip_file is not None:
        _download_resources(
            zip_file=zip_file,
            resource_directory=resource_directory,
            giturl_for_zip_file=giturl_for_zip_file,
            git_folder_name=git_folder_name,
        )
    else:
        os.makedirs(resource_directory)
