# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import html
import os

import ipywidgets as widgets
import numpy as np
import pandas
from IPython.display import display

__author__ = "Joerg Neugebauer, Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "development"
__date__ = "Sep 1, 2017"


def is_h5_file(name: str) -> bool:
    """
    Check if a file name has the extension ".h5".

    Args:
        name (str): The file name.

    Returns:
        bool: True if the file name has the extension ".h5", False otherwise.
    """
    return "h5" == name.split(".")[-1]


def is_h5_dir(name: str) -> bool:
    """
    Check if a directory name has the suffix "_hdf5".

    Args:
        name (str): The directory name.

    Returns:
        bool: True if the directory name has the suffix "_hdf5", False otherwise.
    """
    return "hdf5" == name.split("_")[-1]


class ProjectGUI:
    """
    Gui to quickly browse through projects and objects
    Note: requires "%matplotlib notebook" at the beginning of your notebook (to refresh plots)
    """

    def __init__(self, project):
        self._ref_path = project.path
        self.project = project.copy()
        self.project._inspect_mode = True
        self.parent = None
        self.name = None
        self.fig, self.ax = None, None
        self.w_group = None
        self.w_node = None
        self.w_file = None
        self.w_text = None
        self.w_tab = None
        self.w_path = None
        self.w_type = None

        self.create_widgets()
        self.connect_widgets()
        self.display()

    def create_widgets(self) -> None:
        """
        Create the GUI widgets for browsing through projects and objects.

        Returns:
            None
        """
        select = widgets.Select  # Multiple
        self.w_group = select(description="Groups:")
        self.w_node = select(description="Nodes:")
        self.w_file = select(description="Files:")
        self.w_text = widgets.HTML()
        self.w_text.layout.height = "330px"
        self.w_text.layout.width = "580px"
        self.w_text.disabled = True
        w_list = widgets.VBox([self.w_group, self.w_node, self.w_file])
        self.w_tab = widgets.HBox([w_list, self.w_text])

        self.w_path = widgets.Text(name="Path: ")
        self.w_path.layout.width = "680px"
        self.w_type = widgets.Text(name="Type: ")

        self.refresh_view()

    def connect_widgets(self) -> None:
        """
        Connect the widget observers to the on_value_change method.

        Returns:
            None
        """
        self.w_group.observe(self.on_value_change, names="value")
        self.w_node.observe(self.on_value_change, names="value")
        self.w_file.observe(self.on_value_change, names="value")

    def display(self) -> None:
        """
        Display the GUI widgets.

        Returns:
            None
        """
        w_txt = widgets.HBox([self.w_path, self.w_type])
        display(widgets.VBox([self.w_tab, w_txt]))

    def plot_array(self, val: np.ndarray) -> None:
        """
        Plot an array.

        Args:
            val (np.ndarray): The array to plot.

        Returns:
            None
        """
        try:
            import pylab as plt
        except ImportError:
            import matplotlib.pyplot as plt

        plt.ioff()
        if self.fig is None:
            self.fig, self.ax = plt.subplots()
        else:
            self.ax.clear()

        if val.ndim == 1:
            self.ax.plot(val)
        elif val.ndim == 2:
            if len(val) == 1:
                self.ax.plot(val[0])
            else:
                self.ax.plot(val)
        elif val.ndim == 3:
            self.ax.plot(val[:, :, 0])

        self.w_text.value = self.plot_to_html()
        plt.close()

    def plot_to_html(self) -> str:
        """
        Convert the plot to an HTML image tag.

        Returns:
            str: The HTML image tag.
        """
        import base64
        import io

        # write image data to a string buffer and get the PNG image bytes
        buf = io.BytesIO()
        self.fig.set_size_inches(8, 4)
        self.fig.savefig(buf, format="png")
        buf.seek(0)
        return """<img src='data:image/png;base64,{}'/>""".format(
            base64.b64encode(buf.getvalue()).decode("ascii")
        )

    def on_value_change(self, change: dict) -> None:
        """
        Handle the value change event of the widgets.

        Args:
            change (dict): The change event.

        Returns:
            None
        """
        name = change["new"]
        self.w_text.value = ""
        if name == "..":
            self.move_up()
            self.w_group.value = "."
        else:
            if name is not None:
                if isinstance(name, str):
                    self.update_project(name)

    def get_rel_path(self, path: str) -> str:
        """
        Get the relative path of a given path with respect to the reference path.

        Args:
            path (str): The path to get the relative path for.

        Returns:
            str: The relative path.
        """
        return os.path.relpath(path, self._ref_path).replace("\\", "/")

    @staticmethod
    def dict_to_str(my_dict: dict) -> str:
        """
        Convert a dictionary to a string representation.

        Args:
            my_dict (dict): The dictionary to convert.

        Returns:
            str: The string representation of the dictionary.
        """
        eol = "<br>"
        if "Parameter" in my_dict.keys():
            key = html.escape(my_dict["Parameter"])
            val = html.escape(my_dict["Value"])
            com = html.escape(my_dict["Comment"])
            table = [
                "{}: {} {} {}".format(key, val, com, eol)
                for key, val, com in zip(key, val, com)
            ]
        else:
            table = ["{}: {} {}".format(key, val, eol) for key, val in my_dict.items()]
        return "".join(table)

    def refresh_view(self) -> None:
        """
        Refresh the view based on the current project.

        Returns:
            None
        """
        eol = os.linesep
        self.w_type.value = str(type(self.project))
        if isinstance(self.project, str):
            self.w_text.value = html.escape(self.project)
            self._move_up()
        elif isinstance(self.project, dict):
            self.w_text.value = self.dict_to_str(self.project)
            self._move_up()
        elif isinstance(self.project, (int, float)):
            self.w_text.value = html.escape(str(self.project))
            self._move_up()
        elif isinstance(self.project, list):
            max_length = 2000  # performance of widget above is extremely poor
            if len(self.project) < max_length:
                self.w_text.value = "<br>".join(self.project)
            else:
                self.w_text.value = (
                    "<br>".join(self.project[:max_length])
                    + eol
                    + " .... file too long: skipped ...."
                )

            self.w_type.value = "list: {} lines".format(len(self.project))
            self._move_up()
        elif isinstance(self.project, np.ndarray):
            self.plot_array(self.project)
            self._move_up()
        elif "data_dict" in self.project.list_nodes():
            self.w_text.value = pandas.DataFrame(self.project["data_dict"]).to_html()
            self._move_up()
        elif self.project is None:
            raise ValueError(
                "project is None: {}".format(type(self.project)), self.parent
            )
        else:
            self.w_group.options = [".", ".."]
            self.w_node.options = ["."]
            self.w_file.options = ["."]

            if hasattr(self.project, "path"):
                self.w_path.value = self.get_rel_path(self.project.path)
            else:
                print("path missing: ", type(self.project))

            groups = sorted(
                [el for el in self.project.list_groups() if not is_h5_dir(el)]
            )
            self.w_group.options = list(self.w_group.options) + groups

            nodes = sorted([el for el in self.project.list_nodes()])
            self.w_node.options = list(self.w_node.options) + nodes

            if hasattr(self.project, "list_files"):
                files = sorted(
                    [el for el in self.project.list_files() if not is_h5_file(el)]
                )
                self.w_file.options = list(self.w_file.options) + files
            else:
                self.w_file.options = []

    def update_project(self, name: str) -> None:
        """
        Update the project based on the selected name.

        Args:
            name (str): The name of the selected project.

        Returns:
            None
        """
        if name == ".":
            return
        self.name = name
        self.parent = self.project.copy()
        self.project = self.project[name]
        self.refresh_view()

    def _move_up(self) -> None:
        """
        Move up to the parent project.

        Returns:
            None
        """
        if hasattr(self.project, "path"):
            self.project = self.project[".."]
        else:
            self.project = self.parent

        if self.parent is None:
            self.w_path.value = "/".join(self.w_path.value.split("/")[:-1])
        else:
            self.w_path.value = self.get_rel_path(self.parent.path + "/" + self.name)

    def move_up(self) -> None:
        """
        Move up to the parent project.

        Returns:
            None
        """
        self._move_up()
        self.refresh_view()
