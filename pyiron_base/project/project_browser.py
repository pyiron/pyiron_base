# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os

import ipywidgets as widgets
from IPython.core.display import display

from pyiron_base import Project as BaseProject
from pyiron_base.generic.filedata import FileData


__author__ = "Niklas Siemer"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "0.1"
__maintainer__ = "Niklas Siemer"
__email__ = "siemer@mpie.de"
__status__ = "development"
__date__ = "Feb 02, 2021"


class ProjectBrowser:
    """
        Project Browser Widget

        Allows to browse files/nodes/groups in the Project based file system.
        Selected files may be received from this ProjectBrowser widget by the data attribute.
    """
    def __init__(self,
                 project,
                 Vbox=None,
                 fix_path=False,
                 show_files=True
                 ):
        """
            ProjectBrowser to browse the project file system.

            Args:
                project: Any pyiron project to browse.
                Vbox(:class:`ipython.widget.VBox`/None): Widget used to display the browser (Constructed if None).
                fix_path (bool): If True the path in the file system cannot be changed.
                show_files(bool): If True files (from project.list_files()) are displayed.
        """
        self._project = project.copy()
        self._node_as_dirs = isinstance(self.project, BaseProject)
        self._initial_project = project.copy()
        self._initial_project_path = self.path

        if Vbox is None:
            self.box = widgets.VBox()
        else:
            self.box = Vbox
        self.fix_path = fix_path
        self._busy = False
        self._show_files = show_files
        self._hide_path = True
        self.output = widgets.Output(layout=widgets.Layout(width='50%', height='100%'))
        self._clickedFiles = []
        self._data = None
        self.pathbox = widgets.HBox(layout=widgets.Layout(width='100%', justify_content='flex-start'))
        self.optionbox = widgets.HBox()
        self.filebox = widgets.VBox(layout=widgets.Layout(width='50%', height='100%', justify_content='flex-start'))
        self.path_string_box = widgets.Text(description="(rel) Path", width='min-content')
        self.refresh()

    @property
    def project(self):
        return self._project

    @property
    def path(self):
        """Path of the project."""
        return self.project.path

    @property
    def _project_root_path(self):
        try:
            root_path = self.project.root_path
        except AttributeError:
            root_path = self.project.project.root_path
        return root_path

    def _busy_check(self, busy=True):
        """Function to disable widget interaction while another update is ongoing"""
        if self._busy and busy:
            return True
        else:
            self._busy = busy

    def _update_files(self):
        # HDF and S3 project do not have list_files
        self.files = list()
        if self._show_files:
            try:
                self.files = self.project.list_files()
            except AttributeError:
                pass
        self.nodes = self.project.list_nodes()
        self.dirs = self.project.list_groups()

    def gui(self):
        """Return the VBox containing the browser."""
        self.refresh()
        return self.box

    def refresh(self):
        """Refresh the project browser."""
        self.output.clear_output(True)
        self._node_as_dirs = isinstance(self.project, BaseProject)
        self._update_files()
        body = widgets.HBox([self.filebox, self.output],
                            layout=widgets.Layout(
                                min_height='100px',
                                max_height='800px'
                            ))
        self.path_string_box = self.path_string_box.__class__(description="(rel) Path", value='')
        self._update_optionbox(self.optionbox)
        self._update_filebox(self.filebox)
        self._update_pathbox(self.pathbox)
        self.box.children = tuple([self.optionbox, self.pathbox, body])

    def configure(self, Vbox=None, fix_path=None, show_files=None, hide_path=None):
        """Change configuration of the project browser.

            Args:
                Vbox(:class:`ipython.widget.VBox`/None): Widget used to display the browser.
                fix_path (bool/None): If True the path in the file system cannot be changed.
                show_files(bool/None): If True files (from project.list_files()) are displayed.
                hide_path(bool/None): If True the root_path is omitted in the path.
        """
        if Vbox is not None:
            self.box = Vbox
        if fix_path is not None:
            self.fix_path = fix_path
        if show_files is not None:
            self._show_files = show_files
        if hide_path is not None:
            self._hide_path = hide_path
        self.refresh()

    def _update_optionbox(self, optionbox):

        def click_option_button(b):
            if self._busy_check():
                return
            self._click_option_button(b)
            self._busy_check(False)

        set_path_button = widgets.Button(description='Set Path', tooltip="Sets current path to provided string.")
        set_path_button.on_click(click_option_button)
        if self.fix_path:
            set_path_button.disabled = True
        childs = [set_path_button, self.path_string_box]

        button = widgets.Button(description="Reset selection", width='min-content')
        button.on_click(click_option_button)
        childs.append(button)

        optionbox.children = tuple(childs)

    def _click_option_button(self, b):
        self.output.clear_output(True)
        with self.output:
            print('')
        if b.description == 'Set Path':
            if self.fix_path:
                return
            else:
                path = self.path
            if len(self.path_string_box.value) == 0:
                with self.output:
                    print('No path given')
                return
            elif self.path_string_box.value[0] != '/':
                path = path + '/' + self.path_string_box.value
            else:
                path = self.path_string_box.value
            self._update_project(path)
        if b.description == 'Reset selection':
            self._clickedFiles = []
            self._data = None
            self._update_filebox(self.filebox)

    @property
    def data(self):
        return self._data

    def _update_project_worker(self, rel_path):
        try:
            new_project = self.project[rel_path]
            # Check if the new_project implements list_nodes()
            new_project.list_nodes()
        except (ValueError, AttributeError):
            self.path_string_box = self.path_string_box.__class__(description="(rel) Path", value='')
            with self.output:
                print("No valid path")
            return
        else:
            if new_project is not None:
                self._project = new_project

    def _update_project(self, path):
        if isinstance(path, str):
            rel_path = os.path.relpath(path, self.path)
            if rel_path == '.':
                return
            self._update_project_worker(rel_path)
        else:
            self._project = path
        self.refresh()

    def _update_pathbox(self, box):
        path_color = '#DDDDAA'
        home_color = '#999999'

        def on_click(b):
            if self._busy_check():
                return
            self._update_project(b.path)
            self._busy_check(False)

        buttons = []
        tmppath = os.path.abspath(self.path)
        if tmppath[-1] == '/':
            tmppath = tmppath[:-1]
        len_root_path = len(os.path.split(self._project_root_path[:-1])[0])
        tmppath_old = tmppath + '/'
        while tmppath != tmppath_old:
            tmppath_old = tmppath
            [tmppath, curentdir] = os.path.split(tmppath)
            button = widgets.Button(description=curentdir + '/',
                                    tooltip=curentdir,
                                    layout=widgets.Layout(width='auto'))
            button.style.button_color = path_color
            button.path = tmppath_old
            button.on_click(on_click)
            if self.fix_path or len(tmppath) < len_root_path - 1:
                button.disabled = True
                if self._hide_path:
                    button.layout.display = 'none'
            buttons.append(button)
        button = widgets.Button(icon="fa-home",
                                tooltip=self._initial_project_path,
                                layout=widgets.Layout(width='auto'))
        button.style.button_color = home_color
        button.path = self._initial_project
        if self.fix_path:
            button.disabled = True
        button.on_click(on_click)
        buttons.append(button)
        buttons.reverse()
        box.children = tuple(buttons)

    def _on_click_file(self, filename):
        filepath = os.path.join(self.path, filename)
        self.output.clear_output(True)
        try:
            data = self.project[filename]
        except(KeyError, IOError):
            data = None
        with self.output:
            if data is not None and str(type(data)).split('.')[0] == "<class 'PIL":
                try:
                    data_cp = data.copy()
                    data_cp.thumbnail((800, 800))
                except:
                    data_cp = data
                display(data_cp)
            elif data is not None:
                display(data)
            else:
                print([filename])
        if filepath in self._clickedFiles:
            self._data = None
            self._clickedFiles.remove(filepath)
        else:
            if data is not None:
                self._data = FileData(data=data, file=filename, metadata={"path": filepath})
            # self._clickedFiles.append(filepath)
            self._clickedFiles = [filepath]

    def _update_filebox(self, filebox):
        # color definitions
        dir_color = '#9999FF'
        node_color = '#9999EE'
        file_chosen_color = '#FFBBBB'
        file_color = '#DDDDDD'

        if self._node_as_dirs:
            dirs = self.dirs + self.nodes
            files = self.files
        else:
            files = self.nodes + self.files
            dirs = self.dirs

        def on_click_group(b):
            if self._busy_check():
                return
            path = os.path.join(self.path, b.description)
            self._update_project(path)
            self._busy_check(False)

        def on_click_file(b):
            if self._busy_check():
                return
            self._on_click_file(b.description)
            self._update_filebox(filebox)
            self._busy_check(False)

        buttons = []
        item_layout = widgets.Layout(width='80%',
                                     height='30px',
                                     min_height='24px',
                                     display='flex',
                                     align_items="center",
                                     justify_content='flex-start')

        for f in dirs:
            button = widgets.Button(description=f,
                                    icon="fa-folder",
                                    layout=item_layout)
            button.style.button_color = dir_color
            button.on_click(on_click_group)
            buttons.append(button)

        for f in files:
            button = widgets.Button(description=f,
                                    icon="fa-file-o",
                                    layout=item_layout)
            if os.path.join(self.path, f) in self._clickedFiles:
                button.style.button_color = file_chosen_color
            else:
                button.style.button_color = file_color
            button.on_click(on_click_file)
            buttons.append(button)

        filebox.children = tuple(buttons)

    def _ipython_display_(self):
        """This function is used by Ipython to display the object."""
        display(self.gui())
