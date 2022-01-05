"""Generic File Object."""

# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import json
import os
from abc import ABC, abstractmethod
from functools import lru_cache

import pandas

from pyiron_base import ImportAlarm, FileHDFio, ProjectHDFio

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


_has_imported = {}
try:
    from PIL import Image

    _has_imported["PIL"] = True
    # For some reason I do not know this forces PIL to always be aware of all possible Image extensions.
    Image.registered_extensions()
except ImportError:
    _has_imported["PIL"] = False
try:
    import nbformat, nbconvert

    _has_imported["nbformat"] = True
except ImportError:
    _has_imported["nbformat"] = False

if all(_has_imported.values()):
    import_alarm = ImportAlarm()
else:
    import_alarm = ImportAlarm(
        "Reduced functionality, since "
        + str(
            [package for package in _has_imported.keys() if not _has_imported[package]]
        )
        + " could not be imported."
    )


if _has_imported["nbformat"]:

    class OwnNotebookNode(nbformat.NotebookNode):

        """Wrapper for nbformat.NotebookNode with some additional representation based on nbconvert."""

        def _repr_html_(self):
            html_exporter = nbconvert.HTMLExporter()
            html_exporter.template_name = "classic"
            (html_output, _) = html_exporter.from_notebook_node(self)
            return html_output


@import_alarm
def load_file(fp, filetype=None, project=None):
    """
    Load the file and return an appropriate object containing the data.

    Args:
        fp (str / file): path to the file or file object to be displayed.
        filetype (str/None): File extension, if given this overwrites the assumption based on the filename.
        project (pyiron-Project/None): Project calling this function, provided to all objects referring to such.

        Supported file types are:
        '.h5', '.hdf'
        '.json'
        '.txt'
        '.csv'
        '.ipynb'
        Image extensions supported by PIL

    Returns:
        :class:`FileHDFio`: pointing to the file of filetype = '.h5'
        dict: containing data from file of filetype = '.json'
        list: of all lines from file for filetype = '.txt'
        :class:`pandas.DataFrame`: containing data from file of filetype = '.csv'

    """

    def _load_txt(file):
        if isinstance(file, str):
            with open(file, encoding="utf8") as f:
                return f.readlines()
        else:
            return file.readlines()

    def _load_ipynb(file):
        return OwnNotebookNode(nbformat.read(file, as_version=4))

    def _load_json(file):
        if isinstance(file, str):
            with open(file) as f:
                return json.load(f)
        else:
            return json.load(file)

    def _load_csv(file):
        return pandas.read_csv(file)

    def _load_img(file):
        return Image.open(file)

    def _load_default(file):
        try:
            return _load_txt(file)
        except Exception as e:
            raise IOError("File could not be loaded.") from e

    def _resolve_filetype(file, _filetype):
        if _filetype is None and isinstance(file, str):
            _, _filetype = os.path.splitext(file)
        elif _filetype is None and hasattr(file, "name"):
            _, _filetype = os.path.splitext(file.name)
        elif _filetype is None:
            return None
        elif _filetype[0] != ".":
            _filetype = "." + _filetype
        return _filetype.lower()

    filetype = _resolve_filetype(fp, filetype)

    if filetype in [".h5", ".hdf"] and isinstance(fp, str):
        if project is None:
            return FileHDFio(file_name=fp)
        else:
            return ProjectHDFio(file_name=fp, project=project)
    elif filetype in [".json"]:
        return _load_json(fp)
    elif filetype in [".txt"]:
        return _load_txt(fp)
    elif filetype in [".csv"]:
        return _load_csv(fp)
    elif _has_imported["nbformat"] and filetype in [".ipynb"]:
        return _load_ipynb(fp)
    elif _has_imported["PIL"] and filetype in Image.registered_extensions():
        return _load_img(fp)
    else:
        return _load_default(fp)


class FileDataTemplate(ABC):
    @property
    @abstractmethod
    def data(self):
        """Return the associated data."""
        pass


class FileData(FileDataTemplate):
    """FileData stores an instance of a data file, e.g. a single Image from a measurement."""

    def __init__(self, file, data=None, metadata=None, filetype=None):
        """FileData class to store data and associated metadata.

        Args:
            file (str): path to the data file (if data is None) or filename associated with the data.
            data (object/None): object containing data
            metadata (dict/DataContainer): Dictionary of metadata associated with the data
            filetype (str): File extension associated with the type data,
                            If provided this overwrites the assumption based on the extension of the filename.
        """
        if data is None:
            self.filename = os.path.split(file)[1]
            self.source = file
            self._data = None
        else:
            self.filename = file
            self.source = None
            self._data = data
        if filetype is None:
            filetype = os.path.splitext(self.filename)[1]
            if filetype == "" or filetype == ".":
                self.filetype = None
            else:
                self.filetype = filetype[1:]
        else:
            self.filetype = filetype
        if metadata is None:
            self.metadata = {}
        else:
            self.metadata = metadata
        self._hasdata = True if self._data is not None else False

    @property
    @lru_cache()
    def data(self):
        """Return the associated data."""
        if self._hasdata:
            return self._data
        else:
            return load_file(self.source, filetype=self.filetype)
