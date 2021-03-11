"""Generic File Object."""

# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import json
import os
import pandas
from functools import lru_cache

from pyiron_base import ImportAlarm, FileHDFio

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
    _has_imported['PIL'] = True
    import_alarm = ImportAlarm()
except ImportError:
    _has_imported['PIL'] = False
    import_alarm = ImportAlarm(
        "Reduced functionality, since " + 'PIL' + " could not be imported."
    )


@import_alarm
def load_file(filename, filetype=None):
    """
        Load the file and return an appropriate object containing the data.

        Args:
            filename (str): path to the file to be displayed.
            filetype (str/None): File extension, if given this overwrites the assumption based on the filename.

            Supported file types are:
            '.h5', '.hdf'
            '.json'
            '.txt'
            '.csv'
            Image extensions supported by PIL

        Returns:
            :class:`FileHDFio`: pointing to the file of filetype = '.h5'
            dict: containing data from file of filetype = '.json'
            list: of all lines from file for filetype = '.txt'
            :class:`pandas.DataFrame`: containing data from file of filetype = '.csv'

    """
    def _load_txt(file):
        with open(file, encoding='utf8') as f:
            return f.readlines()

    def _load_json(file):
        with open(file) as f:
            return json.load(f)

    def _load_csv(file):
        return pandas.read_csv(file)

    def _load_img(file):
        return Image.open(file)
 
    def _load_default(file):
        try:
            return _load_txt(file)
        except Exception as e:
            raise IOError("File could not be loaded.") from e

    if filetype is None:
        _, filetype = os.path.splitext(filename)
    elif filetype[0] != '.':
        filetype = '.' + filetype

    if filetype.lower() in ['.h5', '.hdf']:
        return FileHDFio(file_name=filename)
    if filetype.lower() in ['.json']:
        return _load_json(filename)
    elif filetype.lower() in ['.txt']:
        return _load_txt(filename)
    elif filetype.lower() in ['.csv']:
        return _load_csv(filename)
    elif _has_imported['PIL'] and filetype.lower() in Image.registered_extensions():
        return _load_img(filename)
    else:
        return _load_default(filename)


class FileData:
    """FileData stores an instance of a data file, e.g. a single Image from a measurement."""
    def __init__(self, file, data=None, metadata=None, filetype=None):
        """FileData class to store data and associated metadata.

            Args:
                file (str): path to the data file (if data is None) or filename associated with the data.
                data (object/None): object containing data
                metadata (dict/InputList): Dictionary of metadata associated with the data
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
            if filetype == '' or filetype == '.':
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
