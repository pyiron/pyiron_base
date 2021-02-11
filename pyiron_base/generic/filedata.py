"""Generic File Object."""

# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import json
import os
import pandas

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
import_alarm = ImportAlarm()
_not_imported = ''
try:
    from PIL import Image
    _has_imported['PIL'] = True
except ImportError:
    _has_imported['PIL'] = False
if all(_has_imported.values()):
    pass
else:
    for k, j in _has_imported.items():
        if j and len(_not_imported) > 0:
            _not_imported += ', '
        if j:
            _not_imported += k
    import_alarm = ImportAlarm(
        "Reduced functionality, since " + _not_imported + " could not be imported."
    )


def load_file(filename):
    """
        Load the file and return an appropriate object containing the data.

        Args:
            filename (str): path to the file to be displayed.
    """
    def _load_txt(file):
        with open(file) as f:
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
        except UnicodeDecodeError:
            return file

    _, filetype = os.path.splitext(filename)
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
    def __init__(self, source=None, data=None, filename=None, metadata=None, filetype=None):
        """FileData class to store data and associated metadata.

            Args:
                source (str/None): path to the data file
                data (object/None): object containing data
                filename (str/None): filename associated with the data object, Not used if source is given!
                metadata (dict/InputList): Dictionary of metadata associated with the data
                filetype (str): File extension associated with the type data,
                                If provided this overwrites the assumption based on the extension of the filename.
        """
        if (source is None) and (data is None):
            raise ValueError("No data given")
        self._data = data
        if source is not None:
            self.filename = os.path.split(source)[1]
            self.source = source
        elif filename is None:
            raise ValueError("No filename given")
        else:
            self.filename = filename
        if (filetype is None) and (self.filename is not None):
            filetype = os.path.splitext(self.filename)[1]
            if len(filetype[1:]) == 0:
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
    def data(self):
        """Return the associated data."""
        if self._hasdata:
            return self._data
        else:
            return load_file(self.source)
