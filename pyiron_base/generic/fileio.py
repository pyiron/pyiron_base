"""
Functions for reading and writing data files.
"""
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import ast
from collections import namedtuple
import os.path
import yaml
import warnings
import xmltodict
from dicttoxml import dicttoxml
from defusedxml.minidom import parseString

__author__ = "Muhammad Hassani, Marvin Poul"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Muhammad Hassani"
__email__ = "hassani@mpie.de"
__status__ = "production"
__date__ = "Feb 26, 2021"


def read(file_name):
    """
    Read data from a file.

    Format is determined from the file extension as follows
    - yaml: .yaml, .yml
    - xml: .xml

    Args:
        file_name (str): file name, extension defines which format is used

    Returns:
        nested dict/list

    Raises:
        ValueError: if given filename doesn't have one of the specified extensions
    """
    return _lookup_adapter(file_name).parse(file_name)

def write(data, file_name):
    """
    Writes the data to a file.

    Format is determined from the file extension as follows
    - yaml: .yaml, .yml
    - xml: .xml

    Args:
        data (nested dict/list): data to save to file, dictionary keys must be str!
        file_name (str): file name, extension defines which format is used

    Raises:
        ValueError: if given filename doesn't have one of the specified extensions
    """
    return _lookup_adapter(file_name).write(data, file_name)


def _lookup_adapter(file_name):
    """Find adapter for filename based on its extensions."""
    ext = os.path.splitext(file_name)[1][1:]
    try:
        return _FILETYPE_DICT[ext]
    except KeyError:
        raise ValueError("The file type is not supported; expected *.yml, *.yaml, or *.xml, not \"{}\"".format(ext)) \
                from None

def _parse_yml(file_name):
    """
    Parse a YAML file as a dict.  Errors during reading raise
    a warning and return an empty dict.

    Args:
        file_name(str): path to the input file; it should be a YAML file.

    Returns:
        dict: parsed file contents
    """
    with open(file_name, 'r') as input_src:
        try:
            return yaml.safe_load(input_src)
        except yaml.YAMLError as exc:
            warnings.warn(exc)
            return {}

def _parse_xml(file_name):
    """
    Parse a XML file into a dictionary.

    Args:
        file_name(str): path to the input file; it should be an XML file.

    Returns:
        dict: parsed file contents
    """

    def _correct_list_item(inp):
        """
        this function fixes issues with parsed_xml file, when there is
        a nested list in the input file.
        The following funtion
        output = xmltodict.parse(
                    input_src.read(), dict_constructor=dict,
                    postprocessor=postprocessor
                )
        returns each list as a nested dictionary with key equals to 'item'
        By this function, the key "item" is removed.
        """
        if isinstance(inp, dict):
            out = {}
            for k in inp.keys():
                if k == 'item':
                    return _correct_list_item(inp[k])
                else:
                    out[k] = _correct_list_item(inp[k])
            return out
        elif isinstance(inp, list):
            out = []
            for val in inp:
                if isinstance(val, dict):
                    out.append(_correct_list_item(val))
                else:
                    out.append(val)
            return out
        else:
            return inp

    def postprocessor(path, key, value):
        if key[0] == 'n' and key[1:].isdigit():
            key = key[1:]
        try:
            return key, ast.literal_eval(value)
        except (ValueError, TypeError):
            return key, value

    with open(file_name) as input_src:
        try:
            output = xmltodict.parse(
                input_src.read(), dict_constructor=dict,
                postprocessor=postprocessor
            )
            output = _correct_list_item(output)
            if 'root' in output.keys():
                return output['root']
            else:
                return output
        except Exception as message:
            warnings.warn(message)
            return {}

def _to_yml(data, file_name):
    """
    Writes the DataContainer to a yaml file.

    Args:
        data (nested dict/list): data to save to file, dictionary keys must be str!
        file_name (str): the name of the file to be writen to.
    """
    with open(file_name, 'w') as output:
        yaml.dump(data, output, default_flow_style=False)

def _to_xml(data, file_name, attr_flag=False):
    """
    Writes the DataContainer to an xml file.

    Args:
        data (nested dict/list): data to save to file, dictionary keys must be str!
        file_name(str): the name of the file to be writen to.
        attr_flag(bool): if False, it will not include the type of data
            in xml file if True, it also include the type
            of the data in the xml file.
    """
    xml_data = dicttoxml(data, attr_type=attr_flag)
    with open(file_name, 'w') as xmlfile:
        xmlfile.write(parseString(xml_data).toprettyxml())

FileAdapter = namedtuple('FileAdapter', ('parse', 'write'))
YMLAdapter = FileAdapter(_parse_yml, _to_yml)
XMLAdapter = FileAdapter(_parse_xml, _to_xml)

_FILETYPE_DICT = {
        'yaml': YMLAdapter,
        'yml': YMLAdapter,
        'xml': XMLAdapter,
}
