"""
Functions for reading and writing data files.
"""
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import os.path
import yaml
import xmltodict
from dicttoxml import dicttoxml
from defusedxml.minidom import parseString
import ast

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
    ext = os.path.basename(file_name).split('.')[1]
    try:
        parser = {
                'yml': _parse_yaml, 'yaml': _parse_yaml,
                'xml': _parse_xml
        }[ext]
        return parser(file_name)
    except KeyError:
        raise ValueError("The input file is not supported; expected *.yml, *.yaml, or *.xml") from None

def write(data, file_name):
    """
    Writes the data to a file.

    Format is determined from the file extension as follows
    - yaml: .yaml, .yml
    - xml: .xml

    Args:
        data (nested dict/list): data to save to file
        file_name (str): file name, extension defines which format is used
    """
    ext = os.path.splitext(file_name)[1]
    if ext == 'yaml' or ext == 'yml':
        to_yml(data, file_name)
    elif ext == 'xml':
        to_xml(data, file_name, attr_flag=False)
    else:
        raise ValueError("The output file is not supported; expected *.yml"
                         ", *.yaml, or *.xml") from None

def parse_yaml(file_name):
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


def parse_xml(file_name, wrap=False):
    """
    Parse a XML file and update the datacontainer with a dictionary

    Args:
        file_name(str): path to the input file; it should be a XML file.

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

def to_yml(data, file_name):
    """
    Writes the DataContainer to a yaml file.

    Args:
        file_name(str): the name of the file to be writen to.
    """
    with open(file_name, 'w') as output:
        yaml.dump(self.to_builtin(), output, default_flow_style=False)

def to_xml(data, file_name, attr_flag=False):
    """
    Writes the DataContainer to an xml file.

    Args:
        file_name(str): the name of the file to be writen to.
        attr_flag(bool): if False, it will not include the type of data
        in xml file if True, it also include the type
        of the data in the xml file.
    """
    xml_data = dicttoxml(self.to_builtin(), attr_type=attr_flag)
    with open(file_name, 'w') as xmlfile:
        xmlfile.write(parseString(xml_data).toprettyxml())
