# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
GenericParameters class defines the typical input file with a key value structure plus an additional column for comments.
"""

import os
import posixpath
import warnings
from ast import literal_eval
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas

from pyiron_base.interfaces.has_dict import HasDict
from pyiron_base.state import state

__author__ = "Joerg Neugebauer"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


class GenericParameters(HasDict):
    """
    GenericParameters class defines the typical input file with a key value structure plus an additional column for comments.
    Convenience class to easily create, read, and modify input files

    Args:
        table_name (str): name of the input file inside the HDF5 file - optional
        input_file_name (str/Nonetype): name of the input file (if None default parameters are used)
        val_only (bool): input format consists of value (comments) only
        comment_char (str): separator that characterizes comment (e.g. "#" for python)
        separator_char (str): separator that characterizes the split between key and value - default=' '
        end_value_char (str): special character at the end of every line - default=''

    Attributes:

        .. attribute:: file_name

            file name of the input file

        .. attribute:: table_name

            name of the input table inside the HDF5 file

        .. attribute:: val_only

            boolean option to switch from a key value list to an value only input file

        .. attribute:: comment_char

            separator that characterizes comment

        .. attribute:: separator_char

            separator that characterizes the split between key and value

        .. attribute:: multi_word_separator

            multi word separator to have multi word keys

        .. attribute:: end_value_char

            special character at the end of every line

        .. attribute:: replace_char_dict

            dictionary to replace certain character combinations
    """

    def __init__(
        self,
        table_name: Optional[str] = None,
        input_file_name: Optional[Union[str, None]] = None,
        val_only: bool = False,
        comment_char: str = "#",
        separator_char: str = " ",
        end_value_char: str = "",
    ) -> None:
        self.__name__ = "GenericParameters"
        self.__version__ = "0.1"

        self._file_name = None
        self._table_name = None
        self._comment_char = None
        self._val_only = None
        self._separator_char = None
        self._multi_word_separator = None
        self._end_value_char = None
        self._replace_char_dict = None
        self._block_dict = None
        self._bool_dict = {True: "True", False: "False"}
        self._dataset = OrderedDict()
        self._block_line_dict = {}
        self.end_value_char = end_value_char
        self.file_name = input_file_name
        self.table_name = table_name
        self.val_only = val_only
        self.comment_char = comment_char
        self.separator_char = separator_char
        self.multi_word_separator = "___"
        self.read_only = False
        if input_file_name is None:
            self.load_default()
        else:
            self.read_input(self.file_name)

    @property
    def file_name(self) -> Optional[str]:
        """
        Get the file name of the input file

        Returns:
            str: file name
        """
        return self._file_name

    @file_name.setter
    def file_name(self, new_file_name: Optional[Union[str, None]]) -> None:
        """
        Set the file name of the input file

        Args:
            new_file_name (str): file name
        """
        self._file_name = new_file_name

    @property
    def table_name(self) -> Optional[str]:
        """
        Get the name of the input table inside the HDF5 file

        Returns:
            str: table name
        """
        return self._table_name

    @table_name.setter
    def table_name(self, new_table_name: Optional[str]) -> None:
        """
        Set the name of the input table inside the HDF5 file

        Args:
            new_table_name (str): table name
        """
        self._table_name = new_table_name

    @property
    def val_only(self) -> Optional[bool]:
        """
        Get the boolean option to switch from a key value list to an value only input file

        Returns:
            bool: [True/False]
        """
        return self._val_only

    @val_only.setter
    def val_only(self, val_only: bool) -> None:
        """
        Set the boolean option to switch from a key value list to an value only input file

        Args:
            val_only (bool): [True/False]
        """
        self._val_only = val_only

    @property
    def comment_char(self) -> Optional[str]:
        """
        Get the separator that characterizes comment

        Returns:
            str: comment character
        """
        return self._comment_char

    @comment_char.setter
    def comment_char(self, new_comment_char: str) -> None:
        """
        Set the separator that characterizes comment

        Args:
            new_comment_char (str): comment character
        """
        self._comment_char = new_comment_char

    @property
    def separator_char(self) -> Optional[str]:
        """
        Get the separator that characterizes the split between key and value

        Returns:
            str: separator character
        """
        return self._separator_char

    @separator_char.setter
    def separator_char(self, new_separator_char: str) -> None:
        """
        Set the separator that characterizes the split between key and value

        Args:
            new_separator_char (str): separator character
        """
        self._separator_char = new_separator_char

    @property
    def multi_word_separator(self) -> Optional[str]:
        """
        Get the multi word separator to have multi word keys

        Returns:
            str: multi word separator
        """
        return self._multi_word_separator

    @multi_word_separator.setter
    def multi_word_separator(self, new_multi_word_separator: str) -> None:
        """
        Set the multi word separator to have multi word keys

        Args:
            new_multi_word_separator (str): multi word separator
        """
        self._multi_word_separator = new_multi_word_separator

    @property
    def end_value_char(self) -> Optional[str]:
        """
        Get the special character at the end of every line

        Returns:
            str: end of line character
        """
        return self._end_value_char

    @end_value_char.setter
    def end_value_char(self, new_end_value_char: str) -> None:
        """
        Set the special character at the end of every line

        Args:
            new_end_value_char (str): end of line character
        """
        self._end_value_char = new_end_value_char

    @property
    def replace_char_dict(self) -> Optional[Dict[str, str]]:
        """
        Get the dictionary to replace certain character combinations

        Returns:
            dict: character replace dictionary
        """
        return self._replace_char_dict

    @replace_char_dict.setter
    def replace_char_dict(self, new_replace_char_dict: Dict[str, str]) -> None:
        """
        Set the dictionary to replace certain character combinations

        Args:
            new_replace_char_dict (dict): character replace dictionary
        """
        self._replace_char_dict = new_replace_char_dict

    def _read_only_check_dict(
        self, new_dict: Dict[str, List[Union[str, bool]]]
    ) -> None:
        if self.read_only and new_dict != self._dataset:
            self._read_only_error()

    @staticmethod
    def _read_only_error() -> None:
        warnings.warn(
            "The input in GenericParameters changed, while the state of the job was already finished."
        )

    def load_string(self, input_str: str) -> None:
        """
        Load a multi line string to overwrite the current parameter settings

        Args:
            input_str (str): multi line string
        """
        new_dict = self._lines_to_dict(input_str.splitlines())
        self._read_only_check_dict(new_dict=new_dict)
        self._dataset = new_dict

    def load_default(self) -> None:
        """
        Load defaults resets the dataset in the background to be empty
        """
        new_dict = OrderedDict()
        new_dict["Parameter"] = []
        new_dict["Value"] = []
        new_dict["Comment"] = []
        self._read_only_check_dict(new_dict=new_dict)
        self._dataset = new_dict

    def keys(self) -> List[str]:
        """
        Return keys of GenericParameters object
        """
        if self.val_only:
            return []
        else:
            return self._dataset["Parameter"]

    def read_input(self, file_name: str, ignore_trigger: Optional[str] = None) -> None:
        """
        Read input file and store the data in GenericParameters - this overwrites the current parameter settings

        Args:
            file_name (str): absolute path to the input file
            ignore_trigger (str): trigger for lines to be ignored
        """
        state.logger.debug("file: %s %s", file_name, os.path.isfile(file_name))
        if not os.path.isfile(file_name):
            raise ValueError("file does not exist: " + file_name)
        with open(file_name, "r") as f:
            lines = f.readlines()
            new_lines = np.array(lines).tolist()
            if ignore_trigger is not None:
                del_ind = list()
                for i, line in enumerate(lines):
                    line = line.strip()
                    if len(line.split()) > 0:
                        if ignore_trigger == line.strip()[0]:
                            del_ind.append(i)
                        elif ignore_trigger in line:
                            lines[i] = line[: line.find("!")]
                lines = np.array(lines)
                new_lines = lines[np.setdiff1d(np.arange(len(lines)), del_ind)]
        new_dict = self._lines_to_dict(new_lines)
        self._read_only_check_dict(new_dict=new_dict)
        self._dataset = new_dict

    def get_pandas(self) -> pandas.DataFrame:
        """
        Output the GenericParameters object as Pandas Dataframe for human readability.

        Returns:
            pandas.DataFrame: Pandas Dataframe of the GenericParameters object
        """
        return pandas.DataFrame(self._dataset)

    def get(
        self, parameter_name: str, default_value: Optional[str] = None
    ) -> Union[str, None]:
        """
        Get the value of a specific parameter from GenericParameters - if the parameter is not available return
        default_value if that is set.

        Args:
            parameter_name (str): parameter key
            default_value (str): default value to return is the parameter is not set

        Returns:
            str: value of the parameter
        """
        i_line = self._find_line(parameter_name)
        if i_line > -1:
            val = self._dataset["Value"][i_line]
            try:
                val_v = literal_eval(val)
            except (ValueError, SyntaxError):
                val_v = val
            if callable(val_v):
                val_v = val
            return val_v
        elif default_value is not None:
            return default_value
        else:
            raise NameError("parameter not found: " + parameter_name)

    def get_attribute(self, attribute_name: str) -> Union[str, None]:
        """
        Get the value of a specific parameter from GenericParameters

        Args:
            attribute_name (str): parameter key

        Returns:
            str: value of the parameter
        """
        if "_attributes" not in dir(self):
            return None
        i_line = np.where(np.array(self._attributes["Parameter"]) == attribute_name)[0]
        if i_line > -1:
            return self._attributes["Value"][i_line]
        else:
            return None

    def modify(
        self,
        separator: Optional[str] = None,
        append_if_not_present: bool = False,
        **modify_dict: Union[str, bool],
    ) -> None:
        """
        Modify values for existing parameters. The command is called as modify(param1=val1, param2=val2, ...)

        Args:
            separator (str): needed if the parameter name contains special characters such as par:
                       use then as input: modify(separator=":", par=val) - optional
            append_if_not_present (bool): do not raise an exception but append the parameter in practice use set(par=val)
                                          - default=False
            **modify_dict (dict): dictionary of parameter names and values
        """
        if separator is not None:
            modify_dict = {k + separator: v for k, v in modify_dict.items()}

        for key, val in modify_dict.items():
            i_key = self._find_line(key)
            if i_key == -1:
                if append_if_not_present:
                    self._append(**{key: val})
                    continue
                else:
                    raise ValueError("key for modify not found " + key)
            if isinstance(val, tuple):
                val, comment = val
                if self.read_only and self._dataset["Comment"][i_key] != comment:
                    self._read_only_error()
                self._dataset["Comment"][i_key] = comment
            if self.read_only and str(self._dataset["Value"][i_key]) != str(val):
                self._read_only_error()
            self._dataset["Value"][i_key] = str(val)

    def set(
        self, separator: Optional[str] = None, **set_dict: Union[str, bool]
    ) -> None:
        """
        Set the value of multiple parameters or create new parameter key, if they do not exist already.

        Args:
            separator (float/int/str): separator string - optional
            **set_dict (dict): dictionary containing the parameter keys and their corresponding values to be set
        """
        self.modify(separator=separator, append_if_not_present=True, **set_dict)

    def set_value(self, line: int, val: Union[str, bytes]) -> None:
        """
        Set the value of a parameter in a specific line

        Args:
            line (float/int/str): line number - starting with 0
            val (str/bytes): value to be set
        """
        if line < len(self._dataset["Value"]):
            if self.read_only and self._dataset["Value"][line] != val:
                self._read_only_error()
            self._dataset["Value"][line] = val
        elif line >= len(self._dataset["Value"]):
            new_array = []
            new_comments = []
            new_params = []
            for el in self._dataset["Value"]:
                new_array.append(el)
                new_comments.append("")
                new_params.append("")
            new_array.append(val)
            new_comments.append("")
            new_params.append("")
            new_dict = OrderedDict()
            new_dict["Value"] = new_array
            new_dict["Comment"] = new_comments
            new_dict["Parameter"] = new_params
            self._read_only_check_dict(new_dict=new_dict)
            self._dataset = new_dict
        else:
            raise ValueError("Wrong indexing")

    def remove_keys(self, key_list: List[str]) -> None:
        """
        Remove a list of keys from the GenericParameters

        Args:
            key_list (list): list of keys to be removed
        """
        if self.read_only and any([k in self._dataset["Parameter"] for k in key_list]):
            self._read_only_error()
        for key in key_list:
            params = np.array(self._dataset["Parameter"])
            i_keys = np.where(params == key)[0]
            if len(i_keys) == 0:
                continue
            if i_keys[0] == -1:
                continue
            for i_key in i_keys[::-1]:
                self._delete_line(i_key)

    def define_blocks(self, block_dict: Dict[str, List[str]]) -> None:
        """
        Define a block section within the GenericParameters

        Args:
            block_dict (dict): dictionary to define the block
        """
        if not isinstance(block_dict, OrderedDict):
            raise AssertionError()
        self._block_dict = block_dict

    def _to_dict(self) -> Dict[str, List[Union[str, bool]]]:
        """
        Convert the GenericParameters object to a dictionary for storage

        Returns:
            dict: GenericParameters object as a dictionary
        """
        return {"data_dict": self._dataset}

    def _from_dict(
        self, obj_dict: Dict[str, List[Union[str, bool]]], version: Optional[str] = None
    ) -> None:
        """
        Reload GenericParameters from dictionary

        Args:
            obj_dict (dict): dictionary for reloading GenericParameters object
            version (str, optional): version of the HasDict format that is used
        """
        self._dataset = obj_dict["data_dict"]

    def to_hdf(self, hdf, group_name: Optional[str] = None) -> None:
        """
        Store the GenericParameters in an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object
            group_name (str): HDF5 subgroup name - optional
        """
        if group_name:
            h5_path = group_name + "/" + self.table_name
        else:
            h5_path = self.table_name
        with hdf.open(h5_path) as hdf_group:
            for k, v in self.to_dict().items():
                hdf_group[k] = v

    def from_hdf(self, hdf, group_name: Optional[str] = None) -> None:
        """
        Restore the GenericParameters from an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object
            group_name (str): HDF5 subgroup name - optional
        """
        if group_name:
            with hdf.open(group_name) as hdf_group:
                data = hdf_group[self.table_name]
        else:
            data = hdf[self.table_name]
        if isinstance(data, dict):
            self.from_dict(obj_dict={"data_dict": data})
        else:
            self.from_dict(obj_dict={"data_dict": data._read("data_dict")})

    def get_string_lst(self) -> List[str]:
        """
        Get list of strings from GenericParameters to write to input file

        Returns:
            List[str]: List of strings representing the GenericParameters object
        """
        tab_dict = self._dataset
        if "Parameter" not in tab_dict:
            tab_dict["Parameter"] = ["" for _ in tab_dict["Value"]]

        string_lst = []
        if self.val_only:
            value_lst = tab_dict["Value"]
        else:
            try:
                value_lst = [self[p] for p in tab_dict["Parameter"]]
            except ValueError:
                value_lst = tab_dict["Value"]
        for par, v, c in zip(tab_dict["Parameter"], value_lst, tab_dict["Comment"]):
            if isinstance(v, bool):
                v_str = self._bool_dict[v]
            elif isinstance(v, str):
                v_str = v
            else:
                v_str = str(v)

            par = " ".join(par.split(self.multi_word_separator))
            if par == "Comment":
                string_lst.append(str(v) + self.end_value_char + "\n")
            elif c.strip() == "":
                if self.val_only:
                    string_lst.append(v_str + self.end_value_char + "\n")
                else:
                    string_lst.append(
                        par + self.separator_char + v_str + self.end_value_char + "\n"
                    )
            else:
                if self.val_only:
                    string_lst.append(
                        v_str + self.end_value_char + " " + self.comment_char + c + "\n"
                    )
                else:
                    string_lst.append(
                        par
                        + self.separator_char
                        + v_str
                        + " "
                        + self.end_value_char
                        + self.comment_char
                        + c
                        + "\n"
                    )
        return string_lst

    def write_file(self, file_name: str, cwd: Optional[str] = None) -> None:
        """
        Write GenericParameters to input file

        Args:
            file_name (str): name of the file, either absolute (then cwd must be None) or relative
            cwd (str): path name (default: None)
        """
        if cwd is not None:
            file_name = posixpath.join(cwd, file_name)

        with open(file_name, "w") as f:
            for line in self.get_string_lst():
                f.write(line)

    def __repr__(self) -> str:
        """
        Human readable string representation

        Returns:
            str: pandas Dataframe structure as string
        """
        return str(self.get_pandas())

    def __setitem__(self, key: Union[int, str], value: Union[float, int, str]) -> None:
        """
        Set a value for the corresponding key

        Args:
            key (Union[int, str]): key to be set or modified
            value (Union[float, int, str]): value to be set
        """
        if isinstance(key, int):
            if self.read_only and self._dataset["Value"][key] != value:
                self._read_only_error()
            self._dataset["Value"][key] = value
        else:
            self.set(**{key: value})

    def set_dict(self, dictionary: Dict[str, Union[str, bool]]) -> None:
        """
        Set a dictionary of key value pairs

        Args:
            dictionary (dict): dictionary of key value pairs
        """
        self.set(**dictionary)

    def __getitem__(self, item: Union[int, str]) -> str:
        """
        Get a value for the corresponding key

        Args:
            item (int, str): key

        Returns:
            str: value
        """
        if isinstance(item, int):
            return self._dataset["Value"][item]
        elif item in self._dataset["Parameter"]:
            return self.get(item)

    def __delitem__(self, key: str) -> None:
        """
        Delete a key from GenericParameters

        Args:
            key (str): single key
        """
        self.remove_keys([key])

    def _get_block(self, block_name: str) -> Dict[str, List[Union[str, bool]]]:
        """
        Internal helper function to get a block by name

        Args:
            block_name (str): block name

        Returns:
            dict: dictionary of the specific block
        """
        if block_name not in self._block_dict:
            raise ValueError("unknown block: " + block_name)
        keys = self._dataset["Parameter"]
        block_dict = OrderedDict()
        for key in self._dataset:
            block_dict[key] = []
        for i, tag in enumerate(keys):
            if tag in self._block_dict[block_name]:
                for key in self._dataset:
                    block_dict[key].append(self._dataset[key][i])
        return block_dict

    def _get_attributes(self) -> Dict[str, List[str]]:
        """
        Internal helper function to extract pyiron specific commands (start in comments with " @my_command")

        Returns:
            dict: Dictionary containing the parameter tags and values
        """
        tags = self._dataset["Parameter"]
        lst_tag, lst_val = [], []
        for i, tag in enumerate(tags):
            if tag not in ["Comment"]:
                continue
            c = self._dataset["Value"][i]
            s_index = c.find(" @")
            if s_index > -1:
                tag, val = c[s_index:].split()[:2]
                lst_tag.append(tag[1:])
                lst_val.append(val)
        self._attributes = {"Parameter": lst_tag, "Value": lst_val}
        return self._attributes

    def _remove_block(self, block_name: str) -> None:
        """
        Internal helper function to remove a block by name

        Args:
            block_name (str): block name
        """
        if block_name not in self._block_dict:
            raise ValueError("unknown block to be removed")
        self.remove_keys(self._block_dict[block_name])

    def _insert_block(
        self,
        block_dict: Dict[str, List[Union[str, bool]]],
        next_block: Optional[str] = None,
    ) -> None:
        """
        Internal helper function to insert a block by name

        Args:
            block_dict (dict): block dictionary
            next_block (str): name of the following block - optional
        """
        if next_block is None:  # append
            for key in block_dict:
                self._dataset[key] += block_dict[key]
        else:
            for i, tag in enumerate(self._dataset["Parameter"]):
                if tag in self._block_dict[next_block]:
                    self._insert(line_number=i, data_dict=block_dict)  # , shift=1)
                    break

    def _update_block(self, block_dict: Dict[str, List[Union[str, bool]]]) -> None:
        """
        Internal helper function to update a block by name

        Args:
            block_dict (dict): block dictionary containing the parameter tags and values
        """
        tag_lst = block_dict["Parameter"]
        val_lst = block_dict["Value"]
        par_dict = {}
        for t, v in zip(tag_lst, val_lst):
            par_dict[t] = v
        self.modify(**par_dict)

    def _delete_line(self, line_number: int) -> None:
        """
        Internal helper function to delete a single line

        Args:
            line_number (int): line number
        """
        if self.read_only:
            self._read_only_error()
        for key, val in self._dataset.items():
            if "numpy" in str(type(val)):
                val = val.tolist()
            del val[line_number]
            self._dataset[key] = val

    def _insert(
        self,
        line_number: int,
        data_dict: Dict[str, List[Union[str, bool]]],
        shift: int = 0,
    ) -> None:
        """
        Internal helper function to insert a single line by line number

        Args:
            line_number (int): line number
            data_dict (dict): data dictionary
            shift (int): shift line number - default=0
        """
        if self.read_only:
            self._read_only_error()
        for key, val in data_dict.items():
            lst = self._dataset[key]
            val = np.array(val).tolist()
            lst = np.array(lst).tolist()
            self._dataset[key] = lst[: line_number - shift] + val + lst[line_number:]

    def _refresh_block_line_hash_table(self) -> None:
        """
        Internal helper function to refresh the block dictionary hash table
        """
        self._block_line_dict = {}
        for i_line, par in enumerate(self._dataset["Parameter"]):
            if par.strip() == "":
                continue
            for key, val in self._block_dict.items():
                par_single = par.split()[0].split(self.multi_word_separator)[0]
                if par_single in val:
                    if key in self._block_line_dict:
                        self._block_line_dict[key].append(i_line)
                    else:
                        self._block_line_dict[key] = [i_line]
                    break
        i_line_old = 0
        for key in self._block_dict:
            if key in self._block_line_dict:
                i_line_old = np.max(self._block_line_dict[key])
            else:
                self._block_line_dict[key] = [i_line_old]

    def _append_line_in_block(self, parameter_name: str, value: str) -> bool:
        """
        Internal helper function to append a line within a block

        Args:
            parameter_name (str): name of the parameter
            value (str): value of the parameter

        Returns:
            bool: True if the line was successfully appended, False otherwise
        """
        for key, val in self._block_dict.items():
            par_first = parameter_name.split()[0].split(self.multi_word_separator)[0]
            if par_first in val:
                i_last_block_line = max(self._block_line_dict[key])
                self._insert(
                    line_number=i_last_block_line + 1,
                    data_dict={
                        "Parameter": [parameter_name],
                        "Value": [str(value)],
                        "Comment": [""],
                    },
                )
                return True
        else:
            state.logger.warning(
                "Unknown parameter (does not exist in block_dict): {}".format(
                    parameter_name
                )
            )
        return False

    def _append(self, **qwargs: Union[str, Tuple[str, str]]) -> None:
        """
        Internal helper function to append data to the GenericParameters object

        Args:
            **qwargs (dict): dictionary with parameter keys and their corresponding values
                The values can be either a string or a tuple of string and comment.
        """
        if self.read_only:
            self._read_only_error()
        for par, val in qwargs.items():
            if par in self._dataset["Parameter"]:
                raise ValueError("Parameter exists already: " + par)

            if self._block_dict is not None:
                self._refresh_block_line_hash_table()
                if self._append_line_in_block(par, val):
                    continue

            for col in self._dataset:
                self._dataset[col] = np.array(self._dataset[col], dtype=object).tolist()

            comment = ""
            if isinstance(val, tuple):
                val, comment = val
            self._dataset["Parameter"].append(par)
            self._dataset["Value"].append(val)
            self._dataset["Comment"].append(comment)

    def _is_multi_word_parameter(self, key: str) -> bool:
        """
        Internal helper function to check if a parameter included multiple words

        Args:
            key (str): parameter

        Returns:
            bool: True if the parameter includes multiple words, False otherwise
        """
        par_list = key.split(self.multi_word_separator)
        return len(par_list) > 1

    def _repr_html_(self) -> str:
        """
        Internal helper function to represent the GenericParameters object within the Jupyter Framework

        Returns:
            str: HTML representation of the GenericParameters object
        """
        return self.get_pandas()._repr_html_()

    def _lines_to_dict(self, lines: List[str]) -> Dict[str, List[Union[str, bool]]]:
        """
        Internal helper function to convert multiple lines to a dictionary

        Args:
            lines (list): list of lines

        Returns:
            dict: GenericParameters dictionary
        """
        lst = OrderedDict()
        lst["Parameter"] = []
        lst["Value"] = []
        lst["Comment"] = []
        for line in lines:
            if self.replace_char_dict is not None:
                for key, val in self.replace_char_dict.items():
                    line = line.replace(key, val)

            sep = line.split(self.comment_char)
            if len(line.strip()) > 0 and line.strip()[0] == self.comment_char:
                lst["Parameter"].append("Comment")
                lst["Value"].append(self._bool_str_to_bool(line[:-1]))
                lst["Comment"].append("")
            elif not sep[0].strip() == "":
                sep[0] = sep[0].strip()
                if self.val_only:
                    val = sep[0]
                    name = ""
                else:
                    keypos = sep[0].find(self.separator_char)
                    if keypos == -1:
                        name = sep[0]
                        val = ""
                    else:
                        name = sep[0][0:keypos]
                        val = sep[0][keypos + len(self.separator_char) :]
                lst["Parameter"].append(name.strip())
                lst["Value"].append(self._bool_str_to_bool(val.strip()))
                if len(sep) > 1:
                    lst["Comment"].append(sep[-1].strip())
                else:
                    lst["Comment"].append("")
            else:
                lst["Parameter"].append("")
                lst["Value"].append("")
                lst["Comment"].append("")
        return lst

    def _find_line(self, key_name: str) -> Union[int, List[int]]:
        """
        Internal helper function to find a line by key name

        Args:
            key_name (str): key name

        Returns:
            Union[int, List[int]]: line index if a single occurrence is found, list of line indices if multiple occurrences are found, -1 if no occurrence is found
        """
        params = self._dataset["Parameter"]
        if len(params) > 0:
            i_line_lst = np.where(np.array(params) == key_name)[0]
        else:
            i_line_lst = []
        if len(i_line_lst) == 0:
            return -1
        elif len(i_line_lst) == 1:
            return i_line_lst[0]
        else:
            error_msg = list()
            error_msg.append(
                "Multiple occurrences of key_name: "
                + key_name
                + ". They are as follows"
            )
            for i in i_line_lst:
                error_msg.append(
                    "dataset: {}, {}, {}".format(
                        i, self._dataset["Parameter"][i], self._dataset["Value"][i]
                    )
                )
            error_msg = "\n".join(error_msg)
            raise ValueError(error_msg)

    def clear_all(self) -> None:
        """
        Clears all fields in the object
        """
        self._dataset["Parameter"] = []
        self._dataset["Value"] = []
        self._dataset["Comment"] = []

    def _bool_str_to_bool(self, val: str) -> Union[str, bool]:
        """
        Convert a string representation of a boolean value to a boolean

        Args:
            val (str): The string representation of the boolean value

        Returns:
            Union[str, bool]: The converted boolean value
        """
        for key, value in self._bool_dict.items():
            if val == value:
                return key
        return val
