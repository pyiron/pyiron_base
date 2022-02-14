# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Classes to map the Python objects to HDF5 data structures
"""

import numbers
import h5py
import os
from collections.abc import MutableMapping
import importlib
import pandas
import posixpath
import h5io
import numpy as np
import sys
from typing import Union

from pyiron_base.generic.util import deprecate

from pyiron_base.interfaces.has_groups import HasGroups
from pyiron_base.state import state
import warnings

__author__ = "Joerg Neugebauer, Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


def _is_ragged_in_1st_dim_only(value: Union[np.ndarray, list]) -> bool:
    """
    Checks whether array or list of lists is ragged in the first dimension.

    That means all other dimensions (except the first one) still have to match.

    Args:
        value (ndarray/list): array to check

    Returns:
        bool: True if elements of value are not all of the same shape
    """
    if isinstance(value, np.ndarray) and value.dtype != np.dtype("O"):
        return False
    else:

        def extract_dims(v):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                s = np.shape(v)
            return s[0], s[1:]

        dim1, dim_other = zip(*map(extract_dims, value))
        return len(set(dim1)) > 1 and len(set(dim_other)) == 1


def open_hdf5(filename, mode="r", swmr=False):
    if swmr and mode != "r":
        store = h5py.File(filename, mode=mode, libver="latest")
        store.swmr = True
        return store
    else:
        return h5py.File(filename, mode=mode, libver="latest", swmr=swmr)


class FileHDFio(HasGroups, MutableMapping):
    """
    Class that provides all info to access a h5 file. This class is based on h5io.py, which allows to
    get and put a large variety of jobs to/from h5

    Implements :class:`.HasGroups`.  Groups are HDF groups in the file, nodes are HDF datasets.

    Args:
        file_name (str): absolute path of the HDF5 file
        h5_path (str): absolute path inside the h5 path - starting from the root group
        mode (str): mode : {'a', 'w', 'r', 'r+'}, default 'a'
                    See HDFStore docstring or tables.open_file for info about modes

    .. attribute:: file_name
        absolute path to the HDF5 file
    .. attribute:: h5_path
        path inside the HDF5 file - also stored as absolute path
    .. attribute:: history
        previously opened groups / folders
    .. attribute:: file_exists
        boolean if the HDF5 was already written
    .. attribute:: base_name
        name of the HDF5 file but without any file extension
    .. attribute:: file_path
        directory where the HDF5 file is located
    .. attribute:: is_root
        boolean if the HDF5 object is located at the root level of the HDF5 file
    .. attribute:: is_open
        boolean if the HDF5 file is currently opened - if an active file handler exists
    .. attribute:: is_empty
        boolean if the HDF5 file is empty
    """

    def __init__(self, file_name, h5_path="/", mode="a"):
        file_name += ".h5" if not file_name.endswith(".h5") else ""
        if not os.path.isabs(file_name):
            raise ValueError("file_name must be given as absolute path name")
        self._file_name = None
        self.file_name = file_name
        self.history = []
        self.h5_path = h5_path
        self._filter = ["groups", "nodes", "objects"]

    # MutableMapping Impl
    def __contains__(self, item):
        nodes_groups = self.list_all()
        return item in nodes_groups["nodes"] or item in nodes_groups["groups"]

    def __len__(self):
        nodes_groups = self.list_all()
        return len(nodes_groups["nodes"]) + len(nodes_groups["groups"])

    def __iter__(self):
        return iter(self.keys())

    def __getitem__(self, item):
        """
        Get/ read data from the HDF5 file

        Args:
            item (str, slice): path to the data or key of the data object

        Returns:
            dict, list, float, int: data or data object
        """
        if isinstance(item, slice):
            if not (item.start or item.stop or item.step):
                return self.values()
            raise NotImplementedError("Implement if needed, e.g. for [:]")
        else:
            try:
                # fast path, a good amount of accesses will want to fetch a specific dataset it knows exists in the
                # file, there's therefor no point in checking whether item is a group or a node or even worse recursing
                # in case when item contains '/'.  In most cases read_hdf5 will grab the correct data straight away and
                # if not we will still check thoroughly below.  Since list_nodes()/list_groups() each open the
                # underlying file once, this reduces the number of file opens in the most-likely case from 2 to 1 (1 to
                # check whether the data is there and 1 to read it) and increases in the worst case from 1 to 2 (1 to
                # try to read it here and one more time to verify it's not a group below).
                obj = h5io.read_hdf5(self.file_name, title=self._get_h5_path(item))
                if self._is_convertable_dtype_object_array(obj):
                    obj = self._convert_dtype_obj_array(obj.copy())
                return obj
            except (ValueError, OSError):
                # h5io couldn't find a dataset with name item, but there still might be a group with that name, which we
                # check in the rest of the method
                pass

            item_lst = item.split("/")
            if len(item_lst) == 1 and item_lst[0] != "..":
                # if item in self.list_nodes() we would have caught it in the fast path above
                if item in self.list_groups():
                    with self.open(item) as hdf_item:
                        obj = hdf_item.copy()
                        if self._is_convertable_dtype_object_array(obj):
                            obj = self._convert_dtype_obj_array(obj)
                        return obj
                raise ValueError(
                    "Unknown item: {} {} {}".format(item, self.file_name, self.h5_path)
                )
            else:
                if (
                    item_lst[0] == ""
                ):  # item starting with '/', thus we have an absoute HDF5 path
                    item_abs_lst = os.path.normpath(item).replace("\\", "/").split("/")
                else:  # relative HDF5 path
                    # The self.h5_path is an absolute path (/h5_path/in/h5/file), however, to
                    # reach any directory super to root, we start with a
                    # relative path = ./h5_path/in/h5/file and add whatever we get as item.
                    # The normpath finally returns a path to the item which is relative to the hdf-root.
                    item_abs_lst = (
                        os.path.normpath(os.path.join("." + self.h5_path, item))
                        .replace("\\", "/")
                        .split("/")
                    )
                # print('h5_path=', self.h5_path, 'item=', item, 'item_abs_lst=', item_abs_lst)
                if item_abs_lst[0] == "." and len(item_abs_lst) == 1:
                    # Here, we are asked to return the root of the HDF5-file. The resulting self.path would be the
                    # same as the self.file_path and, thus, the path of the pyiron Project this HDF5-file belongs to:
                    return self.create_project_from_hdf5()
                elif item_abs_lst[0] == "..":
                    # Here, we are asked to return a path super to the root of the HDF5-file, a.k.a. the path of it's
                    # pyiron Project, thus we pass the relative path to the pyiron Project to handle it:
                    return self.create_project_from_hdf5()["/".join(item_abs_lst)]
                else:
                    hdf_object = self.copy()
                    hdf_object.h5_path = "/".join(item_abs_lst[:-1])
                    return hdf_object[item_abs_lst[-1]]

    # TODO: remove this function upon 1.0.0 release
    @staticmethod
    def _is_convertable_dtype_object_array(obj):
        if isinstance(obj, np.ndarray) and obj.dtype == np.dtype(object):
            first_element = obj[(0,) * obj.ndim]
            last_element = obj[(-1,) * obj.ndim]
            if (
                isinstance(first_element, numbers.Number)
                and isinstance(last_element, numbers.Number)
                and not _is_ragged_in_1st_dim_only(obj)
            ):
                return True
        return False

    # TODO: remove this function upon 1.0.0 release
    @staticmethod
    def _convert_dtype_obj_array(obj: np.ndarray):
        result = np.array(obj.tolist())
        if result.dtype != np.dtype(object):
            state.logger.warning(
                f"Deprecated data structure! "
                f"Returned array was converted from dtype='O' to dtype={result.dtype} "
                f"via `np.array(result.tolist())`.\n"
                f"Please run rewrite_hdf5() (from a job: job.project_hdf5.rewrite_hdf5() ) to update this data! "
                f"To update all your data run Project.maintenance.update.base_v0_3_to_v0_4('all')."
            )
            return result
        else:
            return obj

    def __setitem__(self, key, value):
        """
        Store data inside the HDF5 file

        Args:
            key (str): key to store the data
            value (pandas.DataFrame, pandas.Series, dict, list, float, int): basically any kind of data is supported
        """
        use_json = True
        if hasattr(value, "to_hdf") & (
            not isinstance(value, (pandas.DataFrame, pandas.Series))
        ):
            value.to_hdf(self, key)
        elif (
            isinstance(value, (list, np.ndarray))
            and len(value) > 0
            and isinstance(value[0], (list, np.ndarray))
            and len(value[0]) > 0
            and not isinstance(value[0][0], str)
            and _is_ragged_in_1st_dim_only(value)
        ):
            # if the sub-arrays in value all share shape[1:], h5io comes up with a more efficient storage format than
            # just writing a dataset for each element, by concatenating along the first axis and storing the indices
            # where to break the concatenated array again
            value = np.array([np.asarray(v) for v in value], dtype=object)
            use_json = False
        elif isinstance(value, tuple):
            value = list(value)
        h5io.write_hdf5(
            self.file_name,
            value,
            title=posixpath.join(self.h5_path, key),
            overwrite="update",
            use_json=use_json,
        )

    def __delitem__(self, key):
        """
        Delete item from the HDF5 file

        Args:
            key (str): key of the item to delete
        """
        if self.file_exists:
            try:
                with open_hdf5(self.file_name, mode="a") as store:
                    del store[key]
            except (AttributeError, KeyError):
                pass

    @property
    def file_exists(self):
        """
        Check if the HDF5 file exists already

        Returns:
            bool: [True/False]
        """
        if os.path.isfile(self.file_name):
            return True
        else:
            return False

    @property
    def file_name(self):
        """
        Get the file name of the HDF5 file

        Returns:
            str: absolute path to the HDF5 file
        """
        return self._file_name

    @file_name.setter
    def file_name(self, new_file_name):
        """
        Set the file name of the HDF5 file

        Args:
            new_file_name (str): absolute path to the HDF5 file
        """
        self._file_name = os.path.abspath(new_file_name).replace("\\", "/")

    @property
    def base_name(self):
        """
        Name of the HDF5 file - but without the file extension .h5

        Returns:
            str: file name without the file extension
        """
        return ".".join(posixpath.basename(self.file_name).split(".")[:-1])

    @property
    def file_path(self):
        """
        Path where the HDF5 file is located - posixpath.dirname()

        Returns:
            str: HDF5 file location
        """
        return posixpath.dirname(self.file_name)

    @property
    def h5_path(self):
        """
        Get the path in the HDF5 file starting from the root group - meaning this path starts with '/'

        Returns:
            str: HDF5 path
        """
        return self._h5_path

    @h5_path.setter
    def h5_path(self, path):
        """
        Set the path in the HDF5 file starting from the root group

        Args:
            path (str): HDF5 path
        """
        if (path is None) or (path == ""):
            path = "/"
        self._h5_path = posixpath.normpath(path)
        if not posixpath.isabs(self._h5_path):
            self._h5_path = "/" + self._h5_path

    @property
    def is_root(self):
        """
        Check if the current h5_path is pointing to the HDF5 root group.

        Returns:
            bool: [True/False]
        """
        return "/" == self.h5_path

    # @property
    # def is_open(self):
    #     """
    #     Check if the HDF5 file is currently opened in h5py
    #
    #     Returns:
    #         bool: [True/False]
    #     """
    #     try:
    #         return self._store.is_open
    #     except AttributeError:
    #         return False

    @property
    def is_empty(self):
        """
        Check if the HDF5 file is empty

        Returns:
            bool: [True/False]
        """
        if self.file_exists:
            with open_hdf5(self.file_name) as h:
                return len(h.keys()) == 0
        else:
            return True

    @staticmethod
    def file_size(hdf):
        """
        Get size of the HDF5 file

        Args:
            hdf (FileHDFio): hdf file

        Returns:
            float: file size in Bytes
        """
        return os.path.getsize(hdf.file_name)

    def get_size(self, hdf):
        """
        Get size of the groups inside the HDF5 file

        Args:
            hdf (FileHDFio): hdf file

        Returns:
            float: file size in Bytes
        """
        return sum([sys.getsizeof(hdf[p]) for p in hdf.list_nodes()]) + sum(
            [self.get_size(hdf[p]) for p in hdf.list_groups()]
        )

    def copy(self):
        """
        Copy the Python object which links to the HDF5 file - in contrast to copy_to() which copies the content of the
        HDF5 file to a new location.

        Returns:
            FileHDFio: New FileHDFio object pointing to the same HDF5 file
        """
        new_h5 = FileHDFio(file_name=self.file_name, h5_path=self.h5_path)
        new_h5._filter = self._filter
        return new_h5

    def copy_to(self, destination, file_name=None, maintain_name=True):
        """
        Copy the content of the HDF5 file to a new location

        Args:
            destination (FileHDFio): FileHDFio object pointing to the new location
            file_name (str): name of the new HDF5 file - optional
            maintain_name (bool): by default the names of the HDF5 groups are maintained

        Returns:
            FileHDFio: FileHDFio object pointing to a file which now contains the same content as file of the current
                       FileHDFio object.
        """

        def _internal_copy(source, source_path, target, target_path, maintain_flag):
            """
            Internal function to copy content of one HDF5 file to another or copy a group within the same HDF5 file.

            Args:
                source (h5py.File): HDF5 File object
                source_path (str): Path inside the source HDF5 file
                target (h5py.File): HDF5 File object
                target_path (str): Path inside the target HDF5 file
                maintain_flag (bool): Maintain the same group name
            """
            if maintain_flag:
                try:
                    target.create_group(target_path)
                except ValueError:
                    pass  # In case the copy_to() function failed previously and the group already exists.

            if target_path == "/":
                source.copy(target_path, "/") if source == target else source.copy(
                    target_path, target
                )
            else:
                if maintain_flag:
                    if dest_path != "":
                        source.copy(source_path, target[dest_path])
                    else:
                        source.copy(source_path, target)
                else:
                    group_name_old = source_path.split("/")[-1]
                    try:
                        target.create_group("/tmp")
                    except ValueError:
                        pass
                    source.copy(source_path, target["/tmp"])
                    try:
                        target.move("/tmp/" + group_name_old, target_path)
                    except ValueError:
                        del target[dest_path]
                        target.move("/tmp/" + group_name_old, target_path)
                    del target["/tmp"]

        if file_name is None:
            file_name = destination.file_name

        if self.file_exists:
            dest_path = (
                destination.h5_path[1:]
                if destination.h5_path[0] == "/"
                else destination.h5_path
            )
            if self.file_name != file_name:
                with open_hdf5(self.file_name, mode="r") as f_source:
                    with open_hdf5(file_name, mode="a") as f_target:
                        _internal_copy(
                            source=f_source,
                            source_path=self._h5_path,
                            target=f_target,
                            target_path=dest_path,
                            maintain_flag=maintain_name,
                        )
            else:
                with open_hdf5(file_name, mode="a") as f_target:
                    _internal_copy(
                        source=f_target,
                        source_path=self._h5_path,
                        target=f_target,
                        target_path=dest_path,
                        maintain_flag=maintain_name,
                    )

        return destination

    def create_group(self, name, track_order=False):
        """
        Create an HDF5 group - similar to a folder in the filesystem - the HDF5 groups allow the users to structure
        their data.

        Args:
            name (str): name of the HDF5 group
            track_order (bool): if False this groups tracks its elements in
                alphanumeric order, if True in insertion order

        Returns:
            FileHDFio: FileHDFio object pointing to the new group
        """
        full_name = posixpath.join(self.h5_path, name)
        with open_hdf5(self.file_name, mode="a") as h:
            try:
                h.create_group(full_name, track_order=track_order)
            except ValueError:
                pass
        h_new = self[name].copy()
        return h_new

    def remove_group(self):
        """
        Remove an HDF5 group - if it exists. If the group does not exist no error message is raised.
        """
        try:
            with open_hdf5(self.file_name, mode="a") as hdf_file:
                del hdf_file[self.h5_path]
        except KeyError:
            pass

    def open(self, h5_rel_path):
        """
        Create an HDF5 group and enter this specific group. If the group exists in the HDF5 path only the h5_path is
        set correspondingly otherwise the group is created first.

        Args:
            h5_rel_path (str): relative path from the current HDF5 path - h5_path - to the new group

        Returns:
            FileHDFio: FileHDFio object pointing to the new group
        """
        new_h5_path = self.copy()
        if os.path.isabs(h5_rel_path):
            raise ValueError(
                "Absolute paths are not supported -> replace by relative path name!"
            )

        if h5_rel_path.strip() == ".":
            h5_rel_path = ""
        if h5_rel_path.strip() != "":
            new_h5_path.h5_path = posixpath.join(new_h5_path.h5_path, h5_rel_path)
        new_h5_path.history.append(h5_rel_path)

        return new_h5_path

    def close(self):
        """
        Close the current HDF5 path and return to the path before the last open
        """
        path_lst = self.h5_path.split("/")
        last = self.history[-1].strip()
        if len(last) > 0:
            hist_lst = last.split("/")
            self.h5_path = "/".join(path_lst[: -len(hist_lst)])
            if len(self.h5_path.strip()) == 0:
                self.h5_path = "/"
        del self.history[-1]

    def show_hdf(self):
        """
        Iterating over the HDF5 datastructure and generating a human readable graph.
        """
        self._walk()

    def remove_file(self):
        """
        Remove the HDF5 file with all the related content
        """
        if self.file_exists:
            os.remove(self.file_name)

    def get_from_table(self, path, name):
        """
        Get a specific value from a pandas.Dataframe

        Args:
            path (str): relative path to the data object
            name (str): parameter key

        Returns:
            dict, list, float, int: the value associated to the specific parameter key
        """
        df_table = self.get(path)
        keys = df_table["Parameter"]
        if name in keys:
            job_id = keys.index(name)
            return df_table["Value"][job_id]
        raise ValueError("Unknown name: {0}".format(name))

    def get_pandas(self, name):
        """
        Load a dictionary from the HDF5 file and display the dictionary as pandas Dataframe

        Args:
            name (str): HDF5 node name

        Returns:
            pandas.Dataframe: The dictionary is returned as pandas.Dataframe object
        """
        val = self.get(name)
        if isinstance(val, dict):
            df = pandas.DataFrame(val)
            return df

    def get(self, key, default=None):
        """
        Internal wrapper function for __getitem__() - self[name]

        Args:
            key (str, slice): path to the data or key of the data object
            default (object): default value to return if key doesn't exist

        Returns:
            dict, list, float, int: data or data object
        """
        try:
            return self.__getitem__(key)
        except ValueError:
            if default is not None:
                return default
            else:
                raise

    def put(self, key, value):
        """
        Store data inside the HDF5 file

        Args:
            key (str): key to store the data
            value (pandas.DataFrame, pandas.Series, dict, list, float, int): basically any kind of data is supported
        """
        self.__setitem__(key=key, value=value)

    def _list_all(self):
        """
        List all groups and nodes of the HDF5 file - where groups are equivalent to directories and nodes to files.

        Returns:
            dict: {'groups': [list of groups], 'nodes': [list of nodes]}
        """
        if self.file_exists:
            groups = set()
            nodes = set()
            with open_hdf5(self.file_name) as h:
                try:
                    h = h[self.h5_path]
                    for k in h.keys():
                        if isinstance(h[k], h5py.Group):
                            groups.add(k)
                        else:
                            nodes.add(k)
                except KeyError:
                    pass
            iopy_nodes = self._filter_io_objects(groups)
            return {
                "groups": sorted(list(groups - iopy_nodes)),
                "nodes": sorted(list((nodes - groups).union(iopy_nodes))),
            }
        else:
            return {"groups": [], "nodes": []}

    def _list_nodes(self):
        return self.list_all()["nodes"]

    def _list_groups(self):
        return self.list_all()["groups"]

    def listdirs(self):
        """
        equivalent to os.listdirs (consider groups as equivalent to dirs)

        Returns:
            (list): list of groups in pytables for the path self.h5_path

        """
        return self.list_groups()

    def list_dirs(self):
        """
        equivalent to os.listdirs (consider groups as equivalent to dirs)

        Returns:
            (list): list of groups in pytables for the path self.h5_path
        """
        return self.list_groups()

    def keys(self):
        """
        List all groups and nodes of the HDF5 file - where groups are equivalent to directories and nodes to files.

        Returns:
            list: all groups and nodes
        """
        list_all_dict = self.list_all()
        return list_all_dict["nodes"] + list_all_dict["groups"]

    def values(self):
        """
        List all values for all groups and nodes of the HDF5 file

        Returns:
            list: list of all values
        """
        return [self[key] for key in self.keys()]

    def items(self):
        """
        List all keys and values as items of all groups and nodes of the HDF5 file

        Returns:
            list: list of sets (key, value)
        """
        return [(key, self[key]) for key in self.keys()]

    def groups(self):
        """
        Filter HDF5 file by groups

        Returns:
            FileHDFio: an HDF5 file which is filtered by groups
        """
        new = self.copy()
        new._filter = ["groups"]
        return new

    def nodes(self):
        """
        Filter HDF5 file by nodes

        Returns:
            FileHDFio: an HDF5 file which is filtered by nodes
        """
        new = self.copy()
        new._filter = ["nodes"]
        return new

    def hd_copy(self, hdf_old, hdf_new, exclude_groups=None, exclude_nodes=None):
        """
        args:
            hdf_old (ProjectHDFio): old hdf
            hdf_new (ProjectHDFio): new hdf
            exclude_groups (list/None): list of groups to delete
            exclude_nodes (list/None): list of nodes to delete
        """
        if exclude_groups is None or len(exclude_groups) == 0:
            exclude_groups_split = list()
            group_list = hdf_old.list_groups()
        else:
            exclude_groups_split = [i.split("/", 1) for i in exclude_groups]
            check_groups = [i[-1] for i in exclude_groups_split]
            group_list = list(
                (set(hdf_old.list_groups()) ^ set(check_groups))
                & set(hdf_old.list_groups())
            )

        if exclude_nodes is None or len(exclude_nodes) == 0:
            exclude_nodes_split = list()
            node_list = hdf_old.list_nodes()
        else:
            exclude_nodes_split = [i.split("/", 1) for i in exclude_nodes]
            check_nodes = [i[-1] for i in exclude_nodes_split]
            node_list = list(
                (set(hdf_old.list_nodes()) ^ set(check_nodes))
                & set(hdf_old.list_nodes())
            )
        for p in node_list:
            hdf_new[p] = hdf_old[p]
        for p in group_list:
            h_new = hdf_new.create_group(p)
            ex_n = [e[-1] for e in exclude_nodes_split if p == e[0] or len(e) == 1]
            ex_g = [e[-1] for e in exclude_groups_split if p == e[0] or len(e) == 1]
            self.hd_copy(hdf_old[p], h_new, exclude_nodes=ex_n, exclude_groups=ex_g)
        ### old ###
        # for p in hdf_old.list_nodes():
        #     if p not in exclude_nodes:
        #         hdf_new[p] = hdf_old[p]
        #
        # for p in hdf_old.list_groups():
        #     if p not in exclude_groups:
        #         h_new = hdf_new.create_group(p)
        #         self.hd_copy(hdf_old[p], h_new, exclude_groups=exclude_groups, exclude_nodes=exclude_nodes)
        return hdf_new

    @deprecate(job_name="ignored!", exclude_groups="ignored!", exclude_nodes="ignored!")
    def rewrite_hdf5(
        self, job_name=None, info=False, exclude_groups=None, exclude_nodes=None
    ):
        """
        Rewrite the entire hdf file.

        Args:
            info (True/False): whether to give the information on how much space has been saved
        """
        if job_name is not None:
            state.logger.warning(
                "Specifying job_name is deprecated and ignored! Future versions will change signature."
            )
        file_name = self.file_name
        new_file = file_name + "_rewrite"

        self_hdf = FileHDFio(file_name=file_name)
        hdf_new = FileHDFio(file_name=new_file, h5_path="/")

        old_logger_level = state.logger.level
        state.logger.level = 50
        hdf_new = self.hd_copy(self_hdf, hdf_new)
        state.logger.level = old_logger_level

        if info:
            print(
                "compression rate from old to new: {}".format(
                    self.file_size(self_hdf) / self.file_size(hdf_new)
                )
            )
            print(
                "data size vs file size: {}".format(
                    self.get_size(hdf_new) / self.file_size(hdf_new)
                )
            )
        self.remove_file()
        os.rename(hdf_new.file_name, file_name)

    def __str__(self):
        """
        Machine readable string representation

        Returns:
            str: list all nodes and groups as string
        """
        return self.__repr__()

    def __repr__(self):
        """
        Human readable string representation

        Returns:
            str: list all nodes and groups as string
        """
        return str(self.list_all())

    def __del__(self):
        del self._file_name
        del self.history
        del self._h5_path

    def __enter__(self):
        """
        Compatibility function for the with statement
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Compatibility function for the with statement
        """
        self.close()
        try:
            self._store.close()
        except AttributeError:
            pass

    def _read(self, item):
        """
        Internal read function to read data from the HDF5 file

        Args:
            item (str): path to the data or key of the data object

        Returns:
            dict, list, float, int: data or data object
        """
        return h5io.read_hdf5(self.file_name, title=self._get_h5_path(item))

    # def _open_store(self, mode="r"):
    #     """
    #     Internal function to open the HDF5 file
    #
    #     Args:
    #         mode (str): file mode can be either 'w': write, 'r': read or 'a': append
    #     """
    #     try:
    #         if not self._store:
    #             self._store = HDFStoreIO(self.file_name, mode=mode)
    #     except AttributeError:
    #         self._store = HDFStoreIO(self.file_name, mode=mode)
    #
    # def _close_store(self):
    #     """
    #     Internal function to close the HDF5 file
    #     """
    #     try:
    #         self._store.close()
    #         self._store = None
    #     except AttributeError:
    #         pass

    def create_project_from_hdf5(self):
        """
        Internal function to create a pyiron project pointing to the directory where the HDF5 file is located.

        Returns:
            Project: pyiron project object
        """
        from pyiron_base.project.generic import Project

        return Project(path=self.file_path)

    def _get_h5_path(self, name):
        """
        Internal function to combine the current h5_path with the relative path

        Args:
            name (str): relative path

        Returns:
            str: combined path
        """
        return posixpath.join(self.h5_path, name)

    def _get_h5io_type(self, name):
        """
        Internal function to get h5io type

        Args:
            name (str): HDF5 key

        Returns:
            str: h5io type
        """
        with open_hdf5(self.file_name) as store:
            return str(store[self.h5_path][name].attrs.get("TITLE", ""))

    def _filter_io_objects(self, groups):
        """
        Internal function to extract h5io objects (which have the same type as normal groups)

        Args:
            groups (list, set): list of groups (as obtained e.g. from listdirs

        Returns:
            set: h5io objects
        """
        h5io_types = (
            "dict",
            "list",
            "tuple",
            "pd_dataframe",
            "pd_series",
            "multiarray",
            "json",
        )
        group_h5io = set(
            [group for group in groups if self._get_h5io_type(group) in h5io_types]
        )
        return group_h5io

    def _walk(self, level=0):
        """
        Internal helper function for show_hdf() - iterating over the HDF5 datastructure and generating a human readable
        graph.

        Args:
            level (int): iteration level
        """
        l_dict = self.list_all()
        indent = level * "  "
        for node in l_dict["nodes"]:
            print(indent + "node", node)
        for group in l_dict["groups"]:
            print(indent + "group: ", group)
            with self.open(group) as hdf_group:
                hdf_group._walk(level=level + 1)


class ProjectHDFio(FileHDFio):
    """
    The ProjectHDFio class connects the FileHDFio and the Project class, it is derived from the FileHDFio class but in
    addition the a project object instance is located at self.project enabling direct access to the database and other
    project related functionality, some of which are mapped to the ProjectHDFio class as well.

    Args:
        project (Project): pyiron Project the current HDF5 project is located in
        file_name (str): name of the HDF5 file - in contrast to the FileHDFio object where file_name represents the
                         absolute path of the HDF5 file.
        h5_path (str): absolute path inside the h5 path - starting from the root group
        mode (str): mode : {'a', 'w', 'r', 'r+'}, default 'a'
                    See HDFStore docstring or tables.open_file for info about modes

    Attributes:

        .. attribute:: project

            Project instance the ProjectHDFio object is located in

        .. attribute:: root_path

            the pyiron user directory, defined in the .pyiron configuration

        .. attribute:: project_path

            the relative path of the current project / folder starting from the root path
            of the pyiron user directory

        .. attribute:: path

            the absolute path of the current project / folder plus the absolute path in the HDF5 file as one path

        .. attribute:: file_name

            absolute path to the HDF5 file

        .. attribute:: h5_path

            path inside the HDF5 file - also stored as absolute path

        .. attribute:: history

            previously opened groups / folders

        .. attribute:: file_exists

            boolean if the HDF5 was already written

        .. attribute:: base_name

            name of the HDF5 file but without any file extension

        .. attribute:: file_path

            directory where the HDF5 file is located

        .. attribute:: is_root

            boolean if the HDF5 object is located at the root level of the HDF5 file

        .. attribute:: is_open

            boolean if the HDF5 file is currently opened - if an active file handler exists

        .. attribute:: is_empty

            boolean if the HDF5 file is empty

        .. attribute:: user

            current unix/linux/windows user who is running pyiron

        .. attribute:: sql_query

            an SQL query to limit the jobs within the project to a subset which matches the SQL query.

        .. attribute:: db

            connection to the SQL database

        .. attribute:: working_directory

            working directory of the job is executed in - outside the HDF5 file
    """

    def __init__(self, project, file_name, h5_path=None, mode=None):
        file_name += ".h5" if not file_name.endswith(".h5") else ""
        self._file_name = file_name.replace("\\", "/")
        if h5_path is None:
            h5_path = "/"
        self._project = project.copy()
        super(ProjectHDFio, self).__init__(
            file_name=os.path.join(self._project.path, self._file_name).replace(
                "\\", "/"
            ),
            h5_path=h5_path,
            mode=mode,
        )

    @property
    def base_name(self):
        """
        The absolute path to of the current pyiron project - absolute path on the file system, not including the HDF5
        path.

        Returns:
            str: current project path
        """
        return self._project.path

    @property
    def db(self):
        """
        Get connection to the SQL database

        Returns:
            DatabaseAccess: database conncetion
        """
        return self._project.db

    @property
    def path(self):
        """
        Absolute path of the HDF5 group starting from the system root - combination of the absolute system path plus the
        absolute path inside the HDF5 file starting from the root group.

        Returns:
            str: absolute path
        """
        return os.path.join(self._project.path, self.h5_path[1:]).replace("\\", "/")

    @property
    def project(self):
        """
        Get the project instance the ProjectHDFio object is located in

        Returns:
            Project: pyiron project
        """
        return self._project

    @property
    def project_path(self):
        """
        the relative path of the current project / folder starting from the root path
        of the pyiron user directory

        Returns:
            str: relative path of the current project / folder
        """
        return self._project.project_path

    @property
    def root_path(self):
        """
        the pyiron user directory, defined in the .pyiron configuration

        Returns:
            str: pyiron user directory of the current project
        """
        return self._project.root_path

    @property
    def sql_query(self):
        """
        Get the SQL query for the project

        Returns:
            str: SQL query
        """
        return self._project.sql_query

    @sql_query.setter
    def sql_query(self, new_query):
        """
        Set the SQL query for the project

        Args:
            new_query (str): SQL query
        """
        self._project.sql_query = new_query

    @property
    def user(self):
        """
        Get current unix/linux/windows user who is running pyiron

        Returns:
            str: username
        """
        return self._project.user

    @property
    def working_directory(self):
        """
        Get the working directory of the current ProjectHDFio object. The working directory equals the path but it is
        represented by the filesystem:
            /absolute/path/to/the/file.h5/path/inside/the/hdf5/file
        becomes:
            /absolute/path/to/the/file_hdf5/path/inside/the/hdf5/file

        Returns:
            str: absolute path to the working directory
        """
        project_full_path = "/".join(self.file_name.split("/")[:-1])
        file_name = self.file_name.split("/")[-1]
        if ".h5" in file_name:
            file_name = file_name.split(".h5")[0]
        file_name += "_hdf5"
        if self.h5_path[0] == "/":
            h5_path = self.h5_path[1:]
        else:
            h5_path = self.h5_path
        return posixpath.join(project_full_path, file_name, h5_path)

    @property
    def _filter(self):
        """
        Get project filter

        Returns:
            str: project filter
        """
        return self._project._filter

    @_filter.setter
    def _filter(self, new_filter):
        """
        Set project filter

        Args:
            new_filter (str): project filter
        """
        self._project._filter = new_filter

    @property
    def _inspect_mode(self):
        """
        Check if inspect mode is activated

        Returns:
            bool: [True/False]
        """
        return self._project._inspect_mode

    @_inspect_mode.setter
    def _inspect_mode(self, read_mode):
        """
        Activate or deactivate inspect mode

        Args:
            read_mode (bool): [True/False]
        """
        self._project._inspect_mode = read_mode

    @property
    def name(self):
        return os.path.basename(self.h5_path)

    def copy(self):
        """
        Copy the ProjectHDFio object - copying just the Python object but maintaining the same pyiron path

        Returns:
            ProjectHDFio: copy of the ProjectHDFio object
        """
        new_h5 = ProjectHDFio(
            project=self._project, file_name=self._file_name, h5_path=self._h5_path
        )
        new_h5._filter = self._filter
        return new_h5

    def create_hdf(self, path, job_name):
        """
        Create an ProjectHDFio object to store project related information - for testing aggregated data

        Args:
            path (str): absolute path
            job_name (str): name of the HDF5 container

        Returns:
            ProjectHDFio: HDF5 object
        """
        return self._project.create_hdf(path=path, job_name=job_name)

    def create_working_directory(self):
        """
        Create the working directory on the file system if it does not exist already.
        """
        if not os.path.isdir(self.working_directory):
            os.makedirs(self.working_directory)

    def import_class(self, class_name):
        """
        Import given class from fully qualified name and return class object.

        Args:
            class_name (str): fully qualified name of a pyiron class

        Returns:
            type: class object of the given name
        """
        internal_class_name = class_name.split(".")[-1][:-2]
        if internal_class_name in self._project.job_type.job_class_dict:
            module_path = self._project.job_type.job_class_dict[internal_class_name]
        else:
            class_path = class_name.split()[-1].split(".")[:-1]
            class_path[0] = class_path[0][1:]
            module_path = ".".join(class_path)
        return getattr(
            importlib.import_module(module_path),
            internal_class_name,
        )

    def create_instance(self, cls, **kwargs):
        """
        Create new instance of the given class from current group.

        Uses the given **kwargs and a special classmethod "from_hdf_args" that
        may be defined on cls to construct a dictionary of arguments and then
        instatiate cls with them.

        Args:
            cls (type): pyiron type to instantiate
            **kwargs: arguments for instance creation

        Returns:
            cls: instance of the given type
        """

        if hasattr(cls, "from_hdf_args"):
            init_args = cls.from_hdf_args(self)
        else:
            init_args = {}

        init_args.update(kwargs)

        return cls(**init_args)

    def to_object(self, class_name=None, **qwargs):
        """
        Load the full pyiron object from an HDF5 file

        Args:
            class_name(str, optional): if the 'TYPE' node is not available in
                        the HDF5 file a manual object type can be set,
                        must be as reported by `str(type(obj))`
            **qwargs: optional parameters optional parameters to override init
                      parameters

        Returns:
            pyiron object of the given class_name
        """
        if "TYPE" not in self.list_nodes() and class_name is None:
            raise ValueError("Objects can be only recovered from hdf5 if TYPE is given")
        elif class_name is not None and class_name != self.get("TYPE"):
            raise ValueError(
                "Object type in hdf5-file must be identical to input parameter"
            )
        class_name = class_name or self.get("TYPE")
        class_object = self.import_class(class_name)

        # Backwards compatibility since the format of TYPE changed
        if class_name != str(class_object):
            self["TYPE"] = str(class_object)

        obj = self.create_instance(class_object, **qwargs)
        obj.from_hdf(hdf=self.open(".."), group_name=self.h5_path.split("/")[-1])
        return obj

    def get_job_id(self, job_specifier):
        """
        get the job_id for job named job_name in the local project path from database

        Args:
            job_specifier (str, int): name of the job or job ID

        Returns:
            int: job ID of the job
        """
        return self._project.get_job_id(job_specifier=job_specifier)

    def inspect(self, job_specifier):
        """
        Inspect an existing pyiron object - most commonly a job - from the database

        Args:
            job_specifier (str, int): name of the job or job ID

        Returns:
            JobCore: Access to the HDF5 object - not a GenericJob object - use load() instead.
        """
        return self._project.inspect(job_specifier=job_specifier)

    def load(self, job_specifier, convert_to_object=True):
        """
        Load an existing pyiron object - most commonly a job - from the database

        Args:
            job_specifier (str, int): name of the job or job ID
            convert_to_object (bool): convert the object to an pyiron object or only access the HDF5 file - default=True
                                      accessing only the HDF5 file is about an order of magnitude faster, but only
                                      provides limited functionality. Compare the GenericJob object to JobCore object.

        Returns:
            GenericJob, JobCore: Either the full GenericJob object or just a reduced JobCore object
        """
        return self._project.load(
            job_specifier=job_specifier, convert_to_object=convert_to_object
        )

    def load_from_jobpath(self, job_id=None, db_entry=None, convert_to_object=True):
        """
        Internal function to load an existing job either based on the job ID or based on the database entry dictionary.

        Args:
            job_id (int): Job ID - optional, but either the job_id or the db_entry is required.
            db_entry (dict): database entry dictionary - optional, but either the job_id or the db_entry is required.
            convert_to_object (bool): convert the object to an pyiron object or only access the HDF5 file - default=True
                                      accessing only the HDF5 file is about an order of magnitude faster, but only
                                      provides limited functionality. Compare the GenericJob object to JobCore object.

        Returns:
            GenericJob, JobCore: Either the full GenericJob object or just a reduced JobCore object
        """
        return self._project.load_from_jobpath(
            job_id=job_id, db_entry=db_entry, convert_to_object=convert_to_object
        )

    def remove_job(self, job_specifier, _unprotect=False):
        """
        Remove a single job from the project based on its job_specifier - see also remove_jobs()

        Args:
            job_specifier (str, int): name of the job or job ID
            _unprotect (bool): [True/False] delete the job without validating the dependencies to other jobs
                               - default=False
        """
        self._project.remove_job(job_specifier=job_specifier, _unprotect=_unprotect)

    def create_project_from_hdf5(self):
        """
        Internal function to create a pyiron project pointing to the directory where the HDF5 file is located.

        Returns:
            Project: pyiron project object
        """
        return self._project.__class__(path=self.file_path)
