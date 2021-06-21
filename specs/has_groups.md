# HasGroup Interface

A number of pyiron objects are modelling a hierarchy of nested objects, e.g. projects contain sub projects and jobs, HDF
files contain groups and datasets, jobs contain different types of output, etc.  Common to this is that each of these
objects represent a vertex in some kind of directed acyclic graph.  In this graph each vertex "contains" or
"is-linked-to" a number of other objects, e.g. again sub-projects, jobs or similar.  We distinguish two types of
vertices another vertex might link to, leaf-level vertices, "nodes", or interior vertices, "groups".

To be able to traverse this in a generic manner all of these classes have a method `list_nodes()` that returns names of
"nodes" and `list_groups()` that returns names of "groups".  Since these are only the string names, the classes must
also provide a way to actually access the contained objects.  This is done via `__getitem__()`.  Since a group is
distinguished from a node by containing other objects it also represents a vertex "containing" or "linked-to" other
vertices.  From this we formulate the requirements for this interface

1. `__getitem__()` must return a non-`None` object for each string returned by `list_nodes()` and `list_groups()`.
2. Objects listed in `list_groups` must be instances of `HasGroups`.
3. Objects listed in `list_nodes` must not be instances of `HasGroups`; the output of `list_nodes()` and `list_groups` is
   therefore mutually exclusive.

This is captured in the `HasGroups` ABC defined in `pyiron_base.interfaces.has_groups`.  See there also for an example
implementation.
