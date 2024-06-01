# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from pyiron_base.state import state

# API of the pyiron_base module - in alphabetical order
from pyiron_base.interfaces.factory import PyironFactory
from pyiron_base.storage.flattenedstorage import FlattenedStorage
from pyiron_base.storage.hdfio import FileHDFio, ProjectHDFio
from pyiron_base.storage.datacontainer import DataContainer
from pyiron_base.storage.has_stored_traits import HasStoredTraits
from pyiron_base.storage.inputlist import InputList
from pyiron_base.storage.parameters import GenericParameters
from pyiron_base.jobs.job.extension.executable import Executable
from pyiron_base.project.external import Notebook, load, dump
from pyiron_base.jobs.dynamic import warn_dynamic_job_classes
from pyiron_base.jobs.flex.factory import create_job_factory
from pyiron_base.jobs.job.extension.server.queuestatus import validate_que_request
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.jobs.job.interactive import InteractiveBase
from pyiron_base.jobs.job.extension.jobstatus import (
    JobStatus,
    job_status_successful_lst,
    job_status_finished_lst,
    job_status_lst,
)
from pyiron_base.jobs.job.jobtype import JOB_CLASS_DICT, JobType, JobTypeChoice
from pyiron_base.jobs.job.template import TemplateJob, PythonTemplateJob
from pyiron_base.jobs.job.factory import JobFactoryCore
from pyiron_base.jobs.master.flexible import FlexibleMaster
from pyiron_base.jobs.master.generic import GenericMaster, get_function_from_string
from pyiron_base.jobs.master.interactivewrapper import InteractiveWrapper
from pyiron_base.jobs.master.list import ListMaster
from pyiron_base.jobs.master.parallel import ParallelMaster, JobGenerator
from pyiron_base.project.generic import Project, Creator
from pyiron_base.utils.parser import Logstatus, extract_data_from_file
from pyiron_base.state.settings import Settings
from pyiron_base.state.install import install_dialog
from pyiron_base.jobs.datamining import PyironTable, TableJob
from pyiron_base.interfaces.object import HasDatabase, HasStorage, PyironObject
from pyiron_base.interfaces.has_groups import HasGroups
from pyiron_base.interfaces.has_hdf import HasHDF

from pyiron_base.jobs.job.toolkit import Toolkit, BaseTools


# Internal init
from ._version import get_versions
from pyiron_base.utils.jedi import fix_ipython_autocomplete


# Give clear deprecation errors for objects removed from the 0.8.4 API
from pyiron_snippets.import_alarm import ImportAlarm as _ImportAlarm
from pyiron_snippets.deprecate import (
    Deprecator as _Deprecator,
    deprecate as _deprecate,  # Just silently guaranteeing it's where we expect
    deprecate_soon as _deprecate_soon,
    # Just silently guaranteeing it's where we expect
)


def _snippets_deprecation_string(obj, old_name):
    return


def ImportAlarm(*args, **kwargs):
    raise ValueError(
        f"pyiron_base.ImportAlarm is deprecated. Please use "
        f"{_ImportAlarm.__module__}.{_ImportAlarm.__name__}"
    ) from None


def Deprecator(*args, **kwargs):
    raise ValueError(
        f"pyiron_base.Deprecator is deprecated. Please use "
        f"{_Deprecator.__module__}.{_Deprecator.__name__}"
    ) from None


def deprecate(*args, **kwargs):
    # The wrapper nature makes this not so easy to automate the message
    raise ValueError(
        f"pyiron_base.deprecate (a {_deprecate.__class__.__name__} instance) is "
        f"deprecated. Please use pyiron_snippets.deprecate.deprecate"
    ) from None


def deprecate_soon(*args, **kwargs):
    # The wrapper nature makes this not so easy to automate the message
    raise ValueError(
        f"pyiron_base.deprecate_soon (a {_deprecate_soon.__class__.__name__} instance) "
        f"is deprecated. Please use pyiron_snippets.deprecate.deprecate_soon"
    ) from None


Project.register_tools("base", BaseTools)

# Set version of pyiron_base
__version__ = get_versions()["version"]

# Jedi fix
fix_ipython_autocomplete()

# Dynamic job class definition is no longer supported in pyiron_base >=0.7.0
warn_dynamic_job_classes(
    resource_folder_lst=state.settings.resource_paths,
    logger=state.logger,
)

__all__ = [
    PyironFactory,
    FlattenedStorage,
    FileHDFio,
    ProjectHDFio,
    DataContainer,
    HasStoredTraits,
    InputList,
    GenericParameters,
    Deprecator,
    deprecate,
    deprecate_soon,
    ImportAlarm,
    Executable,
    Notebook,
    load,
    dump,
    create_job_factory,
    validate_que_request,
    GenericJob,
    InteractiveBase,
    JobStatus,
    job_status_successful_lst,
    job_status_finished_lst,
    job_status_lst,
    JOB_CLASS_DICT,
    JobType,
    JobTypeChoice,
    TemplateJob,
    PythonTemplateJob,
    JobFactoryCore,
    FlexibleMaster,
    GenericMaster,
    get_function_from_string,
    InteractiveWrapper,
    ListMaster,
    ParallelMaster,
    JobGenerator,
    Creator,
    Logstatus,
    extract_data_from_file,
    Settings,
    install_dialog,
    PyironTable,
    TableJob,
    HasDatabase,
    HasStorage,
    PyironObject,
    HasGroups,
    HasHDF,
    Toolkit,
]
