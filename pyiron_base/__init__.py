# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

# API of the pyiron_base module - in alphabetical order
# Internal init
import pyiron_base._version
from pyiron_base.interfaces.factory import PyironFactory
from pyiron_base.interfaces.has_groups import HasGroups
from pyiron_base.interfaces.has_hdf import HasHDF
from pyiron_base.interfaces.object import HasDatabase, HasStorage, PyironObject
from pyiron_base.jobs.datamining import PyironTable, TableJob
from pyiron_base.jobs.dynamic import warn_dynamic_job_classes
from pyiron_base.jobs.flex.factory import create_job_factory
from pyiron_base.jobs.job.extension.executable import Executable
from pyiron_base.jobs.job.extension.jobstatus import (
    JobStatus,
    job_status_finished_lst,
    job_status_lst,
    job_status_successful_lst,
)
from pyiron_base.jobs.job.extension.server.queuestatus import validate_que_request
from pyiron_base.jobs.job.factory import JobFactoryCore
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.jobs.job.interactive import InteractiveBase
from pyiron_base.jobs.job.jobtype import JOB_CLASS_DICT, JobType, JobTypeChoice
from pyiron_base.jobs.job.template import PythonTemplateJob, TemplateJob
from pyiron_base.jobs.job.toolkit import BaseTools, Toolkit
from pyiron_base.jobs.master.flexible import FlexibleMaster
from pyiron_base.jobs.master.generic import GenericMaster, get_function_from_string
from pyiron_base.jobs.master.interactivewrapper import InteractiveWrapper
from pyiron_base.jobs.master.list import ListMaster
from pyiron_base.jobs.master.parallel import JobGenerator, ParallelMaster
from pyiron_base.maintenance.generic import (
    LocalMaintenance,
    Maintenance,
    add_module_conversion,
)
from pyiron_base.project.decorator import job
from pyiron_base.project.external import Notebook, dump, load
from pyiron_base.project.generic import Creator, Project
from pyiron_base.state import state
from pyiron_base.state.install import install_dialog
from pyiron_base.state.settings import Settings
from pyiron_base.storage.datacontainer import DataContainer
from pyiron_base.storage.filedata import FileData, FileDataTemplate, load_file
from pyiron_base.storage.flattenedstorage import FlattenedStorage
from pyiron_base.storage.has_stored_traits import HasStoredTraits
from pyiron_base.storage.hdfio import FileHDFio, ProjectHDFio
from pyiron_base.storage.inputlist import InputList
from pyiron_base.storage.parameters import GenericParameters
from pyiron_base.utils.parser import Logstatus, extract_data_from_file

Project.register_tools("base", BaseTools)

# Set version of pyiron_base
__version__ = pyiron_base._version.__version__

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
    Executable,
    Notebook,
    load,
    dump,
    create_job_factory,
    validate_que_request,
    GenericJob,
    InteractiveBase,
    JobStatus,
    job,
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
    add_module_conversion,
    LocalMaintenance,
    Maintenance,
    load_file,
    FileDataTemplate,
    FileData,
]
