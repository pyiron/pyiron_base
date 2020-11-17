# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

# API of the pyiron_base module
from pyiron_base.generic.hdfio import FileHDFio, ProjectHDFio
from pyiron_base.generic.inputlist import InputList
from pyiron_base.generic.parameters import GenericParameters
from pyiron_base.generic.template import PyironObject
from pyiron_base.job.executable import Executable
from pyiron_base.job.external import Notebook
from pyiron_base.job.generic import GenericJob
from pyiron_base.job.interactive import InteractiveBase
from pyiron_base.job.jobstatus import JobStatus, job_status_successful_lst, job_status_finished_lst, job_status_lst
from pyiron_base.job.jobtype import JOB_CLASS_DICT, JobType, JobTypeChoice
from pyiron_base.job.template import TemplateJob, PythonTemplateJob
from pyiron_base.master.generic import GenericMaster, get_function_from_string
from pyiron_base.master.list import ListMaster
from pyiron_base.master.parallel import ParallelMaster, JobGenerator
from pyiron_base.master.serial import SerialMasterBase
from pyiron_base.project.generic import Project, Creator
from pyiron_base.pyio.parser import Logstatus, extract_data_from_file
from pyiron_base.server.queuestatus import validate_que_request
from pyiron_base.settings.generic import Settings
from pyiron_base.settings.install import install_dialog

# optional API of the pyiron_base module
try:
    from pyiron_base.project.gui import ProjectGUI
except (ImportError, TypeError, AttributeError):
    pass

# Internal init
from ._version import get_versions
from pyiron_base.generic.jedi import fix_ipython_autocomplete

# Set version of pyiron_base
__version__ = get_versions()["version"]
del get_versions

# Jedi fix
fix_ipython_autocomplete()
