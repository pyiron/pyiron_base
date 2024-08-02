# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import warnings
from datetime import datetime

from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.jobs.master.generic import GenericMaster
from pyiron_base.storage.parameters import GenericParameters

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "development"
__date__ = "Jan 8, 2021"


class InteractiveWrapper(GenericMaster):
    def __init__(self, project, job_name):
        super(InteractiveWrapper, self).__init__(project, job_name)
        self._ref_job = None
        self.input = GenericParameters("parameters")

    @property
    def ref_job(self):
        """
        Get the reference job template from which all jobs within the ParallelMaster are generated.

        Returns:
            GenericJob: reference job
        """
        if self._ref_job is not None:
            return self._ref_job
        try:
            if isinstance(self[0], GenericJob):
                self._ref_job = self[0]
                return self._ref_job
            else:
                return None
        except IndexError:
            return None

    @ref_job.setter
    def ref_job(self, ref_job):
        """
        Set the reference job template from which all jobs within the ParallelMaster are generated.

        Args:
            ref_job (GenericJob): reference job
        """
        if not ref_job.server.run_mode.interactive:
            warnings.warn("Run mode of the reference job not set to interactive")
        self.append(ref_job)

    def set_input_to_read_only(self):
        super().set_input_to_read_only()
        self.input.read_only = True

    set_input_to_read_only.__doc__ = GenericMaster.set_input_to_read_only.__doc__

    def validate_ready_to_run(self):
        self.ref_job.validate_ready_to_run()

    validate_ready_to_run.__doc__ = GenericMaster.validate_ready_to_run.__doc__

    def check_setup(self):
        try:
            self.ref_job.check_setup()
        except AttributeError:
            pass

    check_setup.__doc__ = GenericMaster.check_setup.__doc__

    def ref_job_initialize(self):
        if len(self._job_name_lst) > 0:
            self._ref_job = self.pop(-1)
            if self._job_id is not None and self._ref_job._master_id is None:
                self._ref_job.master_id = self.job_id
                self._ref_job.server.cores = self.server.cores

    def to_hdf(self, hdf=None, group_name=None):
        if self._ref_job is not None and self._ref_job.job_id is None:
            self.append(self._ref_job)
        super(InteractiveWrapper, self).to_hdf(hdf=hdf, group_name=group_name)
        with self.project_hdf5.open("input") as hdf5_input:
            self.input.to_hdf(hdf5_input)

    to_hdf.__doc__ = GenericMaster.to_hdf.__doc__

    def from_hdf(self, hdf=None, group_name=None):
        """
        Restore the InteractiveWrapper from an HDF5 file

        Args:
            hdf (ProjectHDFio): HDF5 group object - optional
            group_name (str): HDF5 subgroup name - optional
        """
        super(InteractiveWrapper, self).from_hdf(hdf=hdf, group_name=group_name)
        with self.project_hdf5.open("input") as hdf5_input:
            self.input.from_hdf(hdf5_input)

    from_hdf.__doc__ = GenericMaster.from_hdf.__doc__

    def collect_output(self):
        pass

    def _db_entry_update_run_time(self):
        """

        Returns:

        """
        job_id = self.get_job_id()
        db_dict = {}
        start_time = self.project.db.get_item_by_id(job_id)["timestart"]
        db_dict["timestop"] = datetime.now()
        db_dict["totalcputime"] = (db_dict["timestop"] - start_time).seconds
        self.project.db.item_update(db_dict, job_id)

    def _finish_job(self):
        """

        Returns:

        """
        self.status.finished = True
        self._db_entry_update_run_time()
        self._logger.info(
            "{}, status: {}, monte carlo master".format(self.job_info_str, self.status)
        )
        self.update_master()
