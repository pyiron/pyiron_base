# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from datetime import datetime
import warnings

from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.jobs.job.jobtype import JobType
from pyiron_base.jobs.master.generic import GenericMaster

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
        self.interactive_ref_job_initialize()

    def to_hdf(self, hdf=None, group_name=None):
        if self._ref_job is not None and self._ref_job.job_id is None:
            self.append(self._ref_job)
        super(InteractiveWrapper, self).to_hdf(hdf=hdf, group_name=group_name)

    to_hdf.__doc__ = GenericMaster.to_hdf.__doc__

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
        self._calculate_successor()
        self.send_to_database()
        self.update_master()
