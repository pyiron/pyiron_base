# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
The ListMaster behaves like a list, just for job objects.
"""

from pyiron_base.jobs.job.core import JobCore, _doc_str_job_core_args
from pyiron_base.jobs.job.generic import GenericJob
from pyiron_base.jobs.master.generic import GenericMaster, _doc_str_generic_master_attr
from pyiron_base.jobs.master.submissionstatus import SubmissionStatus

__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2020, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Sep 1, 2017"


# Modular Docstrings
_doc_str_list_master_attr = (
    _doc_str_generic_master_attr
    + "\n"
    + """\
        .. attribute:: submission_status

            Monitors how many jobs have been submitted and how many have to be submitted in future
"""
)


class ListMaster(GenericMaster):
    __doc__ = (
        """
    The ListMaster is the most simple MetaJob derived from the GenericMaster. It behaves like a Python list object. Jobs
    can be append to the ListMaster just like elements are added to a list and then all jobs can be executed together.
    This also works for already executed jobs, unless they are already linked to a different MetaJob - meaning they
    already have a master ID assigned to them.
"""
        + "\n"
        + _doc_str_job_core_args
        + "\n"
        + _doc_str_list_master_attr
    )

    def __init__(self, project, job_name):
        super(ListMaster, self).__init__(project, job_name=job_name)
        self.__version__ = "0.1"
        self._input["mode"] = "parallel"
        self.submission_status = SubmissionStatus(db=project.db, job_id=self.job_id)
        self.refresh_submission_status()

    def reset_job_id(self, job_id=None):
        """
        Reset the job id sets the job_id to None as well as all connected modules like JobStatus and SubmissionStatus.
        """
        super(ListMaster, self).reset_job_id(job_id=job_id)
        self.submission_status = SubmissionStatus(db=self.project.db, job_id=job_id)

    def refresh_submission_status(self):
        """
        Refresh the submission status - if a job ID job_id is set then the submission status is loaded from the
        database.
        """
        if self.job_id:
            self.submission_status = SubmissionStatus(
                db=self._hdf5.db, job_id=self.job_id
            )
            self.submission_status.refresh()

    def save(self):
        """
        Save the object, by writing the content to the HDF5 file and storing an entry in the database.

        Returns:
            (int): Job ID stored in the database
        """
        job_id = super(ListMaster, self).save()
        self.refresh_submission_status()
        return job_id

    def append(self, job):
        """
        Append a job to the ListMaster - just like you would append an element to a list.

        Args:
            job (JobCore, GenericJob, int): job to append
        """
        if isinstance(job, JobCore) and job.job_id:
            job = job.job_id
        if isinstance(job, int):
            job = self._hdf5.project.load(job)
        if isinstance(job, GenericJob):
            if job.status.created or job.status.initialized:
                super(ListMaster, self).append(job=job)
            elif job.job_id and job.status.finished:
                if job.master_id is None:
                    if self.job_id is None:
                        self._job_id = self.save()
                    child_db_entry = self.project.db.get_item_by_id(job.job_id)
                    self.project.db.delete_item(job.job_id)
                    job._job_id = None
                    job.master_id = self._job_id
                    job.save()
                    del child_db_entry["id"]
                    del child_db_entry["masterid"]
                    self.project.db.item_update(child_db_entry, job.job_id)
                    self.submission_status.submit_next()
                    if len(self._job_name_lst) == 0:
                        self.status.finished = True
                        self.run_time_to_db()
                else:
                    raise ValueError(
                        "This job ",
                        job.job_name,
                        " is already connected to a master ",
                        job.master_id,
                        " and can not be appended here.",
                    )
        else:
            raise TypeError(
                "job has to be either GenericJob, JobCore or int, but it - ",
                job,
                " is ",
                type(job),
            )

    def is_finished(self):
        """
        Check if the ListMaster job is finished - by checking the job status and the submission status.

        Returns:
            bool: [True/False]
        """
        if self.status.finished:
            return True
        self.submission_status.refresh()
        if not self.submission_status.finished:
            return False
        else:
            status_set = set(
                [
                    self._hdf5.db.get_item_by_id(child_id)["status"]
                    for child_id in self.child_ids
                ]
            )
            if "finished" in status_set:
                return len(status_set) == 1
            else:
                return False

    def run_static(self):
        """
        The run static function is called by run to execute the simulation. For the
        ListMaster this means executing all the childs appened in parallel.
        """
        self._input["num_points"] = len(self)
        self._logger.info("{} run parallel master (modal)".format(self.job_info_str))
        self.status.running = True
        if len(self._job_name_lst) > 0:
            job_lst = []
            for i in range(len(self._job_name_lst)):
                ham = self.pop(i=0)
                if (
                    ham.server.run_mode.non_modal
                    and self.get_child_cores() + ham.server.cores > self.server.cores
                ):
                    break
                self.submission_status.submit_next()
                if not ham.status.finished:
                    ham.run()
                self._logger.info("ListMaster: finished job {}".format(ham.job_name))
                if ham.server.run_mode.thread:
                    job_lst.append(ham._process)
                else:
                    self.refresh_job_status()
            _ = [process.communicate() for process in job_lst if process]
            self.status.suspended = True
        if self.server.run_mode.modal or (
            (self.server.run_mode.non_modal or self.server.run_mode.queue)
            and self.is_finished()
        ):
            self.status.finished = True

    def copy(self):
        """
        Copy the ListMaster object which links to the job and its HDF5 file

        Returns:
            ListMaster: New ListMaster object pointing to the same job
        """
        new_job = super(ListMaster, self).copy()
        new_job._child_ids = self.child_ids[:]
        return new_job

    def iter_jobs(self, convert_to_object=True):
        """
        Iterate over the jobs within the ListMaster

        Args:
            convert_to_object (bool): load the full GenericJob object (default) or just the HDF5 / JobCore object

        Returns:
            yield: Yield of GenericJob or JobCore
        """
        for job_id in self.child_ids:
            yield self._hdf5.load(job_id, convert_to_object=convert_to_object)

    def run_if_refresh(self):
        """
        Internal helper function the run if refresh function is called when the job status is 'refresh'. If the job was
        suspended previously, the job is going to be started again, to be continued.
        """
        log_str = "{}, status: {}, finished: {} parallel master refresh".format(
            self.job_info_str, self.status, self.is_finished()
        )
        self._logger.info(log_str)
        if self.is_finished() and not self.server.run_mode.modal:
            self.status.finished = True
        elif (
            self.server.run_mode.non_modal or self.server.run_mode.queue
        ) and not self.submission_status.finished:
            self.run_static()
        else:
            self.refresh_job_status()
            if self.status.refresh:
                self.status.suspended = True

    def collect_output(self):
        """
        Collect output is not implemented for ListMaster jobs
        """
        pass

    def run_if_interactive(self):
        """
        run_if_interactive() is not implemented for ListMaster jobs
        """
        pass

    def __len__(self):
        """
        Length of the ListMaster equal the number of childs appended.

        Returns:
            int: length of the ListMaster
        """
        return len(self.child_ids + self._job_name_lst)
