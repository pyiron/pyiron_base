# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Worker Class to execute calculation in an asynchronous way
"""
import os
import time
import multiprocessing
from pyiron_base.job.template import PythonTemplateJob
from pyiron_base.job.wrapper import JobWrapper


__author__ = "Jan Janssen"
__copyright__ = (
    "Copyright 2021, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Jan Janssen"
__email__ = "janssen@mpie.de"
__status__ = "production"
__date__ = "Nov 5, 2021"


def worker_function(queue):
    while True:
        status, working_directory, job_id, _, submit_on_remote, debug = queue.get(
            block=True,
            timeout=None
        )
        if status:
            job_wrap = JobWrapper(
                working_directory=working_directory,
                job_id=job_id,
                submit_on_remote=submit_on_remote,
                debug=debug,
            )
            job_wrap.run()
        else:
            break

class WorkerJob(PythonTemplateJob):
    def __init__(self, project, job_name):
        super(WorkerJob, self).__init__(project, job_name)
        self.input.project = None
        self.input.cores_per_job = 1
        self.input.sleep_interval = 10

    @property
    def project_to_watch(self):
        rel_path = os.path.relpath(
            self.input.project,
            self.project.path
        )
        return self.project.open(rel_path)

    @project_to_watch.setter
    def project_to_watch(self, pr):
        self.input.project = pr.path

    @property
    def cores_per_job(self):
        return self.input.cores_per_job

    @cores_per_job.setter
    def cores_per_job(self, cores):
        self.input.cores_per_job = int(cores)

    @property
    def sleep_interval(self):
        return self.input.sleep_interval

    @sleep_interval.setter
    def sleep_interval(self, interval):
        self.input.sleep_interval = int(interval)

    # This function is executed
    def run_static(self):
        self.status.running = True
        master_id = self.job_id
        pr = self.project_to_watch
        queue = multiprocessing.Queue()
        active_job_ids = []
        with multiprocessing.Pool(
            processes=int(self.server.cores/self.cores_per_job),
            initializer=worker_function,
            initargs=(queue,)
        ) as pool:
            while True:
                df = pr.job_table()
                df_sub = df[
                    (df["status"] == "submitted") &
                    (df["masterid"] == master_id) &
                    (~df["id"].isin(active_job_ids))
                ]
                if len(df_sub) > 0:
                    path_lst = [
                        [pp, p, job_id]
                        for pp, p, job_id in zip(
                            df_sub["projectpath"].values,
                            df_sub["project"].values,
                            df_sub["id"].values
                        ) if job_id not in active_job_ids]
                    job_lst = [
                        [True, p, job_id, None, False, False]
                        if pp is None else
                        [True, os.path.join(pp, p), job_id, None, False, False]
                        for pp, p, job_id in path_lst
                    ]
                    active_job_ids += [j[2] for j in job_lst]
                    _ = [queue.put(j) for j in job_lst]
                elif self.status.collect:
                    break
                else:
                    time.sleep(self.input.sleep_interval)
            for i in range(int(self.server.cores / self.cores_per_job)):
                queue.put([False, False, False, False, False, False])
            pool.close()
            pool.join()
            pool.terminate()
        self.status.finished = True
