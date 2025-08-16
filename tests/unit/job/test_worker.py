import os
import time
from threading import Thread
from pyiron_base._tests import TestWithCleanProject, ToyJob
from pyiron_base.jobs.worker import worker_function


def close_worker_after_sleep(worker_id, sleep_time):
    import time
    from pyiron_base import state

    time.sleep(sleep_time)
    state.database.database.set_job_status(job_id=worker_id, status="collect")


class TestWorker(TestWithCleanProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.script_path = os.path.join(cls.project.path, "funct.py")
        with open(cls.script_path, "w") as f:
            f.write('print("Hello")')
        cls.project.remove_jobs(recursive=True, silently=True)
        cls.sub_project = cls.project.open("sub")

    def test_worker_job(self):
        self.worker = self.project.create.job.WorkerJob("runner")
        self.worker.project_to_watch = self.sub_project
        self.worker.server.run_mode.thread = True
        self.worker.run()
        job = self.sub_project.create.job.ScriptJob("script")
        job.script_path = self.script_path
        job.server.run_mode.worker = True
        job.master_id = self.worker.job_id
        job.run()
        self.sub_project.wait_for_jobs()
        self.worker.status.collect = True
        df = self.sub_project.job_table()
        self.assertEqual(len(df[df.status == "finished"]), 1)
        time.sleep(10)  # Wait for the worker process to finish

    def test_worker_thread(self):
        self.worker = self.project.create.job.WorkerJob("runner")
        self.worker.project_to_watch = self.sub_project
        self.worker.server.run_mode.manual = True
        self.worker.run()
        job = self.sub_project.create.job.ScriptJob("script")
        job.script_path = self.script_path
        job.server.run_mode.worker = True
        job.master_id = self.worker.job_id
        job.run()
        t = Thread(
            target=close_worker_after_sleep,
            kwargs={"worker_id": self.worker.job_id, "sleep_time": 10},
        )
        t.start()
        self.worker.run_static()
        df = self.sub_project.job_table()
        self.assertEqual(len(df[df.status == "finished"]), 1)

    def test_worker_function(self):
        toy_job = self.project.create_job(ToyJob, "toy_job_1")
        toy_job.server.run_mode.worker = True
        toy_job.run()
        self.assertFalse(toy_job.status.finished)
        worker_function((toy_job.working_directory, toy_job.job_id))
        self.assertTrue(toy_job.status.finished)
        toy_job = self.project.create_job(ToyJob, "toy_job_2")
        toy_job.server.run_mode.worker = True
        toy_job.run()
        self.assertFalse(toy_job.status.finished)
        worker_function(
            (
                toy_job.working_directory,
                toy_job.project_hdf5.file_name + "/" + toy_job.job_name,
            )
        )
        toy_job.from_hdf()
        print(toy_job.status.string)
        self.assertTrue(toy_job.status.finished)
