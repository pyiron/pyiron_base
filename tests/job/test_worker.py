import os
from pyiron_base._tests import TestWithCleanProject


class TestScriptJob(TestWithCleanProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.script_path = os.path.join(cls.project.path, "funct.py")
        with open(cls.script_path, "w") as f:
            f.write("print(\"Hello\")")
        cls.project.remove_jobs(recursive=True, silently=True)
        cls.sub_project = cls.project.open("sub")

    def test_worker(self):
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
