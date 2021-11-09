from pyiron_base._tests import TestWithCleanProject, ToyJob


class TestScriptJob(TestWithCleanProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.worker = cls.project.create.job.WorkerJob("runner")
        cls.sub_project = cls.project.open("sub")
        cls.worker.project_to_watch = cls.sub_project
        cls.worker.server.run_mode.thread = True
        cls.worker.run()

    def test_worker(self):
        for i in range(5):
            job = self.sub_project.create_job(ToyJob, "toy_" + str(i))
            job.server.run_mode.worker = True
            job.master_id = self.worker.job_id
            job.run()
        self.sub_project.wait_for_jobs()
        self.worker.status.collect = True
        df = self.sub_project.job_table()
        self.assertEqual(len(df[df.status == "finished"]), 5)