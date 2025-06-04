from pyiron_base._tests import TestWithProject, ToyJob
from pyiron_base.jobs.job.wrapper import job_wrapper_function


class TestJobWrapper(TestWithProject):
    def test_job_wrapper_file(self):
        self.toy_job = self.project.create_job(ToyJob, "toy_job")
        self.toy_job.server.run_mode.manual = True
        self.toy_job.run()
        job_wrapper_function(
            working_directory=self.toy_job.working_directory,
            job_id=None,
            file_path=self.toy_job.project_hdf5.file_name + "/" + self.toy_job.job_name,
            submit_on_remote=False,
            debug=False,
            collect=False,
        )
        self.assertTrue(self.toy_job["status"] == "finished")

    def test_job_wrapper_id(self):
        self.toy_job = self.project.create_job(ToyJob, "toy_job")
        self.toy_job.server.run_mode.manual = True
        self.toy_job.run()
        job_wrapper_function(
            working_directory=self.toy_job.working_directory,
            job_id=self.toy_job.job_id,
            file_path=None,
            submit_on_remote=False,
            debug=False,
            collect=False,
        )
        self.assertTrue(self.toy_job["status"] == "finished")

    def test_job_wrapper_error(self):
        self.toy_job = self.project.create_job(ToyJob, "toy_job")
        self.toy_job.server.run_mode.manual = True
        self.toy_job.run()
        with self.assertRaises(ValueError):
            job_wrapper_function(
                working_directory=self.toy_job.working_directory,
                job_id=None,
                file_path=None,
                submit_on_remote=False,
                debug=False,
                collect=False,
            )
