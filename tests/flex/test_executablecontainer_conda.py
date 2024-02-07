import os
from pyiron_base._tests import TestWithProject


conda_env = """\
channels:
- conda-forge
dependencies:
- python=3.12.1
"""


class TestExecutableContainerConda(TestWithProject):
    def setUp(self):
        super().setUp()
        with open("env.yaml", "w") as f:
            f.writelines(conda_env)

    def tearDown(self):
        super().tearDown()
        os.remove("env.yaml")

    def test_create_job_class(self):
        self.project.create_job_class(
            class_name="PythonVersionJob",
            executable_str="python --version",
        )
        self.project.conda_environment.create(env_name="py312", env_file="env.yaml")
        job = self.project.create.job.PythonVersionJob(job_name="job_conda")
        job.server.conda_environment_path = self.project.conda_environment.py312
        job.run()
        self.assertEqual(job["error.out"][0], "Python 3.12.1\n")
