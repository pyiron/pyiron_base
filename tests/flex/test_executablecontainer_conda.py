import os
import unittest
from pyiron_base._tests import TestWithProject


try:
    import conda
    skip_conda_test = os.name == "nt"
except ImportError:
    skip_conda_test = True


conda_env = """\
channels:
- conda-forge
dependencies:
- python=3.12.1
"""


@unittest.skipIf(skip_conda_test, "conda is not available, so the conda environment related tests are skipped.")
class TestExecutableContainerConda(TestWithProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        with open("env.yaml", "w") as f:
            f.writelines(conda_env)
        cls.project.conda_environment.create(env_name="py312", env_file="env.yaml")

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        os.remove("env.yaml")

    def test_conda_environment_path(self):
        self.project.create_job_class(
            class_name="PythonVersionJob",
            executable_str="python --version",
        )
        job = self.project.create.job.PythonVersionJob(job_name="job_conda_path")
        job.server.conda_environment_path = self.project.conda_environment.py312
        job.run()
        self.assertEqual(job["error.out"][0], "Python 3.12.1\n")
