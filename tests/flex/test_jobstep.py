import os
from pyiron_base._tests import TestWithProject


class TestExecutableContainer(TestWithProject):
    def test_conda_environment_path(self):
        python_version_step = self.project.create_job_step(
            job_name="pythonjobstep",
            executable_str="python --version",
            write_input_funct=None,
            collect_output_funct=None,
            input_dict=None,
            conda_environment_path=None,
            conda_environment_name=None,
            input_file_lst=None,
            execute_job=True,
        )
        self.assertTrue(python_version_step.status.finished)
        self.assertEqual(
            python_version_step.files.error_out,
            os.path.join(python_version_step.working_directory, "error.out")
        )
