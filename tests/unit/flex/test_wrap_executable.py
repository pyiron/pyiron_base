import os
from pyiron_base._tests import TestWithProject


def write_input(input_dict, working_directory="."):
    with open(os.path.join(working_directory, "input_file"), "w") as f:
        f.write(str(input_dict["energy"]))


def collect_output(working_directory="."):
    with open(os.path.join(working_directory, "output_file"), "r") as f:
        return {"energy": float(f.readline())}


class TestWrapExecutable(TestWithProject):
    def test_python_version(self):
        python_version_step = self.project.wrap_executable(
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
        self.assertTrue("Python" in python_version_step["error.out"][0])
        self.assertTrue(python_version_step.status.finished)
        self.assertEqual(
            python_version_step.files.error_out,
            os.path.join(
                os.path.abspath(python_version_step.working_directory), "error.out"
            ),
        )

    def test_cat(self):
        job = self.project.wrap_executable(
            job_name="Cat_Job_energy_1_0",
            write_input_funct=write_input,
            collect_output_funct=collect_output,
            input_dict={"energy": 1.0},
            executable_str="cat input_file > output_file",
            execute_job=False,
        )
        job.input.energy = 2.0
        job.run()
        self.assertEqual(job.output["stdout"], "")
        self.assertEqual(job.output.energy, 2.0)
