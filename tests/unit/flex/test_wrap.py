import os
from pyiron_base._tests import TestWithProject


def my_function(a, b=8):
    return a + b


def write_input(input_dict, working_directory="."):
    with open(os.path.join(working_directory, "input_file"), "w") as f:
        f.write(str(input_dict["energy"]))


def collect_output(working_directory="."):
    with open(os.path.join(working_directory, "output_file"), "r") as f:
        return {"energy": float(f.readline())}


class TestWrap(TestWithProject):
    def test_executable(self):
        job = self.project.wrap(
            job_name="Cat_Job_energy_1_0",
            write_input_funct=write_input,
            collect_output_funct=collect_output,
            input_dict={"energy": 2.0},
            executable="cat input_file > output_file",
        )
        job.run()
        self.assertEqual(job.output["stdout"], "")
        self.assertEqual(job.output.energy, 2.0)
        self.assertTrue(job.status.finished)

    def test_executable_errors(self):
        with self.assertRaises(TypeError):
            self.project.wrap(
                executable="cat input_file > output_file",
                a=1
            )
        with self.assertRaises(TypeError):
            self.project.wrap(
                "cat input_file > output_file",
                1,
            )

    def test_python_function(self):
        job = self.project.wrap(my_function, 4, 5)
        job.run()
        self.assertEqual(job.output["result"], 9)
        self.assertTrue(job.status.finished)

    def test_python_function_error(self):
        with self.assertRaises(TypeError):
            self.project.wrap(executable=my_function, write_input_funct=write_input)
        with self.assertRaises(TypeError):
            self.project.wrap(executable=my_function, collect_output_funct=collect_output)
        with self.assertRaises(TypeError):
            self.project.wrap(executable=my_function, input_dict={"a": 1})
        with self.assertRaises(TypeError):
            self.project.wrap(executable=my_function, conda_environment_path="test/path")
        with self.assertRaises(TypeError):
            self.project.wrap(executable=my_function, conda_environment_name="test")
        with self.assertRaises(TypeError):
            self.project.wrap(executable=my_function, input_file_lst=["test"])