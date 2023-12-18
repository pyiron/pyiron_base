import json
import os
from pyiron_base import Project
from pyiron_base.jobs.dynamic import create_new_job_type
from pyiron_base._tests import TestWithProject


def write_input(input_dict, working_directory="."):
    with open(os.path.join(working_directory, "input.json"), "w") as f:
        json.dump(input_dict, f)


def collect_output(working_directory="."):
    with open(os.path.join(working_directory, "output.json"), "r") as f:
        return json.load(f)


class DynamicTest(TestWithProject):
    def test_dynamic_job_from_functions(self):
        class_name = "MyDynamicJob"
        job_type = create_new_job_type(
            class_name=class_name,
            input_dict={"energy": 1.0},
            write_input_funct=write_input,
            collect_output_funct=collect_output,
            executable_str="cat input.json > output.json"
        )
        job = self.project.create_job(job_type=job_type, job_name="test")
        job.input.energy = 2.0
        job.run()
        self.assertEqual(job["NAME"], class_name)
        self.assertEqual(job["TYPE"], "<class 'abc." + class_name + "'>")
        self.assertEqual(job.output.energy, 2.0)
        self.project.remove_jobs(recursive=True, silently=True)
