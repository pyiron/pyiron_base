import unittest
import json
import os
from pyiron_base import Project
from pyiron_base.jobs.dynamic import create_new_job_type


def write_input(input_dict, working_directory="."):
    with open(os.path.join(working_directory, "input.json"), "w") as f:
        json.dump(input_dict, f)


def collect_output(working_directory="."):
    with open(os.path.join(working_directory, "output.json"), "r") as f:
        return json.load(f)


class DynamicTest(unittest.TestCase):
    def test_dynamic_job_from_functions(self):
        job_type = create_new_job_type(
            class_name="MyDynamicJob",
            input_dict={"energy": 1.0},
            write_input_funct=write_input,
            collect_output_funct=collect_output,
            executable_str="cat input.json > output.json"
        )
        pr = Project(".")
        job = pr.create_job(job_type=job_type, job_name="test")
        job.input.energy = 2.0
        job.run()
        self.assertEqual(job.output.energy, 2.0)
        pr.remove_jobs(recursive=True, silently=True)
