import os
from pyiron_base._tests import TestWithProject
from pyiron_base.jobs.job.jobtype import JOB_CLASS_DICT


def write_input(input_dict, working_directory="."):
    with open(os.path.join(working_directory, "input_file"), "w") as f:
        f.write(str(input_dict["energy"]))


def collect_output(working_directory="."):
    with open(os.path.join(working_directory, "output_file"), "r") as f:
        return {"energy": float(f.readline())}


class TestExecutableContainer(TestWithProject):
    def test_exeuctablecontainer(self):
        energy_value = 2.0
        self.project.create_job_class(
            class_name="CatJob",
            write_input_funct=write_input,
            collect_output_funct=collect_output,
            default_input_dict={"energy": 1.0},
            executable_str="cat input_file > output_file",
        )
        job = self.project.create.job.CatJob(job_name="job_test")
        job.input["energy"] = energy_value
        job.run()
        self.assertEqual(job.output["energy"], energy_value)
        job_reload = self.project.load(job.job_name)
        self.assertEqual(job_reload.input["energy"], energy_value)
        self.assertEqual(job_reload.output["energy"], energy_value)
        del JOB_CLASS_DICT["CatJob"]