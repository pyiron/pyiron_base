import os
from pyiron_base._tests import TestWithProject
from pyiron_base.jobs.job.jobtype import JOB_CLASS_DICT
from pyiron_base import create_job_factory
from pyiron_base.storage.hdfio import ProjectHDFio


def write_input(input_dict, working_directory="."):
    with open(os.path.join(working_directory, "input_file"), "w") as f:
        f.write(str(input_dict["energy"]))


def collect_output(working_directory="."):
    with open(os.path.join(working_directory, "output_file"), "r") as f:
        return {"energy": float(f.readline())}


class TestExecutableContainer(TestWithProject):
    def test_create_job_class(self):
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
        self.assertEqual(str(job.executable), "cat input_file > output_file")
        self.assertEqual(str(job_reload.executable), "cat input_file > output_file")
        executable_dict = {
            'version': 'cat input_file > output_file',
            'name': 'executablecontainerjob',
            'operation_system_nt': os.name == "nt",
            'executable': None,
            'mpi': False,
            'accepted_return_codes': [0]
        }
        self.assertEqual(job.executable._storage.to_builtin(), executable_dict)
        self.assertEqual(job_reload.executable._storage.to_builtin(), executable_dict)
        del JOB_CLASS_DICT["CatJob"]

    def test_create_job_factory_with_project(self):
        energy_value = 2.0
        create_catjob = create_job_factory(
            write_input_funct=write_input,
            collect_output_funct=collect_output,
            default_input_dict={"energy": 1.0},
            executable_str="cat input_file > output_file",
        )
        job = create_catjob(project=self.project, job_name="job_test")
        job.input["energy"] = energy_value
        job.run()
        self.assertEqual(job.output["energy"], energy_value)
        job_reload = self.project.load(job.job_name)
        self.assertEqual(job_reload.input["energy"], energy_value)
        self.assertEqual(job_reload.output["energy"], energy_value)

    def test_create_job_factory_with_projecthdfio(self):
        energy_value = 2.0
        create_catjob = create_job_factory(
            write_input_funct=write_input,
            collect_output_funct=collect_output,
            default_input_dict={"energy": 1.0},
            executable_str="cat input_file > output_file",
        )
        job = create_catjob(
            project=ProjectHDFio(project=self.project, file_name="any.h5", h5_path=None, mode=None),
            job_name="job_test"
        )
        job.input["energy"] = energy_value
        job.run()
        self.assertEqual(job.output["energy"], energy_value)
        job_reload = self.project.load(job.job_name)
        self.assertEqual(job_reload.input["energy"], energy_value)
        self.assertEqual(job_reload.output["energy"], energy_value)

    def test_job_files(self):
        create_catjob = create_job_factory(
            write_input_funct=write_input,
            collect_output_funct=collect_output,
            default_input_dict={"energy": 1.0},
            executable_str="cat input_file > output_file",
        )
        job = create_catjob(
            project=ProjectHDFio(project=self.project, file_name="any.h5", h5_path=None, mode=None),
            job_name="job_output_files"
        )
        job.run()
        self.assertEqual(dir(job.files), ['error_out', 'input_file', 'output_file'])
        output_file_path = os.path.abspath(os.path.join(__file__, "..", "test_executablecontainer", "job_output_files_hdf5", "job_output_files", "error.out"))
        if os.name != "nt":
            self.assertEqual(job.files.error_out, output_file_path)
        else:
            self.assertEqual(job.files.error_out.replace("/", "\\"), output_file_path)

    def test_create_job_factory_typeerror(self):
        create_catjob = create_job_factory(
            write_input_funct=write_input,
            collect_output_funct=collect_output,
            executable_str="cat input_file > output_file",
        )
        with self.assertRaises(TypeError):
            create_catjob(
                project="project",
                job_name="job_test"
            )

    def test_create_job_factory_no_functions(self):
        create_catjob = create_job_factory(
            executable_str="python --version",
        )
        job = create_catjob(
            project=ProjectHDFio(project=self.project, file_name="any.h5", h5_path=None, mode=None),
            job_name="job_no"
        )
        job.run()
        self.assertTrue(job.status.finished)
        self.assertEqual(os.listdir(job.working_directory), ['error.out'])
        with open(os.path.join(job.working_directory, 'error.out'), "r") as f:
            content = f.readlines()
        self.assertEqual(content[0].split()[0], "Python")
