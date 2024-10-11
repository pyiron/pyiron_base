from dataclasses import asdict
import os
import subprocess
import unittest
from time import sleep
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


def write_input_series(working_directory, input_dict):
    for k, v in input_dict.items():
        with open(os.path.join(working_directory, k + ".txt"), "w") as f:
            f.writelines(str(v))


def collect_output_series(working_directory):
    with open(os.path.join(working_directory, "result.txt"), "r") as f:
        return {"result": int(f.readlines()[0])}


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
        self.assertEqual(job.output["stdout"], "")
        self.assertEqual(job.output["energy"], energy_value)
        job_reload = self.project.load(job.job_name)
        self.assertEqual(job_reload.input["energy"], energy_value)
        self.assertEqual(job_reload.output["energy"], energy_value)
        self.assertEqual(str(job.executable), "cat input_file > output_file")
        self.assertEqual(str(job_reload.executable), "cat input_file > output_file")
        executable_dict = {
            "version": "cat input_file > output_file",
            "name": "executablecontainerjob",
            "operation_system_nt": os.name == "nt",
            "executable": None,
            "mpi": False,
            "accepted_return_codes": [0],
        }
        self.assertEqual(asdict(job.executable.storage), executable_dict)
        self.assertEqual(asdict(job_reload.executable.storage), executable_dict)
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
            project=ProjectHDFio(
                project=self.project, file_name="any.h5", h5_path=None, mode=None
            ),
            job_name="job_test",
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
            project=ProjectHDFio(
                project=self.project, file_name="any.h5", h5_path=None, mode=None
            ),
            job_name="job_output_files",
        )
        job.run()
        self.assertEqual(job.output["stdout"], "")
        for file in ["error_out", "input_file", "output_file"]:
            self.assertTrue(file in dir(job.files))
        output_file_path = os.path.abspath(
            os.path.join(
                __file__,
                "..",
                "test_executablecontainer",
                "job_output_files_hdf5",
                "job_output_files",
                "error.out",
            )
        )
        self.assertEqual(str(job.files.error_out), output_file_path)

    def test_create_job_factory_typeerror(self):
        create_catjob = create_job_factory(
            write_input_funct=write_input,
            collect_output_funct=collect_output,
            executable_str="cat input_file > output_file",
        )
        with self.assertRaises(TypeError):
            create_catjob(project="project", job_name="job_test")

    def test_create_job_factory_no_functions(self):
        create_catjob = create_job_factory(
            executable_str="python --version",
        )
        job = create_catjob(
            project=ProjectHDFio(
                project=self.project, file_name="any.h5", h5_path=None, mode=None
            ),
            job_name="job_no",
        )
        job.server.cores = 2
        self.assertEqual(job.server.cores, 2)
        with self.assertWarns(RuntimeWarning):
            # No multi core executable found falling back to the single core executable.
            job.run()
        self.assertEqual(job.server.cores, 1)
        self.assertTrue("Python" in job.output["stdout"])
        self.assertTrue(job.status.finished)
        self.assertEqual(os.listdir(job.working_directory), ["error.out"])
        with open(os.path.join(job.working_directory, "error.out"), "r") as f:
            content = f.readlines()
        self.assertEqual(content[0].split()[0], "Python")

    @unittest.skipIf(
        os.name == "nt", "Starting subprocesses on windows take a long time."
    )
    def test_job_run_mode_manual(self):
        create_sleep_job = create_job_factory(
            executable_str="sleep 10",
        )
        job = create_sleep_job(
            project=ProjectHDFio(
                project=self.project,
                file_name="job_sleep.h5",
                h5_path=None,
                mode=None,
            ),
            job_name="job_sleep",
        )
        job.server.run_mode.manual = True
        job.run()
        self.assertTrue(job.status.submitted)
        self.assertTrue(os.path.exists(job.project_hdf5.file_name))
        process = subprocess.Popen(
            [
                "python",
                "-m",
                "pyiron_base.cli",
                "wrapper",
                "-p",
                job.project.path,
                "-j",
                str(job.job_id),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=job.project.path,
        )
        sleep(5)
        if process.poll() is not None:
            res = process.communicate()
            print("Debug test_job_run_mode_manual():", process.returncode, res)
        else:
            self.assertIsNone(process.poll())
        process.terminate()
        sleep(1)
        self.assertTrue(job.status.aborted)

    @unittest.skipIf(
        os.name == "nt",
        "shell script test is skipped on windows.",
    )
    def test_series_of_jobs(self):
        z = self.project.wrap_executable(
            job_name="job_xy",
            executable_str="x=$(cat x.txt); y=$(cat y.txt); echo $(($x + $y)) > result.txt",
            write_input_funct=write_input_series,
            collect_output_funct=collect_output_series,
            input_dict={"x": 1, "y": 2},
            conda_environment_path=None,
            conda_environment_name=None,
            input_file_lst=None,
            execute_job=True,
        )
        w = self.project.wrap_executable(
            job_name="job_xyz",
            executable_str="x=$(cat x.txt); y=$(cat y.txt); z=$(cat result.txt); echo $(($x + $y + $z)) > result.txt",
            write_input_funct=write_input_series,
            collect_output_funct=collect_output_series,
            input_dict={"x": 1, "y": z.output.result},
            conda_environment_path=None,
            conda_environment_name=None,
            input_file_lst=[z.files.result_txt],
            execute_job=True,
        )
        self.assertEqual(w.output.result, 7)

    @unittest.skipIf(
        os.name == "nt",
        "shell script test is skipped on windows.",
    )
    def test_series_of_jobs(self):
        z = self.project.wrap_executable(
            executable_str="x=$(cat x.txt); y=$(cat y.txt); echo $(($x + $y)) > result.txt",
            write_input_funct=write_input_series,
            collect_output_funct=collect_output_series,
            input_dict={"x": 1, "y": 2},
            conda_environment_path=None,
            conda_environment_name=None,
            input_file_lst=None,
            execute_job=True,
        )
        w = self.project.wrap_executable(
            executable_str="x=$(cat x.txt); y=$(cat y.txt); z=$(cat result.txt); echo $(($x + $y + $z)) > result.txt",
            write_input_funct=write_input_series,
            collect_output_funct=collect_output_series,
            input_dict={"x": 1, "y": z.output.result},
            conda_environment_path=None,
            conda_environment_name=None,
            input_file_lst=[z.files.result_txt],
            execute_job=True,
        )
        self.assertEqual(w.output.result, 7)

    @unittest.skipIf(
        os.name == "nt",
        "delayed shell script test is skipped on windows.",
    )
    def test_delayed_series_of_jobs(self):
        z = self.project.wrap_executable(
            job_name="job_xy",
            executable_str="x=$(cat x.txt); y=$(cat y.txt); echo $(($x + $y)) > result.txt",
            write_input_funct=write_input_series,
            collect_output_funct=collect_output_series,
            input_dict={"x": 1, "y": 2},
            conda_environment_path=None,
            conda_environment_name=None,
            input_file_lst=None,
            delayed=True,
            output_file_lst=["result.txt"],
            output_key_lst=["result"],
        )
        w = self.project.wrap_executable(
            job_name="job_xyz",
            executable_str="x=$(cat x.txt); y=$(cat y.txt); z=$(cat result.txt); echo $(($x + $y + $z)) > result.txt",
            write_input_funct=write_input_series,
            collect_output_funct=collect_output_series,
            input_dict={"x": 1, "y": z.output.result},
            conda_environment_path=None,
            conda_environment_name=None,
            input_file_lst=[z.files.result_txt],
            delayed=True,
            output_file_lst=["result.txt"],
            output_key_lst=["result"],
        )
        self.assertEqual(w.output.result.pull(), 7)
        nodes_dict, edges_lst = w.get_graph()
        self.assertEqual(len(nodes_dict), 17)
        self.assertEqual(len(edges_lst), 27)
        w.server.cores = 2
        self.assertEqual(w.server.cores, 2)
        job_w = w.pull()
        self.assertEqual(w.server.cores, 2)
        self.assertEqual(job_w.server.cores, 1)
        job_z = z.pull()
        self.assertEqual(job_w.output.result, 7)
        self.project.remove_job(job_z.job_name)
        self.project.remove_job(job_w.job_name)

    @unittest.skipIf(
        os.name == "nt",
        "delayed shell script test is skipped on windows.",
    )
    def test_delayed_series_of_jobs_without_job_name(self):
        z = self.project.wrap_executable(
            executable_str="x=$(cat x.txt); y=$(cat y.txt); echo $(($x + $y)) > result.txt",
            write_input_funct=write_input_series,
            collect_output_funct=collect_output_series,
            input_dict={"x": 1, "y": 2},
            conda_environment_path=None,
            conda_environment_name=None,
            input_file_lst=None,
            delayed=True,
            output_file_lst=["result.txt"],
            output_key_lst=["result"],
        )
        w = self.project.wrap_executable(
            executable_str="x=$(cat x.txt); y=$(cat y.txt); z=$(cat result.txt); echo $(($x + $y + $z)) > result.txt",
            write_input_funct=write_input_series,
            collect_output_funct=collect_output_series,
            input_dict={"x": 1, "y": z.output.result},
            conda_environment_path=None,
            conda_environment_name=None,
            input_file_lst=[z.files.result_txt],
            delayed=True,
            output_file_lst=["result.txt"],
            output_key_lst=["result"],
        )
        self.assertEqual(w.output.result.pull(), 7)
        nodes_dict, edges_lst = w.get_graph()
        self.assertEqual(len(nodes_dict), 16)
        self.assertEqual(len(edges_lst), 27)
        job_w = w.pull()
        job_z = z.pull()
        self.assertEqual(job_w.output.result, 7)
        self.project.remove_job(job_z.job_name)
        self.project.remove_job(job_w.job_name)
