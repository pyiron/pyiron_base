import unittest
import os

from pyiron_base import Project
from pyiron_base.job.template import TemplateJob

command = "echo TemplateJob "


class ToyJob(TemplateJob):
    def __init__(self, project, job_name):
        super(ToyJob, self).__init__(project, job_name)
        self.input['input_energy'] = 100

    def write_input(self):
        pass

    def collect_output(self):
        with self.project_hdf5.open("output/generic") as h5out:
            with open(self.working_directory + "/output.txt") as out:
                h5out['output'] = out.read()


class TestTemplateJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        file_location = os.path.dirname(os.path.abspath(__file__))
        cls.file_location = file_location.replace("\\", "/")
        cls.project = Project(os.path.join(cls.file_location, "test_templatejob"))
        if os.name == 'nt':
            cls.exe = os.path.join(file_location, "executable.bat")
            cls.exe_mpi = os.path.join(file_location, "executable_mpi.bat")
        else:
            cls.exe = os.path.join(file_location, "executable.sh")
            cls.exe_mpi = os.path.join(file_location, "executable_mpi.sh")
        if os.name == 'nt':
            with open(cls.exe_mpi, 'w') as f:
                f.write(command + "%* > output.txt")
            with open(cls.exe, 'w') as f:
                f.write(command + "%* > output.txt")
        else:
            with open(cls.exe, 'w') as f:
                f.write("#!/bin/sh\n")
                f.write(command + "$@ > output.txt")
            with open(cls.exe_mpi, 'w') as f:
                f.write("#!/bin/sh\n")
                f.write(command + "$@ > output.txt")

    @classmethod
    def tearDownClass(cls):
        file_location = os.path.dirname(os.path.abspath(__file__))
        cls.file_location = file_location.replace("\\", "/")
        project = Project(os.path.join(cls.file_location, "test_templatejob"))
        project.remove(enable=True)
        if os.name == 'nt':
            cls.exe = os.path.join(file_location, "executable.bat")
        else:
            cls.exe = os.path.join(file_location, "executable.sh")
        os.remove(cls.exe)

    def test_initiating(self):
        cwd = self.file_location
        job = self.project.create_job(job_type=ToyJob, job_name="test_toy")
        self.assertEqual("test_toy", job.job_name)
        self.assertEqual("/test_toy", job.project_hdf5.h5_path)
        self.assertEqual(
            cwd + "/test_templatejob/test_toy.h5", job.project_hdf5.file_name
        )
        self.assertEqual(job.input["input_energy"], 100)
        job.to_hdf()
        self.assertTrue(os.path.isfile(job.project_hdf5.file_name))
        job.input['input_energy'] = 1
        self.assertEqual(job.input["input_energy"], 1)
        job.from_hdf()
        self.assertEqual(job.input["input_energy"], 100)
        job.project_hdf5.remove_file()
        self.assertFalse(os.path.isfile(job.project_hdf5.file_name))

    def test_run(self):
        job = self.project.create_job(job_type=ToyJob, job_name="test_toy")
        job.executable = self.exe
        job.run()
        # using .split() on the output string, since Windows and Unix produce different amount of whitespace.
        self.assertEqual(job["output/generic/output"].split(), ["TemplateJob"])
        self.assertFalse("RUNARGS" in job.list_nodes())

        job = self.project.create_job(job_type=ToyJob, job_name="test_toy2")
        job.executable = self.exe
        job.executable.additional_arguments = 'great job!'
        job.run()
        self.assertEqual(job["output/generic/output"].split(), ["TemplateJob", "great", "job!"])
        self.assertTrue("RUNARGS" in job.list_nodes())
        self.assertEqual(job["RUNARGS"], 'great job!')

        job = self.project.create_job(job_type=ToyJob, job_name="test_toy3")
        job.executable = self.exe_mpi
        job.executable.additional_arguments = 'great job!'
        job.server.cores = 2
        job.run()
        self.assertEqual(job["output/generic/output"].split(), ["TemplateJob", "2", "1", "great", "job!"])
        self.assertTrue("RUNARGS" in job.list_nodes())
        self.assertEqual(job["RUNARGS"], 'great job!')

    def test_reload(self):
        job = self.project.create_job(job_type=ToyJob, job_name="test_reload")
        job.executable = self.exe
        job.executable.additional_arguments = 'great job!'
        job.run()

        job = self.project.load("test_reload")
        self.assertTrue("RUNARGS" in job.list_nodes())
        self.assertEqual(job["RUNARGS"], 'great job!')

        # Custom executable is NOT loaded! Thus the additional RUNARGS cannot be loaded.
        # job.run(delete_existing_job=True)
        # self.assertEqual(job.executable.additional_arguments, 'great job!')


if __name__ == '__main__':
    unittest.main()
