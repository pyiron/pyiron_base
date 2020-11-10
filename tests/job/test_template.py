import unittest
import os

from pyiron_base import Project
from pyiron_base.job.template import PythonTemplateJob


class ToyJob(PythonTemplateJob):
    def __init__(self, project, job_name):
        super(ToyJob, self).__init__(project, job_name)
        self.input['input_energy'] = 100

    def run_static(self):
        self.output["energy_tot"] = self.input["input_energy"]
        self.to_hdf()
        self.status.finished = True

class TestTemplateJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = os.path.dirname(os.path.abspath(__file__)).replace(
            "\\", "/"
        )
        cls.project = Project(os.path.join(cls.file_location, "test_templatejob"))

    @classmethod
    def tearDownClass(cls):
        file_location = os.path.dirname(os.path.abspath(__file__))
        project = Project(os.path.join(file_location, "test_templatejob"))
        project.remove(enable=True)

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
        job.run()
        self.assertEqual(job.output["energy_tot"], 100)

if __name__ == '__main__':
    unittest.main()
