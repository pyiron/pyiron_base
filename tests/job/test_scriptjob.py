import os
import unittest

from pyiron_base import Project

class TestScriptJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = os.path.dirname(os.path.abspath(__file__)).replace(
            "\\", "/"
        )
        cls.project = Project(os.path.join(cls.file_location, "test_scriptjob"))
        cls.job = cls.project.create_job(cls.project.job_type.ScriptJob,
                                               "test")

    @classmethod
    def tearDownClass(cls):
        cls.project.remove(enable=True)


    def test_notebook_input(self):
        """
        Makes sure that the ScriptJob saves its input class in
        hdf["input/custom_group"] as this is needed when running external
        Notebook jobs c.f. `Notebook.get_custom_dict()`.
        """
        self.job.input['value'] = 300
        self.job.save()
        self.assertTrue("custom_dict" in self.job["input"].list_groups(),
                        "Input not saved in the 'custom_dict' group in HDF")
