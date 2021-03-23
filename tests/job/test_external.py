import os
import unittest

from pyiron_base import Project

job_py_source = """
from pyiron_base import Notebook as nb
coeff_tot = nb.get_custom_dict()
print(coeff_tot['value'])
"""

class TestScriptJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.file_location = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")  # replace to satisfy windows
        cls.job_location = os.path.join(cls.file_location, "job.py").replace("\\", "/")  # replace to satisfy windows
        cls.project = Project(os.path.join(cls.file_location, "test_notebook").replace("\\", "/"))
        cls.job = cls.project.create_job(cls.project.job_type.ScriptJob,
                                               "test")
        with open(cls.job_location, 'w') as f:
            f.write(job_py_source)

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.job_location)
        cls.project.remove(enable=True)


    def test_notebook_input(self):
        """
        Test that input is readable from external scripts.
        """
        self.job.input['value'] = 300
        self.job.script_path = self.job_location
        try:
            self.job.run()
        except Exception as e:
            self.fail("Running script job failed with {}".format(e))
