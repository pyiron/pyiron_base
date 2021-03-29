import os
from pyiron_base._tests import TestWithCleanProject

job_py_source = """
from pyiron_base import Notebook as nb
coeff_tot = nb.get_custom_dict()
print(coeff_tot['value'])
"""


class TestScriptJob(TestWithCleanProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.job_location = os.path.join(cls.file_location, "job.py")
        cls.job = cls.project.create.job.ScriptJob("test")
        with open(cls.job_location, 'w') as f:
            f.write(job_py_source)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        os.remove(cls.job_location)

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
