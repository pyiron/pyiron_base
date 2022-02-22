import os
from pyiron_base._tests import TestWithCleanProject


script_py = """\
from pyiron_base import load, dump
input_dict = load()
output_dict = input_dict.copy()
dump(output_dict)
"""


class TestScriptJob(TestWithCleanProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def test_notebook_input(self):
        """
        Makes sure that the ScriptJob saves its input class in
        hdf["input/custom_group"] as this is needed when running external
        Notebook jobs c.f. `Notebook.get_custom_dict()`.
        """
        job = cls.project.create.job.ScriptJob("test_notebook")
        job.input['value'] = 300
        job.save()
        self.assertTrue(
            "custom_dict" in job["input"].list_groups(),
            msg="Input not saved in the 'custom_dict' group in HDF"
        )
        self.project.remove_job("test_notebook")

    def test_python_input(self):
        job = cls.project.create.job.ScriptJob("test_input")
   
        file_name = "test.py"
        with open(file_name, "w") as f:
            f.writelines(script_py)

        input_dict = {"a": 1, "b": [1,2,3]}
            
        job = pr.create.job.ScriptJob("script")
        job.script_path = os.path.abspath(file_name)
        job.input.update(input_dict)
        job.run()

        data_dict = job["output"].to_object().to_builtin()
        for k, v in input_dict.items():
            self.assertTrue(data_dict[k], v)
        self.project.remove_job("test_input")
