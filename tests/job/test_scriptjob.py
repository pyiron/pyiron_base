from pyiron_base._tests import TestWithCleanProject


class TestScriptJob(TestWithCleanProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.job = cls.project.create.job.ScriptJob("test")

    def test_notebook_input(self):
        """
        Makes sure that the ScriptJob saves its input class in
        hdf["input/custom_group"] as this is needed when running external
        Notebook jobs c.f. `Notebook.get_custom_dict()`.
        """
        self.job.input['value'] = 300
        self.job.save()
        self.assertTrue("custom_dict" in self.job["input"].list_groups(),
                        msg="Input not saved in the 'custom_dict' group in HDF")
