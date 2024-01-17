from pyiron_base._tests import TestWithProject


def my_function(a, b=8):
    return a+b


class TestPythonFunctionContainer(TestWithProject):
    def test_as_job(self):
        job = self.project.wrap_python_function(my_function)
        job.input["a"] = 4
        job.input["b"] = 5
        job.run()
        self.assertEqual(job.output["result"], 9)
        self.assertTrue(job.status.finished)
        job_reload = self.project.load(job.job_name)
        self.assertEqual(job_reload.input["a"], 4)
        self.assertEqual(job_reload.input["b"], 5)
        self.assertEqual(job_reload.output["result"], 9)

    def test_as_function(self):
        my_function_as_job = self.project.wrap_python_function(my_function)
        self.assertEqual(my_function_as_job(a=5, b=6), 11)
        self.assertTrue(my_function_as_job.status.finished)
        self.assertEqual(my_function_as_job(a=5, b=6), 11)
        job_reload = self.project.load(my_function_as_job.job_name)
        self.assertEqual(job_reload.input["a"], 5)
        self.assertEqual(job_reload.input["b"], 6)
        self.assertEqual(job_reload.output["result"], 11)
