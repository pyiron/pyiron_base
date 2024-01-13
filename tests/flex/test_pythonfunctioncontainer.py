from pyiron_base._tests import TestWithProject


def my_function(a, b=8):
    return a+b


class TestPythonFunctionContainer(TestWithProject):
    def test_pythonfunctioncontainer(self):
        job = self.project.wrap_python_function(my_function)
        job.input["a"] = 4
        job.input["b"] = 5
        job.run()
        self.assertEqual(job.output["result"], 9)
        self.assertEqual(job.status, "finished")
        job_reload = self.project.load(job.job_name)
        self.assertEqual(job_reload.input["a"], 4)
        self.assertEqual(job_reload.input["b"], 5)
        self.assertEqual(job_reload.output["result"], 9)
