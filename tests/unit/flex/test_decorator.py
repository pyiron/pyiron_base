from pyiron_base._tests import TestWithProject
from pyiron_base import pyiron_job
import unittest


class TestPythonFunctionDecorator(TestWithProject):
    def tearDown(self):
        self.project.remove_jobs(recursive=True, silently=True)

    def test_delayed(self):
        @pyiron_job(project=self.project)
        def my_function_a(a, b=8):
            return a + b

        @pyiron_job(project=self.project, cores=2)
        def my_function_b(a, b=8):
            return a + b

        c = my_function_a(a=1, b=2)
        d = my_function_b(a=c, b=3)
        self.assertEqual(d.pull(), 6)
        nodes_dict, edges_lst = d.get_graph()
        self.assertEqual(len(nodes_dict), 6)
        self.assertEqual(len(edges_lst), 6)

    def test_delayed_simple(self):
        @pyiron_job
        def my_function_a(a, b=8):
            return a + b

        @pyiron_job
        def my_function_b(a, b=8):
            return a + b

        c = my_function_a(a=1, b=2, project=self.project)
        d = my_function_b(a=c, b=3, project=self.project, resource_dict={"cores": 2})
        self.assertEqual(d.pull(), 6)
        nodes_dict, edges_lst = d.get_graph()
        self.assertEqual(len(nodes_dict), 6)
        self.assertEqual(len(edges_lst), 6)


if __name__ == "__main__":
    unittest.main()
