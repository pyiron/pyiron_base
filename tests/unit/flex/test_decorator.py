from pyiron_base._tests import TestWithProject
from pyiron_base import job
import unittest


class TestPythonFunctionDecorator(TestWithProject):
    def tearDown(self):
        self.project.remove_jobs(recursive=True, silently=True)

    def test_delayed(self):
        @job()
        def my_function_a(a, b=8):
            return a + b

        @job(cores=2)
        def my_function_b(a, b=8):
            return a + b

        c = my_function_a(a=1, b=2, pyiron_project=self.project)
        d = my_function_b(a=c, b=3, pyiron_project=self.project)
        self.assertEqual(d.pull(), 6)
        nodes_dict, edges_lst = d.get_graph()
        self.assertEqual(len(nodes_dict), 6)
        self.assertEqual(len(edges_lst), 6)

    def test_delayed_simple(self):
        @job
        def my_function_a(a, b=8):
            return a + b

        @job
        def my_function_b(a, b=8):
            return a + b

        c = my_function_a(a=1, b=2, pyiron_project=self.project)
        d = my_function_b(
            a=c, b=3, pyiron_project=self.project, pyiron_resource_dict={"cores": 2}
        )
        self.assertEqual(d.pull(), 6)
        nodes_dict, edges_lst = d.get_graph()
        self.assertEqual(len(nodes_dict), 6)
        self.assertEqual(len(edges_lst), 6)


if __name__ == "__main__":
    unittest.main()
