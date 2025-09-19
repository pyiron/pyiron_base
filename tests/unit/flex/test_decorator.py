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
        self.assertEqual(c.input, {"a": 1, "b": 2})
        c.input["a"] = 3
        self.assertEqual(c.input, {"a": 3, "b": 2})
        self.assertEqual(d.pull(), 8)
        nodes_dict, edges_lst = d.get_graph()
        self.assertEqual(len(nodes_dict), 7)
        self.assertEqual(len(edges_lst), 8)

    def test_return_dict(self):
        my_dict = {1: 33, 2: 151, 3: 6}

        @job
        def return_dict():
            return my_dict

        with self.assertRaises(ValueError):
            d = return_dict(pyiron_project=self.project)

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
        self.assertEqual(c.input, {"a": 1, "b": 2})
        c.input["a"] = 3
        self.assertEqual(c.input, {"a": 3, "b": 2})
        self.assertEqual(d.pull(), 8)
        nodes_dict, edges_lst = d.get_graph()
        self.assertEqual(len(nodes_dict), 7)
        self.assertEqual(len(edges_lst), 8)

    def test_delayed_return_types(self):
        @job
        def my_function_a(a, b=8):
            return [a + b]

        @job(cores=2, output_key_lst=["0"])
        def my_function_b(a, b=8):
            return [a + b]

        c = my_function_a(a=1, b=2, pyiron_project=self.project, list_length=1)
        for a in c:
            d = my_function_b(a=a, b=3, pyiron_project=self.project)
        self.assertEqual(c.input, {"a": 1, "b": 2})
        c.input["a"] = 3
        self.assertEqual(c.input, {"a": 3, "b": 2})
        self.assertEqual(d.pull(), [8])
        self.assertEqual(c.pull(), [5])
        nodes_dict, edges_lst = d.get_graph()
        self.assertEqual(len(nodes_dict), 8)
        self.assertEqual(len(edges_lst), 8)


if __name__ == "__main__":
    unittest.main()
