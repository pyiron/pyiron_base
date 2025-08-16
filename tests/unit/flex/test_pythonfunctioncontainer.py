import os
import unittest
from concurrent.futures import ProcessPoolExecutor
import sys
from time import sleep
from pyiron_base._tests import TestWithProject


def my_function(a, b=8):
    return a + b


def my_function_str():
    return "hello"


def my_sleep_funct(a, b=8, sleep_time=0.01):
    sleep(sleep_time)
    return a + b


def my_function_exe(a_lst, b_lst, executor):
    future_lst = [executor.submit(my_function, a=a, b=b) for a, b in zip(a_lst, b_lst)]
    return [future.result() for future in future_lst]


def function_with_dict(a, b=1, c=3):
    return {"a": a, "b": b, "c": c}


def function_with_error(a, b):
    raise ValueError()


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
        self.project.remove_job(job.job_name)

    def test_as_job_without_arguments(self):
        job = self.project.wrap_python_function(my_function_str)
        job.run()
        self.assertEqual(job.output["result"], "hello")
        self.assertTrue(job.status.finished)
        job_reload = self.project.load(job.job_name)
        self.assertEqual(job_reload.output["result"], "hello")
        self.project.remove_job(job.job_name)

    def test_as_job_without_arguments_delayed(self):
        delayed_obj = self.project.wrap_python_function(
            python_function=my_function_str,
            delayed=True,
        )
        self.assertEqual(delayed_obj.input, {})
        self.assertEqual(delayed_obj.pull(), "hello")

    def test_as_function(self):
        my_function_as_job = self.project.wrap_python_function(my_function)
        self.assertEqual(my_function_as_job(a=5, b=6), 11)
        self.assertTrue(my_function_as_job.status.finished)
        self.assertEqual(my_function_as_job(a=5, b=6), 11)
        job_reload = self.project.load(my_function_as_job.job_name)
        self.assertEqual(job_reload.input["a"], 5)
        self.assertEqual(job_reload.input["b"], 6)
        self.assertEqual(job_reload.output["result"], 11)
        self.project.remove_job(job_reload.job_name)

    def test_direct_function_call(self):
        result = self.project.wrap_python_function(
            my_function, 7, b=8, execute_job=True
        )
        self.assertEqual(result, 15)
        job_reload = self.project.load(self.project.get_job_ids()[-1])
        self.assertEqual(job_reload.input["a"], 7)
        self.assertEqual(job_reload.input["b"], 8)
        self.assertEqual(job_reload.output["result"], 15)
        self.project.remove_job(job_reload.job_name)

    def test_with_executor(self):
        with ProcessPoolExecutor() as exe:
            job = self.project.wrap_python_function(my_sleep_funct)
            job.input["a"] = 4
            job.input["b"] = 5
            job.server.executor = exe
            self.assertTrue(job.server.run_mode.executor)
            job.run()
            self.assertFalse(job.server.future.done())
            self.assertIsNone(job.server.future.result())
            self.assertTrue(job.server.future.done())
            self.project.remove_job(job.job_name)

    @unittest.skipIf(
        os.name == "nt", "Starting subprocesses on windows take a long time."
    )
    def test_terminate_job(self):
        job = self.project.wrap_python_function(my_sleep_funct)
        job.input["a"] = 5
        job.input["b"] = 6
        job.input["sleep_time"] = 20
        job.server.run_mode.thread = True
        job.run()
        self.assertIsNotNone(job._process)
        sleep(5)
        job._process.terminate()
        sleep(1)
        self.assertTrue(job.status.aborted)
        self.assertEqual(job["status"], "aborted")
        self.project.remove_job(job.job_name)

    @unittest.skipIf(sys.version_info < (3, 11), reason="requires python3.11 or higher")
    def test_with_executor_wait(self):
        with ProcessPoolExecutor() as exe:
            job = self.project.wrap_python_function(my_sleep_funct)
            job.input["a"] = 4
            job.input["b"] = 6
            job.server.executor = exe
            self.assertTrue(job.server.run_mode.executor)
            job.run()
            self.assertFalse(job.server.future.done())
            self.project.wait_for_job(job=job, interval_in_s=0.01, max_iterations=1000)
            self.assertTrue(job.server.future.done())
            self.project.remove_job(job.job_name)

    def test_with_internal_executor(self):
        job = self.project.wrap_python_function(my_function_exe)
        job.input["a_lst"] = [1, 2, 3, 4]
        job.input["b_lst"] = [5, 6, 7, 8]
        job.server.cores = 2
        with self.assertRaises(ImportError):
            job.executor_type = "Executor"
        job.executor_type = ProcessPoolExecutor
        self.assertTrue(
            isinstance(job._get_executor(max_workers=2), ProcessPoolExecutor)
        )
        job.executor_type = None
        with self.assertRaises(ValueError):
            job._get_executor(max_workers=2)
        job.executor_type = "concurrent.futures.ProcessPoolExecutor"
        self.assertTrue(
            isinstance(job._get_executor(max_workers=2), ProcessPoolExecutor)
        )
        job.run()
        self.assertEqual(job.output["result"], [6, 8, 10, 12])
        self.assertTrue(job.status.finished)
        self.project.remove_job(job.job_name)

    def test_name_options(self):
        with self.subTest("Auto name and rename"):
            job = self.project.wrap_python_function(my_function)
            job.input["a"] = 1
            job.input["b"] = 2

            self.assertEqual(
                my_function.__name__,
                job.job_name,
                msg="Docs claim job name takes function name by default",
            )
            pre_save_name = job.job_name
            try:
                job.save()
                self.assertNotEqual(
                    pre_save_name,
                    job.job_name,
                    msg="Docs claim default is to modify the name on save",
                )
                self.assertTrue(
                    pre_save_name in job.job_name,
                    msg="The job name should still be based off the original name",
                )
            finally:
                job.remove()

        with self.subTest("Custom name and rename"):
            name = "foo"
            job = self.project.wrap_python_function(my_function, job_name=name)
            job.input["a"] = 1
            job.input["b"] = 2

            self.assertEqual(name, job.job_name, msg="Provided name should be used")
            try:
                job.save()
                self.assertNotEqual(
                    name,
                    job.job_name,
                    msg="Docs claim default is to modify the name on save",
                )
                print("NAME STUFF", name, job.job_name)
                self.assertTrue(
                    name in job.job_name,
                    msg="The job name should still be based off the original name",
                )
            finally:
                job.remove()

        with self.subTest("No rename"):
            job = self.project.wrap_python_function(
                my_function, automatically_rename=False
            )
            job.input["a"] = 1
            job.input["b"] = 2

            pre_save_name = job.job_name
            try:
                job.save()
                self.assertEqual(
                    pre_save_name,
                    job.job_name,
                    msg="We should be able to deactivate the automatic renaming",
                )
                n_ids = len(self.project.job_table())
                job.save()
                self.assertEqual(
                    n_ids,
                    len(self.project.job_table()),
                    msg="When re-saving, the job should be found and loaded instead",
                )
            finally:
                job.remove()

    def test_series(self):
        c = self.project.wrap_python_function(
            python_function=my_function, a=1, b=2, execute_job=True
        )
        self.assertEqual(c, 3)
        d = self.project.wrap_python_function(
            python_function=my_function, a=c, b=3, execute_job=True
        )
        self.assertEqual(d, 6)

    @unittest.skipIf(sys.version_info < (3, 11), reason="requires python3.11 or higher")
    def test_function_with_error(self):
        delayed_obj = self.project.wrap_python_function(
            python_function=function_with_error, a=1, b=2, delayed=True
        )
        future = delayed_obj.pull()
        with self.assertRaises(ValueError):
            future.result()
        self.assertTrue(delayed_obj._job.status.aborted)

    def test_delayed(self):
        c = self.project.wrap_python_function(
            python_function=my_function, a=1, b=2, delayed=True
        )
        d = self.project.wrap_python_function(
            python_function=my_function, a=c, b=3, delayed=True
        )
        self.assertEqual(d.pull(), 6)
        nodes_dict, edges_lst = d.get_graph()
        self.assertEqual(len(nodes_dict), 7)
        self.assertEqual(len(edges_lst), 8)

    def test_delayed_non_modal(self):
        c = self.project.wrap_python_function(
            python_function=my_sleep_funct, a=1, b=2, delayed=True
        )
        c.server.run_mode.non_modal = True
        future = c.pull()
        self.assertFalse(future.done())
        self.project.wait_for_job(future.job)
        self.assertTrue(future.done())
        self.assertEqual(future.result(), 3)
        nodes_dict, edges_lst = c.get_graph()
        self.assertEqual(len(nodes_dict), 5)
        self.assertEqual(len(edges_lst), 4)

    def test_delayed_dict(self):
        job_1 = self.project.wrap_python_function(
            python_function=function_with_dict,
            a=6,
            b=4,
            c=3,
            delayed=True,
            output_key_lst=["a", "b", "c"],
        )
        job_2 = self.project.wrap_python_function(
            python_function=function_with_dict,
            a=job_1.output.a,
            b=5,
            c=job_1.output.c,
            delayed=True,
            output_key_lst=["a", "b", "c"],
        )
        self.assertEqual(job_2.pull(), {"a": 6, "b": 5, "c": 3})
