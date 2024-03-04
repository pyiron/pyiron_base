import os
import unittest
from concurrent.futures import ProcessPoolExecutor
import sys
from time import sleep
from pyiron_base._tests import TestWithProject


def my_function(a, b=8):
    return a+b


def my_sleep_funct(a, b=8, sleep_time=0.01):
    sleep(sleep_time)
    return a+b


def my_function_exe(a_lst, b_lst, executor):
    future_lst = [executor.submit(my_function, a=a, b=b) for a, b in zip(a_lst, b_lst)]
    return [future.result() for future in future_lst]


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

    def test_with_internal_executor(self):
        job = self.project.wrap_python_function(my_function_exe)
        job.input["a_lst"] = [1, 2, 3, 4]
        job.input["b_lst"] = [5, 6, 7, 8]
        job.server.cores = 2
        with self.assertRaises(ImportError):
            job.executor_type = "Executor"
        job.executor_type = ProcessPoolExecutor
        self.assertTrue(isinstance(job._get_executor(max_workers=2), ProcessPoolExecutor))
        job.executor_type = None
        with self.assertRaises(ValueError):
            job._get_executor(max_workers=2)
        job.executor_type = "concurrent.futures.ProcessPoolExecutor"
        self.assertTrue(isinstance(job._get_executor(max_workers=2), ProcessPoolExecutor))
        job.run()
        self.assertEqual(job.output["result"], [6, 8, 10, 12])
        self.assertTrue(job.status.finished)

    def test_name_mangling(self):
        def make_a_simple_job():
            job = self.project.wrap_python_function(my_function)
            job.input["a"] = 1
            job.input["b"] = 2
            return job

        job = make_a_simple_job()
        self.assertEqual(
            job.job_name,
            my_function.__name__,
            msg="Sanity check"
        )
        try:
            job.save()
            self.assertNotEqual(
                job.job_name,
                my_function.__name__,
                msg="By default, we expect the wrapped job names to get mangled based "
                    "on their input so multiple calls to the wrap get unique names"
            )
            loaded = self.project.load(job.job_name)
            self.assertTrue(
                loaded._automatically_rename_on_save_using_input,
                msg="The mangling preference should survive saving and loading"
            )
        finally:
            job.remove()

        job = make_a_simple_job()
        job._automatically_rename_on_save_using_input = False
        try:
            job.save()
            self.assertEqual(
                job.job_name,
                my_function.__name__,
                msg="When requested, the job name should retain its original value"
            )
            loaded = self.project.load(job.job_name)
            self.assertFalse(
                loaded._automatically_rename_on_save_using_input,
                msg="The mangling preference should survive saving and loading"
            )
        finally:
            job.remove()
