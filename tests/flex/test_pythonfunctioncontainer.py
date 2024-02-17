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

    def test_terminate_job(self):
        job = self.project.wrap_python_function(my_sleep_funct)
        job.input["a"] = 5
        job.input["b"] = 6
        job.input["sleep_time"] = 20
        job.server.run_mode.thread = True
        job.run()
        self.assertIsNotNone(job._process)
        sleep(10)
        job._process.terminate()
        sleep(2)
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
