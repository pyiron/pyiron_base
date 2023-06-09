# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from pyiron_base.jobs.job.extension.server.generic import Server
from pyiron_base._tests import PyironTestCase
from pyiron_base.state.queue_adapter import queue_adapters
from pyiron_base import state
import shutil
import os


def _write_queue_config(file_name, queue_name):
    with open(file_name, "w") as f:
        f.write("queue_type: SLURM\n")
        f.write(f"queue_primary: {queue_name}\n")
        f.write("queues:\n")
        f.write(
            f"  {queue_name}: {{cores_max: 42, cores_min: 1, run_time_max: 42, script: {queue_name}.sh}}\n"
        )
    with open(os.path.join(os.path.dirname(file_name), f"{queue_name}.sh"), "w") as f:
        f.write("")


class TestRunmode(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        """
        Populate three resource directories: one with nothing, one with a queue.yaml, and one with both queue and
        clusters.yaml. Then set these resources as resource paths.
        """
        cls.resource_paths = list(state.settings.resource_paths)
        here = os.getcwd()
        cls.test_resources = os.path.join(here, "testqueueadapters")
        resources = [os.path.join(cls.test_resources, "res0")]
        queue_dir = os.path.join(resources[0], "queues")
        os.makedirs(queue_dir)
        _write_queue_config(
            file_name=os.path.join(queue_dir, "queue.yaml"), queue_name="main"
        )

        cls.resource_paths = list(state.settings.resource_paths)
        state.settings.configuration["resource_paths"] = [
            state.settings.convert_path_to_abs_posix(p) for p in resources
        ]
        queue_adapters.construct_adapters()

    def setUp(self) -> None:
        self.server = Server()
        self.server_main = Server(queue="main")

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.test_resources)
        state.settings.configuration["resource_paths"] = cls.resource_paths
        queue_adapters.update()

    def test___init__(self):
        with self.subTest("None queue"):
            self.assertIs(
                self.server.queue, None, "init without arguments did set queue."
            )
            self.assertEqual(
                self.server.cores, 1, "init without arguments should be single core"
            )
            self.assertEqual(
                self.server.threads, 1, "init without arguments should be single thread"
            )
            self.assertIs(
                self.server.run_time,
                None,
                "init without arguments should not have a run_time",
            )
            self.assertIs(
                self.server.memory_limit,
                None,
                "init without arguments should not set a memory_limit",
            )
        with self.subTest("main queue"):
            self.assertIs(
                self.server_main.queue,
                "main",
                "init with queue main did not set queue.",
            )
            self.assertEqual(
                self.server_main.cores, 1, "init with queue main should be single core"
            )
            self.assertEqual(
                self.server_main.threads,
                1,
                "init with queue main should be single thread",
            )
            self.assertIs(
                self.server_main.run_time,
                None,
                "init with queue main should not have a run_time",
            )
            self.assertIs(
                self.server_main.memory_limit,
                None,
                "init with queue main should not set a memory_limit",
            )

    def test_queue_set_None(self):
        for server in [self.server, self.server_main]:
            with self.subTest(server.queue):
                try:
                    server.queue = None
                except Exception as err:
                    self.fail(f"queue should accept None, but got {err}")

                self.assertEqual(
                    server._active_queue, None, "active queue not set to None"
                )
                self.assertTrue(
                    server.run_mode.modal,
                    "run_mode default not restored after reseting queue",
                )
                self.assertEqual(
                    server.cores, 1, "cores default not restored after reseting queue"
                )
                self.assertEqual(
                    server.threads,
                    1,
                    "threads default not restored after reseting queue",
                )
                self.assertEqual(
                    server.run_time,
                    None,
                    "run_time default not restored after reseting queue",
                )
                self.assertEqual(
                    server.memory_limit,
                    None,
                    "memory_limit default not restored after reseting queue",
                )

    def test_list_queues(self):
        self.assertEqual(self.server.list_queues(), ["main"])
        self.assertEqual(self.server_main.list_queues(), ["main"])

    def test_set_cores(self):
        with self.subTest("None queue"):
            self.server.cores = 5
            self.assertEqual(self.server.cores, 5, "could not set cores to 5")
            self.assertIs(
                self.server.run_time, None, "setting cores should not change run_time"
            )
            self.assertIs(
                self.server.memory_limit,
                None,
                "setting cores should not set a memory_limit",
            )
        with self.subTest("main queue"):
            self.server_main.cores = 5
            self.assertEqual(self.server_main.cores, 5, "could not set cores to 5")
            self.assertIs(
                self.server_main.run_time,
                None,
                "setting cores should not change run_time",
            )
            self.assertIs(
                self.server_main.memory_limit,
                None,
                "setting cores should not set a memory_limit",
            )
        with self.subTest("main queue too many cores"):
            with self.assertLogs(state.logger) as w:
                self.server_main.cores = 500
                self.assertEqual(len(w.output), 1)
                self.assertEqual(
                    w.output[0], "WARNING:pyiron_log:Updated the number of cores to: 42"
                )
            self.assertEqual(
                self.server_main.cores,
                42,
                "cores should be maximum 42 defined as by the config",
            )
            self.assertIs(
                self.server_main.run_time,
                None,
                "setting cores should not change run_time",
            )
            self.assertIs(
                self.server_main.memory_limit,
                None,
                "setting cores should not set a memory_limit",
            )

    def test_set_queue(self):
        with self.subTest("None -> main"):
            self.server.queue = "main"
            self.assertEqual(self.server.queue, "main")
            self.assertEqual(self.server.cores, 1)
            self.assertIsNone(
                self.server.run_time,
                "On changing queue, None is conserved.",
            )
            self.assertIsNone(self.server.memory_limit)
        with self.subTest("main -> main"):
            self.server_main.queue = "main"
            self.assertEqual(self.server_main.queue, "main")
            self.assertEqual(self.server_main.cores, 1)
            self.assertIsNone(
                self.server_main.run_time,
                "On changing queue, None is conserved.",
            )
            self.assertIsNone(self.server_main.memory_limit)

    def test_set_runtime(self):
        with self.subTest("None queue"):
            self.server.run_time = 15
            self.assertEqual(self.server.run_time, 15, "could not set run_time to 15")
            self.assertEqual(
                self.server.cores, 1, "setting run_time should not change cores"
            )
            self.assertIs(
                self.server.memory_limit,
                None,
                "setting run_time should not set a memory_limit",
            )
        with self.subTest("main queue"):
            self.server_main.run_time = 15
            self.assertEqual(
                self.server_main.run_time, 15, "could not set run_time to 15"
            )
            self.assertEqual(
                self.server_main.cores,
                1,
                "setting run_time should not change run_time",
            )
            self.assertIs(
                self.server_main.memory_limit,
                None,
                "setting run_time should not set a memory_limit",
            )
        with self.subTest("main queue long run_time"):
            with self.assertLogs(state.logger) as w:
                self.server_main.run_time = 500
                self.assertEqual(len(w.output), 1)
                self.assertEqual(
                    w.output[0], "WARNING:pyiron_log:Updated the run time limit to: 42"
                )
            self.assertEqual(
                self.server_main.run_time,
                42,
                "run_time should be maximum 42 defined as by the config",
            )
            self.assertEqual(
                self.server_main.cores,
                1,
                "setting run_time should not change cores",
            )
            self.assertIs(
                self.server_main.memory_limit,
                None,
                "setting run_time should not set a memory_limit",
            )

    def test_set_memory_limit(self):
        with self.subTest("None queue"):
            self.server.memory_limit = 25
            self.assertEqual(
                self.server.memory_limit, 25, "could not set memory_limit to 25"
            )
            self.assertEqual(
                self.server.cores, 1, "setting run_time should not change cores"
            )
            self.assertIs(
                self.server.run_time,
                None,
                "setting memory_limit should not set a run_time",
            )
        with self.subTest("main queue"):
            self.server_main.memory_limit = 25
            self.assertEqual(
                self.server_main.memory_limit, 25, "could not set memory_limit to 25"
            )
            self.assertEqual(
                self.server_main.cores,
                1,
                "setting memory_limit should not change run_time",
            )
            self.assertIs(
                self.server_main.run_time,
                None,
                "setting memory_limit should not set a run_time",
            )


class TestServerHDF(unittest.TestCase):
    def test_to_and_from_hdf(self):
        server_empty = Server()
        server_full = Server()
        server_full.gpus = 10
        server_full.cores = 10
        server_full.threads = 2
        server_full.new_hdf = False
        hdf_dict_empty = {"server": {}}
        hdf_dict_full = {"server": {}}
        self.assertTrue(server_empty.new_hdf)
        self.assertFalse(server_full.new_hdf)
        self.assertIsNone(server_empty.gpus)
        self.assertEqual(server_full.gpus, 10)
        self.assertEqual(server_full.cores, 10)
        self.assertEqual(server_full.threads, 2)
        self.assertFalse(server_full.new_hdf)
        server_empty.to_hdf(hdf=hdf_dict_empty)
        server_full.to_hdf(hdf=hdf_dict_full)
        server_from_hdf = Server()
        server_from_hdf.from_hdf(hdf=hdf_dict_full)
        self.assertEqual(hdf_dict_full["server"]["gpus"], 10)
        self.assertTrue("gpus" not in hdf_dict_empty["server"].keys())
        self.assertEqual(server_from_hdf.gpus, 10)
        self.assertEqual(server_from_hdf.cores, 10)
        self.assertEqual(server_from_hdf.threads, 2)
        self.assertFalse(server_from_hdf.new_hdf)


if __name__ == "__main__":
    unittest.main()
