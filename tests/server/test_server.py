# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import unittest
from pyiron_base.server.generic import Server
from pyiron_base._tests import PyironTestCase


class TestRunmode(PyironTestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = Server()

    def test_queue_set_None(self):
        try:
            self.server.queue = None
        except:
            self.fail("queue should accept None")

        self.assertEqual(self.server._active_queue, None,
                "active queue not set to None")
        self.assertTrue(self.server.run_mode.modal,
                "run_mode default not restored after reseting queue")
        self.assertEqual(self.server.cores, 1,
                "cores default not restored after reseting queue")
        self.assertEqual(self.server.threads, 1,
                "threads default not restored after reseting queue")
        self.assertEqual(self.server.run_time, None,
                "run_time default not restored after reseting queue")
        self.assertEqual(self.server.memory_limit, None,
                "memory_limit default not restored after reseting queue")

if __name__ == "__main__":
    unittest.main()
