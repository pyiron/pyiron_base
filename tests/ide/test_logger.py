# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from unittest import TestCase
from pyiron_base.ide.logger import logger
import os


class TestLogger(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.logger_file = os.path.join(os.getcwd(), 'pyiron.log')

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            os.remove(cls.logger_file)
        except OSError:
            pass

    def test_logger(self):
        logsize = os.path.getsize(self.logger_file)

        logger.warning("Here is a warning")

        self.assertGreater(os.path.getsize(self.logger_file), logsize)

    def test_set_logging_level(self):
        logger.set_logging_level(10)
        self.assertEqual(10, logger.getEffectiveLevel(), "Overall logger level should match input")
        self.assertEqual(10, logger.handlers[0].level, "Stream level should match input")
        self.assertEqual(10, logger.handlers[0].level, "File level should match input")

        logger.set_logging_level(20, channel=1)
        self.assertEqual(10, logger.getEffectiveLevel(), "Overall logger level should not have changed")
        self.assertEqual(10, logger.handlers[0].level, "Stream level should not have changed")
        self.assertEqual(20, logger.handlers[1].level, "File level should match input")

        logger.set_logging_level("WARNING", channel=0)
        self.assertEqual(30, logger.handlers[0].level, "Should be able to set by string")
