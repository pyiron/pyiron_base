# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import signal
import unittest
from unittest.mock import MagicMock

from pyiron_base.state.signal import catch_signals


class TestCatchSignals(unittest.TestCase):
    def _get_handler(self, cleanup=None):
        """Enter the context manager and return the installed handler for SIGINT."""
        if cleanup is None:
            cleanup = MagicMock()
        cm = catch_signals(cleanup)
        cm.__enter__()
        handler = signal.getsignal(signal.SIGINT)
        cm.__exit__(None, None, None)
        return handler, cleanup

    def test_sigint_raises_keyboard_interrupt(self):
        cleanup = MagicMock()
        with self.assertRaises(KeyboardInterrupt):
            with catch_signals(cleanup):
                signal.raise_signal(signal.SIGINT)
        cleanup.assert_called_once_with(signal.SIGINT)

    def test_sigterm_calls_sys_exit(self):
        cleanup = MagicMock()
        with self.assertRaises(SystemExit):
            with catch_signals(cleanup):
                signal.raise_signal(signal.SIGTERM)
        cleanup.assert_called_once_with(signal.SIGTERM)

    def test_sigabrt_calls_sys_exit(self):
        cleanup = MagicMock()
        with self.assertRaises(SystemExit):
            with catch_signals(cleanup):
                signal.raise_signal(signal.SIGABRT)
        cleanup.assert_called_once_with(signal.SIGABRT)

    def test_cleanup_called_before_raise(self):
        call_order = []
        def cleanup(sig):
            call_order.append("cleanup")

        with self.assertRaises(KeyboardInterrupt):
            with catch_signals(cleanup):
                call_order.append("before")
                signal.raise_signal(signal.SIGINT)

        self.assertEqual(call_order, ["before", "cleanup"])

    def test_default_handlers_restored_after_context(self):
        cleanup = MagicMock()
        with catch_signals(cleanup):
            pass
        self.assertEqual(signal.getsignal(signal.SIGINT), signal.SIG_DFL)
        self.assertEqual(signal.getsignal(signal.SIGTERM), signal.SIG_DFL)
        self.assertEqual(signal.getsignal(signal.SIGABRT), signal.SIG_DFL)


if __name__ == "__main__":
    unittest.main()