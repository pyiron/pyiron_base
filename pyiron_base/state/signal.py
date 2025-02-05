import contextlib
import signal
import sys

intercepted_signals = [
    signal.SIGINT,
    signal.SIGTERM,
    signal.SIGABRT,
]


@contextlib.contextmanager
def catch_signals(cleanup):
    """
    Context manager to catch signals.

    During the `with` statement a signal handler is installed to catch SIGINT,
    SIGTERM and SIGABRT.  On exit from the statement the default handlers from
    the operating system are reinstalled.  The handler first calles the given
    `cleanup` function and then either raises KeyboardInterrupt (on SIGINT) or
    calls `sys.exit` (on SIGTERM/SIGABRT).

    Raises:
        KeyboardInterrupt: received SIGINT
        SystemExit: received SIGTERM or SIGABRT
    """

    def handler(sig, frame):
        cleanup(sig)
        if sig == signal.SIGINT:
            raise KeyboardInterrupt()
        elif sig in (signal.SIGTERM, signal.SIGABRT):
            sys.exit(128 & sig)

    for sig in intercepted_signals:
        signal.signal(sig, handler)

    try:
        yield
    finally:
        for sig in intercepted_signals:
            signal.signal(sig, signal.SIG_DFL)
