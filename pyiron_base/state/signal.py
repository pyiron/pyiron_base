import contextlib
import signal
import sys

intercepted_signals = [
    signal.SIGINT,
    signal.SIGTERM,
    signal.SIGABRT,
]  # , signal.SIGQUIT]

@contextlib.contextmanager
def catch_signals(cleanup):
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
