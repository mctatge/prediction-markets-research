"""POSIX advisory locks with a readable error on unsupported platforms."""

try:
    import fcntl
except ImportError:
    class _UnsupportedLocks:
        LOCK_EX = 2
        LOCK_SH = 1
        LOCK_NB = 4
        LOCK_UN = 8

        def __getattr__(self, name):
            raise RuntimeError("Native Windows is unsupported; use Linux, macOS, or WSL.")

        def flock(self, *args):
            raise RuntimeError("Native Windows is unsupported; use Linux, macOS, or WSL.")

    fcntl = _UnsupportedLocks()
