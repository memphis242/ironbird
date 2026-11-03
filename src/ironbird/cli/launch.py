"""Console-script entry point.

Importing the CLI takes a moment (the argument parser and its dependencies), and Python's own
handler for Ctrl-C would print a traceback from wherever the import was at that point. This
module imports nothing heavy: it imports the real entry point inside a guard, so an interrupt
that arrives that early ends the process quietly with the usual exit code.
"""

from __future__ import annotations

import signal
import sys


def main() -> int:
    try:
        from ironbird.cli.main import main as run  # noqa: PLC0415 - the point of this module

        return run()
    except KeyboardInterrupt:  # before main() has its own handling in place
        sys.stderr.write(f"interrupted ({signal.Signals(signal.SIGINT).name})\n")
        return 128 + int(signal.SIGINT)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
