"""Run the real ``ib`` entry point with a scripted file system under the ledger.

``python -m support.faulty_main SCRIPT -- ARGS...`` evaluates the Python expression ``SCRIPT``
(a ``FaultFs`` configuration, trusted test input) and installs the result in
:data:`ironbird.cli.main.LEDGER_OPTIONS` before calling :func:`ironbird.cli.main.main`.
"""

from __future__ import annotations

import sys

from support.faultfs import FakeClock, FaultFs


def run(setup: str, argv: list[str]) -> int:
    from ironbird.cli import main as cli

    scope: dict[str, object] = {
        "FaultFs": FaultFs,
        "FakeClock": FakeClock,
        "errno": __import__("errno"),
    }
    exec(setup, scope)  # noqa: S102 - test-only wrapper; the script is the test's own
    options = scope.get("options")
    if isinstance(options, dict):
        cli.LEDGER_OPTIONS.update(options)
    sys.argv = ["ib", *argv]
    return cli.main()


if __name__ == "__main__":
    separator = sys.argv.index("--")
    sys.exit(run(sys.argv[1], sys.argv[separator + 1 :]))
