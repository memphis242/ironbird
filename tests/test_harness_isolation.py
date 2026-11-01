"""The test harness itself: what one test leaves behind in a worker never reaches the next.

xdist decides which tests share a worker process and in what order, so a test whose outcome
depends on module state an earlier test left fails only on some runs. These tests run a small
inner session in a fresh interpreter, with this suite's conftest, in a fixed order.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

CONFTEST = Path(__file__).resolve().parent / "conftest.py"


def test_a_command_run_in_process_does_not_change_how_the_next_test_sees_a_signal(
    tmp_path: Path,
) -> None:
    """An in-process command ends by detaching the CLI's signal state, after which its handler
    only records a signal, and a test can leave a signal owed. The next test in the same worker
    still gets the handler of a fresh process, which delivers a signal outside a command, and
    owes nothing."""
    inner = tmp_path / "inner"
    inner.mkdir()
    (inner / "pytest.ini").write_text("[pytest]\n")
    (inner / "conftest.py").write_text(
        textwrap.dedent(
            f"""
            import importlib.util

            _spec = importlib.util.spec_from_file_location("ib_conftest", {str(CONFTEST)!r})
            assert _spec is not None and _spec.loader is not None
            _suite = importlib.util.module_from_spec(_spec)
            _spec.loader.exec_module(_suite)
            _isolated_state = _suite._isolated_state
            _fresh_cli_signal_state = _suite._fresh_cli_signal_state
            mock_config_path = _suite.mock_config_path
            """
        )
    )
    (inner / "test_order.py").write_text(
        textwrap.dedent(
            """
            import signal
            import threading

            import pytest
            from typer.testing import CliRunner

            from ironbird import sigmask
            from ironbird.cli import main as cli_main


            def test_1_an_in_process_command(mock_config_path):
                result = CliRunner().invoke(cli_main.app, ["-c", str(mock_config_path), "nodes"])
                assert result.exit_code == 0, result.output


            def test_2_the_handler_delivers_outside_a_command():
                previous = signal.signal(signal.SIGTERM, cli_main._STATE._handle)
                try:
                    signal.pthread_kill(threading.get_ident(), signal.SIGTERM)
                    with pytest.raises(sigmask.Interrupted):
                        sigmask.checkpoint()
                finally:
                    signal.signal(signal.SIGTERM, previous)


            def test_3_a_test_that_leaves_a_signal_owed():
                sigmask.deliver(signal.SIGTERM)
                assert sigmask.owed() == signal.SIGTERM


            def test_4_owes_nothing():
                assert sigmask.owed() == 0
                sigmask.checkpoint()
            """
        )
    )
    # The inner session runs with its own options only: PYTEST_ADDOPTS of the outer run (say
    # ``-n auto`` or a ``-k``) would change what runs and in which order.
    environment = {key: value for key, value in os.environ.items() if key != "PYTEST_ADDOPTS"}
    done = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "-p",
            "no:randomly",
            "-p",
            "no:xdist",
            "--rootdir",
            str(inner),
            "-c",
            str(inner / "pytest.ini"),
            str(inner / "test_order.py"),
        ],
        cwd=inner,
        env=environment,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    assert "4 passed" in done.stdout, done.stdout
