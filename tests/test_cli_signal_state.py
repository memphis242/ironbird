"""The CLI's signal handler: it never raises and starts nothing; it delivers a signal to the
command outside an event loop, cancels the main task while the loop runs it, and only records
one once the command is over. Only the first signal acts; every one is counted."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from ironbird import sigmask
from ironbird.cli import main as cli_main
from ironbird.sigmask import HANDLED_SIGNALS, Interrupted
from recutil import CLI_MAIN

pytestmark = pytest.mark.skipif(
    threading.current_thread() is not threading.main_thread(),
    reason="signal handlers can only be set in the main thread",
)


@pytest.fixture
def handlers() -> Iterator[None]:
    """The handlers as they were before the test, put back after it."""
    previous = {sig: signal.getsignal(sig) for sig in HANDLED_SIGNALS}
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def _send(sig: int) -> None:
    signal.pthread_kill(threading.get_ident(), sig)


def test_install_starts_no_thread_and_leaves_the_unraisable_hook_alone(handlers: None) -> None:
    """Nothing to start means nothing that can fail to start (a process or thread limit)."""
    state = cli_main._STATE
    threads, hook = threading.active_count(), sys.unraisablehook
    state.install()
    try:
        assert threading.active_count() == threads and sys.unraisablehook is hook
        assert all(signal.getsignal(sig) == state._handle for sig in HANDLED_SIGNALS)
    finally:
        state.restore_default()
    assert all(signal.getsignal(sig) == signal.SIG_DFL for sig in HANDLED_SIGNALS)


def test_outside_a_loop_the_first_signal_is_delivered_and_repeats_are_counted(
    handlers: None,
) -> None:
    state = cli_main._STATE
    state.install()
    _send(signal.SIGTERM)  # the handler returns: nothing raised here
    _send(signal.SIGINT)
    assert state.signum == signal.SIGTERM and state.received == 2
    with pytest.raises(Interrupted) as caught:
        sigmask.checkpoint()
    assert caught.value.signum == signal.SIGTERM
    state.detach()


def test_after_detach_a_signal_is_only_recorded_for_the_exit_to_announce(handlers: None) -> None:
    state = cli_main._STATE
    state.install()
    _send(signal.SIGHUP)
    state.detach()  # the command ended before it acted on the signal
    assert sigmask.owed() == 0 and state.unannounced() == signal.SIGHUP
    sigmask.checkpoint()
    _send(signal.SIGTERM)
    assert state.signum == signal.SIGHUP and state.received == 2 and sigmask.owed() == 0


def test_while_the_loop_runs_the_command_a_signal_cancels_it_and_raises_nothing(
    handlers: None,
) -> None:
    state = cli_main._STATE
    state.install()

    async def command() -> str:
        task = asyncio.current_task()
        assert task is not None
        state.attach(task, asyncio.get_running_loop())
        _send(signal.SIGTERM)
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            return "cancelled"
        return "not cancelled"

    assert asyncio.run(command()) == "cancelled"
    assert sigmask.owed() == 0 and state.signum == signal.SIGTERM
    _send(signal.SIGINT)  # the task is done: recorded, not delivered
    assert sigmask.owed() == 0 and state.received == 2
    state.detach()


# Child-side hooks: the configuration is read from a FIFO that nobody writes; the mark says the
# read is about to begin.
_FIFO_HOOKS = """
import os
from pathlib import Path
import ironbird.config as config
MARK, _read = Path(os.environ["IB_TEST_MARK"]), config.read_capped

def read_capped(path, limit):
    MARK.touch()
    return _read(path, limit)

config.read_capped = read_capped
"""


def test_a_signal_ends_a_command_whose_configuration_is_a_fifo_nobody_writes(
    tmp_path: Path,
) -> None:
    """A FIFO has no data until a writer comes: the command waits for one, and a signal ends
    that wait."""
    fifo, mark = tmp_path / "ironbird.toml", tmp_path / "reading"
    os.mkfifo(fifo)
    env = {
        "PATH": "/usr/bin:/bin",
        "IRONBIRD_STATE_DIR": str(tmp_path / "state"),
        "IB_TEST_MARK": str(mark),
    }
    proc = subprocess.Popen(
        [sys.executable, "-c", _FIFO_HOOKS + CLI_MAIN, "-c", str(fifo), "nodes"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 60
        while not mark.exists():
            assert proc.poll() is None, proc.communicate()[1]
            assert time.monotonic() < deadline
            time.sleep(0.005)
        proc.send_signal(signal.SIGTERM)
        _, err = proc.communicate(timeout=60)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert proc.returncode == 128 + signal.SIGTERM, err
    assert "Traceback" not in err, err
