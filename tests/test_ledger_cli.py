"""Stage 2d, layer 1 through the real entry point: what a user sees when the ledger cannot write.

Subprocesses run ``ib`` under :mod:`support.faulty_main`, which puts a scripted file system under
the ledger (no environment variable or file in ``src/`` can do that). A plain UDP socket plays the
mock responder and counts what reaches the wire.
"""

from __future__ import annotations

import socket
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from conftest import REPO
from ironbird.config import load_config
from ledgerutil import session_records

TESTS = REPO / "tests"
SEND = ["send", "fc", "MOCK_NOOP"]


def run_ib(
    config: Path, setup: str, *args: str, timeout: float = 60.0
) -> subprocess.CompletedProcess[str]:
    env_path = f"{TESTS}"
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "support.faulty_main",
            setup,
            "--",
            "-c",
            str(config),
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        env={"PYTHONPATH": env_path, "PATH": "/usr/bin:/bin", "IRONBIRD_STATE_DIR": str(_state)},
    )


_state = Path("/nonexistent")


@pytest.fixture(autouse=True)
def _bind_state(_isolated_state: Path) -> None:
    global _state  # noqa: PLW0603 - the helper above reads the per-test state directory
    _state = _isolated_state


class Wire:
    """A UDP socket on the mock fc's command endpoint, counting datagrams."""

    def __init__(self, config: Path) -> None:
        endpoint = load_config(config).links[0].command_endpoint
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((endpoint.host, endpoint.port))
        self.sock.settimeout(0.2)

    def count(self) -> int:
        total = 0
        try:
            while True:
                self.sock.recv(4096)
                total += 1
        except TimeoutError:
            return total

    def close(self) -> None:
        self.sock.close()


@pytest.fixture
def wire(mock_config_path: Path) -> Iterator[Wire]:
    w = Wire(mock_config_path)
    yield w
    w.close()


FULL_DISK = (
    "fs = FaultFs()\n"
    "fs.fail('write', errno.ENOSPC)\n"
    "fs.fail('pwrite', errno.ENOSPC)\n"
    "options = {'fs': fs, 'join_timeout_s': 2.0}\n"
)


def test_a_send_with_the_ledger_stalled_still_goes_out_prints_one_line_and_exits_6(
    mock_config_path: Path, wire: Wire, _isolated_state: Path
) -> None:
    done = run_ib(mock_config_path, FULL_DISK, *SEND)
    assert done.returncode == 6, done.stderr
    assert wire.count() == 1  # the transmission reached the wire
    lines = [x for x in done.stderr.splitlines() if "IB-LED" in x]
    assert len(lines) == 1, done.stderr
    assert lines[0].startswith(
        "warning IB-LED-004: MOCK_NOOP sent to fc but not logged "
        "(ledger not writable: No space left on device); free space in "
    )
    assert "IB-LED-001" not in done.stderr  # the stall folds into the one line
    assert done.stdout.startswith("sent MOCK_NOOP")


def test_a_send_whose_ledger_recovers_before_exit_writes_late_and_exits_0(
    mock_config_path: Path, wire: Wire, _isolated_state: Path
) -> None:
    setup = (
        "fs = FaultFs()\n"
        "fs.fail('write', errno.EIO, times=2)\n"  # the open's first record, then one retry
        "clock = FakeClock()\n"
        "options = {'fs': fs, 'clock': clock, 'join_timeout_s': 2.0}\n"
    )
    # With a clock that never reaches 5 s the retry is not due; the exit's last attempt is.
    done = run_ib(mock_config_path, setup, *SEND)
    assert wire.count() == 1
    assert done.returncode == 0, done.stderr
    assert "IB-LED" not in done.stderr  # written by the last attempt: nothing to act on
    rows = session_records(_isolated_state, bookkeeping=True)
    events = [r["event"] for r in rows if r["event"] in ("tx.command", "ledger.resumed")]
    assert events == ["tx.command", "ledger.resumed"], events  # written late, and counted as such


def test_refuse_commands_sends_nothing_and_exits_1_with_one_line(
    mock_config_path: Path, wire: Wire
) -> None:
    config = mock_config_path.read_text() + '\n[ledger]\non_full = "refuse_commands"\n'
    mock_config_path.write_text(config)
    done = run_ib(mock_config_path, FULL_DISK, *SEND)
    assert done.returncode == 1, done.stderr
    assert wire.count() == 0
    lines = [x for x in done.stderr.splitlines() if "IB-LED" in x]
    assert len(lines) == 1 and lines[0].startswith(
        "error IB-LED-004: MOCK_NOOP to fc refused: ledger not writable "
        "(No space left on device); free space in "
    )


def test_a_configuration_failure_refuses_the_send_and_nothing_goes_out(
    mock_config_path: Path, wire: Wire, tmp_path: Path
) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("x")
    done = subprocess.run(
        [sys.executable, "-m", "ironbird.cli.main", "-c", str(mock_config_path), *SEND],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
        env={"IRONBIRD_STATE_DIR": str(blocker / "sub"), "PATH": "/usr/bin:/bin"},
    )
    assert done.returncode == 1 and "IB-LED-002" in done.stderr
    assert "nothing was sent" in done.stderr and wire.count() == 0


def test_a_transient_open_failure_no_longer_refuses_a_send(
    mock_config_path: Path, wire: Wire
) -> None:
    setup = (
        "fs = FaultFs()\n"
        "fs.fail('open_at', errno.ENOSPC, when=lambda d, name, *a: name.endswith('.jsonl'))\n"
        "options = {'fs': fs, 'join_timeout_s': 2.0}\n"
    )
    done = run_ib(mock_config_path, setup, *SEND)
    assert wire.count() == 1 and done.returncode == 6, done.stderr
    assert "but not logged" in done.stderr


def test_a_stalled_listen_says_so_once_and_again_at_exit(mock_config_path: Path) -> None:
    done = run_ib(mock_config_path, FULL_DISK, "listen", "fc", "--timeout", "1.2")
    assert done.returncode == 0, done.stderr
    first = [x for x in done.stderr.splitlines() if x.startswith("warning IB-LED-001")]
    assert len(first) == 2, done.stderr  # one on entering the stall, one at exit
    assert (
        "ledger not writable (No space left on device): records queued, retrying every 5 s"
        in first[0]
    )
    assert "ledger stalled at exit;" in first[1]


def test_a_signal_while_a_transmission_is_unwritten_prints_the_summary_and_exits_128_plus_n(
    mock_config_path: Path, wire: Wire
) -> None:
    setup = (
        "import os, signal\n"
        "from ironbird.cli import main as m\n"
        "orig = m._SignalState.detach\n"
        "def detach(self):\n"
        "    orig(self)\n"
        "    os.kill(os.getpid(), signal.SIGTERM)\n"  # arrives with the exit still to be made
        "m._SignalState.detach = detach\n"
        "fs = FaultFs()\n"
        "fs.fail('write', errno.ENOSPC)\n"
        "options = {'fs': fs, 'join_timeout_s': 2.0}\n"
    )
    done = run_ib(mock_config_path, setup, *SEND)
    assert done.returncode == 143, done.stderr  # the signal takes precedence over exit 6
    assert wire.count() == 1
    assert "IB-LED-004" in done.stderr and "but not logged" in done.stderr
    assert "interrupted (SIGTERM)" in done.stderr
