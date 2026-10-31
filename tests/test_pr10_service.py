"""Regression tests for the PR #10 review findings on the retention service, the notices file
and the exit: nothing blocks the loop or `ib send` on the state directory, a failing scan reports
and recovers, and a hung disk bounds the exit and yields to a signal."""

from __future__ import annotations

import errno
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from conftest import REPO
from ironbird.config import RetentionConfig, load_config
from ironbird.errors import Code
from ironbird.retnotices import FILE_NAME, Notices
from retutil import make_session
from support.faultfs import FaultFs, Rule
from test_retention_service import Rig

TESTS = REPO / "tests"
SEND = ["send", "fc", "MOCK_NOOP"]


# -- B3: a scan thread that fails reports, and the next scan runs ----


def test_a_scan_that_hits_an_unexpected_error_reports_once_and_scans_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raised: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", raised.append)
    rig = Rig(tmp_path, RetentionConfig(scan_interval_s=600))
    make_session(rig.state, 1, age_days=2, klass="short")
    boom = Rule("scandir", exc=RuntimeError("bug in the scan"), times=1)
    rig.fs.rules.append(boom)
    rig.retention.start()
    rig.settle()
    (warning,) = [n for n in rig.shown if n.code is Code.RET_SCAN]
    assert "retention scan failed" in warning.summary and "RuntimeError" in warning.summary
    assert not raised  # no raw thread traceback
    rig.clock.advance(601)
    rig.poll()  # the failed scan did not leave the service waiting for a result for ever
    rig.settle()
    assert rig.fs.count("scandir") >= 2
    assert [n.code for n in rig.shown].count(Code.RET_SCAN) == 1  # coalesced by code
    rig.ledger.close()


def test_a_directory_whose_fstat_fails_gives_a_partial_scan_not_a_dead_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raised: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", raised.append)
    rig = Rig(tmp_path, RetentionConfig())
    make_session(rig.state, 1, age_days=2, klass="short")
    rig.fs.fail("fstat", errno.EIO, when=lambda fd: fd >= 0)
    rig.retention.start()
    rig.settle()
    assert any(n.code is Code.RET_SCAN for n in rig.shown)
    assert not raised
    rig.ledger.close()


# -- B1: the notices file is never touched on the calling thread ----


def hung_notices(fs: FaultFs) -> Rule:
    return fs.block("open_at", when=lambda _dir_fd, name, *_rest: name == FILE_NAME)


def test_a_short_commands_start_does_not_wait_for_the_notices_file(tmp_path: Path) -> None:
    fs = FaultFs()
    gate = hung_notices(fs)
    rig = Rig(tmp_path, RetentionConfig(), long=False, notices=Notices(tmp_path / "st", fs=fs))
    start = time.monotonic()
    rig.retention.start()
    assert time.monotonic() - start < 0.5
    assert gate.entered.wait(5)  # the scan thread is the one stuck on it
    gate.gate.set()  # type: ignore[union-attr]
    rig.settle()
    rig.ledger.close()


def test_a_long_sessions_poll_and_warnings_do_not_wait_for_the_notices_file(
    tmp_path: Path,
) -> None:
    fs = FaultFs()
    gate = hung_notices(fs)
    rig = Rig(tmp_path, RetentionConfig(max_total_bytes=1), notices=Notices(tmp_path / "st", fs=fs))
    make_session(rig.state, 1, age_days=2, size=5000, klass="short")
    rig.retention.start()
    deadline = time.monotonic() + 5
    while rig.retention._thread is not None and rig.retention._thread.is_alive():
        if time.monotonic() > deadline:
            break
        time.sleep(0.01)
    start = time.monotonic()
    rig.retention.poll()  # handles the scan: warns through the gate, which is stuck
    rig.ledger.tick()
    assert time.monotonic() - start < 0.5
    assert gate.entered.wait(5)
    gate.gate.set()  # type: ignore[union-attr]
    rig.ledger.close()
    assert Code.RET_TOTAL in [n.code for n in rig.shown]  # decided once the file answered


@pytest.mark.parametrize("err", [errno.EIO, errno.EACCES, errno.ENOTCONN, errno.EDQUOT])
def test_any_lock_refusal_still_rate_limits_through_the_notices_file(
    tmp_path: Path, err: int
) -> None:
    fs = FaultFs()
    fs.fail("flock", err)
    notices = Notices(tmp_path, fs=fs)
    assert notices.allow("IB-LED-009", tmp_path, 86400)
    assert not notices.allow("IB-LED-009", tmp_path, 86400)  # not one display per run
    assert not Notices(tmp_path, fs=fs).allow("IB-LED-009", tmp_path, 86400)  # nor per process


def test_a_check_that_changes_nothing_does_not_rewrite_the_notices_file(tmp_path: Path) -> None:
    fs = FaultFs()
    notices = Notices(tmp_path, fs=fs)
    assert notices.allow("IB-RET-001", tmp_path, 86400)
    writes = fs.count("pwrite")
    assert not notices.allow("IB-RET-001", tmp_path, 86400)
    assert fs.count("pwrite") == writes


# -- the real entry point ----


class Wire:
    def __init__(self, config: Path) -> None:
        endpoint = load_config(config).links[0].command_endpoint
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((endpoint.host, endpoint.port))

    def wait(self, timeout: float) -> bool:
        self.sock.settimeout(timeout)
        try:
            self.sock.recv(4096)
        except TimeoutError:
            return False
        return True

    def close(self) -> None:
        self.sock.close()


@pytest.fixture
def wire(mock_config_path: Path) -> Iterator[Wire]:
    w = Wire(mock_config_path)
    yield w
    w.close()


def start_ib(config: Path, setup: str, state: Path, *args: str) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-m", "support.faulty_main", setup, "--", "-c", str(config), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={"PYTHONPATH": str(TESTS), "PATH": "/usr/bin:/bin", "IRONBIRD_STATE_DIR": str(state)},
    )


def test_ib_send_goes_out_at_once_when_the_notices_file_hangs(
    mock_config_path: Path, wire: Wire, _isolated_state: Path
) -> None:
    setup = (
        "fs = FaultFs()\n"
        "fs.block('open_at', when=lambda d, name, *a: name == 'retention-notices.json')\n"
        "options = {'fs': fs, 'join_timeout_s': 3.0}\n"
    )
    started = time.monotonic()
    process = start_ib(mock_config_path, setup, _isolated_state, *SEND)
    try:
        assert wire.wait(20), "the command never reached the wire"
        sent_after = time.monotonic() - started
        _out, err = process.communicate(timeout=30)
    finally:
        if process.poll() is None:
            process.kill()
    assert process.returncode == 0, err
    assert sent_after < 15
    assert time.monotonic() - started < 25  # and the exit is bounded, not stuck on the file


def test_a_hung_disk_at_exit_bounds_the_process_and_a_signal_ends_the_wait_early(
    mock_config_path: Path, wire: Wire, _isolated_state: Path
) -> None:
    setup = (
        "fs = FaultFs()\n"
        "fs.block('fdatasync')\n"  # the exit sync never returns
        "options = {'fs': fs, 'join_timeout_s': 6.0}\n"
    )
    started = time.monotonic()
    quiet = start_ib(mock_config_path, setup, _isolated_state, *SEND)
    try:
        assert wire.wait(20)
        _out, err = quiet.communicate(timeout=40)
    finally:
        if quiet.poll() is None:
            quiet.kill()
    elapsed = time.monotonic() - started
    assert quiet.returncode == 6, err
    assert elapsed < 20, elapsed  # 6 s of waiting once, not once per step
    lines = [x for x in err.splitlines() if "IB-LED" in x]
    assert len(lines) == 1 and "not responding" in lines[0], err

    interrupted = start_ib(mock_config_path, setup, _isolated_state, *SEND)
    try:
        assert wire.wait(20)
        time.sleep(1.5)  # inside the exit wait now
        interrupted_at = time.monotonic()
        interrupted.send_signal(signal.SIGTERM)
        _out, err = interrupted.communicate(timeout=40)
    finally:
        if interrupted.poll() is None:
            interrupted.kill()
    assert interrupted.returncode == 128 + signal.SIGTERM, err
    assert time.monotonic() - interrupted_at < 5, "the signal did not end the wait"


# -- who recovers: long sessions do, a short command only looks ----


def _leftover(state: Path) -> Path:
    import json

    from retutil import make_reserve, reserve_header, sid

    make_session(state, 1, age_days=3, klass="long")
    record = json.dumps({"session": sid(1), "n": 10, "kind": "action", "event": "x"}) + "\n"
    body = (reserve_header(sid(1)) + record * 2).encode() + b"\x00" * 16 + b"JUNK"
    return make_reserve(state, 1, 1, body, age_days=3)


def test_a_short_command_leaves_a_leftover_reserve_to_a_long_session(tmp_path: Path) -> None:
    short = Rig(tmp_path / "s", RetentionConfig(), long=False)
    reserve = _leftover(short.state)
    short.retention.start()
    short.settle()
    assert reserve.exists()  # looked at, not rewritten
    short.ledger.close()

    long = Rig(tmp_path / "l", RetentionConfig(), long=True)
    reserve = _leftover(long.state)
    long.retention.start()
    assert long.retention._thread is not None
    long.retention._thread.join(10)  # the exit asks a running scan to start no new step
    long.settle()
    assert not reserve.exists() and (long.state / f"{reserve.name}.damaged").exists()
    assert any(r["event"] == "ledger.recovered" for r in _records(long))
    long.ledger.close()


def _records(rig: Rig) -> list[dict[str, object]]:
    from ledgerutil import session_records

    rig.ledger.finish()
    return session_records(rig.state, bookkeeping=True)
