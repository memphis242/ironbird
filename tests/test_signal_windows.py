"""Signals in the corners of the cooperative cancellation: a FIFO configuration, the exit code
of a usage error, the ledger's open wait, a prune recovery, the loop's boundaries, the last
window of the exit, and a signal that ends the command before any ledger exists. Each runs the
real CLI in a subprocess, since the process's own signal disposition is what is under test."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from ironbird import sigmask
from ironbird.config import load_config
from ironbird.ledger import Ledger
from ironbird.session import Session
from ledgerutil import session_records
from recutil import CLI_MAIN, frame, record, write_config
from retutil import make_reserve, make_session, reserve_header, sid


def _records(state: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for path in sorted(state.glob("*.jsonl"))
        for line in path.read_text().splitlines()
    ]


def _signals(state: Path) -> list[dict[str, object]]:
    return [r for r in _records(state) if r.get("event") == "cli.signal"]


# -- the configuration is read once ------------------------------------------------------


def test_sigterm_ends_record_with_a_fifo_config(tmp_path: Path) -> None:
    """The configuration is read once, by ``load_config``; nothing reopens it inside the loop,
    where a FIFO without a writer would hold the command past a signal."""
    real, _, _ = write_config(tmp_path)
    text = real.read_bytes()
    fifo = tmp_path / "fifo.toml"
    os.mkfifo(fifo)

    def feed() -> None:  # one writer: the configuration is delivered exactly once
        with open(fifo, "wb") as w:
            w.write(text)

    threading.Thread(target=feed, daemon=True).start()
    env = {
        "PATH": "/usr/bin:/bin",
        "IRONBIRD_STATE_DIR": str(tmp_path / "state"),
        "IRONBIRD_RECORD_DIR": str(tmp_path / "rec"),
    }
    err = tmp_path / "err"
    with err.open("wb") as e:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                CLI_MAIN,
                "-c",
                str(fifo),
                "record",
                "-o",
                str(tmp_path / "o.ibrec"),
                "--duration",
                "30",
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=e,
        )
        try:
            time.sleep(3.0)
            assert proc.poll() is None, err.read_text()
            proc.send_signal(signal.SIGTERM)
            code = proc.wait(timeout=10)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
    assert code == 143, err.read_text()


def test_config_digest_is_of_the_bytes_that_were_parsed(tmp_path: Path) -> None:
    path, _, _ = write_config(tmp_path)
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    config = load_config(path)
    path.write_bytes(b"changed afterwards")
    assert config.sha256 == expected
    with pytest.raises(AttributeError):
        config.sha256 = "x"  # type: ignore[misc]


def test_definition_bytes_are_the_ones_the_codec_was_built_from(tmp_path: Path) -> None:
    """What a recording embeds and digests is held from the load, never read again."""
    path, _, _ = write_config(tmp_path)
    config = load_config(path)
    node = config.nodes["fc"]
    assert node.ccdd is not None
    expected = node.ccdd.read_bytes()
    session = Session(
        config, ledger=Ledger(tmp_path / "ledger", session_id="2c", enabled=False), notify=print
    )
    held = session.definitions_of(node.name)
    assert held.ccdd == expected
    assert session.definitions_of(node.name) is held  # loaded once


# -- the exit code of a usage error ----------------------------------------------------------


def test_signal_while_leaving_by_a_usage_error_exits_with_the_signal(tmp_path: Path) -> None:
    hook = """
import os, signal
import ironbird.cli.main as m
from ironbird.errors import Code, IronbirdError
def _usage_error(run, role, precheck):
    os.kill(os.getpid(), signal.SIGTERM)  # the handler records it; nothing raises
    return IronbirdError(Code.CLI_USAGE, "bad argument")
m._usage_error = _usage_error
"""
    cfg, _, _ = write_config(tmp_path)
    env = {"PATH": "/usr/bin:/bin", "IRONBIRD_STATE_DIR": str(tmp_path / "state")}
    done = subprocess.run(
        [sys.executable, "-c", hook + CLI_MAIN, "-c", str(cfg), "nodes"],
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert "interrupted (SIGTERM)" in done.stderr
    assert done.returncode == 143, done.stderr


# -- the ledger's waits ---------------------------------------------------------------------


def test_sigterm_during_a_hung_ledger_open_is_not_held_for_the_open_wait(tmp_path: Path) -> None:
    """The open wait ends at a slice once a signal is owed. The exit's own budget is lowered to
    keep the bound small; without the sliced wait the command would sit out the whole hang."""
    hook = """
import os, time
from pathlib import Path
import ironbird.cli.main as m
import ironbird.ledgerstore as ls
m.LEDGER_OPTIONS["join_timeout_s"] = 0.3
MARK = Path(os.environ["IB_MARK"])
_orig = ls.LedgerStore._open_steps
def _open_steps(self):
    MARK.touch()
    time.sleep(4.5)   # a hung state directory
    return _orig(self)
ls.LedgerStore._open_steps = _open_steps
"""
    rec = tmp_path / "x.ibrec"
    record(rec, [frame(i) for i in range(1, 4)])
    mark = tmp_path / "mark"
    env = {
        "PATH": "/usr/bin:/bin",
        "IRONBIRD_STATE_DIR": str(tmp_path / "state"),
        "IB_MARK": str(mark),
    }
    proc = subprocess.Popen(
        [sys.executable, "-c", hook + CLI_MAIN, "rec", "info", str(rec)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 30
    while not mark.exists():
        assert time.monotonic() < deadline and proc.poll() is None
        time.sleep(0.01)
    time.sleep(0.1)
    began = time.monotonic()
    proc.send_signal(signal.SIGTERM)
    _, err = proc.communicate(timeout=60)
    took = time.monotonic() - began
    assert proc.returncode == 143, err
    assert took < 2.5, f"SIGTERM took {took:.2f} s to end the command"


def test_wait_event_ends_at_a_slice_when_a_signal_is_owed() -> None:
    event = threading.Event()
    assert sigmask.wait_event(event, 0.05) is False  # nothing owed: the timeout
    sigmask.deliver(signal.SIGTERM)
    try:
        began = time.monotonic()
        assert sigmask.wait_event(event, 30.0) is False
        assert time.monotonic() - began < 1.0
        with pytest.raises(sigmask.Interrupted), sigmask.signals_held():
            began = time.monotonic()  # a held scope defers the signal, so the wait runs on
            assert sigmask.wait_event(event, 0.2) is False
            assert time.monotonic() - began >= 0.2
    finally:
        sigmask.discard()
    event.set()
    assert sigmask.wait_event(event, 30.0) is True


# -- the prune recovery -------------------------------------------------------------------


def test_sigterm_during_a_prune_recovery_copy_ends_it_within_a_block(tmp_path: Path) -> None:
    hook = """
import os, time
from pathlib import Path
import ironbird.fsops as fsops
MARK = Path(os.environ["IB_MARK"])
_orig = fsops.FileSystem.pread
def pread(self, fd, n, offset):
    if n >= 1 << 20:              # the reserve's 1 MiB blocks: a slow file server
        MARK.touch()
        time.sleep(0.25)
    return _orig(self, fd, n, offset)
fsops.FileSystem.pread = pread
"""
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    now = time.time()
    make_session(state, 1, age_days=3, now=now)
    line = json.dumps({"session": sid(1), "n": 10, "kind": "action", "event": "x" * 200}) + "\n"
    body = (reserve_header(sid(1)) + line * 60000).encode()  # about 13 MiB of records
    make_reserve(state, 1, 1, body, now=now, pad_to=len(body) + (8 << 20))
    mark = tmp_path / "mark"
    env = {"PATH": "/usr/bin:/bin", "IRONBIRD_STATE_DIR": str(state), "IB_MARK": str(mark)}
    proc = subprocess.Popen(
        [sys.executable, "-c", hook + CLI_MAIN, "ledger", "prune", "--dry-run"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 30
    while not mark.exists():
        assert time.monotonic() < deadline and proc.poll() is None, proc.communicate()
        time.sleep(0.01)
    began = time.monotonic()
    proc.send_signal(signal.SIGTERM)
    _, err = proc.communicate(timeout=120)
    took = time.monotonic() - began
    assert proc.returncode == 143, err
    assert took < 1.5, f"SIGTERM took {took:.2f} s"
    leftovers = [
        p.name for p in state.iterdir() if p.name.endswith(".tmp") or p.name.startswith(".")
    ]
    assert not leftovers, leftovers  # the interrupted copy left no temporary behind


def test_a_signal_mid_prune_says_how_many_sessions_were_deleted(tmp_path: Path) -> None:
    hook = """
import os, signal
import ironbird.fsops as fsops
_orig = fsops.FileSystem.unlink_at
_calls = []
def unlink_at(self, dir_fd, name):
    _orig(self, dir_fd, name)
    _calls.append(name)
    if len(_calls) == 1:
        os.kill(os.getpid(), signal.SIGTERM)  # after the first file of the first session
fsops.FileSystem.unlink_at = unlink_at
"""
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    for n in range(1, 5):
        make_session(state, n, age_days=30 + n, klass="short")
    env = {"PATH": "/usr/bin:/bin", "IRONBIRD_STATE_DIR": str(state)}
    done = subprocess.run(
        [
            sys.executable,
            "-c",
            hook + CLI_MAIN,
            "ledger",
            "prune",
            "--keep",
            "0",
            "--yes",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert done.returncode == 143, (done.returncode, done.stderr)
    assert "deleted 1 of 4 ledger sessions" in done.stderr, done.stderr
    assert "interrupted (SIGTERM)" in done.stderr
    interrupted = [e for e in session_records(state) if e["event"] == "prune.interrupted"]
    assert len(interrupted) == 1 and interrupted[0]["deleted_sessions"] == 1


def test_a_signal_while_a_reused_temporary_is_judged_leaks_no_descriptor(tmp_path: Path) -> None:
    from ironbird.atomicfile import TempOutput

    old = tmp_path / ".out.0123456789abcdef.tmp"
    old.write_bytes(b"partial")
    old.chmod(0o600)

    def judge(_fd: int) -> bool:
        raise sigmask.Interrupted(signal.SIGTERM)

    before = len(os.listdir("/proc/self/fd"))
    with (
        pytest.raises(sigmask.Interrupted),
        TempOutput(tmp_path / "out", reuse=[old.name], accept=judge),
    ):
        pass
    assert len(os.listdir("/proc/self/fd")) == before
    assert old.read_bytes() == b"partial"  # an earlier attempt's file is kept


# -- the loop's boundaries and the last window of the exit ---------------------------------

_BEFORE = """
import os, signal
import ironbird.cli.main as m
_orig = m._check_descriptors
def _check_descriptors():
    os.kill(os.getpid(), signal.SIGTERM)   # owed before the loop
    _orig()
m._check_descriptors = _check_descriptors
"""
_AFTER = """
import os, signal
import ironbird.cli.main as m
_orig = m._main_body
async def _main_body(*a, **k):
    code = await _orig(*a, **k)
    os.kill(os.getpid(), signal.SIGTERM)   # the body is over, the main task is still running
    return code
m._main_body = _main_body
"""
_TEARDOWN = """
import os, signal, asyncio
import ironbird.cli.main as m
_orig_run = asyncio.run
def run(coro, **k):
    try:
        return _orig_run(coro, **k)
    finally:
        os.kill(os.getpid(), signal.SIGTERM)  # the loop is closed, before detach
m.asyncio.run = run
"""


@pytest.mark.parametrize("hook", [_BEFORE, _AFTER, _TEARDOWN], ids=["before", "after", "teardown"])
def test_signal_at_the_loop_boundaries(tmp_path: Path, hook: str) -> None:
    cfg, _, _ = write_config(tmp_path)
    state = tmp_path / "state"
    env = {"PATH": "/usr/bin:/bin", "IRONBIRD_STATE_DIR": str(state)}
    done = subprocess.run(
        [sys.executable, "-c", hook + CLI_MAIN, "-c", str(cfg), "nodes"],
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert done.returncode == 143, (done.returncode, done.stderr)
    assert "Traceback" not in done.stderr
    assert len(_signals(state)) == 1, _signals(state)


def test_signal_in_the_last_window_of_the_exit_takes_the_default_action(tmp_path: Path) -> None:
    """From the last check of the exit to the return of the default actions the signals are
    blocked: one that arrives there is not lost, it ends the process by the signal."""
    hook = """
import os, signal
import ironbird.cli.main as m
_orig = m._SignalState.restore_default
def restore_default(self):
    if self._installed:
        os.kill(os.getpid(), signal.SIGTERM)
    _orig(self)
m._SignalState.restore_default = restore_default
"""
    rec = tmp_path / "x.ibrec"
    record(rec, [frame(i) for i in range(1, 4)])
    env = {"PATH": "/usr/bin:/bin", "IRONBIRD_STATE_DIR": str(tmp_path / "state")}
    done = subprocess.run(
        [sys.executable, "-c", hook + CLI_MAIN, "rec", "check", str(rec)],
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert done.returncode == -signal.SIGTERM, (done.returncode, done.stderr)


def test_signal_while_the_ledger_closes_is_announced_once(tmp_path: Path) -> None:
    """A signal that arrives before the last check of the exit is announced and ledgered."""
    hook = """
import os, signal
import ironbird.cli.main as m
import ironbird.ledger as ledger
_orig = ledger.Ledger.close
def close(self):
    result = _orig(self)
    if not getattr(self, "_sent", False):
        self._sent = True
        os.kill(os.getpid(), signal.SIGTERM)
    return result
ledger.Ledger.close = close
"""
    rec = tmp_path / "x.ibrec"
    record(rec, [frame(i) for i in range(1, 4)])
    state = tmp_path / "state"
    env = {"PATH": "/usr/bin:/bin", "IRONBIRD_STATE_DIR": str(state)}
    done = subprocess.run(
        [sys.executable, "-c", hook + CLI_MAIN, "rec", "check", str(rec)],
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert done.returncode == 143, (done.returncode, done.stderr)
    assert "interrupted" in done.stderr
    assert len(_signals(tmp_path / "state")) == 1


# -- a signal before any ledger exists ----------------------------------------------------


def test_signal_during_a_fifo_config_read_prints_the_notice(tmp_path: Path) -> None:
    """The signal ends ``_run`` before its ledger exists: ``main`` is the one to announce it."""
    fifo = tmp_path / "fifo.toml"
    os.mkfifo(fifo)  # nobody writes: the read waits for a writer
    rec = tmp_path / "x.ibrec"
    record(rec, [frame(i) for i in range(1, 4)])
    env = {"PATH": "/usr/bin:/bin", "IRONBIRD_STATE_DIR": str(tmp_path / "state")}
    proc = subprocess.Popen(
        [sys.executable, "-c", CLI_MAIN, "-c", str(fifo), "rec", "info", str(rec)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(2.0)
    assert proc.poll() is None, proc.communicate()
    proc.send_signal(signal.SIGTERM)
    out, err = proc.communicate(timeout=30)
    assert proc.returncode == 143, (out, err)
    assert err.count("interrupted (SIGTERM)") == 1, err
    assert "Traceback" not in err
