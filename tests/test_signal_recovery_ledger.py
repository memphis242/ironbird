"""A signal that ends a prune's ledger recovery leaves a record of every file the recovery
changed: each step is ledgered as it ends, the rename of a reserve and its record are one held
step, and a salvage copy a signal cuts is recorded as incomplete. Each runs the real CLI in a
subprocess, since the process's own signal disposition is what is under test."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from ledgerutil import session_records
from recutil import CLI_MAIN, frame, record
from retutil import make_reserve, make_session, reserve_header, sid


def _env(state: Path, **extra: str) -> dict[str, str]:
    return {"PATH": "/usr/bin:/bin", "IRONBIRD_STATE_DIR": str(state), **extra}


def _state(tmp_path: Path) -> Path:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    return state


def _events(state: Path, event: str) -> list[dict[str, object]]:
    return [r for r in session_records(state, bookkeeping=True) if r["event"] == event]


def _small_reserve_line(n: int) -> bytes:
    record = {"session": sid(n), "n": 10, "kind": "action", "event": "a"}
    return (reserve_header(sid(n)) + json.dumps(record) + "\n").encode()


HOOK_SLOW_BLOCKS = """
import os, time
from pathlib import Path
import ironbird.fsops as fsops
MARK = Path(os.environ["IB_MARK"])
_orig = fsops.FileSystem.pread
def pread(self, fd, n, offset):
    if n >= 1 << 20:              # only the large reserve of session 2 reads full blocks
        MARK.touch()
        time.sleep(0.25)
    return _orig(self, fd, n, offset)
fsops.FileSystem.pread = pread
"""


def test_the_recovery_of_an_earlier_session_is_ledgered_when_a_signal_ends_the_scan(
    tmp_path: Path,
) -> None:
    state = _state(tmp_path)
    now = time.time()
    make_session(state, 1, age_days=3, now=now)
    make_session(state, 2, age_days=3, now=now)
    first = make_reserve(state, 1, 1, _small_reserve_line(1), now=now)
    line = json.dumps({"session": sid(2), "n": 10, "kind": "action", "event": "x" * 200}) + "\n"
    big = (reserve_header(sid(2)) + line * 30000).encode()
    make_reserve(state, 2, 1, big, now=now, pad_to=len(big) + 4096)
    mark = tmp_path / "mark"
    proc = subprocess.Popen(
        [sys.executable, "-c", HOOK_SLOW_BLOCKS + CLI_MAIN, "ledger", "prune", "--dry-run"],
        env=_env(state, IB_MARK=str(mark)),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 30
    while not mark.exists():
        assert time.monotonic() < deadline and proc.poll() is None, proc.communicate()
        time.sleep(0.01)
    proc.send_signal(signal.SIGTERM)
    _, err = proc.communicate(timeout=60)
    assert proc.returncode == 143, err
    assert not first.exists(), "session 1's reserve was recovered (renamed to a part)"
    recovered = _events(state, "ledger.recovered")
    assert [r["old"] for r in recovered] == [first.name]


HOOK_SIGNAL_AFTER_RENAME = """
import os, signal
import ironbird.fsops as fsops
_ren = fsops.FileSystem.renameat2_noreplace
def renameat2_noreplace(self, dir_fd, old, new):
    r = _ren(self, dir_fd, old, new)
    if old.endswith(".reserve"):
        os.kill(os.getpid(), signal.SIGTERM)   # lands inside the rename's held scope
    return r
fsops.FileSystem.renameat2_noreplace = renameat2_noreplace
"""


def test_a_signal_during_the_recovery_rename_still_ledgers_it(tmp_path: Path) -> None:
    state = _state(tmp_path)
    now = time.time()
    make_session(state, 1, age_days=3, now=now)
    first = make_reserve(state, 1, 1, _small_reserve_line(1), now=now)
    done = subprocess.run(
        [sys.executable, "-c", HOOK_SIGNAL_AFTER_RENAME + CLI_MAIN, "ledger", "prune", "--dry-run"],
        env=_env(state),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert done.returncode == 143, done.stderr
    assert not first.exists(), "renamed to a part"
    assert [r["old"] for r in _events(state, "ledger.recovered")] == [first.name], done.stderr


def test_a_signal_during_the_salvage_set_aside_ledgers_it_as_incomplete(tmp_path: Path) -> None:
    state = _state(tmp_path)
    now = time.time()
    make_session(state, 1, age_days=3, now=now)
    body = _small_reserve_line(1) + b"\x00" * 16 + b"junk after NUL\n"
    first = make_reserve(state, 1, 1, body, now=now)
    done = subprocess.run(
        [sys.executable, "-c", HOOK_SIGNAL_AFTER_RENAME + CLI_MAIN, "ledger", "prune", "--dry-run"],
        env=_env(state),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert done.returncode == 143, done.stderr
    assert (state / (first.name + ".damaged")).exists()
    incomplete = _events(state, "ledger.recovery_incomplete")
    assert [r["damaged"] for r in incomplete] == [first.name + ".damaged"], done.stderr
    assert incomplete[0]["interrupted"] is True


HOOK_SIGNAL_IN_COPY = """
import os, signal
import ironbird.fsops as fsops
_w = fsops.FileSystem.write
_n = [0]
def write(self, fd, data):
    if not _n[0] and len(data) >= 1 << 20:
        _n[0] = 1
        os.kill(os.getpid(), signal.SIGTERM)   # the first block of the salvage copy
    return _w(self, fd, data)
fsops.FileSystem.write = write
"""


def test_an_interrupted_salvage_copy_is_ledgered(tmp_path: Path) -> None:
    state = _state(tmp_path)
    now = time.time()
    make_session(state, 1, age_days=3, now=now)
    line = json.dumps({"session": sid(1), "n": 10, "kind": "action", "event": "x" * 200}) + "\n"
    body = (reserve_header(sid(1)) + line * 20000).encode() + b"\x00" * 16 + b"junk after NUL\n"
    first = make_reserve(state, 1, 1, body, now=now, pad_to=len(body) + 4096)
    done = subprocess.run(
        [sys.executable, "-c", HOOK_SIGNAL_IN_COPY + CLI_MAIN, "ledger", "prune", "--dry-run"],
        env=_env(state),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    files = sorted(p.name for p in state.iterdir())
    assert done.returncode == 143, done.stderr
    assert (state / (first.name + ".damaged")).exists(), files  # the set-aside happened
    assert not [f for f in files if f.endswith(".tmp")], files  # the copy left no temporary
    incomplete = _events(state, "ledger.recovery_incomplete")
    assert [r["damaged"] for r in incomplete] == [first.name + ".damaged"], done.stderr
    assert incomplete[0]["interrupted"] is True and incomplete[0]["new"] is None


HOOK_SKIP_THEN_SIGNAL = """
import errno, os, signal
import ironbird.fsops as fsops
_orig = fsops.FileSystem.unlink_at
_calls = []
def unlink_at(self, dir_fd, name):
    _calls.append(name)
    if len(_calls) == 1:
        raise OSError(errno.EACCES, "denied")      # the first session is skipped
    _orig(self, dir_fd, name)
    os.kill(os.getpid(), signal.SIGTERM)           # after the first file of the second
fsops.FileSystem.unlink_at = unlink_at
"""


def test_an_interrupted_prune_still_shows_the_sessions_it_skipped(tmp_path: Path) -> None:
    state = _state(tmp_path)
    for n in range(1, 5):
        make_session(state, n, age_days=30 + n, klass="short")
    done = subprocess.run(
        [
            sys.executable,
            "-c",
            HOOK_SKIP_THEN_SIGNAL + CLI_MAIN,
            *("ledger", "prune", "--keep", "0", "--yes"),
        ],
        env=_env(state),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert done.returncode == 143, (done.returncode, done.stderr)
    assert "before the interruption" in done.stderr, done.stderr
    assert "IB-RET-" in done.stderr and "not deleted" in done.stderr, done.stderr
    assert [r["reason"] for r in session_records(state) if r["event"] == "prune.skip"]


def test_the_pre_ledger_interrupted_line_cannot_block_the_exit_on_a_full_stderr(
    tmp_path: Path,
) -> None:
    """SIGTERM ends the read of a FIFO configuration before any ledger exists; the notice goes
    to a stderr pipe nobody reads (full) without making the process unkillable."""
    fifo = tmp_path / "fifo.toml"
    os.mkfifo(fifo)  # nobody writes: the read waits for a writer
    rec = tmp_path / "x.ibrec"
    record(rec, [frame(i) for i in range(1, 4)])
    read_end, write_end = os.pipe()
    os.set_blocking(write_end, False)
    try:
        while True:  # fill the pipe: the next write to it blocks
            os.write(write_end, b"x" * 4096)
    except BlockingIOError:
        pass
    os.set_blocking(write_end, True)
    proc = subprocess.Popen(
        [sys.executable, "-c", CLI_MAIN, "-c", str(fifo), "rec", "info", str(rec)],
        env=_env(tmp_path / "state"),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=write_end,
    )
    try:
        time.sleep(2.0)
        assert proc.poll() is None
        began = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=20) == 143
        assert time.monotonic() - began < 10
    finally:
        if proc.poll() is None:
            proc.kill()
        os.close(read_end)
        os.close(write_end)
