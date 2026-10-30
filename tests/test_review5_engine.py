"""Regression tests for the fifth Stage 1 review: a signal notice that must not block the exit,
a signal after the shutdown, exact abandoned-line counts, clean.sh key comparison, and the
working directory staying untouched.

Each test reproduces a reviewer-found failure through the real entry point where that is
feasible (subprocesses with real signals and pipes), otherwise through the module API.
"""

from __future__ import annotations

import fcntl
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import test_review2_engine as r2
from ironbird.cli.output import CHUNK_BYTES, WRITE_SLICE, BoundedWriter, slice_end
from test_review2_engine import cli_argv, events_named, kill_quietly, patched_argv, shm_row
from test_review4_engine import ftok_key, run_clean

state_dir = r2.state_dir

# -- 1: the direct signal notice never blocks the exit --

# stderr is a pipe nobody reads. As the ledger closes, with everything else already written to
# stderr (however much the host made the run print, e.g. an IB-TX-005 receive-buffer warning),
# the pipe is filled to within five bytes of full and SIGTERM lands, so the notice is written
# with the shutdown signals held.
FULL_STDERR_SCRIPT = """
import os, signal, sys, fcntl, termios, struct
import ironbird.cli.main as m
from ironbird.ledger import Ledger
r, w = os.pipe()
os.dup2(w, 2)
original = Ledger.close
fired = []
def close(self):
    if not fired:
        fired.append(1)
        cap = fcntl.fcntl(w, fcntl.F_GETPIPE_SZ)
        queued = struct.unpack("i", fcntl.ioctl(r, termios.FIONREAD, bytes(4)))[0]
        assert 0 < queued < cap - 5, (queued, cap)
        os.set_blocking(w, False)  # a short write still leaves at most five bytes free
        try:
            os.write(w, b"." * (cap - queued - 5))
        except BlockingIOError:
            pass
        os.set_blocking(w, True)
        os.kill(os.getpid(), signal.SIGTERM)
    return original(self)
Ledger.close = close
sys.argv = ["ib", "-c", sys.argv[1], "listen", "fc", "--timeout", "0.3"]
sys.exit(m.main())
"""


def test_signal_notice_on_a_full_stderr_pipe_does_not_hang_and_the_ledger_has_it(
    mock_config_path: Path, state_dir: Path
) -> None:
    proc = subprocess.Popen(
        [sys.executable, "-c", FULL_STDERR_SCRIPT, str(mock_config_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        proc.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        kill_quietly(proc)
        raise AssertionError(
            "the process hung writing the signal notice to a full stderr"
        ) from None
    finally:
        kill_quietly(proc)
    assert proc.returncode == 143
    (event,) = events_named(state_dir, "cli.signal")
    assert event["signum"] == int(signal.SIGTERM)


# -- 2: a signal after the shutdown still ends the process by that signal --

SIGNAL_AFTER_FINISH = """
    import os, signal
    import ironbird.cli.main as _m
    _orig_finish = _m._finish
    def _finish(*args, **kwargs):
        code = _orig_finish(*args, **kwargs)
        os.kill(os.getpid(), signal.SIGTERM)  # after the ledger is closed
        return code
    _m._finish = _finish
"""


def test_signal_right_after_finish_is_not_swallowed(mock_config_path: Path) -> None:
    done = subprocess.run(
        patched_argv(SIGNAL_AFTER_FINISH, mock_config_path, "nodes"),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert done.returncode in (-int(signal.SIGTERM), 128 + int(signal.SIGTERM)), done.stderr


# -- 3: abandoned-line counts are exact once the reader catches up ----------


def test_abandoned_plus_received_equals_accepted_when_the_reader_drains_afterwards() -> None:
    """A write end that refuses with EAGAIN: the retry loop must not write once abandoned."""
    rfd, wfd = os.pipe()
    capacity = fcntl.fcntl(wfd, fcntl.F_GETPIPE_SZ)
    os.set_blocking(wfd, False)
    stream = os.fdopen(wfd, "w", closefd=False)
    writer = BoundedWriter(stream)
    accepted = capacity // 8 + 600  # 8-byte lines: the pipe fills, then ~600 more are queued
    try:
        for i in range(accepted):
            writer.write(f"{i:07d}")
        time.sleep(0.3)  # the thread is now waiting for room in the full pipe
        abandoned = writer.abandon()
        os.set_blocking(rfd, False)
        received = 0
        quiet_since = time.monotonic()
        while time.monotonic() - quiet_since < 0.3:
            try:
                data = os.read(rfd, 1 << 20)
            except BlockingIOError:
                time.sleep(0.02)
                continue
            quiet_since = time.monotonic()
            received += data.count(b"\n")
        assert writer.wait(5)
        assert received + abandoned == accepted
    finally:
        os.close(rfd)
        stream.close()
        os.close(wfd)


def test_slices_end_on_a_newline_when_the_slice_holds_one() -> None:
    short = b"".join(b"%06d\n" % i for i in range(2 * CHUNK_BYTES // 7))
    offset = 0
    while offset < len(short):
        end = slice_end(short, offset)
        assert 0 < end - offset <= WRITE_SLICE
        assert short[end - 1 : end] == b"\n"
        offset = end
    long_line = b"x" * (3 * WRITE_SLICE) + b"\n"
    assert slice_end(long_line, 0) == WRITE_SLICE  # no newline in the slice: cut at the limit
    assert slice_end(b"ab\n", 0) == 3  # the rest of the chunk fits


# -- 4: clean.sh compares keys as canonical decimal strings --


def test_clean_sh_ignores_octal_looking_and_malformed_rows_explicitly(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    keyfile = run_dir / ".cdskeyfile"
    keyfile.write_text("")
    key = ftok_key(keyfile)
    uid = 70000 if os.getuid() == 0 else os.getuid()
    table = tmp_path / "shm"
    table.write_text(
        "key shmid perms size cpid lpid nattch uid gid cuid cgid atime dtime ctime rss swap\n"
        "garbage row\n"
        f"0{key:o} 101 600 1 1 1 0 {uid} 0 0 0 0 0 0 0 0\n"  # equals the key read as octal
        f"089 102 600 1 1 1 0 {uid} 0 0 0 0 0 0 0 0\n"  # not a valid octal number
        f"{shm_row(key, 103, 0, uid)}\n"
    )
    errors: list[str] = []
    removed = run_clean(tmp_path, table, run_dir, errors=errors)
    assert removed == ["-m", "103"]
    assert "value too great" not in errors[0] and "syntax error" not in errors[0], errors[0]
    assert errors[0].count("ignoring malformed") == 3, errors[0]


# -- 5: the working directory stays untouched --


def test_cli_leaves_an_empty_working_directory_empty(
    mock_config_path: Path, tmp_path: Path
) -> None:
    cwd = tmp_path / "empty-cwd"
    cwd.mkdir()
    for args in (("nodes",), ("send", "fc", "MOCK_NOOP"), ("listen", "fc", "--timeout", "0.3")):
        done = subprocess.run(
            cli_argv(mock_config_path, *args),
            cwd=cwd,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=30,
            check=False,
        )
        assert done.returncode in (0, 1, 3), (args, done.stderr)
    assert sorted(cwd.iterdir()) == []
