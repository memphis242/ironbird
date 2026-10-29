"""Regression tests for the fourth Stage 1 review: the drain notice reaching the ledger, signals
in the gaps around the event loop and the ledger close, the address-family fallback, clean.sh
ownership by numeric uid, closed standard descriptors, and exact abandoned-line counts.

Each test reproduces a reviewer-found failure through the real entry point where that is
feasible (subprocesses with real signals and pipes), otherwise through the module API.
"""

from __future__ import annotations

import asyncio
import ctypes
import errno
import fcntl
import os
import re
import resource
import signal
import socket
import struct
import subprocess
import termios
import threading
import time
import types
from pathlib import Path
from typing import Any

import pytest

import test_review2_engine as r2
from conftest import REPO
from ironbird import render
from ironbird.cli.output import BoundedWriter
from ironbird.config import Endpoint
from ironbird.errors import Code, IronbirdError
from ironbird.transport import udp
from ironbird.transport.udp import UdpTransport
from test_review2_engine import (
    cli_argv,
    events_named,
    kill_quietly,
    patched_argv,
    shm_row,
    start_blocked_listen,
    wait_for_file_text,
)

state_dir = r2.state_dir
TOOLS = REPO / "tools" / "cfs"

# -- 2: the drain notice is in the ledger before the wait begins ----------


def test_drain_notice_is_ledgered_so_a_kill_during_the_wait_leaves_a_trace(
    mock_config_path: Path, tmp_path: Path, state_dir: Path
) -> None:
    proc, errfile = start_blocked_listen(mock_config_path, tmp_path, "--timeout", "1")
    try:
        wait_for_file_text(errfile, Code.OUT_WAITING.value, proc, timeout=20)
        proc.kill()  # no handler runs: only what was already flushed survives
        proc.wait(timeout=10)
    finally:
        kill_quietly(proc)
    (waiting,) = events_named(state_dir, Code.OUT_WAITING.value)
    assert waiting["kind"] == "warning" and "queued" in waiting["summary"]


# -- 3, 4: a signal in the gaps still ends the command by signal, announced ----------

SIGNAL_AFTER_LOOP = """
    import os, signal
    import ironbird.cli.main as _m
    _orig_detach = _m._SignalState.detach
    def _detach(self):
        _orig_detach(self)
        os.kill(os.getpid(), signal.SIGTERM)  # process-directed, as kill(1) sends it
    _m._SignalState.detach = _detach
"""

SIGNAL_IN_CLOSE = """
    import os, signal
    import ironbird.ledger as _l
    _orig_close = _l.Ledger.close
    _calls = [0]
    def _close(self):
        _calls[0] += 1
        if _calls[0] == 1:
            os.kill(os.getpid(), signal.SIGTERM)  # arrives while the signals are held
        _orig_close(self)
    _l.Ledger.close = _close
"""


@pytest.mark.parametrize(
    "patch", [SIGNAL_AFTER_LOOP, SIGNAL_IN_CLOSE], ids=["after_loop", "in_close"]
)
def test_signal_in_a_gap_is_announced_ledgered_and_exits_128_plus_n(
    mock_config_path: Path, state_dir: Path, patch: str
) -> None:
    proc = subprocess.Popen(
        patched_argv(patch, mock_config_path, "nodes"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _, err = proc.communicate(timeout=30)
    finally:
        kill_quietly(proc)
    assert proc.returncode == 143, err
    assert "interrupted (SIGTERM)" in err, err
    (event,) = events_named(state_dir, "cli.signal")
    assert event["signum"] == int(signal.SIGTERM)


# -- 5: the address-family fallback keeps the real error ----------


def test_open_error_is_the_bind_failure_not_a_later_socket_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hog = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    hog.bind(("127.0.0.1", 0))
    port = int(hog.getsockname()[1])
    real_getaddrinfo = socket.getaddrinfo

    def getaddrinfo(host: Any, service: Any, *args: Any, **kwargs: Any) -> Any:
        if host == "dual.test":  # IPv4 first, then IPv6
            return [
                (socket.AF_INET, socket.SOCK_DGRAM, 17, "", ("127.0.0.1", port)),
                (socket.AF_INET6, socket.SOCK_DGRAM, 17, "", ("::1", port, 0, 0)),
            ]
        return real_getaddrinfo(host, service, *args, **kwargs)

    def make_socket(family: int = -1, *args: Any, **kwargs: Any) -> socket.socket:
        if family == socket.AF_INET6:  # a host without IPv6
            raise OSError(errno.EAFNOSUPPORT, os.strerror(errno.EAFNOSUPPORT))
        return socket.socket(family, *args, **kwargs)

    fake = types.SimpleNamespace(**{k: getattr(socket, k) for k in dir(socket) if k[:2] != "__"})
    fake.socket = make_socket
    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    monkeypatch.setattr(udp, "socket", fake)
    try:
        with pytest.raises(IronbirdError) as info:
            asyncio.run(UdpTransport(local=Endpoint("dual.test", port)).open())
    finally:
        hog.close()
    assert info.value.code is Code.TX_OPEN
    summary = info.value.summary.lower()
    assert "bind" in summary and "in use" in summary, summary
    assert "address famil" not in summary


# -- 6: clean.sh compares numeric uids ----------


def ftok_key(path: Path) -> int:
    st = path.stat()
    return (0x52 << 24) | ((st.st_dev & 0xFF) << 16) | (st.st_ino & 0xFFFF)


def run_clean(
    tmp_path: Path, table: Path, run_dir: Path, errors: list[str] | None = None, **extra: str
) -> list[str]:
    bin_dir = tmp_path / "stubs"
    bin_dir.mkdir(exist_ok=True)
    log = tmp_path / "ipcrm.log"
    (bin_dir / "ipcrm").write_text(f'#!/usr/bin/env bash\necho "$@" >> {log}\n')
    (bin_dir / "ipcrm").chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "CFS_SHM_TABLE": str(table),
        "CFS_RUN_UID": "70000",
        **extra,
    }
    done = subprocess.run(
        [str(TOOLS / "clean.sh"), str(run_dir)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    if errors is not None:
        errors.append(done.stderr)
    return log.read_text().split() if log.exists() else []


def test_clean_sh_tells_users_apart_by_uid_not_by_a_truncated_name(tmp_path: Path) -> None:
    if os.getuid() != 0:
        pytest.skip("CFS_RUN_UID applies when clean.sh is invoked as root")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    keyfile = run_dir / ".cdskeyfile"
    keyfile.write_text("")
    key = ftok_key(keyfile)
    table = tmp_path / "shm"
    table.write_text(
        "key shmid perms size cpid lpid nattch uid gid cuid cgid atime dtime ctime rss swap\n"
        + "\n".join(
            [
                shm_row(key, 201, 0, 70000),  # cFS's user (a long name ipcs would truncate)
                shm_row(key, 202, 0, 70001),  # a different user with the same name prefix
                shm_row(key, 203, 3, 70000),  # still attached
            ]
        )
        + "\n"
    )
    removed = run_clean(tmp_path, table, run_dir)
    assert removed == ["-m", "201"]


def test_clean_sh_removes_a_real_segment_and_leaves_the_table_without_it(tmp_path: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    keyfile = run_dir / ".cdskeyfile"
    keyfile.write_text("")
    key = ftok_key(keyfile)
    shmid = int(libc.shmget(key, 4096, 0o600 | 0o1000))  # IPC_CREAT
    if shmid < 0:
        pytest.skip(f"no SysV shared memory here: {os.strerror(ctypes.get_errno())}")
    try:
        done = subprocess.run(
            [str(TOOLS / "clean.sh"), str(run_dir)],
            env={**os.environ, "CFS_RUN_UID": str(os.getuid())},
            capture_output=True,
            text=True,
            check=False,
        )
        assert done.returncode == 0, done.stderr
        listed = Path("/proc/sysvipc/shm").read_text().splitlines()[1:]
        assert not any(row.split()[1] == str(shmid) for row in listed), done.stderr
    finally:
        libc.shmctl(shmid, 0, None)  # IPC_RMID; harmless when already gone


# -- 7: closed standard descriptors are refilled ----------

SHOW_FDS = """
    import os
    import ironbird.ledger as _l
    _orig_open = _l.Ledger.open
    def _open(self):
        _orig_open(self)
        seen = []
        for fd in (0, 1, 2):
            try:
                seen.append(os.readlink(f"/proc/self/fd/{fd}"))
            except OSError:
                seen.append("closed")
        with open("FDFILE", "w") as handle:
            handle.write("\\n".join(seen))
    _l.Ledger.open = _open
"""


def test_closed_standard_descriptors_are_refilled_before_any_file_is_opened(
    mock_config_path: Path, tmp_path: Path
) -> None:
    fdfile = tmp_path / "fds.txt"
    argv = patched_argv(SHOW_FDS.replace("FDFILE", str(fdfile)), mock_config_path, "nodes")
    done = subprocess.run(
        ["sh", "-c", 'exec "$@" <&- >&- 2>&-', "sh", *argv],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert done.returncode in (0, 4), done.stderr
    assert fdfile.read_text().split("\n") == [os.devnull] * 3


# -- 10: IPv6 endpoints print with brackets ----------


def test_ipv6_endpoints_print_in_bracket_form() -> None:
    assert str(Endpoint("127.0.0.1", 5)) == "127.0.0.1:5"
    assert str(Endpoint("::1", 5)) == "[::1]:5"
    assert "[::1]:5" in render.listen_banner("fc", ("::1", 5))
    assert "[::1]:5" in render.events_banner([("fc", ("::1", 5))])


# -- 10: the descriptor message states what is free and what is needed ----------


def test_descriptor_shortage_message_states_free_and_needed(mock_config_path: Path) -> None:
    def limit() -> None:
        resource.setrlimit(resource.RLIMIT_NOFILE, (5, 5))

    done = subprocess.run(
        cli_argv(mock_config_path, "nodes"),
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        preexec_fn=limit,
        timeout=30,
        check=False,
    )
    assert done.returncode == 1, done.stderr
    assert "error IB-SYS-001" in done.stderr
    assert re.search(r"\d+ free.*need \d+", done.stderr), done.stderr


# -- 10: abandoned-line counts are exact ----------


def pipe_with_blocked_writer(lines: int, width: int) -> tuple[BoundedWriter, int, int, int]:
    """A writer whose reader never reads, after it filled the pipe and blocked in ``write``.

    Returns ``(writer, read_fd, bytes_in_pipe, line_bytes)``.
    """
    rfd, wfd = os.pipe()
    stream = os.fdopen(wfd, "w", buffering=1)
    writer = BoundedWriter(stream)
    for _ in range(lines):
        writer.write("x" * (width - 1))
    capacity = fcntl.fcntl(wfd, fcntl.F_GETPIPE_SZ)
    deadline = time.monotonic() + 10
    last, stable_since = -1, time.monotonic()
    while time.monotonic() < deadline:
        buf = struct.pack("i", 0)
        held = struct.unpack("i", fcntl.ioctl(rfd, termios.FIONREAD, buf))[0]
        if held != last:
            last, stable_since = held, time.monotonic()
        elif held >= capacity - width and time.monotonic() - stable_since > 0.3:
            break
        time.sleep(0.02)
    return writer, rfd, last, width


def test_abandon_counts_only_lines_that_did_not_reach_the_pipe() -> None:
    lines, width = 1000, 100
    writer, rfd, delivered_bytes, _ = pipe_with_blocked_writer(lines, width)
    try:
        assert 0 < delivered_bytes < lines * width, "the pipe must be full with lines to spare"
        writer.begin_close()
        lost = writer.abandon()
        assert lost == lines - delivered_bytes // width
        assert writer.abandoned_total == lost
    finally:
        os.close(rfd)


def test_lines_accepted_between_a_write_failure_and_reader_gone_are_counted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with open("/dev/full", "w") as full:  # every write fails with ENOSPC
        writer = BoundedWriter(full)
        accepted = [0]
        original = writer._mark_gone  # pyright: ignore[reportPrivateUsage]

        def late_write_then_mark() -> None:
            writer.write("late")  # a producer racing the failing thread
            accepted[0] += 1
            original()

        monkeypatch.setattr(writer, "_mark_gone", late_write_then_mark)
        total = 1
        writer.write("first")
        deadline = time.monotonic() + 10
        while not writer.reader_gone and time.monotonic() < deadline:
            time.sleep(0.01)
        assert writer.reader_gone
        for _ in range(50):  # producers that arrive after the failure
            writer.write("after")
            total += 1
        total += accepted[0]
        writer.begin_close()
        assert writer.wait(5)
    assert writer.error is not None and writer.error.errno == errno.ENOSPC
    assert writer.dropped_total + writer.undelivered_total == total


def test_concurrent_producers_never_lose_count_when_the_stream_fails() -> None:
    with open("/dev/full", "w") as full:
        writer = BoundedWriter(full)
        per_thread, threads_n = 5000, 4

        def produce() -> None:
            for _ in range(per_thread):
                writer.write("line")

        workers = [threading.Thread(target=produce) for _ in range(threads_n)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        writer.begin_close()
        assert writer.wait(5)
    assert writer.dropped_total + writer.undelivered_total == per_thread * threads_n
