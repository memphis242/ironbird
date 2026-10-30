"""Regression tests for the third Stage 1 review: lossless output on a normal end, signal
masks in the output threads, closed standard streams, socket-creation failures, kernel-drop
scanning off the loop, final drop counts, and a signal during session teardown.

Each test reproduces a reviewer-found failure through the real entry point where that is
feasible (subprocesses with real signals and pipes), otherwise through the session API.
"""

from __future__ import annotations

import asyncio
import errno
import os
import resource
import signal
import socket
import subprocess
import sys
import threading
import time
import types
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import test_review2_engine as r2
from conftest import REPO
from ironbird.cli.main import app
from ironbird.cli.output import BoundedWriter
from ironbird.config import Endpoint
from ironbird.errors import Code, IronbirdError
from ironbird.ledger import default_state_dir
from ironbird.session import Session
from ironbird.transport import udp
from ironbird.transport.udp import UdpTransport
from test_review2_engine import (
    cli_argv,
    endpoints,
    events_named,
    hk_wire,
    kill_quietly,
    patched_argv,
    read_ledger,
    shm_row,
    start_blocked_listen,
    wait_for_file_text,
    wait_for_text,
)

# Fixtures shared with the second review's tests.
state_dir = r2.state_dir
run_session = r2.run_session

EXIT_OUTPUT_LOST = 4
TOOLS = REPO / "tools" / "cfs"


def send_burst(config: Path, count: int, label: str = "x") -> None:
    _, telemetry = endpoints(config)
    wire = hk_wire(config, label)
    raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for i in range(count):
            raw.sendto(wire, (telemetry.host, telemetry.port))
            if i % 100 == 99:
                time.sleep(0.005)
    finally:
        raw.close()


# -- 1: normal end drains with no deadline; losses are reported --


def test_slow_reader_receives_every_line_when_listen_ends_normally(
    mock_config_path: Path, tmp_path: Path, state_dir: Path
) -> None:
    count = 1500
    errfile = tmp_path / "err.txt"
    proc = subprocess.Popen(
        cli_argv(
            mock_config_path,
            "listen",
            "fc",
            "--count",
            str(count),
            "--format",
            "json",
            "--timeout",
            "60",
        ),
        stdout=subprocess.PIPE,  # unread until the run is long over
        stderr=errfile.open("w"),
    )
    try:
        wait_for_file_text(errfile, "listening", proc)
        send_burst(mock_config_path, count)
        time.sleep(3)  # the run has ended; the pipe is full and the process waits for us
        assert proc.poll() is None, "the process must wait for its reader, not abandon lines"
        assert proc.stdout is not None
        out = proc.stdout.read()
        proc.wait(timeout=15)
    finally:
        kill_quietly(proc)
    assert out.count(b"\n") == count
    assert proc.returncode == 0, errfile.read_text()
    assert "IB-OUT" not in errfile.read_text().replace("IB-OUT-003", "")


def test_dropped_stdout_lines_get_one_coded_line_a_ledger_count_and_exit_4(
    mock_config_path: Path, tmp_path: Path, state_dir: Path
) -> None:
    proc, errfile = start_blocked_listen(mock_config_path, tmp_path, "--timeout", "1")
    try:
        # The run ends at the timeout; shutdown then waits for the reader of the full pipe.
        wait_for_file_text(errfile, Code.OUT_WAITING.value, proc, timeout=20)
        assert proc.poll() is None
        assert proc.stdout is not None
        proc.stdout.read()
        proc.wait(timeout=15)
    finally:
        kill_quietly(proc)
    err = errfile.read_text()
    assert proc.returncode == EXIT_OUTPUT_LOST, err
    assert err.count(Code.OUT_LOST.value) == 1, err
    (lost,) = events_named(state_dir, Code.OUT_LOST.value)
    assert lost["stdout_lost"] > 0


def test_signal_cuts_the_final_drain_short_and_counts_what_was_abandoned(
    mock_config_path: Path, tmp_path: Path, state_dir: Path
) -> None:
    proc, errfile = start_blocked_listen(mock_config_path, tmp_path, "--timeout", "1")
    try:
        wait_for_file_text(errfile, Code.OUT_WAITING.value, proc, timeout=20)
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=15)
    finally:
        kill_quietly(proc)
    assert proc.returncode == 143
    assert Code.OUT_LOST.value in errfile.read_text()
    (lost,) = events_named(state_dir, Code.OUT_LOST.value)
    assert lost["stdout_abandoned"] > 0
    assert events_named(state_dir, "cli.signal")


def test_reader_exiting_during_the_final_drain_is_a_normal_end_without_a_traceback(
    mock_config_path: Path, tmp_path: Path, state_dir: Path
) -> None:
    proc = subprocess.Popen(
        cli_argv(
            mock_config_path,
            "listen",
            "fc",
            "--count",
            "1500",
            "--format",
            "json",
            "--timeout",
            "60",
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_text(proc, "listening")
        send_burst(mock_config_path, 1500)
        deadline = time.monotonic() + 15
        while not events_named(state_dir, "session.end") and time.monotonic() < deadline:
            time.sleep(0.05)
        time.sleep(0.3)  # the event loop is closed; the process is draining a full pipe
        assert proc.stdout is not None
        proc.stdout.close()
        _, err = proc.communicate(timeout=15)
    finally:
        kill_quietly(proc)
    assert proc.returncode == 0, err
    assert "Traceback" not in err and "RuntimeError" not in err and "IB-INT-001" not in err


# -- 2: the writer threads hold the shutdown signals --


def blocked_signals(tid: int) -> set[int]:
    for line in Path(f"/proc/self/task/{tid}/status").read_text().splitlines():
        if line.startswith("SigBlk:"):
            mask = int(line.split()[1], 16)
            return {n for n in range(1, 65) if mask & (1 << (n - 1))}
    raise AssertionError("no SigBlk line")


@pytest.mark.skipif(not Path("/proc/self/task").exists(), reason="needs /proc")
def test_output_threads_block_the_shutdown_signals() -> None:
    read_fd, write_fd = os.pipe()
    stream = os.fdopen(write_fd, "w")
    writer = BoundedWriter(stream)
    try:
        (thread,) = [t for t in threading.enumerate() if t.name == "ib-output"]
        assert thread.native_id is not None
        held = blocked_signals(thread.native_id)
        assert {signal.SIGINT, signal.SIGTERM, signal.SIGHUP} <= held
        assert not {signal.SIGINT, signal.SIGTERM, signal.SIGHUP} & blocked_signals(
            threading.get_native_id()
        ), "starting the thread must not leave the signals blocked in the caller"
    finally:
        writer.close()
        stream.close()
        os.close(read_fd)


SLOW_CLOSE = """
    import ironbird.ledger as _l
    _orig_close = _l.Ledger.close
    def _slow_close(self):
        print("IN_CLOSE", file=sys.__stderr__, flush=True)
        time.sleep(1.5)
        _orig_close(self)
        print("CLOSE_DONE", file=sys.__stderr__, flush=True)
    _l.Ledger.close = _slow_close
"""


def test_a_signal_during_the_ledger_close_does_not_interrupt_it(
    mock_config_path: Path, state_dir: Path
) -> None:
    proc = subprocess.Popen(
        patched_argv(SLOW_CLOSE, mock_config_path, "nodes"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_text(proc, "IN_CLOSE")
        proc.send_signal(signal.SIGTERM)
        _, err = proc.communicate(timeout=15)
    finally:
        kill_quietly(proc)
    assert "CLOSE_DONE" in err, "the ledger close must run to completion"
    assert proc.returncode == 143


# -- 3: clean.sh as root --


def test_clean_sh_run_as_root_cleans_the_segments_of_the_cfs_run_uid(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    keyfile = run_dir / ".cdskeyfile"
    keyfile.write_text("")
    st = keyfile.stat()
    key = (0x52 << 24) | ((st.st_dev & 0xFF) << 16) | (st.st_ino & 0xFFFF)
    rows = [
        shm_row(key, 101, 0, 4242),  # cFS's uid: removed
        shm_row(key, 102, 0, 0),  # not cFS's uid
        shm_row(key, 103, 2, 4242),  # still attached
    ]
    bin_dir = tmp_path / "stubs"
    bin_dir.mkdir()
    log = tmp_path / "ipcrm.log"
    table = tmp_path / "shm"
    table.write_text("header\n" + "\n".join(rows) + "\n")
    (bin_dir / "ipcrm").write_text(f'#!/usr/bin/env bash\necho "$@" >> {log}\n')
    (bin_dir / "id").write_text('#!/usr/bin/env bash\ncase "$1" in -u) echo 0;; *) exit 1;; esac\n')
    for stub in bin_dir.iterdir():
        stub.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "CFS_RUN_UID": "4242",
        "CFS_SHM_TABLE": str(table),
    }
    done = subprocess.run(
        [str(TOOLS / "clean.sh"), str(run_dir)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    assert log.read_text().split() == ["-m", "101"]


# -- 4: closed standard streams --


@pytest.mark.parametrize(
    ("redirect", "code"),
    [(">&-", EXIT_OUTPUT_LOST), ("2>&-", 0), (">&- 2>&-", EXIT_OUTPUT_LOST)],
    ids=["stdout", "stderr", "both"],
)
def test_closed_standard_streams_do_not_crash_and_send_still_goes_out(
    mock_config_path: Path, state_dir: Path, redirect: str, code: int
) -> None:
    command_ep, _ = endpoints(mock_config_path)
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver.bind((command_ep.host, command_ep.port))
    receiver.settimeout(15)
    argv = cli_argv(mock_config_path, "send", "fc", "MOCK_NOOP")
    proc = subprocess.Popen(
        ["sh", "-c", f'exec "$@" {redirect}', "sh", *argv],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        receiver.recvfrom(2048)  # the command went out
        out, err = proc.communicate(timeout=15)
    finally:
        receiver.close()
        kill_quietly(proc)
    assert proc.returncode == code, (out, err)
    assert "Traceback" not in err and "IB-INT-001" not in err
    assert not events_named(state_dir, "IB-INT-001")
    (tx,) = events_named(state_dir, "tx.command")
    assert tx["sent"] is True and tx["result"] == "sent"
    if code == EXIT_OUTPUT_LOST:
        assert events_named(state_dir, Code.OUT_LOST.value)
        if redirect == ">&-":
            assert err.count(Code.OUT_LOST.value) == 1 and "was sent" in err


# -- 5: socket creation and the event loop under resource limits --


def fake_socket_module(error: int) -> types.SimpleNamespace:
    def boom(*_args: Any, **_kwargs: Any) -> socket.socket:
        raise OSError(error, os.strerror(error))

    fake = types.SimpleNamespace(**{k: getattr(socket, k) for k in dir(socket) if k[:2] != "__"})
    fake.socket = boom
    return fake


@pytest.mark.parametrize(
    ("error", "remedy"),
    [(errno.EAFNOSUPPORT, "address famil"), (errno.EMFILE, "ulimit")],
)
def test_socket_creation_failure_is_a_coded_open_error_with_a_remedy(
    monkeypatch: pytest.MonkeyPatch, error: int, remedy: str
) -> None:
    monkeypatch.setattr(udp, "socket", fake_socket_module(error))
    for transport, receiver in (
        (UdpTransport(remote=Endpoint("127.0.0.1", 9)), None),
        (UdpTransport(local=Endpoint("127.0.0.1", 0)), lambda *_: None),
    ):
        with pytest.raises(IronbirdError) as info:
            asyncio.run(transport.open(receiver))
        assert info.value.code is Code.TX_OPEN
        assert remedy in info.value.summary.lower()


def test_socket_creation_failure_reaches_the_user_as_a_coded_line(
    mock_config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(udp, "socket", fake_socket_module(errno.EAFNOSUPPORT))
    result = CliRunner().invoke(app, ["-c", str(mock_config_path), "send", "fc", "MOCK_NOOP"])
    assert result.exit_code == 1, result.output
    assert Code.TX_OPEN.value in result.output and "IB-INT-001" not in result.output


def limited(nofile: int) -> Any:
    def apply() -> None:
        resource.setrlimit(resource.RLIMIT_NOFILE, (nofile, nofile))

    return apply


@pytest.mark.parametrize("nofile", [5, 6])
def test_too_few_file_descriptors_for_the_event_loop_is_a_coded_error(
    mock_config_path: Path, nofile: int
) -> None:
    done = subprocess.run(
        [
            sys.executable,
            "-W",
            "default",
            "-m",
            "ironbird.cli.main",
            "-c",
            str(mock_config_path),
            "nodes",
        ],
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        preexec_fn=limited(nofile),
        timeout=30,
        check=False,
    )
    assert done.returncode == 1, done.stderr
    assert "never awaited" not in done.stderr and "IB-INT-001" not in done.stderr, done.stderr
    assert "error IB-SYS-001" in done.stderr and "ulimit" in done.stderr


# -- 6: /proc/net/udp is scanned off the loop, and not needlessly --


class ScanProbe:
    """Stand-in for ``udp.read_proc_drops`` that records who calls it and how long it takes."""

    def __init__(self, delay: float = 0.0) -> None:
        self.delay = delay
        self.threads: list[threading.Thread] = []
        self.running = 0
        self.max_running = 0
        self._real = udp.read_proc_drops

    def __call__(self, inodes: Iterable[int]) -> dict[int, int]:
        self.threads.append(threading.current_thread())
        self.running += 1
        self.max_running = max(self.max_running, self.running)
        try:
            time.sleep(self.delay)
            return self._real(inodes)
        finally:
            self.running -= 1


def transport_of(listener: Any) -> UdpTransport:
    """The UDP transport behind a listener (the engine's, reached through its capture)."""
    transport = listener.capture._subs[0].io.transport
    assert isinstance(transport, UdpTransport)
    return transport


def test_no_proc_scan_while_the_kernel_counter_rides_on_datagrams(
    run_session: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    probe = ScanProbe()
    monkeypatch.setattr(udp, "read_proc_drops", probe)
    monkeypatch.setattr("ironbird.session.TICK_S", 0.05)
    seen: list[int] = []

    async def scenario(ground: Session, *_: Any) -> None:
        listener = await ground.listen("fc")
        if not transport_of(listener).overflow_counter_active:
            await listener.close()
            pytest.skip("SO_RXQ_OVFL is not available on this kernel")
        await asyncio.sleep(0.5)  # ten ticks with an idle socket
        seen.append(len(probe.threads))
        await listener.close()

    run_session(scenario)
    assert seen == [0], "an idle listener with SO_RXQ_OVFL must not read /proc/net/udp"


def test_tail_of_a_burst_is_scanned_once_after_the_traffic_stops(
    run_session: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    probe = ScanProbe()
    monkeypatch.setattr(udp, "read_proc_drops", probe)
    monkeypatch.setattr("ironbird.session.TICK_S", 0.05)
    seen: list[int] = []

    async def scenario(ground: Session, *_: Any) -> None:
        listener = await ground.listen("fc")
        if not transport_of(listener).overflow_counter_active:
            await listener.close()
            pytest.skip("SO_RXQ_OVFL is not available on this kernel")
        addr = listener.local_address
        assert addr is not None
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for _i in range(2000):
            raw.sendto(bytes(1000), addr)
        raw.close()
        await asyncio.sleep(0.6)
        seen.append(len(probe.threads))
        await listener.close()

    run_session(scenario)
    assert 1 <= seen[0] <= 2, seen


def test_proc_scan_without_the_kernel_counter_runs_off_the_loop_once_per_tick(
    run_session: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    probe = ScanProbe(delay=0.15)
    monkeypatch.setattr(udp, "read_proc_drops", probe)
    monkeypatch.setattr(UdpTransport, "_tune_listener", lambda self, sock: None)
    monkeypatch.setattr("ironbird.session.TICK_S", 0.05)
    gaps: list[float] = []
    scans: list[int] = []
    overlap: list[int] = []

    async def beat() -> None:
        last = time.monotonic()
        while True:
            await asyncio.sleep(0.01)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    async def scenario(ground: Session, *_: Any) -> None:
        listener = await ground.listen("fc")
        assert not transport_of(listener).overflow_counter_active
        heartbeat = asyncio.ensure_future(beat())
        await asyncio.sleep(1.0)
        heartbeat.cancel()
        scans.append(len(probe.threads))
        overlap.append(probe.max_running)  # the close-time scan may overlap a tick's
        await listener.close()

    run_session(scenario)
    assert all(t is not threading.main_thread() for t in probe.threads)
    assert scans[0] >= 2 and overlap == [1]
    assert max(gaps) < 0.1, f"the loop stalled for {max(gaps):.3f} s"


# -- 7: relative IRONBIRD_STATE_DIR --


def test_relative_state_dir_override_is_refused_with_a_remedy() -> None:
    with pytest.raises(IronbirdError) as info:
        default_state_dir({"IRONBIRD_STATE_DIR": "relative/state", "HOME": "/home/someone"})
    assert info.value.code is Code.LED_DIR
    assert "IRONBIRD_STATE_DIR" in info.value.summary and "absolute" in info.value.summary
    assert default_state_dir({"IRONBIRD_STATE_DIR": "/var/ib"}) == Path("/var/ib")


def test_send_with_a_relative_state_dir_sends_nothing(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)  # the relative directory resolves under tmp_path, never the tree
    monkeypatch.setenv("IRONBIRD_STATE_DIR", "relative/state")
    command_ep, _ = endpoints(mock_config_path)
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver.bind((command_ep.host, command_ep.port))
    receiver.setblocking(False)
    try:
        result = CliRunner().invoke(app, ["-c", str(mock_config_path), "send", "fc", "MOCK_NOOP"])
        assert result.exit_code == 1 and Code.LED_DIR.value in result.output
        with pytest.raises(BlockingIOError):
            receiver.recvfrom(2048)
    finally:
        receiver.close()


# -- 8: drops counted while closing are reported --


def test_frames_a_consumer_queue_refused_are_reported_and_counted(
    run_session: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ironbird.engine.CONSUMER_QUEUE_FRAMES", 4)
    final: list[int] = []

    async def scenario(ground: Session, *_: Any) -> None:
        listener = await ground.listen("fc")  # opened, never read: the 4-frame queue fills
        addr = listener.local_address
        assert addr is not None
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for _i in range(10):
            raw.sendto(b"x" * 40, addr)
        raw.close()
        await asyncio.sleep(0.2)
        await listener.close()  # the final count is part of the close
        final.append(listener.capture.stats().not_shown)

    ledger, _, _ = run_session(scenario)
    drops = [e for e in read_ledger(ledger) if e["event"] == "rx.queue_drops"]
    assert drops and drops[-1]["total"] == final[0] == 6


# -- 9: a signal during session teardown still ends the command by signal --

SLOW_TASK_STOP = """
    import ironbird.ledger as _l
    async def _drain_task(self):
        try:
            while True:
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            print("READY", file=sys.stderr, flush=True)
            await asyncio.sleep(3)
            raise
    _l.Ledger.drain_task = _drain_task
    import ironbird.session as _s
    _orig_aexit = _s.Session.__aexit__
    async def _aexit(self, *exc):
        await asyncio.sleep(0.2)  # the background tasks have started by the time they are stopped
        return await _orig_aexit(self, *exc)
    _s.Session.__aexit__ = _aexit
"""


def test_signal_while_the_session_stops_its_tasks_still_exits_128_plus_n(
    mock_config_path: Path, state_dir: Path
) -> None:
    proc = subprocess.Popen(
        patched_argv(SLOW_TASK_STOP, mock_config_path, "nodes"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_text(proc, "READY")
        proc.send_signal(signal.SIGTERM)
        proc.communicate(timeout=15)
    finally:
        kill_quietly(proc)
    assert proc.returncode == 143
    assert events_named(state_dir, "cli.signal")[0]["signum"] == int(signal.SIGTERM)


# -- 10: find's no-match line ---------------------------------------------------------------------


def test_find_no_match_shows_the_escaped_query_once(mock_config_path: Path) -> None:
    result = CliRunner().invoke(app, ["-c", str(mock_config_path), "find", "zz\x01qq"])
    assert result.exit_code == 1
    assert "no match for" in result.output
    assert "\\x01" in result.output and "\\\\x01" not in result.output
