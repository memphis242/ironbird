"""Host-timing guard for ``ib send --every``: the pacer thread keeps a 1 ms period within a small
margin of what the host itself allows, a 10 ms period costs little CPU, a 50 ms period stays within
half a period, and a helper thread busy in Python costs the pacer few slots.

These measure the host as much as the tool, so they skip under xdist and run, enforced, in the
serial CI step ``pytest tests/bench -n0``. Each prints its numbers. The figures come from the
series' own ``send.outcome`` record (lateness is measured in the pacer thread when each send
returns) and from the child's CPU time, net of a single send's (interpreter start-up).

The 1 ms guard is measured against the host, not against a fixed figure: a serial run on a shared
VM is no quiet host, and there a bare thread sleeping to the same deadlines (``wake_reference``,
measured before and after the series) already wakes 20-100 us late on average. What the guard
gates is the pacer's own share on top of that floor (its wait, the send, the interpreter lock),
and its skips beyond the host's own; on a quiet host (a floor under 25 us) that keeps the mean
lateness under 100 us. That share is 30-50 us on a busy 4-vCPU development VM, and a send through
the loop instead of from the thread adds about 400 us. A host whose bare thread cannot hold the
period itself cannot measure the pacer at all, and the guard skips there, with its numbers.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from hosttiming import needs_a_quiet_host, wake_reference
from ironbird import pacer, sendmodes
from recutil import CLI_MAIN
from replayutil import child_env, write_config_58k

MS = 1_000_000
SKIPPED_SHARE_MAX = 0.02  # of the slots at 1 ms, beyond the share the host's bare thread loses
LATE_OVER_HOST_MAX_NS = 75_000  # mean lateness at 1 ms, beyond the host's bare thread's mean
REFERENCE_SLOTS = 1000  # of the host's bare thread at 1 ms, before and after the series
HOST_LATE_MEAN_MAX_NS = 250_000  # beyond this the host's bare thread cannot measure 1 ms
HOST_SKIPPED_SHARE_MAX = 0.05  # ... nor beyond this share of its slots passed over
CPU_SHARE_MAX_10MS = 0.10  # of one core, at a 10 ms period
LATE_MAX_50MS_NS = 25_000_000  # the longest send, at a 50 ms period (half a period)
SKIPPED_SHARE_MAX_BUSY = 0.10  # of the slots at 10 ms, with a helper thread busy in Python


class _Sink:
    def __init__(self, port: int) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 << 20)
        self.sock.bind(("127.0.0.1", port))
        self.sock.settimeout(0.05)
        self.count = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.sock.recv(4096)
            except TimeoutError:
                continue
            self.count += 1

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self.sock.close()


def _send(tmp_path: Path, config: Path, env: dict[str, str], *flags: str) -> float:
    """Run ``ib send fc MOCK_NOOP FLAGS``; returns its CPU seconds."""
    argv = [sys.executable, "-c", CLI_MAIN, "-c", str(config), "send", "fc", "MOCK_NOOP", *flags]
    child = subprocess.Popen(
        argv, env=env, cwd=tmp_path, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
    )
    _, status, usage = os.wait4(child.pid, 0)
    assert child.stderr is not None
    stderr = child.stderr.read().decode(errors="replace")
    child.stderr.close()
    child.returncode = os.waitstatus_to_exitcode(status)
    assert child.returncode == 0, stderr
    return usage.ru_utime + usage.ru_stime


def _outcome(tmp_path: Path) -> dict[str, Any]:
    found: dict[str, Any] = {}
    for path in sorted((tmp_path / "state").glob("*.jsonl")):
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if record["event"] == "send.outcome":
                found = record
    assert found, "no send.outcome"
    return found


@needs_a_quiet_host
def test_a_one_millisecond_period_is_kept(tmp_path: Path) -> None:
    config, port, _ = write_config_58k(tmp_path)
    env = child_env(tmp_path)
    before = wake_reference(MS, REFERENCE_SLOTS)
    sink = _Sink(port)
    try:
        _send(tmp_path, config, env, "--every", "1ms", "--for", "3s")
    finally:
        sink.close()
    host = before + wake_reference(MS, REFERENCE_SLOTS)
    outcome = _outcome(tmp_path)
    print(
        f"\n1 ms: sent {outcome['sent']} of {outcome['planned']}, skipped {outcome['skipped']} "
        f"(starved {outcome['starved']}), lateness mean {outcome['late_mean_ns'] / 1000:.1f} us "
        f"max {outcome['late_max_ns'] / 1000:.1f} us, pacer {outcome['pacer']}; host's bare "
        f"thread: lateness mean {host.late_mean_ns / 1000:.1f} us, skipped {host.skipped} "
        f"of {host.slots}"
    )
    assert outcome["planned"] == 3000
    assert sink.count == outcome["sent"]
    assert outcome["pacer"]["timer_slack_ns"] == 1000, outcome["pacer"]
    if host.late_mean_ns > HOST_LATE_MEAN_MAX_NS or host.skipped_share > HOST_SKIPPED_SHARE_MAX:
        pytest.skip(
            f"this host cannot measure a 1 ms period: a bare thread sleeping to it wakes "
            f"{host.late_mean_ns / 1000:.0f} us late on average and loses "
            f"{host.skipped_share:.1%} of its slots"
        )
    skipped_share = outcome["skipped"] / outcome["planned"]
    assert skipped_share - host.skipped_share <= SKIPPED_SHARE_MAX, (
        f"the pacer lost {skipped_share:.1%} of its slots, the host's bare thread "
        f"{host.skipped_share:.1%}"
    )
    assert outcome["late_mean_ns"] - host.late_mean_ns < LATE_OVER_HOST_MAX_NS, (
        f"the pacer's mean lateness {outcome['late_mean_ns'] / 1000:.1f} us is "
        f"{LATE_OVER_HOST_MAX_NS / 1000:.0f} us or more over the host's bare thread's "
        f"{host.late_mean_ns / 1000:.1f} us"
    )


@needs_a_quiet_host
def test_a_ten_millisecond_period_costs_little_cpu(tmp_path: Path) -> None:
    config, port, _ = write_config_58k(tmp_path)
    env = child_env(tmp_path)
    sink = _Sink(port)
    try:
        start_up = _send(tmp_path, config, env)
        cpu = _send(tmp_path, config, env, "--every", "10ms", "--for", "5s")
    finally:
        sink.close()
    share = (cpu - start_up) / 5
    print(f"\n10 ms: CPU {share:.1%} of a core (start-up {start_up:.2f} s excluded)")
    assert share < CPU_SHARE_MAX_10MS


@needs_a_quiet_host
def test_a_fifty_millisecond_period_stays_within_half_a_period(tmp_path: Path) -> None:
    config, port, _ = write_config_58k(tmp_path)
    env = child_env(tmp_path)
    sink = _Sink(port)
    try:
        _send(tmp_path, config, env, "--every", "50ms", "--for", "2s")
    finally:
        sink.close()
    outcome = _outcome(tmp_path)
    print(
        f"\n50 ms: sent {outcome['sent']} of {outcome['planned']}, lateness mean "
        f"{outcome['late_mean_ns'] / 1000:.1f} us max {outcome['late_max_ns'] / 1000:.1f} us"
    )
    assert outcome["planned"] == 40 and outcome["sent"] + outcome["skipped"] == 40
    assert outcome["skipped"] <= 2 and outcome["late_max_ns"] < LATE_MAX_50MS_NS, outcome


def _busy_thread_series(every_ms: int, slots: int, burst_ms: int, interval_s: float) -> int:
    """Run a series of ``slots`` sends that do nothing, while another thread runs Python in
    ``burst_ms`` bursts, with the pacer lowering the switch interval to ``interval_s``; returns
    the slots skipped."""
    ms = 1_000_000
    stop = threading.Event()

    def hog() -> None:
        while not stop.is_set():
            end = time.perf_counter_ns() + burst_ms * ms
            while time.perf_counter_ns() < end:
                pass
            time.sleep(0.004)

    skipped = 0

    def handle(report: pacer.Report[int]) -> BaseException | None:
        nonlocal skipped
        skipped += report.slots if report.kind is pacer.Kind.SKIPPED else 0
        return report.error

    async def main() -> None:
        start = time.monotonic_ns() + 20 * ms
        grid = sendmodes.Grid(
            start, time.time_ns(), every_ms * ms, start + slots * every_ms * ms, None, False
        )
        series: pacer.SeriesPacer[int] = pacer.SeriesPacer(
            grid, lambda data, stop: True, pacer.ThreadWaiter(0), timed=False
        )
        await series.run(lambda n, slot: pacer.Built(n, slot, b"x", n), handle)

    previous = pacer.SWITCH_INTERVAL_S
    pacer.SWITCH_INTERVAL_S = interval_s
    thread = threading.Thread(target=hog, daemon=True)
    thread.start()
    try:
        asyncio.run(main())
    finally:
        pacer.SWITCH_INTERVAL_S = previous
        stop.set()
        thread.join(5)
    return skipped


@needs_a_quiet_host
def test_a_busy_python_thread_costs_the_pacer_little() -> None:
    """Another thread running Python in 8 ms bursts holds the interpreter lock for up to the
    switch interval after the pacer wakes: at the default 5 ms that skips about a fifth of the
    slots of a 10 ms period; the series' lowered interval keeps it to a few."""
    default = _busy_thread_series(10, 300, 8, 1.0)  # not lowered: min(5 ms, 1 s)
    lowered = _busy_thread_series(10, 300, 8, pacer.SWITCH_INTERVAL_S)
    print(f"\n10 ms under 8 ms bursts: skipped {default} of 300 (5 ms switch), {lowered} (lowered)")
    assert lowered <= SKIPPED_SHARE_MAX_BUSY * 300
    assert lowered < default


def test_the_bench_helper_leaves_the_switch_interval_alone() -> None:
    before = sys.getswitchinterval()
    _busy_thread_series(10, 5, 0, pacer.SWITCH_INTERVAL_S)
    assert sys.getswitchinterval() == before
