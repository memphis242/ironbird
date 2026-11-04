"""Host-timing guard for ``ib send --every``: the pacer thread keeps a 1 ms period on a quiet
host, and a 10 ms period costs little CPU.

These measure the host as much as the tool, so they skip under xdist and run, enforced, in the
serial CI step ``pytest tests/bench -n0``. Each prints its numbers. The figures come from the
series' own ``send.outcome`` record (lateness is measured in the pacer thread when each send
returns) and from the child's CPU time, net of a single send's (interpreter start-up).
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from hosttiming import needs_a_quiet_host
from recutil import CLI_MAIN
from replayutil import child_env, write_config_58k

SKIPPED_SHARE_MAX = 0.02  # of the slots at 1 ms
LATE_MEAN_MAX_NS = 100_000  # mean lateness at 1 ms
CPU_SHARE_MAX_10MS = 0.10  # of one core, at a 10 ms period


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
    sink = _Sink(port)
    try:
        _send(tmp_path, config, env, "--every", "1ms", "--for", "3s")
    finally:
        sink.close()
    outcome = _outcome(tmp_path)
    print(
        f"\n1 ms: sent {outcome['sent']} of {outcome['planned']}, skipped {outcome['skipped']} "
        f"(starved {outcome['starved']}), lateness mean {outcome['late_mean_ns'] / 1000:.1f} us "
        f"max {outcome['late_max_ns'] / 1000:.1f} us, pacer {outcome['pacer']}"
    )
    assert outcome["planned"] == 3000
    assert outcome["skipped"] <= SKIPPED_SHARE_MAX * 3000, outcome
    assert outcome["late_mean_ns"] < LATE_MEAN_MAX_NS, outcome
    assert sink.count == outcome["sent"]
    assert outcome["pacer"]["timer_slack_ns"] == 1000, outcome["pacer"]


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
