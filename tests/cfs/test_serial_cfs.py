"""Stage 2k against a real cFS instance, over a serial link made of a pty.

``tools/cfs/serial_bridge.py`` joins a pseudo-terminal to CI_LAB (commands) and TO_LAB
(telemetry); it frames the command direction with the CCSDS primary header's length field alone,
independently of ironbird's header-profile framer, so a disagreement between the two shows up here.
One process owns the serial device at a time, so ``ib send`` and ``ib listen`` take turns; where a
test needs a command while it listens, the command goes straight to CI_LAB over UDP.

Opt-in (marker ``cfs``), like ``test_acceptance.py``; ``tools/cfs/build.sh`` first.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import test_acceptance

from conftest import EXAMPLES, REPO
from ironbird.ccdd.loader import load_ccdd
from ironbird.codec import Codec
from ironbird.header import load_profile

pytestmark = pytest.mark.cfs

# The module-scoped fixture that starts cFS, shared with the Stage 1 acceptance tests.
cfs = test_acceptance.cfs

BRIDGE = REPO / "tools" / "cfs" / "serial_bridge.py"
CI_LAB = ("127.0.0.1", 1234)


def codec() -> Codec:
    return Codec(
        load_ccdd(EXAMPLES / "ccdd" / "cfs.json"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm_cfe_time.toml"),
    )


def to_ci_lab(command: str, **args: object) -> None:
    """A command straight to CI_LAB over UDP, as a ground station on the lab network would."""
    data = codec().encode_command(command, args).data
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.sendto(data, CI_LAB)


class Bridge:
    def __init__(self, directory: Path, *extra: str) -> None:
        self.link = directory / "uart"
        self.proc = subprocess.Popen(
            [sys.executable, str(BRIDGE), "--link", str(self.link), *extra],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert self.proc.stdout is not None
        assert "ready" in self.proc.stdout.readline()
        text = (EXAMPLES / "cfs-serial.toml").read_text()
        text = text.replace("/tmp/ib-cfs-uart", str(self.link))
        text = text.replace('"ccdd/', f'"{EXAMPLES}/ccdd/').replace(
            '"headers/', f'"{EXAMPLES}/headers/'
        )
        self.config = directory / "cfs-serial.toml"
        self.config.write_text(text)

    def stop(self) -> str:
        self.proc.send_signal(signal.SIGTERM)
        _, err = self.proc.communicate(timeout=10)
        return err


@pytest.fixture
def env(tmp_path: Path) -> dict[str, str]:
    environ = {**os.environ, "IRONBIRD_STATE_DIR": str(tmp_path / "state")}
    environ.pop("IRONBIRD_CONFIG", None)
    return environ


def ib(bridge: Bridge, *args: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "ironbird.cli.main", "-c", str(bridge.config), *args],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env=env,
    )


class Listener:
    """``ib listen fc --format json`` over the serial link; the packets it prints, as dicts."""

    def __init__(self, bridge: Bridge, env: dict[str, str], directory: Path) -> None:
        self.err_path = directory / "listen.err"
        self.packets: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        with self.err_path.open("w") as err:
            self.proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "ironbird.cli.main",
                    "-c",
                    str(bridge.config),
                    "listen",
                    "fc",
                    "--format",
                    "json",
                ],
                stdout=subprocess.PIPE,
                stderr=err,
                text=True,
                env=env,
            )
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()
        self.wait_stderr("listening for")

    def _read(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            with self._lock:
                self.packets.append(json.loads(line))

    @property
    def stderr(self) -> str:
        return self.err_path.read_text(errors="replace")

    def named(self, name: str) -> list[dict[str, Any]]:
        with self._lock:
            return [p for p in self.packets if p.get("name") == name]

    def wait_stderr(self, needle: str, timeout: float = 20.0) -> None:
        self.wait(lambda: needle in self.stderr, f"{needle!r} on stderr", timeout)

    def wait(self, predicate: Callable[[], bool], what: str, timeout: float = 40.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            assert self.proc.poll() is None, f"ib listen exited: {self.stderr}"
            assert time.monotonic() < deadline, f"timed out waiting for {what}:\n{self.stderr}"
            time.sleep(0.05)

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        self._reader.join(timeout=5)


def console_count(log: Path, needle: str) -> int:
    return log.read_text(errors="replace").count(needle)


def wait_for_log(log: Path, needle: str, count: int, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while console_count(log, needle) < count:
        assert time.monotonic() < deadline, f"cFS never logged {needle!r} x{count}"
        time.sleep(0.05)


@pytest.fixture
def bridge(tmp_path: Path, cfs: Path) -> Iterator[Bridge]:
    box = Bridge(tmp_path)
    yield box
    box.stop()


def test_ib_send_over_serial_is_accepted_by_cfs_and_telemetry_comes_back_decoded(
    bridge: Bridge,
    env: dict[str, str],
    cfs: Path,
    tmp_path: Path,
) -> None:
    before = console_count(cfs, "SAMPLE: NOOP command")
    enable = ib(bridge, "send", "fc", "TO_LAB_ENABLE_OUTPUT", "dest_IP=127.0.0.1", env=env)
    assert enable.returncode == 0, enable.stderr
    assert "via serial" in enable.stdout
    noop = ib(bridge, "send", "fc", "SAMPLE_APP_NOOP", env=env)
    assert noop.returncode == 0, noop.stderr
    wait_for_log(cfs, "SAMPLE: NOOP command", before + 1)  # cFS accepted what ib framed

    listener = Listener(bridge, env, tmp_path)
    try:
        to_ci_lab("ES_SEND_HK")
        listener.wait(lambda: bool(listener.named("ES_HK")), "an ES housekeeping packet")
        hk = listener.named("ES_HK")[0]
        assert hk["fields"]["CommandCounter"] >= 0 and hk["mid"] == "0x0800"
        to_ci_lab("SAMPLE_APP_NOOP")
        listener.wait(
            lambda: any("NOOP command" in json.dumps(p) for p in listener.packets),
            "the NOOP event over serial",
        )
    finally:
        listener.stop()
    assert "IB-TX-007" not in listener.stderr  # real cFS on a clean line needs no resync


def test_line_noise_between_cfs_packets_is_reported_and_costs_none_of_them(
    tmp_path: Path,
    env: dict[str, str],
    cfs: Path,
) -> None:
    box = Bridge(tmp_path, "--garbage-every", "3", "--garbage-bytes", "7")
    try:
        enable = ib(box, "send", "fc", "TO_LAB_ENABLE_OUTPUT", "dest_IP=127.0.0.1", env=env)
        assert enable.returncode == 0, enable.stderr
        listener = Listener(box, env, tmp_path)
        try:
            requests = 12
            for _ in range(requests):
                to_ci_lab("ES_SEND_HK")
                time.sleep(0.3)
            listener.wait(
                lambda: len(listener.named("ES_HK")) >= requests, f"{requests} ES_HK packets"
            )
            listener.wait_stderr("IB-TX-007")
        finally:
            listener.stop()
    finally:
        box.stop()
    assert "resynchronised" in listener.stderr
    assert "IB-DEC-" not in listener.stderr  # the noise is one notice, not a decode error each
