"""Stage 2k: ``tools/cfs/serial_bridge.py``, the pty-to-UDP bridge the real-cFS serial cases use.

Run against stand-ins for CI_LAB and TO_LAB (plain UDP sockets), so it needs no cFS.
"""

from __future__ import annotations

import importlib.util
import os
import select
import socket
import subprocess
import sys
import time
import tty
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest

from conftest import REPO
from recutil import high_ports

BRIDGE = REPO / "tools" / "cfs" / "serial_bridge.py"


def load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("serial_bridge", BRIDGE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def command(function: int, extra: bytes = b"") -> bytes:
    """A cFS-shaped command: stream id 0x1806, sequence flags, length = total - 7."""
    total = 8 + len(extra)
    return (
        bytes([0x18, 0x06, 0xC0, 0x00])
        + (total - 7).to_bytes(2, "big")
        + bytes([function, 0])
        + extra
    )


# -- the command framing, written independently of ironbird's ------------------------------


def test_commands_are_cut_by_the_primary_header_length_field() -> None:
    bridge = load()
    one, two = command(1), command(2, b"abcd")
    buf = bytearray(one + two + two[:5])
    packets, dropped = bridge.cut_packets(buf, 2048)
    assert packets == [one, two] and dropped == 0
    assert bytes(buf) == two[:5]  # the rest waits for its bytes


def test_a_length_above_the_maximum_is_skipped_a_byte_at_a_time_until_a_header_fits() -> None:
    bridge = load()
    liar = bytes([0x18, 0x06, 0xC0, 0x00, 0xFF, 0xFF])
    good = command(3)
    buf = bytearray(liar + good)
    packets, dropped = bridge.cut_packets(buf, 2048)
    assert packets == [good] and dropped >= 1
    assert not buf


# -- the process ----------------------------------------------------------------------------


class Running:
    def __init__(self, tmp_path: Path, *extra: str) -> None:
        self.link = tmp_path / "uart"
        self.ci_port, self.to_port = high_ports(2)
        self.ci = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ci.bind(("127.0.0.1", self.ci_port))
        self.ci.settimeout(10)
        self.proc = subprocess.Popen(
            [
                sys.executable,
                str(BRIDGE),
                "--link",
                str(self.link),
                "--ci-lab",
                f"127.0.0.1:{self.ci_port}",
                "--to-lab-port",
                str(self.to_port),
                *extra,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert self.proc.stdout is not None
        assert "ready" in self.proc.stdout.readline()
        self.fd = os.open(self.link, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
        tty.setraw(self.fd)

    def read_until(self, size: int, timeout: float = 15.0) -> bytes:
        got = bytearray()
        deadline = time.monotonic() + timeout
        while len(got) < size:
            assert time.monotonic() < deadline, f"timed out with {bytes(got)!r}"
            if select.select([self.fd], [], [], 0.05)[0]:
                got += os.read(self.fd, 4096)
        return bytes(got)

    def telemetry(self, data: bytes) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.sendto(data, ("127.0.0.1", self.to_port))

    def close(self) -> str:
        os.close(self.fd)
        self.ci.close()
        self.proc.terminate()
        _, err = self.proc.communicate(timeout=10)
        return err


@pytest.fixture
def running(tmp_path: Path) -> Iterator[Running]:
    box = Running(tmp_path)
    yield box
    if box.proc.poll() is None:
        box.close()


def test_a_command_written_to_the_pty_arrives_as_one_datagram(running: Running) -> None:
    packet = command(1, b"xyz")
    os.write(running.fd, packet[:4])
    os.write(running.fd, packet[4:])
    assert running.ci.recv(4096) == packet


def test_a_length_that_lies_costs_the_bridge_only_its_own_bytes(running: Running) -> None:
    good = command(7)
    os.write(running.fd, bytes([0x18, 0x06, 0xC0, 0x00, 0xFF, 0xFF]) + good)
    assert running.ci.recv(4096) == good


def test_telemetry_datagrams_become_a_byte_stream_on_the_pty(running: Running) -> None:
    first, second = command(1), command(2, b"1234")
    running.telemetry(first)
    running.telemetry(second)
    assert running.read_until(len(first) + len(second)) == first + second


def test_the_link_goes_away_with_the_bridge_and_the_summary_says_what_it_did(
    running: Running,
) -> None:
    running.telemetry(command(1))
    running.read_until(8)
    err = running.close()
    assert not running.link.exists() and "relayed" in err


def test_garbage_is_injected_with_silence_on_each_side(tmp_path: Path) -> None:
    box = Running(
        tmp_path, "--garbage-every", "2", "--garbage-bytes", "5", "--garbage-gap-ms", "10"
    )
    try:
        packets = [command(n) for n in range(1, 5)]
        for packet in packets:
            box.telemetry(packet)
            time.sleep(0.02)
        stream = box.read_until(4 * 8 + 5)
    finally:
        box.close()
    noise = stream[16:21]
    assert stream == packets[0] + packets[1] + noise + packets[2] + packets[3]
    assert all(b >= 0xF0 for b in noise)  # cannot begin a CCSDS version 0 header
