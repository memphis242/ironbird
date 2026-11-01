"""The loopback blaster keeps its schedule and bounds its catch-up bursts."""

from __future__ import annotations

import struct
from types import SimpleNamespace

import blaster
import pytest

RATE = 1_000
COUNT = 3_000
TEMPLATE = bytes(16)


class _World:
    """A clock that only moves when the blaster sleeps, or when a test stalls the process, and a
    socket that records the order of sends and sleeps."""

    def __init__(self, stall_after_sends: int, stall_s: float) -> None:
        self.now = 0.0
        self.stall_after_sends = stall_after_sends
        self.stall_s = stall_s
        self.serials: list[int] = []
        self.batches: list[int] = []  # sends between two sleeps
        self._run = 0

    def perf_counter(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        if self._run:
            self.batches.append(self._run)
            self._run = 0
        self.now += max(seconds, 1e-6)

    def socket(self, *_: object) -> SimpleNamespace:
        def sendto(data: bytes, _target: object) -> None:
            self.serials.append(struct.unpack_from("<I", data, 0)[0])
            self._run += 1
            if len(self.serials) == self.stall_after_sends:
                self.now += self.stall_s  # the process was descheduled

        return SimpleNamespace(sendto=sendto, close=lambda: None)


def test_a_stalled_blaster_catches_up_in_bounded_bursts(monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(stall_after_sends=500, stall_s=1.0)  # 1000 datagrams fall due at once
    monkeypatch.setattr(
        blaster,
        "time",
        SimpleNamespace(perf_counter=world.perf_counter, sleep=world.sleep),
    )
    monkeypatch.setattr(
        blaster, "socket", SimpleNamespace(socket=world.socket, AF_INET=0, SOCK_DGRAM=0)
    )
    result = blaster.blast(9, RATE, COUNT, TEMPLATE, 0)
    world.batches.append(world._run)
    assert result["sent"] == COUNT
    assert world.serials == list(range(COUNT)), "every datagram once, in order"
    assert max(world.batches) <= blaster.MAX_BATCH, max(world.batches)
    assert max(world.batches) > 1, "the stall was followed by a catch-up burst"
