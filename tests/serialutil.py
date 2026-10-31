"""Helpers for the serial tests: a transport on a pty, and ``ib`` configs for serial links."""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

import ser_corpus
from conftest import EXAMPLES
from ironbird.ccdd.model import MessageKind
from ironbird.config import FramingSpec, SerialLinkSpec
from ironbird.errors import IronbirdError
from ironbird.transport.framing import NS_PER_MS, HeaderLengthFramer, WorkBudget
from ironbird.transport.serial import SerialTransport
from support.ptybridge import PtyDevice

TLM = MessageKind.TELEMETRY
CODEC = ser_corpus.codec()
UNLIMITED = WorkBudget(per_byte=1 << 30, base=1 << 40, cap=1 << 41)


@functools.cache
def hk(counter: int = 1) -> bytes:
    """One MOCK_HK packet per counter value, built once (the sequence field advances on every
    encode, so the same call would otherwise give different bytes)."""
    return CODEC.encode_telemetry("MOCK_HK", {"cmd_counter": counter}).data


async def wait_for(predicate: Callable[[], bool], what: str, timeout: float = 15.0) -> None:
    """Wait until ``predicate`` holds; a timeout is a failure naming ``what`` (never a fixed
    sleep standing in for a condition)."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.005)


@dataclass
class Line:
    """A :class:`SerialTransport` on one end of a pty, with everything it reports collected."""

    device: PtyDevice
    idle_ms: int = 50
    sync: bytes = b""
    fast_reconnect: bool = True
    frames: list[tuple[bytes, int, int]] = field(default_factory=list)  # data, time_ns, flags
    reports: list[IronbirdError] = field(default_factory=list)
    states: list[tuple[bool, int, IronbirdError | None]] = field(default_factory=list)
    transport: SerialTransport = field(init=False)

    def __post_init__(self) -> None:
        spec = SerialLinkSpec(str(self.device.path), 115200)
        framing = FramingSpec(sync=self.sync, max_idle_ms=self.idle_ms)
        profile = CODEC.profile(TLM)
        framer = HeaderLengthFramer(
            profile,
            max_packet_bytes=ser_corpus.CAP,
            sync=self.sync,
            max_idle_ns=self.idle_ms * NS_PER_MS,
            payload_bounds=lambda mid: CODEC.payload_bounds(TLM, mid),
            largest_payload=CODEC.largest_payload(TLM),
            budget=UNLIMITED,
        )
        low, high = (0.02, 0.1) if self.fast_reconnect else (0.5, 5.0)
        self.transport = SerialTransport(
            spec,
            framing,
            framer,
            report=self.reports.append,
            link_state=lambda up, t, err: self.states.append((up, t, err)),
            max_packet_bytes=ser_corpus.CAP,
            reconnect_min_s=low,
            reconnect_max_s=high,
        )

    def receive(self, data: bytes, time_ns: int, source: str, orig_len: int, flags: int) -> None:
        assert source == "" and orig_len == len(data)
        self.frames.append((data, time_ns, flags))

    async def open(self, *, receiving: bool = True) -> None:
        await self.transport.open(self.receive if receiving else None)

    def data(self, flag: int = 0) -> list[bytes]:
        return [d for d, _, f in self.frames if f == flag]


async def run_with(timeout: float, body: Awaitable[None]) -> None:
    await asyncio.wait_for(body, timeout)


# -- ib configs ----------------------------------------------------------------------------


def serial_config(
    directory: Path,
    name: str,
    device: Path,
    acts_as: str,
    *,
    framing: str = "framing = { max_idle_ms = 50 }",
    serial_extra: str = "",
    extra: str = "",
) -> Path:
    """examples/mock.toml with its UDP link replaced by a serial one on ``device``."""
    text = (EXAMPLES / "mock.toml").read_text()
    text = text[: text.index("[[links]]")]
    text = text.replace('"ccdd/', f'"{EXAMPLES}/ccdd/').replace(
        '"headers/', f'"{EXAMPLES}/headers/'
    )
    text = text.replace('acts_as = "ground"', f'acts_as = "{acts_as}"')
    text += f"""[[links]]
name = "ground-fc"
from = "ground"
to = "fc"
transport = "serial"
serial = {{ device = "{device}", baud = 115200{serial_extra} }}
{framing}

[recording]
min_free_bytes = 0
{extra}
"""
    path = directory / name
    path.write_text(text)
    return path
