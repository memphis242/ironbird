"""The frame: the unit the engine produces and every consumer shares.

A :class:`Frame` is one raw packet (or one run of garbage bytes) as it crossed one stream of one
link, stamped and numbered at ingest. It is built only by the engine's ingest path (and, in a
later step, the recording reader), after the inputs were normalised, so its invariants are
asserted here rather than checked: a violation is an ironbird bug, not bad input.

Decoding is never done here. The engine yields frames; a consumer pairs one with the packet it
decodes to as a :class:`Decoded`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntFlag, StrEnum
from typing import Final

from ironbird.ccdd.model import MessageKind
from ironbird.codec import DecodedPacket

MAX_FRAME_BYTES: Final = 65535  # the largest max_packet_bytes the config accepts
MAX_STREAM_ID: Final = 0xFFFF
MAX_MID: Final = 0xFFFFFFFF  # MIDs are 32 bits (the CCDD and the header codec enforce it)
SERIAL_TRANSPORT: Final = "serial"


class Direction(StrEnum):
    RX = "rx"
    TX = "tx"


class FrameFlag(IntFlag):
    NONE = 0
    HAS_MID = 0x01  # mid was peeked from the profile's MID window
    TRUNCATED = 0x02  # data cut at max_packet_bytes; orig_len is the size on the wire
    GARBAGE = 0x04  # serial: bytes skipped while resynchronising, not a packet
    STALE = 0x08  # serial: partial packet flushed by the inter-byte timeout


KNOWN_FLAGS: Final = FrameFlag.HAS_MID | FrameFlag.TRUNCATED | FrameFlag.GARBAGE | FrameFlag.STALE


@dataclass(frozen=True, slots=True)
class Stream:
    """One direction of one link, as seen by this process."""

    id: int  # u16, unique within a session (and its recording)
    link: str
    node: str  # node whose CCDD and profiles decode it (link.destination)
    kind: MessageKind  # what the bytes are: command or telemetry
    direction: Direction
    transport: str  # "udp" | "serial"
    endpoint: str  # sanitized display text: "udp 0.0.0.0:2234", "serial /dev/ttyUSB0"

    def __post_init__(self) -> None:
        assert 0 <= self.id <= MAX_STREAM_ID


@dataclass(frozen=True, slots=True)
class Frame:
    seq: int  # u64, session-wide, strictly increasing from 1, assigned at ingest
    time_ns: int  # CLOCK_REALTIME taken in the transport callback (receive or send)
    stream: Stream
    source: str  # interned peer text ("" for serial)
    mid: int | None  # peeked, used only for routing and filtering, never trusted
    flags: FrameFlag
    orig_len: int  # == len(data) unless TRUNCATED
    data: bytes  # len(data) <= max_packet_bytes

    def __post_init__(self) -> None:
        flags, size = self.flags, len(self.data)
        assert self.seq >= 1 and self.time_ns >= 0
        assert not int(flags) & ~int(KNOWN_FLAGS), f"unknown frame flag bits {int(flags):#x}"
        assert (FrameFlag.HAS_MID in flags) == (self.mid is not None), "HAS_MID iff mid is set"
        assert self.mid is None or 0 <= self.mid <= MAX_MID
        assert size <= MAX_FRAME_BYTES
        assert self.orig_len >= size, "orig_len is the size on the wire, never below the data"
        assert (FrameFlag.TRUNCATED in flags) == (self.orig_len > size), "TRUNCATED iff cut"
        if FrameFlag.GARBAGE in flags:
            assert FrameFlag.STALE not in flags, "GARBAGE and STALE exclude each other"
            assert not flags & (FrameFlag.HAS_MID | FrameFlag.TRUNCATED), "garbage has no MID"
        if flags & (FrameFlag.GARBAGE | FrameFlag.STALE):
            assert self.stream.transport == SERIAL_TRANSPORT, "only a serial stream resynchronises"


@dataclass(frozen=True, slots=True)
class Decoded:
    """A frame paired with the packet it decoded to; live and replay share this shape."""

    frame: Frame
    packet: DecodedPacket
