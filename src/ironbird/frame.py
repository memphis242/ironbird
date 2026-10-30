"""The frame: the unit the engine produces and every consumer shares.

A :class:`Frame` is one raw packet (or one run of garbage bytes) as it crossed one stream of one
link, stamped and numbered at ingest. It is built only by the engine's ingest path (and, in a
later step, the recording reader), after the inputs were normalised, so its invariants are
asserted here rather than checked: a violation is an ironbird bug, not bad input.

Decoding is never done here. The engine yields frames; a consumer pairs one with the packet it
decodes to as a :class:`Decoded`.

Frames are built once per received packet, so both types are tuples: immutable, built without
a Python-level ``__init__`` per field, and (for :class:`Frame`) validated by one assert-only
``__new__`` that does not exist under ``python -O``.

A frame's identity is its eight packet fields. The ninth, ``checked``, is a cache of what live
ingest established: it takes no part in equality, hashing, copying or pickling, and every way
of rebuilding a frame (``_replace``, ``_make``, ``copy``, unpickling) leaves it behind.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import IntEnum, IntFlag, StrEnum
from typing import Any, Final, NamedTuple, Self

from ironbird.ccdd.model import MessageKind
from ironbird.codec import DecodedPacket, Resolved

IDENTITY_FIELDS: Final = 8  # the packet fields of a frame; ``checked`` follows them
MAX_FRAME_BYTES: Final = 65535  # the largest max_packet_bytes the config accepts
MAX_STREAM_ID: Final = 0xFFFF
MAX_MID: Final = 0xFFFFFFFF  # MIDs are 32 bits (the CCDD and the header codec enforce it)
SERIAL_TRANSPORT: Final = "serial"

# Flag bits as plain ints, for the per-frame path (an ``IntFlag`` operation costs a Python call).
FLAG_HAS_MID: Final = 0x01  # mid was peeked from the profile's MID window
FLAG_TRUNCATED: Final = 0x02  # data cut at max_packet_bytes; orig_len is the size on the wire
FLAG_GARBAGE: Final = 0x04  # serial: bytes skipped while resynchronising, not a packet
FLAG_STALE: Final = 0x08  # serial: partial packet flushed by the inter-byte timeout
KNOWN_FLAG_BITS: Final = FLAG_HAS_MID | FLAG_TRUNCATED | FLAG_GARBAGE | FLAG_STALE


class Direction(StrEnum):
    RX = "rx"
    TX = "tx"


class FrameFlag(IntFlag):
    NONE = 0
    HAS_MID = FLAG_HAS_MID
    TRUNCATED = FLAG_TRUNCATED
    GARBAGE = FLAG_GARBAGE
    STALE = FLAG_STALE


class GapReason(IntEnum):
    """Why frames that belong in a capture are missing from it (the value is the recording's)."""

    KERNEL_DROP = 1  # the OS dropped datagrams before the transport read them
    QUEUE_DROP = 2  # a transport-side queue overflowed
    RECORDER_OVERRUN = 3  # the recorder's buffer was full; frames were not written
    LINK_DOWN = 4  # the link was down (serial reconnect)
    NOT_RECORDED = 5  # frames of a stream this recorder was not given


KNOWN_FLAGS: Final = FrameFlag.HAS_MID | FrameFlag.TRUNCATED | FrameFlag.GARBAGE | FrameFlag.STALE

# Every defined combination of flag bits, indexed by its integer value.
FLAGS_BY_BITS: Final[tuple[FrameFlag, ...]] = tuple(
    FrameFlag(bits) for bits in range(KNOWN_FLAG_BITS + 1)
)
assert KNOWN_FLAG_BITS == int(KNOWN_FLAGS) == len(FLAGS_BY_BITS) - 1


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


class _FrameFields(NamedTuple):
    seq: int  # u64, session-wide, strictly increasing from 1, assigned at ingest
    time_ns: int  # CLOCK_REALTIME taken in the transport callback (receive or send)
    stream: Stream
    source: str  # interned peer text ("" for serial)
    mid: int | None  # peeked, used only for routing and filtering, never trusted
    flags: FrameFlag
    orig_len: int  # == len(data) unless TRUNCATED
    data: bytes  # len(data) <= max_packet_bytes
    # What ingest's check established, so a live consumer need not repeat it. ``None`` for a
    # frame that failed the check and for any frame not built by live ingest (replay). A cache,
    # not part of the frame's identity; the codec trusts it only for the bytes it was made from.
    checked: Resolved | None = None


class Frame(_FrameFields):
    __slots__ = ()

    if __debug__:  # the invariants are asserts; without them the tuple constructor runs as is

        def __new__(
            cls,
            seq: int,
            time_ns: int,
            stream: Stream,
            source: str,
            mid: int | None,
            flags: FrameFlag,
            orig_len: int,
            data: bytes,
            checked: Resolved | None = None,
        ) -> Frame:
            bits, size = int(flags), len(data)
            assert seq >= 1 and time_ns >= 0
            assert not bits & ~KNOWN_FLAG_BITS, f"unknown frame flag bits {bits:#x}"
            assert bool(bits & FLAG_HAS_MID) == (mid is not None), "HAS_MID iff mid is set"
            assert mid is None or 0 <= mid <= MAX_MID
            assert size <= MAX_FRAME_BYTES
            assert orig_len >= size, "orig_len is the size on the wire, never below the data"
            assert bool(bits & FLAG_TRUNCATED) == (orig_len > size), "TRUNCATED iff cut"
            if bits & FLAG_GARBAGE:
                assert not bits & FLAG_STALE, "GARBAGE and STALE exclude each other"
                assert not bits & (FLAG_HAS_MID | FLAG_TRUNCATED), "garbage has no MID"
            if bits & (FLAG_GARBAGE | FLAG_STALE):
                assert stream.transport == SERIAL_TRANSPORT, "only a serial stream resynchronises"
            assert checked is None or checked.data is data, "a check token belongs to its data"
            return super().__new__(
                cls, seq, time_ns, stream, source, mid, flags, orig_len, data, checked
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Frame):
            return NotImplemented
        return self[:IDENTITY_FIELDS] == other[:IDENTITY_FIELDS]

    def __ne__(self, other: object) -> bool:
        if not isinstance(other, Frame):
            return NotImplemented
        return self[:IDENTITY_FIELDS] != other[:IDENTITY_FIELDS]

    def __hash__(self) -> int:
        return hash(self[:IDENTITY_FIELDS])

    def __reduce__(self) -> tuple[Any, ...]:
        return (Frame, tuple(self[:IDENTITY_FIELDS]))

    def __copy__(self) -> Self:
        return self.__class__(*self[:IDENTITY_FIELDS])

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        return self.__class__(*self[:IDENTITY_FIELDS])

    @classmethod
    def _make(cls, iterable: Iterable[Any]) -> Self:  # pyright: ignore[reportIncompatibleMethodOverride]
        """A frame from its fields, through the same checks as the constructor. Takes the eight
        packet fields (a ninth, the check token, is accepted and dropped)."""
        fields = tuple(iterable)
        if len(fields) not in (IDENTITY_FIELDS, IDENTITY_FIELDS + 1):
            raise TypeError(f"Expected {IDENTITY_FIELDS} or {IDENTITY_FIELDS + 1} fields")
        return cls(*fields[:IDENTITY_FIELDS])

    def _replace(self, **changes: Any) -> Self:  # pyright: ignore[reportIncompatibleMethodOverride]
        """A copy with ``changes`` applied, through the same checks as the constructor; the check
        token is dropped, because it described the bytes this frame held."""
        unknown = changes.keys() - set(self._fields)
        if unknown:
            raise ValueError(f"Got unexpected field names: {sorted(unknown)!r}")
        changes.pop("checked", None)
        values = {name: changes.get(name, getattr(self, name)) for name in self._fields[:-1]}
        return self.__class__(**values)


class Decoded(NamedTuple):
    """A frame paired with the packet it decoded to; live and replay share this shape."""

    frame: Frame
    packet: DecodedPacket
