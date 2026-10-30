"""The ``.ibrec`` v1 byte layout (plan S3): structs, limits, and the pure record codecs.

All integers are little-endian and explicit. A file is a 24-byte preamble followed by records;
every record is an 12-byte envelope, a body, and zero padding up to the next multiple of 8. The
envelope's CRC-32 is seeded with the low 32 bits of the preamble's nonce, which the writer picks
after the traffic it will hold is unknowable, so frame data that looks like a record cannot
carry a valid CRC.

This module only encodes and decodes single records. Every decoder is a decode boundary for
untrusted bytes: it checks every length before it unpacks, and raises :class:`FormatError`
(never ``struct.error`` or ``IndexError``) for anything that is not a well-formed record body.
Rules that need the records before this one (stream ids defined, sequence increasing) belong to
:mod:`ironbird.recording.reader`.
"""

from __future__ import annotations

import itertools
import json
import struct
import zlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import IntEnum
from types import MappingProxyType
from typing import Any, Final, NamedTuple

from ironbird.ccdd.model import MessageKind
from ironbird.config import _IDENT_RE, _NAME_RE
from ironbird.frame import (
    MAX_FRAME_BYTES,
    MAX_STREAM_ID,
    Direction,
    GapReason,
    Stream,
)
from ironbird.sanitize import clean

MAGIC: Final = b"\x89IBR\r\n\x1a\n"
VERSION_MAJOR: Final = 1
VERSION_MINOR: Final = 0
PREAMBLE: Final = struct.Struct("<8sHHIQ")
PREAMBLE_SIZE: Final = 24
ENVELOPE: Final = struct.Struct("<IBBHI")  # body_len, type, flags, reserved, crc32
ENVELOPE_SIZE: Final = 12
ENVELOPE_HEAD: Final = struct.Struct("<IBBH")  # the first 8 bytes, which the CRC covers
ALIGN: Final = 8

FRAME_FIXED: Final = struct.Struct("<QQHHHHII")  # seq, time_ns, stream, source, flags, 0, mid, orig
FRAME_FIXED_SIZE: Final = 32
_GAP: Final = struct.Struct("<HBBIQQQQQ")
_CHUNK_FIXED: Final = struct.Struct("<QQQQQQQIIIHH")
_MARK: Final = struct.Struct("<QBxxx")
_TRAILER: Final = struct.Struct("<QQQ")
_STREAM_HEAD: Final = struct.Struct("<HBB")
_U8: Final = struct.Struct("<B")
_U16: Final = struct.Struct("<H")
_U32: Final = struct.Struct("<I")
_SOURCE_ID: Final = struct.Struct("<H")
CHUNK_FIXED_SIZE: Final = 72
GAP_SIZE: Final = 48
TRAILER_SIZE: Final = 24
MARK_FIXED_SIZE: Final = 12

assert PREAMBLE.size == PREAMBLE_SIZE == 24
assert ENVELOPE.size == ENVELOPE_SIZE == 12 and ENVELOPE_HEAD.size == 8
assert FRAME_FIXED.size == FRAME_FIXED_SIZE == 32
assert _GAP.size == GAP_SIZE == 48
assert _CHUNK_FIXED.size == CHUNK_FIXED_SIZE == 72
assert _MARK.size == MARK_FIXED_SIZE == 12
assert _TRAILER.size == TRAILER_SIZE == 24
assert ENVELOPE_SIZE + FRAME_FIXED_SIZE == 44  # the ring slot arithmetic of plan S6
assert PREAMBLE_SIZE % ALIGN == 0 and ENVELOPE_SIZE % 4 == 0

CRITICAL_LIMIT: Final = 0x80  # types below it must be understood; 0x80 and above are skippable
MAX_META_BODY: Final = 1024 * 1024
MAX_BLOB_CONTENT: Final = 8 * 1024 * 1024
MAX_BLOB_BODY: Final = MAX_BLOB_CONTENT + 1024
MAX_STREAM_BODY: Final = 4096
MAX_SOURCE_BODY: Final = 256
MAX_CHUNK_MIDS: Final = 256
MAX_CHUNK_BODY: Final = 2200
MAX_MARK_BODY: Final = 4096
MAX_INDEX_BODY: Final = 16 * 1024 * 1024
MAX_SKIPPABLE_BODY: Final = 16 * 1024 * 1024
MAX_INDEX_ENTRIES: Final = (MAX_INDEX_BODY - 4) // 8
MAX_SOURCES: Final = 4096  # source ids 0..4095; overflow uses OTHER_SOURCE
OTHER_SOURCE: Final = 0xFFFF
OTHER_SOURCE_TEXT: Final = "(other)"
NO_STREAM: Final = 0xFFFF
MAX_TEXT: Final = 256  # the longest text kept from an untrusted string, after sanitizing
MAX_META_DEPTH: Final = 16  # nesting of the META JSON (the writer's own is 3)
assert CHUNK_FIXED_SIZE + 8 * MAX_CHUNK_MIDS <= MAX_CHUNK_BODY
assert MAX_STREAM_ID + 1 == 0xFFFF == OTHER_SOURCE == NO_STREAM  # 0xFFFF names no stream

META_SCHEMA: Final = "ironbird-rec-meta/1"
CHUNK_FLAG_MID_OVERFLOW: Final = 0x0001
KNOWN_CHUNK_FLAGS: Final = CHUNK_FLAG_MID_OVERFLOW
MAX_U32: Final = 0xFFFFFFFF
MAX_U64: Final = 0xFFFFFFFFFFFFFFFF


class RecordType(IntEnum):
    META = 0x01
    BLOB = 0x02
    STREAM = 0x03
    SOURCE = 0x04
    FRAME = 0x05
    GAP = 0x06
    CHUNK = 0x07
    MARK = 0x08
    INDEX = 0x09
    TRAILER = 0x0A


KNOWN_TYPES: Final = frozenset(int(t) for t in RecordType)
# Records that carry no frame; the writer puts them at the start of a chunk.
HEAD_TYPES: Final = frozenset(
    int(t)
    for t in (
        RecordType.META,
        RecordType.BLOB,
        RecordType.STREAM,
        RecordType.SOURCE,
        RecordType.GAP,
        RecordType.MARK,
    )
)


class BlobKind(IntEnum):
    CCDD = 1
    COMMAND_PROFILE = 2
    TELEMETRY_PROFILE = 3


class MarkKind(IntEnum):
    START = 1
    STOP = 2
    SIGNAL = 3
    LINK_UP = 4
    LINK_DOWN = 5
    CLOCK_STEP = 6
    NOTE = 7


_FIXED_BODY: Final[Mapping[int, tuple[int, int]]] = MappingProxyType(
    {
        RecordType.META: (2, MAX_META_BODY),
        RecordType.BLOB: (1 + 1 + 1 + 32, MAX_BLOB_BODY),
        RecordType.STREAM: (_STREAM_HEAD.size + 3 + 2, MAX_STREAM_BODY),
        RecordType.SOURCE: (_SOURCE_ID.size, MAX_SOURCE_BODY),
        RecordType.GAP: (GAP_SIZE, GAP_SIZE),
        RecordType.CHUNK: (CHUNK_FIXED_SIZE, MAX_CHUNK_BODY),
        RecordType.MARK: (MARK_FIXED_SIZE, MAX_MARK_BODY),
        RecordType.INDEX: (4, MAX_INDEX_BODY),
        RecordType.TRAILER: (TRAILER_SIZE, TRAILER_SIZE),
    }
)


def body_limits(rtype: int, max_packet_bytes: int) -> tuple[int, int]:
    """The ``(minimum, maximum)`` body length of a record type; a type this version does not
    know gets the generous skippable bound (a critical one is refused after its CRC passes)."""
    if rtype == RecordType.FRAME:
        return FRAME_FIXED_SIZE, FRAME_FIXED_SIZE + max_packet_bytes
    return _FIXED_BODY.get(rtype, (0, MAX_SKIPPABLE_BODY))


def record_size(body_len: int) -> int:
    """Bytes a record occupies in the file: envelope, body and padding to a multiple of 8."""
    return (ENVELOPE_SIZE + body_len + ALIGN - 1) & ~(ALIGN - 1)


class FormatError(Exception):
    """A record body that breaks the format; the reader turns it into damage."""


def seed_of(nonce: int) -> int:
    """The CRC seed: the low 32 bits of the nonce."""
    return nonce & MAX_U32


def encode_preamble(nonce: int) -> bytes:
    return PREAMBLE.pack(MAGIC, VERSION_MAJOR, VERSION_MINOR, 0, nonce)


def encode_record(rtype: int, body: bytes | bytearray | memoryview, seed: int) -> bytes:
    """A whole record (envelope, body, padding) with its nonce-seeded CRC."""
    size = len(body)
    head = ENVELOPE_HEAD.pack(size, rtype, 0, 0)
    crc = zlib.crc32(body, zlib.crc32(head, seed))
    pad = -(ENVELOPE_SIZE + size) & (ALIGN - 1)
    return b"".join((ENVELOPE.pack(size, rtype, 0, 0, crc), body, bytes(pad)))


# -- META --


@dataclass(frozen=True, slots=True)
class Meta:
    data: Mapping[str, Any]  # the decoded JSON object; strings are untrusted
    max_packet_bytes: int
    part: int
    previous_last_seq: int


def encode_meta(data: Mapping[str, Any]) -> bytes:
    body = json.dumps(dict(data), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    raw = body.encode("ascii")
    assert len(raw) <= MAX_META_BODY, "the writer keeps META under its limit"
    return raw


def _int_field(data: Mapping[str, Any], key: str, low: int, high: int, default: int | None) -> int:
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise FormatError(f"META {key} must be an integer in {low}..{high}")
    return value


def _depth(root: Any) -> int:
    """Nesting depth of decoded JSON, measured without recursion (the data is untrusted)."""
    deepest = 0
    pending: list[tuple[Any, int]] = [(root, 1)]
    while pending:
        value, level = pending.pop()
        if isinstance(value, dict):
            children: Any = value.values()
        elif isinstance(value, list):
            children = value
        else:
            continue
        deepest = max(deepest, level)
        if deepest > MAX_META_DEPTH:
            return deepest
        pending.extend((child, level + 1) for child in children)
    return deepest


def decode_meta(body: bytes | memoryview) -> Meta:
    try:
        data = json.loads(bytes(body).decode("utf-8"))
    except (ValueError, RecursionError) as exc:
        raise FormatError(f"META is not valid JSON ({type(exc).__name__})") from exc
    if not isinstance(data, dict):
        raise FormatError("META is not a JSON object")
    if _depth(data) > MAX_META_DEPTH:
        raise FormatError(f"META is nested deeper than {MAX_META_DEPTH} levels")
    if data.get("schema") != META_SCHEMA:
        raise FormatError("META schema is not " + META_SCHEMA)
    return Meta(
        MappingProxyType(data),
        _int_field(data, "max_packet_bytes", 1, MAX_FRAME_BYTES, None),
        _int_field(data, "part", 1, MAX_U32, 1),
        _int_field(data, "previous_last_seq", 0, MAX_U64, 0),
    )


# -- BLOB --


class Blob(NamedTuple):
    kind: BlobKind
    node: str
    adapter: str
    sha256: bytes
    content: bytes


def encode_blob(kind: BlobKind, node: str, adapter: str, sha256: bytes, content: bytes) -> bytes:
    node_raw, adapter_raw = node.encode("ascii"), adapter.encode("ascii")
    assert len(sha256) == 32 and len(node_raw) < 256 and len(adapter_raw) < 256
    assert len(content) <= MAX_BLOB_CONTENT, "the writer refuses larger definitions"
    return b"".join(
        (
            _U8.pack(kind),
            _U8.pack(len(node_raw)),
            node_raw,
            _U8.pack(len(adapter_raw)),
            adapter_raw,
            sha256,
            content,
        )
    )


def _take8(body: bytes | memoryview, pos: int) -> tuple[bytes, int]:
    if pos >= len(body):
        raise FormatError("a length-prefixed string runs past the record")
    size = body[pos]
    end = pos + 1 + size
    if end > len(body):
        raise FormatError("a length-prefixed string runs past the record")
    return bytes(body[pos + 1 : end]), end


def _ascii(raw: bytes, pattern: Any, what: str) -> str:
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise FormatError(f"{what} is not ASCII") from exc
    if not pattern.fullmatch(text):
        raise FormatError(f"{what} is not a valid name")
    return text


def decode_blob(body: bytes | memoryview, sha256: Any) -> Blob:
    """Decode a BLOB; ``sha256`` is ``hashlib.sha256`` (injected so the digest is the caller's)."""
    try:
        kind = BlobKind(body[0])
    except (ValueError, IndexError) as exc:
        raise FormatError("BLOB kind is not 1, 2 or 3") from exc
    node_raw, pos = _take8(body, 1)
    adapter_raw, pos = _take8(body, pos)
    node = _ascii(node_raw, _NAME_RE, "BLOB node")
    adapter = _ascii(adapter_raw, _IDENT_RE, "BLOB adapter")
    if len(body) < pos + 32:
        raise FormatError("BLOB has no digest")
    digest = bytes(body[pos : pos + 32])
    content = bytes(body[pos + 32 :])
    if len(content) > MAX_BLOB_CONTENT:
        raise FormatError("BLOB content is larger than 8 MiB")
    if sha256(content).digest() != digest:
        raise FormatError("BLOB content does not match its SHA-256")
    return Blob(kind, node, adapter, digest, content)


# -- STREAM and SOURCE --

_KIND_CODES: Final[Mapping[MessageKind, int]] = MappingProxyType(
    {MessageKind.COMMAND: 1, MessageKind.TELEMETRY: 2}
)
_KINDS_BY_CODE: Final[Mapping[int, MessageKind]] = MappingProxyType(
    {code: kind for kind, code in _KIND_CODES.items()}
)
_DIRECTION_CODES: Final[Mapping[Direction, int]] = MappingProxyType(
    {Direction.RX: 1, Direction.TX: 2}
)
_DIRECTIONS_BY_CODE: Final[Mapping[int, Direction]] = MappingProxyType(
    {code: d for d, code in _DIRECTION_CODES.items()}
)
READABLE_TRANSPORTS: Final = frozenset({"udp", "serial"})


def encode_stream(stream: Stream) -> bytes:
    assert stream.id <= MAX_STREAM_ID, "a stream id never reaches the id that means no stream"
    link, node = stream.link.encode("ascii"), stream.node.encode("ascii")
    transport = stream.transport.encode("ascii")
    endpoint = stream.endpoint.encode("utf-8")[: MAX_TEXT * 2]
    # Cut at a character boundary if the cut split one; the reader sanitizes what is left.
    endpoint = endpoint.decode("utf-8", "ignore").encode("utf-8")
    assert max(len(link), len(node), len(transport)) < 256
    return b"".join(
        (
            _STREAM_HEAD.pack(
                stream.id, _KIND_CODES[stream.kind], _DIRECTION_CODES[stream.direction]
            ),
            _U8.pack(len(link)),
            link,
            _U8.pack(len(node)),
            node,
            _U8.pack(len(transport)),
            transport,
            _U16.pack(len(endpoint)),
            endpoint,
        )
    )


def decode_stream(body: bytes | memoryview) -> Stream:
    if len(body) < _STREAM_HEAD.size:
        raise FormatError("STREAM is too short")
    sid, kind_code, dir_code = _STREAM_HEAD.unpack_from(body, 0)
    if sid > MAX_STREAM_ID:
        raise FormatError(f"STREAM id {sid} is reserved (it names no stream in a GAP)")
    kind, direction = _KINDS_BY_CODE.get(kind_code), _DIRECTIONS_BY_CODE.get(dir_code)
    if kind is None or direction is None:
        raise FormatError("STREAM kind or direction is not defined")
    link_raw, pos = _take8(body, _STREAM_HEAD.size)
    node_raw, pos = _take8(body, pos)
    transport_raw, pos = _take8(body, pos)
    if pos + 2 > len(body):
        raise FormatError("STREAM has no endpoint")
    (endpoint_len,) = _U16.unpack_from(body, pos)
    pos += 2
    if pos + endpoint_len != len(body):
        raise FormatError("STREAM endpoint length disagrees with the record")
    link = _ascii(link_raw, _NAME_RE, "STREAM link")
    node = _ascii(node_raw, _NAME_RE, "STREAM node")
    transport = transport_raw.decode("ascii", "replace")
    if transport not in READABLE_TRANSPORTS:
        raise FormatError("STREAM transport is not udp or serial")
    try:
        endpoint = bytes(body[pos:]).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise FormatError("STREAM endpoint is not UTF-8") from exc
    return Stream(sid, link, node, kind, direction, transport, clean(endpoint, MAX_TEXT))


def encode_source(source_id: int, text: str) -> bytes:
    raw = text.encode("utf-8")[: MAX_SOURCE_BODY - _SOURCE_ID.size]
    raw = raw.decode("utf-8", "ignore").encode("utf-8")
    return _SOURCE_ID.pack(source_id) + raw


def decode_source(body: bytes | memoryview) -> tuple[int, str]:
    (source_id,) = _SOURCE_ID.unpack_from(body, 0)
    if source_id >= MAX_SOURCES:
        raise FormatError("SOURCE id is outside the table")
    try:
        text = bytes(body[_SOURCE_ID.size :]).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise FormatError("SOURCE text is not UTF-8") from exc
    return source_id, clean(text, MAX_TEXT)


# -- GAP, MARK --


class Gap(NamedTuple):
    stream: int  # stream id, or NO_STREAM when the loss is not tied to one
    reason: GapReason
    lost: int  # frames missing
    first_ns: int
    last_ns: int
    first_seq: int
    last_seq: int


def encode_gap(gap: Gap) -> bytes:
    return _GAP.pack(
        gap.stream,
        gap.reason,
        0,
        0,
        gap.lost,
        gap.first_ns,
        gap.last_ns,
        gap.first_seq,
        gap.last_seq,
    )


def decode_gap(body: bytes | memoryview) -> Gap:
    stream, reason, zero8, zero32, count, first_ns, last_ns, first_seq, last_seq = _GAP.unpack_from(
        body, 0
    )
    if zero8 or zero32:
        raise FormatError("GAP reserved fields are not zero")
    try:
        why = GapReason(reason)
    except ValueError as exc:
        raise FormatError(f"GAP reason {reason} is not defined") from exc
    if count < 1:
        raise FormatError("GAP counts no frames")
    if first_seq > last_seq:
        raise FormatError("GAP sequence range runs backwards")
    return Gap(stream, why, count, first_ns, last_ns, first_seq, last_seq)


class Mark(NamedTuple):
    time_ns: int
    kind: int  # a MarkKind, or a kind a later minor version defined
    text: str


def encode_mark(time_ns: int, kind: MarkKind, text: str) -> bytes:
    raw = text.encode("utf-8")[: MAX_MARK_BODY - MARK_FIXED_SIZE]
    raw = raw.decode("utf-8", "ignore").encode("utf-8")
    return _MARK.pack(time_ns, kind) + raw


def decode_mark(body: bytes | memoryview) -> Mark:
    time_ns, kind = _MARK.unpack_from(body, 0)
    if bytes(body[9:MARK_FIXED_SIZE]) != b"\x00\x00\x00":
        raise FormatError("MARK reserved bytes are not zero")
    try:
        text = bytes(body[MARK_FIXED_SIZE:]).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise FormatError("MARK text is not UTF-8") from exc
    return Mark(time_ns, kind, clean(text, MAX_TEXT))


# -- CHUNK, INDEX, TRAILER --


class Chunk(NamedTuple):
    chunk_start: int
    first_seq: int
    last_seq: int
    min_time_ns: int
    max_time_ns: int
    first_mono_ns: int
    last_mono_ns: int
    frames: int
    bytes: int  # file bytes of the chunk's FRAME records
    frames_without_mid: int
    flags: int
    mids: Sequence[tuple[int, int]]  # (mid, frames), sorted by MID

    @property
    def mid_overflow(self) -> bool:
        return bool(self.flags & CHUNK_FLAG_MID_OVERFLOW)


def encode_chunk(chunk: Chunk) -> bytes:
    assert len(chunk.mids) <= MAX_CHUNK_MIDS
    head = _CHUNK_FIXED.pack(
        chunk.chunk_start,
        chunk.first_seq,
        chunk.last_seq,
        chunk.min_time_ns,
        chunk.max_time_ns,
        chunk.first_mono_ns,
        chunk.last_mono_ns,
        chunk.frames,
        chunk.bytes,
        chunk.frames_without_mid,
        chunk.flags,
        len(chunk.mids),
    )
    table = b"".join(struct.pack("<II", mid, count) for mid, count in chunk.mids)
    return head + table


def decode_chunk(body: bytes | memoryview) -> Chunk:
    (
        start,
        first_seq,
        last_seq,
        min_time,
        max_time,
        first_mono,
        last_mono,
        frames,
        size,
        no_mid,
        flags,
        count,
    ) = _CHUNK_FIXED.unpack_from(body, 0)
    if count > MAX_CHUNK_MIDS or len(body) != CHUNK_FIXED_SIZE + 8 * count:
        raise FormatError("CHUNK MID table length disagrees with the record")
    if flags & ~KNOWN_CHUNK_FLAGS:
        raise FormatError("CHUNK has unknown flag bits")
    mids: list[tuple[int, int]] = []
    previous = -1
    total = 0
    for index in range(count):
        mid, per_mid = struct.unpack_from("<II", body, CHUNK_FIXED_SIZE + 8 * index)
        if mid <= previous:
            raise FormatError("CHUNK MID table is not sorted")
        previous = mid
        total += per_mid
        mids.append((mid, per_mid))
    if total + no_mid > frames:
        raise FormatError("CHUNK counts more frames than it holds")
    if not flags & CHUNK_FLAG_MID_OVERFLOW and total + no_mid != frames:
        raise FormatError("CHUNK MID counts do not add up to its frames")
    if frames and (first_seq > last_seq or min_time > max_time):
        raise FormatError("CHUNK bounds run backwards")
    return Chunk(
        start, first_seq, last_seq, min_time, max_time, first_mono, last_mono, frames, size,
        no_mid, flags, tuple(mids),
    )  # fmt: skip


def encode_index(offsets: Sequence[int]) -> bytes:
    assert len(offsets) <= MAX_INDEX_ENTRIES
    return _U32.pack(len(offsets)) + struct.pack(f"<{len(offsets)}Q", *offsets)


def decode_index(body: bytes | memoryview) -> tuple[int, ...]:
    (count,) = _U32.unpack_from(body, 0)
    if len(body) != 4 + 8 * count:
        raise FormatError("INDEX length disagrees with its entry count")
    offsets = struct.unpack_from(f"<{count}Q", body, 4)
    for earlier, later in itertools.pairwise(offsets):
        if later <= earlier:
            raise FormatError("INDEX offsets do not increase")
    return offsets


class Trailer(NamedTuple):
    index_offset: int  # 0: the file has no index
    frame_count: int
    last_seq: int


def encode_trailer(trailer: Trailer) -> bytes:
    return _TRAILER.pack(*trailer)


def decode_trailer(body: bytes | memoryview) -> Trailer:
    return Trailer(*_TRAILER.unpack_from(body, 0))


def describe_type(rtype: int) -> str:
    try:
        return RecordType(rtype).name
    except ValueError:
        return f"type {rtype:#04x}"
