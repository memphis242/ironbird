"""The wire format of the export worker pool (plan 2m): every message between the parent and a
worker, in both directions, is one of these frames, sent as one message on the private pipe.

A worker decodes data read from untrusted recordings, so what it sends back is treated as
untrusted too: it is parsed only by :func:`decode`, which returns a typed value or raises
:class:`FrameError` (``IB-EXP-003``), and then checked against the job it answers by
:func:`check_reply`, which refuses anything the parent could not apply as it is. Nothing in a
frame is ever executed or turned into an object of a type the frame names: the layouts are fixed
here, numbers are fixed width, enums travel by explicit code tables, and every length is checked
against a hard cap before anything is read or allocated for it.

**The frame.** A 12-byte header, then the body:

====== ======= ========================================================================
offset size    field
====== ======= ========================================================================
0      4       magic ``IBPF``
4      1       version (:data:`VERSION`); any other is refused
5      1       type: 1 definitions, 2 job, 3 outcome, 4 fault; any other is refused
6      2       reserved, zero
8      4       body length (little endian), which must be the rest of the message
====== ======= ========================================================================

The body is the fields of the type in a fixed order (see the ``_put_*``/``_take_*`` pairs), all
little endian: fixed-width integers packed by one :class:`struct.Struct` per group, flag bytes
whose undefined bits must be zero, and length-prefixed blocks (a ``u32`` length, then the bytes)
for text, raw bytes and lists (a ``u32`` count, then the elements). Text is UTF-8 and must be
valid; paths and command-line text (the filter, aliases, message names) are the operating
system's bytes (:func:`os.fsencode`), as Python itself carries them. A value that is absent has
its flag clear and its field zero. The encoding is canonical: a frame that :func:`decode`
accepts is exactly what :func:`encode` writes for the value it returns (sets in sorted order, no
duplicate keys, paths in normal form, META in the one JSON form), which the property tests and
the fuzz harness check.

**Bounds.** A frame is at most :data:`MAX_INIT_BYTES`, :data:`MAX_JOB_BYTES` or
:data:`MAX_REPLY_BYTES` long by type (the receiver also passes the cap to the pipe, which refuses a
longer message before reading it), every list has its own cap, and one frame decodes to at most
:data:`MAX_ITEMS` strings and list elements in all, so memory is bounded by a small multiple of
the frame's length whatever it claims. The encoder applies the same caps, so a value it cannot
send is an error at the sender, not a frame the receiver refuses.
"""

from __future__ import annotations

import array
import dataclasses
import itertools
import json
import os
import struct
import sys
from collections import Counter
from collections.abc import Callable, Collection, Mapping
from enum import IntEnum
from pathlib import Path
from typing import Any, Final, NoReturn

from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec
from ironbird.errors import Code, IronbirdError
from ironbird.frame import MAX_STREAM_ID, Direction, Stream
from ironbird.recording import format as fmt
from ironbird.recording.defs import MAX_DEFINED_NODES, MAX_HEAD_BYTES
from ironbird.recording.export import (
    NO_LINE,
    ExportFormat,
    ExportResult,
    TableKey,
    table_columns,
)
from ironbird.recording.player import Ending
from ironbird.recording.poolmsg import (
    Block,
    ConfigDefinitions,
    Definitions,
    EmbeddedDefinitions,
    Fault,
    Job,
    Outcome,
    Seed,
    Shape,
)
from ironbird.recording.reader import Damage
from ironbird.units import MIB

MAGIC: Final = b"IBPF"
VERSION: Final = 1
HEADER: Final = struct.Struct("<4sBBHI")  # magic, version, type, reserved, body length


class Kind(IntEnum):
    """The type of a frame."""

    DEFINITIONS = 1  # parent to worker, first: what the worker decodes with
    JOB = 2  # parent to worker: a range to decode
    OUTCOME = 3  # worker to parent: what a job made
    FAULT = 4  # worker to parent: a message it could not read


# -- the caps ---------------------------------------------------------------------------------

MAX_INIT_BYTES: Final = 128 * MIB  # the embedded definitions: at most the head of a recording
MAX_JOB_BYTES: Final = 64 * MIB  # a job: a seed of every stream and source, and the shape
MAX_REPLY_BYTES: Final = 128 * MIB  # an outcome: rows up to the text cap, one row past it
MAX_NAME_BYTES: Final = 64 * 1024  # one name, reason, filter, endpoint or summary
MAX_DIAG_BYTES: Final = 1 * MIB  # error details and tracebacks (the worker keeps the end)
MAX_META_BYTES: Final = 8 * MIB  # META as ASCII JSON: its 1 MiB, every character escaped
MAX_CODE_BYTES: Final = 32  # an error code ("IB-EXP-003")
MAX_NAMES: Final = 1 << 16  # names in one list: links, aliases, messages, nodes, columns
MAX_BLOBS: Final = 3 * MAX_DEFINED_NODES  # a CCDD and two header profiles per node
MAX_SEED_STREAMS: Final = MAX_STREAM_ID + 1
MAX_SEED_SOURCES: Final = 1 << 16
MAX_TEXT_CAP: Final = 16 * MIB  # the largest ``Job.text_cap`` (characters)
MAX_EVENT_CAP: Final = 1 << 16  # the largest ``Job.event_cap``
MAX_NOTES: Final = MAX_EVENT_CAP + 1  # damage, gap marks and definitions in one outcome
MAX_BLOCKS: Final = 1 << 16  # tables in one outcome
MAX_IDS: Final = 1 << 16  # stream or source ids a range found in its seed
MAX_ITEMS: Final = 1 << 22  # strings and list elements one frame decodes to, in all

# Rows of up to the text cap, four bytes a character at most, leave room for the row that
# passes the cap, the kinds of the rows and the order of the lines.
assert 4 * MAX_TEXT_CAP <= MAX_REPLY_BYTES // 2
assert MAX_INIT_BYTES >= MAX_HEAD_BYTES + fmt.MAX_BLOB_BODY + MAX_META_BYTES
assert fmt.MAX_META_BODY * 6 <= MAX_META_BYTES  # "\uXXXX" for each byte at worst

_U8: Final = struct.Struct("<B")
_U16: Final = struct.Struct("<H")
_U32: Final = struct.Struct("<I")
_U64: Final = struct.Struct("<Q")
_STREAM: Final = struct.Struct("<HBB")  # id, kind, direction
_SOURCE: Final = struct.Struct("<H")
_SEED: Final = struct.Struct("<Qq")  # last_seq, work
_SHAPE: Final = struct.Struct("<BB")  # format, flags
# index, guess, stop, flags, start_ns, end_ns, device, inode, text_cap, event_cap; the two times
# are signed 16-byte integers (see ``_TIME_BYTES``)
_JOB: Final = struct.Struct("<QQQB16s16sQQQI")
_TIME_BYTES: Final = 16
# Every instant ``--from``/``--to`` accept (years 1 to 9999 in nanoseconds, a bound the recording's
# u64 times never reach) fits a signed time, so a bound is never a reason to refuse a job.
assert 1 << (8 * _TIME_BYTES - 1) > 253_402_300_800 * 10**9
# index, start, end, first_seq, last_seq, work, scanned, frames_read, frames_in_range, flags
_OUTCOME: Final = struct.Struct("<QQQQQqqQQH")
_DAMAGE: Final = struct.Struct("<QQQQB")  # offset, span, resumed_at, rows before, flags
_GAP_MARK: Final = struct.Struct("<QQQ")  # rows before, gaps, frames lost
# rows, decoded, malformed, unknown, garbage, undecoded, skipped_no_definitions, not_written,
# files, ending
_COUNTS: Final = struct.Struct("<QQQQQQQQQB")
_BLOCK: Final = struct.Struct("<BQI")  # flags, rows, handle
_SHA256_BYTES: Final = 32

# Flag bits.
_SHAPE_HEADER_FIELDS, _SHAPE_RAW_ENUMS, _SHAPE_DIRECTORY = 0x01, 0x02, 0x04
_SHAPE_SKIP_DAMAGE, _SHAPE_CATEGORIES, _SHAPE_WHERE, _SHAPE_ALIASES = 0x08, 0x10, 0x20, 0x40
_SHAPE_FLAGS: Final = 0x7F
_JOB_STOP, _JOB_EXACT, _JOB_START_NS, _JOB_END_NS = 0x01, 0x02, 0x04, 0x08
_JOB_FLAGS: Final = 0x0F
_OUT_START, _OUT_REACHED_STOP, _OUT_FIRST_SEQ, _OUT_TRAILER = 0x001, 0x002, 0x004, 0x008
_OUT_PARTIAL, _OUT_MOVED, _OUT_FAILURE, _OUT_CRASH = 0x010, 0x020, 0x040, 0x080
_OUT_CATEGORIES, _OUT_ORDER = 0x100, 0x200
_OUT_FLAGS: Final = 0x3FF
_DAMAGE_TORN, _DAMAGE_RESUMED, _DAMAGE_TRAILING, _DAMAGE_INCOMPLETE = 0x01, 0x02, 0x04, 0x08
_DAMAGE_FLAGS: Final = 0x0F
_BLOCK_KEY, _BLOCK_MALFORMED = 0x01, 0x02
_BLOCK_FLAGS: Final = 0x03

# Code tables: the numbers are the wire's, never reused.
_FORMATS: Final = {ExportFormat.CSV: 1, ExportFormat.TEXT: 2, ExportFormat.JSONL: 3}
_KINDS: Final = {MessageKind.COMMAND: 1, MessageKind.TELEMETRY: 2}
_DIRECTIONS: Final = {Direction.RX: 1, Direction.TX: 2}
_ENDINGS: Final = {Ending.DONE: 1, Ending.COUNT: 2, Ending.OUTPUT: 3, Ending.DAMAGE: 4}
_BLOB_KINDS: Final = {
    fmt.BlobKind.CCDD: 1,
    fmt.BlobKind.COMMAND_PROFILE: 2,
    fmt.BlobKind.TELEMETRY_PROFILE: 3,
}
_DEFS_EMBEDDED, _DEFS_CONFIG = 1, 2
_DEF_STREAM, _DEF_SOURCE = 1, 2
_CODES: Final = {code.value: code for code in Code}  # an error code travels as its stable text


def _inverse(table: dict[Any, int]) -> dict[int, Any]:
    inverse = {number: value for value, number in table.items()}
    assert len(inverse) == len(table) and 0 not in inverse, "a code table is one to one, from 1"
    return inverse


_FORMATS_BY_CODE: Final = _inverse(_FORMATS)
_KINDS_BY_CODE: Final = _inverse(_KINDS)
_DIRECTIONS_BY_CODE: Final = _inverse(_DIRECTIONS)
_ENDINGS_BY_CODE: Final = _inverse(_ENDINGS)
_BLOB_KINDS_BY_CODE: Final = _inverse(_BLOB_KINDS)

# Every value of every enum has a code, and every field of every message is written: a field
# or an enum value added to a message type fails here until the format carries it.
assert set(_FORMATS) == set(ExportFormat) and set(_KINDS) == set(MessageKind)
assert set(_DIRECTIONS) == set(Direction) and set(_ENDINGS) == set(Ending)
assert set(_BLOB_KINDS) == set(fmt.BlobKind)


def _names(cls: Any) -> tuple[str, ...]:
    return tuple(f.name for f in dataclasses.fields(cls))


assert _names(Stream) == ("id", "link", "node", "kind", "direction", "transport", "endpoint")
assert _names(Seed) == ("streams", "sources", "last_seq", "work")
assert _names(Shape) == (
    "fmt",
    "where",
    "links",
    "aliases",
    "messages",
    "header_fields",
    "raw_enums",
    "directory",
    "skip_damage",
    "categories",
)
assert _names(Job) == (
    "index",
    "path",
    "guess",
    "stop",
    "exact",
    "seed",
    "start_ns",
    "end_ns",
    "shape",
    "identity",
    "text_cap",
    "event_cap",
)
assert _names(Damage) == (
    "offset",
    "span",
    "torn",
    "reason",
    "resumed_at",
    "trailing",
    "incomplete",
)
assert _names(ExportResult) == (
    "rows",
    "decoded",
    "malformed",
    "unknown",
    "garbage",
    "undecoded",
    "skipped_no_definitions",
    "not_written",
    "files",
    "ending",
    "rejected",
    "per_table",
)
assert _names(Block) == ("key", "malformed", "rows", "text", "handle")
assert _names(Outcome) == (
    "index",
    "start",
    "end",
    "reached_stop",
    "first_seq",
    "last_seq",
    "definitions",
    "used_streams",
    "used_sources",
    "work",
    "scanned",
    "trailer",
    "seen",  # the parent's own note: not sent
    "partial",
    "moved",
    "damage",
    "damage_rows",
    "failure",
    "crash",
    "frames_read",
    "frames_in_range",
    "gap_marks",
    "counts",
    "blocks",
    "categories",
    "order",
)
assert fmt.Blob._fields == ("kind", "node", "adapter", "sha256", "content")
assert _names(EmbeddedDefinitions) == (
    "path",
    "blobs",
    "problem",
    "nodes",
    "meta",
    "max_packet_bytes",
    "hint",
)
assert _names(ConfigDefinitions) == (
    "config_path",
    "config_sha256",
    "nodes",
    "max_packet_bytes",
    "command",
    "fingerprint",
)
assert _names(Fault) == ("reason",)
assert array.array("H").itemsize == 2 and array.array("I").itemsize == 4
_SWAP: Final = sys.byteorder != "little"  # the wire is little endian

# The kinds of a row (``CAT_*``, with and without ``NO_LINE``); anything else is refused.
_ROW_KINDS: Final = 5
_VALID_KIND_BYTES: Final = bytes([*range(_ROW_KINDS), *(NO_LINE | k for k in range(_ROW_KINDS))])
_NO_LINE_BYTES: Final = tuple(bytes([NO_LINE | k]) for k in range(_ROW_KINDS))

Message = EmbeddedDefinitions | ConfigDefinitions | Job | Outcome | Fault


class FrameError(IronbirdError):
    """A pool message that is not one this version writes, or a value that cannot be written:
    why, and the byte of the message where it was found. ``sending``: the value is one the
    sender cannot write (``offset`` is the length written so far), not a message that was read."""

    def __init__(self, reason: str, offset: int = 0, *, sending: bool = False) -> None:
        super().__init__(
            Code.EXP_WORKER,
            f"a message for the export pool cannot be sent: {reason}"
            if sending
            else f"a message of the export pool is malformed at byte {offset}: {reason}",
        )
        self.reason = reason
        self.offset = offset


# -- writing ----------------------------------------------------------------------------------


class _Out:
    """The parts of a frame being written, and their length so far against the cap."""

    __slots__ = ("cap", "items", "parts", "size")

    def __init__(self, cap: int) -> None:
        self.parts: list[bytes] = [b""]  # the header goes first, when the length is known
        self.size = 0
        self.cap = cap
        self.items = MAX_ITEMS  # spent exactly as the reader spends it (see ``_In``)

    def put(self, data: bytes) -> None:
        self.parts.append(data)
        self.size += len(data)
        if self.size > self.cap:
            self.fail(f"the message is longer than {self.cap} bytes")

    def pack(self, layout: struct.Struct, *values: Any) -> None:
        self.put(layout.pack(*values))

    def fail(self, reason: str) -> NoReturn:
        raise FrameError(reason, self.size, sending=True)

    def spend(self, n: int, what: str) -> None:
        self.items -= n
        if self.items < 0:
            self.fail(f"{what}: the message holds more than {MAX_ITEMS} items")

    def count(self, n: int, cap: int, what: str) -> None:
        if n > cap:
            self.fail(f"{what}: {n} is more than {cap}")
        self.spend(n, what)
        self.put(_U32.pack(n))

    def blob(self, data: bytes, cap: int, what: str) -> None:
        if len(data) > cap:
            self.fail(f"{what} is longer than {cap} bytes")
        self.put(_U32.pack(len(data)))
        if data:
            self.put(data)

    def text(self, value: str, cap: int, what: str) -> None:
        try:
            data = value.encode("utf-8")
        except UnicodeEncodeError:
            self.fail(f"{what} is not valid Unicode")
        self.spend(1, what)
        self.blob(data, cap, what)

    def os_text(self, value: str, cap: int, what: str) -> None:
        try:
            data = os.fsencode(value)
        except UnicodeEncodeError:
            self.fail(f"{what} cannot be given to the operating system")
        self.spend(1, what)
        self.blob(data, cap, what)

    def path(self, value: Path, what: str) -> None:
        self.os_text(str(value), MAX_NAME_BYTES, what)

    def finish(self, kind: Kind) -> bytes:
        self.parts[0] = HEADER.pack(MAGIC, VERSION, kind, 0, self.size)
        return b"".join(self.parts)


def _flag(condition: bool, bit: int) -> int:
    return bit if condition else 0


def _put_stream(out: _Out, stream: Stream) -> None:
    out.pack(_STREAM, stream.id, _KINDS[stream.kind], _DIRECTIONS[stream.direction])
    out.text(stream.link, MAX_NAME_BYTES, "a stream's link")
    out.text(stream.node, MAX_NAME_BYTES, "a stream's node")
    out.text(stream.transport, MAX_NAME_BYTES, "a stream's transport")
    out.text(stream.endpoint, MAX_NAME_BYTES, "a stream's endpoint")


def _put_names(out: _Out, names: tuple[str, ...], what: str) -> None:
    out.count(len(names), MAX_NAMES, what)
    for name in names:
        out.text(name, MAX_NAME_BYTES, what)


def _put_ids(out: _Out, ids: list[int], what: str) -> None:
    packed = array.array("H", ids)
    if _SWAP:  # pragma: no cover - a big-endian host
        packed.byteswap()
    out.count(len(ids), MAX_IDS, what)
    out.put(packed.tobytes())


def _put_definitions(out: _Out, definitions: Definitions) -> None:
    match definitions:
        case EmbeddedDefinitions():
            out.pack(_U8, _DEFS_EMBEDDED)
            out.path(definitions.path, "the recording's path")
            out.count(len(definitions.blobs), MAX_BLOBS, "embedded definitions")
            for key, blob in definitions.blobs.items():
                if key != (blob.node, blob.kind) or len(blob.sha256) != _SHA256_BYTES:
                    out.fail("an embedded definition is not filed under its own node and kind")
                out.pack(_U8, _BLOB_KINDS[blob.kind])
                out.text(blob.node, MAX_NAME_BYTES, "an embedded definition's node")
                out.text(blob.adapter, MAX_NAME_BYTES, "an embedded definition's adapter")
                out.put(blob.sha256)
                out.blob(blob.content, fmt.MAX_BLOB_CONTENT, "an embedded definition")
            out.text(definitions.problem, MAX_NAME_BYTES, "the definitions' problem")
            _put_names(out, definitions.nodes, "nodes")
            try:
                meta = _meta_text(definitions.meta)
            except (TypeError, ValueError, RecursionError) as exc:
                out.fail(f"META is not JSON data ({type(exc).__name__})")
            out.text(meta, MAX_META_BYTES, "META")
            out.pack(_U32, definitions.max_packet_bytes)
            out.text(definitions.hint, MAX_NAME_BYTES, "the definitions' hint")
        case ConfigDefinitions():
            out.pack(_U8, _DEFS_CONFIG)
            out.path(definitions.config_path, "the configuration's path")
            digest = definitions.config_sha256
            out.pack(_U8, digest is not None)
            if digest is not None:
                out.text(digest, MAX_NAME_BYTES, "the configuration's digest")
            _put_names(out, definitions.nodes, "nodes")
            out.pack(_U32, definitions.max_packet_bytes)
            out.text(definitions.command, MAX_NAME_BYTES, "the command")
            out.text(definitions.fingerprint, MAX_NAME_BYTES, "the definitions' fingerprint")


def _meta_text(meta: dict[str, Any]) -> str:
    """The one JSON form META travels in (ASCII, so a lone surrogate survives as an escape)."""
    return json.dumps(meta, ensure_ascii=True, separators=(",", ":"))


def _put_job(out: _Out, job: Job) -> None:
    if not 1 <= job.text_cap <= MAX_TEXT_CAP or not 1 <= job.event_cap <= MAX_EVENT_CAP:
        out.fail(_cap_reason(job.text_cap, job.event_cap))
    flags = (
        _flag(job.stop is not None, _JOB_STOP)
        | _flag(job.exact, _JOB_EXACT)
        | _flag(job.start_ns is not None, _JOB_START_NS)
        | _flag(job.end_ns is not None, _JOB_END_NS)
    )
    device, inode = job.identity
    out.pack(
        _JOB,
        job.index,
        job.guess,
        job.stop or 0,
        flags,
        _time_field(job.start_ns),
        _time_field(job.end_ns),
        device,
        inode,
        job.text_cap,
        job.event_cap,
    )
    out.path(job.path, "the recording's path")
    seed = job.seed
    out.pack(_SEED, seed.last_seq, seed.work)
    out.count(len(seed.streams), MAX_SEED_STREAMS, "the seed's streams")
    for stream_id, stream in seed.streams.items():
        if stream_id != stream.id:
            out.fail(f"the seed files stream {stream.id} under {stream_id}")
        _put_stream(out, stream)
    out.count(len(seed.sources), MAX_SEED_SOURCES, "the seed's sources")
    for source_id, text in seed.sources.items():
        out.pack(_SOURCE, source_id)
        out.text(text, MAX_NAME_BYTES, "a source")
    _put_shape(out, job.shape)


def _time_field(value: int | None) -> bytes:
    """A time bound as the wire carries it: signed, little endian (``OverflowError`` beyond it)."""
    return (value or 0).to_bytes(_TIME_BYTES, "little", signed=True)


def _put_shape(out: _Out, shape: Shape) -> None:
    flags = (
        _flag(shape.header_fields, _SHAPE_HEADER_FIELDS)
        | _flag(shape.raw_enums, _SHAPE_RAW_ENUMS)
        | _flag(shape.directory, _SHAPE_DIRECTORY)
        | _flag(shape.skip_damage, _SHAPE_SKIP_DAMAGE)
        | _flag(shape.categories, _SHAPE_CATEGORIES)
        | _flag(shape.where is not None, _SHAPE_WHERE)
        | _flag(shape.aliases is not None, _SHAPE_ALIASES)
    )
    out.pack(_SHAPE, _FORMATS[shape.fmt], flags)
    if shape.where is not None:
        out.os_text(shape.where, MAX_NAME_BYTES, "the filter")
    _put_names(out, tuple(sorted(shape.links)), "links")
    if shape.aliases is not None:
        out.count(len(shape.aliases), MAX_NAMES, "aliases")
        for alias, name in shape.aliases:
            out.os_text(alias, MAX_NAME_BYTES, "an alias")
            out.os_text(name, MAX_NAME_BYTES, "an alias's name")
    out.count(len(shape.messages), MAX_NAMES, "messages")
    for message in shape.messages:
        out.os_text(message, MAX_NAME_BYTES, "a message name")


def _cap_reason(text_cap: int, event_cap: int) -> str:
    """Why the caps of a job are not ones the format carries (they must not both be fine)."""
    if not 1 <= text_cap <= MAX_TEXT_CAP:
        return f"a text cap of {text_cap} is outside 1..{MAX_TEXT_CAP}"
    assert not 1 <= event_cap <= MAX_EVENT_CAP
    return f"an event cap of {event_cap} is outside 1..{MAX_EVENT_CAP}"


def _put_outcome(out: _Out, outcome: Outcome) -> None:
    if len(outcome.damage) != len(outcome.damage_rows):
        out.fail("the damage and the rows before each disagree in number")
    flags = (
        _flag(outcome.start is not None, _OUT_START)
        | _flag(outcome.reached_stop, _OUT_REACHED_STOP)
        | _flag(outcome.first_seq is not None, _OUT_FIRST_SEQ)
        | _flag(outcome.trailer, _OUT_TRAILER)
        | _flag(outcome.partial, _OUT_PARTIAL)
        | _flag(outcome.moved, _OUT_MOVED)
        | _flag(outcome.failure is not None, _OUT_FAILURE)
        | _flag(outcome.crash is not None, _OUT_CRASH)
        | _flag(outcome.categories is not None, _OUT_CATEGORIES)
        | _flag(outcome.order is not None, _OUT_ORDER)
    )
    out.pack(
        _OUTCOME,
        outcome.index,
        outcome.start or 0,
        outcome.end,
        outcome.first_seq or 0,
        outcome.last_seq,
        outcome.work,
        outcome.scanned,
        outcome.frames_read,
        outcome.frames_in_range,
        flags,
    )
    out.count(len(outcome.definitions), MAX_NOTES, "definitions")
    for entry in outcome.definitions:
        match entry:
            case ("stream", Stream() as stream):
                out.pack(_U8, _DEF_STREAM)
                _put_stream(out, stream)
            case ("source", int() as source_id, str() as text):
                out.pack(_U8, _DEF_SOURCE)
                out.pack(_SOURCE, source_id)
                out.text(text, MAX_NAME_BYTES, "a source")
            case _:
                out.fail("a definition is neither a stream nor a source")
    _put_ids(out, outcome.used_streams, "stream ids used")
    _put_ids(out, outcome.used_sources, "source ids used")
    out.count(len(outcome.damage), MAX_NOTES, "damage")
    for damage, rows in zip(outcome.damage, outcome.damage_rows, strict=True):
        damage_flags = (
            _flag(damage.torn, _DAMAGE_TORN)
            | _flag(damage.resumed_at is not None, _DAMAGE_RESUMED)
            | _flag(damage.trailing, _DAMAGE_TRAILING)
            | _flag(damage.incomplete, _DAMAGE_INCOMPLETE)
        )
        out.pack(_DAMAGE, damage.offset, damage.span, damage.resumed_at or 0, rows, damage_flags)
        out.text(damage.reason, MAX_NAME_BYTES, "a damage reason")
    if outcome.failure is not None:
        code, summary, details = outcome.failure
        if code not in _CODES:
            out.fail("a failure's code is not an error code")
        out.text(code, MAX_CODE_BYTES, "a failure's code")
        out.text(summary, MAX_DIAG_BYTES, "a failure's summary")
        out.text(details, MAX_DIAG_BYTES, "a failure's details")
    if outcome.crash is not None:
        out.text(outcome.crash, MAX_DIAG_BYTES, "a crash")
    out.count(len(outcome.gap_marks), MAX_NOTES, "gap marks")
    for mark in outcome.gap_marks:
        out.pack(_GAP_MARK, *mark)
    _put_counts(out, outcome.counts)
    out.count(len(outcome.blocks), MAX_BLOCKS, "blocks")
    for block in outcome.blocks:
        block_flags = _flag(block.key is not None, _BLOCK_KEY) | _flag(
            block.malformed, _BLOCK_MALFORMED
        )
        out.pack(_BLOCK, block_flags, block.rows, block.handle)
        if block.key is not None:
            name, columns = block.key
            out.text(name, MAX_NAME_BYTES, "a table's message")
            _put_names(out, columns, "a table's columns")
        out.text(block.text, MAX_REPLY_BYTES, "rows")
    if outcome.categories is not None:
        out.blob(outcome.categories, MAX_REPLY_BYTES, "the kinds of the rows")
    if outcome.order is not None:
        order = outcome.order
        if order.typecode != "I":
            out.fail("the order of the lines is not of unsigned ints")
        if _SWAP:  # pragma: no cover - a big-endian host
            order = array.array("I", order)
            order.byteswap()
        out.blob(order.tobytes(), MAX_REPLY_BYTES, "the order of the lines")


def _put_counts(out: _Out, counts: ExportResult) -> None:
    out.pack(
        _COUNTS,
        counts.rows,
        counts.decoded,
        counts.malformed,
        counts.unknown,
        counts.garbage,
        counts.undecoded,
        counts.skipped_no_definitions,
        counts.not_written,
        counts.files,
        _ENDINGS[counts.ending],
    )
    for table, what, cap in (
        (counts.rejected, "rejections", len(_CODES)),
        (counts.per_table, "tables", MAX_NAMES),
    ):
        out.count(len(table), cap, what)
        for name, number in table.items():
            if what == "rejections" and name not in _CODES:
                out.fail("a rejection's code is not an error code")
            out.text(name, MAX_NAME_BYTES, what)
            out.pack(_U64, number)


def _put_fault(out: _Out, fault: Fault) -> None:
    out.text(fault.reason, MAX_DIAG_BYTES, "a fault")


_ENCODERS: Final[dict[type, tuple[Kind, int, Callable[[_Out, Any], None]]]] = {
    EmbeddedDefinitions: (Kind.DEFINITIONS, MAX_INIT_BYTES, _put_definitions),
    ConfigDefinitions: (Kind.DEFINITIONS, MAX_INIT_BYTES, _put_definitions),
    Job: (Kind.JOB, MAX_JOB_BYTES, _put_job),
    Outcome: (Kind.OUTCOME, MAX_REPLY_BYTES, _put_outcome),
    Fault: (Kind.FAULT, MAX_REPLY_BYTES, _put_fault),
}


def encode(message: Message) -> bytes:
    """The frame of ``message``. Raises :class:`FrameError` for a value the format cannot carry
    (a number out of its range, text that is not valid Unicode, a list or a frame over its cap)."""
    kind, cap, put = _ENCODERS[type(message)]
    out = _Out(cap - HEADER.size)
    try:
        put(out, message)
    except (struct.error, OverflowError) as exc:
        raise FrameError(
            f"a number is out of the range of its field ({exc})", out.size, sending=True
        ) from exc
    return out.finish(kind)


# -- reading ----------------------------------------------------------------------------------


class _In:
    """A frame being read: the bytes, where reading is, and the items it may still make."""

    __slots__ = ("items", "pos", "view")

    def __init__(self, view: memoryview, pos: int) -> None:
        self.view = view
        self.pos = pos
        self.items = MAX_ITEMS

    def need(self, n: int, what: str) -> int:
        """Where the next ``n`` bytes begin, which must be in the frame; reading moves past them."""
        start = self.pos
        if n > len(self.view) - start:
            raise FrameError(f"{what} runs past the end of the message", start)
        self.pos = start + n
        return start

    def unpack(self, layout: struct.Struct, what: str) -> tuple[Any, ...]:
        return layout.unpack_from(self.view, self.need(layout.size, what))

    def u8(self, what: str) -> int:
        value: int = self.unpack(_U8, what)[0]
        return value

    def flags(self, value: int, known: int, what: str) -> int:
        if value & ~known:
            raise FrameError(f"{what} has undefined flags set (0x{value:x})", self.pos)
        return value

    def spend(self, n: int, what: str) -> None:
        self.items -= n
        if self.items < 0:
            raise FrameError(f"{what}: the message holds more than {MAX_ITEMS} items", self.pos)

    def count(self, cap: int, what: str) -> int:
        at = self.pos
        n: int = self.unpack(_U32, what)[0]
        if n > cap:
            raise FrameError(f"{what}: {n} is more than {cap}", at)
        self.spend(n, what)
        return n

    def chunk(self, cap: int, what: str) -> memoryview:
        at = self.pos
        n: int = self.unpack(_U32, what)[0]
        if n > cap:
            raise FrameError(f"{what} is longer than {cap} bytes", at)
        start = self.need(n, what)
        return self.view[start : start + n]

    def blob(self, cap: int, what: str) -> bytes:
        return bytes(self.chunk(cap, what))

    def text(self, cap: int, what: str) -> str:
        data = self.chunk(cap, what)
        self.spend(1, what)
        try:
            return str(data, "utf-8")
        except UnicodeDecodeError as exc:
            raise FrameError(
                f"{what} is not valid UTF-8", self.pos - len(data) + exc.start
            ) from exc

    def os_text(self, cap: int, what: str) -> str:
        data = self.blob(cap, what)
        self.spend(1, what)
        return os.fsdecode(data)

    def path(self, what: str) -> Path:
        at = self.pos
        data = self.blob(MAX_NAME_BYTES, what)
        self.spend(1, what)
        path = Path(os.fsdecode(data))
        if not data or b"\0" in data or os.fsencode(path) != data:
            raise FrameError(f"{what} is not a path in normal form", at)
        return path

    def zero(self, value: int, what: str) -> None:
        """An absent value's field, which must be zero."""
        if value:
            raise FrameError(f"{what} is absent but not zero", self.pos)

    def end(self) -> None:
        if self.pos != len(self.view):
            raise FrameError("bytes follow the end of the message", self.pos)


def _take_stream(src: _In) -> Stream:
    at = src.pos
    stream_id, kind_code, direction_code = src.unpack(_STREAM, "a stream")
    kind, direction = _KINDS_BY_CODE.get(kind_code), _DIRECTIONS_BY_CODE.get(direction_code)
    if stream_id > MAX_STREAM_ID or kind is None or direction is None:
        raise FrameError("a stream's id, kind or direction is not defined", at)
    link = src.text(MAX_NAME_BYTES, "a stream's link")
    node = src.text(MAX_NAME_BYTES, "a stream's node")
    transport = src.text(MAX_NAME_BYTES, "a stream's transport")
    endpoint = src.text(MAX_NAME_BYTES, "a stream's endpoint")
    return Stream(stream_id, link, node, kind, direction, transport, endpoint)


def _take_names(src: _In, what: str) -> tuple[str, ...]:
    return tuple(src.text(MAX_NAME_BYTES, what) for _ in range(src.count(MAX_NAMES, what)))


def _take_ids(src: _In, what: str) -> list[int]:
    n = src.count(MAX_IDS, what)
    at = src.need(2 * n, what)
    ids = array.array("H")
    ids.frombytes(src.view[at : at + 2 * n])
    if _SWAP:  # pragma: no cover - a big-endian host
        ids.byteswap()
    return ids.tolist()


def _take_definitions(src: _In) -> Definitions:
    at = src.pos
    kind = src.u8("the kind of definitions")
    take = _DEFINITION_READERS.get(kind)
    if take is None:
        raise FrameError(f"definitions of kind {kind} are not defined", at)
    return take(src)


def _take_config(src: _In) -> ConfigDefinitions:
    path = src.path("the configuration's path")
    has_digest = src.u8("the configuration's digest")
    if has_digest > 1:
        raise FrameError("the configuration's digest flag is not 0 or 1", src.pos - 1)
    digest = src.text(MAX_NAME_BYTES, "the configuration's digest") if has_digest else None
    nodes = _take_names(src, "nodes")
    (max_packet_bytes,) = src.unpack(_U32, "the packet cap")
    command = src.text(MAX_NAME_BYTES, "the command")
    fingerprint = src.text(MAX_NAME_BYTES, "the definitions' fingerprint")
    return ConfigDefinitions(path, digest, nodes, max_packet_bytes, command, fingerprint)


def _take_embedded(src: _In) -> EmbeddedDefinitions:
    path = src.path("the recording's path")
    blobs: dict[tuple[str, fmt.BlobKind], fmt.Blob] = {}
    for _ in range(src.count(MAX_BLOBS, "embedded definitions")):
        at = src.pos
        kind = _BLOB_KINDS_BY_CODE.get(src.u8("an embedded definition's kind"))
        if kind is None:
            raise FrameError("an embedded definition's kind is not defined", at)
        node = src.text(MAX_NAME_BYTES, "an embedded definition's node")
        adapter = src.text(MAX_NAME_BYTES, "an embedded definition's adapter")
        digest_at = src.need(_SHA256_BYTES, "a digest")
        digest = bytes(src.view[digest_at : digest_at + _SHA256_BYTES])
        content = src.blob(fmt.MAX_BLOB_CONTENT, "an embedded definition")
        if (node, kind) in blobs:
            raise FrameError("two embedded definitions of one node and kind", at)
        blobs[(node, kind)] = fmt.Blob(kind, node, adapter, digest, content)
    problem = src.text(MAX_NAME_BYTES, "the definitions' problem")
    nodes = _take_names(src, "nodes")
    at = src.pos
    meta_text = src.text(MAX_META_BYTES, "META")
    try:
        meta = json.loads(meta_text)
    except (ValueError, RecursionError) as exc:
        raise FrameError(f"META is not JSON ({type(exc).__name__})", at) from exc
    if not isinstance(meta, dict) or fmt.json_depth(meta) > fmt.MAX_META_DEPTH:
        raise FrameError(f"META is not an object nested at most {fmt.MAX_META_DEPTH} deep", at)
    if _meta_text(meta) != meta_text:
        raise FrameError("META is not in the form the pool writes it", at)
    (max_packet_bytes,) = src.unpack(_U32, "the packet cap")
    hint = src.text(MAX_NAME_BYTES, "the definitions' hint")
    return EmbeddedDefinitions(path, blobs, problem, nodes, meta, max_packet_bytes, hint)


_DEFINITION_READERS: Final[dict[int, Callable[[_In], Definitions]]] = {
    _DEFS_EMBEDDED: _take_embedded,
    _DEFS_CONFIG: _take_config,
}


def _take_job(src: _In) -> Job:
    at = src.pos
    (index, guess, stop, flags, start_raw, end_raw, device, inode, text_cap, event_cap) = (
        src.unpack(_JOB, "a job")
    )
    start_ns = int.from_bytes(start_raw, "little", signed=True)
    end_ns = int.from_bytes(end_raw, "little", signed=True)
    src.flags(flags, _JOB_FLAGS, "a job")
    if not 1 <= text_cap <= MAX_TEXT_CAP or not 1 <= event_cap <= MAX_EVENT_CAP:
        raise FrameError(_cap_reason(text_cap, event_cap), at)
    for present, value, what in (
        (flags & _JOB_STOP, stop, "the stop"),
        (flags & _JOB_START_NS, start_ns, "the start time"),
        (flags & _JOB_END_NS, end_ns, "the end time"),
    ):
        if not present:
            src.zero(value, what)
    path = src.path("the recording's path")
    last_seq, work = src.unpack(_SEED, "the seed")
    streams: dict[int, Stream] = {}
    for _ in range(src.count(MAX_SEED_STREAMS, "the seed's streams")):
        stream_at = src.pos
        stream = _take_stream(src)
        if stream.id in streams:
            raise FrameError(f"the seed defines stream {stream.id} twice", stream_at)
        streams[stream.id] = stream
    sources: dict[int, str] = {}
    for _ in range(src.count(MAX_SEED_SOURCES, "the seed's sources")):
        source_at = src.pos
        (source_id,) = src.unpack(_SOURCE, "a source")
        text = src.text(MAX_NAME_BYTES, "a source")
        if source_id in sources:
            raise FrameError(f"the seed defines source {source_id} twice", source_at)
        sources[source_id] = text
    shape = _take_shape(src)
    return Job(
        index,
        path,
        guess,
        stop if flags & _JOB_STOP else None,
        bool(flags & _JOB_EXACT),
        Seed(streams, sources, last_seq, work),
        start_ns if flags & _JOB_START_NS else None,
        end_ns if flags & _JOB_END_NS else None,
        shape,
        (device, inode),
        text_cap,
        event_cap,
    )


def _take_shape(src: _In) -> Shape:
    at = src.pos
    format_code, flags = src.unpack(_SHAPE, "the shape")
    src.flags(flags, _SHAPE_FLAGS, "the shape")
    export_format = _FORMATS_BY_CODE.get(format_code)
    if export_format is None:
        raise FrameError(f"export format {format_code} is not defined", at)
    where = src.os_text(MAX_NAME_BYTES, "the filter") if flags & _SHAPE_WHERE else None
    links_at = src.pos
    links = _take_names(src, "links")
    if any(a >= b for a, b in itertools.pairwise(links)):
        raise FrameError("the links are not in strictly increasing order", links_at)
    aliases: tuple[tuple[str, str], ...] | None = None
    if flags & _SHAPE_ALIASES:
        aliases = tuple(
            (src.os_text(MAX_NAME_BYTES, "an alias"), src.os_text(MAX_NAME_BYTES, "a name"))
            for _ in range(src.count(MAX_NAMES, "aliases"))
        )
    messages = tuple(
        src.os_text(MAX_NAME_BYTES, "a message name")
        for _ in range(src.count(MAX_NAMES, "messages"))
    )
    return Shape(
        export_format,
        where,
        frozenset(links),
        aliases,
        messages,
        header_fields=bool(flags & _SHAPE_HEADER_FIELDS),
        raw_enums=bool(flags & _SHAPE_RAW_ENUMS),
        directory=bool(flags & _SHAPE_DIRECTORY),
        skip_damage=bool(flags & _SHAPE_SKIP_DAMAGE),
        categories=bool(flags & _SHAPE_CATEGORIES),
    )


def _take_outcome(src: _In) -> Outcome:
    (
        index,
        start,
        end,
        first_seq,
        last_seq,
        work,
        scanned,
        frames_read,
        frames_in_range,
        flags,
    ) = src.unpack(_OUTCOME, "an outcome")
    src.flags(flags, _OUT_FLAGS, "an outcome")
    if not flags & _OUT_START:
        src.zero(start, "the start")
    if not flags & _OUT_FIRST_SEQ:
        src.zero(first_seq, "the first sequence number")
    out = Outcome(
        index,
        start=start if flags & _OUT_START else None,
        end=end,
        reached_stop=bool(flags & _OUT_REACHED_STOP),
        first_seq=first_seq if flags & _OUT_FIRST_SEQ else None,
        last_seq=last_seq,
        work=work,
        scanned=scanned,
        trailer=bool(flags & _OUT_TRAILER),
        partial=bool(flags & _OUT_PARTIAL),
        moved=bool(flags & _OUT_MOVED),
        frames_read=frames_read,
        frames_in_range=frames_in_range,
    )
    for _ in range(src.count(MAX_NOTES, "definitions")):
        at = src.pos
        tag = src.u8("a definition")
        if tag == _DEF_STREAM:
            out.definitions.append(("stream", _take_stream(src)))
        elif tag == _DEF_SOURCE:
            (source_id,) = src.unpack(_SOURCE, "a source")
            out.definitions.append(("source", source_id, src.text(MAX_NAME_BYTES, "a source")))
        else:
            raise FrameError(f"a definition of kind {tag} is not defined", at)
    out.used_streams = _take_ids(src, "stream ids used")
    out.used_sources = _take_ids(src, "source ids used")
    for _ in range(src.count(MAX_NOTES, "damage")):
        offset, span, resumed_at, rows, damage_flags = src.unpack(_DAMAGE, "damage")
        src.flags(damage_flags, _DAMAGE_FLAGS, "damage")
        if not damage_flags & _DAMAGE_RESUMED:
            src.zero(resumed_at, "where valid records resume")
        reason = src.text(MAX_NAME_BYTES, "a damage reason")
        out.damage.append(
            Damage(
                offset,
                span,
                bool(damage_flags & _DAMAGE_TORN),
                reason,
                resumed_at if damage_flags & _DAMAGE_RESUMED else None,
                bool(damage_flags & _DAMAGE_TRAILING),
                bool(damage_flags & _DAMAGE_INCOMPLETE),
            )
        )
        out.damage_rows.append(rows)
    if flags & _OUT_FAILURE:
        at = src.pos
        code = src.text(MAX_CODE_BYTES, "a failure's code")
        if code not in _CODES:
            raise FrameError("a failure's code is not an error code", at)
        summary = src.text(MAX_DIAG_BYTES, "a failure's summary")
        out.failure = (code, summary, src.text(MAX_DIAG_BYTES, "a failure's details"))
    if flags & _OUT_CRASH:
        out.crash = src.text(MAX_DIAG_BYTES, "a crash")
    for _ in range(src.count(MAX_NOTES, "gap marks")):
        out.gap_marks.append(src.unpack(_GAP_MARK, "a gap mark"))
    out.counts = _take_counts(src)
    for _ in range(src.count(MAX_BLOCKS, "blocks")):
        block_flags, rows, handle = src.unpack(_BLOCK, "a block")
        src.flags(block_flags, _BLOCK_FLAGS, "a block")
        key = None
        if block_flags & _BLOCK_KEY:
            name = src.text(MAX_NAME_BYTES, "a table's message")
            key = (name, _take_names(src, "a table's columns"))
        text = src.text(MAX_REPLY_BYTES, "rows")
        out.blocks.append(Block(key, bool(block_flags & _BLOCK_MALFORMED), rows, text, handle))
    if flags & _OUT_CATEGORIES:
        out.categories = src.blob(MAX_REPLY_BYTES, "the kinds of the rows")
    if flags & _OUT_ORDER:
        at = src.pos
        data = src.chunk(MAX_REPLY_BYTES, "the order of the lines")
        if len(data) % 4:
            raise FrameError("the order of the lines is not a whole number of entries", at)
        order = array.array("I")
        order.frombytes(data)
        if _SWAP:  # pragma: no cover - a big-endian host
            order.byteswap()
        out.order = order
    return out


def _take_counts(src: _In) -> ExportResult:
    at = src.pos
    (
        rows,
        decoded,
        malformed,
        unknown,
        garbage,
        undecoded,
        skipped_no_definitions,
        not_written,
        files,
        ending_code,
    ) = src.unpack(_COUNTS, "the tally")
    ending = _ENDINGS_BY_CODE.get(ending_code)
    if ending is None:
        raise FrameError(f"ending {ending_code} is not defined", at)
    tables: list[dict[str, int]] = []
    for what, cap in (("rejections", len(_CODES)), ("tables", MAX_NAMES)):
        table: dict[str, int] = {}
        for _ in range(src.count(cap, what)):
            entry_at = src.pos
            name = src.text(MAX_NAME_BYTES, what)
            if what == "rejections" and name not in _CODES:
                raise FrameError("a rejection's code is not an error code", entry_at)
            (number,) = src.unpack(_U64, what)
            if name in table:
                raise FrameError(f"{what} name one entry twice", entry_at)
            table[name] = number
        tables.append(table)
    rejected, per_table = tables
    return ExportResult(
        rows,
        decoded,
        malformed,
        unknown,
        garbage,
        undecoded,
        skipped_no_definitions,
        not_written,
        files,
        ending,
        Counter(rejected),
        per_table,
    )


def _take_fault(src: _In) -> Fault:
    return Fault(src.text(MAX_DIAG_BYTES, "a fault"))


_DECODERS: Final[dict[int, tuple[int, Callable[[_In], Message]]]] = {
    Kind.DEFINITIONS: (MAX_INIT_BYTES, _take_definitions),
    Kind.JOB: (MAX_JOB_BYTES, _take_job),
    Kind.OUTCOME: (MAX_REPLY_BYTES, _take_outcome),
    Kind.FAULT: (MAX_REPLY_BYTES, _take_fault),
}
assert set(_DECODERS) == set(Kind)
assert {kind for kind, _, _ in _ENCODERS.values()} == set(Kind)


def decode(data: bytes | bytearray | memoryview) -> Message:
    """The message of the frame ``data``: the single place a pool message is read. Raises
    :class:`FrameError` for anything that is not exactly a frame this version writes."""
    view = memoryview(data).cast("B") if isinstance(data, memoryview) else memoryview(data)
    if len(view) < HEADER.size:
        raise FrameError(f"the message is shorter than its {HEADER.size}-byte header", len(view))
    magic, version, kind, reserved, length = HEADER.unpack_from(view, 0)
    if magic != MAGIC:
        raise FrameError("the message does not begin with the pool's magic", 0)
    if version != VERSION:
        raise FrameError(f"version {version} is not {VERSION}", 4)
    found = _DECODERS.get(kind)
    if found is None:
        raise FrameError(f"message type {kind} is not defined", 5)
    if reserved:
        raise FrameError("the reserved field is not zero", 6)
    cap, take = found
    if length != len(view) - HEADER.size:
        raise FrameError(
            f"the body length {length} disagrees with the {len(view) - HEADER.size} bytes sent", 8
        )
    if len(view) > cap:
        raise FrameError(f"a message of type {kind} is longer than {cap} bytes", 8)
    src = _In(view, HEADER.size)
    message = take(src)
    src.end()
    return message


def receive_cap(kind: Kind) -> int:
    """The longest frame of type ``kind``: what a receiver lets the pipe deliver."""
    return _DECODERS[kind][0]


def diagnostic(text: str) -> str:
    """``text`` (an error's summary or details, a traceback) as a frame can carry it: a lone
    surrogate as its ``\\udcXX`` escape (what the display shows for it anyway), and at most
    :data:`MAX_DIAG_BYTES`, keeping the end (where a traceback names the error)."""
    data = text.encode("utf-8", "backslashreplace")
    if len(data) <= MAX_DIAG_BYTES:
        return text if len(data) == len(text) else data.decode("utf-8")
    mark = b"... "
    return (mark + data[len(data) - MAX_DIAG_BYTES + len(mark) :]).decode("utf-8", "ignore")


# -- what a reply may claim -------------------------------------------------------------------


def layouts_of(codecs: Mapping[str, Codec], *, header_fields: bool) -> frozenset[TableKey]:
    """Every table a CSV export of ``codecs`` can make: the key (message name, columns) of each
    message of each codec. The one set a reply's tables are checked against (see
    :func:`check_reply`), so a name a worker sends is never taken for a table nothing defines."""
    return frozenset(
        (message.name, table_columns(message, codec, header_fields))
        for codec in codecs.values()
        for message in codec.model.messages
    )


def read_reply(
    job: Job, data: bytes | bytearray | memoryview, layouts: Collection[TableKey]
) -> Outcome:
    """The outcome a worker's message ``data`` gives for ``job``, whose tables are among
    ``layouts``: the one way the parent reads a reply. Raises :class:`FrameError` for a frame
    :func:`decode` refuses, a worker's :class:`~ironbird.recording.poolmsg.Fault`, any other
    message, and an outcome :func:`check_reply` refuses."""
    reply = decode(data)
    if isinstance(reply, Fault):
        raise FrameError(f"the worker could not read what it was sent: {reply.reason}")
    if not isinstance(reply, Outcome):
        raise FrameError(f"the worker answered with a message of type {type(reply).__name__}")
    check_reply(job, reply, layouts)
    return reply


def check_reply(job: Job, outcome: Outcome, layouts: Collection[TableKey]) -> None:
    """Raise :class:`FrameError` unless ``outcome`` answers ``job`` and holds together the way the
    parent applies it: the range asked for; a start where the job says reading starts (an exact
    range always has one, unless it crashed or its file moved) and an end not before it; damage
    and gaps only with the kind of every row; every row kind defined; the tables the format has,
    each a layout in ``layouts`` (a name from a worker reaches the file system only this way) and
    at most one block of each; and the lines of the blocks, the rows that made a line and the
    order of the lines all in agreement. A worker that tells the truth always passes; the rows
    themselves are the worker's to decode."""
    if outcome.index != job.index:
        raise FrameError(f"asked for range {job.index}, the answer is for {outcome.index}")
    start = outcome.start
    if start is None:
        if job.exact and outcome.crash is None and not outcome.moved:
            raise FrameError(f"exact range {job.index} came back without a start")
    else:
        stop = job.stop
        if job.exact:
            if start != job.guess:
                raise FrameError(f"an exact range read from {start}, not {job.guess}")
        elif start < job.guess or (stop is not None and start >= stop):
            raise FrameError(f"range {job.index} began at {start}, outside it")
    healthy = start is not None and outcome.failure is None and outcome.crash is None
    if healthy and start is not None and outcome.end < start:
        raise FrameError(f"range {job.index} ended at {outcome.end}, before it began")
    if outcome.partial and not (healthy and start is not None and outcome.end > start):
        raise FrameError(f"range {job.index} was cut short without moving on")
    shape = job.shape
    blocks = outcome.blocks
    if shape.fmt is ExportFormat.CSV:
        handles: set[int] = set()
        keys: set[TableKey] = set()
        for block in blocks:
            if block.malformed != (block.key is None) or (block.malformed and not shape.directory):
                raise FrameError("a block is not of a table the export has")
            if block.key is not None:
                if block.key not in layouts:
                    raise FrameError("a block is of a table that no definition makes")
                keys.add(block.key)
            handles.add(block.handle)
        if len(handles) != len(blocks) or len(keys) != len(blocks) - sum(
            b.malformed for b in blocks
        ):
            raise FrameError("two blocks are of one table")
    elif len(blocks) > 1 or any(b.key is not None or b.malformed or b.handle for b in blocks):
        raise FrameError(f"a {shape.fmt.value} export has one stream of rows")
    written = [block for block in blocks if block.text]
    if any(block.text[-1] != "\n" for block in written):
        raise FrameError("a block does not end with a whole line")
    categories = outcome.categories
    if categories is None:
        if outcome.damage or outcome.gap_marks or (shape.categories and written):
            raise FrameError("rows to be written one by one came without their kinds")
        if outcome.order is not None:
            raise FrameError("the order of the lines came without their kinds")
        return
    if categories.translate(None, _VALID_KIND_BYTES):
        raise FrameError("a row is of a kind that is not defined")
    lines = len(categories) - sum(categories.count(kind) for kind in _NO_LINE_BYTES)
    per_block = {block.handle: block.text.count("\n") for block in written}
    if sum(per_block.values()) != lines:
        raise FrameError("the rows that made a line and the lines of the blocks disagree")
    order = outcome.order
    if order is None:
        if len(written) > 1:
            raise FrameError("lines of several tables came without their order")
        return
    if len(order) != lines or Counter(order) != Counter(per_block):
        raise FrameError("the order of the lines does not match the blocks")


__all__ = [
    "HEADER",
    "MAGIC",
    "MAX_INIT_BYTES",
    "MAX_JOB_BYTES",
    "MAX_REPLY_BYTES",
    "VERSION",
    "FrameError",
    "Kind",
    "Message",
    "check_reply",
    "decode",
    "diagnostic",
    "encode",
    "layouts_of",
    "read_reply",
    "receive_cap",
]
