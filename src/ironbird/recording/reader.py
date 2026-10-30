"""The recording reader: an untrusted-input decode boundary (plan S3 "Reading").

A recording is as untrusted as a packet: it may be torn by a crash, damaged by a disk, or
forged. :class:`RecordingReader` validates a record completely before it builds anything from
it, and a :class:`~ironbird.frame.Frame` is only ever built from a record that passed every
rule, so the ``Frame`` asserts and the codec's ``oversize_error`` assertion are unreachable from
a file. A record that fails a rule is *damage*, never a frame, and never an exception.

Per record, in order: envelope present, ``body_len`` inside the type's bounds, body and padding
present, CRC (seeded with the preamble's nonce), type known or skippable, then the type's rules
(and, for a frame, the consistency rules of S3). Reading uses plain ``os.pread`` in 1 MiB blocks;
a file that shrinks or grows under the reader is never mapped, so it cannot raise ``SIGBUS``.

After damage the scan for the next record is conservative: a candidate must pass every record
rule *and* be followed by a second valid record where its length says it ends. The scan shares
one work budget with the rest of the pass (bytes read and CRC-checked, proportional to the size of
the file and never reset by a new stretch of damage), and a record whose declared length runs
past the end of the file is refused before any of it is read, so a hostile file cannot make the
reader quadratic. Damage that reaches the end of the file with no valid chain after it is a
*torn tail* (a crash); damage followed by a valid chain is *mid-file damage*.

Frames carry no ``checked`` token: the reader never copies one from anywhere.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import hashlib
import os
import re
import stat
import zlib
from collections import ChainMap
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, NamedTuple

from ironbird.errors import Code, IronbirdError
from ironbird.frame import (
    FLAG_GARBAGE,
    FLAG_HAS_MID,
    FLAG_STALE,
    FLAG_TRUNCATED,
    FLAGS_BY_BITS,
    KNOWN_FLAG_BITS,
    SERIAL_TRANSPORT,
    Frame,
    Stream,
)
from ironbird.recording import format as fmt
from ironbird.recording.format import RecordType
from ironbird.sanitize import clean

READ_BLOCK: Final = 1 << 20
SMALL_BLOCK: Final = 16 * 1024  # random access (index, chunk heads) reads this much, not a MiB
WORK_FLOOR: Final = 4 * 1024 * 1024  # bytes of work a resync may do however small the file
WORK_FACTOR: Final = 4  # ... plus this many times the size of the file, over the whole pass
CANDIDATE_COST: Final = 64  # work charged for looking at one candidate, however cheap
MAX_DAMAGE_KEPT: Final = 1000  # Damage objects retained; the count and bytes cover them all
MAX_STREAMS: Final = fmt.MAX_STREAM_ID + 1

# A record envelope's type byte followed by its flags and reserved bytes (all zero in v1) is the
# cheap shape a resync scan looks for; only a match at an 8-byte aligned offset is a candidate.
_CANDIDATE: Final = re.compile(rb"[\x01-\x0a\x80-\xff]\x00\x00\x00")

_Effect = tuple[Any, ...] | None


class _Bad(Exception):
    """A record that fails a rule; the message is the reason shown to the user."""


class _Fail(NamedTuple):
    reason: str
    eof: bool = False  # no bytes at all at the offset: the file ended at a record boundary
    unsupported: bool = False  # a valid record of a type this version cannot skip
    incomplete: bool = False  # the file ends inside the record (as opposed to a bad record)


class _Raw(NamedTuple):
    offset: int
    end: int  # offset of the next record
    rtype: int
    body: memoryview


class Item(NamedTuple):
    """One validated record. ``value`` is a Frame, Gap, Mark, Stream, Chunk, Blob, ... by type."""

    offset: int
    end: int
    rtype: int
    value: Any


@dataclass(frozen=True, slots=True)
class Damage:
    """A stretch of the file that is not valid records."""

    offset: int
    span: int  # bytes from ``offset`` to the next valid record (or to the end of the file)
    torn: bool  # no valid record follows: the file ends in an incomplete record
    reason: str
    resumed_at: int | None = None  # where valid records resume, when they do
    trailing: bool = False  # bytes after a valid trailer
    incomplete: bool = True  # the file ends inside the record, rather than the record being bad

    def to_error(self) -> IronbirdError:
        """The coded error for this damage (not raised; the caller decides how loud to be)."""
        where = f"at offset {self.offset}"
        if self.torn:
            what = "an incomplete record" if self.incomplete else "a damaged record"
            return IronbirdError(
                Code.REC_TORN,
                f"the recording ends in {what} {where} ({self.reason}); "
                "everything before it is readable",
            )
        span = f"{self.span} bytes" if self.resumed_at is not None else "to the end of the file"
        return IronbirdError(
            Code.REC_DAMAGED,
            f"damaged recording {where}, {span} ({self.reason})"
            + (
                f"; valid records resume at {self.resumed_at}"
                if self.resumed_at is not None
                else ""
            ),
        )


@dataclass(slots=True)
class _State:
    streams: dict[int, Stream] = field(default_factory=dict)
    sources: dict[int, str] = field(default_factory=dict)
    last_seq: int = 0


def _is_magic_prefix(head: bytes) -> bool:
    return bool(head) and fmt.MAGIC.startswith(head[: len(fmt.MAGIC)])


class RecordingReader:
    """Reads one ``.ibrec`` file. Construct, iterate :meth:`items` or :meth:`frames`, close.

    Construction validates the preamble and the META record (``IB-REC-004`` not a recording,
    ``IB-REC-005`` unsupported version, ``IB-REC-006`` damaged first record, ``IB-REC-007`` ends
    before META). After that nothing a file contains raises: damage is reported as
    :class:`Damage` and iteration stops there, or resumes when ``skip_damage`` is set.
    """

    def __init__(self, path: Path | str, *, skip_damage: bool = False) -> None:
        self.path: Path = Path(path)
        self._skip_damage = skip_damage
        self.damage: list[Damage] = []  # the first MAX_DAMAGE_KEPT stretches
        self.damage_count = 0  # every stretch met
        self.damage_bytes = 0
        self.trailer: fmt.Trailer | None = None
        self.version: tuple[int, int] = (0, 0)
        self.nonce = 0
        self._seed = 0
        self._start = 0
        self.meta: fmt.Meta
        self.writer_active = False  # a writer holds the file's lock: it is being recorded now
        self.locked = False
        self._state = _State()
        self._work = 0  # bytes read and CRC-checked this pass, charged against the budget
        self._known_size = 0
        self._buf = b""
        self._base = 0
        try:
            self._fd = os.open(self.path, os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK | os.O_NOCTTY)
        except OSError as exc:
            raise IronbirdError(
                Code.REC_FORMAT,
                f"cannot open recording {_shown(self.path)}: {exc.strerror or exc}",
                cause=exc,
            ) from exc
        try:
            self._open()
        except BaseException:
            os.close(self._fd)
            raise

    # -- opening --

    def _open(self) -> None:
        shown = _shown(self.path)
        try:
            info = os.fstat(self._fd)
        except OSError as exc:
            raise IronbirdError(
                Code.REC_FORMAT, f"cannot read {shown}: {exc.strerror or exc}", cause=exc
            ) from exc
        if not stat.S_ISREG(info.st_mode):
            raise IronbirdError(Code.REC_FORMAT, f"{shown} is not a regular file")
        # A reader takes a shared lock where it can, so retention sees the session as in use; a
        # writer's exclusive lock refuses it, which says the recording is still being written.
        try:
            fcntl.flock(self._fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            self.locked = True
        except OSError as exc:
            self.writer_active = exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN)
        head = os.pread(self._fd, fmt.PREAMBLE_SIZE, 0)
        if len(head) < fmt.PREAMBLE_SIZE:
            if _is_magic_prefix(head):
                raise IronbirdError(
                    Code.REC_TORN, f"{shown} ends inside its preamble ({len(head)} bytes)"
                )
            raise IronbirdError(
                Code.REC_FORMAT,
                f"{shown} is not an ironbird recording "
                + (
                    "(the file is empty; a recorder killed just after creating a split part "
                    "leaves one, and the earlier parts are complete)"
                    if not head
                    else "(it is shorter than a preamble)"
                ),
            )
        magic, major, minor, flags, nonce = fmt.PREAMBLE.unpack(head)
        if magic != fmt.MAGIC:
            raise IronbirdError(Code.REC_FORMAT, f"{shown} is not an ironbird recording")
        if major != fmt.VERSION_MAJOR or flags:
            raise IronbirdError(
                Code.REC_VERSION,
                f"{shown} is recording format {major}.{minor} with flags {flags:#x}; "
                f"this version reads {fmt.VERSION_MAJOR}.x",
            )
        self.version = (major, minor)
        self.nonce = nonce
        self._seed = fmt.seed_of(nonce)
        self._max_packet = fmt.MAX_FRAME_BYTES  # until META says how large a packet may be
        first = self._read_raw(fmt.PREAMBLE_SIZE)
        if isinstance(first, _Fail):
            if first.unsupported:
                raise _unsupported(self.path, first.reason)
            if first.eof or first.reason.startswith("ends"):
                raise IronbirdError(
                    Code.REC_TORN,
                    f"{shown} ends before its first record is complete; it holds no frames",
                )
            raise IronbirdError(
                Code.REC_DAMAGED,
                f"the first record of {shown} is damaged ({first.reason}); the recording "
                "cannot be read",
            )
        if first.rtype != RecordType.META:
            raise IronbirdError(
                Code.REC_DAMAGED,
                f"the first record of {shown} is {fmt.describe_type(first.rtype)}, not META",
            )
        try:
            self.meta = fmt.decode_meta(first.body)
        except fmt.FormatError as exc:
            raise IronbirdError(
                Code.REC_DAMAGED, f"the META record of {shown} is unusable: {exc}"
            ) from exc
        self._max_packet = self.meta.max_packet_bytes
        self._start = first.end

    # -- state --

    @property
    def streams(self) -> Mapping[int, Stream]:
        """The streams defined so far, by id (grows as records are read)."""
        return self._state.streams

    @property
    def max_packet_bytes(self) -> int:
        return self.meta.max_packet_bytes

    @property
    def body_start(self) -> int:
        """Offset of the first record after META."""
        return self._start

    def read_at(self, offset: int, size: int) -> bytes:
        """Up to ``size`` raw bytes at ``offset`` (fewer at the end of the file)."""
        return bytes(self._window(offset, size, SMALL_BLOCK))

    def size(self) -> int:
        try:
            return os.fstat(self._fd).st_size
        except OSError:
            return 0

    def close(self) -> None:
        fd, self._fd = self._fd, -1
        if fd >= 0:
            with contextlib.suppress(OSError):
                os.close(fd)  # also drops the shared lock

    def __enter__(self) -> RecordingReader:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- low level: one record --

    def _file_covers(self, end: int) -> bool:
        """Whether the file extends to ``end`` (the size is looked up again only when needed)."""
        if end <= self._known_size:
            return True
        self._known_size = self.size()
        return end <= self._known_size

    def _window(self, offset: int, size: int, block: int = READ_BLOCK) -> memoryview:
        """Up to ``size`` bytes at ``offset`` (fewer only at the end of the file)."""
        buf, base = self._buf, self._base
        end = offset + size
        if base <= offset and end <= base + len(buf):
            return memoryview(buf)[offset - base : end - base]
        try:
            data = os.pread(self._fd, max(block, size), offset)
        except OSError as exc:  # the file went away or the disk failed under the reader
            raise IronbirdError(
                Code.REC_FORMAT,
                f"cannot read {_shown(self.path)}: {exc.strerror or exc}",
                cause=exc,
            ) from exc
        self._buf, self._base = data, offset
        return memoryview(data)[:size]

    def _read_raw(self, offset: int, block: int = READ_BLOCK) -> _Raw | _Fail:
        """The record at ``offset`` if its envelope, bounds, body, padding and CRC are all good."""
        head = self._window(offset, fmt.ENVELOPE_SIZE, block)
        if len(head) < fmt.ENVELOPE_SIZE:
            if not head:
                return _Fail("end of file", eof=True)
            return _Fail("ends inside a record envelope", incomplete=True)
        body_len, rtype, flags, reserved, crc = fmt.ENVELOPE.unpack_from(head, 0)
        if flags or reserved:
            return _Fail("envelope flags or reserved bytes are not zero")
        low, high = fmt.body_limits(rtype, self._max_packet)
        if not low <= body_len <= high:
            return _Fail(
                f"{fmt.describe_type(rtype)} body length {body_len} is outside {low}..{high}"
            )
        total = fmt.record_size(body_len)
        if not self._file_covers(
            offset + total
        ):  # nothing of a record that runs past the end is read
            return _Fail(f"ends inside a {fmt.describe_type(rtype)} record", incomplete=True)
        self._work += total
        data = self._window(offset, total, block)
        if len(data) < total:
            return _Fail(f"ends inside a {fmt.describe_type(rtype)} record", incomplete=True)
        end_body = fmt.ENVELOPE_SIZE + body_len
        if zlib.crc32(data[fmt.ENVELOPE_SIZE : end_body], zlib.crc32(data[:8], self._seed)) != crc:
            return _Fail(f"{fmt.describe_type(rtype)} record fails its CRC")
        if any(data[end_body:total]):
            return _Fail(f"{fmt.describe_type(rtype)} record padding is not zero")
        if rtype < fmt.CRITICAL_LIMIT and rtype not in fmt.KNOWN_TYPES:
            return _Fail(f"record type {rtype:#04x} is not known to this version", unsupported=True)
        return _Raw(offset, offset + total, rtype, data[fmt.ENVELOPE_SIZE : end_body])

    # -- validation: the rules that need the records before this one --

    def _frame(
        self,
        body: memoryview,
        streams: Mapping[int, Stream],
        sources: Mapping[int, str],
        last_seq: int,
    ) -> Frame:
        seq, time_ns, sid, src, bits, reserved, mid, orig_len = fmt.FRAME_FIXED.unpack_from(body, 0)
        size = len(body) - fmt.FRAME_FIXED_SIZE
        if reserved:
            raise _Bad("frame reserved field is not zero")
        if bits & ~KNOWN_FLAG_BITS:
            raise _Bad(f"frame has unknown flag bits {bits:#x}")
        if seq <= last_seq:
            raise _Bad(f"frame sequence {seq} does not increase past {last_seq}")
        stream = streams.get(sid)
        if stream is None:
            raise _Bad(f"frame names undefined stream {sid}")
        if src == fmt.OTHER_SOURCE:
            source = fmt.OTHER_SOURCE_TEXT
        else:
            found = sources.get(src)
            if found is None:
                raise _Bad(f"frame names undefined source {src}")
            source = found
        if bits & FLAG_HAS_MID:
            peeked: int | None = mid
        elif mid:
            raise _Bad("frame has a MID but no HAS_MID flag")
        else:
            peeked = None
        if bits & FLAG_TRUNCATED:
            if orig_len <= size or orig_len <= self._max_packet:
                raise _Bad(
                    f"frame is marked truncated but its original length {orig_len} is not "
                    f"above the {size} bytes kept and the {self._max_packet} byte cap"
                )
        elif orig_len != size:
            raise _Bad(f"frame original length {orig_len} disagrees with its {size} bytes")
        if bits & (FLAG_GARBAGE | FLAG_STALE):
            if stream.transport != SERIAL_TRANSPORT:
                raise _Bad("frame has serial flags on a stream that is not serial")
            if bits & FLAG_GARBAGE and bits & (FLAG_STALE | FLAG_HAS_MID | FLAG_TRUNCATED):
                raise _Bad("frame flags contradict each other")
        return Frame(
            seq,
            time_ns,
            stream,
            source,
            peeked,
            FLAGS_BY_BITS[bits],
            orig_len,
            bytes(body[fmt.FRAME_FIXED_SIZE :]),
        )

    def _validate(
        self,
        raw: _Raw,
        streams: Mapping[int, Stream],
        sources: Mapping[int, str],
        last_seq: int,
    ) -> tuple[Any, _Effect]:
        """The value of ``raw`` and what it changes, or :class:`_Bad`. Changes nothing itself."""
        rtype, body = raw.rtype, raw.body
        try:
            match rtype:
                case RecordType.FRAME:
                    frame = self._frame(body, streams, sources, last_seq)
                    return frame, ("seq", frame.seq)
                case RecordType.STREAM:
                    stream = fmt.decode_stream(body)
                    known = streams.get(stream.id)
                    if known is not None and known != stream:
                        raise _Bad(f"stream {stream.id} is defined twice, differently")
                    return stream, ("stream", stream)
                case RecordType.SOURCE:
                    source_id, text = fmt.decode_source(body)
                    known_text = sources.get(source_id)
                    if known_text is not None and known_text != text:
                        raise _Bad(f"source {source_id} is defined twice, differently")
                    return (source_id, text), ("source", source_id, text)
                case RecordType.GAP:
                    gap = fmt.decode_gap(body)
                    if gap.stream != fmt.NO_STREAM and gap.stream not in streams:
                        raise _Bad(f"GAP names undefined stream {gap.stream}")
                    return gap, None
                case RecordType.BLOB:
                    return fmt.decode_blob(body, hashlib.sha256), None
                case RecordType.CHUNK:
                    chunk = fmt.decode_chunk(body)
                    if not fmt.PREAMBLE_SIZE <= chunk.chunk_start <= raw.offset:
                        raise _Bad(f"CHUNK starts at {chunk.chunk_start}, outside the file so far")
                    return chunk, None
                case RecordType.MARK:
                    return fmt.decode_mark(body), None
                case RecordType.INDEX:
                    offsets = fmt.decode_index(body)
                    if offsets and not (
                        offsets[0] >= fmt.PREAMBLE_SIZE and offsets[-1] < raw.offset
                    ):
                        raise _Bad("INDEX entry outside the file so far")
                    return offsets, None
                case RecordType.TRAILER:
                    trailer = fmt.decode_trailer(body)
                    if trailer.index_offset >= raw.offset:
                        raise _Bad("TRAILER index offset is outside the file so far")
                    return trailer, None
                case RecordType.META:
                    raise _Bad("a second META record")
                case _:
                    return None, None  # a skippable record this version does not know
        except fmt.FormatError as exc:
            raise _Bad(str(exc)) from exc

    def _apply(self, effect: _Effect) -> None:
        if effect is None:
            return
        match effect[0]:
            case "seq":
                self._state.last_seq = effect[1]
            case "stream":
                self._state.streams[effect[1].id] = effect[1]
            case "source":
                self._state.sources[effect[1]] = effect[2]
            case _:
                pass

    # -- resynchronising after damage --

    def _chain_valid(self, offset: int) -> bool:
        """Whether a record at ``offset`` passes every rule and is followed by another one."""
        first = self._read_raw(offset, SMALL_BLOCK)
        if isinstance(first, _Fail):
            return False
        state = self._state
        try:
            _, effect = self._validate(first, state.streams, state.sources, state.last_seq)
        except _Bad:
            return False
        streams: Mapping[int, Stream] = state.streams
        sources: Mapping[int, str] = state.sources
        last_seq = state.last_seq
        if effect is not None:
            match effect[0]:
                case "seq":
                    last_seq = effect[1]
                case "stream":
                    streams = ChainMap({effect[1].id: effect[1]}, state.streams)
                case "source":
                    sources = ChainMap({effect[1]: effect[2]}, state.sources)
                case _:
                    pass
        second = self._read_raw(first.end, SMALL_BLOCK)
        if isinstance(second, _Fail):
            return False
        try:
            self._validate(second, streams, sources, last_seq)
        except _Bad:
            return False
        return True

    def _over_budget(self) -> bool:
        """Whether this pass has done more work than its file size allows a resync."""
        return self._work > WORK_FLOOR + WORK_FACTOR * self.size()

    def _resync(self, damaged_at: int) -> tuple[int | None, bool]:
        """The offset of the next valid record after damage at ``damaged_at``, or ``None``.

        The second value says the scan gave up (the pass's work budget ran out on implausible
        record headers) instead of reaching the end of the file.
        """
        position = (damaged_at + fmt.ALIGN) & ~(fmt.ALIGN - 1)
        step = SMALL_BLOCK  # doubles up to a block: a record close by is cheap, a long scan fast
        while True:
            try:
                block = os.pread(self._fd, step + 16, position)
            except OSError as exc:
                raise IronbirdError(
                    Code.REC_FORMAT,
                    f"cannot read {_shown(self.path)}: {exc.strerror or exc}",
                    cause=exc,
                ) from exc
            if len(block) < fmt.ENVELOPE_SIZE:
                return None, False
            for match in _CANDIDATE.finditer(block, 4, step + 7):
                candidate = position + match.start() - 4
                if candidate & (fmt.ALIGN - 1):
                    continue
                self._work += CANDIDATE_COST
                if self._over_budget():
                    return None, True
                if self._chain_valid(candidate):
                    return candidate, False
            if len(block) <= step:
                return None, False
            position += step
            step = min(step * 2, READ_BLOCK)

    # -- iteration --

    def _note_damage(self, damage: Damage) -> None:
        self.damage_count += 1
        self.damage_bytes += damage.span
        if len(self.damage) < MAX_DAMAGE_KEPT:
            self.damage.append(damage)

    def items(self) -> Iterator[Item | Damage]:
        """Every valid record after META in file order, and a :class:`Damage` where the file is
        not valid records. Stops at damage unless ``skip_damage``; a torn tail always ends it."""
        state = self._state
        offset = self._start
        while True:
            raw = self._read_raw(offset)
            reason: str | None = None
            incomplete = False
            value: Any = None
            effect: _Effect = None
            if isinstance(raw, _Fail):
                if raw.eof:
                    return
                if raw.unsupported:
                    raise _unsupported(self.path, raw.reason)
                reason, incomplete = raw.reason, raw.incomplete
            else:
                try:
                    value, effect = self._validate(
                        raw, state.streams, state.sources, state.last_seq
                    )
                except _Bad as bad:
                    reason = str(bad)
            if reason is not None:
                found, gave_up = self._resync(offset)
                size = max(self.size(), offset)
                if found is None:
                    gave = " (the scan for a valid record gave up)" if gave_up else ""
                    damage = Damage(
                        offset, size - offset, not gave_up, reason + gave, None, False, incomplete
                    )
                    self._note_damage(damage)
                    yield damage
                    return
                damage = Damage(offset, found - offset, False, reason, found, False, incomplete)
                self._note_damage(damage)
                yield damage
                if not self._skip_damage:
                    return
                offset = found
                continue
            assert isinstance(raw, _Raw)
            self._apply(effect)
            yield Item(raw.offset, raw.end, raw.rtype, value)
            offset = raw.end
            if raw.rtype == RecordType.TRAILER:
                if self.trailer is None:
                    self.trailer = value
                extra = self.size() - offset
                if extra <= 0:
                    return
                # Bytes after a trailer: another file appended, or a trailer that was not the
                # last record. Reading on (when asked to) finds whatever valid records follow.
                found = None
                if self._skip_damage:
                    found, _ = self._resync(offset - fmt.ALIGN)
                resumed = Damage(
                    offset,
                    (found if found is not None else offset + extra) - offset,
                    False,
                    "bytes after the trailer",
                    found,
                    True,
                )
                self._note_damage(resumed)
                yield resumed
                if found is None:
                    return
                offset = found

    def frames(self) -> Iterator[Frame]:
        """Every valid frame in file order. Damage ends the iteration (see :attr:`damage`) unless
        ``skip_damage`` was given."""
        for item in self.items():
            if isinstance(item, Item) and item.rtype == RecordType.FRAME:
                yield item.value

    # -- random access for the index (a hint, never trusted) --

    def read_trailer(self) -> tuple[int, fmt.Trailer] | None:
        """The trailer and its offset when the file ends in a valid one, else ``None``."""
        size = self.size()
        offset = size - fmt.record_size(fmt.TRAILER_SIZE)
        if offset < self._start or offset & (fmt.ALIGN - 1):
            return None
        raw = self._read_raw(offset, SMALL_BLOCK)
        if isinstance(raw, _Fail) or raw.rtype != RecordType.TRAILER or raw.end != size:
            return None
        trailer = fmt.decode_trailer(raw.body)
        if trailer.index_offset and not self._start <= trailer.index_offset < offset:
            return None
        return offset, trailer

    def read_chunks(self, trailer: fmt.Trailer, trailer_offset: int) -> list[tuple[int, fmt.Chunk]]:
        """The chunk summaries the index points at, each checked, as ``(offset, chunk)``.

        Raises :class:`ValueError` (message: why) when the index or a chunk it names is not
        usable, so the caller falls back to a forward scan.
        """
        if not trailer.index_offset:
            raise ValueError("the trailer names no index")
        raw = self._read_raw(trailer.index_offset, SMALL_BLOCK)
        if isinstance(raw, _Fail) or raw.rtype != RecordType.INDEX:
            raise ValueError("the trailer does not point at an index record")
        if raw.end != trailer_offset:
            raise ValueError("the index does not end where the trailer begins")
        try:
            offsets = fmt.decode_index(raw.body)
        except fmt.FormatError as exc:
            raise ValueError(str(exc)) from exc
        chunks: list[tuple[int, fmt.Chunk]] = []
        previous_end = fmt.PREAMBLE_SIZE
        for offset in offsets:
            if not previous_end <= offset < trailer.index_offset:
                raise ValueError(f"index entry {offset} is out of order or outside the file")
            record = self._read_raw(offset, SMALL_BLOCK)
            if isinstance(record, _Fail) or record.rtype != RecordType.CHUNK:
                raise ValueError(f"index entry {offset} is not a CHUNK record")
            try:
                chunk = fmt.decode_chunk(record.body)
            except fmt.FormatError as exc:
                raise ValueError(str(exc)) from exc
            if chunk.chunk_start != previous_end:
                raise ValueError(f"chunk at {offset} does not start where the last one ended")
            chunks.append((offset, chunk))
            previous_end = record.end
        if previous_end != trailer.index_offset:
            raise ValueError("the index does not end at the last chunk")
        return chunks

    def closes_cleanly_at(self, offset: int) -> bool:
        """Whether the file from ``offset`` is exactly an optional INDEX and then a TRAILER that
        ends it, the way a recording is closed (so nothing follows the valid records)."""
        raw = self._read_raw(offset, SMALL_BLOCK)
        if not isinstance(raw, _Fail) and raw.rtype == RecordType.INDEX:
            raw = self._read_raw(raw.end, SMALL_BLOCK)
        return (
            not isinstance(raw, _Fail)
            and raw.rtype == RecordType.TRAILER
            and raw.end == self.size()
        )

    def head_records(self, start: int, stop: int) -> Iterator[Item]:
        """The leading non-frame records of a chunk (``start`` up to its first FRAME or ``stop``),
        read without the stream state; for tools that summarise a chunk without reading it."""
        offset = start
        while offset < stop:
            raw = self._read_raw(offset, SMALL_BLOCK)
            if isinstance(raw, _Fail) or raw.rtype not in fmt.HEAD_TYPES:
                return
            try:
                match raw.rtype:
                    case RecordType.STREAM:
                        value: Any = fmt.decode_stream(raw.body)
                    case RecordType.SOURCE:
                        value = fmt.decode_source(raw.body)
                    case RecordType.GAP:
                        value = fmt.decode_gap(raw.body)
                    case RecordType.MARK:
                        value = fmt.decode_mark(raw.body)
                    case RecordType.BLOB:
                        value = fmt.decode_blob(raw.body, hashlib.sha256)
                    case _:
                        value = None
            except fmt.FormatError:
                return
            yield Item(raw.offset, raw.end, raw.rtype, value)
            offset = raw.end


def _shown(path: Path) -> str:
    return clean(str(path), 300)


def _unsupported(path: Path, reason: str) -> IronbirdError:
    return IronbirdError(
        Code.REC_VERSION,
        f"{_shown(path)}: {reason}; it was written by a newer ironbird, update to read it",
    )


def open_reader(path: Path | str, *, skip_damage: bool = False) -> RecordingReader:
    """Open a recording for reading (a thin alias that keeps call sites short)."""
    return RecordingReader(path, skip_damage=skip_damage)


__all__ = ["READ_BLOCK", "Damage", "Item", "RecordingReader", "open_reader"]
