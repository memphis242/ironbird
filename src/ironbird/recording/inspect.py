"""Looking inside recordings: ``ib rec info``, ``ib rec check`` and ``ib rec repair``.

``check`` is the full pass: every record's CRC and rules through :class:`RecordingReader`, plus
the cross-checks only a whole-file reading can make (each chunk summary against the frames it
summarises, the index against the chunks, the trailer against the totals, and sequence holes
that no GAP record explains). ``info`` answers from the trailer, the index and the chunk
summaries when they are valid (it reads no frames), and falls back to the full pass when they
are not. ``repair`` writes the valid prefix plus a fresh index and trailer to a new file and
never modifies the original.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from ironbird.atomicfile import Collision, TempOutput
from ironbird.errors import Code, IronbirdError
from ironbird.frame import GapReason, Stream
from ironbird.recording import format as fmt
from ironbird.recording.format import RecordType
from ironbird.recording.reader import Damage, Item, RecordingReader
from ironbird.sanitize import clean

MAX_FINDINGS: Final = 100
MAX_HOLES: Final = 1000
MAX_TRACKED_MIDS: Final = 65536
MAX_CHUNK_TRACKED_MIDS: Final = fmt.MAX_CHUNK_MIDS + 1
MAX_LISTED: Final = 200  # gaps and marks kept for display
COPY_BLOCK: Final = 1 << 20
_SEQ_GAP_REASONS: Final = frozenset({GapReason.RECORDER_OVERRUN, GapReason.NOT_RECORDED})


@dataclass(frozen=True, slots=True)
class BlobInfo:
    kind: str
    node: str
    adapter: str
    sha256: str
    size: int


@dataclass(slots=True)
class Report:
    """What a pass over one recording found. ``source`` is ``scan`` (every record read) or
    ``index`` (only the trailer, index and chunk summaries)."""

    path: Path
    size: int
    version: tuple[int, int]
    meta: dict[str, Any]
    source: str = "scan"
    writer_active: bool = False
    frames: int = 0
    data_bytes: int = 0
    first_seq: int = 0
    last_seq: int = 0
    first_ns: int = 0
    last_ns: int = 0
    chunks: int = 0
    mids: dict[int, int] = field(default_factory=dict)
    mids_dropped: int = 0  # frames of MIDs past the table cap (scan only)
    frames_without_mid: int = 0
    streams: dict[int, Stream] = field(default_factory=dict)
    gaps: list[fmt.Gap] = field(default_factory=list)
    gap_total: int = 0
    marks: list[fmt.Mark] = field(default_factory=list)
    blobs: list[BlobInfo] = field(default_factory=list)
    trailer: fmt.Trailer | None = None
    damage: list[Damage] = field(default_factory=list)
    findings: list[str] = field(default_factory=list)
    findings_total: int = 0
    holes: list[tuple[int, int]] = field(default_factory=list)
    hole_frames: int = 0
    index: str = "missing"  # "ok" | "missing" | "invalid"
    index_note: str = ""

    @property
    def lost_by_reason(self) -> dict[str, int]:
        lost: dict[str, int] = {}
        for gap in self.gaps:
            lost[gap.reason.name.lower()] = lost.get(gap.reason.name.lower(), 0) + gap.lost
        return lost

    @property
    def torn(self) -> bool:
        return any(d.torn for d in self.damage)

    @property
    def mid_file_damage(self) -> bool:
        return any(not d.torn for d in self.damage)

    @property
    def status(self) -> str:
        """``closed``, ``recording`` (a writer holds it), ``torn``, ``unclosed`` (ends cleanly
        with no trailer), ``damaged`` (damage mid-file) or ``inconsistent`` (a closed file whose
        summaries disagree with its records)."""
        if self.mid_file_damage:
            return "damaged"
        if self.writer_active and self.trailer is None:
            return "recording"
        if self.torn:
            return "torn"
        if self.trailer is None:
            return "unclosed"
        return "inconsistent" if self.findings_total or self.hole_frames else "closed"

    def find(self, text: str) -> None:
        self.findings_total += 1
        if len(self.findings) < MAX_FINDINGS:
            self.findings.append(clean(text, 300))

    def add_mid(self, mid: int, count: int) -> None:
        if mid in self.mids or len(self.mids) < MAX_TRACKED_MIDS:
            self.mids[mid] = self.mids.get(mid, 0) + count
        else:
            self.mids_dropped += count


def _blob_info(blob: fmt.Blob) -> BlobInfo:
    return BlobInfo(
        blob.kind.name.lower(), blob.node, blob.adapter, blob.sha256.hex(), len(blob.content)
    )


def _new_report(reader: RecordingReader, source: str) -> Report:
    return Report(
        reader.path,
        reader.size(),
        reader.version,
        dict(reader.meta.data),
        source,
        reader.writer_active,
    )


class _ChunkTally:
    """The frames seen since the last CHUNK record, to check the next one against."""

    __slots__ = (
        "bytes",
        "frames",
        "max_ns",
        "mids",
        "min_ns",
        "no_mid",
        "over",
        "seq_first",
        "seq_last",
        "start",
        "tainted",
    )

    def __init__(self, start: int) -> None:
        self.start = start
        self.tainted = False  # damage was skipped inside this chunk: its summary cannot match
        self.reset(start)

    def reset(self, start: int) -> None:
        self.start = start
        self.frames = self.bytes = self.no_mid = 0
        self.seq_first = self.seq_last = self.min_ns = self.max_ns = 0
        self.mids: dict[int, int] = {}
        self.over = False
        self.tainted = False

    def add(self, item: Item) -> None:
        frame = item.value
        if not self.frames:
            self.seq_first, self.min_ns, self.max_ns = frame.seq, frame.time_ns, frame.time_ns
        self.min_ns = min(self.min_ns, frame.time_ns)
        self.max_ns = max(self.max_ns, frame.time_ns)
        self.seq_last = frame.seq
        self.frames += 1
        self.bytes += item.end - item.offset
        if frame.mid is None:
            self.no_mid += 1
        elif frame.mid in self.mids:
            self.mids[frame.mid] += 1
        elif len(self.mids) < MAX_CHUNK_TRACKED_MIDS:
            self.mids[frame.mid] = 1
        else:
            self.over = True

    def verify(self, chunk: fmt.Chunk, offset: int, report: Report) -> None:
        where = f"chunk at {offset}"
        if self.tainted:
            return
        if chunk.chunk_start != self.start:
            report.find(f"{where} says it starts at {chunk.chunk_start}, not {self.start}")
        if chunk.frames != self.frames or chunk.bytes != self.bytes:
            report.find(
                f"{where} summarises {chunk.frames} frames / {chunk.bytes} bytes but holds "
                f"{self.frames} / {self.bytes}"
            )
            return
        if self.frames and (
            (chunk.first_seq, chunk.last_seq) != (self.seq_first, self.seq_last)
            or (chunk.min_time_ns, chunk.max_time_ns) != (self.min_ns, self.max_ns)
        ):
            report.find(f"{where} has sequence or time bounds that its frames do not have")
        if chunk.frames_without_mid != self.no_mid:
            report.find(
                f"{where} counts {chunk.frames_without_mid} frames without a MID, not {self.no_mid}"
            )
        for mid, count in chunk.mids:
            seen = self.mids.get(mid)
            if (seen is not None and seen != count) or (seen is None and not self.over):
                report.find(f"{where} counts {count} frames of MID {mid:#x}, not {seen or 0}")
        if not chunk.mid_overflow and not self.over and len(chunk.mids) != len(self.mids):
            report.find(f"{where} lists {len(chunk.mids)} MIDs but holds {len(self.mids)}")
        if self.over and not chunk.mid_overflow:
            report.find(f"{where} omits MIDs without its overflow flag")


def scan(path: Path | str, *, reader: RecordingReader | None = None) -> Report:
    """The full pass: read every record with every rule and cross-check the summaries.

    Damage does not stop it (the reader resumes at the next valid chain), so one pass reports
    every damaged span. Only a file that cannot be read at all raises (``IB-REC-004`` to
    ``IB-REC-007`` from the reader).
    """
    own = reader is None
    rd = reader or RecordingReader(path, skip_damage=True)
    try:
        return _scan(rd)
    finally:
        if own:
            rd.close()


def _scan(rd: RecordingReader) -> Report:
    report = _new_report(rd, "scan")
    tally = _ChunkTally(fmt.PREAMBLE_SIZE)
    chunk_offsets: list[int] = []
    index_item: tuple[int, tuple[int, ...]] | None = None
    after_damage = False
    previous_seq = int(rd.meta.previous_last_seq)
    seen_frame = False
    for item in rd.items():
        if isinstance(item, Damage):
            report.damage.append(item)
            tally.tainted = True
            after_damage = True
            continue
        rtype, value = item.rtype, item.value
        match rtype:
            case RecordType.FRAME:
                seq = value.seq
                if seq != previous_seq + 1 and not after_damage:
                    _hole(report, previous_seq + 1, seq - 1)
                after_damage = False
                previous_seq = seq
                if not seen_frame:
                    report.first_seq = seq
                    report.first_ns = report.last_ns = value.time_ns
                    seen_frame = True
                report.last_seq = seq
                report.first_ns = min(report.first_ns, value.time_ns)
                report.last_ns = max(report.last_ns, value.time_ns)
                report.frames += 1
                report.data_bytes += len(value.data)
                if value.mid is None:
                    report.frames_without_mid += 1
                else:
                    report.add_mid(value.mid, 1)
                tally.add(item)
            case RecordType.STREAM:
                report.streams[value.id] = value
            case RecordType.GAP:
                report.gap_total += 1
                if len(report.gaps) < MAX_LISTED:
                    report.gaps.append(value)
            case RecordType.MARK:
                if len(report.marks) < MAX_LISTED:
                    report.marks.append(value)
            case RecordType.BLOB:
                report.blobs.append(_blob_info(value))
            case RecordType.CHUNK:
                report.chunks += 1
                tally.verify(value, item.offset, report)
                chunk_offsets.append(item.offset)
                tally.reset(item.end)
            case RecordType.INDEX:
                index_item = (item.offset, value)
            case RecordType.TRAILER:
                report.trailer = value
            case _:
                pass
    _cross_check(report, rd, chunk_offsets, index_item)
    return report


def _hole(report: Report, first: int, last: int) -> None:
    report.hole_frames += last - first + 1
    if len(report.holes) < MAX_HOLES:
        report.holes.append((first, last))


def _cross_check(
    report: Report,
    rd: RecordingReader,
    chunk_offsets: list[int],
    index_item: tuple[int, tuple[int, ...]] | None,
) -> None:
    damaged = bool(report.damage)
    trailer = report.trailer
    if trailer is None:
        report.index = "missing"
        report.index_note = "no trailer: the recording was not closed cleanly"
    elif index_item is None:
        report.index = "missing" if not trailer.index_offset else "invalid"
        report.index_note = (
            "the trailer names no index" if not trailer.index_offset else "no index record found"
        )
    else:
        offset, entries = index_item
        if trailer.index_offset != offset:
            report.index, report.index_note = "invalid", "the trailer points elsewhere"
        elif not damaged and list(entries) != chunk_offsets:
            report.index, report.index_note = "invalid", "the index lists different chunks"
        else:
            report.index = "ok"
    if trailer is not None and not damaged:
        if trailer.frame_count != report.frames:
            report.find(
                f"the trailer counts {trailer.frame_count} frames, the file holds {report.frames}"
            )
        if trailer.last_seq != report.last_seq:
            report.find(
                f"the trailer's last sequence is {trailer.last_seq}, "
                f"the file's is {report.last_seq}"
            )
    if report.index == "invalid" and trailer is not None:
        report.find(f"index: {report.index_note}")
    _explain_holes(report)


def _explain_holes(report: Report) -> None:
    """Drop the holes that GAP records account for (frames the recorder dropped on purpose)."""
    ranges = sorted((g.first_seq, g.last_seq) for g in report.gaps if g.reason in _SEQ_GAP_REASONS)
    merged: list[list[int]] = []
    for first, last in ranges:
        if merged and first <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], last)
        else:
            merged.append([first, last])
    unexplained: list[tuple[int, int]] = []
    lost = 0
    for first, last in report.holes:
        cursor = first
        for gap_first, gap_last in merged:
            if gap_last < cursor:
                continue
            if gap_first > cursor:
                unexplained.append((cursor, min(last, gap_first - 1)))
                lost += min(last, gap_first - 1) - cursor + 1
            cursor = max(cursor, gap_last + 1)
            if cursor > last:
                break
        if cursor <= last:
            unexplained.append((cursor, last))
            lost += last - cursor + 1
    report.holes = unexplained
    report.hole_frames = lost


def info(path: Path | str) -> Report:
    """A summary from the trailer, index and chunk summaries when valid, else the full pass."""
    reader = RecordingReader(path)
    try:
        located = reader.read_trailer()
        if located is None:
            note = "no valid trailer at the end of the file"
        else:
            try:
                return _from_index(reader, *located)
            except ValueError as exc:
                note = str(exc)
    finally:
        reader.close()
    report = scan(path)
    if report.index != "ok" and not report.index_note:
        report.index_note = note
    return report


def _from_index(reader: RecordingReader, trailer_offset: int, trailer: fmt.Trailer) -> Report:
    chunks = reader.read_chunks(trailer, trailer_offset)
    report = _new_report(reader, "index")
    report.trailer = trailer
    report.index = "ok"
    report.chunks = len(chunks)
    seen_streams: set[int] = set()
    for offset, chunk in chunks:
        if chunk.frames:
            if not report.frames:
                report.first_seq, report.first_ns, report.last_ns = (
                    chunk.first_seq,
                    chunk.min_time_ns,
                    chunk.max_time_ns,
                )
            report.last_seq = chunk.last_seq
            report.first_ns = min(report.first_ns, chunk.min_time_ns)
            report.last_ns = max(report.last_ns, chunk.max_time_ns)
        report.frames += chunk.frames
        report.frames_without_mid += chunk.frames_without_mid
        for mid, count in chunk.mids:
            report.add_mid(mid, count)
        if chunk.mid_overflow:
            report.mids_dropped += 1  # at least one frame is not in the table
        for item in reader.head_records(chunk.chunk_start, offset):
            _head_item(report, item, seen_streams)
    if trailer.frame_count != report.frames:
        raise ValueError("the trailer's frame count disagrees with the chunk summaries")
    return report


def _head_item(report: Report, item: Item, seen_streams: set[int]) -> None:
    match item.rtype:
        case RecordType.STREAM:
            if item.value.id not in seen_streams:
                seen_streams.add(item.value.id)
                report.streams[item.value.id] = item.value
        case RecordType.GAP:
            report.gap_total += 1
            if len(report.gaps) < MAX_LISTED:
                report.gaps.append(item.value)
        case RecordType.MARK:
            if len(report.marks) < MAX_LISTED:
                report.marks.append(item.value)
        case RecordType.BLOB:
            if not any(b.sha256 == item.value.sha256.hex() for b in report.blobs):
                report.blobs.append(_blob_info(item.value))
        case _:
            pass


# -- repair ------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RepairResult:
    output: Path
    frames: int  # frames in the new file
    bytes_copied: int
    size: int
    dropped_bytes: int  # bytes of the original after the valid prefix
    damage: Damage | None
    frames_beyond: int  # valid frames after mid-file damage, left only in the original


def repair(source: Path, output: Path, *, force: bool = False) -> RepairResult:
    """Write the valid prefix of ``source`` plus a fresh index and trailer to ``output``.

    ``source`` is never modified. An existing ``output`` is refused unless ``force`` (and then
    only a regular file is replaced); ``output`` being ``source`` is always refused. Raises
    ``IB-REC-001`` for the refusals and a failed write.
    """
    _refuse_same_file(source, output)
    if output.exists() and not force:
        raise IronbirdError(Code.REC_CREATE, f"{output} already exists; pass --force to replace it")
    reader = RecordingReader(source)
    try:
        prefix_end, chunk_offsets, frames, last_seq, tail = _valid_prefix(reader)
        damage = reader.damage[0] if reader.damage else None
        if damage is not None and not damage.torn and not damage.trailing:
            beyond = _frames_after(source, last_seq)
        else:
            beyond = 0
        suffix = _closing_records(reader, prefix_end, chunk_offsets, frames, last_seq, tail)
        size = reader.size()
        total = prefix_end + len(suffix)
        try:
            with TempOutput(output, force=force) as out:
                _copy(reader, out, prefix_end)
                out.write(suffix)
                out.commit()
        except Collision as exc:
            raise IronbirdError(
                Code.REC_CREATE,
                f"{output} appeared while repairing; it was not replaced (pass --force)",
                cause=exc,
            ) from exc
        except OSError as exc:
            raise IronbirdError(
                Code.REC_CREATE,
                f"cannot write the repaired recording {output}: {exc.strerror or exc}",
                cause=exc,
            ) from exc
        dropped = size - prefix_end if damage is not None else 0  # a clean file's index is rebuilt
        return RepairResult(output, frames, prefix_end, total, dropped, damage, beyond)
    finally:
        reader.close()


def _refuse_same_file(source: Path, output: Path) -> None:
    try:
        same = output.exists() and os.path.samefile(source, output)
    except OSError:
        same = False
    if same:
        raise IronbirdError(
            Code.REC_CREATE,
            f"{output} is the recording being repaired; the original is never modified, "
            "choose another output",
        )


class _Tail:
    """The records after the last CHUNK of the valid prefix, which a fresh CHUNK must summarise."""

    __slots__ = ("records", "tally")

    def __init__(self, start: int) -> None:
        self.tally = _ChunkTally(start)
        self.records = 0


def _valid_prefix(reader: RecordingReader) -> tuple[int, list[int], int, int, _Tail]:
    """Where the valid records end (before the first damage, INDEX or TRAILER), the chunk
    offsets seen, the frame count, the last sequence number and the unsummarised tail."""
    offsets: list[int] = []
    frames = 0
    last_seq = 0
    tail = _Tail(fmt.PREAMBLE_SIZE)
    # META is the first record and is part of the first chunk.
    end = reader.body_start
    tail.records = 1
    for item in reader.items():
        if isinstance(item, Damage):
            break
        if item.rtype in (RecordType.INDEX, RecordType.TRAILER):
            break
        end = item.end
        if item.rtype == RecordType.CHUNK:
            offsets.append(item.offset)
            tail = _Tail(item.end)
            continue
        tail.records += 1
        if item.rtype == RecordType.FRAME:
            frames += 1
            last_seq = item.value.seq
            tail.tally.add(item)
    return end, offsets, frames, last_seq, tail


def _closing_records(
    reader: RecordingReader,
    prefix_end: int,
    chunk_offsets: list[int],
    frames: int,
    last_seq: int,
    tail: _Tail,
) -> bytes:
    seed = fmt.seed_of(reader.nonce)
    parts: list[bytes] = []
    offset = prefix_end
    offsets = list(chunk_offsets)
    if tail.records:
        tally = tail.tally
        chunk = fmt.Chunk(
            tally.start,
            tally.seq_first,
            tally.seq_last,
            tally.min_ns,
            tally.max_ns,
            0,
            0,
            tally.frames,
            tally.bytes,
            tally.no_mid,
            fmt.CHUNK_FLAG_MID_OVERFLOW if tally.over else 0,
            tuple(sorted(tally.mids.items()))[: fmt.MAX_CHUNK_MIDS],
        )
        if len(tally.mids) > fmt.MAX_CHUNK_MIDS:
            chunk = chunk._replace(flags=fmt.CHUNK_FLAG_MID_OVERFLOW)
        record = fmt.encode_record(RecordType.CHUNK, fmt.encode_chunk(chunk), seed)
        offsets.append(offset)
        offset += len(record)
        parts.append(record)
    index_offset = 0
    if len(offsets) <= fmt.MAX_INDEX_ENTRIES:
        index_offset = offset
        record = fmt.encode_record(RecordType.INDEX, fmt.encode_index(offsets), seed)
        offset += len(record)
        parts.append(record)
    trailer = fmt.Trailer(index_offset, frames, last_seq)
    parts.append(fmt.encode_record(RecordType.TRAILER, fmt.encode_trailer(trailer), seed))
    return b"".join(parts)


def _copy(reader: RecordingReader, out: TempOutput, length: int) -> None:
    offset = 0
    while offset < length:
        block = reader.read_at(offset, min(COPY_BLOCK, length - offset))
        if not block:
            raise OSError(5, "the recording is shorter than the valid prefix measured")
        out.write(block)
        offset += len(block)


def _frames_after(source: Path, last_seq: int) -> int:
    """Valid frames beyond the first mid-file damage (a second pass that skips damage)."""
    count = 0
    reader = RecordingReader(source, skip_damage=True)
    try:
        for frame in reader.frames():
            if frame.seq > last_seq:
                count += 1
    finally:
        reader.close()
    return count
