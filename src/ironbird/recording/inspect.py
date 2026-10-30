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
from bisect import bisect_left, bisect_right
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
MAX_HOLES: Final = 1000  # holes kept for display; the count covers them all
MAX_PENDING_HOLES: Final = 65536  # holes awaiting a GAP that may yet explain them
MAX_GAP_RANGES: Final = 65536  # merged sequence ranges the GAP records explain
MAX_TRACKED_MIDS: Final = 65536
MAX_CHUNK_TRACKED_MIDS: Final = fmt.MAX_CHUNK_MIDS + 1
MAX_LISTED: Final = 200  # gaps, marks, definitions and damage kept for display
MAX_TRACKED_BLOBS: Final = 4096  # distinct definition digests remembered to count each once
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
    gaps: list[fmt.Gap] = field(default_factory=list)  # the first MAX_LISTED
    gap_total: int = 0
    lost_by_reason: dict[str, int] = field(default_factory=dict)  # over every GAP record
    marks: list[fmt.Mark] = field(default_factory=list)  # the first MAX_LISTED
    mark_total: int = 0
    blobs: list[BlobInfo] = field(default_factory=list)  # the first MAX_LISTED distinct ones
    blob_total: int = 0
    trailer: fmt.Trailer | None = None
    damage: list[Damage] = field(default_factory=list)  # the first MAX_LISTED stretches
    damage_total: int = 0
    damage_bytes: int = 0
    findings: list[str] = field(default_factory=list)
    findings_total: int = 0
    holes: list[tuple[int, int]] = field(default_factory=list)  # unexplained, the first MAX_HOLES
    hole_frames: int = 0  # frames in unexplained holes, all of them
    index: str = "missing"  # "ok" | "missing" | "invalid"
    index_note: str = ""
    torn: bool = False
    mid_file_damage: bool = False
    blob_seen: set[bytes] = field(default_factory=set, repr=False)

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

    def add_damage(self, damage: Damage) -> None:
        self.damage_total += 1
        self.damage_bytes += damage.span
        if damage.torn:
            self.torn = True
        else:
            self.mid_file_damage = True
        if len(self.damage) < MAX_LISTED:
            self.damage.append(damage)

    def add_gap(self, gap: fmt.Gap) -> None:
        self.gap_total += 1
        why = gap.reason.name.lower()
        self.lost_by_reason[why] = self.lost_by_reason.get(why, 0) + gap.lost
        if len(self.gaps) < MAX_LISTED:
            self.gaps.append(gap)

    def add_mark(self, mark: fmt.Mark) -> None:
        self.mark_total += 1
        if len(self.marks) < MAX_LISTED:
            self.marks.append(mark)

    def add_blob(self, blob: fmt.Blob) -> None:
        """Count a definition once however often the file repeats it."""
        if blob.sha256 in self.blob_seen:
            return
        if len(self.blob_seen) < MAX_TRACKED_BLOBS:
            self.blob_seen.add(blob.sha256)
        self.blob_total += 1
        if len(self.blobs) < MAX_LISTED:
            self.blobs.append(_blob_info(blob))

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
    index_item: tuple[int, int, tuple[int, ...]] | None = None
    trailer_offset = 0
    index_offset = -1  # an INDEX just read, which only the trailer may follow
    holes = _Holes()
    after_damage = False
    previous_seq = int(rd.meta.previous_last_seq)
    seen_frame = False
    for item in rd.items():
        if isinstance(item, Damage):
            report.add_damage(item)
            tally.tainted = True
            after_damage = True
            continue
        rtype, value = item.rtype, item.value
        if index_offset >= 0:
            if rtype != RecordType.TRAILER:
                report.find(f"the INDEX at {index_offset} is followed by more records")
            index_offset = -1
        match rtype:
            case RecordType.FRAME:
                seq = value.seq
                if seq != previous_seq + 1 and not after_damage:
                    holes.add_hole(previous_seq + 1, seq - 1)
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
                report.add_gap(value)
                if value.reason in _SEQ_GAP_REASONS:
                    holes.add_gap(value.first_seq, value.last_seq)
            case RecordType.MARK:
                report.add_mark(value)
            case RecordType.BLOB:
                report.add_blob(value)
            case RecordType.CHUNK:
                report.chunks += 1
                tally.verify(value, item.offset, report)
                chunk_offsets.append(item.offset)
                tally.reset(item.end)
            case RecordType.INDEX:
                index_item = (item.offset, item.end, value)
                index_offset = item.offset
            case RecordType.TRAILER:
                report.trailer = value
                trailer_offset = item.offset
            case _:
                pass
    _cross_check(report, chunk_offsets, index_item, trailer_offset)
    holes.finish(report)
    return report


class _Holes:
    """Sequence numbers missing from the file that no GAP record accounts for.

    Holes are found as frames are read, GAP records as they appear (a GAP usually follows the
    frames it explains), so both are kept as sorted, disjoint intervals: the merged ranges the
    GAPs account for, and the holes still waiting for a GAP. A hole is tested against the ranges
    when it appears and a range against the waiting holes when it appears; what is left at the
    end is the unexplained set. Both sets are bounded, and a file that exceeds either is a
    finding (check fails) rather than a silent pass.
    """

    def __init__(self) -> None:
        self._gap_first: list[int] = []
        self._gap_last: list[int] = []
        self._hole_first: list[int] = []
        self._hole_last: list[int] = []
        self._overflow_holes = 0
        self._overflow_frames = 0
        self._ranges_untracked = 0

    def add_hole(self, first: int, last: int) -> None:
        """Frames ``first..last`` are missing; keep whatever no GAP seen so far explains."""
        assert first <= last
        cursor = first
        index = bisect_left(self._gap_last, first)
        while cursor <= last:
            if index < len(self._gap_first) and self._gap_first[index] <= last:
                if self._gap_first[index] > cursor:
                    self._keep(cursor, self._gap_first[index] - 1)
                cursor = self._gap_last[index] + 1
                index += 1
            else:
                self._keep(cursor, last)
                break

    def _keep(self, first: int, last: int) -> None:
        if len(self._hole_first) >= MAX_PENDING_HOLES:
            self._overflow_holes += 1
            self._overflow_frames += last - first + 1
            return
        self._hole_first.append(first)
        self._hole_last.append(last)

    def add_gap(self, first: int, last: int) -> None:
        """A GAP accounts for ``first..last``: merge it in and clear the holes it covers."""
        first_gap = bisect_left(self._gap_last, first - 1)
        end_gap = bisect_right(self._gap_first, last + 1)
        if first_gap < end_gap:
            first = min(first, self._gap_first[first_gap])
            last = max(last, self._gap_last[end_gap - 1])
        elif len(self._gap_first) >= MAX_GAP_RANGES:
            self._ranges_untracked += 1
            return
        self._gap_first[first_gap:end_gap] = [first]
        self._gap_last[first_gap:end_gap] = [last]
        start = bisect_left(self._hole_last, first)
        stop = bisect_right(self._hole_first, last)
        if start >= stop:
            return
        left = (self._hole_first[start], first - 1) if self._hole_first[start] < first else None
        right = (last + 1, self._hole_last[stop - 1]) if self._hole_last[stop - 1] > last else None
        left_over = [piece for piece in (left, right) if piece is not None]
        self._hole_first[start:stop] = [a for a, _ in left_over]
        self._hole_last[start:stop] = [b for _, b in left_over]

    def finish(self, report: Report) -> None:
        """Put what is unexplained into ``report`` (``holes``, ``hole_frames``) and say so when
        a bound was reached."""
        report.holes = list(zip(self._hole_first, self._hole_last, strict=True))[:MAX_HOLES]
        report.hole_frames = (
            sum(b - a + 1 for a, b in zip(self._hole_first, self._hole_last, strict=True))
            + self._overflow_frames
        )
        if self._overflow_holes:
            report.find(
                f"{self._overflow_holes} more sequence holes than can be tracked "
                f"({MAX_PENDING_HOLES}) were left unexplained"
            )
        if self._ranges_untracked:
            report.find(
                f"{self._ranges_untracked} GAP ranges beyond the {MAX_GAP_RANGES} that can be "
                "tracked, so sequence holes cannot be fully checked"
            )


def _cross_check(
    report: Report,
    chunk_offsets: list[int],
    index_item: tuple[int, int, tuple[int, ...]] | None,
    trailer_offset: int,
) -> None:
    damaged = report.damage_total > 0
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
        offset, end, entries = index_item
        if trailer.index_offset != offset:
            report.index, report.index_note = "invalid", "the trailer points elsewhere"
        elif end != trailer_offset:
            report.index, report.index_note = "invalid", "the index does not end at the trailer"
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
    elif report.index == "ok" and located is not None:
        # The scan found nothing wrong with the index's own records, but its summaries did not
        # hold up against the chunks, so info did not use it.
        report.index, report.index_note = "invalid", note
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
            report.add_gap(item.value)
        case RecordType.MARK:
            report.add_mark(item.value)
        case RecordType.BLOB:
            report.add_blob(item.value)
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
    if os.path.lexists(output) and not force:
        raise IronbirdError(Code.REC_CREATE, f"{output} already exists; pass --force to replace it")
    reader = RecordingReader(source)
    try:
        prefix_end, chunk_offsets, frames, last_seq, tail = _valid_prefix(reader)
        damage = reader.damage[0] if reader.damage else None
        size = reader.size()
        if damage is None and prefix_end < size and not reader.closes_cleanly_at(prefix_end):
            # The valid records stop at an INDEX or TRAILER that is not the end of the file: a
            # stale index, a trailer in mid-file, or another file appended. All of it is left out.
            damage = Damage(
                prefix_end,
                size - prefix_end,
                False,
                "an INDEX or TRAILER record is followed by more data",
                None,
                True,
            )
        beyond = _frames_after(source, last_seq) if damage is not None and not damage.torn else 0
        suffix = _closing_records(reader, prefix_end, chunk_offsets, frames, last_seq, tail)
        total = prefix_end + len(suffix)
        try:
            with TempOutput(output, force=force) as out:
                _copy(reader, out, prefix_end)
                out.write(suffix)
                out.commit()
        except Collision as exc:
            raise IronbirdError(
                Code.REC_CREATE,
                f"{output} is not a regular file; --force replaces only a regular file"
                if force
                else f"{output} appeared while repairing; it was not replaced (pass --force)",
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
        counts = (tally.frames, tally.bytes, tally.no_mid, *tally.mids.values())
        if max(counts) > fmt.MAX_U32:
            raise IronbirdError(
                Code.REC_CREATE,
                f"the {tally.frames} frame(s) after the last chunk summary are too many to "
                f"summarise in one chunk (limit {fmt.MAX_U32}); repair cannot index this recording",
            )
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
