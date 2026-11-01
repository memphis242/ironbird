"""Helpers for the parallel export tests: a destination that keeps what it is given, a driver that
runs one export either way (in this process, or through the pool in tiny ranges), and a corpus of
recordings, clean and damaged, large enough to span many ranges."""

from __future__ import annotations

import atexit
import dataclasses
import random
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from ironbird.errors import IronbirdError
from ironbird.frame import Frame, GapReason
from ironbird.recording import format as fmt
from ironbird.recording.defs import embedded_codecs
from ironbird.recording.export import (
    ExportFormat,
    ExportResult,
    Selection,
    check_messages,
    export,
)
from ironbird.recording.format import MarkKind, RecordType
from ironbird.recording.pexport import (
    MAX_RANGE_EVENTS,
    PENDING_TEXT_BUDGET,
    RESULT_TEXT_CAP,
    EmbeddedDefinitions,
    PoolReport,
    WorkerPool,
    make_options,
)
from ironbird.recording.player import compile_where
from ironbird.recording.reader import Damage
from ironbird.recording.replay import Replay, TimeSpec
from ironbird.recording.writer import RecorderOptions
from recutil import RX, SERIAL, TX, Builder, frame
from replayutil import (
    STEP_NS,
    T0,
    codec_status_frames,
    crafted,
    good_blobs,
    hk,
    hk_frames,
    write_recording,
)

RANGE = 4096  # the smallest range: a few hundred frames at most
JOBS = 3
_POOLS: list[WorkerPool] = []


def shared_pool(definitions: EmbeddedDefinitions) -> WorkerPool:
    """Workers kept for the whole test process (starting them is the cost of an export of a small
    file), for the exports that decode with ``definitions``; ended when the process ends."""
    for pool in _POOLS:
        if pool.definitions == definitions and not pool.broken:
            return pool
    pool = WorkerPool(definitions, JOBS)
    _POOLS.append(pool)
    return pool


@atexit.register
def _end_pools() -> None:
    for pool in _POOLS:
        pool.close()


class Capture:
    """A destination that keeps every write, by member, in order. ``per_line``: takes a line at a
    time like stdout; ``refuse_after``: refuses the line after that many."""

    def __init__(self, *, per_line: bool = False, refuse_after: int | None = None) -> None:
        self.per_line = per_line
        self.refuse_after = refuse_after
        self.members: list[str] = []
        self.texts: dict[int, list[str]] = {}
        self.taken = 0

    def member(self, name: str) -> int:
        self.members.append(name)
        return len(self.members) - 1

    def write(self, handle: int, text: str) -> bool:
        if self.refuse_after is not None and self.taken >= self.refuse_after:
            return False
        self.taken += 1
        self.texts.setdefault(handle, []).append(text)
        return True

    def flush(self) -> None:
        pass

    def content(self) -> dict[int, str]:
        return {h: "".join(parts) for h, parts in self.texts.items()}


@dataclass
class Run:
    """What one export did, to compare between the two ways of doing it."""

    members: list[str]
    content: dict[int, str]
    result: ExportResult | None
    error: tuple[str, str] | None
    stats: dict[str, Any]
    damage: list[tuple[Any, ...]]
    report: PoolReport = field(default_factory=PoolReport)


def run_export(
    files: Path | Sequence[Path],
    fmt_: ExportFormat = ExportFormat.CSV,
    *,
    jobs: int = 1,
    where: str | None = None,
    messages: Sequence[str] = (),
    header_fields: bool = False,
    raw_enums: bool = False,
    skip_damage: bool = False,
    start: TimeSpec | None = None,
    end: TimeSpec | None = None,
    per_line: bool = False,
    refuse_after: int | None = None,
    range_bytes: int = RANGE,
    single: bool | None = None,
    pool: WorkerPool | bool | None = True,
    text_cap: int = RESULT_TEXT_CAP,
    text_budget: int = PENDING_TEXT_BUDGET,
    event_cap: int = MAX_RANGE_EVENTS,
) -> Run:
    """Do what ``ib export`` does, with ``jobs`` workers when above 1 (ranges of ``range_bytes``
    whatever the size of the files; ``text_cap``, ``text_budget`` and ``event_cap`` as in
    :func:`make_options`). Errors are returned, not raised."""
    paths = [files] if isinstance(files, Path) else list(files)
    damage: list[tuple[Any, ...]] = []

    def on_damage(item: Damage, path: Path) -> None:
        damage.append((path.name, *dataclasses.astuple(item)))

    dest = Capture(per_line=per_line, refuse_after=refuse_after)
    report = PoolReport()
    result: ExportResult | None = None
    error: tuple[str, str] | None = None
    replaying: Replay | None = None
    try:
        replaying = Replay(
            paths, skip_damage=skip_damage, start=start, end=end, on_damage=on_damage
        )
        codecs = embedded_codecs(
            replaying.parts[0],
            replaying.head,
            replaying.nodes,
            replaying.meta.data,
            replaying.max_packet_bytes,
        )
        flt = compile_where(replaying, where, codecs) if where is not None else None
        names = tuple(dict.fromkeys(messages))
        selection = None
        if names:
            check_messages(names, codecs)
            selection = Selection(names, codecs)
        mids = [m for m in (flt.mids if flt else None, selection.mids if selection else None) if m]
        if mids:
            replaying.select_mids(frozenset.intersection(*mids))
        as_csv = fmt_ is ExportFormat.CSV
        one = as_csv and len(names) == 1 if single is None else single
        directory = as_csv and not one
        parallel = None
        if jobs > 1:
            definitions = EmbeddedDefinitions.of(replaying, "")
            kept = shared_pool(definitions) if pool is True and jobs == JOBS else None
            parallel = make_options(
                replaying,
                definitions,
                fmt_,
                jobs=jobs,
                where=where,
                messages=names,
                header_fields=header_fields,
                raw_enums=raw_enums,
                directory=directory,
                min_bytes=0,
                range_bytes=range_bytes,
                pool=kept if pool is True else (pool or None),
                text_cap=text_cap,
                text_budget=text_budget,
                event_cap=event_cap,
            )
            parallel = dataclasses.replace(parallel, report=report)
        result = export(
            replaying,
            codecs,
            dest,
            fmt_,
            where=flt,
            selection=selection,
            header_fields=header_fields,
            raw_enums=raw_enums,
            single=one,
            directory=directory,
            parallel=parallel,
        )
    except IronbirdError as exc:
        error = (exc.code.value, exc.summary)
    stats: dict[str, Any] = {}
    if replaying is not None:
        stats = dataclasses.asdict(replaying.stats)
        stats["ended_by_damage"] = replaying.ended_by_damage
        replaying.close()
    return Run(dest.members, dest.content(), result, error, stats, damage, report)


#: What an export tallies from the frames it decodes without writing a row for them: after the
#: output closed it covers what was read ahead of the row that was refused.
READ_AHEAD: Final = ("rejected", "skipped_no_definitions", "not_written")


def same_result(a: ExportResult | None, b: ExportResult | None, *, closed: bool = False) -> bool:
    if a is None or b is None:
        return a is b
    first, second = dataclasses.asdict(a), dataclasses.asdict(b)
    if closed:
        for name in READ_AHEAD:
            first.pop(name)
            second.pop(name)
    return first == second


def assert_same(
    single: Run, pooled: Run, *, counts: bool = True, closed: bool = False, label: str = ""
) -> None:
    """The two runs did the same: the same files with the same bytes, the same tally, the same
    damage reported, the same error. ``counts``: also the replay's own tally of what it read."""
    assert pooled.error == single.error, label
    assert pooled.members == single.members, label
    assert pooled.content == single.content, label
    assert same_result(pooled.result, single.result, closed=closed), label
    assert pooled.damage == single.damage, label
    if counts:
        assert pooled.stats == single.stats, label
    else:
        for key in ("damage", "damage_bytes", "gaps", "gap_frames", "mid_file_damage", "torn_tail"):
            assert pooled.stats.get(key) == single.stats.get(key)


# -- recordings ---------------------------------------------------------------------------------


def mixed_frames(count: int, *, start: int = 1) -> list[Frame]:
    """Frames of every kind an export tells apart: housekeeping, status, a payload of the wrong
    size, an unknown MID, a frame on the command stream, and a serial-stream frame."""
    status = codec_status_frames(1, 1)[0].data
    out: list[Frame] = []
    for seq in range(start, start + count):
        time_ns = T0 + (seq - start) * STEP_NS
        if seq % 7 == 0:
            out.append(frame(seq, stream=RX, data=status, mid=0x891, time_ns=time_ns))
        elif seq % 11 == 0:
            out.append(frame(seq, stream=RX, data=hk(seq)[:-3], mid=0x890, time_ns=time_ns))
        elif seq % 13 == 0:
            out.append(frame(seq, stream=RX, data=b"\x01" * 30, mid=0x7FF, time_ns=time_ns))
        elif seq % 17 == 0:
            out.append(frame(seq, stream=TX, data=b"\x02" * 16, mid=0x1805, time_ns=time_ns))
        else:
            out.append(frame(seq, stream=RX, data=hk(seq), mid=0x890, time_ns=time_ns))
    return out


def clean_recording(path: Path, count: int = 2500, **options: Any) -> Path:
    opts = RecorderOptions(min_free_bytes=0, chunk_bytes=6000, **options)
    write_recording(path, mixed_frames(count), options=opts, streams=(RX, TX, SERIAL))
    return path


def builder_recording(
    count: int = 1200,
    *,
    late_stream_at: int | None = None,
    late_every: int = 3,
    redefine_at: int | None = None,
    repeat_seq_at: int | None = None,
    gaps_every: int | None = None,
) -> Builder:
    """A recording built record by record (no chunks, index or trailer) with the mock definitions
    embedded: ``count`` housekeeping frames. From ``late_stream_at`` on, a serial stream and a
    second source are defined then and used by every ``late_every``-th frame. ``redefine_at``
    defines the serial stream again, differently, at that frame. ``repeat_seq_at``: that frame
    carries the sequence number of the one before it. ``gaps_every``: a GAP record (of each reason
    in turn) and a MARK record before every such frame."""
    builder = crafted(good_blobs(), frames=0)
    for seq in range(1, count + 1):
        stream = source = 0
        if late_stream_at is not None and seq == late_stream_at:
            builder.stream(SERIAL)
            builder.source(1, "other-peer")
        if redefine_at is not None and seq == redefine_at:
            builder.stream(dataclasses.replace(SERIAL, link="uart2"))
        if late_stream_at is not None and seq >= late_stream_at and seq % late_every == 0:
            stream, source = 2, 1
        if gaps_every is not None and seq % gaps_every == 0:
            reason = tuple(GapReason)[(seq // gaps_every) % len(GapReason)]
            time_ns = T0 + seq * STEP_NS
            gap = fmt.Gap(0, reason, 1 + seq % 5, time_ns - STEP_NS, time_ns, seq, seq)
            builder.add(RecordType.GAP, fmt.encode_gap(gap))
            builder.add(RecordType.MARK, fmt.encode_mark(time_ns, MarkKind.SIGNAL, f"mark {seq}"))
        builder.frame(
            seq - 1 if seq == repeat_seq_at else seq,
            data=hk(seq),
            stream=stream,
            source=source,
            mid=0x890,
            time_ns=T0 + seq * STEP_NS,
        )
    return builder


def frame_offsets(builder: Builder) -> list[int]:
    """The offsets of the FRAME records of ``builder``, in order."""
    out = bytes(builder.out)
    found = []
    for offset in builder.offsets:
        if out[offset + 4] == RecordType.FRAME:
            found.append(offset)
    return found


def write_builder(builder: Builder, path: Path) -> Path:
    """A builder's records as a file with the mock definitions embedded at the head."""
    return builder.write(path)


def mutate(data: bytes, rng: random.Random) -> bytes:
    """A copy of a recording with one kind of damage somewhere past its head."""
    low = min(len(data) // 5, 4000)
    where = rng.randrange(low, len(data) - 1)
    kind = rng.choice(["flip", "zero", "truncate", "dup", "cut", "swap", "garbage", "tail"])
    out = bytearray(data)
    match kind:
        case "flip":
            for _ in range(rng.randrange(1, 4)):
                out[rng.randrange(low, len(out))] ^= 1 << rng.randrange(8)
        case "zero":
            length = rng.choice([1, 8, 40, 300, 3000, 9000])
            out[where : where + length] = bytes(len(out[where : where + length]))
        case "truncate":
            del out[where:]
        case "dup":
            chunk = bytes(out[where : where + rng.choice([64, 500, 5000])])
            out[where:where] = chunk
        case "cut":
            del out[where : where + rng.choice([7, 64, 800, 6000])]
        case "swap":
            other = rng.randrange(low, len(out) - 200)
            a, b = sorted((where, other))
            out[a : a + 100], out[b : b + 100] = out[b : b + 100], out[a : a + 100]
        case "garbage":
            out[where:where] = rng.randbytes(rng.choice([1, 9, 100, 5000]))
        case "tail":
            out += rng.randbytes(rng.choice([1, 24, 700]))
        case _:
            raise AssertionError(kind)
    return bytes(out)


def unsupported_record(builder: Builder) -> None:
    """A record of a critical type this version does not know, in the middle of the frames."""
    builder.raw(0x1F, b"\x00" * 16)


__all__ = [
    "RANGE",
    "RX",
    "SERIAL",
    "T0",
    "TX",
    "Capture",
    "Counter",
    "RecordType",
    "Run",
    "assert_same",
    "builder_recording",
    "clean_recording",
    "fmt",
    "frame_offsets",
    "hk_frames",
    "mixed_frames",
    "mutate",
    "run_export",
    "same_result",
    "unsupported_record",
    "write_builder",
]
