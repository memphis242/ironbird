"""Parallel export decode (plan 2m): the same rows as :func:`~ironbird.recording.export.export`,
decoded by a pool of worker processes.

**What a worker does.** A file is cut into byte ranges. A worker is given a range (path, offsets, a
small seed), reads the file itself through the one validated reader
(:class:`~ironbird.recording.reader.RecordingReader`), and returns the text of the rows and a tally:
only offsets go in and compact results come out. It decodes with the same
:func:`~ironbird.recording.export.make_pipeline` the single process uses, so a row is formatted by
one piece of code.

**Why the result is the single-process result.** Reading a recording is a walk from record to
record, and what a record means depends on the records before it (the streams and sources defined,
the last sequence number). A range other than the first begins at a guess: the first record
boundary at or after its byte offset, found by the reader's own resynchronisation, with the
definitions the parent knows. The parent takes the results strictly in file order and accepts a
guess only when all of this holds, which makes the result what the walk from the start would have
made:

- the range is *clean* (no damage, no reader error, no worker failure);
- it began where the previous range ended (the walk, record by record, reaches that offset);
- what it took from its seed (the streams and sources its frames named that it had not defined
  itself) is in force there as it was in the seed, and what it defined does not contradict what
  the walk has defined;
- its first frame's sequence number is above the last one before it;
- or it holds no record at all, because none begins inside it (the scan for a first record reads
  no further than the end of the range) and the walk is already past its end.

Otherwise the range is read again with the exact state and start (:class:`Job.exact`), and its
result stands whatever it holds: damage, a reader error and an end at the damage are then exactly
what the single process meets, and stop the export at the same place. A range that ends the walk
early (damage that stops it, the end of the file) discards every range after it. So a recording
with damage is handled as the single process handles it, at the cost of reading the damaged ranges
twice.

**The file read is the file opened.** The parent holds the recording open and tells each worker
its identity (device and inode). A worker whose name leads somewhere else (the file was renamed or
replaced) or nowhere reads nothing; the parent then reads that range and every later one itself,
from the file it holds, exactly as the single process would.

**Bounded memory.** A result carries at most :data:`RESULT_TEXT_CAP` characters of rows (and at
most :data:`MAX_RANGE_EVENTS` damage and gap notes): a worker that reaches either stops at a
record boundary, and the parent asks for the rest of the range with an exact job from there. At
most :data:`WINDOW_PER_WORKER` ranges per worker are submitted and not yet written, so the rows
held, in the parent and in the workers, never pass ``WINDOW_PER_WORKER x workers x
RESULT_TEXT_CAP`` characters. A range is submitted beyond the first one per worker only while the
bytes of rows waiting plus the largest result seen for each range still running fit in
:data:`PENDING_TEXT_BUDGET`; that estimate follows what came back, so after a stretch of narrow
rows the ranges already submitted may all turn out wide, and the held rows then come to the
first figure. Results are written in order and dropped. Ranges are planned one after another and
are shorter when the rows are wide (:meth:`_Runner._length`), so that a result is rarely cut.
Nothing buffers the recording.

**When the output closes** (standard output into ``head``) the single process stops at the row
the output refused, and when a write fails (a full disk) it stops at the row that made the file
flush. A worker notes how many rows came before each damage and gap record, and what kind of row
each was; a range that holds damage or gaps, or that goes to an output taking one line at a time,
is written row by row with the damage and gaps noted between the rows where they came, so the
damage reported and the tally are those of the single process whichever row the output stops at.
A file destination flushes after the line that fills its buffer wherever that line is in a
block, so a failing flush is at the same row either way.

**Processes.** Workers are started with the ``spawn`` method, ignore SIGINT and SIGHUP (the parent
coordinates), exit if the parent goes away, and are terminated when the export ends abnormally (a
signal, an error, the output closing). The pool is owned: one process and one pipe per worker, jobs
and results over that pipe, no helper thread and no queue shared between workers, so a worker that
is terminated or dies part way through a message is an end of file on its pipe and nothing waits
for bytes that will not come. The parent waits in :func:`multiprocessing.connection.wait` in short
slices. The processes, and the resource tracker the first of them starts, are created with the
shutdown signals blocked, so a hangup sent to the process group reaches the parent alone. A worker
that dies, or whose result cannot be read, is ``IB-EXP-003``.

Untrusted input: everything a worker reads is validated by the reader before it is decoded, and
what comes back is plain data that the parent checks against the walk before it is used. Jobs and
results are pickled over the private pipe between the parent and its own worker; nothing read from
a recording or the wire is ever unpickled, and a message that is not the result asked for is
``IB-EXP-003``.
"""

from __future__ import annotations

import array
import contextlib
import dataclasses
import hashlib
import multiprocessing
import os
import pickle
import signal
import threading
import time
import traceback
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from multiprocessing import connection, resource_tracker
from pathlib import Path
from typing import Any, Final

from ironbird.codec import Codec
from ironbird.config import (
    DEFAULT_POOL_MIN_BYTES,
    DEFAULT_RANGE_BYTES,
    MAX_EXPORT_JOBS,
    MIN_RANGE_BYTES,
    load_config,
)
from ironbird.errors import Code, IronbirdError
from ironbird.filter import Filter, compile_filter
from ironbird.frame import Frame, GapReason, Stream
from ironbird.fsops import read_capped
from ironbird.header import MAX_PROFILE_FILE_BYTES
from ironbird.recording import format as fmt
from ironbird.recording.defs import (
    Blobs,
    Head,
    config_codecs,
    embedded_codecs,
)
from ironbird.recording.export import (
    NO_LINE,
    Destination,
    ExportFormat,
    ExportResult,
    Pipeline,
    Selection,
    Table,
    TableKey,
    make_pipeline,
)
from ironbird.recording.format import RecordType
from ironbird.recording.player import Ending
from ironbird.recording.reader import Damage, RecordingReader
from ironbird.recording.replay import JumpTable, Replay, gap_in_range
from ironbird.sigmask import HANDLED_SIGNALS, WAIT_SLICE_S, checkpoint, masked
from ironbird.units import MIB

RANGE_BYTES: Final = DEFAULT_RANGE_BYTES
MIN_POOL_BYTES: Final = DEFAULT_POOL_MIN_BYTES
MAX_JOBS: Final = MAX_EXPORT_JOBS
WINDOW_PER_WORKER: Final = 2  # ranges submitted and not yet written, per worker
RESULT_TEXT_CAP: Final = 8 * MIB  # characters of rows one result carries before the range is cut
PENDING_TEXT_BUDGET: Final = 32 * MIB  # characters of rows waiting or expected, over all ranges
MIN_RANGE_TEXT: Final = 256 * 1024  # the least text a range is planned to make, however wide
FIRST_RANGE_DIVISOR: Final = 32  # ranges planned before any width is known are this much shorter
MAX_RANGE_EVENTS: Final = 1000  # damage and gap notes one result carries before the range is cut
PARENT_WATCH_S: Final = 1.0  # how often a worker looks for its parent
STOP_WAIT_S: Final = 5.0  # how long terminated workers are waited for before they are killed
ALIGN: Final = fmt.ALIGN
assert RANGE_BYTES % ALIGN == 0 and MIN_RANGE_BYTES % ALIGN == 0


CGROUP_ROOT: Final = Path("/sys/fs/cgroup")
PROC_CGROUP: Final = Path("/proc/self/cgroup")
_CPU_MAX_BYTES: Final = 256  # "max 100000" or "<quota> <period>"
_MEMBERSHIP_BYTES: Final = 64 * 1024  # one line per hierarchy; a unified host has one


def _cgroup_text(path: Path, limit: int) -> str | None:
    """Up to ``limit`` bytes of a control group file as text, ``None`` when it cannot be read."""
    try:
        with path.open("rb") as handle:
            return handle.read(limit).decode("ascii", "replace")
    except OSError:
        return None


def cpu_quota(root: Path = CGROUP_ROOT, membership: Path = PROC_CGROUP) -> int | None:
    """The whole cores the control group this process is in may use (the smallest ``cpu.max``
    quota from its group up to the root, rounded up), or ``None`` when no limit is set, the host
    has no unified hierarchy, or the group cannot be examined."""
    text = _cgroup_text(membership, _MEMBERSHIP_BYTES)
    if text is None:
        return None
    relative = ""
    for line in text.splitlines():
        if line.startswith("0::"):
            relative = line[3:].strip()
    parts = [p for p in relative.split("/") if p and p not in (".", "..")]
    try:
        under_root = root.joinpath(*parts).is_dir()
    except OSError:  # a directory that cannot be looked at (EACCES, EIO): no limit is known
        return None
    if not under_root:
        parts = []  # a namespace whose paths are not under this root: only its top is known
    found: int | None = None
    for depth in range(len(parts), -1, -1):
        limit = _cgroup_text(root.joinpath(*parts[:depth]) / "cpu.max", _CPU_MAX_BYTES)
        fields = limit.split() if limit is not None else []
        if len(fields) != 2 or not (fields[0].isdecimal() and fields[1].isdecimal()):
            continue  # "max" (no limit), or a file that is not a quota
        quota, period = int(fields[0]), int(fields[1])
        if period > 0 and quota > 0:
            cores = -(-quota // period)
            found = cores if found is None else min(found, cores)
    return found


def default_jobs() -> int:
    """The workers to use when nothing says: one per core this process may run on (its CPU
    affinity, and the quota of its control group), at most :data:`MAX_JOBS`."""
    try:
        cores = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):  # pragma: no cover - not Linux
        cores = os.cpu_count() or 1
    quota = cpu_quota()
    if quota is not None:
        cores = min(cores, quota)
    return max(1, min(cores, MAX_JOBS))


# -- definitions, rebuilt in each worker ------------------------------------------------------


def _plain(value: object) -> object:
    """``value`` (read-only mappings and sequences from JSON) as plain dicts and lists, which
    pickle."""
    if isinstance(value, Mapping):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(v) for v in value]
    return value


@dataclass(frozen=True, slots=True)
class EmbeddedDefinitions:
    """The codecs of ``--defs recording``: built again from the definitions the recording embeds
    (their bytes travel, so every worker builds from what the parent built from). Two recordings
    that embed the same definitions have equal recipes."""

    path: Path = field(compare=False)  # names the recording in an error
    blobs: Blobs
    problem: str
    nodes: tuple[str, ...]
    meta: dict[str, Any]
    max_packet_bytes: int
    hint: str

    @staticmethod
    def of(replay: Replay, hint: str) -> EmbeddedDefinitions:
        meta = _plain(replay.meta.data)
        assert isinstance(meta, dict)
        head = replay.head
        return EmbeddedDefinitions(
            replay.parts[0],
            dict(head.blobs),
            head.problem,
            tuple(replay.nodes),
            meta,
            replay.max_packet_bytes,
            hint,
        )

    def build(self) -> dict[str, Codec]:
        head = Head({}, self.blobs, self.problem)
        return embedded_codecs(
            self.path, head, self.nodes, self.meta, self.max_packet_bytes, self.hint
        )


def config_fingerprint(config_nodes: Mapping[str, Any], nodes: tuple[str, ...]) -> str:
    """A digest of the definition files the configuration names for ``nodes``, to tell a worker
    that the files are not what the parent decoded with."""
    digest = hashlib.sha256()
    for name in sorted(set(nodes)):
        digest.update(name.encode("utf-8", "surrogateescape") + b"\0")
        node = config_nodes.get(name)
        if node is None:
            continue
        for path, limit in (
            (node.ccdd, fmt.MAX_BLOB_CONTENT),
            (node.command_header, MAX_PROFILE_FILE_BYTES),
            (node.telemetry_header, MAX_PROFILE_FILE_BYTES),
        ):
            if path is None:
                digest.update(b"-\0")
                continue
            try:
                content = read_capped(path, limit)
            except OSError as exc:
                digest.update(f"unreadable:{exc.errno}\0".encode())
                continue
            digest.update(len(content).to_bytes(8, "little") + hashlib.sha256(content).digest())
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class ConfigDefinitions:
    """The codecs of ``--defs config``: built again from the configuration's files. A worker
    refuses to decode if those files are not what the parent saw (``fingerprint``)."""

    config_path: Path
    config_sha256: str | None
    nodes: tuple[str, ...]
    max_packet_bytes: int
    command: str
    fingerprint: str

    @staticmethod
    def of(
        config_path: Path,
        config_sha256: str | None,
        config_nodes: Mapping[str, Any],
        nodes: tuple[str, ...],
        max_packet_bytes: int,
        command: str,
    ) -> ConfigDefinitions:
        return ConfigDefinitions(
            config_path,
            config_sha256,
            nodes,
            max_packet_bytes,
            command,
            config_fingerprint(config_nodes, nodes),
        )

    def build(self) -> dict[str, Codec]:
        config = load_config(self.config_path)
        if self.config_sha256 is not None and config.sha256 != self.config_sha256:
            raise _Changed(f"{self.config_path} changed while the export ran")
        before = config_fingerprint(config.nodes, self.nodes)
        codecs = config_codecs(config, self.nodes, self.max_packet_bytes, self.command)
        if before != self.fingerprint or config_fingerprint(config.nodes, self.nodes) != before:
            raise _Changed("a definition file named by the configuration changed during the export")
        return codecs


class _Changed(Exception):
    """The definitions on disk are not the ones the parent decoded with."""


Definitions = EmbeddedDefinitions | ConfigDefinitions


# -- what the parent sends and gets back ------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Shape:
    """What an export writes: the format and options, the filter as text (a compiled filter does
    not travel) and the messages asked for. It travels with each range, so a pool built for some
    definitions serves any export that decodes with them."""

    fmt: ExportFormat
    where: str | None
    links: frozenset[str]
    aliases: tuple[tuple[str, str], ...] | None
    messages: tuple[str, ...]
    header_fields: bool
    raw_enums: bool
    directory: bool
    skip_damage: bool
    categories: bool = False  # the output takes rows one at a time and may refuse one


@dataclass(frozen=True, slots=True)
class Seed:
    """The state a range begins with: the streams and sources defined before it, the last
    sequence number, the work charged to the resync budget."""

    streams: dict[int, Stream]
    sources: dict[int, str]
    last_seq: int
    work: int


@dataclass(frozen=True, slots=True)
class Job:
    """A range: records from the first at or after ``guess`` (``exact``: the record at ``guess``)
    until the first at or after ``stop`` (``None``: the end of the file)."""

    index: int
    path: Path
    guess: int
    stop: int | None
    exact: bool
    seed: Seed
    start_ns: int | None
    end_ns: int | None
    shape: Shape
    identity: tuple[int, int]  # (device, inode) of the file the parent has open
    text_cap: int  # characters of rows after which the range is cut short
    event_cap: int  # damage and gap notes after which the range is cut short


@dataclass(slots=True)
class Block:
    """The rows of one table from one range: ``key`` identifies the table (``None``: the malformed
    table, or the only stream of text and JSON Lines), ``text`` has no header row."""

    key: TableKey | None
    malformed: bool
    rows: int
    text: str
    handle: int = 0  # the table in the worker that made it (what ``Outcome.order`` names)


@dataclass(slots=True)
class Outcome:
    """What a worker made of a :class:`Job`. Plain data; the parent trusts none of it until it has
    been checked against the walk."""

    index: int
    start: int | None = None  # where the first record was read (None: no record boundary found)
    end: int = 0  # where reading ended
    reached_stop: bool = False
    first_seq: int | None = None
    last_seq: int = 0
    definitions: list[tuple[Any, ...]] = field(default_factory=list)
    used_streams: list[int] = field(default_factory=list)  # ids the range found in its seed
    used_sources: list[int] = field(default_factory=list)
    work: int = 0  # bytes charged to the resync budget by the range
    # bytes the range's resync scans read, the search for its first record included
    scanned: int = 0
    trailer: bool = False
    seen: bool = False  # the parent has learned from its size
    partial: bool = False  # cut short at a cap: the range goes on from ``end``
    moved: bool = False  # the name does not lead to the file the parent has open: nothing was read
    damage: list[Damage] = field(default_factory=list)
    damage_rows: list[int] = field(default_factory=list)  # rows made before each damage
    failure: tuple[str, str, str] | None = None  # (code, summary, details) the reader raised
    crash: str | None = None  # a worker fault, as text
    frames_read: int = 0
    frames_in_range: int = 0
    gap_marks: list[tuple[int, int, int]] = field(default_factory=list)
    """``(rows made before, gaps, frames lost)``: the gap records in range, run together when no
    row came between them."""
    counts: ExportResult = field(default_factory=ExportResult)
    blocks: list[Block] = field(default_factory=list)
    categories: bytes | None = None
    """The kind of each row, in the order they were made (``NO_LINE`` set: no line); ``None``
    unless the parent writes row by row or the range holds damage or gaps."""
    order: array.array[int] | None = None
    """With ``categories`` and more than one block: the :attr:`Block.handle` each line belongs to,
    line by line in the order they were made."""

    @property
    def clean(self) -> bool:
        return self.crash is None and self.failure is None and not self.damage and not self.moved


# -- the worker -------------------------------------------------------------------------------


class _BlockDestination:
    """The destination of a worker: keeps the text of each member in memory."""

    per_line = False

    def __init__(self, single_stream: bool) -> None:
        self.single_stream = single_stream
        self.parts: list[list[str]] = [[]] if single_stream else []
        self.chars = 0  # characters held, headers included
        self.order = array.array("I")  # the table of each row line (CSV: not the header rows)

    def member(self, name: str) -> int:
        self.parts.append([])
        return len(self.parts) - 1

    def write(self, handle: int, text: str) -> bool:
        parts = self.parts[handle]
        if parts or self.single_stream:  # the first part of a table is its header row
            self.order.append(handle)
        parts.append(text)
        self.chars += len(text)
        return True

    def flush(self) -> None:
        pass


class _Worker:
    """A worker's long-lived state: the codecs, and the filter and selection of each shape it has
    been given, built once."""

    MAX_SHAPES: Final = 16

    def __init__(self, definitions: Definitions, codecs: Mapping[str, Codec] | None = None) -> None:
        """``codecs``: the codecs the caller decodes with, when it has them (the parent); a worker
        process builds its own from ``definitions``."""
        self.codecs = codecs if codecs is not None else definitions.build()
        self._shapes: dict[Shape, tuple[Filter | None, Selection | None]] = {}

    def _compiled(self, shape: Shape) -> tuple[Filter | None, Selection | None]:
        found = self._shapes.get(shape)
        if found is None:
            if len(self._shapes) >= self.MAX_SHAPES:
                self._shapes.clear()
            where = (
                compile_filter(
                    shape.where,
                    self.codecs,
                    links=shape.links,
                    aliases=dict(shape.aliases) if shape.aliases is not None else None,
                )
                if shape.where is not None
                else None
            )
            selection = Selection(shape.messages, self.codecs) if shape.messages else None
            found = self._shapes[shape] = (where, selection)
        return found

    def _open(self, job: Job) -> RecordingReader | None:
        """The reader of the file the parent has open, or ``None`` when the name no longer
        leads to it."""
        try:
            reader = RecordingReader(job.path, skip_damage=job.shape.skip_damage)
        except IronbirdError:
            return None
        if reader.identity != job.identity:
            reader.close()
            return None
        return reader

    def run(self, job: Job, local: RecordingReader | None = None) -> Outcome:
        """The outcome of ``job``. ``local``: the reader of the file, when the caller has it open
        (the parent reading a range itself); it is left open."""
        shape = job.shape
        where, selection = self._compiled(shape)
        out = Outcome(job.index)
        reader = local if local is not None else self._open(job)
        if reader is None:
            out.moved = True
            return out
        scanned = reader.scanned
        try:
            seed = job.seed
            reader.seed_state(seed.streams, seed.sources, seed.last_seq, seed.work)
            if job.exact:
                begin: int | None = job.guess
            else:
                limit = job.stop if job.stop is not None else reader.size()
                begin = reader.find_boundary(job.guess, limit)
                reader.restart_work(seed.work)
            out.start = begin
            if begin is None:
                out.end = job.guess
                return out
            definitions = reader.log_definitions()
            lookups = reader.track_lookups()
            dest = _BlockDestination(single_stream=shape.fmt is not ExportFormat.CSV)
            pipe = make_pipeline(
                self.codecs,
                dest,
                shape.fmt,
                where=where,
                selection=selection,
                header_fields=shape.header_fields,
                raw_enums=shape.raw_enums,
                single=False,  # the parent keeps the one table of a single-table export
                directory=shape.directory,
                categories=True,
            )
            cut: int | None = None
            try:
                cut = self._read(reader, job, begin, out, pipe, dest)
            except IronbirdError as exc:
                out.failure = (exc.code.value, exc.summary, exc.detail_text())
            if cut is None:
                out.end = reader.position
                out.reached_stop = reader.reached_stop
            elif job.stop is not None and cut >= job.stop:
                out.end, out.reached_stop = cut, True
            else:
                out.end, out.partial = cut, True
            out.last_seq = reader.last_seq
            out.definitions = definitions
            out.used_streams = sorted(lookups.streams)
            out.used_sources = sorted(lookups.sources)
            out.work = reader.work - seed.work
            out.trailer = reader.trailer is not None
            self._collect(shape, pipe, dest, out)
            return out
        finally:
            out.scanned = reader.scanned - scanned
            if local is None:
                reader.close()

    def _read(
        self,
        reader: RecordingReader,
        job: Job,
        begin: int,
        out: Outcome,
        pipe: Pipeline,
        dest: _BlockDestination,
    ) -> int | None:
        """The loop of :meth:`Replay.read_part`, over a range, into ``pipe``. Returns the offset
        of the record to go on from when the range was cut short (the text or the notes reached
        their caps), at a record boundary the single process passes through too; ``None`` when
        the range was read to its end."""
        start, end = job.start_ns, job.end_ns
        step = pipe.step
        kinds = pipe.categories
        assert kinds is not None
        # A range read from a guess is used only if it holds no damage, so it need not look
        # past any: the parent reads it again from the exact state.
        for item in reader.items(start=begin, stop=job.stop, halt_on_damage=not job.exact):
            rows = len(kinds)
            if isinstance(item, Damage):
                out.damage.append(item)
                out.damage_rows.append(rows)
                # Reading goes on after damage only when asked to; what follows is then the
                # same whether it is read here or from the offset the reader resumed at.
                if (
                    job.shape.skip_damage
                    and item.resumed_at is not None
                    and self._full(job, out, dest)
                ):
                    return item.resumed_at
                continue
            match item.rtype:
                case RecordType.FRAME:
                    frame: Frame = item.value
                    out.frames_read += 1
                    if out.first_seq is None:
                        out.first_seq = frame.seq
                    if start is not None and frame.time_ns < start:
                        continue
                    if end is not None and frame.time_ns >= end:
                        continue
                    out.frames_in_range += 1
                    step(frame)
                    if self._full(job, out, dest):
                        return item.end
                case RecordType.GAP:
                    gap: fmt.Gap = item.value
                    if gap_in_range(gap, start, end):
                        lost = gap.lost if gap.reason is not GapReason.LINK_DOWN else 0
                        marks = out.gap_marks
                        if marks and marks[-1][0] == rows:
                            marks[-1] = (rows, marks[-1][1] + 1, marks[-1][2] + lost)
                        else:
                            marks.append((rows, 1, lost))
                        if self._full(job, out, dest):
                            return item.end
                case _:
                    pass
        return None

    @staticmethod
    def _full(job: Job, out: Outcome, dest: _BlockDestination) -> bool:
        """Whether the range has made all a result may carry."""
        return dest.chars >= job.text_cap or len(out.damage) + len(out.gap_marks) >= job.event_cap

    @staticmethod
    def _collect(shape: Shape, pipe: Pipeline, dest: _BlockDestination, out: Outcome) -> None:
        out.counts = pipe.result
        tables = pipe.tables
        if tables is None:
            rows = (
                pipe.result.decoded
                + pipe.result.malformed
                + pipe.result.unknown
                + pipe.result.garbage
                + pipe.result.undecoded
            )
            out.blocks.append(Block(None, False, rows, "".join(dest.parts[0])))
        else:
            for table in tables.all:
                parts = dest.parts[table.handle]
                text = "".join(parts[1:])  # the first part is the header row
                if text:
                    out.blocks.append(
                        Block(table.key, table is tables.malformed, table.rows, text, table.handle)
                    )
        if shape.categories or out.damage or out.gap_marks:
            assert pipe.categories is not None
            out.categories = bytes(pipe.categories)
            if len(out.blocks) > 1:
                out.order = dest.order


class _Process:
    """The state of this worker process: set by :func:`_init_worker`, used by :func:`run_job`."""

    worker: _Worker | None = None
    fault: str | None = None


def _watch_parent(parent: int) -> None:
    while True:
        time.sleep(PARENT_WATCH_S)
        if os.getppid() != parent:
            os._exit(1)


def _init_worker(definitions: Definitions, parent: int) -> None:
    """Run once in each worker process: the parent coordinates shutdown, so Ctrl-C and a hangup
    reach the parent only. The worker starts with the shutdown signals blocked (the pool is made
    with them blocked), so one that arrived while it started is pending: SIGINT and SIGHUP are
    set to be ignored before the mask is lifted, so such a signal is dropped, not raised in the
    middle of this function; SIGTERM keeps its default so the parent can terminate the worker."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.pthread_sigmask(signal.SIG_UNBLOCK, HANDLED_SIGNALS)
    threading.Thread(target=_watch_parent, args=(parent,), daemon=True).start()
    try:
        _Process.worker = _Worker(definitions)
    except _Changed as exc:
        _Process.fault = f"IB-EXP-003 {exc}"
    except IronbirdError as exc:
        _Process.fault = f"{exc.code.value}: {exc.summary}"
    except BaseException:
        _Process.fault = traceback.format_exc()


def run_job(job: Job) -> Outcome:
    """The task a worker runs: never raises (a fault is reported in the outcome)."""
    worker = _Process.worker
    if worker is None:
        return Outcome(job.index, crash=_Process.fault or "the worker has no state")
    try:
        return worker.run(job)
    except BaseException:
        return Outcome(job.index, crash=traceback.format_exc())


def serve(conn: connection.Connection, definitions: Definitions, parent: int) -> None:
    """The main function of a worker process: build the state, then answer the jobs that come
    over ``conn`` one at a time until the parent closes its end (or is gone). A result that cannot
    be pickled is answered with a crash outcome naming why."""
    _init_worker(definitions, parent)
    with contextlib.closing(conn):
        while True:
            try:
                job = pickle.loads(conn.recv_bytes())  # noqa: S301 - the parent's own message
            except (EOFError, OSError):
                return
            outcome = run_job(job)
            try:
                reply = pickle.dumps(outcome, pickle.HIGHEST_PROTOCOL)
            except Exception:
                crash = Outcome(job.index, crash=traceback.format_exc())
                reply = pickle.dumps(crash, pickle.HIGHEST_PROTOCOL)
            try:
                conn.send_bytes(reply)
            except OSError:
                return


# -- the parent -------------------------------------------------------------------------------


def make_options(
    replay: Replay,
    definitions: Definitions,
    fmt: ExportFormat,
    *,
    jobs: int,
    where: str | None = None,
    aliases: Mapping[str, str] | None = None,
    messages: tuple[str, ...] = (),
    header_fields: bool = False,
    raw_enums: bool = False,
    directory: bool = False,
    min_bytes: int = MIN_POOL_BYTES,
    range_bytes: int = RANGE_BYTES,
    pool: WorkerPool | None = None,
    text_cap: int = RESULT_TEXT_CAP,
    text_budget: int = PENDING_TEXT_BUDGET,
    event_cap: int = MAX_RANGE_EVENTS,
    codecs: Mapping[str, Codec] | None = None,
) -> ParallelOptions:
    """The options of an export of ``replay`` with ``jobs`` workers: ``definitions`` says how a
    worker rebuilds the codecs the parent decodes with; the rest repeats what the export was given
    (the filter as text, since a compiled filter does not travel). ``pool``: workers that are
    already running, for these ``definitions`` (the export does not end them); ``codecs``: the
    codecs the export decodes with, which the parent reuses for a range it reads itself."""
    shape = Shape(
        fmt,
        where,
        replay.link_names,
        tuple(sorted(aliases.items())) if aliases is not None else None,
        messages,
        header_fields,
        raw_enums,
        directory,
        replay.skips_damage,
    )
    return ParallelOptions(
        jobs,
        definitions,
        shape,
        min_bytes,
        range_bytes,
        pool,
        text_cap=text_cap,
        text_budget=text_budget,
        event_cap=event_cap,
        codecs=codecs,
    )


@dataclass(slots=True)
class PoolReport:
    """What the pool did, for the ledger and the tests."""

    workers: int = 0
    parts_pooled: int = 0
    ranges: int = 0
    reruns: int = 0  # ranges read again because a guess did not hold
    continued: int = 0  # ranges cut short at the caps and read on from there
    local: int = 0  # ranges the parent read itself because the file moved from under its name
    max_pending: int = 0  # most ranges submitted and not yet written
    max_text_bytes: int = 0  # most characters of rows held at once, results waiting included
    scanned: int = 0  # bytes the resync scans of the outcomes the walk received read, in all


class Ticket:
    """A submitted job: waiting for a worker, running, or answered (an outcome, or the error that
    ended the pool before it was)."""

    __slots__ = ("cancelled", "data", "error", "job", "outcome")

    def __init__(self, job: Job, data: bytes) -> None:
        self.job = job
        self.data: bytes | None = data  # the job as sent; dropped once sent
        self.outcome: Outcome | None = None
        self.error: IronbirdError | None = None
        self.cancelled = False  # nobody wants the answer any more

    def done(self) -> bool:
        return self.outcome is not None or self.error is not None

    def result(self) -> Outcome:
        """The outcome, or the error that ended the pool first; the ticket must be done."""
        if self.error is not None:
            raise self.error
        assert self.outcome is not None
        return self.outcome

    def cancel(self) -> None:
        self.cancelled = True


class _Slot:
    """A worker process and the pipe to it: the tickets sent to it and not yet answered, oldest
    first (a worker answers in the order it was asked)."""

    __slots__ = ("conn", "process", "sent")

    def __init__(self, process: Any, conn: connection.Connection) -> None:
        self.process = process
        self.conn = conn
        self.sent: deque[Ticket] = deque()


class WorkerPool:
    """Worker processes that decode ranges for exports that use ``definitions``. An export starts
    one when it needs it and ends it when it is done; a caller that runs many exports may keep one
    (``with WorkerPool(...) as pool``) and hand it to each.

    The pool is owned end to end: one spawned process and one pipe per worker, no helper thread,
    no queue shared between workers. A worker's end of its pipe is the only other copy of it, so
    a worker that dies (killed, out of memory, ended mid-message) is an end of file on its pipe,
    never a read that waits for bytes that will not come. Only the calling thread reads the pipes:
    :meth:`poll` waits in short slices, so a shutdown signal is acted on between two."""

    #: Jobs sent to a worker and not yet answered: the one running and one ready behind it, so a
    #: worker that finishes starts at once instead of after the parent has seen its result.
    DEPTH: Final = 2
    #: A job message longer than this goes only to a worker with nothing outstanding: two unread
    #: messages (the one being read and the next) then always fit in a pipe's buffer, and a send
    #: never waits for a worker that is busy.
    AHEAD_BYTES: Final = 16 * 1024

    def __init__(self, definitions: Definitions, jobs: int) -> None:
        assert 1 <= jobs <= MAX_JOBS
        self.definitions = definitions
        self.jobs = jobs
        self._slots: list[_Slot] = []
        self._queued: deque[Ticket] = deque()  # submitted, not yet sent to a worker
        self._error: IronbirdError | None = None
        self.broken = False

    # -- the tickets --

    def submit(self, job: Job) -> Ticket:
        """Queue ``job`` and hand it to a worker as soon as one can take it (starting a worker
        when all those running are busy and fewer than ``jobs`` exist)."""
        if self._error is not None:
            raise self._error
        try:
            data = pickle.dumps(job, pickle.HIGHEST_PROTOCOL)
        except Exception as exc:
            raise self._undeliverable("a job could not be sent to a worker", exc) from exc
        ticket = Ticket(job, data)
        self._queued.append(ticket)
        self._dispatch()
        return ticket

    def pending(self) -> bool:
        """Whether a ticket is queued or sent and has no answer yet."""
        return bool(self._queued) or any(slot.sent for slot in self._slots)

    def poll(self, timeout: float = 0.0) -> None:
        """Receive every result that is ready (waiting up to ``timeout`` seconds for the first)
        and send the queued jobs to the workers that have room. Raises nothing for a worker that
        died or an answer that cannot be read: the pool is then broken and the tickets without an
        answer carry the error."""
        self._dispatch()
        busy: dict[Any, _Slot] = {slot.conn: slot for slot in self._slots if slot.sent}
        if not busy:
            return
        for ready in connection.wait(list(busy), timeout):
            if self._error is not None:
                break
            self._receive(busy[ready])
        self._dispatch()

    def _receive(self, slot: _Slot) -> None:
        try:
            reply = pickle.loads(slot.conn.recv_bytes())  # noqa: S301 - our own worker's message
        except (EOFError, OSError) as exc:
            self._fail(self.died(exc))
            return
        except Exception as exc:  # bytes that are not an outcome
            self._fail(
                self._undeliverable("an export worker returned a result that cannot be read", exc)
            )
            return
        ticket = slot.sent.popleft()
        if not isinstance(reply, Outcome) or reply.index != ticket.job.index:
            self._fail(
                self._undeliverable(
                    "an export worker returned the wrong result",
                    AssertionError(
                        f"asked for range {ticket.job.index}, got {type(reply).__name__}"
                    ),
                )
            )
            return
        if not ticket.cancelled:
            ticket.outcome = reply

    def _fail(self, error: IronbirdError) -> None:
        """The pool is of no more use: every ticket without an answer ends with ``error``."""
        self.broken = True
        self._error = error
        waiting = [*self._queued, *(t for slot in self._slots for t in slot.sent)]
        for ticket in waiting:
            if not ticket.done():
                ticket.error = error
        self._queued.clear()
        for slot in self._slots:
            slot.sent.clear()

    def died(self, exc: BaseException) -> IronbirdError:
        """The error for a worker process that died; the pool is of no more use."""
        return IronbirdError(
            Code.EXP_WORKER,
            "an export worker process died (killed, or out of memory); run the export again, or "
            "with --jobs 1 to decode in one process",
            details=f"{type(exc).__name__}: {exc}",
            cause=exc,
        )

    @staticmethod
    def _undeliverable(summary: str, exc: BaseException) -> IronbirdError:
        return IronbirdError(
            Code.EXP_WORKER,
            f"{summary}; run the export again, or with --jobs 1 to decode in one process",
            details="".join(traceback.format_exception(exc)),
            cause=exc,
        )

    # -- the workers --

    def _dispatch(self) -> None:
        """Send queued jobs, in order, to the workers with room."""
        queued = self._queued
        while queued and self._error is None:
            ticket = queued[0]
            if ticket.cancelled:
                queued.popleft()
                continue
            assert ticket.data is not None
            slot = self._slot_for(len(ticket.data))
            if slot is None:
                return
            queued.popleft()
            try:
                slot.conn.send_bytes(ticket.data)
            except OSError as exc:  # the worker is gone
                slot.sent.append(ticket)
                self._fail(self.died(exc))
                return
            ticket.data = None
            slot.sent.append(ticket)

    def _slot_for(self, size: int) -> _Slot | None:
        """The worker to send a job of ``size`` bytes to: the least loaded, or a new one when
        every worker has a job and fewer than ``jobs`` exist; ``None``: none has room."""
        slots = self._slots
        best = min(slots, key=lambda slot: len(slot.sent), default=None)
        if (best is None or best.sent) and len(slots) < self.jobs:
            best = self._start()
        assert best is not None
        load = len(best.sent)
        if load >= self.DEPTH or (load and size > self.AHEAD_BYTES):
            return None
        return best

    def _start(self) -> _Slot:
        """Start a worker. The pool's processes, and the resource tracker the first of them
        starts, are made with the shutdown signals blocked, so a hangup or an interrupt sent to
        the process group reaches the parent alone. Two scopes: starting the resource tracker
        leaves SIGINT and SIGTERM unblocked in the thread that did it (multiprocessing unblocks
        them after the spawn), and a worker must not inherit that."""
        context = multiprocessing.get_context("spawn")
        ours = theirs = None
        try:
            ours, theirs = context.Pipe()
            with masked():
                resource_tracker.ensure_running()
            process = context.Process(
                target=serve,
                args=(theirs, self.definitions, os.getpid()),
                name="ironbird-export-worker",
                daemon=True,
            )
            with masked():
                process.start()
        except (OSError, RuntimeError) as exc:  # a process or file limit, memory
            if ours is not None:
                ours.close()
            self.broken = True
            reason = exc.strerror if isinstance(exc, OSError) and exc.strerror else str(exc)
            self._error = IronbirdError(
                Code.EXP_WORKER,
                f"cannot start export workers ({reason[:200]}); run the export with --jobs 1 to "
                "decode in one process",
                details=f"{type(exc).__name__}: {exc}",
                cause=exc,
            )
            raise self._error from exc
        finally:
            if theirs is not None:
                theirs.close()  # the worker holds the only other copy: its death is our end of file
        slot = _Slot(process, ours)
        self._slots.append(slot)
        return slot

    def close(self, *, abnormal: bool = False) -> None:
        """End the workers: idle ones are told to go (their pipe closes) and exit; after an error
        or a signal (``abnormal``), and any worker still holding a job nobody wants, they are
        terminated, and killed if they do not go within :data:`STOP_WAIT_S`."""
        slots, self._slots = self._slots, []
        for ticket in (*self._queued, *(t for slot in slots for t in slot.sent)):
            ticket.cancel()
        self._queued.clear()
        stop = abnormal or self.broken
        for slot in slots:
            if stop or slot.sent:
                with contextlib.suppress(Exception):
                    slot.process.terminate()
            with contextlib.suppress(Exception):
                slot.conn.close()
        deadline = time.monotonic() + STOP_WAIT_S
        for slot in slots:
            process = slot.process
            with contextlib.suppress(Exception):
                process.join(max(0.0, deadline - time.monotonic()))
                if process.is_alive():
                    process.kill()
                    process.join()
                process.close()

    def __enter__(self) -> WorkerPool:
        return self

    def __exit__(self, kind: object, value: object, trace: object) -> None:
        self.close(abnormal=kind is not None)


@dataclass(frozen=True, slots=True)
class ParallelOptions:
    """Whether and how an export uses the pool. ``jobs``: workers (1: never); ``min_bytes``: the
    size of the files below which the export stays in this process; ``range_bytes``: the size of
    a range; ``pool``: workers kept by the caller (``None``: the export starts its own);
    ``text_cap``, ``text_budget`` and ``event_cap``: the bounds on what a result carries and on
    what is waiting at once (see the module); ``codecs``: what the export decodes with."""

    jobs: int
    definitions: Definitions
    shape: Shape
    min_bytes: int = MIN_POOL_BYTES
    range_bytes: int = RANGE_BYTES
    pool: WorkerPool | None = None
    report: PoolReport = field(default_factory=PoolReport)
    text_cap: int = RESULT_TEXT_CAP
    text_budget: int = PENDING_TEXT_BUDGET
    event_cap: int = MAX_RANGE_EVENTS
    codecs: Mapping[str, Codec] | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        assert 1 <= self.jobs <= MAX_JOBS
        assert self.range_bytes >= MIN_RANGE_BYTES and self.range_bytes % ALIGN == 0
        assert self.text_cap >= 1 and self.text_budget >= 1 and self.event_cap >= 1
        assert self.pool is None or self.pool.definitions == self.definitions

    def wanted(self, replay: Replay) -> bool:
        """Whether the pool is used for ``replay``: not for one worker, a followed recording, or
        files that together are under ``min_bytes``."""
        if self.jobs < 2 or replay.following:
            return False
        total = 0
        for path in replay.parts:
            with contextlib.suppress(OSError):
                total += path.stat().st_size
        return total >= self.min_bytes

    def drive(self, replay: Replay, pipe: Pipeline, dest: Destination) -> Ending:
        """Run ``replay`` into ``pipe`` through the pool, part by part (see the module). Returns
        how the export ended. A part too small to share, one read with skips, and one still
        being written are read in this process, with the same ``pipe``."""
        owned = self.pool is None
        pool = WorkerPool(self.definitions, self.jobs) if self.pool is None else self.pool
        runner = _Runner(replay, pipe, dest, self, pool)
        try:
            ending = runner.run()
        except BaseException:
            runner.cancel()
            pool.close(abnormal=True)
            raise
        runner.cancel()
        if owned or ending is Ending.OUTPUT:
            pool.close(abnormal=ending is Ending.OUTPUT)
        return ending


@dataclass(slots=True)
class _Walk:
    """The state of the walk from the start of a part, as far as it has been checked."""

    streams: dict[int, Stream] = field(default_factory=dict)
    sources: dict[int, str] = field(default_factory=dict)
    last_seq: int = 0
    work: int = 0
    end: int = 0  # the offset of the next record: where the next range must begin

    def agrees_with(self, seed: Seed, outcome: Outcome) -> bool:
        """Whether a range read from ``seed`` made the reads the walk would: what it found in the
        seed is in force here as it was in the seed, and what it defined does not contradict
        what is defined."""
        for stream_id in outcome.used_streams:
            if self.streams.get(stream_id) != seed.streams.get(stream_id):
                return False
        for source_id in outcome.used_sources:
            if self.sources.get(source_id) != seed.sources.get(source_id):
                return False
        for entry in outcome.definitions:
            if entry[0] == "stream":
                known = self.streams.get(entry[1].id)
                if known is not None and known != entry[1]:
                    return False
            else:
                known_text = self.sources.get(entry[1])
                if known_text is not None and known_text != entry[2]:
                    return False
        return True

    def apply(self, definitions: list[tuple[Any, ...]]) -> None:
        for entry in definitions:
            if entry[0] == "stream":
                self.streams[entry[1].id] = entry[1]
            else:
                self.sources[entry[1]] = entry[2]

    def seed(self) -> Seed:
        return Seed(dict(self.streams), dict(self.sources), self.last_seq, self.work)


def _align_up(value: int) -> int:
    return (value + ALIGN - 1) & ~(ALIGN - 1)


def head_seed(reader: RecordingReader, size: int) -> Seed:
    """The streams and sources the head of the file defines: what the first ranges are seeded
    with, before the walk has shown what is in force."""
    streams: dict[int, Stream] = {}
    sources: dict[int, str] = {}
    for item in reader.head_records(reader.body_start, size):
        if item.rtype == RecordType.STREAM:
            streams[item.value.id] = item.value
        elif item.rtype == RecordType.SOURCE:
            sources[item.value[0]] = item.value[1]
    return Seed(streams, sources, 0, 0)


class _Runner:
    """Drives the parts of one replay through the pool and the calling process."""

    def __init__(
        self,
        replay: Replay,
        pipe: Pipeline,
        dest: Destination,
        options: ParallelOptions,
        pool: WorkerPool,
    ) -> None:
        self._replay = replay
        self._pipe = pipe
        self._dest = dest
        self._options = options
        self._report = options.report
        self._per_line = bool(getattr(dest, "per_line", False))
        self._shape = dataclasses.replace(options.shape, categories=self._per_line)
        self._pool = pool
        self._live: dict[int, Ticket] = {}
        self._moved = False  # the file is no longer under its name: this process reads it all
        self._local: _Worker | None = None
        self._largest = 0  # an estimate of the most text a running range will return
        self._ratio = 0.0  # an estimate of the characters of text a byte of the file makes

    def _waiting_text(self) -> int:
        """Characters of row text in the results that have come back and are not yet written."""
        total = 0
        for ticket in self._live.values():
            if ticket.outcome is not None:
                total += sum(len(block.text) for block in ticket.outcome.blocks)
        return total

    def _room(self, live: int) -> bool:
        """Whether another range may be submitted when ``live`` are submitted and not yet
        written: the window is not full, and the rows waiting plus the rows the ranges still
        running are expected to return (as much as the largest recent result, until one is seen:
        as much as a result may carry) fit in the budget; each worker always has one range."""
        options = self._options
        if live >= options.jobs * WINDOW_PER_WORKER:
            return False
        running = sum(1 for ticket in self._live.values() if not ticket.done())
        if running < options.jobs:
            return True
        expected = min(options.text_cap, self._largest) if self._largest else options.text_cap
        return self._waiting_text() + running * expected <= options.text_budget

    def _note_size(self, outcome: Outcome) -> None:
        """Learn how large a result is, and how much text a byte of the file makes (once)."""
        if outcome.seen:
            return
        outcome.seen = True
        size = sum(len(block.text) for block in outcome.blocks)
        self._largest = max(size, self._largest * 7 // 8)
        if outcome.start is not None and outcome.end > outcome.start:
            self._ratio = max(size / (outcome.end - outcome.start), self._ratio * 7 / 8)

    def _observe(self) -> None:
        """Take in the results that have come back, and learn from them, written or not."""
        self._pool.poll()
        for ticket in self._live.values():
            if ticket.outcome is not None:
                self._note_size(ticket.outcome)

    def _length(self) -> int:
        """The size of the next range. Before any result has shown how much text a byte of the
        file makes, a short one (the first window of ranges is the one that cannot be sized).
        After, the configured size, or less when the rows are so wide that it would make more
        text than a worker's share of the budget (the ranges running together, and the ones
        waiting behind them, then fit in it) and a result may carry: a range whose text passes
        the cap is cut and read on serially, which shares nothing."""
        options = self._options
        if self._ratio <= 0:
            length = options.range_bytes // FIRST_RANGE_DIVISOR
        else:
            share = options.text_budget // (2 * options.jobs)
            text = max(MIN_RANGE_TEXT, min(options.text_cap * 3 // 4, share))
            length = min(options.range_bytes, int(text / self._ratio))
        return max(MIN_RANGE_BYTES, length - length % ALIGN)

    def cancel(self) -> None:
        """Drop the ranges still waiting for a worker."""
        for ticket in self._live.values():
            ticket.cancel()
        self._live.clear()

    def _await(self, ticket: Ticket) -> Outcome:
        """The outcome of ``ticket``, waiting in slices so a shutdown signal is acted on."""
        pool = self._pool
        while not ticket.done():
            assert pool.pending(), "a ticket that is waited for is queued or running"
            pool.poll(WAIT_SLICE_S)
            checkpoint()
        outcome = ticket.result()
        self._report.scanned += outcome.scanned
        return outcome

    # -- the parts --

    def run(self) -> Ending:
        replay, pipe = self._replay, self._pipe
        start, end = replay.resolve_range()
        index = 0
        while index < len(replay.parts) and not replay.ended_by_damage:
            replay.stats.parts += 1
            if not self._part(replay.parts[index], start, end):
                return Ending.OUTPUT
            index += 1
        return Ending.DAMAGE if replay.ended_by_damage else pipe.result.ending

    def _part(self, path: Path, start: int | None, end: int | None) -> bool:
        """Export one file; ``False``: the output refused a row."""
        replay = self._replay
        reader = replay.open_part(path)
        try:
            jumps = None if replay.selects_nothing else replay.jump_table(reader, start, end)
            size = reader.size()
            span = size - reader.body_start
            if (
                replay.selects_nothing
                or jumps
                or reader.writer_active
                or span < 2 * self._options.range_bytes
            ):
                return self._single(reader, path, start, end, jumps)
            return self._pooled(reader, path, start, end, size)
        finally:
            reader.close()

    def _single(
        self,
        reader: RecordingReader,
        path: Path,
        start: int | None,
        end: int | None,
        jumps: JumpTable | None,
    ) -> bool:
        step = self._pipe.step
        frames = self._replay.read_part(reader, path, start, end, jumps)
        with contextlib.closing(frames):
            for frame in frames:
                if not step(frame):
                    return False
        return True

    def _pooled(
        self, reader: RecordingReader, path: Path, start: int | None, end: int | None, size: int
    ) -> bool:
        replay = self._replay
        options, report = self._options, self._report
        body = reader.body_start
        walk = _Walk(end=body, work=reader.work)  # the work of the single process at this point
        guess_seed = head_seed(reader, size)
        edges = [body]  # edges[k]: where range k begins; the last range has no edge after it
        last = False  # the last range (it reads to the end of the file) is planned
        jobs: dict[int, Job] = {}
        tickets = self._live
        tickets.clear()
        self._moved = False
        self._largest, self._ratio = 0, 0.0
        report.parts_pooled += 1
        damaged = closed = False
        submitted = 0

        def planned() -> int:
            return len(edges) if last else len(edges) - 1

        def plan() -> None:
            """Decide where the next range that has no end yet stops."""
            nonlocal last
            stop = _align_up(edges[-1] + self._length())
            if stop >= size:
                last = True
            else:
                edges.append(stop)

        def job_of(index: int, *, exact: bool, guess: int, seed: Seed) -> Job:
            return Job(
                index,
                path,
                guess,
                edges[index + 1] if index + 1 < len(edges) else None,
                exact,
                seed,
                start,
                end,
                self._shape,
                reader.identity,
                options.text_cap,
                options.event_cap,
            )

        def submit(job: Job) -> None:
            jobs[job.index] = job
            tickets[job.index] = self._pool.submit(job)
            report.workers = self._pool.jobs

        def exactly(index: int) -> Outcome:
            """Range ``index`` read from where the walk is, with the state it has."""
            job = job_of(index, exact=True, guess=walk.end, seed=walk.seed())
            if not self._moved:
                submit(job)
                outcome = self._await(tickets[index])
                if not outcome.moved:
                    return outcome
                self._leave_pool(jobs)
            report.local += 1
            return self._here(job, reader)

        def fetch(index: int) -> Outcome:
            """The outcome of the first read of range ``index``, once it is what the walk reads."""
            job = jobs.get(index)
            if job is None or self._moved:
                return exactly(index)
            outcome = self._await(tickets[index])
            if outcome.moved:
                self._leave_pool(jobs)
                return exactly(index)
            limit = job.stop if job.stop is not None else size
            if not job.exact and not self._accepted(job, outcome, walk, limit):
                report.reruns += 1
                return exactly(index)
            return outcome

        index = 0
        while True:
            while not self._moved and not (last and submitted >= len(edges)):
                self._observe()
                if submitted >= planned():
                    plan()
                if not self._room(submitted - index):
                    break
                if submitted == 0:
                    submit(job_of(0, exact=True, guess=body, seed=walk.seed()))
                else:
                    seed = walk.seed() if walk.streams else guess_seed
                    submit(job_of(submitted, exact=False, guess=edges[submitted], seed=seed))
                submitted += 1
                report.ranges += 1
                report.max_pending = max(report.max_pending, submitted - index)
            while index >= planned():
                plan()
            outcome = fetch(index)
            while True:
                self._note_size(outcome)
                report.max_text_bytes = max(report.max_text_bytes, self._waiting_text())
                tickets.pop(index, None)
                jobs.pop(index, None)
                if outcome.start is None:
                    break  # no record begins in the range and the walk is past it
                refused = not self._write(outcome, reader, path)
                damaged = damaged or bool(outcome.damage)
                closed = closed or outcome.trailer
                if refused:
                    return False
                if outcome.failure is not None:
                    code, summary, details = outcome.failure
                    raise IronbirdError(Code(code), summary, details=details)
                if outcome.crash is not None:
                    raise self._crashed(outcome.crash)
                walk.apply(outcome.definitions)
                if outcome.first_seq is not None:  # a range without frames leaves the sequence
                    walk.last_seq = outcome.last_seq
                walk.work += outcome.work
                assert not outcome.partial or outcome.end > walk.end, "a cut range goes on"
                walk.end = outcome.end
                if not outcome.partial:
                    break
                report.continued += 1
                outcome = exactly(index)
            index += 1
            if replay.ended_by_damage or (outcome.start is not None and not outcome.reached_stop):
                break
            if last and index >= len(edges):
                break
        for ticket in tickets.values():
            ticket.cancel()
        tickets.clear()
        replay.finish_part(reader, damaged=damaged, closed=closed)
        return True

    def _leave_pool(self, jobs: dict[int, Job]) -> None:
        """The workers cannot read the file under its name: the rest of the part is read here."""
        self._moved = True
        self.cancel()
        jobs.clear()

    def _here(self, job: Job, reader: RecordingReader) -> Outcome:
        """``job`` run in this process, on the file this process has open."""
        if self._local is None:
            options = self._options
            try:
                self._local = _Worker(options.definitions, options.codecs)
            except _Changed as exc:
                raise IronbirdError(
                    Code.EXP_WORKER,
                    f"{exc}; run the export again, or with --jobs 1 to decode in one process",
                ) from exc
        outcome = self._local.run(job, reader)
        self._report.scanned += outcome.scanned
        return outcome

    @staticmethod
    def _accepted(job: Job, outcome: Outcome, walk: _Walk, limit: int) -> bool:
        """Whether a range read from a guess is what the walk reads: see the module. ``limit``:
        where the range ends (the end of the file for the last)."""
        if not outcome.clean:
            return False
        if outcome.start is None:  # no record begins in the range
            return walk.end >= limit
        return (
            outcome.start == walk.end
            and walk.agrees_with(job.seed, outcome)
            and (outcome.first_seq is None or outcome.first_seq > walk.last_seq)
        )

    def _crashed(self, text: str) -> IronbirdError:
        lines = text.strip().splitlines()
        return IronbirdError(
            Code.EXP_WORKER,
            f"an export worker failed: {lines[-1][:300] if lines else 'no detail'}; run the "
            "export again, or with --jobs 1 to decode in one process",
            details=text,
        )

    # -- applying a range --

    def _write(self, outcome: Outcome, reader: RecordingReader, path: Path) -> bool:
        """Put the range's rows, tally and damage where the single process would have; ``False``:
        the output refused a row (the damage and gaps after it are not noted: the single process
        stops there and never reads them).

        A range with no damage and no gaps, for an output that takes whole blocks, is written
        block by block. Otherwise the rows are written one at a time with the damage and gaps
        noted between them where they came, so an output that fails part way has been told of
        exactly the damage and gaps the single process had met by then."""
        replay, pipe = self._replay, self._pipe
        stats = replay.stats
        stats.frames_read += outcome.frames_read
        stats.frames_in_range += outcome.frames_in_range
        result = pipe.result
        counts = outcome.counts
        result.skipped_no_definitions += counts.skipped_no_definitions
        result.not_written += counts.not_written
        result.rejected.update(counts.rejected)
        if outcome.categories is not None:
            return self._write_rows(outcome, _Events(replay, outcome, reader, path))
        assert not outcome.damage and not outcome.gap_marks
        result.absorb_rows(counts)
        tables = pipe.tables
        for block in outcome.blocks:
            if not block.text:
                continue
            if tables is None:
                handle = 0
            elif block.malformed:
                assert tables.malformed is not None
                table = tables.malformed
                handle = table.handle
                table.rows += block.rows
            else:
                assert block.key is not None
                table = tables.table_for(*block.key)
                handle = table.handle
                table.rows += block.rows
            self._dest.write(handle, block.text)
        return True

    def _write_rows(self, outcome: Outcome, events: _Events) -> bool:
        """The rows of a range written one by one and counted as far as the output takes them,
        with the damage and gaps that came before each. A table is opened when its first row is
        written, as the single process opens it."""
        pipe = self._pipe
        result = pipe.result
        blocks = [b for b in outcome.blocks if b.text]
        lines: list[list[str]] = []
        for block in blocks:
            block_lines = block.text.split("\n")
            assert block_lines.pop() == ""
            lines.append(block_lines)
        index_of = {block.handle: i for i, block in enumerate(blocks)}
        order = outcome.order
        assert len(blocks) <= 1 or order is not None
        cursors = [0] * len(blocks)
        handles: list[int | None] = [None] * len(blocks)
        owners: list[Table | None] = [None] * len(blocks)
        write = self._dest.write
        taken = 0  # lines written so far, over all tables
        assert outcome.categories is not None
        for row, kind in enumerate(outcome.categories):
            events.up_to(row)
            if not kind & NO_LINE:
                at = index_of[order[taken]] if order is not None else 0
                handle = handles[at]
                if handle is None:
                    handle, owners[at] = self._open(blocks[at])
                    handles[at] = handle
                if not write(handle, lines[at][cursors[at]] + "\n"):
                    return False
                cursors[at] += 1
                taken += 1
                table = owners[at]
                if table is not None:
                    table.rows += 1
            result.tally(kind & ~NO_LINE)
        assert cursors == [len(block_lines) for block_lines in lines]
        events.up_to(None)
        return True

    def _open(self, block: Block) -> tuple[int, Table | None]:
        """The handle in the output, and the table, that ``block``'s rows go to (the table is
        opened if it was not)."""
        tables = self._pipe.tables
        if tables is None:
            return 0, None
        if block.malformed:
            assert tables.malformed is not None
            return tables.malformed.handle, tables.malformed
        assert block.key is not None
        table = tables.table_for(*block.key)
        return table.handle, table


class _Events:
    """The damage and gap records of one range, noted in the replay as the rows before them are
    taken by the output."""

    def __init__(
        self, replay: Replay, outcome: Outcome, reader: RecordingReader, path: Path
    ) -> None:
        assert len(outcome.damage) == len(outcome.damage_rows)
        self._replay = replay
        self._outcome = outcome
        self._reader = reader
        self._path = path
        self._damage = 0
        self._gaps = 0

    def up_to(self, rows: int | None) -> None:
        """Note every record that came after at most ``rows`` rows (``None``: all that are left)."""
        outcome, replay = self._outcome, self._replay
        while self._damage < len(outcome.damage) and (
            rows is None or outcome.damage_rows[self._damage] <= rows
        ):
            replay.note_damage(outcome.damage[self._damage], self._reader, self._path)
            self._damage += 1
        while self._gaps < len(outcome.gap_marks) and (
            rows is None or outcome.gap_marks[self._gaps][0] <= rows
        ):
            _, gaps, lost = outcome.gap_marks[self._gaps]
            replay.note_gaps(gaps, lost)
            self._gaps += 1


__all__ = [
    "MAX_JOBS",
    "MIN_POOL_BYTES",
    "PENDING_TEXT_BUDGET",
    "RANGE_BYTES",
    "RESULT_TEXT_CAP",
    "ConfigDefinitions",
    "Definitions",
    "EmbeddedDefinitions",
    "ParallelOptions",
    "PoolReport",
    "Shape",
    "WorkerPool",
    "default_jobs",
    "make_options",
]
