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
- its first frame's sequence number is above the last one before it.

Otherwise the range is read again with the exact state and start (:class:`Job.exact`), and its
result stands whatever it holds: damage, a reader error and an end at the damage are then exactly
what the single process meets, and stop the export at the same place. A range that ends the walk
early (damage that stops it, the end of the file) discards every range after it. So a recording
with damage is handled as the single process handles it, at the cost of reading the damaged ranges
twice.

**Bounded memory.** At most :data:`WINDOW_PER_WORKER` ranges per worker are submitted and not yet
written; their results are written in order and dropped. Nothing buffers the recording.

**Processes.** Workers are started with the ``spawn`` method, ignore SIGINT and SIGHUP (the parent
coordinates), exit if the parent goes away, and are terminated when the export ends abnormally (a
signal, an error, the output closing). A worker that dies is ``IB-EXP-003``.

Untrusted input: everything a worker reads is validated by the reader before it is decoded, and
what comes back is plain data that the parent checks against the walk before it is used.
"""

from __future__ import annotations

import concurrent.futures as cf
import contextlib
import dataclasses
import hashlib
import itertools
import multiprocessing
import os
import signal
import threading
import time
import traceback
from collections.abc import Mapping
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
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
    TableKey,
    make_pipeline,
)
from ironbird.recording.format import RecordType
from ironbird.recording.player import Ending
from ironbird.recording.reader import Damage, RecordingReader
from ironbird.recording.replay import JumpTable, Replay, gap_in_range
from ironbird.sigmask import HANDLED_SIGNALS, WAIT_SLICE_S, checkpoint, masked

RANGE_BYTES: Final = DEFAULT_RANGE_BYTES
MIN_POOL_BYTES: Final = DEFAULT_POOL_MIN_BYTES
MAX_JOBS: Final = MAX_EXPORT_JOBS
WINDOW_PER_WORKER: Final = 2  # ranges submitted and not yet written, per worker
PARENT_WATCH_S: Final = 1.0  # how often a worker looks for its parent
STOP_WAIT_S: Final = 5.0  # how long terminated workers are waited for before they are killed
ALIGN: Final = fmt.ALIGN
assert RANGE_BYTES % ALIGN == 0 and MIN_RANGE_BYTES % ALIGN == 0


def default_jobs() -> int:
    """The workers to use when nothing says: one per core this process may run on."""
    try:
        return max(1, len(os.sched_getaffinity(0)))
    except (AttributeError, OSError):  # pragma: no cover - not Linux
        return max(1, os.cpu_count() or 1)


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


@dataclass(slots=True)
class Block:
    """The rows of one table from one range: ``key`` identifies the table (``None``: the malformed
    table, or the only stream of text and JSON Lines), ``text`` has no header row."""

    key: TableKey | None
    malformed: bool
    rows: int
    text: str


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
    trailer: bool = False
    damage: list[Damage] = field(default_factory=list)
    failure: tuple[str, str, str] | None = None  # (code, summary, details) the reader raised
    crash: str | None = None  # a worker fault, as text
    frames_read: int = 0
    frames_in_range: int = 0
    gaps: int = 0
    gap_lost: int = 0
    counts: ExportResult = field(default_factory=ExportResult)
    blocks: list[Block] = field(default_factory=list)
    categories: bytes | None = None

    @property
    def clean(self) -> bool:
        return self.crash is None and self.failure is None and not self.damage


# -- the worker -------------------------------------------------------------------------------


class _BlockDestination:
    """The destination of a worker: keeps the text of each member in memory."""

    per_line = False

    def __init__(self, single_stream: bool) -> None:
        self.parts: list[list[str]] = [[]] if single_stream else []

    def member(self, name: str) -> int:
        self.parts.append([])
        return len(self.parts) - 1

    def write(self, handle: int, text: str) -> bool:
        self.parts[handle].append(text)
        return True

    def flush(self) -> None:
        pass


class _Worker:
    """A worker's long-lived state: the codecs, and the filter and selection of each shape it has
    been given, built once."""

    MAX_SHAPES: Final = 16

    def __init__(self, definitions: Definitions) -> None:
        self.codecs = definitions.build()
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

    def run(self, job: Job) -> Outcome:
        shape = job.shape
        where, selection = self._compiled(shape)
        out = Outcome(job.index)
        reader = RecordingReader(job.path, skip_damage=shape.skip_damage)
        try:
            seed = job.seed
            reader.seed_state(seed.streams, seed.sources, seed.last_seq, seed.work)
            if job.exact:
                begin: int | None = job.guess
            else:
                begin = reader.find_boundary(job.guess)
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
                categories=shape.categories,
            )
            try:
                self._read(reader, job, begin, out, pipe)
            except IronbirdError as exc:
                out.failure = (exc.code.value, exc.summary, exc.detail_text())
            out.end = reader.position
            out.reached_stop = reader.reached_stop
            out.last_seq = reader.last_seq
            out.definitions = definitions
            out.used_streams = sorted(lookups.streams)
            out.used_sources = sorted(lookups.sources)
            out.work = reader.work - seed.work
            out.trailer = reader.trailer is not None
            self._collect(pipe, dest, out)
            return out
        finally:
            reader.close()

    def _read(
        self, reader: RecordingReader, job: Job, begin: int, out: Outcome, pipe: Pipeline
    ) -> None:
        """The loop of :meth:`Replay.read_part`, over a range, into ``pipe``."""
        start, end = job.start_ns, job.end_ns
        step = pipe.step
        for item in reader.items(start=begin, stop=job.stop):
            if isinstance(item, Damage):
                out.damage.append(item)
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
                case RecordType.GAP:
                    gap: fmt.Gap = item.value
                    if gap_in_range(gap, start, end):
                        out.gaps += 1
                        if gap.reason is not GapReason.LINK_DOWN:
                            out.gap_lost += gap.lost
                case _:
                    pass

    def _collect(self, pipe: Pipeline, dest: _BlockDestination, out: Outcome) -> None:
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
                    out.blocks.append(Block(table.key, table is tables.malformed, table.rows, text))
        if pipe.categories is not None:
            out.categories = bytes(pipe.categories)


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
    reach the parent only; the parent's blocked signals (the pool is started with them blocked)
    are unblocked so it can still terminate the worker."""
    signal.pthread_sigmask(signal.SIG_UNBLOCK, HANDLED_SIGNALS)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
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
) -> ParallelOptions:
    """The options of an export of ``replay`` with ``jobs`` workers: ``definitions`` says how a
    worker rebuilds the codecs the parent decodes with; the rest repeats what the export was given
    (the filter as text, since a compiled filter does not travel). ``pool``: workers that are
    already running, for these ``definitions`` (the export does not end them)."""
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
    return ParallelOptions(jobs, definitions, shape, min_bytes, range_bytes, pool)


@dataclass(slots=True)
class PoolReport:
    """What the pool did, for the ledger and the tests."""

    workers: int = 0
    parts_pooled: int = 0
    ranges: int = 0
    reruns: int = 0  # ranges read again because a guess did not hold
    max_pending: int = 0  # most ranges submitted and not yet written
    max_text_bytes: int = 0  # most characters of rows held at once, results waiting included


class WorkerPool:
    """Worker processes that decode ranges for exports that use ``definitions``. An export starts
    one when it needs it and ends it when it is done; a caller that runs many exports may keep one
    (``with WorkerPool(...) as pool``) and hand it to each."""

    def __init__(self, definitions: Definitions, jobs: int) -> None:
        assert 1 <= jobs <= MAX_JOBS
        self.definitions = definitions
        self.jobs = jobs
        self._executor: cf.ProcessPoolExecutor | None = None
        self._processes: list[Any] = []
        self.broken = False

    def submit(self, job: Job) -> cf.Future[Outcome]:
        if self._executor is None:
            self._executor = cf.ProcessPoolExecutor(
                max_workers=self.jobs,
                mp_context=multiprocessing.get_context("spawn"),
                initializer=_init_worker,
                initargs=(self.definitions, os.getpid()),
            )
        try:
            with (
                masked()
            ):  # threads and processes the pool starts here never take a shutdown signal
                future = self._executor.submit(run_job, job)
        except BrokenProcessPool as exc:
            raise self.died(exc) from exc
        except (OSError, RuntimeError) as exc:  # a process or thread limit, memory
            self.broken = True
            reason = exc.strerror if isinstance(exc, OSError) and exc.strerror else str(exc)
            raise IronbirdError(
                Code.EXP_WORKER,
                f"cannot start export workers ({reason[:200]}); run the export with --jobs 1 to "
                "decode in one process",
                details=f"{type(exc).__name__}: {exc}",
                cause=exc,
            ) from exc
        self._processes = list(getattr(self._executor, "_processes", {}).values())
        return future

    def died(self, exc: BaseException) -> IronbirdError:
        """The error for a pool whose worker process died; the pool is of no more use."""
        self.broken = True
        return IronbirdError(
            Code.EXP_WORKER,
            "an export worker process died (killed, or out of memory); run the export again, or "
            "with --jobs 1 to decode in one process",
            details=f"{type(exc).__name__}: {exc}",
            cause=exc,
        )

    def close(self, *, abnormal: bool = False) -> None:
        """End the workers: idle ones just exit; after an error or a signal (``abnormal``) they
        are terminated, and killed if they do not go."""
        pool, self._executor = self._executor, None
        if pool is None:
            return
        processes = list(getattr(pool, "_processes", {}).values()) or self._processes
        if abnormal or self.broken:
            pool.shutdown(wait=False, cancel_futures=True)
            for process in processes:
                with contextlib.suppress(Exception):
                    process.terminate()
            deadline = time.monotonic() + STOP_WAIT_S
            for process in processes:
                with contextlib.suppress(Exception):
                    process.join(max(0.0, deadline - time.monotonic()))
                    if process.is_alive():
                        process.kill()
                        process.join()
        else:
            pool.shutdown(wait=True, cancel_futures=True)
        self._processes = []

    def __enter__(self) -> WorkerPool:
        return self

    def __exit__(self, kind: object, value: object, trace: object) -> None:
        self.close(abnormal=kind is not None)


@dataclass(frozen=True, slots=True)
class ParallelOptions:
    """Whether and how an export uses the pool. ``jobs``: workers (1: never); ``min_bytes``: the
    size of the files below which the export stays in this process; ``range_bytes``: the size of
    a range; ``pool``: workers kept by the caller (``None``: the export starts its own)."""

    jobs: int
    definitions: Definitions
    shape: Shape
    min_bytes: int = MIN_POOL_BYTES
    range_bytes: int = RANGE_BYTES
    pool: WorkerPool | None = None
    report: PoolReport = field(default_factory=PoolReport)

    def __post_init__(self) -> None:
        assert 1 <= self.jobs <= MAX_JOBS
        assert self.range_bytes >= MIN_RANGE_BYTES and self.range_bytes % ALIGN == 0
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
        self._live: dict[int, cf.Future[Outcome]] = {}

    def _waiting_text(self) -> int:
        """Characters of row text in the results that have come back and are not yet written."""
        total = 0
        for future in self._live.values():
            if future.done() and not future.cancelled() and future.exception() is None:
                total += sum(len(block.text) for block in future.result().blocks)
        return total

    def cancel(self) -> None:
        """Drop the ranges still waiting for a worker."""
        for future in self._live.values():
            future.cancel()
        self._live.clear()

    def _await(self, future: cf.Future[Outcome]) -> Outcome:
        """The outcome of ``future``, waiting in slices so a shutdown signal is acted on."""
        while not future.done():
            cf.wait([future], timeout=WAIT_SLICE_S)
            checkpoint()
        try:
            return future.result()
        except BrokenProcessPool as exc:
            raise self._pool.died(exc) from exc
        except Exception as exc:  # a result that could not be sent back, a worker out of memory
            raise IronbirdError(
                Code.EXP_WORKER,
                "an export worker failed to return its result; run the export again, or with "
                "--jobs 1 to decode in one process",
                details="".join(traceback.format_exception(exc)),
                cause=exc,
            ) from exc

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
        count = -(-(size - body) // options.range_bytes)
        assert count >= 2
        bounds = [body] + [_align_up(body + i * options.range_bytes) for i in range(1, count)]
        assert all(a < b for a, b in itertools.pairwise(bounds))
        guess_seed = self._head_seed(reader, size)
        walk = _Walk(end=body)
        window = options.jobs * WINDOW_PER_WORKER
        jobs: dict[int, Job] = {}
        futures = self._live
        futures.clear()
        report.parts_pooled += 1
        damaged = closed = False
        submitted = 0

        def submit(index: int, *, exact: bool, guess: int, seed: Seed) -> None:
            stop = bounds[index + 1] if index + 1 < count else None
            job = Job(index, path, guess, stop, exact, seed, start, end, self._shape)
            jobs[index] = job
            futures[index] = self._pool.submit(job)
            report.workers = self._pool.jobs

        for index in range(count):
            while submitted < count and submitted - index < window:
                if submitted == 0:
                    submit(0, exact=True, guess=body, seed=walk.seed())
                else:
                    seed = walk.seed() if walk.streams else guess_seed
                    submit(submitted, exact=False, guess=bounds[submitted], seed=seed)
                submitted += 1
                report.ranges += 1
                report.max_pending = max(report.max_pending, submitted - index)
            outcome = self._await(futures[index])
            job = jobs[index]
            if not job.exact and not self._accepted(job, outcome, walk):
                report.reruns += 1
                submit(index, exact=True, guess=walk.end, seed=walk.seed())
                outcome = self._await(futures[index])
            report.max_text_bytes = max(report.max_text_bytes, self._waiting_text())
            futures.pop(index, None)
            jobs.pop(index, None)
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
            walk.last_seq = outcome.last_seq
            walk.work += outcome.work
            walk.end = outcome.end
            if replay.ended_by_damage or (not outcome.reached_stop and index + 1 < count):
                break
        for future in futures.values():
            future.cancel()
        futures.clear()
        replay.finish_part(reader, damaged=damaged, closed=closed)
        return True

    def _head_seed(self, reader: RecordingReader, size: int) -> Seed:
        """The streams and sources the head of the file defines: what the first ranges are
        seeded with, before the walk has shown what is in force."""
        streams: dict[int, Stream] = {}
        sources: dict[int, str] = {}
        for item in reader.head_records(reader.body_start, size):
            if item.rtype == RecordType.STREAM:
                streams[item.value.id] = item.value
            elif item.rtype == RecordType.SOURCE:
                sources[item.value[0]] = item.value[1]
        return Seed(streams, sources, 0, 0)

    def _accepted(self, job: Job, outcome: Outcome, walk: _Walk) -> bool:
        """Whether a range read from a guess is what the walk reads: see the module."""
        return (
            outcome.clean
            and outcome.start == walk.end
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
        the output refused a row."""
        replay, pipe = self._replay, self._pipe
        stats = replay.stats
        stats.frames_read += outcome.frames_read
        stats.frames_in_range += outcome.frames_in_range
        replay.note_gaps(outcome.gaps, outcome.gap_lost)
        for damage in outcome.damage:
            replay.note_damage(damage, reader, path)
        result = pipe.result
        counts = outcome.counts
        result.skipped_no_definitions += counts.skipped_no_definitions
        result.not_written += counts.not_written
        result.rejected.update(counts.rejected)
        if self._per_line:
            return self._write_lines(outcome)
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

    def _write_lines(self, outcome: Outcome) -> bool:
        """A destination that takes a line at a time and may refuse one (standard output: one
        stream, so one table at most): the rows are written one by one and counted as far as it
        takes them."""
        pipe = self._pipe
        result = pipe.result
        blocks = [b for b in outcome.blocks if b.text]
        assert len(blocks) <= 1
        lines: list[str] = []
        table = None
        if blocks:
            lines = blocks[0].text.split("\n")
            assert lines.pop() == ""
            if pipe.tables is not None:
                assert blocks[0].key is not None
                table = pipe.tables.table_for(*blocks[0].key)
        handle = table.handle if table is not None else 0
        taken = 0
        for kind in outcome.categories or b"":
            if not kind & NO_LINE:
                if not self._dest.write(handle, lines[taken] + "\n"):
                    return False
                taken += 1
                if table is not None:
                    table.rows += 1
            result.tally(kind & ~NO_LINE)
        assert taken == len(lines)
        return True


__all__ = [
    "MAX_JOBS",
    "MIN_POOL_BYTES",
    "RANGE_BYTES",
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
