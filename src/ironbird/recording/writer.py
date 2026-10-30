"""The recorder: an engine sink that writes frames to an ``.ibrec`` file (plan S3 "Writing").

:meth:`Recorder.append` is called synchronously on the event-loop thread for every frame the
session ingests or sends. It encodes the frame into a ``bytearray`` with precompiled structs
and never blocks: a full buffer hands over to a writer thread (``os.write`` releases the GIL),
which also owns ``fdatasync`` and the idle heartbeat. Nothing here can slow a socket read
beyond the encoding itself.

Layout rules the writer keeps so a reader (and ``ib rec info``) can use the chunk summaries:

* A *chunk* is one or more records closed by a CHUNK summary, every 1 MiB of frames or 1 s.
* Non-frame records (META, BLOB, STREAM, SOURCE, GAP, MARK) only appear at the start of a chunk:
  one that is needed while a chunk holds frames closes the chunk first.
* STREAM and SOURCE definitions are repeated every minute at a chunk start, so one damaged
  definition cannot make the rest of a stream unreadable.

Crash safety: every part is created ``O_EXCL`` and locked ``LOCK_EX``, and its preamble, META,
definitions and streams are written and synced (file and directory) before the part takes a
frame, so a crash never leaves a part without a readable head. ``fdatasync`` runs once per chunk
(policy ``chunk``): a power loss loses about a second; a process crash loses what was not yet
handed to the disk, which is the frames of the last 250 ms flush interval plus up to one timer
tick (about 500 ms at the worst); a clean stop or a signal loses nothing.

Back-pressure is bounded: buffered plus queued bytes never exceed ``max_buffer_bytes``; past it
frames are dropped and counted, and a GAP record (reason ``recorder_overrun``) says which once
there is room again. A failed write stops the recorder (``IB-REC-002``) and the capture goes on.
The statistics count frames *written* (handed to the disk by a completed write); frames encoded
but never written, and frames that arrive after a stop, are counted as not recorded.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import fcntl
import hashlib
import os
import queue
import signal
import threading
import time
import zlib
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from ironbird.config import FSYNC_POLICIES
from ironbird.errors import Code, IronbirdError
from ironbird.frame import Frame, GapReason, Stream
from ironbird.ledger import Ledger
from ironbird.notice import Level
from ironbird.recording import format as fmt
from ironbird.recording.format import BlobKind, MarkKind, RecordType

DEFAULT_MAX_BUFFER_BYTES: Final = 256 * 1024 * 1024
DEFAULT_MIN_FREE_BYTES: Final = 1024 * 1024 * 1024
FLUSH_BYTES: Final = 256 * 1024
FLUSH_INTERVAL_NS: Final = 250_000_000
CHUNK_BYTES: Final = 1024 * 1024
CHUNK_NS: Final = 1_000_000_000
REDEFINE_NS: Final = 60_000_000_000
HEARTBEAT_S: Final = 60.0
SPACE_CHECK_NS: Final = 10_000_000_000
SPACE_WARN_S: Final = 600.0  # warn when the projected time to a full disk drops under this
RATE_WINDOW_NS: Final = 60_000_000_000
CLOCK_STEP_NS: Final = 250_000_000  # wall clock vs. monotonic drift that is a clock step
CLOSE_STALL_S: Final = 5.0  # a close gives up when the writer makes no progress for this long
LOCK_WAIT_S: Final = 1.0
_DIRSYNC_UNSUPPORTED: Final = frozenset({errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP})
TICK_S: Final = 0.25
MIN_SPLIT_BYTES: Final = 1024 * 1024
MIN_SPLIT_SECONDS: Final = 10.0
MAX_PARTS: Final = 9999

_PADS: Final = tuple(bytes(n) for n in range(fmt.ALIGN))
_FRAME_TYPE: Final = int(RecordType.FRAME)
_HAS_MID: Final = 0x01
_HANDLED_SIGNALS: Final = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
assert FLUSH_BYTES < CHUNK_BYTES

Report = Callable[..., None]  # Session.report: (error, *, context, level, evidence)


@dataclass(frozen=True, slots=True)
class RecorderOptions:
    """Tunables of one recorder (the ``[recording]`` table and the CLI fill these in)."""

    fsync: str = "chunk"
    max_buffer_bytes: int = DEFAULT_MAX_BUFFER_BYTES
    min_free_bytes: int = DEFAULT_MIN_FREE_BYTES
    split_bytes: int | None = None
    split_seconds: float | None = None
    chunk_bytes: int = CHUNK_BYTES
    chunk_ns: int = CHUNK_NS
    flush_bytes: int = FLUSH_BYTES
    flush_ns: int = FLUSH_INTERVAL_NS
    heartbeat_s: float = HEARTBEAT_S

    def __post_init__(self) -> None:
        assert self.fsync in FSYNC_POLICIES
        assert self.max_buffer_bytes > 0 and self.min_free_bytes >= 0
        assert self.split_bytes is None or self.split_bytes > 0
        assert self.split_seconds is None or self.split_seconds > 0


@dataclass(frozen=True, slots=True)
class BlobSpec:
    """A definition file to embed: its kind, node, adapter and raw bytes."""

    kind: BlobKind
    node: str
    adapter: str
    content: bytes

    @property
    def sha256(self) -> bytes:
        return hashlib.sha256(self.content).digest()


@dataclass(frozen=True, slots=True)
class RecorderStats:
    """A snapshot of what the recorder has done."""

    frames: int  # frames written to the file (a completed write handed them to the disk)
    data_bytes: int  # payload bytes of those frames
    file_bytes: int  # bytes written to the files, all parts
    overrun_frames: int  # frames dropped because the buffer was full
    not_recorded_frames: int  # frames of streams this recorder was not given
    after_stop_frames: int  # frames that arrived after the recorder stopped
    unwritten_frames: int  # encoded but never written (a failed write, an abandoned close)
    gaps: int  # GAP records written
    chunks: int
    parts: int
    pending_bytes: int  # buffered plus queued for the writer
    peak_pending_bytes: int
    stopped: bool

    @property
    def lost_frames(self) -> int:
        """Frames offered to the recorder for its streams that are not in the file."""
        return self.overrun_frames + self.unwritten_frames + self.after_stop_frames


@dataclass(slots=True)
class _Loss:
    """Frames of one stream not written in one episode: count, time and sequence bounds."""

    count: int = 0
    first_ns: int = 0
    last_ns: int = 0
    first_seq: int = 0
    last_seq: int = 0

    def add(self, frame: Frame) -> None:
        if self.count == 0:
            self.first_ns, self.first_seq = frame.time_ns, frame.seq
        self.count += 1
        self.last_ns, self.last_seq = frame.time_ns, frame.seq


@contextlib.contextmanager
def _signals_blocked() -> Any:
    """Threads started inside inherit a mask that blocks the shutdown signals, so one never lands
    in a thread that cannot act on it."""
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, _HANDLED_SIGNALS)
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


class _WriterThread(threading.Thread):
    """Writes the jobs the event loop queues: ``(fd, data, sync, frames, data_bytes)``,
    ``("close", fd)``, ``("active", fd)`` and ``None`` (stop). The only code that blocks on the
    disk."""

    def __init__(
        self,
        on_error: Callable[[OSError], None],
        heartbeat_s: float,
        write: Callable[[int, memoryview], int],
        sync: Callable[[int], None],
    ) -> None:
        super().__init__(name="recorder-writer", daemon=True)
        self.jobs: queue.SimpleQueue[Any] = queue.SimpleQueue()
        self.written = 0  # bytes taken off the queue, written or discarded; this thread only
        self.bytes_ok = 0  # bytes of jobs that were written whole
        self.frames_ok = 0  # frames in those jobs
        self.data_ok = 0  # their payload bytes
        self.progress = 0  # bumped after every write, sync and job: a close watches it move
        self.synced = 0  # fdatasync calls made
        self.error: OSError | None = None
        self._on_error = on_error
        self._heartbeat_s = heartbeat_s
        self._write = write
        self._sync = sync
        self._active_fd = -1

    def run(self) -> None:
        while True:
            try:
                job = self.jobs.get(timeout=self._heartbeat_s)
            except queue.Empty:
                self._heartbeat()
                continue
            if job is None:
                return
            head = job[0]
            if head == "close":
                with contextlib.suppress(OSError):
                    os.close(job[1])
            elif head == "active":
                self._active_fd = job[1]
            else:
                fd, data, sync, frames, data_bytes = job
                size = len(data)
                if self.error is None:
                    self._guarded(self._write_all, fd, data, sync)
                    if self.error is None:
                        self.bytes_ok += size
                        self.frames_ok += frames
                        self.data_ok += data_bytes
                self.written += size
            self.progress += 1

    def _heartbeat(self) -> None:
        """Keep the file's mtime fresh while nothing is written, so retention sees it as live."""
        if self._active_fd >= 0 and self.error is None:
            with contextlib.suppress(OSError):
                os.utime(self._active_fd, None)

    def _guarded(self, func: Callable[..., None], *args: Any) -> None:
        try:
            func(*args)
        except OSError as exc:
            self.error = exc
            self._on_error(exc)

    def _write_all(self, fd: int, data: bytearray | bytes, sync: bool) -> None:
        view = memoryview(data)
        while view:
            count = self._write(fd, view)
            if count <= 0:
                raise OSError(errno.EIO, "write made no progress")
            view = view[count:]
            self.progress += 1
        if sync:
            self._sync(fd)
            self.synced += 1
            self.progress += 1


def create_exclusive(path: Path, *, part: int = 1) -> int:
    """Create ``path`` (mode 0600) and lock it exclusively; an existing file is never touched.

    Where the file system cannot lock (``ENOLCK``, ``EINVAL``, ``EOPNOTSUPP``) the recording goes
    on unlocked. ``part`` only words the error. Raises ``IB-REC-001``.
    """
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
    what = "recording" if part == 1 else f"part {part} of the recording"
    try:
        fd = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise IronbirdError(
            Code.REC_CREATE,
            f"cannot start part {part}: {path} already exists; the recording stops here "
            "(move that file away, or use a different name for the next recording)"
            if part > 1
            else f"{path} already exists; a recording is never overwritten (choose another -o)",
            cause=exc,
        ) from exc
    except OSError as exc:
        raise IronbirdError(
            Code.REC_CREATE,
            f"cannot create {what} {path}: {exc.strerror or exc}",
            cause=exc,
        ) from exc
    deadline = time.monotonic() + LOCK_WAIT_S
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN) and time.monotonic() < deadline:
                time.sleep(0.01)
                continue
            # Unsupported or held past a second: record unlocked (liveness rests on freshness).
        break
    return fd


def _discard_partial(fd: int, path: Path) -> bool:
    """Remove ``path`` if it is still the file ``fd`` denotes (the one this process created
    with ``O_EXCL``); a name that now denotes anything else is left alone."""
    try:
        held, named = os.fstat(fd), os.stat(path, follow_symlinks=False)
        if (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino):
            return False
        os.unlink(path)
    except OSError:
        return False
    return True


def sync_directory(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def part_path(base: Path, part: int) -> Path:
    """``name.ibrec`` for part 1, ``name.p0002.ibrec`` after it."""
    if part == 1:
        return base
    stem = base.name.removesuffix(".ibrec")
    return base.with_name(f"{stem}.p{part:04d}.ibrec")


class Recorder:
    """An engine sink that writes every frame it is given to a recording. See the module text.

    Build one with :meth:`open`, attach it with ``session.add_sink``, and ``await close()``.
    ``append`` and ``gap`` run on the loop thread; so does everything else but the writer thread.
    """

    def __init__(
        self,
        base_path: Path,
        meta: Mapping[str, Any],
        blobs: Sequence[BlobSpec],
        streams: Sequence[Stream],
        *,
        accept: frozenset[int] | None,
        options: RecorderOptions,
        report: Report | None,
        ledger: Ledger | None,
        loop: asyncio.AbstractEventLoop | None,
        mono_ns: Callable[[], int],
        wall_ns: Callable[[], int],
        write: Callable[[int, memoryview], int],
        sync: Callable[[int], None],
        capture_continues: bool,
    ) -> None:
        self._capture_continues = capture_continues
        self._base = base_path
        self._meta = dict(meta)
        self._blobs = tuple(blobs)
        self._accept = accept
        self._options = options
        self._report = report
        self._ledger = ledger
        self._loop = loop
        self._mono = mono_ns
        self._wall = wall_ns
        self._streams: dict[int, Stream] = {}  # defined in the current part, by id
        self._known: dict[int, Stream] = {}  # the fast path: the stream objects seen by identity
        self._source_ids: dict[str, int] = {}
        self._sources_by_id: list[str] = []
        self._initial_streams = tuple(streams)
        self._buf = bytearray()
        self._part = 1
        self._parts: list[Path] = []
        self._fd = -1
        self._held_fd = -1  # part 1 stays open and locked for the life of a split recording
        self._nonce = 0
        self._seed = 0
        self._offset = 0
        self._chunk_start = 0
        self._chunk_offsets: list[int] = []
        self._part_frames = 0
        self._part_started = 0
        self._part_last_seq = 0
        self._head_written = 0  # bytes of part heads, written synchronously
        self._buf_frames = 0  # frames and payload bytes in ``_buf``, not yet handed over
        self._buf_data = 0
        self._dirsync_warned = False
        self._closing = False
        self._last_seq = 0
        self._previous_last_seq = 0
        self._reset_chunk()
        self._frames = 0
        self._file_bytes = 0
        self._chunks = 0
        self._gaps = 0
        self._overrun_frames = 0
        self._not_recorded = 0
        self._after_stop = 0
        self._enqueued = 0
        self._peak_pending = 0
        self._stopped = False
        self._closed = False
        self._failed: IronbirdError | None = None
        self._overrun = False
        self._overrun_losses: dict[int, _Loss] = {}
        self._overrun_total = 0
        self._overrun_since_check = 0
        self._unrecorded: dict[int, _Loss] = {}
        self._clock_offset: int | None = None
        self._buf_since = 0
        self._last_redefine = 0
        self._last_space = 0
        self._rate_samples: deque[tuple[int, int]] = deque()
        self._timer: asyncio.TimerHandle | None = None
        self.failed_event: asyncio.Event | None = asyncio.Event() if loop is not None else None
        self._writer = _WriterThread(
            self._writer_failed, options.heartbeat_s, write, sync
        )  # started by open()

    # -- construction --

    @classmethod
    def open(
        cls,
        path: Path,
        meta: Mapping[str, Any],
        *,
        blobs: Sequence[BlobSpec] = (),
        streams: Sequence[Stream] = (),
        accept: frozenset[int] | None = None,
        options: RecorderOptions | None = None,
        report: Report | None = None,
        ledger: Ledger | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
        mono_ns: Callable[[], int] = time.monotonic_ns,
        wall_ns: Callable[[], int] = time.time_ns,
        write: Callable[[int, memoryview], int] = os.write,
        sync: Callable[[int], None] = os.fdatasync,
        capture_continues: bool = True,
    ) -> Recorder:
        """Create the recording at ``path`` and make it durable: preamble, META, definitions and
        streams are synced (file and directory) before this returns.

        Raises ``IB-REC-001`` (cannot create; exists), ``IB-REC-010`` (too little free space) or
        ``IB-REC-002`` (the first writes failed). ``loop`` enables the 250 ms timer; without one
        the recorder flushes only when frames arrive (tests, benchmarks). ``capture_continues``
        words the failure notice (whether the command keeps capturing without the recording). A
        start that fails after the file was created removes that file.
        """
        opts = options or RecorderOptions()
        recorder = cls(
            path,
            meta,
            blobs,
            streams,
            accept=accept,
            options=opts,
            report=report,
            ledger=ledger,
            loop=loop,
            mono_ns=mono_ns,
            wall_ns=wall_ns,
            write=write,
            sync=sync,
            capture_continues=capture_continues,
        )
        try:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError as exc:
            raise IronbirdError(
                Code.REC_CREATE,
                f"cannot create the recording directory {path.parent}: {exc.strerror or exc}",
                cause=exc,
            ) from exc
        recorder._check_start_space(path.parent)
        recorder._begin_part(first=True)
        with _signals_blocked():
            recorder._writer.start()
        recorder._writer.jobs.put(("active", recorder._fd))
        now = recorder._mono()
        recorder._last_redefine = recorder._last_space = recorder._buf_since = now
        if loop is not None:
            recorder._timer = loop.call_later(TICK_S, recorder._on_timer)
        return recorder

    def _check_start_space(self, directory: Path) -> None:
        if not self._options.min_free_bytes:
            return
        try:
            usage = os.statvfs(directory)
        except OSError:
            return  # the create that follows reports a directory that is unusable
        free = usage.f_bavail * usage.f_frsize
        if free < self._options.min_free_bytes:
            raise IronbirdError(
                Code.REC_SPACE,
                f"only {_size_text(free)} free in {directory}, below recording.min_free_bytes "
                f"({_size_text(self._options.min_free_bytes)}); free space, record elsewhere "
                "(-o), or lower recording.min_free_bytes",
            )

    # -- parts: create, head, rotate --

    def _head_bytes(self) -> bytes:
        """Preamble, META, BLOBs and every definition known, for the part being started."""
        wall = self._wall()
        meta = dict(self._meta)
        meta["created_ns"] = wall
        meta["created"] = _iso(wall)
        meta["part"] = self._part
        meta["previous_last_seq"] = self._previous_last_seq
        parts = [fmt.encode_preamble(self._nonce)]
        seed = self._seed
        parts.append(fmt.encode_record(RecordType.META, fmt.encode_meta(meta), seed))
        for blob in self._blobs:
            body = fmt.encode_blob(blob.kind, blob.node, blob.adapter, blob.sha256, blob.content)
            parts.append(fmt.encode_record(RecordType.BLOB, body, seed))
        for stream in self._streams.values():
            parts.append(fmt.encode_record(RecordType.STREAM, fmt.encode_stream(stream), seed))
        for source_id, text in enumerate(self._sources_by_id):
            parts.append(
                fmt.encode_record(RecordType.SOURCE, fmt.encode_source(source_id, text), seed)
            )
        return b"".join(parts)

    def _begin_part(self, *, first: bool) -> None:
        """Create the file of the current part and write its head, synced with its directory,
        before the part is used: a crash leaves a readable head or no file, never half a head.
        A failure removes the file this call created."""
        path = part_path(self._base, self._part)
        fd = create_exclusive(path, part=self._part)
        try:
            self._nonce = int.from_bytes(os.urandom(8), "little")
            self._seed = fmt.seed_of(self._nonce)
            if first:
                for stream in self._initial_streams:
                    if self._accept is None or stream.id in self._accept:
                        self._streams[stream.id] = stream
            head = self._head_bytes()
            self._write_head_now(fd, head, path)
        except BaseException as exc:
            removed = _discard_partial(fd, path)
            os.close(fd)
            if isinstance(exc, IronbirdError):
                note = (
                    "; the partial file was removed"
                    if removed
                    else f"; the partial file {path} could not be removed"
                )
                raise IronbirdError(
                    exc.code, exc.summary + note, cause=exc.cause, details=exc.details
                ) from exc
            raise
        self._parts.append(path)
        self._fd = fd
        self._offset = len(head)
        self._chunk_start = fmt.PREAMBLE_SIZE
        self._chunk_offsets = []
        self._part_frames = 0
        self._part_last_seq = 0
        self._part_started = self._mono()
        self._reset_chunk()
        self._file_bytes += len(head)
        self._head_written += len(head)
        if not first:
            self._writer.jobs.put(("active", fd))

    def _write_head_now(self, fd: int, head: bytes, path: Path) -> None:
        try:
            view = memoryview(head)
            while view:
                view = view[os.write(fd, view) :]
            os.fsync(fd)
        except OSError as exc:
            raise IronbirdError(
                Code.REC_WRITE,
                f"cannot write the start of recording {path}: {exc.strerror or exc}",
                cause=exc,
            ) from exc
        try:
            sync_directory(path.parent)
        except OSError as exc:
            if exc.errno not in _DIRSYNC_UNSUPPORTED:
                raise IronbirdError(
                    Code.REC_WRITE,
                    f"cannot sync the directory of recording {path}: {exc.strerror or exc}",
                    cause=exc,
                ) from exc
            if not self._dirsync_warned:
                self._dirsync_warned = True
                self._notify(
                    IronbirdError(
                        Code.REC_DIRSYNC,
                        f"{path.parent} cannot be synced ({exc.strerror or exc}): after a power "
                        "loss a recording's name may be missing even though its data was written",
                    ),
                    Level.WARNING,
                )

    def _reset_chunk(self) -> None:
        self._chunk_frames = 0
        self._chunk_bytes_used = 0
        self._chunk_nomid = 0
        self._chunk_first_seq = self._chunk_last_seq = 0
        self._chunk_min_ns = self._chunk_max_ns = 0
        self._chunk_first_mono = self._chunk_last_mono = 0
        self._chunk_mids: dict[int, int] = {}
        self._chunk_overflow = False
        self._chunk_opened = 0

    # -- properties --

    @property
    def path(self) -> Path:
        """The first part's path (the recording's name)."""
        return self._base

    @property
    def parts(self) -> tuple[Path, ...]:
        return tuple(self._parts)

    @property
    def failed(self) -> IronbirdError | None:
        """The write failure that stopped the recorder, if any."""
        return self._failed

    @property
    def stopped(self) -> bool:
        return self._stopped

    @property
    def frames(self) -> int:
        """Frames accepted for the recording so far, written or not yet (cheaper than
        :meth:`stats`, which counts the written ones)."""
        return self._frames

    @property
    def nonce(self) -> int:
        return self._nonce

    def stats(self) -> RecorderStats:
        pending = self._enqueued - self._writer.written + len(self._buf)
        written = self._writer.frames_ok
        # While recording, frames not yet written are in flight, not lost; once the recorder has
        # stopped or closed, whatever was encoded and never written is.
        unwritten = self._frames - written if self._stopped or self._closed else 0
        return RecorderStats(
            written,
            self._writer.data_ok,
            self._head_written + self._writer.bytes_ok,
            self._overrun_frames,
            self._not_recorded,
            self._after_stop,
            unwritten,
            self._gaps,
            self._chunks,
            self._part,
            pending,
            max(self._peak_pending, pending),
            self._stopped,
        )

    def rate_bytes_per_s(self) -> float:
        """Bytes per second written over the last minute, for retention's projection."""
        samples = self._rate_samples
        if len(samples) < 2:
            return 0.0
        (t0, b0), (t1, b1) = samples[0], samples[-1]
        return (b1 - b0) * 1e9 / (t1 - t0) if t1 > t0 else 0.0

    # -- the sink --

    def append(self, frame: Frame) -> None:
        """Encode ``frame`` into the recording. Cheap, synchronous, and never raises: a failure
        stops the recorder (``IB-INT-001``) and the frame is counted as not recorded."""
        encoded = self._frames
        try:
            self._append(frame)
        except Exception as exc:
            self._internal_failure(exc)
            if self._frames == encoded:  # the frame itself never got into the buffer
                self._after_stop += 1

    def _append(self, frame: Frame) -> None:
        if self._stopped:
            self._after_stop += 1
            return
        stream = frame.stream
        sid = stream.id
        if self._known.get(sid) is not stream and not self._admit(stream, frame):
            return
        source = self._source_ids.get(frame.source)
        if source is None:
            source = self._define_source(frame.source)
        if self._overrun:
            self._drop_overrun(frame, sid)
            return
        seq = frame.seq
        data = frame.data
        size = len(data)
        bits = int(frame.flags)
        mid = frame.mid or 0
        mono = self._mono()
        wall = frame.time_ns
        offset_now = wall - mono
        clock_offset = self._clock_offset
        if clock_offset is None:
            self._clock_offset = offset_now
        elif abs(offset_now - clock_offset) > CLOCK_STEP_NS:
            self._clock_step(wall, offset_now - clock_offset)
            self._clock_offset = offset_now
        body_len = fmt.FRAME_FIXED_SIZE + size
        fixed = fmt.FRAME_FIXED.pack(seq, wall, sid, source, bits, 0, mid, frame.orig_len)
        head = fmt.ENVELOPE_HEAD.pack(body_len, _FRAME_TYPE, 0, 0)
        crc = zlib.crc32(data, zlib.crc32(fixed, zlib.crc32(head, self._seed)))
        pad = -(fmt.ENVELOPE_SIZE + body_len) & 7
        buf = self._buf
        buf += head
        buf += crc.to_bytes(4, "little")
        buf += fixed
        buf += data
        if pad:
            buf += _PADS[pad]
        record = fmt.ENVELOPE_SIZE + body_len + pad
        self._offset += record
        # chunk summary
        if self._chunk_frames == 0:
            self._chunk_first_seq = seq
            self._chunk_min_ns = self._chunk_max_ns = wall
            self._chunk_first_mono = mono
            self._chunk_opened = mono
        elif wall < self._chunk_min_ns:
            self._chunk_min_ns = wall
        elif wall > self._chunk_max_ns:
            self._chunk_max_ns = wall
        self._chunk_frames += 1
        self._chunk_bytes_used += record
        self._chunk_last_seq = seq
        self._chunk_last_mono = mono
        if bits & _HAS_MID:
            mids = self._chunk_mids
            count = mids.get(mid)
            if count is not None:
                mids[mid] = count + 1
            elif len(mids) < fmt.MAX_CHUNK_MIDS:
                mids[mid] = 1
            else:
                self._chunk_overflow = True
        else:
            self._chunk_nomid += 1
        self._frames += 1
        self._buf_frames += 1
        self._buf_data += size
        self._part_frames += 1
        self._part_last_seq = self._last_seq = seq
        self._file_bytes += record
        if self._chunk_bytes_used >= self._options.chunk_bytes or (
            mono - self._chunk_opened >= self._options.chunk_ns
        ):
            self._close_chunk()
        elif len(buf) >= self._options.flush_bytes or (
            mono - self._buf_since >= self._options.flush_ns
        ):
            self._flush(sync=self._options.fsync == "always")

    def _admit(self, stream: Stream, frame: Frame) -> bool:
        """Slow path for a stream not seen by identity: define it, or count its frame as not
        recorded when this recorder was not given the stream."""
        sid = stream.id
        if self._accept is not None and sid not in self._accept:
            self._define_stream(stream)
            self._unrecorded.setdefault(sid, _Loss()).add(frame)
            self._not_recorded += 1
            return False
        defined = self._streams.get(sid)
        if defined is None:
            self._define_stream(stream)
        else:
            assert defined == stream, f"stream id {sid} was reused for a different stream"
        self._known[sid] = stream
        return True

    def _define_stream(self, stream: Stream) -> None:
        if stream.id in self._streams:
            return
        self._streams[stream.id] = stream
        self._head_record(RecordType.STREAM, fmt.encode_stream(stream))

    def _define_source(self, text: str) -> int:
        if len(self._sources_by_id) >= fmt.MAX_SOURCES:
            return fmt.OTHER_SOURCE
        source_id = len(self._sources_by_id)
        self._sources_by_id.append(text)
        self._source_ids[text] = source_id
        self._head_record(RecordType.SOURCE, fmt.encode_source(source_id, text))
        return source_id

    def _drop_overrun(self, frame: Frame, sid: int) -> None:
        self._overrun_frames += 1
        self._overrun_total += 1
        loss = self._overrun_losses.get(sid)
        if loss is None:
            loss = self._overrun_losses[sid] = _Loss()
        loss.add(frame)
        self._overrun_since_check += 1
        if self._overrun_since_check >= 256:
            self._overrun_since_check = 0
            self._recheck_overrun()

    def _clock_step(self, wall_ns: int, delta_ns: int) -> None:
        self.mark(MarkKind.CLOCK_STEP, f"wall clock stepped by {delta_ns / 1e6:+.1f} ms", wall_ns)

    # -- records outside frames --

    def _head_record(self, rtype: RecordType, body: bytes) -> None:
        """Append a non-frame record at a chunk start (closing a chunk that holds frames)."""
        if self._stopped:
            return
        if self._chunk_frames:
            self._close_chunk()
        record = fmt.encode_record(rtype, body, self._seed)
        self._buf += record
        self._offset += len(record)
        self._file_bytes += len(record)

    def gap(
        self,
        stream: Stream,
        reason: GapReason,
        count: int,
        first_ns: int,
        last_ns: int,
        first_seq: int,
        last_seq: int,
    ) -> None:
        """Frames the engine knows were lost before they were given to this sink (kernel drop).
        Never raises: a failure stops the recorder (``IB-INT-001``)."""
        try:
            self._gap(stream, reason, count, first_ns, last_ns, first_seq, last_seq)
        except Exception as exc:
            self._internal_failure(exc)

    def _gap(
        self,
        stream: Stream,
        reason: GapReason,
        count: int,
        first_ns: int,
        last_ns: int,
        first_seq: int,
        last_seq: int,
    ) -> None:
        if self._stopped or (self._accept is not None and stream.id not in self._accept):
            return
        if count < 1:
            return
        if stream.id not in self._streams:
            self._define_stream(stream)
            self._known[stream.id] = stream
        self._write_gap(
            fmt.Gap(
                stream.id, reason, count, first_ns, last_ns, first_seq, max(first_seq, last_seq)
            )
        )

    def _write_gap(self, gap: fmt.Gap) -> None:
        self._gaps += 1
        self._head_record(RecordType.GAP, fmt.encode_gap(gap))

    def mark(self, kind: MarkKind, text: str = "", time_ns: int | None = None) -> None:
        """A session event (start, stop, signal, link up or down, clock step, note). Never
        raises: a failure stops the recorder (``IB-INT-001``)."""
        if self._stopped:
            return
        try:
            when = self._wall() if time_ns is None else time_ns
            self._head_record(RecordType.MARK, fmt.encode_mark(when, kind, text))
        except Exception as exc:
            self._internal_failure(exc)

    # -- chunks and flushing --

    def _close_chunk(self, *, final: bool = False) -> None:
        """Write the CHUNK summary of the records since the chunk start and flush with a sync."""
        if self._stopped:
            return
        mids = sorted(self._chunk_mids.items())
        chunk = fmt.Chunk(
            self._chunk_start,
            self._chunk_first_seq,
            self._chunk_last_seq,
            self._chunk_min_ns,
            self._chunk_max_ns,
            self._chunk_first_mono,
            self._chunk_last_mono,
            self._chunk_frames,
            self._chunk_bytes_used,
            self._chunk_nomid,
            fmt.CHUNK_FLAG_MID_OVERFLOW if self._chunk_overflow else 0,
            tuple(mids),
        )
        record = fmt.encode_record(RecordType.CHUNK, fmt.encode_chunk(chunk), self._seed)
        self._chunk_offsets.append(self._offset)
        self._buf += record
        self._offset += len(record)
        self._file_bytes += len(record)
        self._chunk_start = self._offset
        self._chunks += 1
        self._reset_chunk()
        self._flush(sync=self._options.fsync != "never")
        if final:
            return
        now = self._mono()
        if now - self._last_redefine >= REDEFINE_NS:
            self._last_redefine = now
            self._redefine()
        self._maybe_rotate(now)

    def _redefine(self) -> None:
        """Repeat every STREAM and SOURCE at the start of the chunk just begun."""
        seed = self._seed
        for stream in self._streams.values():
            record = fmt.encode_record(RecordType.STREAM, fmt.encode_stream(stream), seed)
            self._buf += record
            self._offset += len(record)
            self._file_bytes += len(record)
        for source_id, text in enumerate(self._sources_by_id):
            record = fmt.encode_record(RecordType.SOURCE, fmt.encode_source(source_id, text), seed)
            self._buf += record
            self._offset += len(record)
            self._file_bytes += len(record)

    def _flush(self, *, sync: bool) -> None:
        buf = self._buf
        if not buf:
            return
        self._buf = bytearray()
        self._buf_since = self._mono()
        self._enqueued += len(buf)
        self._writer.jobs.put((self._fd, buf, sync, self._buf_frames, self._buf_data))
        self._buf_frames = self._buf_data = 0
        pending = self._enqueued - self._writer.written
        self._peak_pending = max(self._peak_pending, pending)
        if pending >= self._options.max_buffer_bytes and not self._overrun and not self._closing:
            self._begin_overrun(pending)

    # -- back-pressure ----------------------------------------------------------------------------

    def _begin_overrun(self, pending: int) -> None:
        self._overrun = True
        self._overrun_total = 0
        self._notify(
            IronbirdError(
                Code.REC_OVERRUN,
                f"recorder buffer full ({_size_text(pending)} waiting for the disk): "
                "frames are being dropped, the recording will show a GAP",
            ),
            Level.ERROR,
        )
        if self._ledger is not None:
            self._ledger.record("state", "recording.overrun", pending=pending)

    def _recheck_overrun(self) -> None:
        """End the episode once the writer has caught up to half the cap."""
        if not self._overrun:
            return
        pending = self._enqueued - self._writer.written + len(self._buf)
        if pending >= self._options.max_buffer_bytes // 2:
            return
        self._overrun = False
        dropped, losses = self._overrun_total, self._overrun_losses
        self._overrun_losses = {}
        self._overrun_total = 0
        for sid, loss in sorted(losses.items()):
            self._write_gap(
                fmt.Gap(
                    sid,
                    GapReason.RECORDER_OVERRUN,
                    loss.count,
                    loss.first_ns,
                    loss.last_ns,
                    loss.first_seq,
                    loss.last_seq,
                )
            )
        self._notify(
            IronbirdError(
                Code.REC_OVERRUN,
                f"recorder caught up; {dropped} frame(s) were dropped while the buffer was full",
            ),
            Level.ERROR,
        )
        if self._ledger is not None:
            self._ledger.record("state", "recording.overrun_end", dropped=dropped)

    def _flush_unrecorded(self) -> None:
        if not self._unrecorded:
            return
        losses, self._unrecorded = self._unrecorded, {}
        for sid, loss in sorted(losses.items()):
            self._write_gap(
                fmt.Gap(
                    sid,
                    GapReason.NOT_RECORDED,
                    loss.count,
                    loss.first_ns,
                    loss.last_ns,
                    loss.first_seq,
                    loss.last_seq,
                )
            )

    # -- timer: the work that must happen even when no frame arrives --

    def _on_timer(self) -> None:
        self._timer = None
        if self._closed:
            return
        try:
            self.tick()
        except Exception as exc:  # a bug here must not become an unhandled loop exception
            self._internal_failure(exc)
            return
        loop = self._loop
        if loop is not None and not self._closed:
            self._timer = loop.call_later(TICK_S, self._on_timer)

    def tick(self) -> None:
        """Periodic work (every 250 ms from the timer): time-based flush and chunk close, the end
        of an overrun, the disk-space projection, the rate samples. Public for tests."""
        if self._stopped:
            return
        now = self._mono()
        self._recheck_overrun()
        self._flush_unrecorded()
        if self._chunk_frames and now - self._chunk_opened >= self._options.chunk_ns:
            self._close_chunk()
        elif self._buf and now - self._buf_since >= self._options.flush_ns:
            self._flush(sync=self._options.fsync == "always")
        samples = self._rate_samples
        if not samples or now - samples[-1][0] >= 1_000_000_000:
            samples.append((now, self._file_bytes))
            while len(samples) > 2 and now - samples[0][0] > RATE_WINDOW_NS:
                samples.popleft()
        if now - self._last_space >= SPACE_CHECK_NS:
            self._last_space = now
            self._check_space()

    def _check_space(self) -> None:
        rate = self.rate_bytes_per_s()
        if rate <= 0:
            return
        try:
            usage = os.statvfs(self._base.parent)
        except OSError:
            return
        free = usage.f_bavail * usage.f_frsize
        if free / rate < SPACE_WARN_S:
            self._notify(
                IronbirdError(
                    Code.REC_SPACE,
                    f"{_size_text(free)} free in {self._base.parent} at {_size_text(int(rate))}/s "
                    f"is {free / rate / 60:.0f} min to a full disk; free space or stop recording",
                ),
                Level.WARNING,
            )

    # -- splitting --

    def _maybe_rotate(self, now: int) -> None:
        opts = self._options
        due = (opts.split_bytes is not None and self._offset >= opts.split_bytes) or (
            opts.split_seconds is not None and now - self._part_started >= opts.split_seconds * 1e9
        )
        if not due or self._part >= MAX_PARTS:
            return
        self._finish_part()
        old_fd = self._fd
        self._previous_last_seq = self._last_seq
        self._part += 1
        try:
            self._begin_part(first=False)
        except IronbirdError as exc:
            self._part -= 1
            self._stop_with(exc)
            return
        except OSError as exc:  # pragma: no cover - create_exclusive wraps what it can
            self._part -= 1
            self._stop_with(
                IronbirdError(Code.REC_CREATE, f"cannot start the next part: {exc}", cause=exc)
            )
            return
        if self._part == 2:
            self._held_fd = old_fd  # part 1 stays open and locked until the session ends
        else:
            self._writer.jobs.put(("close", old_fd))
        self._ledger_part()

    def _ledger_part(self) -> None:
        if self._ledger is not None:
            self._ledger.record(
                "state",
                "recording.part",
                part=self._part,
                path=self._parts[-1],
                previous_last_seq=self._previous_last_seq,
            )

    def _finish_part(self) -> None:
        """Index and trailer for the part being closed (its last chunk is already closed)."""
        index_offset = 0
        if len(self._chunk_offsets) <= fmt.MAX_INDEX_ENTRIES:
            index_offset = self._offset
            record = fmt.encode_record(
                RecordType.INDEX, fmt.encode_index(self._chunk_offsets), self._seed
            )
            self._buf += record
            self._offset += len(record)
            self._file_bytes += len(record)
        trailer = fmt.Trailer(index_offset, self._part_frames, self._part_last_seq)
        record = fmt.encode_record(RecordType.TRAILER, fmt.encode_trailer(trailer), self._seed)
        self._buf += record
        self._offset += len(record)
        self._file_bytes += len(record)
        self._flush(sync=True)

    # -- failure --

    def _notify(self, error: IronbirdError, level: Level) -> None:
        report = self._report
        if report is not None:
            report(error, context=f"recording {self._base.name}", level=level)

    def _writer_failed(self, exc: OSError) -> None:
        """The writer thread's I/O failed (called on that thread): hand over to the loop."""
        loop = self._loop
        if loop is None:
            self._stop_after_error(exc)
            return
        with contextlib.suppress(RuntimeError):  # the loop may already be gone at shutdown
            loop.call_soon_threadsafe(self._stop_after_error, exc)

    def _stop_after_error(self, exc: OSError) -> None:
        written = self._writer.frames_ok
        goes_on = (
            "the capture continues and later frames are not recorded"
            if self._capture_continues
            else "the capture ends"
        )
        self._stop_with(
            IronbirdError(
                Code.REC_WRITE,
                f"recording stopped after {written} frame(s), {self._frames - written} not "
                f"recorded: writing {self._parts[-1] if self._parts else self._base} failed "
                f"({exc.strerror or exc}); free space or record elsewhere - {goes_on}",
                cause=exc,
            )
        )

    def _internal_failure(self, exc: Exception) -> None:
        self._stop_with(
            IronbirdError(
                Code.INTERNAL,
                f"the recorder failed and stopped: {type(exc).__name__}: {exc}",
                cause=exc,
            )
        )

    def _stop_with(self, error: IronbirdError) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._failed = error
        if self.failed_event is not None:
            self.failed_event.set()
        if self._ledger is not None:
            self._ledger.record(
                "error", "recording.failed", code=error.code.value, summary=error.summary
            )
        self._notify(error, Level.ERROR)

    # -- closing --

    async def close(self) -> RecorderStats:
        """Write the closing records (final chunk, index, trailer), sync, and close the files.

        The wait for the disk has no overall limit, because a backlog that is draining must
        finish (the GAP for dropped frames and the trailer are behind it); it ends when the
        writer makes no progress for :data:`CLOSE_STALL_S`. A disk that stops answering then
        leaves a file without its trailer, readable up to the last record that reached it, and
        the notice says how many frames were not written.
        """
        if self._closed:
            return self.stats()
        self._closed = self._closing = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self._stopped:
            try:
                self._recheck_overrun_final()
                self._flush_unrecorded()
                self._close_chunk(final=True)
                self._finish_part()
            except Exception as exc:
                self._internal_failure(exc)
        jobs = self._writer.jobs
        jobs.put(("close", self._fd))
        if self._held_fd >= 0:
            jobs.put(("close", self._held_fd))
        jobs.put(None)
        seen, moved_at = self._writer.progress, time.monotonic()
        while self._writer.is_alive():
            await asyncio.sleep(0.005)
            now = time.monotonic()
            if self._writer.progress != seen:
                seen, moved_at = self._writer.progress, now
            elif now - moved_at >= CLOSE_STALL_S:
                break
        if self._writer.is_alive():
            self._stop_with(
                IronbirdError(
                    Code.REC_WRITE,
                    f"the disk made no progress for {CLOSE_STALL_S:g} s while closing "
                    f"{self._base}: {self._frames - self._writer.frames_ok} frame(s) were not "
                    "written and the recording may lack its trailer (it is readable up to the "
                    "last record that reached the disk)",
                )
            )
        stats = self.stats()
        if self._ledger is not None:
            self._ledger.record(
                "state",
                "recording.stop",
                path=self._base,
                parts=[str(p) for p in self._parts],
                frames=stats.frames,
                file_bytes=stats.file_bytes,
                overrun_frames=stats.overrun_frames,
                not_recorded_frames=stats.not_recorded_frames,
                unwritten_frames=stats.unwritten_frames,
                after_stop_frames=stats.after_stop_frames,
                lost_frames=stats.lost_frames,
                gaps=stats.gaps,
                chunks=stats.chunks,
                failed=self._failed.code.value if self._failed else None,
            )
        return stats

    def _recheck_overrun_final(self) -> None:
        """At close the dropped frames are accounted for even if the buffer never drained."""
        if not self._overrun:
            return
        self._overrun = False
        losses, self._overrun_losses = self._overrun_losses, {}
        for sid, loss in sorted(losses.items()):
            self._write_gap(
                fmt.Gap(
                    sid,
                    GapReason.RECORDER_OVERRUN,
                    loss.count,
                    loss.first_ns,
                    loss.last_ns,
                    loss.first_seq,
                    loss.last_seq,
                )
            )


def _size_text(count: int) -> str:
    value = float(count)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")  # pragma: no cover


def _iso(time_ns: int) -> str:
    seconds, ns = divmod(time_ns, 1_000_000_000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)) + f".{ns // 1000:06d}Z"


__all__ = [
    "FSYNC_POLICIES",
    "BlobSpec",
    "Recorder",
    "RecorderOptions",
    "RecorderStats",
    "create_exclusive",
    "part_path",
]
