"""The ledger's on-disk side: part files, the essential reserve, rotation, stall handling.

:class:`LedgerStore` is a synchronous state machine over a :class:`~ironbird.fsops.FileSystem`
and an injected clock; :class:`LedgerWriter` runs it on one daemon thread fed by the ledger's
drain task, so a slow or hung disk never blocks the event loop. Every file system call of the
ledger happens here, on that thread.

Files of a session (all in the state directory, mode 0600)::

    <stamp>-<session>.jsonl            part 1, held under flock for the session's life
    <stamp>-<session>.pNNNN.jsonl      later parts (rotation, retired reserves)
    <stamp>-<session>.rNNNN.reserve    the preallocated essential reserve of a long session

Nothing here deletes anything. A reserve is cut to its last complete record and renamed without
replacing (:func:`ironbird.atomicfile.rename_noreplace`) when it retires.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import itertools
import os
import queue
import signal
import stat
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, NamedTuple

from ironbird.atomicfile import Collision, SyncAfterCommit, rename_noreplace
from ironbird.fsops import (
    REAL_FS,
    RESERVE_FS_ALLOWLIST,
    FileSystem,
    close_quietly,
    same_file,
    try_stat_at,
    write_all,
)
from ironbird.ledgertiers import TIER_COUNT, Tier

SHUTDOWN_SIGNALS: Final = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
MAX_PART_NUMBER: Final = 999_999_999
NAME_TRIES: Final = 16  # part or reserve names tried per attempt
OPEN_TRIES: Final = 3  # session ids tried when part 1 already exists
LOCK_TRIES: Final = 50  # x 20 ms: the opening rule's 1 s
LOCK_STEP_S: Final = 0.02
MAX_RUN: Final = 512  # lines written by one write call
HANG_S: Final = 5.0
RETRY_S: Final = 5.0
SYNC_S: Final = 1.0
HEARTBEAT_S: Final = 60.0
MINUTE_S: Final = 60.0
POLL_S: Final = 0.25
RESERVE_HEADER_MAX: Final = 4096

# Errnos that mean the location is wrong and only the user can fix it, versus the disk or the
# system failing right now (decision D-b). Anything not listed as transient is a configuration
# failure while the ledger is being opened.
TRANSIENT_ERRNOS: Final = frozenset(
    {
        errno.ENOSPC,
        errno.EDQUOT,
        errno.EIO,
        errno.EMFILE,
        errno.ENFILE,
        errno.ENOMEM,
        errno.ESTALE,
        errno.ETIMEDOUT,
    }
)
FULL_ERRNOS: Final = frozenset({errno.ENOSPC, errno.EDQUOT})
NO_LOCK_ERRNOS: Final = frozenset({errno.ENOLCK, errno.EINVAL, errno.EOPNOTSUPP, errno.EBADF})

_PART_FLAGS: Final = (
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_APPEND | os.O_NOFOLLOW | os.O_CLOEXEC
)
# A reserve is never opened O_APPEND: pwrite on an O_APPEND descriptor appends past the extent.
_RESERVE_FLAGS: Final = os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
assert not _RESERVE_FLAGS & os.O_APPEND


class TxInfo:
    """A transmission's trace: what it was, where it went, when, and how many sends it stands
    for (a counted batch of responder replies is one record standing for many)."""

    __slots__ = ("count", "kind", "subject", "target", "time_ns")

    def __init__(
        self, subject: str, target: str, time_ns: int, count: int = 1, kind: str = "command"
    ) -> None:
        self.subject = subject  # "NOOP", "reply MOCK_HK"
        self.target = target  # "fc" (empty for a reply)
        self.time_ns = time_ns
        self.count = count
        self.kind = kind  # "command" | "reply"

    def sent(self) -> str:
        """``NOOP sent to fc``"""
        return f"{self.subject} sent to {self.target}" if self.target else f"{self.subject} sent"

    def to(self) -> str:
        """``NOOP to fc``"""
        return f"{self.subject} to {self.target}" if self.target else self.subject


class Line(NamedTuple):
    n: int
    tier: int
    data: bytes  # one complete JSON line, newline included
    tx: TxInfo | None


class TxTally:
    """Transmissions in a set of lines: how many, and the first and last."""

    __slots__ = ("count", "first", "kinds", "last")

    def __init__(self) -> None:
        self.count = 0
        self.first: TxInfo | None = None
        self.last: TxInfo | None = None
        self.kinds: set[str] = set()

    def add(self, info: TxInfo) -> None:
        self.count += info.count
        self.kinds.add(info.kind)
        if self.first is None:
            self.first = info
        self.last = info

    def merge(self, other: TxTally) -> None:
        if other.first is None or other.last is None:
            return
        self.count += other.count
        self.kinds |= other.kinds
        if self.first is None or other.first.time_ns < self.first.time_ns:
            self.first = other.first
        if self.last is None or other.last.time_ns >= self.last.time_ns:
            self.last = other.last


Formatter = Callable[[str, int, int, str, str, Mapping[str, Any]], bytes]


@dataclass(frozen=True, slots=True)
class StoreConfig:
    directory: Path
    max_file_bytes: int
    reserve_bytes: int  # 0: no reserve (short commands hold none)
    long_session: bool
    command: str
    version: str
    sync_s: float = SYNC_S
    heartbeat_s: float = HEARTBEAT_S
    retry_s: float = RETRY_S
    hang_s: float = HANG_S
    minute_s: float = MINUTE_S


class ConfigFailure(Exception):
    """The ledger location is wrong; only the user can fix it (``IB-LED-002``)."""

    def __init__(self, cause: OSError) -> None:
        super().__init__(str(cause))
        self.cause = cause


class _Stall(Exception):
    """Internal: an essential record cannot be written right now."""

    def __init__(self, cause: OSError | None, text: str = "", *, internal: bool = False) -> None:
        super().__init__(text or str(cause))
        self.cause = cause
        self.internal = internal  # a bug in ironbird, not a failing disk
        self.text = text or (cause.strerror if cause is not None else "") or "write failed"
        self.errno = cause.errno if cause is not None else None


def classify_open_errno(exc: OSError) -> bool:
    """``True`` when an open failure is a configuration problem rather than a transient one."""
    return exc.errno not in TRANSIENT_ERRNOS


def stamp_of(time_ns: int) -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(time_ns // 1_000_000_000))


def part_name(stamp: str, session: str, number: int) -> str:
    """Part 1 has no number in its name; later ones are ``pNNNN`` (at least four digits)."""
    assert 1 <= number <= MAX_PART_NUMBER
    if number == 1:
        return f"{stamp}-{session}.jsonl"
    return f"{stamp}-{session}.p{number:04d}.jsonl"


def reserve_name(stamp: str, session: str, number: int) -> str:
    assert 1 <= number <= MAX_PART_NUMBER
    return f"{stamp}-{session}.r{number:04d}.reserve"


@dataclass(slots=True)
class _File:
    fd: int
    name: str
    size: int  # end of the last complete line
    header_len: int
    is_reserve: bool = False
    extent: int = 0  # reserve: allocated size
    allocated: bool = False  # reserve: blocks really reserved
    locked: bool = False
    number: int = 0  # part number (a reserve: the number it takes when it retires)


class LedgerStore:
    """Synchronous on-disk state of one ledger session. Not thread-safe: one thread drives it
    (the writer thread); the loop reads only the plain attributes documented as shared."""

    def __init__(
        self,
        cfg: StoreConfig,
        session_id: str,
        start_ns: int,
        fmt: Formatter,
        next_number: Callable[[], int],
        *,
        fs: FileSystem = REAL_FS,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        resume_info: Callable[[], Mapping[str, int]] = lambda: {},
    ) -> None:
        self.cfg = cfg
        self.fs = fs
        self.clock = clock
        self._sleep = sleep
        self._fmt = fmt
        self._next_number = next_number
        self._resume_info = resume_info
        self.session_id = session_id
        self.start_ns = start_ns
        # -- shared with the loop (plain attributes, written by the writer thread only)
        self.events: deque[tuple[str, dict[str, Any]]] = deque()
        self.tier_bytes: list[int] = [0] * TIER_COUNT
        self.tier_records: list[int] = [0] * TIER_COUNT
        self.opened = False
        self.stalled = False
        self.stall_text = ""
        self.stall_errno: int | None = None
        self.stall_internal = False  # the stall is a bug in ironbird, not the disk
        self.stall_start = 0.0
        self.in_reserve = False
        self.call_started: float | None = None
        self.possibly_lost_records = 0
        self.possibly_lost_tx = TxTally()
        self.backlog: deque[Line] = deque()
        self.inflight: list[Line] = []  # taken from the backlog, being written right now
        self.resolved_n = 0  # every line numbered at most this has been written or dropped
        self.confirmed_lines = 0  # lines written so far (for an exit while a call is blocked)
        # Guards ``backlog``, ``inflight``, ``resolved_n`` and ``_unsynced_tx`` where the writer
        # moves lines between them, so :meth:`unconfirmed` sees one consistent picture. Held for
        # a few list operations, never across a file system call.
        self._state_lock = threading.Lock()
        # -- owned by the writer thread
        self._dir_fd = -1
        self._stamp = stamp_of(start_ns)
        self._part1: _File | None = None
        self._active: _File | None = None
        self._standby: _File | None = None
        self._highest_part = 1
        self._highest_reserve = 0
        self._last_attempt = float("-inf")
        self._last_sync = clock()
        self._last_activity = clock()
        self._dirty = False
        self._unsynced_records = 0
        self._unsynced_tx = TxTally()
        self._stall_written = 0
        self._stall_written_tx = 0
        self._pending_internal: list[tuple[str, dict[str, Any]]] = []
        self._rotation_retry_at = float("-inf")
        self._rotation_blocked_reported = False
        self._reserve_retry_at = float("-inf")
        self._reserve_problem = ""
        self._unlocked_reported = False
        self._last_reserve_problem_emit = ""
        self._closed = False
        self._attempts = 0
        self._stall_start_ns = 0
        self.config_failure: OSError | None = None  # a lazy first open found the location wrong
        self.final_name: str | None = None  # the file the session ended in (its name after close)
        # (device, inode) of that file as it was closed; ``None`` when it could not be read
        self.final_identity: tuple[int, int] | None = None

    # -- shared reads -----------------------------------------------------------------

    @property
    def degraded(self) -> bool:
        """Event and evidence records are counted only while this holds."""
        return self.stalled or self.in_reserve

    @property
    def active_number(self) -> int:
        return self._active.number if self._active is not None else 1

    @property
    def active_name(self) -> str:
        return self._active.name if self._active is not None else self.part1_name()

    def part1_name(self) -> str:
        return part_name(self._stamp, self.session_id, 1)

    def hung(self, now: float) -> bool:
        started = self.call_started
        return started is not None and now - started > self.cfg.hang_s

    # -- plumbing -------------------------------------------------------------------------

    def _emit(self, kind: str, **data: Any) -> None:
        self.events.append((kind, data))

    def _call(self, func: Callable[..., Any], *args: Any) -> Any:
        """Run one file system call, noting when it started so a hang is visible to the loop."""
        started = self.clock()
        self.call_started = started
        try:
            return func(*args)
        finally:
            self.call_started = None
            elapsed = self.clock() - started
            if elapsed > self.cfg.hang_s:
                self._emit("slow_call", seconds=elapsed)

    def _line(
        self, kind: str, event: str, fields: Mapping[str, Any], n: int | None = None
    ) -> bytes:
        number = self._next_number() if n is None else n
        return self._fmt(self.session_id, number, time.time_ns(), kind, event, fields)

    def _lock(self, f: _File, what: str) -> None:
        """Take the file's exclusive lock (opening rule): retried for 1 s on ``EWOULDBLOCK``;
        unsupported or held past a second, the session runs unlocked and says so once."""
        error: OSError | None = None
        for attempt in range(LOCK_TRIES):
            try:
                self.fs.flock(f.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                f.locked = True
                return
            except OSError as exc:
                error = exc
                if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN) and attempt + 1 < LOCK_TRIES:
                    self._sleep(LOCK_STEP_S)
                    continue
                break
        if not self._unlocked_reported:
            self._unlocked_reported = True
            self._emit(
                "no_lock",
                what=what,
                errno=error.errno if error else None,
                text=(error.strerror if error else "") or "lock refused",
            )

    # -- opening ---------------------------------------------------------------------------

    def open(self) -> None:
        """Run every open step from ``mkdir``. Raises :class:`ConfigFailure` when the location
        is wrong (first attempt only: afterwards the location has proven usable or the command
        is already running, so every errno is transient); a transient failure stalls the store
        and returns."""
        if self.opened or self._closed:
            return
        first = self._attempts == 0
        self._attempts += 1
        self._last_attempt = self.clock()
        try:
            self._open_steps()
        except (OSError, _Stall) as exc:
            cause = exc.cause if isinstance(exc, _Stall) else exc
            self._cleanup_failed_open()
            if first and cause is not None and classify_open_errno(cause):
                raise ConfigFailure(cause) from exc
            self._enter_stall(exc if isinstance(exc, _Stall) else _Stall(exc))

    def _cleanup_failed_open(self) -> None:
        """Drop the directory descriptor; part 1, if it was created, stays open for the retry
        (which repeats every step from ``mkdir``, then checks the name still denotes it)."""
        close_quietly(self.fs, self._dir_fd)
        self._dir_fd = -1
        self._active = None

    def _open_steps(self) -> None:
        fs, cfg = self.fs, self.cfg
        self._call(fs.mkdir_p, cfg.directory, 0o700)
        self._dir_fd = self._call(fs.open_dir, cfg.directory)
        part = self._part1
        if part is not None:
            named = try_stat_at(fs, self._dir_fd, part.name)
            if named is None or not same_file(fs.fstat(part.fd), named):
                close_quietly(fs, part.fd)  # the directory was removed or replaced meanwhile
                part = self._part1 = None
        if part is None:
            fd = -1
            for _ in range(OPEN_TRIES):
                name = part_name(self._stamp, self.session_id, 1)
                try:
                    fd = self._call(fs.open_at, self._dir_fd, name, _PART_FLAGS, 0o600)
                    break
                except FileExistsError:
                    self.session_id = uuid.uuid4().hex[:12]
            if fd < 0:
                raise OSError(errno.EEXIST, "no free ledger name", str(cfg.directory))
            part = _File(fd, part_name(self._stamp, self.session_id, 1), 0, 0, number=1)
            self._part1 = part
        self._active = part
        if not part.locked:
            self._lock(part, "ledger")
        host = os.uname().nodename
        first = self._line(
            "state",
            "ledger.open",
            {
                "class": "long" if cfg.long_session else "short",
                "command": cfg.command,
                "version": cfg.version,
                "pid": os.getpid(),
                "host": host,
            },
            n=0,
        )
        self._append(part, first)
        self.opened = True
        part.header_len = part.size

    # -- low-level writes ------------------------------------------------------------------

    def _append(self, f: _File, data: bytes) -> None:
        """Write whole ``data`` to an ``O_APPEND`` part; on failure cut the part back."""
        try:
            self._call(write_all, self.fs, f.fd, data)
        except OSError as exc:
            self._cut_back(f)
            raise _Stall(exc) from exc
        f.size += len(data)
        self._touch()

    def _cut_back(self, f: _File) -> None:
        """Shrink the file to its last complete line (shrinking needs no space)."""
        with contextlib.suppress(OSError):
            self._call(self.fs.ftruncate, f.fd, f.size)

    def _touch(self) -> None:
        self._dirty = True
        self._last_activity = self.clock()

    # -- accounting ------------------------------------------------------------------------

    def _written(self, lines: list[Line]) -> None:
        """Account for ``lines``, which reached the file: they are resolved (in order, so the
        highest number stands for all of them) and, until the next sync, unconfirmed."""
        if not lines:
            return
        with self._state_lock:
            self.resolved_n = max(self.resolved_n, lines[-1].n)
            for line in lines:
                if line.tx is not None:
                    self._unsynced_tx.add(line.tx)
        for line in lines:
            self.tier_bytes[line.tier] += len(line.data)
            self.tier_records[line.tier] += 1
            self._unsynced_records += 1
        self.confirmed_lines += len(lines)
        if self.stalled:
            self._stall_written += len(lines)
            self._stall_written_tx += sum(line.tx.count for line in lines if line.tx is not None)

    def unconfirmed(self) -> tuple[list[Line], int, TxTally]:
        """What the store cannot vouch for, as one consistent snapshot (callable from any
        thread): the lines handed to it that are not written yet (in order: those being written,
        then the backlog), the highest record number written, and the transmissions written
        since the last good sync."""
        with self._state_lock:
            resolved = self.resolved_n
            lines = [
                line for line in itertools.chain(self.inflight, self.backlog) if line.n > resolved
            ]
            unsynced = TxTally()
            unsynced.merge(self._unsynced_tx)
        return lines, resolved, unsynced

    def internal_failure(self, exc: BaseException) -> None:
        """A bug, not an I/O error, interrupted a job (writer thread). What was being written
        goes back to the front of the backlog and the part is cut back to its last complete
        line, so no line is lost or torn; the store stalls (the lines are retried, and counted
        at exit if never written) and the failure is reported, never just noted."""
        with self._state_lock:
            if self.inflight:
                self.backlog.extendleft(reversed(self.inflight))
                self.inflight = []
        active = self._active
        if active is not None and not active.is_reserve and active.fd >= 0:
            self._cut_back(active)
        self._enter_stall(_Stall(None, f"internal error {type(exc).__name__}", internal=True))
        self._emit("internal", error=repr(exc))

    # -- the write path ----------------------------------------------------------------------

    def write(self, lines: list[Line], *, force: bool = False) -> None:
        """Take ``lines`` (in order) and write what can be written now. Never raises: a
        failure stalls the store and the lines wait in :attr:`backlog`."""
        with self._state_lock:
            self.backlog.extend(lines)
        self._advance(force)

    def _advance(self, force: bool) -> None:
        if self._closed:
            return
        now = self.clock()
        if self.stalled and not force and now - self._last_attempt < self.cfg.retry_s:
            return
        if self.stalled:
            self._last_attempt = now
        try:
            if not self.opened:
                self.open()
                if not self.opened:
                    return
            active = self._active
            if self.stalled and active is not None and active.is_reserve:
                self._leave_reserve()  # space may have returned
            self._drain_backlog()
            if self.stalled:
                self._resume()
        except _Stall as stall:
            self._enter_stall(stall)
        except ConfigFailure as failure:  # only a lazy first open: the ledger has no location
            self.config_failure = failure.cause
            with self._state_lock:
                self.backlog.clear()
                self.inflight = []
            self._closed = True

    def _drain_backlog(self) -> None:
        backlog = self.backlog
        while backlog:
            active = self._active
            assert active is not None
            if active.is_reserve:
                self._drain_into_reserve(active)
                return
            try:
                self._drain_into_part(active)
            except _Stall as stall:
                if stall.errno in FULL_ERRNOS and self._activate_reserve(stall):
                    continue
                raise

    def _rotation_due(self, part: _File, first: Line) -> bool:
        return (
            part.size > part.header_len
            and part.size + len(first.data) > self.cfg.max_file_bytes
            and self.clock() >= self._rotation_retry_at
        )

    def _drain_into_part(self, part: _File) -> None:
        backlog, limit = self.backlog, self.cfg.max_file_bytes
        while backlog:
            if self._rotation_due(part, backlog[0]):
                part = self._rotate(part)
                if part.is_reserve:
                    return
            taken: list[Line] = []
            total = 0
            with self._state_lock:
                while backlog and len(taken) < MAX_RUN:
                    size = len(backlog[0].data)
                    over = part.size + total + size > limit
                    if taken and over and self.clock() >= self._rotation_retry_at:
                        break
                    taken.append(backlog.popleft())
                    total += size
                self.inflight = taken
            data = b"".join(line.data for line in taken)
            try:
                self._call(write_all, self.fs, part.fd, data)
            except OSError as exc:
                done = getattr(exc, "written", 0)
                complete, used = 0, 0
                for line in taken:
                    if used + len(line.data) > done:
                        break
                    used += len(line.data)
                    complete += 1
                part.size += used
                self._written(taken[:complete])
                with self._state_lock:
                    backlog.extendleft(reversed(taken[complete:]))
                    self.inflight = []
                self._cut_back(part)
                self._touch()
                raise _Stall(exc) from exc
            part.size += total
            self._written(taken)
            self.inflight = []
            self._touch()

    def _drain_into_reserve(self, reserve: _File) -> None:
        """Essential lines go into the reserve in place (``pwrite``); the others are counted."""
        backlog = self.backlog
        while backlog:
            line = backlog[0]
            if line.tier != Tier.ESSENTIAL and self.in_reserve:
                with self._state_lock:
                    backlog.popleft()
                    self.resolved_n = max(self.resolved_n, line.n)
                self._emit("dropped", tier=int(line.tier), records=1, bytes=len(line.data))
                continue
            if reserve.size + len(line.data) > reserve.extent:
                raise _Stall(None, "the essential reserve is used up")
            try:
                self._call(self._pwrite_all, reserve, line.data)
            except OSError as exc:
                raise _Stall(exc) from exc
            reserve.size += len(line.data)
            self._written([line])  # resolved before it leaves the backlog: never in neither
            with self._state_lock:
                backlog.popleft()
            self._touch()

    def _pwrite_all(self, f: _File, data: bytes) -> None:
        view, done = memoryview(data), 0
        while done < len(view):
            count = self.fs.pwrite(f.fd, view[done:], f.size + done)
            if count <= 0:
                raise OSError(errno.EIO, "pwrite made no progress")
            done += count

    # -- stall and resume --------------------------------------------------------------------

    def _enter_stall(self, stall: _Stall) -> None:
        first = not self.stalled
        self.stalled = True
        self.stall_text = stall.text
        self.stall_errno = stall.errno
        self.stall_internal = stall.internal
        self._last_attempt = self.clock()
        if first:
            self.stall_start = self.clock()
            self._stall_start_ns = time.time_ns()
            self._stall_written = 0
            self._stall_written_tx = 0
            self._emit("stall", text=stall.text, errno=stall.errno)

    def _resume(self) -> None:
        """A retry wrote everything waiting: ``ledger.resumed`` ends the episode."""
        info = dict(self._resume_info())
        duration = self.clock() - self.stall_start
        delayed = self._stall_written + int(info.get("queued", 0))
        fields = {
            "stall_start": self._stall_start_ns,
            "duration_s": round(duration, 3),
            "delayed": delayed,
            "lost": int(info.get("lost", 0)),
            "possibly_lost": self.possibly_lost_records,
            "unlogged_sent": self._stall_written_tx
            + int(info.get("queued_tx", 0))
            + self.possibly_lost_tx.count,
            "cause": self.stall_text,
        }
        data = self._line("state", "ledger.resumed", fields)
        active = self._active
        assert active is not None
        if active.is_reserve:
            if active.size + len(data) > active.extent:
                raise _Stall(None, "the essential reserve is used up")
            try:
                self._call(self._pwrite_all, active, data)
            except OSError as exc:
                raise _Stall(exc) from exc
            active.size += len(data)
        else:
            self._append(active, data)
        self.tier_bytes[Tier.ESSENTIAL] += len(data)
        self.tier_records[Tier.ESSENTIAL] += 1
        self._sync_now()
        self.stalled = False
        self._emit(
            "resumed", duration=duration, **{k: v for k, v in fields.items() if k != "cause"}
        )

    # -- sync and heartbeat --------------------------------------------------------------------

    def _sync_now(self) -> None:
        active = self._active
        if active is None or not self._dirty:
            return
        try:
            self._call(self.fs.fdatasync, active.fd)
        except Exception as exc:  # an OSError, or a bug: either way what was written is unconfirmed
            with self._state_lock:
                self.possibly_lost_tx.merge(self._unsynced_tx)
                self._unsynced_tx = TxTally()
            self.possibly_lost_records += self._unsynced_records
            self._unsynced_records = 0
            self._dirty = False
            if isinstance(exc, OSError):
                self._emit("sync_error", errno=exc.errno, text=exc.strerror or str(exc))
                raise _Stall(exc) from exc
            self._emit("sync_error", errno=None, text=f"internal error {type(exc).__name__}")
            self._emit("internal", error=repr(exc))
            raise _Stall(None, f"internal error {type(exc).__name__}", internal=True) from exc
        with self._state_lock:
            self._unsynced_tx = TxTally()
        self._unsynced_records = 0
        self._dirty = False
        self._last_sync = self.clock()

    def sync_for_exit(self) -> None:
        """Sync what was written, so a deferred error surfaces now (never raises)."""
        if self.opened and not self._closed:
            try:
                self._sync_now()
            except _Stall as stall:
                self._enter_stall(stall)

    def tick(self) -> None:
        """Periodic upkeep: retry a stall, sync (at most once a second), heartbeat, reserve."""
        if self._closed or (not self.opened and not self.stalled):
            return
        now = self.clock()
        try:
            if self.stalled and now - self._last_attempt >= self.cfg.retry_s:
                self._advance(force=True)
            if not self.stalled:
                if self._dirty and now - self._last_sync >= self.cfg.sync_s:
                    self._sync_now()
                elif not self._dirty and now - self._last_activity >= self.cfg.heartbeat_s:
                    self._heartbeat()
                self._reserve_upkeep(now)
        except _Stall as stall:
            self._enter_stall(stall)

    def _heartbeat(self) -> None:
        active = self._active
        self._last_activity = self.clock()
        if active is not None:
            # a failure here is not news: the next write or sync finds out if the file is unusable
            with contextlib.suppress(OSError):
                self._call(self.fs.utime, active.fd)

    # -- the essential reserve --------------------------------------------------------------

    def _reserve_upkeep(self, now: float) -> None:
        if not self.cfg.long_session or self.cfg.reserve_bytes <= 0 or now < self._reserve_retry_at:
            return
        if self.in_reserve:
            self._reserve_retry_at = now + self.cfg.minute_s
            self._leave_reserve()
        elif self._standby is None or not self._standby.allocated:
            self._reserve_retry_at = now + self.cfg.minute_s
            self._allocate_reserve()

    def ensure_reserve(self) -> None:
        """Long sessions allocate their reserve right after part 1 (never raises)."""
        if self.cfg.long_session and self.cfg.reserve_bytes > 0 and self.opened:
            self._reserve_retry_at = self.clock() + self.cfg.minute_s
            self._allocate_reserve()

    def _problem(self, text: str) -> None:
        self._reserve_problem = text
        if text != self._last_reserve_problem_emit:
            self._last_reserve_problem_emit = text
            self._emit("reserve_problem", text=text)

    def _allocate_reserve(self) -> bool:
        """Create (or complete the allocation of) the standby reserve; ``True`` when one is
        really allocated. The file system type is checked first; outside the allowlist no file
        is created."""
        fs = self.fs
        standby = self._standby
        if standby is not None and standby.allocated:
            return True
        try:
            f_type = self._call(fs.fstatfs_type, self._dir_fd)
        except OSError as exc:
            self._problem(f"the file system type could not be determined ({exc.strerror})")
            return False
        if f_type not in RESERVE_FS_ALLOWLIST:
            self._problem(
                f"file system type {f_type:#x} does not guarantee preallocated space "
                "(ext4, XFS and tmpfs do)"
            )
            return False
        if standby is None:
            standby = self._create_reserve_file()
            if standby is None:
                return False
        header_ok = self._reserve_blocks(standby)
        if header_ok:
            self._standby = standby
            self._last_reserve_problem_emit = ""
            self._reserve_problem = ""
            self._emit("reserve_ready", name=standby.name, bytes=standby.extent)
        else:
            self._standby = standby  # kept at its header; allocation is retried every minute
        return header_ok

    def _create_reserve_file(self) -> _File | None:
        fs = self.fs
        for _ in range(NAME_TRIES):
            number = self._highest_reserve + 1
            if number > MAX_PART_NUMBER:
                break
            self._highest_reserve = number
            name = reserve_name(self._stamp, self.session_id, number)
            try:
                fd = self._call(fs.open_at, self._dir_fd, name, _RESERVE_FLAGS, 0o600)
            except FileExistsError:
                self._name_taken(name)
                continue
            except OSError as exc:
                self._problem(f"the reserve file could not be created ({exc.strerror})")
                return None
            reserve = _File(fd, name, 0, 0, is_reserve=True, extent=self.cfg.reserve_bytes)
            self._lock(reserve, "reserve")
            header = self._line(
                "state",
                "ledger.reserve",
                {"reserve": name, "reserve_bytes": self.cfg.reserve_bytes},
            )
            try:
                self._call(self._pwrite_all, reserve, header)
            except OSError as exc:
                self._problem(f"the reserve header could not be written ({exc.strerror})")
                close_quietly(
                    fs, fd
                )  # an empty file of ours stays; the next scan sees a header-less file
                return None
            reserve.size = reserve.header_len = len(header)
            self._flush_taken()
            return reserve
        self._flush_taken()
        self._problem("no free reserve name (the next 16 are taken)")
        return None

    def _reserve_blocks(self, reserve: _File) -> bool:
        """``posix_fallocate`` the extent and verify the blocks are really there."""
        fs = self.fs
        try:
            self._call(fs.posix_fallocate, reserve.fd, 0, reserve.extent)
            blocks = self._call(fs.fstat, reserve.fd).st_blocks * 512
            if blocks < reserve.extent:
                raise OSError(errno.ENOSPC, "the allocation is short")
        except OSError as exc:
            with contextlib.suppress(OSError):  # shrinking needs no space
                self._call(fs.ftruncate, reserve.fd, reserve.header_len)
            self._problem(f"the reserve could not be allocated ({exc.strerror or exc})")
            reserve.allocated = False
            return False
        reserve.allocated = True
        return True

    def _name_taken(self, name: str) -> None:
        found = try_stat_at(self.fs, self._dir_fd, name)
        fields = {
            "name": name,
            "device": found.st_dev if found else None,
            "inode": found.st_ino if found else None,
        }
        self._pending_internal.append(("ledger.name_taken", fields))

    def _flush_taken(self) -> None:
        """Ledger the names found taken while allocating a reserve, into the active part."""
        active = self._active
        if active is not None and not active.is_reserve and self._pending_internal:
            self._flush_internal(active)

    def _flush_internal(self, f: _File) -> None:
        """Write the ``ledger.name_taken`` records found while choosing a name."""
        pending, self._pending_internal = self._pending_internal, []
        for event, fields in pending:
            data = self._line("state", event, fields)
            try:
                self._write_record(f, data)
            except _Stall:
                return

    def _write_record(self, f: _File, data: bytes) -> None:
        if f.is_reserve:
            if f.size + len(data) > f.extent:
                raise _Stall(None, "the essential reserve is used up")
            try:
                self._call(self._pwrite_all, f, data)
            except OSError as exc:
                raise _Stall(exc) from exc
            f.size += len(data)
        else:
            self._append(f, data)
        self.tier_bytes[Tier.ESSENTIAL] += len(data)
        self.tier_records[Tier.ESSENTIAL] += 1
        self._touch()

    def _activate_reserve(self, stall: _Stall, *, reason: str | None = None) -> bool:
        """Make the standby reserve the active part (the disk filled up, or the session ends).

        The interrupted essential lines then go into it in place. Returns ``False`` when there
        is no allocated reserve (the caller stalls).
        """
        reserve = self._standby
        previous = self._active
        if reserve is None or not reserve.allocated or previous is None or previous.is_reserve:
            return False
        number = self._claim_part_number()
        if number is None:
            return False
        reserve.number = number
        part_line = self._line(
            "state",
            "ledger.part",
            {
                "previous": previous.name,
                "reason": reason or f"disk full ({stall.text})",
                "file": reserve.name,
                "part": number,
            },
        )
        try:
            self._write_record(reserve, part_line)
        except _Stall:
            return False
        self._standby = None
        self._active = reserve
        self.in_reserve = reason is None
        if reason is None:
            self._emit("disk_full", previous=previous.name, text=stall.text, errno=stall.errno)
            self._reserve_retry_at = self.clock() + self.cfg.minute_s
        self._flush_internal(reserve)
        return True

    def _claim_part_number(self) -> int | None:
        if self._highest_part >= MAX_PART_NUMBER:
            return None
        self._highest_part += 1
        return self._highest_part

    def _leave_reserve(self) -> bool:
        """Space may have returned: allocate the next reserve, retire the one in use and
        continue in a new part. ``False`` when the disk is still full."""
        reserve = self._active
        if reserve is None or not reserve.is_reserve:
            return True
        if (self._standby is None or not self._standby.allocated) and not self._allocate_reserve():
            return False
        retired = self._retire(reserve)
        previous = retired or reserve.name
        self.in_reserve = False
        try:
            self._new_part(previous, "space returned")
        except _Stall:
            # still short of room for a plain file: carry on in the new reserve
            standby = self._standby
            assert standby is not None
            self._active = standby
            self._standby = None
            standby.number = self._claim_part_number() or standby.number
            self.in_reserve = True
            return False
        self._emit("space_returned", previous=previous)
        return True

    def _rename_reserve(self, reserve: _File, name: str) -> None:
        rename_noreplace(self._dir_fd, reserve.name, name, reserve.fd, fs=self.fs)

    def _retire(self, reserve: _File) -> str | None:
        """Cut a reserve to its last record, sync it and rename it to its part name without
        replacing anything. Returns the new name, or ``None`` when it keeps its reserve name."""
        fs = self.fs
        try:
            self._call(fs.ftruncate, reserve.fd, reserve.size)
            self._call(fs.fdatasync, reserve.fd)
        except OSError as exc:
            self._emit("retire_failed", name=reserve.name, text=exc.strerror or str(exc))
            return None
        number = reserve.number or (self._claim_part_number() or 0)
        result: str | None = None
        failure = "no free part name"
        for _ in range(NAME_TRIES):
            if not 1 <= number <= MAX_PART_NUMBER:
                break
            name = part_name(self._stamp, self.session_id, number)
            try:
                self._call(self._rename_reserve, reserve, name)
            except Collision:
                self._name_taken(name)
                number = self._claim_part_number() or 0
                continue
            except SyncAfterCommit as exc:
                self._emit("retire_sync_failed", name=name, text=exc.strerror or str(exc))
                result = name
            except OSError as exc:
                failure = exc.strerror or str(exc)
            else:
                result = name
            break
        if result is None:
            self._emit("retire_failed", name=reserve.name, text=failure)
        if result is not None:
            reserve.name = result
        close_quietly(fs, reserve.fd)
        reserve.fd = -1
        return result

    # -- rotation ---------------------------------------------------------------------------

    def _rotate(self, previous: _File) -> _File:
        """Continue in the next part. On a blocked rotation (16 names taken, numbers
        exhausted) the current part keeps growing and the attempt repeats every minute."""
        now = self.clock()
        self._sync_now()
        for _ in range(NAME_TRIES):
            number = self._claim_part_number()
            if number is None:
                self._block_rotation("part numbers exhausted", now)
                return previous
            name = part_name(self._stamp, self.session_id, number)
            try:
                fd = self._call(self.fs.open_at, self._dir_fd, name, _PART_FLAGS, 0o600)
            except FileExistsError:
                self._name_taken(name)
                continue
            except OSError as exc:
                raise _Stall(exc) from exc
            part = _File(fd, name, 0, 0, number=number)
            self._activate_new_part(part, previous, "rotation")
            return part
        self._block_rotation("the next 16 part names are taken", now)
        return previous

    def _activate_new_part(self, part: _File, previous: _File, reason: str) -> None:
        header = self._line(
            "state",
            "ledger.part",
            {"previous": previous.name, "reason": reason, "file": part.name, "part": part.number},
        )
        try:
            self._append(part, header)
        except _Stall:
            close_quietly(self.fs, part.fd)  # an empty part of ours stays; later numbers skip it
            raise
        part.header_len = part.size
        self._active = part
        self._rotation_blocked_reported = False
        if previous is not self._part1:
            close_quietly(self.fs, previous.fd)
        self._flush_internal(part)

    def _new_part(self, previous_name: str, reason: str) -> None:
        number = self._claim_part_number()
        if number is None:
            raise _Stall(None, "part numbers exhausted")
        for _ in range(NAME_TRIES):
            name = part_name(self._stamp, self.session_id, number)
            try:
                fd = self._call(self.fs.open_at, self._dir_fd, name, _PART_FLAGS, 0o600)
            except FileExistsError:
                self._name_taken(name)
                number = self._claim_part_number() or 0
                if not number:
                    break
                continue
            except OSError as exc:
                raise _Stall(exc) from exc
            part = _File(fd, name, 0, 0, number=number)
            stub = _File(-1, previous_name, 0, 0)
            self._activate_new_part(part, stub, reason)
            return
        raise _Stall(None, "no free part name")

    def _block_rotation(self, reason: str, now: float) -> None:
        self._rotation_retry_at = now + self.cfg.minute_s
        if not self._rotation_blocked_reported:
            self._rotation_blocked_reported = True
            self._emit("rotation_blocked", text=reason)
        active = self._active
        if active is not None:
            self._flush_internal(active)

    # -- closing ----------------------------------------------------------------------------

    def close(self, lines: list[Line]) -> None:
        """The last write attempt: write what is left, sync, and (long sessions) let an unused
        reserve hold the closing records and retire as the final part. Never raises."""
        if self._closed:
            return
        with self._state_lock:
            self.backlog.extend(lines)
        try:
            if not self.opened and not self._closed:
                self.open()
            if self.opened:
                self._close_writes()
        except ConfigFailure:
            pass
        except _Stall as stall:
            self._enter_stall(stall)
        self._finish_files()
        self._closed = True

    def _close_writes(self) -> None:
        try:
            if self.stalled:
                self._advance(force=True)
            else:
                active = self._active
                standby = self._standby
                if (
                    self.cfg.long_session
                    and active is not None
                    and not active.is_reserve
                    and standby is not None
                    and standby.allocated
                ):
                    self._activate_reserve(_Stall(None, "closing"), reason="closing")
                self._drain_backlog()
            self._sync_now()
        except _Stall as stall:
            self._enter_stall(stall)

    def _finish_files(self) -> None:
        fs = self.fs
        active = self._active
        identity: tuple[int, int] | None = None  # taken before a retirement closes the file
        if active is not None and active.fd >= 0:
            try:
                found = fs.fstat(active.fd)
                identity = (found.st_dev, found.st_ino)
            except OSError:
                pass  # unknown: nothing is appended to the file later
        if active is not None and active.is_reserve and not self.stalled:
            self._retire(active)
        elif active is not None and active.is_reserve:
            try:
                self._call(fs.ftruncate, active.fd, active.size)  # complete records only
                self._call(fs.fdatasync, active.fd)
            except OSError:
                pass
        standby = self._standby
        if standby is not None and not self.stalled and standby.fd >= 0:
            self._retire(standby)  # an unused reserve becomes a part, never a leftover
        if active is not None:
            self.final_name = active.name  # after a retirement: the part it became
            self.final_identity = identity
        for f in (self._standby, active, self._part1):
            if f is not None and f.fd >= 0:
                close_quietly(fs, f.fd)
                f.fd = -1
        close_quietly(fs, self._dir_fd)
        self._dir_fd = -1


class LedgerWriter:
    """Runs a :class:`LedgerStore` on one daemon thread, fed with jobs in order."""

    def __init__(self, store: LedgerStore, poll_s: float = POLL_S) -> None:
        self.store = store
        self._poll_s = poll_s
        self._jobs: queue.SimpleQueue[tuple[str, Any]] = queue.SimpleQueue()
        self._thread = threading.Thread(target=self._run, name="ledger-writer", daemon=True)
        self._started = False
        self.open_result: str = ""

    def start(self) -> None:
        if not self._started:
            self._started = True
            # A thread started with the shutdown signals blocked never receives one: they are
            # handled by the main thread, which must see them all.
            previous = signal.pthread_sigmask(signal.SIG_BLOCK, SHUTDOWN_SIGNALS)
            try:
                self._thread.start()
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous)

    @property
    def pending_jobs(self) -> int:
        return self._jobs.qsize()

    def submit(self, kind: str, payload: Any = None) -> None:
        self.start()
        self._jobs.put((kind, payload))

    def barrier(self) -> threading.Event:
        """An event set once every job submitted so far has been handled."""
        done = threading.Event()
        self.submit("barrier", done)
        return done

    def wait(self, timeout: float) -> bool:
        """Wait for every job submitted so far to be handled (``False`` on timeout)."""
        return self.barrier().wait(timeout)

    def _run(self) -> None:
        jobs = self._jobs
        while True:
            try:
                kind, payload = jobs.get(timeout=self._poll_s)
            except queue.Empty:
                kind, payload = "tick", None
            try:
                if self._handle(kind, payload) == "closed":
                    return
                if kind != "tick":
                    self.store.tick()
            except BaseException as exc:  # the thread must survive a bug in a job or a tick
                self._survive(kind, payload, exc)
                if kind == "close":
                    return

    def _handle(self, kind: str, payload: Any) -> str:
        store = self.store
        match kind:
            case "write":
                lines, force = payload
                store.write(lines, force=force)
            case "open":
                done, box = payload
                try:
                    store.open()
                    if store.opened:
                        store.ensure_reserve()
                    box.append(None if store.opened else "stalled")
                except ConfigFailure as failure:
                    box.append(failure)
                done.set()
            case "tick":
                store.tick()
            case "barrier":
                store.tick()
                payload.set()
            case "finish":
                lines, done = payload
                store.write(lines, force=True)
                store.sync_for_exit()
                done.set()
            case "close":
                lines, done = payload
                store.close(lines)
                done.set()
                return "closed"
            case _:
                pass
        return ""

    def _survive(self, kind: str, payload: Any, exc: BaseException) -> None:
        """A bug interrupted a job: the store puts back what it was writing and reports, and
        whoever waits on the job is released (the lines stay unwritten, so they are counted)."""
        try:
            self.store.internal_failure(exc)
        except BaseException as again:  # never let the report of a failure end the thread
            self.store.events.append(("internal", {"error": f"{exc!r}; then {again!r}"}))
        waiter = {"open": 0, "finish": 1, "close": 1}.get(kind)
        if waiter is not None:
            payload[waiter].set()
        elif kind == "barrier":
            payload.set()


def is_regular_owned(st: os.stat_result) -> bool:
    """A regular file owned by this user (what retention and recovery accept)."""
    return stat.S_ISREG(st.st_mode) and st.st_uid == os.getuid()


__all__ = [
    "ConfigFailure",
    "Formatter",
    "LedgerStore",
    "LedgerWriter",
    "Line",
    "StoreConfig",
    "TxInfo",
    "TxTally",
    "classify_open_errno",
    "is_regular_owned",
    "part_name",
    "reserve_name",
    "same_file",
    "stamp_of",
]
