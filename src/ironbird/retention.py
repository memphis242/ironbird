"""Retention: what the state and recording directories hold, which sessions are protected, and
the recovery of the reserve a crashed session left behind.

Nothing here deletes anything. :func:`scan_directory` lists the files that match the anchored
name patterns of ledgers (``<stamp>-<session>[.pN].jsonl``, ``.rN.reserve``) and recordings
(``[.pN].ibrec``), groups them into sessions, and decides which are *protected*; the warnings
(:mod:`ironbird.retservice`) and the confirmed prune (:mod:`ironbird.prune`) are built on it.

The whole module treats the directory as hostile (another user, a crash, a lookalike name): it
opens the directory once and works relative to that descriptor, never follows symbolic links,
opens files ``O_NONBLOCK`` (a FIFO with a matching name cannot block), and counts as ``foreign``
anything that is not a regular file of this user. Every system call goes through
:class:`~ironbird.fsops.FileSystem`.
"""

from __future__ import annotations

import calendar
import contextlib
import errno
import fcntl
import json
import os
import re
import stat
import threading
import time
from bisect import bisect_right
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from ironbird.atomicfile import (
    Collision,
    LeftoverAfterCommit,
    SyncAfterCommit,
    TempOutput,
    rename_noreplace,
)
from ironbird.fsops import (
    INSPECT_FLAGS,
    INSPECT_RW_FLAGS,
    FileSystem,
    close_quietly,
    same_file,
    try_stat_at,
)
from ironbird.ledgerstore import NAME_TRIES, part_name
from ironbird.units import MIB, format_size

MAX_ENTRIES: Final = 100_000
HEAD_BYTES: Final = 4096
MAX_RESERVE_BYTES: Final = 1 << 30
READ_BLOCK: Final = MIB
MAX_CLASS_READS: Final = 10_000  # head reads per scan; a session past it counts as long
MAX_DAMAGED_CHECK_BYTES: Final = 256 * MIB  # bytes read per scan to tell whether a salvage ended
FRESH_S: Final = 600.0  # a session whose newest file is younger than this is live
_N: Final = r"(?:[0-9]{4}|[1-9][0-9]{4,8})"
_STAMP: Final = r"([0-9]{8}T[0-9]{6}Z)-([0-9a-f]{12})"
LEDGER_RE: Final = re.compile(_STAMP + rf"(?:\.p({_N}))?\.jsonl")
RESERVE_RE: Final = re.compile(_STAMP + rf"\.r({_N})\.reserve")
RECORDING_RE: Final = re.compile(_STAMP + rf"(?:\.p({_N}))?\.ibrec")
# A reserve set aside by a salvage, and the dot-temporary file a salvage copies into (the
# temporary name is ``.<part name>.<16 hex>.tmp``, as the temporary-file helper makes it).
DAMAGED_RE: Final = re.compile(_STAMP + rf"\.r({_N})\.reserve\.damaged")
SALVAGE_TMP_RE: Final = re.compile(r"\." + _STAMP + rf"\.p({_N})\.jsonl\.([0-9a-f]{{16}})\.tmp")
LONG_COMMANDS: Final = frozenset({"listen", "events", "respond", "record", "replay"})
NO_LOCK_ERRNOS: Final = frozenset({errno.ENOLCK, errno.EINVAL, errno.EOPNOTSUPP, errno.EBADF})

# What a file of a ledger directory is (``FileEntry.role``).
PART: Final = "part"  # a ledger part or a recording (part 1 has no number in its name)
RESERVE: Final = "reserve"  # ``.rNNNN.reserve``
DAMAGED: Final = "damaged"  # ``.rNNNN.reserve.damaged``, the original of a salvaged reserve
TEMP: Final = "temp"  # a salvage's dot-temporary file; ``number`` is the part it would become

# Protection reasons, in the order they are attributed to a session's bytes.
LIVE: Final = "live"
FUTURE: Final = "future mtime"
INCOMPLETE: Final = "incomplete"
HARDLINKED: Final = "hard-linked"
UNRECOVERABLE: Final = "unrecoverable reserve"
DAMAGED_KEPT: Final = "damaged reserve kept"
UNREADABLE: Final = "unreadable file"
KEEP_RECENT: Final = "keep_recent"
INSIDE_LONG: Final = "inside a long session"
HAS_RECORDING: Final = "recording present"
REASON_GROUPS: Final = {
    INCOMPLETE: "incomplete or hard-linked",
    HARDLINKED: "incomplete or hard-linked",
}


LEDGER_PATTERNS: Final = (LEDGER_RE, RESERVE_RE, DAMAGED_RE, SALVAGE_TMP_RE)
ROLE_OF: Final = {
    LEDGER_RE: PART,
    RESERVE_RE: RESERVE,
    DAMAGED_RE: DAMAGED,
    SALVAGE_TMP_RE: TEMP,
    RECORDING_RE: PART,
}


def is_managed_name(name: str) -> bool:
    """Whether ``name`` is one retention manages: a ledger part, a reserve (or its set-aside
    original), a salvage's temporary file or a recording."""
    return any(
        p.fullmatch(name) for p in (LEDGER_RE, RESERVE_RE, DAMAGED_RE, SALVAGE_TMP_RE, RECORDING_RE)
    )


def _valid_stamp(stamp: str) -> float | None:
    """Seconds since the epoch of a ``YYYYMMDDTHHMMSSZ`` UTC stamp, or ``None`` if it is not a
    valid time."""
    try:
        parsed = time.strptime(stamp, "%Y%m%dT%H%M%SZ")
    except ValueError:
        return None
    if parsed.tm_sec > 59 or parsed.tm_year < 1970:
        return None
    return float(calendar.timegm(parsed))


@dataclass(frozen=True, slots=True)
class FileEntry:
    name: str
    number: int  # part number (1 for the unnumbered first part); a reserve: its rNNNN
    role: str  # PART, RESERVE, DAMAGED or TEMP
    dev: int
    ino: int
    size: int
    mtime: float
    nlink: int

    @property
    def is_part(self) -> bool:
        return self.role == PART

    @property
    def is_reserve(self) -> bool:
        return self.role == RESERVE


@dataclass(slots=True)
class Session:
    id: str
    start: float  # from the stamp in the name
    files: list[FileEntry] = field(default_factory=list)
    klass: str | None = None  # "long" | "short" (read lazily from part 1's head)
    protected: list[str] = field(default_factory=list)

    @property
    def has_part1(self) -> bool:
        return any(f.is_part and f.number == 1 for f in self.files)

    @property
    def newest_mtime(self) -> float:
        return max((f.mtime for f in self.files), default=0.0)

    @property
    def size(self) -> int:
        """Bytes on disk, counted once per (device, inode)."""
        seen: set[tuple[int, int]] = set()
        total = 0
        for f in self.files:
            if (f.dev, f.ino) not in seen:
                seen.add((f.dev, f.ino))
                total += f.size
        return total

    @property
    def parts(self) -> int:
        return sum(1 for f in self.files if f.is_part)

    @property
    def reserves(self) -> list[FileEntry]:
        return sorted((f for f in self.files if f.is_reserve), key=lambda f: f.number)

    @property
    def damaged(self) -> list[FileEntry]:
        """The originals of salvaged reserves (``.damaged``), by reserve number."""
        return sorted((f for f in self.files if f.role == DAMAGED), key=lambda f: f.number)

    @property
    def temps(self) -> list[FileEntry]:
        """The dot-temporary files of salvages that did not finish."""
        return [f for f in self.files if f.role == TEMP]

    @property
    def hard_linked(self) -> bool:
        return any(f.nlink > 1 for f in self.files)

    def part1(self) -> FileEntry | None:
        for f in self.files:
            if f.is_part and f.number == 1:
                return f
        return None

    def reason(self) -> str:
        """The first protection reason (the one that holds this session's bytes)."""
        return self.protected[0] if self.protected else ""


@dataclass(slots=True)
class DirScan:
    path: Path
    kind: str  # "ledger" | "recording"
    exists: bool = False
    sessions: dict[str, Session] = field(default_factory=dict)
    foreign: int = 0
    unparseable: int = 0
    entries: int = 0
    partial: str | None = None  # why the figures are partial (over the entry bound, unreadable)
    unreadable_files: int = 0
    unreadable_why: str = ""
    unsafe: bool = False  # writable by group or others without the sticky bit
    unlocked: bool = False  # a lock probe was unsupported here
    recovered: int = 0
    unrecoverable: set[str] = field(default_factory=set)
    damaged_kept: set[str] = field(default_factory=set)  # saved, original kept as .damaged
    unreadable: set[str] = field(default_factory=set)  # a file that could not be examined
    # .damaged files this scan produced or ended, and salvage temporaries it already reported
    handled: set[str] = field(default_factory=set)
    check_budget: int = field(default_factory=lambda: MAX_DAMAGED_CHECK_BYTES)
    messages: list[str] = field(default_factory=list)  # IB-RET-004 lines, ready to show
    recoveries: list[dict[str, Any]] = field(default_factory=list)  # ledger.recovered
    incompletes: list[dict[str, Any]] = field(default_factory=list)  # ledger.recovery_incomplete

    def carry_to(self, other: DirScan) -> None:
        """Hand what recovery established to a fresh listing of the same directory."""
        other.recovered, other.recoveries, other.incompletes = (
            self.recovered,
            self.recoveries,
            self.incompletes,
        )
        other.unrecoverable, other.messages, other.unlocked = (
            self.unrecoverable,
            self.messages,
            self.unlocked,
        )
        other.damaged_kept, other.handled, other.check_budget = (
            self.damaged_kept,
            self.handled,
            self.check_budget,
        )

    def total_bytes(self) -> int:
        """Bytes of all pattern files, hard links counted once."""
        seen: set[tuple[int, int]] = set()
        total = 0
        for session in self.sessions.values():
            for f in session.files:
                if (f.dev, f.ino) not in seen:
                    seen.add((f.dev, f.ino))
                    total += f.size
        return total

    def ordered(self) -> list[Session]:
        """Sessions, newest start first (ties by id)."""
        return sorted(self.sessions.values(), key=lambda s: (s.start, s.id), reverse=True)


@dataclass(slots=True)
class ScanControl:
    """The two-way signal between a scan's thread and whoever waits for it.

    ``stop`` asks the scan not to begin any further recovery step; ``stepping`` is set while a
    step that must not be cut short is under way (a salvage after its original was set aside),
    so the waiter can give it the time to end instead of exiting mid-copy."""

    stop: threading.Event = field(default_factory=threading.Event)
    stepping: threading.Event = field(default_factory=threading.Event)
    step: dict[str, Any] | None = None  # what the step under way is (read by the waiter)


def directory_unsafe(mode: int) -> bool:
    """Writable by group or others without the sticky bit: another user could swap names
    between a check and an unlink or rename."""
    return bool(mode & (stat.S_IWGRP | stat.S_IWOTH)) and not mode & stat.S_ISVTX


# -- scanning ----


def scan_directory(
    fs: FileSystem,
    path: Path,
    kind: str,
    *,
    entry_limit: int = MAX_ENTRIES,
) -> DirScan:
    """List and group the pattern files of one directory (no protection decisions yet)."""
    assert kind in ("ledger", "recording")
    scan = DirScan(path, kind)
    try:
        dir_fd = fs.open_dir(path)
    except FileNotFoundError:
        return scan  # no directory yet: nothing to retain
    except OSError as exc:
        scan.partial = f"{path} cannot be read ({exc.strerror or exc})"
        return scan
    scan.exists = True
    try:
        try:
            scan.unsafe = directory_unsafe(fs.fstat(dir_fd).st_mode)
        except OSError as exc:
            scan.partial = f"{path} cannot be examined ({exc.strerror or exc})"
            return scan
        try:
            entries = fs.scandir(dir_fd)
        except OSError as exc:
            scan.partial = f"{path} cannot be listed ({exc.strerror or exc})"
            return scan
        patterns = LEDGER_PATTERNS if kind == "ledger" else (RECORDING_RE,)
        for entry in entries:
            scan.entries += 1
            if scan.entries > entry_limit:
                scan.partial = (
                    f"{path} has more than {entry_limit} entries; the figures are partial"
                )
                break
            _take_entry(fs, dir_fd, entry.name, patterns, scan)
    finally:
        close_quietly(fs, dir_fd)
    if scan.unreadable_files and scan.partial is None:
        scan.partial = (
            f"{scan.unreadable_files} file(s) in {path} could not be examined "
            f"({scan.unreadable_why}); the figures are partial and their sessions are protected"
        )
    return scan


def _take_entry(
    fs: FileSystem, dir_fd: int, name: str, patterns: tuple[re.Pattern[str], ...], scan: DirScan
) -> None:
    for pattern in patterns:
        found = pattern.fullmatch(name)
        if found is None:
            continue
        role = ROLE_OF[pattern]
        start = _valid_stamp(found.group(1))
        number_text = found.group(3)
        number = int(number_text) if number_text else 1
        if start is None or (number_text and role in (PART, TEMP) and number < 2):
            scan.unparseable += 1
            return
        session_id = found.group(2)
        try:
            st = _regular_owned_stat(fs, dir_fd, name)
        except OSError as exc:  # the file system failed: not "not ours", so not "foreign"
            scan.unreadable.add(session_id)
            scan.unreadable_files += 1
            scan.unreadable_why = exc.strerror or str(exc)
            return
        if st is None:
            scan.foreign += 1  # a symbolic link, a vanished name, not ours
            return
        session = scan.sessions.get(session_id)
        if session is None:
            session = scan.sessions[session_id] = Session(session_id, start)
        else:
            session.start = min(session.start, start)
        session.files.append(_file_entry(name, number, role, st))
        return


# An open that fails this way says the name is not a file of ours (gone, a symbolic link, a
# directory, someone else's); any other failure says nothing about the file.
_NOT_OURS_ERRNOS: Final = frozenset(
    {errno.ENOENT, errno.ELOOP, errno.ENXIO, errno.EISDIR, errno.ENOTDIR, errno.EACCES, errno.EPERM}
)


def _regular_owned_stat(fs: FileSystem, dir_fd: int, name: str) -> os.stat_result | None:
    """``fstat`` of ``name`` opened the inspection way; ``None`` unless it is a regular file of
    this user (a symbolic link, a FIFO, someone else's file, a vanished name).

    Raises ``OSError`` when the file system failed in a way that says nothing about the file (an
    I/O error, a stale handle, no descriptors left): the caller must not count it as foreign."""
    try:
        fd = fs.open_at(dir_fd, name, INSPECT_FLAGS)
    except OSError as exc:
        if exc.errno in _NOT_OURS_ERRNOS:
            return None
        raise
    try:
        st = fs.fstat(fd)
    finally:
        close_quietly(fs, fd)
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid():
        return None
    return st


def _file_entry(name: str, number: int, role: str, st: os.stat_result) -> FileEntry:
    return FileEntry(name, number, role, st.st_dev, st.st_ino, st.st_size, st.st_mtime, st.st_nlink)


def _relist(fs: FileSystem, dir_fd: int, session: Session) -> list[FileEntry]:
    """The session's pattern files as the directory holds them right now (a scan's listing can
    be stale by the time a recovery holds the session's lock). Raises ``OSError`` when the
    directory or a file cannot be examined."""
    files: list[FileEntry] = []
    for entry in fs.scandir(dir_fd):
        for pattern in LEDGER_PATTERNS:
            found = pattern.fullmatch(entry.name)
            if found is None:
                continue
            if found.group(2) == session.id:
                role = ROLE_OF[pattern]
                number_text = found.group(3)
                number = int(number_text) if number_text else 1
                st = _regular_owned_stat(fs, dir_fd, entry.name)
                if st is not None and not (number_text and role in (PART, TEMP) and number < 2):
                    files.append(_file_entry(entry.name, number, role, st))
            break
    return files


# -- liveness ----


def probe_lock(fs: FileSystem, dir_fd: int, name: str) -> str:
    """Try ``LOCK_EX | LOCK_NB`` on a file and release at once: ``"held"`` (someone holds it),
    ``"free"``, or ``"unknown"`` (locks unsupported, the file unreadable or gone)."""
    try:
        fd = fs.open_at(dir_fd, name, INSPECT_FLAGS)
    except OSError:
        return "unknown"
    try:
        try:
            fs.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                return "held"
            return "unknown"  # ENOLCK, EINVAL, EOPNOTSUPP, EBADF: the probe proves nothing
        with contextlib.suppress(OSError):
            fs.flock(fd, fcntl.LOCK_UN)
        return "free"
    finally:
        close_quietly(fs, fd)


def liveness(
    fs: FileSystem, path: Path, scan: DirScan, now: float, own_session: str | None
) -> dict[str, str]:
    """Why each live session is live: ``"live"`` (a lock the checker cannot take, or a newest
    mtime within 10 minutes) or ``"future mtime"``. The checker's own session is live by
    definition and never probed (it holds the locks itself)."""
    result: dict[str, str] = {}
    try:
        dir_fd = fs.open_dir(path)
    except OSError:
        return result
    try:
        for session in scan.sessions.values():
            if session.id == own_session:
                result[session.id] = LIVE
                continue
            held = False
            for f in session.files:
                if (f.is_reserve or (f.is_part and f.number == 1)) and probe_lock(
                    fs, dir_fd, f.name
                ) == "held":
                    held = True
                    break
            newest = session.newest_mtime
            if newest > now:
                result[session.id] = FUTURE
            elif held or now - newest <= FRESH_S:
                result[session.id] = LIVE
    finally:
        close_quietly(fs, dir_fd)
    return result


# -- classes ----


def _refuse_constant(token: str) -> object:
    raise ValueError(f"non-standard JSON constant {token}")


def parse_head(raw: bytes) -> dict[str, Any] | None:
    """The first line of a ledger part as a strict JSON object (at most 4 KiB), else ``None``."""
    end = raw.find(b"\n")
    if end < 0 or end > HEAD_BYTES:
        return None
    try:
        value = json.loads(raw[:end].decode("ascii"), parse_constant=_refuse_constant)
    except (ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def session_class(fs: FileSystem, dir_fd: int, session: Session, kind: str) -> str:
    """``"long"`` or ``"short"`` from the head of part 1 (``ledger.open``, or a Stage 1 ledger's
    ``cli.invoke``); an unreadable or invalid head counts as long, so it is protected rather than
    spent. Recordings are long."""
    if kind == "recording":
        return "long"
    part1 = session.part1()
    if part1 is None:
        return "long"
    try:
        fd = fs.open_at(dir_fd, part1.name, INSPECT_FLAGS)
    except OSError:
        return "long"
    try:
        head = parse_head(fs.pread(fd, HEAD_BYTES + 1, 0))
    except OSError:
        return "long"
    finally:
        close_quietly(fs, fd)
    if head is None or head.get("session") != session.id:
        return "long"
    event = head.get("event")
    if event == "ledger.open":
        return (
            "long"
            if head.get("class") == "long"
            else "short"
            if head.get("class") == "short"
            else "long"
        )
    if event == "cli.invoke":  # a Stage 1 ledger
        return "long" if head.get("command") in LONG_COMMANDS else "short"
    return "long"


# -- protection ----


class _ClassReader:
    """Reads each session's class at most once, and at most ``budget`` heads per scan. A session
    past the budget, or with no directory to read it from, counts as long: protected rather than
    spent."""

    def __init__(self, fs: FileSystem, dir_fd: int, kind: str, budget: int = MAX_CLASS_READS):
        self._fs, self._dir_fd, self._kind, self._budget = fs, dir_fd, kind, budget

    def __call__(self, session: Session) -> str:
        if session.klass is None:
            if self._kind == "recording" or self._dir_fd < 0 or self._budget <= 0:
                session.klass = "long"
            else:
                self._budget -= 1
                session.klass = session_class(self._fs, self._dir_fd, session, self._kind)
        return session.klass


def _recency(session: Session) -> tuple[float, str]:
    """How recent a session is for ``keep_recent``: its stamp, or its newest file time when that
    is earlier (a stamp from a clock that ran ahead must not outrank the real recent ones)."""
    return min(session.start, session.newest_mtime), session.id


def _merge_spans(spans: list[tuple[float, float]]) -> tuple[list[float], list[float]]:
    """The union of ``(lo, hi)`` intervals as sorted, disjoint ``los`` and ``his``."""
    los: list[float] = []
    his: list[float] = []
    for lo, hi in sorted(spans):
        if his and lo <= his[-1]:
            his[-1] = max(his[-1], hi)
        else:
            los.append(lo)
            his.append(hi)
    return los, his


def _inside(los: list[float], his: list[float], moment: float) -> bool:
    index = bisect_right(los, moment) - 1
    return index >= 0 and moment <= his[index]


def apply_protection(
    fs: FileSystem,
    scan: DirScan,
    *,
    keep_recent: int,
    live: dict[str, str],
    has_recording: Callable[[str], bool] | None = None,
) -> None:
    """Fill ``Session.protected`` for every session of the directory (see the module notes).

    A session is protected when it is live, incomplete, hard-linked, holds an unrecoverable
    reserve, a salvaged reserve's original (``.damaged``) or a file that could not be examined,
    has its recording, is among the ``keep_recent`` most recent (of any class, and of
    the long ones), or is a short session that started inside the span (start to newest file
    time) of any protected long session.
    """
    try:
        dir_fd = fs.open_dir(scan.path)
    except OSError:
        dir_fd = -1
    try:
        class_of = _ClassReader(fs, dir_fd, scan.kind)
        sessions = list(scan.sessions.values())
        for session in sessions:
            session.protected = []
            if session.id in live:
                session.protected.append(live[session.id])
            if not session.has_part1:
                session.protected.append(INCOMPLETE)
            if session.hard_linked:
                session.protected.append(HARDLINKED)
            if session.id in scan.unrecoverable:
                session.protected.append(UNRECOVERABLE)
            if session.id in scan.damaged_kept:
                session.protected.append(DAMAGED_KEPT)
            if session.id in scan.unreadable:
                session.protected.append(UNREADABLE)
            if has_recording is not None and has_recording(session.id):
                session.protected.append(HAS_RECORDING)
        # keep_recent: the most recent of any class, and the most recent long ones. A session
        # with a future mtime is already protected and takes no slot, so a run of files from a
        # clock that was ahead cannot push the real recent sessions out of the kept set.
        eligible = sorted(
            (s for s in sessions if FUTURE not in s.protected), key=_recency, reverse=True
        )
        for session in eligible[:keep_recent]:
            if KEEP_RECENT not in session.protected:
                session.protected.append(KEEP_RECENT)
        long_kept = 0
        for session in eligible:
            if long_kept >= keep_recent:
                break
            if class_of(session) == "long":
                long_kept += 1
                if KEEP_RECENT not in session.protected:
                    session.protected.append(KEEP_RECENT)
        if scan.kind == "recording":
            return  # recordings are all long: no short session to hold back
        # a short session that started while any protected long session ran is kept with it
        spans = [
            (s.start, s.newest_mtime) for s in sessions if s.protected and class_of(s) == "long"
        ]
        if not spans:
            return
        los, his = _merge_spans(spans)
        for session in sessions:
            if (
                not session.protected
                and _inside(los, his, session.start)
                and class_of(session) == "short"
            ):
                session.protected.append(INSIDE_LONG)
    finally:
        close_quietly(fs, dir_fd)


def recording_blocks(
    recordings: DirScan | None, now: float, older_than_s: int | None
) -> Callable[[str], bool] | None:
    """Whether a ledger session is held back by its recording: the recording (same session id)
    is present in the recording directory and not itself proposed, that is, protected or not
    old enough to be selected. Pruning the recording first releases the ledger."""
    if recordings is None:
        return None

    def blocks(session_id: str) -> bool:
        rec = recordings.sessions.get(session_id)
        if rec is None:
            return False
        proposed = (
            not rec.protected and older_than_s is not None and now - rec.newest_mtime > older_than_s
        )
        return not proposed

    return blocks


def protected_bytes_by_reason(
    scan: DirScan, only: Callable[[Session], bool] = lambda _s: True
) -> dict[str, int]:
    """Bytes of protected sessions, attributed to each session's first reason, grouped for
    display (``incomplete or hard-linked`` together)."""
    totals: dict[str, int] = {}
    for session in scan.sessions.values():
        if session.protected and only(session):
            group = REASON_GROUPS.get(session.reason(), session.reason())
            totals[group] = totals.get(group, 0) + session.size
    return totals


def describe_reasons(totals: dict[str, int]) -> str:
    """``live 0.2 GiB, keep_recent 0.8 GiB`` for the protected-bytes line."""
    return ", ".join(f"{name} {format_size(size)}" for name, size in sorted(totals.items()))


# -- recovery of a crashed session's reserve ----


@dataclass(slots=True)
class _Facts:
    size: int
    first_nul: int | None
    last_newline: int  # before the first NUL (or in the whole file); -1 when there is none
    junk_after_nul: int | None  # offset of the first non-NUL byte after the first NUL
    read: int = 0  # bytes read to find these out
    cut_short: bool = False  # the read limit ended the scan of the file before its answer


def _read_facts(fs: FileSystem, fd: int, size: int, limit: int | None = None) -> _Facts:
    """The facts of a reserve's layout, reading at most ``limit`` bytes (``None``: no limit);
    ``cut_short`` says the limit stopped the read before the facts were settled."""
    first_nul: int | None = None
    last_nl = -1
    junk: int | None = None
    offset = 0
    read = 0
    while offset < size:
        want = min(READ_BLOCK, size - offset)
        if limit is not None:
            want = min(want, limit - read)
            if want <= 0:
                return _Facts(size, first_nul, last_nl, junk, read, cut_short=True)
        block = fs.pread(fd, want, offset)
        if not block:
            break
        read += len(block)
        if first_nul is None:
            nul = block.find(b"\x00")
            head = block if nul < 0 else block[:nul]
            nl = head.rfind(b"\n")
            if nl >= 0:
                last_nl = offset + nl
            if nul >= 0:
                first_nul = offset + nul
                rest = block[nul:].lstrip(b"\x00")
                if rest:
                    junk = offset + len(block) - len(rest)
        elif junk is None:
            rest = block.lstrip(b"\x00")
            if rest:
                junk = offset + len(block) - len(rest)
        if junk is not None and first_nul is not None:
            break
        offset += len(block)
    return _Facts(size, first_nul, last_nl, junk, read)


@dataclass(slots=True)
class RecoveryContext:
    fs: FileSystem
    dir_fd: int
    scan: DirScan
    session: Session
    next_part: int = 2
    control: ScanControl | None = None
    on_record: Callable[[str, dict[str, Any]], None] | None = None
    dirty: bool = False  # a file was renamed: the listing is stale

    @property
    def stopping(self) -> bool:
        return self.control is not None and self.control.stop.is_set()

    def report(self, event: str, fields: dict[str, Any]) -> None:
        """Hand a ledger record to whoever wants it, now - not when the scan's result is
        collected, which a process that is already exiting never does."""
        if self.on_record is not None:
            self.on_record(event, dict(fields))


def _unrecoverable(scan: DirScan, session: Session, name: str, why: str) -> None:
    scan.unrecoverable.add(session.id)
    scan.messages.append(
        f"reserve {name} not recovered ({why}); inspect it and move it away, or keep it; "
        "its session is protected"
    )


def recover_directory(
    fs: FileSystem,
    scan: DirScan,
    live: dict[str, str],
    own_session: str | None,
    control: ScanControl | None = None,
    on_record: Callable[[str, dict[str, Any]], None] | None = None,
) -> bool:
    """Recover the leftover reserves of every non-live session of a ledger directory (cut at the
    last complete record and renamed to the next part, never deleted) and finish the salvages
    that were cut short (a ``.damaged`` original whose records are in no part). A directory
    writable by others without the sticky bit is not touched. Returns whether any file changed.

    ``control.stop`` ends the pass between sessions: no recovery step begins after it is set.
    ``on_record(event, fields)`` is called as each step ends, from the calling thread."""
    if scan.kind != "ledger" or scan.unsafe or not scan.exists:
        return False
    try:
        dir_fd = fs.open_dir(scan.path)
    except OSError:
        return False
    changed = False
    try:
        for session in sorted(scan.sessions.values(), key=lambda s: s.id):
            if control is not None and control.stop.is_set():
                break
            if session.id in live or session.id == own_session:
                continue
            if not (session.reserves or session.damaged):
                continue
            ctx = RecoveryContext(fs, dir_fd, scan, session, control=control, on_record=on_record)
            changed |= _recover_session(ctx)
    finally:
        close_quietly(fs, dir_fd)
    return changed


def _recover_session(ctx: RecoveryContext) -> bool:
    """Lock the session and do its recovery steps; ``True`` when a file was renamed."""
    fs, scan, session = ctx.fs, ctx.scan, ctx.session
    part1 = session.part1()
    named = (session.reserves or session.damaged)[0].name
    if part1 is None:
        _unrecoverable(scan, session, named, "its session has no part 1")
        return False
    try:
        fd1 = fs.open_at(ctx.dir_fd, part1.name, INSPECT_FLAGS)
    except OSError as exc:
        _unrecoverable(scan, session, named, f"part 1 cannot be opened: {exc.strerror}")
        return False
    held: list[int] = [fd1]
    changed = False
    try:
        try:
            fs.flock(fd1, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                return False  # a writer, or another scan recovering the session: try next scan
            scan.unlocked = True
            _unrecoverable(scan, session, named, "file locks are unavailable here")
            return False
        # The listing is from before the lock: another scan may have recovered the session
        # meanwhile, so what is left to do is what the directory holds now.
        try:
            session.files = _relist(fs, ctx.dir_fd, session)
        except OSError as exc:
            _unrecoverable(
                scan, session, named, f"the directory cannot be listed: {exc.strerror or exc}"
            )
            return False
        if not (session.reserves or session.damaged):
            return False  # the session was recovered by another scan: nothing left, nothing to say
        named = (session.reserves or session.damaged)[0].name
        if session.part1() is None:
            _unrecoverable(scan, session, named, "its session has no part 1")
            return False
        ctx.next_part = max((f.number for f in session.files if f.is_part), default=1) + 1
        changed = _recover_locked(ctx, held)
    finally:
        for fd in held:
            close_quietly(fs, fd)
    return changed or ctx.dirty


def _recover_locked(ctx: RecoveryContext, held: list[int]) -> bool:
    """The steps for one session whose part 1 this scan has locked: its reserves in order, then
    the salvages that were cut short."""
    scan, session = ctx.scan, ctx.session
    changed = False
    for reserve in session.reserves:
        if ctx.stopping:
            return changed
        try:
            outcome = _recover_reserve(ctx, reserve, held)
        except OSError as exc:  # the file system failed mid-way: nothing more is touched
            _unrecoverable(
                scan, session, reserve.name, f"the file system failed: {exc.strerror or exc}"
            )
            return changed
        if outcome == "skip":
            return changed
        changed |= outcome == "changed"
        if outcome == "stop":
            break
    for entry in session.damaged:
        if ctx.stopping:
            break
        if entry.name in scan.handled:
            continue
        try:
            outcome = _resume_salvage(ctx, entry)
        except OSError as exc:
            _unrecoverable(
                scan, session, entry.name, f"the file system failed: {exc.strerror or exc}"
            )
            break
        changed |= outcome == "changed"
        if outcome == "stop":
            break
    return changed


def _recover_reserve(ctx: RecoveryContext, reserve: FileEntry, held: list[int]) -> str:
    """One reserve: ``"changed"``, ``"unchanged"``, ``"skip"`` (leave the session for the next
    scan) or ``"stop"`` (left untouched and reported)."""
    fs, scan, session, name = ctx.fs, ctx.scan, ctx.session, reserve.name
    try:
        fd = fs.open_at(ctx.dir_fd, name, INSPECT_RW_FLAGS)
    except OSError as exc:
        _unrecoverable(scan, session, name, f"cannot be opened: {exc.strerror}")
        return "stop"
    held.append(fd)
    st = fs.fstat(fd)
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_size > MAX_RESERVE_BYTES:
        _unrecoverable(scan, session, name, "it is not a regular file of yours, or over 1 GiB")
        return "stop"
    try:
        fs.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
            return "skip"
        scan.unlocked = True
        _unrecoverable(scan, session, name, "file locks are unavailable here")
        return "stop"
    first = fs.pread(fd, HEAD_BYTES + 1, 0)
    head = parse_head(first)
    if head is None or head.get("event") != "ledger.reserve":
        _unrecoverable(scan, session, name, "it has no valid ledger.reserve header")
        return "stop"
    if head.get("session") != session.id:
        _unrecoverable(scan, session, name, "header is another session's")
        return "stop"
    header_len = first.find(b"\n") + 1
    completed = _complete_interrupted_rename(ctx, reserve, fd, st)
    if completed is not None:
        return completed
    facts = _read_facts(fs, fd, st.st_size)
    cut = facts.last_newline + 1
    if facts.junk_after_nul is None:
        if cut < header_len:
            _unrecoverable(scan, session, name, "no complete record after its header")
            return "stop"
        return _commit_recovered(ctx, reserve, fd, st, cut)
    if cut > header_len:
        return _salvage(ctx, reserve, fd, st, cut)
    _unrecoverable(scan, session, name, "damaged, with no record after its header")
    return "stop"


def _complete_interrupted_rename(
    ctx: RecoveryContext, reserve: FileEntry, fd: int, st: os.stat_result
) -> str | None:
    """A reserve whose inode also carries a part name of the session, or its own ``.damaged``
    name, is an interrupted rename (the ``link`` path of the helper, cut short or with an unlink
    that failed): the reserve name is unlinked, leaving the other name. A completed set-aside is
    recorded with ``new`` ``None`` and the ``.damaged`` name, and is not a recovery: its records
    are saved, or not yet, by the salvage of that file.

    Both names are looked at again now, and the inode must really have two links, so a stale
    listing cannot make this unlink a name that is not a second name of the file."""
    fs, scan, session, name = ctx.fs, ctx.scan, ctx.session, reserve.name
    for other in session.files:
        set_aside = other.role == DAMAGED and other.name == f"{name}.damaged"
        if not (other.is_part or set_aside) or (other.dev, other.ino) != (st.st_dev, st.st_ino):
            continue
        held = fs.fstat(fd)
        current = try_stat_at(fs, ctx.dir_fd, other.name)
        named = try_stat_at(fs, ctx.dir_fd, name)
        if (
            current is None
            or named is None
            or not same_file(held, current)
            or not same_file(held, named)
            or held.st_nlink < 2
        ):
            continue
        fs.unlink_at(ctx.dir_fd, name)  # completes the rename: one name remains
        if set_aside:
            ctx.dirty = True
            scan.recoveries.append(
                {"old": name, "new": None, "kept": 0, "cut": st.st_size, "damaged": other.name}
            )
            return "changed"
        scan.recovered += 1
        scan.recoveries.append({"old": name, "new": other.name, "kept": st.st_size, "cut": 0})
        return "changed"
    return None


def _commit_recovered(
    ctx: RecoveryContext, reserve: FileEntry, fd: int, st: os.stat_result, cut: int
) -> str:
    fs, scan, session, name = ctx.fs, ctx.scan, ctx.session, reserve.name
    new = part_name(_stamp_of(session), session.id, ctx.next_part)
    record: dict[str, Any] = {"old": name, "new": new, "kept": cut, "cut": st.st_size - cut}
    try:
        fs.ftruncate(fd, cut)
        fs.fdatasync(fd)
        rename_noreplace(ctx.dir_fd, name, new, fd, fs=fs)
    except Collision:
        _unrecoverable(scan, session, name, f"a file already has the next part name {new}")
        return "stop"
    except LeftoverAfterCommit as exc:
        # renamed; the reserve name stays a second name of the part until the next scan
        # completes the rename (_complete_interrupted_rename)
        record.update(_leftover_fields(exc))
    except SyncAfterCommit as exc:
        record["sync_error"] = exc.strerror  # renamed; the rename is not undone
    except OSError as exc:
        _unrecoverable(scan, session, name, f"it could not be renamed: {exc.strerror}")
        return "stop"
    scan.recovered += 1
    scan.recoveries.append(record)
    ctx.next_part += 1
    return "changed"


def _salvage(
    ctx: RecoveryContext, reserve: FileEntry, fd: int, st: os.stat_result, cut: int
) -> str:
    """A valid prefix followed by damage.

    The damaged original is set aside first, byte-identical, as ``<name>.damaged`` (one atomic
    non-replacing rename): that name is outside the reserve pattern, so no later scan treats it as
    a reserve again, whatever happens next. Then the prefix's complete lines are copied from the
    held descriptor to a temporary file and committed to the session's next part name; a crash or
    a full disk leaves only a dot-temporary file and the intact ``.damaged`` file, never a torn or
    a duplicate part, and a later long session or prune finishes the copy
    (:func:`_resume_salvage`)."""
    fs, scan, session, name = ctx.fs, ctx.scan, ctx.session, reserve.name
    damaged = f"{name}.damaged"
    if ctx.stopping:
        return "stop"  # nothing is committed to yet: the next scan salvages it
    set_aside: dict[str, Any] = {}
    try:
        rename_noreplace(ctx.dir_fd, name, damaged, fd, fs=fs)
    except LeftoverAfterCommit as exc:
        # set aside; the reserve name stays a second name of the .damaged file until the next
        # scan completes the rename (_complete_interrupted_rename)
        set_aside = _leftover_fields(exc)
    except SyncAfterCommit as exc:
        set_aside = {"sync_error": exc.strerror}
    except Collision:
        _unrecoverable(scan, session, name, f"a file named {damaged} already exists")
        return "stop"
    except OSError as exc:
        _unrecoverable(
            scan, session, name, f"it could not be set aside as .damaged: {exc.strerror}"
        )
        return "stop"
    ctx.dirty = True
    return _finish_salvage(ctx, fd, name, damaged, st.st_size, cut, set_aside)


def _leftover_fields(exc: LeftoverAfterCommit) -> dict[str, Any]:
    """The record fields of a rename that was committed but left its old name behind."""
    fields: dict[str, Any] = {"leftover": exc.leftover, "leftover_error": exc.strerror}
    if exc.sync_error is not None:
        fields["sync_error"] = exc.sync_error
    return fields


def _finish_salvage(
    ctx: RecoveryContext,
    fd: int,
    name: str,
    damaged: str,
    size: int,
    cut: int,
    set_aside: dict[str, Any] | None = None,
) -> str:
    """Copy the first ``cut`` bytes of the set-aside original (``fd``) into the session's next
    part, and say how it ended: ``ledger.recovered`` when the part is committed,
    ``ledger.recovery_incomplete`` when it is not. Either record is handed out here, by whoever
    ends the step, so a scan that outlives the process's exit wait still leaves its trace.
    ``set_aside``: fields of a set-aside rename that committed with a leftover or an unsynced
    directory, added to the record."""
    scan, session = ctx.scan, ctx.session
    scan.handled.add(damaged)
    control = ctx.control
    if control is not None:
        control.step = {
            "old": name,
            "damaged": damaged,
            "cut": size,
            "kept": 0,
        }  # cut = size - kept
        control.stepping.set()  # from here the copy runs to its end
    try:
        new, error, copied = _copy_prefix(ctx, fd, cut)
    finally:
        if control is not None:
            control.stepping.clear()
            control.step = None
    record: dict[str, Any] = {
        "old": name,
        "new": new,
        "kept": cut if new else 0,
        "cut": size - (cut if new else 0),
        "damaged": damaged,
        **(set_aside or {}),
    }
    if copied:
        record["copy"] = copied  # its commit left the temporary name, or the sync failed
    if new is None:
        record["error"] = error
        scan.incompletes.append(record)
        ctx.report("ledger.recovery_incomplete", record)
        scan.unrecoverable.add(session.id)
        scan.messages.append(
            f"reserve {name} was damaged and its records could not be saved ({error}); "
            f"the whole file is kept as {damaged}; the next long session or 'ib ledger prune' "
            "tries again, or inspect that file"
        )
        return "stop"
    scan.recovered += 1
    scan.recoveries.append(record)
    ctx.report("ledger.recovered", record)
    scan.damaged_kept.add(session.id)
    message = (
        f"reserve {name} was damaged: {format_size(cut)} saved as part "
        f"p{ctx.next_part:04d}, the rest kept as {damaged}; inspect or delete that file"
    )
    if "leftover" in copied:
        scan.handled.add(copied["leftover"])  # said here; the accounting pass need not repeat it
        message += (
            f"; the temporary file {copied['leftover']} of the copy could not be removed "
            f"({copied['leftover_error']}); delete it"
        )
    scan.messages.append(message)
    ctx.next_part += 1
    return "changed"


@dataclass(slots=True)
class _Check:
    """What a ``.damaged`` file turned out to be: ``saved`` (its records are in a part, or it
    holds none), ``pending`` (records only in it), or ``unverified`` (``why``)."""

    state: str = "unverified"
    why: str = ""
    cut: int = 0
    size: int = 0
    part: str = ""
    fd: int = -1  # open on the file when pending and the caller asked to keep it


def _check_damaged(
    fs: FileSystem, dir_fd: int, scan: DirScan, session: Session, entry: FileEntry, *, keep: bool
) -> _Check:
    """Decide whether the records of a ``.damaged`` original are in a part of its session.

    A salvage commits its copy by one rename, so a part exists only when the copy is whole: the
    file is ``saved`` when some part of the session has exactly the bytes of its prefix (up to
    the last complete line before the damage). Reads are charged to the scan's budget before they
    are made. Only a comparison that ran to its end and found no part makes a file ``pending``;
    a budget that ran out, a candidate that could not be read and too many candidates leave it
    ``unverified``, because copying then could write a second part of records already saved."""
    check = _Check()
    if scan.check_budget <= 0:
        check.why = BUDGET_WHY
        return check
    try:
        fd = fs.open_at(dir_fd, entry.name, INSPECT_FLAGS)
    except OSError as exc:
        check.why = f"cannot be opened: {exc.strerror or exc}"
        return check
    keep_fd = False
    try:
        st = fs.fstat(fd)
        if (
            not stat.S_ISREG(st.st_mode)
            or st.st_uid != os.getuid()
            or st.st_size > MAX_RESERVE_BYTES
        ):
            check.why = "it is not a regular file of yours, or over 1 GiB"
            return check
        first = fs.pread(fd, HEAD_BYTES + 1, 0)
        head = parse_head(first)
        if (
            head is None
            or head.get("event") != "ledger.reserve"
            or head.get("session") != session.id
        ):
            check.why = "it does not start with a ledger.reserve record of its session"
            return check
        header_len = first.find(b"\n") + 1
        facts = _read_facts(fs, fd, st.st_size, scan.check_budget)
        scan.check_budget -= facts.read
        if facts.cut_short:
            check.why = BUDGET_WHY
            return check
        check.size = st.st_size
        check.cut = facts.last_newline + 1
        if check.cut <= header_len:
            check.state = "saved"  # no record after the header: nothing to save
            return check
        part, why = _saved_part(fs, dir_fd, scan, session, fd, check.cut)
        if part:
            check.state, check.part = "saved", part
            return check
        if why:
            check.why = why
            return check
        check.state = "pending"
        keep_fd = keep
        check.fd = fd if keep else -1
        return check
    except OSError as exc:
        check.state, check.why = "unverified", f"cannot be read: {exc.strerror or exc}"
        return check
    finally:
        if not keep_fd:
            close_quietly(fs, fd)


MAX_PART_CANDIDATES: Final = 8  # parts of the right size compared per file
BUDGET_WHY: Final = "too many bytes to check in one scan"


def _saved_part(
    fs: FileSystem, dir_fd: int, scan: DirScan, session: Session, fd: int, cut: int
) -> tuple[str, str]:
    """Look for the part of ``session`` that holds exactly the first ``cut`` bytes of ``fd``.

    Returns ``(part, "")`` when one does, ``("", "")`` when every candidate was compared and none
    matches, and ``("", why)`` when the comparison could not be finished: a candidate was too
    large for the remaining budget, could not be opened or read, or there were more candidates
    than are compared. The budget is charged for a candidate before it is read."""
    candidates = [f for f in session.files if f.is_part and f.number >= 2 and f.size == cut]
    if len(candidates) > MAX_PART_CANDIDATES:
        return "", f"more than {MAX_PART_CANDIDATES} parts have the size of its records"
    for candidate in candidates:
        if cut > scan.check_budget:
            return "", BUDGET_WHY
        scan.check_budget -= cut
        try:
            other = fs.open_at(dir_fd, candidate.name, INSPECT_FLAGS)
        except OSError as exc:
            return "", f"{candidate.name} cannot be opened: {exc.strerror or exc}"
        try:
            if _same_prefix(fs, other, fd, cut):
                return candidate.name, ""
        except OSError as exc:
            return "", f"{candidate.name} cannot be read: {exc.strerror or exc}"
        finally:
            close_quietly(fs, other)
    return "", ""


def _same_prefix(fs: FileSystem, a: int, b: int, length: int) -> bool:
    """Whether the first ``length`` bytes of ``a`` and ``b`` are equal (``OSError`` propagates)."""
    offset = 0
    while offset < length:
        want = min(READ_BLOCK, length - offset)
        left, right = fs.pread(a, want, offset), fs.pread(b, want, offset)
        if not left or left != right:
            return False
        offset += len(left)
    return True


def _resume_salvage(ctx: RecoveryContext, entry: FileEntry) -> str:
    """Finish the copy of a ``.damaged`` original whose records are in no part (a salvage that was
    cut short by the process's exit, a kill, or a failing disk). The original is only read."""
    fs, scan, session = ctx.fs, ctx.scan, ctx.session
    check = _check_damaged(fs, ctx.dir_fd, scan, session, entry, keep=True)
    if check.state == "saved":
        scan.handled.add(entry.name)  # the accounting pass need not read it again
        scan.damaged_kept.add(session.id)
    if check.state != "pending":
        return "unchanged"  # saved already, or not judgeable: the report says which
    try:
        name = entry.name.removesuffix(".damaged")
        return _finish_salvage(ctx, check.fd, name, entry.name, check.size, check.cut)
    finally:
        close_quietly(fs, check.fd)


def classify_damaged(fs: FileSystem, scan: DirScan) -> None:
    """Account for the ``.damaged`` originals and salvage temporaries a listing holds.

    A session with a ``.damaged`` file is protected: ``saved`` ones (their records are in a part)
    keep the file for the user to inspect or delete, with the session; a ``pending`` or
    ``unverified`` one holds records that are in no part, so its session is protected as holding
    an unrecovered reserve, and an ``IB-RET-004`` line names the file and the action - at every
    scan, so the display rate limit (not this function) decides how often it is shown. A
    dot-temporary salvage file with nothing to resume is reported and protects its session too.
    Read-only: finishing a salvage is :func:`recover_directory`'s step."""
    if scan.kind != "ledger" or not scan.exists:
        return
    sessions = sorted(
        (s for s in scan.sessions.values() if s.damaged or s.temps), key=lambda s: s.id
    )
    if not sessions:
        return
    try:
        dir_fd = fs.open_dir(scan.path)
    except OSError as exc:
        for session in sessions:
            scan.unrecoverable.add(session.id)
        scan.messages.append(
            f"{scan.path} cannot be opened to check salvaged reserves ({exc.strerror or exc}); "
            "their sessions are protected"
        )
        return
    try:
        for session in sessions:
            unfinished = False
            for entry in session.damaged:
                if entry.name in scan.handled:
                    continue
                check = _check_damaged(fs, dir_fd, scan, session, entry, keep=False)
                if check.state == "saved":
                    scan.damaged_kept.add(session.id)
                    continue
                unfinished = True
                scan.unrecoverable.add(session.id)
                scan.messages.append(_unfinished_message(entry, check))
            temps = [t for t in session.temps if t.name not in scan.handled]
            if temps and not unfinished:
                scan.unrecoverable.add(session.id)
                first = temps[0]
                scan.messages.append(
                    f"{first.name} ({format_size(first.size)}) is left over from an interrupted "
                    "salvage and nothing is waiting for it; inspect it and delete it; "
                    "its session is protected"
                )
    finally:
        close_quietly(fs, dir_fd)


def _unfinished_message(entry: FileEntry, check: _Check) -> str:
    if check.state == "pending":
        return (
            f"{entry.name} holds {format_size(check.cut)} of records that are in no part (a "
            "salvage was cut short); the next long session or 'ib ledger prune' saves them, or "
            "inspect that file; its session is protected"
        )
    return (
        f"{entry.name} could not be checked ({check.why}); inspect it and keep it or move it "
        "away; its session is protected"
    )


def _copy_prefix(ctx: RecoveryContext, fd: int, cut: int) -> tuple[str | None, str, dict[str, Any]]:
    """Copy the first ``cut`` bytes of ``fd`` to the session's next free part name through a
    temporary file. Returns ``(part name, "", commit)``, or ``(None, why, {})``; ``commit``
    holds the fields of a commit that renamed but left the temporary name behind
    (``leftover``) or could not sync the directory (``sync_error``), else is empty.

    A temporary file an earlier attempt of this session left for that part is continued when its
    content is exactly the start of the source; any other file is left alone."""
    fs, session = ctx.fs, ctx.session
    new = ""
    for _ in range(NAME_TRIES):
        new = part_name(_stamp_of(session), session.id, ctx.next_part)
        if try_stat_at(fs, ctx.dir_fd, new) is None:
            break
        ctx.next_part += 1
    else:
        return None, "no free part name", {}
    reusable = sorted(
        (t for t in session.temps if t.number == ctx.next_part), key=lambda t: -t.size
    )
    resume_at = [0]

    def starts_the_source(tmp_fd: int) -> bool:
        have = fs.fstat(tmp_fd).st_size
        if have > cut or not _same_prefix(fs, tmp_fd, fd, have):
            return False
        resume_at[0] = have
        return True

    commit: dict[str, Any] = {}
    try:
        with TempOutput(
            ctx.scan.path / new,
            fs=fs,
            reuse=[t.name for t in reusable],
            accept=starts_the_source,
        ) as out:
            offset = resume_at[0]
            while offset < cut:
                block = fs.pread(fd, min(READ_BLOCK, cut - offset), offset)
                if not block:
                    raise OSError(errno.EIO, "the reserve ended early")
                out.write(block)
                offset += len(block)
            try:
                out.commit()
            except LeftoverAfterCommit as exc:  # renamed all the same
                commit = _leftover_fields(exc)
            except SyncAfterCommit as exc:  # renamed; the rename is not undone
                commit = {"sync_error": exc.strerror}
    except Collision:
        return None, f"a file appeared at {new}", {}
    except OSError as exc:
        return None, exc.strerror or str(exc), {}
    return new, "", commit


def _stamp_of(session: Session) -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(session.start))


def full_scan(
    fs: FileSystem,
    path: Path,
    kind: str,
    *,
    now: float,
    own_session: str | None,
    keep_recent: int,
    has_recording: Callable[[str], bool] | None = None,
    recover: bool = True,
    control: ScanControl | None = None,
    on_record: Callable[[str, dict[str, Any]], None] | None = None,
) -> DirScan:
    """Scan, probe liveness, recover leftover reserves (a ledger directory; ``recover=False``
    leaves them for a scan that may), account for the salvaged originals, then decide the
    protections - the one pass a warning check and a prune both start from."""
    scan = scan_directory(fs, path, kind)
    live = liveness(fs, path, scan, now, own_session)
    if kind == "ledger":
        if recover and recover_directory(fs, scan, live, own_session, control, on_record):
            again = scan_directory(fs, path, kind)
            scan.carry_to(again)
            scan = again
            live = liveness(fs, path, scan, now, own_session)
        classify_damaged(fs, scan)
    apply_protection(fs, scan, keep_recent=keep_recent, live=live, has_recording=has_recording)
    return scan
