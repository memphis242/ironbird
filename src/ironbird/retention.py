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

from ironbird.atomicfile import Collision, SyncAfterCommit, TempOutput, rename_noreplace
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
FRESH_S: Final = 600.0  # a session whose newest file is younger than this is live
_N: Final = r"(?:[0-9]{4}|[1-9][0-9]{4,8})"
_STAMP: Final = r"([0-9]{8}T[0-9]{6}Z)-([0-9a-f]{12})"
LEDGER_RE: Final = re.compile(_STAMP + rf"(?:\.p({_N}))?\.jsonl")
RESERVE_RE: Final = re.compile(_STAMP + rf"\.r({_N})\.reserve")
RECORDING_RE: Final = re.compile(_STAMP + rf"(?:\.p({_N}))?\.ibrec")
LONG_COMMANDS: Final = frozenset({"listen", "events", "respond", "record", "replay"})
NO_LOCK_ERRNOS: Final = frozenset({errno.ENOLCK, errno.EINVAL, errno.EOPNOTSUPP, errno.EBADF})

# Protection reasons, in the order they are attributed to a session's bytes.
LIVE: Final = "live"
FUTURE: Final = "future mtime"
INCOMPLETE: Final = "incomplete"
HARDLINKED: Final = "hard-linked"
UNRECOVERABLE: Final = "unrecoverable reserve"
KEEP_RECENT: Final = "keep_recent"
INSIDE_LONG: Final = "inside a long session"
HAS_RECORDING: Final = "recording present"
REASON_GROUPS: Final = {
    INCOMPLETE: "incomplete or hard-linked",
    HARDLINKED: "incomplete or hard-linked",
}


def is_managed_name(name: str) -> bool:
    """Whether ``name`` is one retention manages: a ledger part, a reserve or a recording."""
    return any(p.fullmatch(name) for p in (LEDGER_RE, RESERVE_RE, RECORDING_RE))


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
    is_reserve: bool
    dev: int
    ino: int
    size: int
    mtime: float
    nlink: int


@dataclass(slots=True)
class Session:
    id: str
    start: float  # from the stamp in the name
    files: list[FileEntry] = field(default_factory=list)
    klass: str | None = None  # "long" | "short" (read lazily from part 1's head)
    protected: list[str] = field(default_factory=list)

    @property
    def has_part1(self) -> bool:
        return any(not f.is_reserve and f.number == 1 for f in self.files)

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
        return sum(1 for f in self.files if not f.is_reserve)

    @property
    def reserves(self) -> list[FileEntry]:
        return sorted((f for f in self.files if f.is_reserve), key=lambda f: f.number)

    @property
    def hard_linked(self) -> bool:
        return any(f.nlink > 1 for f in self.files)

    def part1(self) -> FileEntry | None:
        for f in self.files:
            if not f.is_reserve and f.number == 1:
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
    unsafe: bool = False  # writable by group or others without the sticky bit
    unlocked: bool = False  # a lock probe was unsupported here
    recovered: int = 0
    unrecoverable: set[str] = field(default_factory=set)
    messages: list[str] = field(default_factory=list)  # IB-RET-004 lines, ready to show
    recoveries: list[dict[str, Any]] = field(default_factory=list)

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
        patterns = (LEDGER_RE, RESERVE_RE) if kind == "ledger" else (RECORDING_RE,)
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
    return scan


def _take_entry(
    fs: FileSystem, dir_fd: int, name: str, patterns: tuple[re.Pattern[str], ...], scan: DirScan
) -> None:
    for pattern in patterns:
        found = pattern.fullmatch(name)
        if found is None:
            continue
        is_reserve = pattern is RESERVE_RE
        start = _valid_stamp(found.group(1))
        number_text = found.group(3)
        number = int(number_text) if number_text else 1
        if start is None or (number_text and not is_reserve and number < 2):
            scan.unparseable += 1
            return
        st = _regular_owned_stat(fs, dir_fd, name)
        if st is None:
            scan.foreign += 1  # a symbolic link, a vanished or unreadable name, not ours
            return
        session_id = found.group(2)
        session = scan.sessions.get(session_id)
        if session is None:
            session = scan.sessions[session_id] = Session(session_id, start)
        else:
            session.start = min(session.start, start)
        session.files.append(_file_entry(name, number, is_reserve, st))
        return


def _regular_owned_stat(fs: FileSystem, dir_fd: int, name: str) -> os.stat_result | None:
    """``fstat`` of ``name`` opened the inspection way; ``None`` unless it is a regular file of
    this user (a symbolic link, a FIFO, someone else's file, a vanished name)."""
    try:
        fd = fs.open_at(dir_fd, name, INSPECT_FLAGS)
    except OSError:
        return None
    try:
        st = fs.fstat(fd)
    except OSError:
        return None
    finally:
        close_quietly(fs, fd)
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid():
        return None
    return st


def _file_entry(name: str, number: int, is_reserve: bool, st: os.stat_result) -> FileEntry:
    return FileEntry(
        name, number, is_reserve, st.st_dev, st.st_ino, st.st_size, st.st_mtime, st.st_nlink
    )


def _relist(fs: FileSystem, dir_fd: int, session: Session) -> list[FileEntry]:
    """The session's pattern files as the directory holds them right now (a scan's listing can
    be stale by the time a recovery holds the session's lock)."""
    files: list[FileEntry] = []
    for entry in fs.scandir(dir_fd):
        for pattern in (LEDGER_RE, RESERVE_RE):
            found = pattern.fullmatch(entry.name)
            if found is None or found.group(2) != session.id:
                continue
            is_reserve = pattern is RESERVE_RE
            number_text = found.group(3)
            number = int(number_text) if number_text else 1
            st = _regular_owned_stat(fs, dir_fd, entry.name)
            if st is not None and not (number_text and not is_reserve and number < 2):
                files.append(_file_entry(entry.name, number, is_reserve, st))
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
                if (f.is_reserve or f.number == 1) and probe_lock(fs, dir_fd, f.name) == "held":
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
    reserve, has its recording, is among the ``keep_recent`` most recent (of any class, and of
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


def _read_facts(fs: FileSystem, fd: int, size: int) -> _Facts:
    first_nul: int | None = None
    last_nl = -1
    junk: int | None = None
    offset = 0
    while offset < size:
        block = fs.pread(fd, min(READ_BLOCK, size - offset), offset)
        if not block:
            break
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
    return _Facts(size, first_nul, last_nl, junk)


@dataclass(slots=True)
class RecoveryContext:
    fs: FileSystem
    dir_fd: int
    scan: DirScan
    session: Session
    next_part: int = 2
    control: ScanControl | None = None

    @property
    def stopping(self) -> bool:
        return self.control is not None and self.control.stop.is_set()


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
) -> bool:
    """Recover the leftover reserves of every non-live session of a ledger directory (cut at the
    last complete record and renamed to the next part, never deleted). A directory writable by
    others without the sticky bit is not touched. Returns whether any file changed.

    ``control.stop`` ends the pass between sessions: no recovery step begins after it is set."""
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
            if session.id in live or session.id == own_session or not session.reserves:
                continue
            ctx = RecoveryContext(fs, dir_fd, scan, session, control=control)
            changed |= _recover_session(ctx)
    finally:
        close_quietly(fs, dir_fd)
    return changed


def _recover_session(ctx: RecoveryContext) -> bool:
    fs, scan, session = ctx.fs, ctx.scan, ctx.session
    part1 = session.part1()
    reserves = session.reserves
    if part1 is None:
        _unrecoverable(scan, session, reserves[0].name, "its session has no part 1")
        return False
    try:
        fd1 = fs.open_at(ctx.dir_fd, part1.name, INSPECT_FLAGS)
    except OSError as exc:
        _unrecoverable(scan, session, reserves[0].name, f"part 1 cannot be opened: {exc.strerror}")
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
            _unrecoverable(scan, session, reserves[0].name, "file locks are unavailable here")
            return False
        # The listing is from before the lock: another scan may have recovered the session
        # meanwhile, so what is left to do is what the directory holds now.
        try:
            session.files = _relist(fs, ctx.dir_fd, session)
        except OSError as exc:
            _unrecoverable(
                scan, session, reserves[0].name, f"the directory cannot be listed: {exc.strerror}"
            )
            return False
        reserves = session.reserves
        if not reserves:
            return False  # the session was recovered by another scan: nothing left, nothing to say
        if session.part1() is None:
            _unrecoverable(scan, session, reserves[0].name, "its session has no part 1")
            return False
        numbers = [f.number for f in session.files if not f.is_reserve]
        ctx.next_part = max(numbers, default=1) + 1
        for reserve in reserves:
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
                return changed
    finally:
        for fd in held:
            close_quietly(fs, fd)
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
    """A reserve whose inode also carries a part name of the session is an interrupted rename
    (the ``link`` path of the helper): the reserve name is unlinked, leaving the part name.

    Both names are looked at again now, and the inode must really have two links, so a stale
    listing cannot make this unlink a name that is not a second name of the file."""
    fs, scan, session, name = ctx.fs, ctx.scan, ctx.session, reserve.name
    for other in session.files:
        if other.is_reserve or (other.dev, other.ino) != (st.st_dev, st.st_ino):
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
        scan.recovered += 1
        scan.recoveries.append({"old": name, "new": other.name, "kept": st.st_size, "cut": 0})
        return "changed"
    return None


def _commit_recovered(
    ctx: RecoveryContext, reserve: FileEntry, fd: int, st: os.stat_result, cut: int
) -> str:
    fs, scan, session, name = ctx.fs, ctx.scan, ctx.session, reserve.name
    new = part_name(_stamp_of(session), session.id, ctx.next_part)
    try:
        fs.ftruncate(fd, cut)
        fs.fdatasync(fd)
        rename_noreplace(ctx.dir_fd, name, new, fd, fs=fs)
    except Collision:
        _unrecoverable(scan, session, name, f"a file already has the next part name {new}")
        return "stop"
    except SyncAfterCommit:
        pass  # renamed; the directory sync failed - the rename is not undone
    except OSError as exc:
        _unrecoverable(scan, session, name, f"it could not be renamed: {exc.strerror}")
        return "stop"
    scan.recovered += 1
    scan.recoveries.append({"old": name, "new": new, "kept": cut, "cut": st.st_size - cut})
    ctx.next_part += 1
    return "changed"


def _salvage(
    ctx: RecoveryContext, reserve: FileEntry, fd: int, st: os.stat_result, cut: int
) -> str:
    """A valid prefix followed by damage.

    The damaged original is set aside first, byte-identical, as ``<name>.damaged`` (one atomic
    non-replacing rename): that name is outside the retention patterns, so no later scan finds
    the reserve again, whatever happens next. Then the prefix's complete lines are copied from
    the held descriptor to a temporary file and committed to the session's next part name; a
    crash or a full disk leaves only a dot-temporary file and the intact ``.damaged`` file,
    never a torn or a duplicate part. ``ledger.recovered`` is always written."""
    fs, scan, session, name = ctx.fs, ctx.scan, ctx.session, reserve.name
    damaged = f"{name}.damaged"
    if ctx.stopping:
        return "stop"  # nothing is committed to yet: the next scan salvages it
    try:
        rename_noreplace(ctx.dir_fd, name, damaged, fd, fs=fs)
    except SyncAfterCommit:
        pass
    except Collision:
        _unrecoverable(scan, session, name, f"a file named {damaged} already exists")
        return "stop"
    except OSError as exc:
        _unrecoverable(
            scan, session, name, f"it could not be set aside as .damaged: {exc.strerror}"
        )
        return "stop"
    control = ctx.control
    if control is not None:
        control.stepping.set()  # from here the copy runs to its end
    try:
        new, error = _copy_prefix(ctx, fd, cut)
    finally:
        if control is not None:
            control.stepping.clear()
    record: dict[str, Any] = {
        "old": name,
        "new": new,
        "kept": cut if new else 0,
        "cut": st.st_size - (cut if new else 0),
        "damaged": damaged,
    }
    scan.recovered += 1
    if new is None:
        record["error"] = error
        scan.recoveries.append(record)
        scan.unrecoverable.add(session.id)
        scan.messages.append(
            f"reserve {name} was damaged and its records could not be saved ({error}); "
            f"the whole file is kept as {damaged}; inspect or delete that file"
        )
        return "stop"
    scan.recoveries.append(record)
    scan.messages.append(
        f"reserve {name} was damaged: {format_size(cut)} saved as part "
        f"p{ctx.next_part:04d}, the rest kept as {damaged}; inspect or delete that file"
    )
    ctx.next_part += 1
    return "changed"


def _copy_prefix(ctx: RecoveryContext, fd: int, cut: int) -> tuple[str | None, str]:
    """Copy the first ``cut`` bytes of ``fd`` to the session's next free part name through a
    temporary file. Returns ``(part name, "")``, or ``(None, why)``."""
    fs, session = ctx.fs, ctx.session
    new = ""
    for _ in range(NAME_TRIES):
        new = part_name(_stamp_of(session), session.id, ctx.next_part)
        if try_stat_at(fs, ctx.dir_fd, new) is None:
            break
        ctx.next_part += 1
    else:
        return None, "no free part name"
    try:
        with TempOutput(ctx.scan.path / new, fs=fs) as out:
            offset = 0
            while offset < cut:
                block = fs.pread(fd, min(READ_BLOCK, cut - offset), offset)
                if not block:
                    raise OSError(errno.EIO, "the reserve ended early")
                out.write(block)
                offset += len(block)
            with contextlib.suppress(SyncAfterCommit):
                out.commit()  # a failed directory sync after the rename does not undo it
    except Collision:
        return None, f"a file appeared at {new}"
    except OSError as exc:
        return None, exc.strerror or str(exc)
    return new, ""


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
) -> DirScan:
    """Scan, probe liveness, recover leftover reserves (a ledger directory; ``recover=False``
    leaves them for a scan that may), then decide the protections - the one pass a warning check
    and a prune both start from."""
    scan = scan_directory(fs, path, kind)
    live = liveness(fs, path, scan, now, own_session)
    if kind == "ledger" and recover and recover_directory(fs, scan, live, own_session, control):
        again = scan_directory(fs, path, kind)
        again.recovered, again.recoveries = scan.recovered, scan.recoveries
        again.unrecoverable, again.messages = scan.unrecoverable, scan.messages
        again.unlocked = scan.unlocked
        scan = again
        live = liveness(fs, path, scan, now, own_session)
    apply_protection(fs, scan, keep_recent=keep_recent, live=live, has_recording=has_recording)
    return scan
