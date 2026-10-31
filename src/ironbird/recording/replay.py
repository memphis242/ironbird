"""Replay: the frames of one recording, or of a split set, in order (plan 2g).

:class:`Replay` is a read-only iterator of :class:`~ironbird.frame.Frame` over the files of a
recording. It never puts anything on a wire (replay to the wire is Stage 5), it decodes nothing
(the caller does, with the recording's own definitions or the current configuration's), and it
takes everything from the file as untrusted: frames come from
:class:`~ironbird.recording.reader.RecordingReader`, which validates a record completely before it
builds a ``Frame``, so what is yielded can never trip the ``Frame`` asserts or the codec's.

- **Sets.** One file that is part 1 of a split recording plays all its parts, in order; several
  files are played as given (same session, ascending part numbers).
- **Selection.** A time range (half-open, ``start <= time_ns < end``) and a candidate MID set are
  applied per frame, and where the file has a usable index they also skip whole chunks by their
  summaries, so a narrow range of a big recording reads a fraction of it. The index is a hint:
  a chunk is skipped only when its summary says nothing in it can be wanted, and what is read is
  validated as ever. Skipping means the skipped chunks are not checked for damage (their GAP
  records, at the chunk head, are still counted when they overlap the range).
- **Damage.** A torn tail ends a part with ``IB-REC-007``; mid-file damage (``IB-REC-006``) ends
  the replay there unless ``skip_damage`` resumes at the next valid record. Both are handed to
  ``on_damage``; the totals are in :attr:`Replay.stats`.
- **Following.** ``follow`` tails a recording being written (see :class:`Follower`).
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import re
import time
from collections.abc import Callable, Generator, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final

from ironbird.errors import Code, IronbirdError
from ironbird.filter.diag import FilterError
from ironbird.filter.lexer import parse_time
from ironbird.frame import Frame, GapReason
from ironbird.recording import format as fmt
from ironbird.recording.defs import Head, label_of, read_head
from ironbird.recording.format import RecordType
from ironbird.recording.reader import Damage, RecordingReader
from ironbird.recording.writer import part_path
from ironbird.sanitize import clean

POLL_S: Final = 0.1  # how often a followed file is looked at again
HEAD_WAIT_POLLS: Final = (
    30  # polls a new part's head is awaited (it is written just after the file)
)
MAX_PARTS_PROBED: Final = 8  # names past a missing part looked at to tell a hole from the end
MAX_PARTS: Final = 9999
MAX_OFFSET_NS: Final = 100 * 365 * 24 * 3600 * 1_000_000_000
MIN_SPEED: Final = 1e-6
MAX_SPEED: Final = 1e9
_OFFSET_RE: Final = re.compile(r"\+(?:[0-9]{1,12}(?:\.[0-9]{1,9})?(?:ms|h|m|s))+")
_OFFSET_PART_RE: Final = re.compile(r"([0-9]{1,12}(?:\.[0-9]{1,9})?)(ms|h|m|s)")
_UNIT_NS: Final = {"h": 3600 * 1_000_000_000, "m": 60 * 1_000_000_000, "s": 1_000_000_000}
_UNIT_NS["ms"] = 1_000_000
_PART_NAME_RE: Final = re.compile(r"(?P<stem>.+)\.p(?P<part>[0-9]{4})\.ibrec")


# -- time ranges --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TimeSpec:
    """A point in time: an absolute UTC instant, or an offset from the first frame."""

    absolute_ns: int | None = None
    offset_ns: int | None = None

    def resolve(self, first_ns: int | None) -> int | None:
        """The instant in nanoseconds since the epoch; ``None`` for an offset when there is no
        frame to measure from."""
        if self.absolute_ns is not None:
            return self.absolute_ns
        assert self.offset_ns is not None
        return None if first_ns is None else first_ns + self.offset_ns


def parse_time_spec(text: str, option: str) -> TimeSpec:
    """``--from``/``--to`` value: ISO 8601 with ``Z`` or an offset (``2026-10-30T12:00:00Z``), or
    ``+90s``, ``+1m30s``, ``+0.5s``, ``+2h`` from the first frame. ``IB-CLI-001`` otherwise."""
    shown = clean(text, 40)
    if text.startswith("+"):
        if _OFFSET_RE.fullmatch(text) is None:
            raise IronbirdError(
                Code.CLI_USAGE,
                f"{option} {shown!r} is not an offset such as +90s, +1m30s or +250ms",
            )
        total = Decimal(0)
        for number, unit in _OFFSET_PART_RE.findall(text):
            try:
                total += Decimal(number) * _UNIT_NS[unit]
            except InvalidOperation:  # pragma: no cover - the pattern admits only numbers
                raise IronbirdError(Code.CLI_USAGE, f"{option} {shown!r} is not a number") from None
        if total > MAX_OFFSET_NS:
            raise IronbirdError(Code.CLI_USAGE, f"{option} {shown!r} is more than 100 years")
        return TimeSpec(offset_ns=int(total))
    try:
        return TimeSpec(absolute_ns=parse_time(text))
    except FilterError as exc:
        raise IronbirdError(
            Code.CLI_USAGE,
            f"{option} {shown!r} is not a time: use a UTC time with seconds and a zone, such as "
            "2026-10-30T12:00:00Z (or 2026-10-30T14:00:00+02:00), or +90s, +1m30s from the "
            "first frame",
            details=exc.detail_text(),
            cause=exc,
        ) from exc


def check_range(start: TimeSpec | None, end: TimeSpec | None) -> None:
    """``--from`` must be earlier than ``--to``: ``IB-CLI-001`` when two times of the same kind
    (both absolute, or both offsets) are not. (A time and an offset are compared once the first
    frame is known, by :class:`Replay`.)"""
    if start is None or end is None:
        return
    pairs = (
        (start.absolute_ns, end.absolute_ns),
        (start.offset_ns, end.offset_ns),
    )
    for first, second in pairs:
        if first is not None and second is not None and first >= second:
            raise IronbirdError(Code.CLI_USAGE, "--from must be earlier than --to")


def check_speed(value: float) -> float:
    """``--speed``: 0 (as fast as possible) or a multiple of real time from 1e-6 to 1e9."""
    if not (value == 0.0 or MIN_SPEED <= value <= MAX_SPEED):  # also false for NaN
        raise IronbirdError(
            Code.CLI_USAGE,
            f"--speed must be 0 (as fast as possible) or between {MIN_SPEED:.0e} and "
            f"{MAX_SPEED:.0e} times real time",
        )
    return value


# -- pacing ---------------------------------------------------------------------------------


class Pacer:
    """Holds frames back so they leave at the pace they arrived: speed 1 is real time, 2 twice as
    fast, 0 no waiting. The schedule is anchored at the first frame, so a slow consumer does not
    stretch the rest of the run. The schedule advances by the recorded time between consecutive
    frames, and a delta that is negative (the recording's clock stepped backwards, which it marks)
    counts as zero: frames after a step keep their own spacing instead of waiting until the clock
    has caught up with where it was before the step."""

    def __init__(
        self,
        speed: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        slice_s: float = POLL_S,
    ) -> None:
        assert speed >= 0
        self._speed = speed
        self._clock = clock
        self._sleep = sleep
        self._slice = slice_s
        self._anchor_wall = 0.0
        self._last_rec = 0  # time of the previous frame as recorded
        self._elapsed = 0  # nanoseconds of recording the schedule has advanced by
        self._started = False

    def wait(self, time_ns: int, stop: Callable[[], bool]) -> bool:
        """Block until ``time_ns`` is due. ``False``: ``stop()`` became true first."""
        if self._speed == 0:
            return True
        if not self._started:
            self._started = True
            self._anchor_wall, self._last_rec = self._clock(), time_ns
            return True
        self._elapsed += max(0, time_ns - self._last_rec)
        self._last_rec = time_ns
        due = self._anchor_wall + self._elapsed / 1e9 / self._speed
        while (remaining := due - self._clock()) > 0:
            if stop():
                return False
            self._sleep(min(remaining, self._slice))
        return True


# -- following ------------------------------------------------------------------------------


class Follower:
    """Tells a recording still being written from one whose writer is gone.

    The writer holds ``flock(LOCK_EX)`` on part 1 of a recording until the session ends (the
    kernel drops it if the process dies). A reader that finds nothing new takes
    ``LOCK_SH | LOCK_NB`` on the same file: refused means the writer is alive, granted means it
    is gone and the file will not grow, so one more look at the end of the file is the last.
    Where the file system cannot lock (``ENOLCK``, ``EINVAL``, ``EOPNOTSUPP``, as on some NFS
    mounts) :attr:`lockable` is false and only ``idle_s`` ends a wait: that many seconds without
    growth. A recorder that ran unlocked is indistinguishable from a dead one to the probe.
    """

    def __init__(
        self,
        lock_path: Path,
        idle_s: float | None,
        *,
        stop: Callable[[], bool] = lambda: False,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        poll_s: float = POLL_S,
    ) -> None:
        self._idle_s = idle_s
        self._stop = stop
        self._clock = clock
        self._sleep = sleep
        self._poll = poll_s
        self._fd = -1
        self.lockable = True
        try:
            self._fd = os.open(lock_path, os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK | os.O_NOCTTY)
        except OSError:
            self.lockable = False
        self._last_growth = clock()
        self.ended = ""  # why a wait gave up: "stopped", "writer gone" or "idle"
        self._probe_lockable()

    def _probe_lockable(self) -> None:
        if self._fd >= 0 and self.alive() is None:
            self.lockable = False

    def alive(self) -> bool | None:
        """Whether a writer holds the file: ``True``, ``False``, or ``None`` when it cannot be
        told (no lock support, or the file cannot be opened)."""
        if self._fd < 0:
            return None
        try:
            fcntl.flock(self._fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        except OSError:
            return None
        return False

    def close(self) -> None:
        fd, self._fd = self._fd, -1
        if fd >= 0:
            with contextlib.suppress(OSError):
                os.close(fd)

    def waiter(self, size: Callable[[], int]) -> Callable[[], bool]:
        """The ``wait`` for :meth:`RecordingReader.items` over a file whose size ``size()`` gives:
        blocks (polling) until the file has grown, then ``True``; ``False`` when nothing more is
        coming (the writer is gone and the end was read, ``idle_s`` passed, or the run was
        stopped), with :attr:`ended` saying which."""
        known = size()

        def wait() -> bool:
            nonlocal known
            self._last_growth = self._clock()
            while True:
                if self._stop():
                    self.ended = "stopped"
                    return False
                now = size()
                if now != known:
                    known = now
                    return True
                if self.alive() is False:
                    # The writer is gone: nothing is appended from here on. A look at the size
                    # after the probe is the last one.
                    last = size()
                    if last != known:
                        known = last
                        return True
                    self.ended = "writer gone"
                    return False
                if self._idle_s is not None and self._clock() - self._last_growth >= self._idle_s:
                    self.ended = "idle"
                    return False
                self._sleep(self._poll)

        return wait

    def wait_for_file(self, path: Path) -> bool:
        """Block until ``path`` exists (``True``), or nothing more is coming (``False``)."""
        self._last_growth = self._clock()
        while True:
            if os.path.lexists(path):
                return True
            if self._stop():
                self.ended = "stopped"
                return False
            if self.alive() is False:
                self.ended = "writer gone"
                return os.path.lexists(path)
            if self._idle_s is not None and self._clock() - self._last_growth >= self._idle_s:
                self.ended = "idle"
                return False
            self._sleep(self._poll)


# -- the set of files --------------------------------------------------------------------------


def _base_of(path: Path, part: int) -> Path | None:
    """The first part's path for ``path``, which is part ``part`` of its recording."""
    if part == 1:
        return path
    found = _PART_NAME_RE.fullmatch(path.name)
    if found is None or int(found.group("part")) != part:
        return None
    return path.with_name(found.group("stem") + ".ibrec")


@dataclass(slots=True)
class ReplayStats:
    """What a replay found, filled in as it reads."""

    parts: int = 0
    opened: set[tuple[int, int]] = field(default_factory=set)  # (device, inode) of every part read
    frames_read: int = 0  # frames read from the files (chunks skipped by the index are not)
    frames_in_range: int = 0  # ... of them inside the time range
    gaps: int = 0  # GAP records met whose lost frames overlap the range (skipped chunks included)
    gap_frames: int = 0  # frames those records say were lost
    chunks_skipped: int = 0  # chunks the index ruled out: not read, so not checked for damage
    damage: int = 0  # stretches of damage met
    damage_bytes: int = 0
    mid_file_damage: bool = False  # valid records follow some damage, whether read or not
    torn_tail: bool = False
    unclosed: int = 0  # parts that end without a trailer and without damage (a recorder killed)
    live_end: bool = False  # a part ended where a writer is still writing it
    missing_parts: list[int] = field(default_factory=list)


class Replay:
    """The frames of a recording or a split set. Construct (this opens and checks the files),
    iterate :meth:`frames`, :meth:`close`."""

    def __init__(
        self,
        paths: Sequence[Path],
        *,
        skip_damage: bool = False,
        follow: bool = False,
        follow_idle_s: float | None = None,
        start: TimeSpec | None = None,
        end: TimeSpec | None = None,
        on_damage: Callable[[Damage, Path], None] | None = None,
        on_note: Callable[[Code, str], None] | None = None,
        stop: Callable[[], bool] = lambda: False,
        follower: Follower | None = None,
    ) -> None:
        assert paths, "a replay needs a file"
        self._skip_damage = skip_damage
        self._follow = follow
        self._start_spec, self._end_spec = start, end
        self._mids: frozenset[int] | None = None
        self._on_damage = on_damage
        self._on_note = on_note
        self._stop = stop
        self.stats = ReplayStats()
        self.parts: list[Path] = []
        self._base: Path | None = None
        self._stopped = False  # damage ended the replay
        self._session: object = None  # META's session of the first file
        self._select_nothing = False  # the range names an offset from a frame that was not found
        first = RecordingReader(paths[0], skip_damage=skip_damage)
        try:
            self.meta: fmt.Meta = first.meta
            self.max_packet_bytes: int = first.max_packet_bytes
            self.head: Head = read_head(first)
            self.label = label_of(first.path)
            self.writer_active = first.writer_active
            self._resolve_parts(list(paths), first)
        finally:
            first.close()
        self._follower = follower
        if follow and self._follower is None:
            # The writer holds part 1 for the whole session, whichever part is being followed.
            self._follower = Follower(self._base or self.parts[0], follow_idle_s, stop=stop)
        if (
            follow
            and self._follower is not None
            and not self._follower.lockable
            and (follow_idle_s is None)
        ):
            self._follower.close()
            raise IronbirdError(
                Code.CLI_USAGE,
                "--follow cannot tell a recorder that died from one that is quiet here: "
                "this file system has no usable file locks; pass --follow-idle SECONDS "
                "to end after that long without growth",
            )

    # -- resolving what to play --

    def _resolve_parts(self, paths: list[Path], first: RecordingReader) -> None:
        part = first.meta.part
        session = self._session = first.meta.data.get("session")
        if len(paths) == 1:
            base = _base_of(paths[0], part)
            self._base = base
            self.parts = [paths[0]]
            if part == 1 and base is not None:
                self._scan_parts(base)
            self._check_set(self.parts[1:], part, session)
            return
        self._base = _base_of(paths[0], part)
        self.parts = list(paths)
        self._check_set(paths[1:], part, session)

    def _scan_parts(self, base: Path) -> None:
        """Every further part of the recording that starts at ``base``, found by name. A part
        that is absent while a later one exists is missing (``_check_set`` records it, the later
        one still plays); :data:`MAX_PARTS_PROBED` absent names in a row end the search."""
        absent: list[int] = []
        for number in range(2, MAX_PARTS + 1):
            path = part_path(base, number)
            if os.path.lexists(path):
                absent.clear()
                self.parts.append(path)
            else:
                absent.append(number)
                if len(absent) >= MAX_PARTS_PROBED:
                    break

    def _check_set(self, others: Sequence[Path], part: int, session: object) -> None:
        """Every further file is a later part of the same session (else ``IB-CLI-001``)."""
        previous = part
        for path in others:
            try:
                reader = RecordingReader(path)
            except IronbirdError as exc:
                raise IronbirdError(
                    exc.code,
                    f"part {clean(path.name, 120)}: {exc.summary}",
                    details=exc.details,
                    cause=exc.cause,
                ) from exc
            try:
                number = reader.meta.part
                theirs = reader.meta.data.get("session")
                cap = reader.max_packet_bytes
            finally:
                reader.close()
            if theirs != session:
                raise IronbirdError(
                    Code.CLI_USAGE,
                    f"{clean(path.name, 120)} belongs to another recording session than "
                    f"{clean(self.parts[0].name, 120)}; replay files of one session only",
                )
            if number <= previous:
                raise IronbirdError(
                    Code.CLI_USAGE,
                    f"{clean(path.name, 120)} is part {number} but follows part {previous}; "
                    "name the parts in order",
                )
            if cap != self.max_packet_bytes:
                raise IronbirdError(
                    Code.CLI_USAGE,
                    f"{clean(path.name, 120)} was recorded with another packet cap "
                    f"({cap}) than {clean(self.parts[0].name, 120)} ({self.max_packet_bytes})",
                )
            self.stats.missing_parts.extend(range(previous + 1, number))
            previous = number

    def select_mids(self, mids: frozenset[int] | None) -> None:
        """Candidate MIDs of the filter in use (``None``: any): chunks whose summary shows none of
        them are skipped. Call before :meth:`frames`."""
        self._mids = mids

    @property
    def ended_by_damage(self) -> bool:
        """Whether mid-file damage stopped the replay (``skip_damage`` was not given)."""
        return self._stopped

    @property
    def follow_ended(self) -> str:
        """Why a followed replay stopped waiting: ``"writer gone"`` (the lock is free), ``"idle"``
        (``follow_idle_s`` passed without growth; a writer may be alive), ``"stopped"`` (the
        output went away), or ``""`` (it did not wait)."""
        return self._follower.ended if self._follower is not None else ""

    @property
    def nodes(self) -> list[str]:
        """Nodes whose definitions decode the recording's streams (in order of first stream)."""
        return list(dict.fromkeys(stream.node for stream in self.head.streams.values()))

    @property
    def link_names(self) -> frozenset[str]:
        return frozenset(stream.link for stream in self.head.streams.values())

    # -- frames --

    def first_frame_ns(self) -> int | None:
        """The time of the first frame of the first part, or ``None`` if it has none (yet), or if
        damage that ends the replay comes first. Damage is not reported from here: the replay
        itself reads the file again from the start and meets it."""
        reader = RecordingReader(self.parts[0], skip_damage=self._skip_damage)
        try:
            wait = (
                self._follower.waiter(reader.size)
                if self._follow and self._follower is not None
                else None
            )
            for item in reader.items(wait=wait):
                if isinstance(item, Damage):
                    if self._skip_damage:
                        continue  # the replay reports it when it reads there
                    return None
                if item.rtype == RecordType.FRAME:
                    return int(item.value.time_ns)
        finally:
            reader.close()
        return None

    def frames(self) -> Generator[Frame, None, None]:
        """Every frame of the set inside the time range and the MID set, in file order."""
        first_ns: int | None = None
        specs = (self._start_spec, self._end_spec)
        if any(spec is not None and spec.offset_ns is not None for spec in specs):
            first_ns = self.first_frame_ns()
        start = self._start_spec.resolve(first_ns) if self._start_spec is not None else None
        end = self._end_spec.resolve(first_ns) if self._end_spec is not None else None
        if start is not None and end is not None and start >= end:
            raise IronbirdError(Code.CLI_USAGE, "--from must be earlier than --to")
        # An offset from a first frame that does not exist (an empty file, or damage that ends the
        # replay before one) selects nothing, but the files are still read, so that damage is
        # met, reported and counted rather than silently passed over.
        self._select_nothing = (self._start_spec is not None and start is None) or (
            self._end_spec is not None and end is None
        )
        index = 0
        while index < len(self.parts) and not self._stopped:
            path = self.parts[index]
            last = index == len(self.parts) - 1
            self.stats.parts += 1
            closed = yield from self._part(path, start, end, wait_for_more=last and self._follow)
            index += 1
            if last and self._follow and closed is not None and not self._stopped:
                self._next_part(closed)

    def _next_part(self, closed: int) -> None:
        """After the last known part of a followed recording ended at its trailer (it was part
        ``closed``): wait for the next part to appear, and queue it if it does. Adds nothing when
        the session is over."""
        base, follower = self._base, self._follower
        if base is None or follower is None or closed >= MAX_PARTS:
            return
        path = part_path(base, closed + 1)
        if not follower.wait_for_file(path):
            return
        # The part is created before its head is written: a head that is not there yet is awaited
        # for a moment, and one that stays unreadable is reported like any other.
        for attempt in range(HEAD_WAIT_POLLS + 1):
            try:
                self._check_set([path], closed, self._session)
                break
            except IronbirdError as exc:
                if (
                    exc.code not in (Code.REC_FORMAT, Code.REC_TORN)
                    or attempt == HEAD_WAIT_POLLS
                    or self._stop()
                ):
                    raise
                time.sleep(POLL_S)
        self.parts.append(path)

    def _part(
        self, path: Path, start: int | None, end: int | None, *, wait_for_more: bool
    ) -> Generator[Frame, None, int | None]:
        """The frames of one file. Returns (as the generator's value) the part number if the file
        ended at its trailer, else ``None``."""
        stats = self.stats
        reader = RecordingReader(path, skip_damage=self._skip_damage)
        stats.opened.add(reader.identity)
        try:
            skip = None if self._select_nothing else self._chunk_skipper(reader, start, end)
            wait = (
                self._follower.waiter(reader.size)
                if wait_for_more and self._follower is not None
                else None
            )
            damaged = False
            for item in reader.items(skip=skip, wait=wait):
                if isinstance(item, Damage):
                    damaged = True
                    self._damage(item, reader, path)
                    continue
                match item.rtype:
                    case RecordType.FRAME:
                        frame: Frame = item.value
                        stats.frames_read += 1
                        if self._select_nothing:
                            continue
                        if start is not None and frame.time_ns < start:
                            continue
                        if end is not None and frame.time_ns >= end:
                            continue
                        stats.frames_in_range += 1
                        yield frame
                    case RecordType.GAP:
                        self._count_gap(item.value, start, end)
                    case _:
                        pass
            if reader.trailer is None and not damaged:
                if reader.writer_active and not self._follow:
                    stats.live_end = True
                elif self.follow_ended in ("", "writer gone"):
                    # The file ends at a record boundary and its writer is not there to close it.
                    # (A follow that gave up while a writer may be alive says nothing about it.)
                    stats.unclosed += 1
            return reader.meta.part if reader.trailer is not None else None
        finally:
            reader.close()

    def _count_gap(self, gap: fmt.Gap, start: int | None, end: int | None) -> None:
        """Count a GAP record if the frames it says were lost overlap the selected range."""
        if (start is None or gap.last_ns >= start) and (end is None or gap.first_ns < end):
            self.stats.gaps += 1
            if gap.reason is not GapReason.LINK_DOWN:  # an outage's frames cannot be counted
                self.stats.gap_frames += gap.lost

    def _damage(self, damage: Damage, reader: RecordingReader, path: Path) -> None:
        stats = self.stats
        stats.damage += 1
        stats.damage_bytes += damage.span
        if damage.torn and self._follow and self._stop():
            return  # the follow was stopped (the output went away), not a recorder that died
        if damage.torn and reader.writer_active and not self._follow:
            stats.live_end = True  # being written right now: the end is not damage
            return
        if damage.torn:
            stats.torn_tail = True
        else:
            stats.mid_file_damage = True
            if not self._skip_damage:
                self._stopped = True
        if self._on_damage is not None:
            self._on_damage(damage, path)

    def _chunk_skipper(
        self, reader: RecordingReader, start: int | None, end: int | None
    ) -> Callable[[int], int | None] | None:
        """A ``skip`` for :meth:`RecordingReader.items` that jumps the chunks of a closed file the
        range and MID set rule out, or ``None`` when nothing is selected or there is no usable
        index (a recording still being written has none)."""
        mids = self._mids
        if start is None and end is None and mids is None:
            return None
        located = reader.read_trailer()
        if located is None:
            return None
        trailer_offset, trailer = located
        try:
            chunks = reader.read_chunks(trailer, trailer_offset)
        except ValueError as exc:
            if self._on_note is not None:
                self._on_note(
                    Code.REC_INDEX,
                    f"no usable index ({clean(str(exc), 200)}); every record is read instead",
                )
            return None
        jumps: dict[int, tuple[int, int, int]] = {}
        for position, (offset, chunk) in enumerate(chunks):
            if _wanted(chunk, start, end, mids):
                continue
            after = chunks[position + 1][1].chunk_start if position + 1 < len(chunks) else None
            resume = after if after is not None else trailer.index_offset or trailer_offset
            here = max(chunk.chunk_start, reader.body_start)
            if resume > here:
                jumps[here] = (offset, resume, here)
        stats = self.stats

        def skip(offset: int) -> int | None:
            jump = jumps.get(offset)
            if jump is None:
                return None
            record_offset, resume, here = jump
            for gap in reader.skip_chunk(here, record_offset):
                self._count_gap(gap, start, end)
            stats.chunks_skipped += 1
            return resume

        return skip

    def close(self) -> None:
        if self._follower is not None:
            self._follower.close()


def _wanted(
    chunk: fmt.Chunk, start: int | None, end: int | None, mids: frozenset[int] | None
) -> bool:
    """Whether a chunk's summary leaves open that it holds a frame in the range and MID set."""
    if not chunk.frames:
        return False
    if start is not None and chunk.max_time_ns < start:
        return False
    if end is not None and chunk.min_time_ns >= end:
        return False
    if mids is not None and not chunk.mid_overflow:
        return any(mid in mids for mid, _ in chunk.mids)
    return True


__all__ = [
    "Follower",
    "Pacer",
    "Replay",
    "ReplayStats",
    "TimeSpec",
    "check_range",
    "check_speed",
    "parse_time_spec",
]
