"""Send modes of ``ib send``: at a time, repeated, periodic.

The flags and how they combine (every refusal is ``IB-CLI-001``, exit 2, before anything is
opened or sent):

- ``--at T`` starts the series at T. T is an ISO 8601 time with seconds (``2026-11-04T12:00:00Z``,
  ``...+02:00``; a time with no zone is read as **UTC**, never as local time) or an offset from
  the moment ``ib`` started (``+10s``, ``+1m30s``, ``+250ms``). It must be in the future.
- ``--repeat N`` sends N packets. With no ``--every`` they go out back to back (a burst: the
  next one as soon as the previous one left).
- ``--every D`` sends one packet per period D (``1ms``, ``250ms``, ``1.5s``, ``2m``; units
  ``h m s ms``, largest first and each at most once, no bare numbers: ``5`` could be anything).
  It needs an end: ``--repeat N`` (a cap on the packets), ``--for D`` (a duration from the
  first slot) or ``--until T`` (a time, as for ``--at``); whichever of those is given, the first
  to be reached ends the series. ``--until`` and ``--for`` together contradict each other. Both
  mean nothing without ``--every``. The window ``[start, end)`` is half open: a packet is sent
  at ``start + k*D`` while that is before the end, so ``--every 1s --for 3s`` sends at 0, 1, 2 s.
- Refused as meaningless: a window no longer than the period (one packet at most), ``--repeat 1``
  with ``--every`` (the period is never used), a period under :data:`MIN_EVERY_NS`, a series of
  more than :data:`MAX_SENDS` packets, a start or an end more than :data:`MAX_LEAD_NS` ahead (an
  offset or ``--for`` longer than that is refused as it is parsed), an end not after the start.

Timing. Deadlines are absolute points of the monotonic clock (``start + k*D``), so a late send
never moves the later ones and no error accumulates. A wall-clock ``--at`` is converted to the
monotonic clock one second before it is due, so a clock step in a long wait is followed and one
in the last second is not. ``--until`` is converted when the series starts: a wall-clock step
during the series is not followed. The packets are sent by the pacer thread
(:mod:`ironbird.pacer`); this module holds the plan and the slot arithmetic it follows.

The start. With ``--at``, the start and the window are fixed by the wall clock before the link
is opened: a link that opens after T makes the first slots late, and they are skipped and
counted as any late slot is (a series with no ``--every`` is sent late, and says how late); a
window that passes entirely during the open is refused with nothing sent. With no ``--at`` the
series starts when the link is open (:data:`START_LEAD_NS` later, so that the first packet is
built ahead of its slot); a ``--until`` window then shrinks by the time the open took, and one
left no longer than the period is refused (nothing is sent), as it is when typed.

Overruns. A slot whose packet cannot leave until more than half a period after its deadline is
**skipped**: counted and ledgered, and the series carries on at the slot nearest to now. The
alternative, catching up, sends the missed packets back to back, which turns a slow link or
vehicle into a burst and puts packets at times no one asked for; a test of a cadence wants the
cadence kept, and wants to be told how often it was not. Every packet therefore starts its send
at least half a period after the previous one did. A skipped slot does not count toward
``--repeat`` (N packets are sent), but is lost from a ``--for``/``--until`` window.

Lateness is the monotonic time from a slot's deadline to the return of the send call that handed
its packet to the link (measured in the pacer thread); the series reports the maximum and the
mean, and for ``--at`` how late the first packet left.
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final, Protocol

from ironbird.errors import Code, IronbirdError
from ironbird.filter.diag import FilterError
from ironbird.filter.lexer import parse_time
from ironbird.sanitize import clean

NS: Final = 1_000_000_000
MIN_EVERY_NS: Final = 1_000_000  # 1 ms
MAX_EVERY_NS: Final = 30 * 86_400 * NS
MAX_LEAD_NS: Final = 366 * 86_400 * NS  # a start or an end further ahead than this is a typo
MAX_SENDS: Final = 100_000_000
WALL_FINE_NS: Final = NS  # within this of a wall-clock start, switch to the monotonic clock
WALL_SLICE_S: Final = 30.0  # the longest sleep while waiting for a wall-clock start
START_LEAD_NS: Final = 20_000_000  # a series with no --at starts this long after it is ready

_UNIT_NS: Final = {"h": 3600 * NS, "m": 60 * NS, "s": NS, "ms": 1_000_000}
_NUMBER: Final = r"[0-9]{1,9}(?:\.[0-9]{1,9})?"
# Units largest first, each at most once: ``1m30s`` and ``1.5s``, not ``30s1m`` or ``1s1s``.
_DURATION_RE: Final = re.compile(
    rf"(?=.)(?:(?P<h>{_NUMBER})h)?(?:(?P<m>{_NUMBER})m)?(?:(?P<s>{_NUMBER})s)?"
    rf"(?:(?P<ms>{_NUMBER})ms)?"
)
_ZONE_RE: Final = re.compile(r"(?:Z|[+-][0-9]{2}(?::?[0-9]{2})?)$")
DURATION_FORMS: Final = (
    "a number with a unit, such as 250ms, 1.5s, 2m or 1m30s (units h m s ms, largest first)"
)
TIME_FORMS: Final = (
    "a UTC time such as 2026-11-04T12:00:00Z (a time without a zone is read as UTC; "
    "2026-11-04T14:00:00+02:00 names another), or an offset from now such as +10s or +1m30s"
)


class Clock(Protocol):
    """What waiting for a start needs from time; tests supply a simulated one."""

    def wall_ns(self) -> int: ...

    def mono_ns(self) -> int: ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def wall_ns(self) -> int:
        return time.time_ns()

    def mono_ns(self) -> int:
        return time.monotonic_ns()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


def _usage(summary: str) -> IronbirdError:
    return IronbirdError(Code.CLI_USAGE, summary)


def parse_duration(text: str, option: str, limit_ns: int = MAX_LEAD_NS) -> int:
    """``250ms``, ``1.5s``, ``1m30s`` as nanoseconds, at most ``limit_ns``; ``IB-CLI-001`` for
    anything else."""
    found = _DURATION_RE.fullmatch(text)
    if found is None:
        raise _usage(f"{option} {clean(text, 40)!r} is not a duration: use {DURATION_FORMS}")
    total = sum(
        (Decimal(number) * _UNIT_NS[unit] for unit, number in found.groupdict().items() if number),
        Decimal(0),
    )
    if total > limit_ns:
        raise _usage(f"{option} {clean(text, 40)} is too long: the most is {fmt_ns(limit_ns)}")
    return int(total)


def iso_utc(wall_ns: int) -> str:
    """``2026-11-04T12:00:00.250Z`` for nanoseconds since the Unix epoch; a value outside the
    years 1 to 9999 is shown as its seconds since the epoch."""
    try:
        stamp = datetime.fromtimestamp(wall_ns // NS, UTC)
    except (OverflowError, OSError, ValueError):
        return f"{wall_ns // NS} s since 1970-01-01T00:00:00Z"
    return f"{stamp:%Y-%m-%dT%H:%M:%S}.{wall_ns % NS // 1_000_000:03d}Z"


def fmt_ns(ns: int) -> str:
    """A duration for people: ``850 us``, ``1.25 ms``, ``2.5 s``, ``3m 05s``, ``1h 02m``."""
    magnitude = abs(ns)
    if magnitude < 1_000_000:
        return f"{ns / 1000:.0f} us"
    if magnitude < NS:
        return f"{ns / 1_000_000:.2f} ms"
    if magnitude < 60 * NS:
        return f"{ns / NS:.2f} s"
    if magnitude < 3600 * NS:
        minutes, seconds = divmod(ns // NS, 60)
        return f"{minutes}m {seconds:02d}s"
    hours, rest = divmod(ns // NS, 3600)
    return f"{hours}h {rest // 60:02d}m"


@dataclass(frozen=True, slots=True)
class TimeArg:
    """A start or an end as typed: an absolute UTC instant, or an offset from the invocation."""

    absolute_ns: int | None = None
    offset_ns: int | None = None
    assumed_utc: bool = False  # the text had no zone and was read as UTC

    def resolve(self, anchor_wall_ns: int) -> int:
        if self.absolute_ns is not None:
            return self.absolute_ns
        assert self.offset_ns is not None
        return anchor_wall_ns + self.offset_ns


def parse_time_arg(text: str, option: str) -> TimeArg:
    """An ISO 8601 time (no zone means UTC) or ``+offset`` (at most :data:`MAX_LEAD_NS`);
    ``IB-CLI-001`` otherwise."""
    shown = clean(text, 40)
    if text.startswith("+"):
        if _DURATION_RE.fullmatch(text[1:]) is None:
            raise _usage(f"{option} {shown!r} is not a time: use {TIME_FORMS}")
        return TimeArg(offset_ns=parse_duration(text[1:], option))
    clock_part = text.partition("T")[2]
    has_zone = _ZONE_RE.search(clock_part) is not None
    try:
        return TimeArg(
            absolute_ns=parse_time(text if has_zone else text + "Z"), assumed_utc=not has_zone
        )
    except FilterError as exc:
        raise IronbirdError(
            Code.CLI_USAGE,
            f"{option} {shown!r} is not a time: use {TIME_FORMS}",
            details=exc.detail_text(),
            cause=exc,
        ) from exc


@dataclass(frozen=True, slots=True)
class SendSpec:
    """The send-mode flags as typed and checked for combination, before the clock is read."""

    at: TimeArg | None = None
    repeat: int | None = None
    every_ns: int | None = None
    until: TimeArg | None = None
    for_ns: int | None = None

    @property
    def is_series(self) -> bool:
        """Whether anything but a plain single send was asked for."""
        return any(v is not None for v in (self.at, self.repeat, self.every_ns))


def parse_spec(
    at: str | None,
    repeat: int | None,
    every: str | None,
    until: str | None,
    for_: str | None,
) -> SendSpec:
    """Parse and cross-check the flags (``IB-CLI-001`` for a bad value or a combination that
    contradicts itself or means nothing); nothing here reads the clock."""
    if until is not None and for_ is not None:
        raise _usage("--until and --for both end the series; give one")
    if every is None:
        if until is not None or for_ is not None:
            which = "--until" if until is not None else "--for"
            raise _usage(
                f"{which} ends a periodic series and needs --every; "
                "to send a number of packets back to back use --repeat N"
            )
    elif repeat is None and until is None and for_ is None:
        raise _usage(
            "--every needs an end so a series cannot run unbounded by accident: "
            "add --repeat N, --for DURATION or --until TIME"
        )
    if repeat is not None:
        if not 1 <= repeat <= MAX_SENDS:
            raise _usage(f"--repeat {repeat} is out of range: use 1 to {MAX_SENDS}")
        if repeat == 1 and every is not None:
            raise _usage("--repeat 1 sends once, so --every has no effect; drop one of them")
    every_ns = None if every is None else parse_duration(every, "--every", MAX_EVERY_NS)
    if every_ns is not None and every_ns < MIN_EVERY_NS:
        raise _usage(
            f"--every {clean(every or '', 40)} is out of range: use "
            f"{fmt_ns(MIN_EVERY_NS)} to {fmt_ns(MAX_EVERY_NS)}"
        )
    for_ns = None if for_ is None else parse_duration(for_, "--for")
    if for_ns is not None:
        assert every_ns is not None, "checked above: --for needs --every"
        if for_ns <= every_ns:
            raise _usage(
                f"--for {clean(for_ or '', 40)} is not longer than --every "
                f"{clean(every or '', 40)}, so at most one packet would be sent; "
                "check the units"
            )
    spec = SendSpec(
        at=None if at is None else parse_time_arg(at, "--at"),
        repeat=repeat,
        every_ns=every_ns,
        until=None if until is None else parse_time_arg(until, "--until"),
        for_ns=for_ns,
    )
    _check_count(spec, for_ns)
    return spec


def slots_in(window_ns: int, every_ns: int) -> int:
    """Slots in a half-open window: those at ``k*every`` below ``window``."""
    assert every_ns > 0
    return -(-window_ns // every_ns) if window_ns > 0 else 0


def _check_count(spec: SendSpec, window_ns: int | None) -> None:
    if spec.every_ns is None or window_ns is None:
        return
    count = slots_in(window_ns, spec.every_ns)
    if spec.repeat is None and count > MAX_SENDS:
        raise _usage(f"that is {count} packets; the limit is {MAX_SENDS}")


@dataclass(frozen=True, slots=True)
class SendPlan:
    """A series ready to run: the spec checked against the clock.

    ``start_wall_ns`` None means start as soon as the link is open. The window is ``for_ns``,
    or the time from the start to ``until_wall_ns``; None means only ``repeat`` ends it.
    """

    start_wall_ns: int | None
    repeat: int | None
    every_ns: int | None
    for_ns: int | None
    until_wall_ns: int | None
    assumed_utc: bool  # a time without a zone was read as UTC

    def window_ns(self, start_wall_ns: int) -> int | None:
        if self.for_ns is not None:
            return self.for_ns
        if self.until_wall_ns is not None:
            return self.until_wall_ns - start_wall_ns
        return None

    def planned(self, start_wall_ns: int) -> int:
        """Packets the series is meant to send, if no slot is skipped."""
        if self.every_ns is None:
            return 1 if self.repeat is None else self.repeat
        window = self.window_ns(start_wall_ns)
        slots = None if window is None else slots_in(window, self.every_ns)
        if self.repeat is None:
            assert slots is not None, "a periodic series is bounded"
            return slots
        return self.repeat if slots is None else min(self.repeat, slots)

    @property
    def mode(self) -> str:
        if self.every_ns is not None:
            return "every"
        return "repeat" if self.repeat is not None else "at"


def resolve(spec: SendSpec, anchor_wall_ns: int, now_wall_ns: int) -> SendPlan:
    """Check ``spec`` against the clock. Offsets count from ``anchor_wall_ns`` (when ``ib``
    started); a start must be after ``now_wall_ns``. ``IB-CLI-001`` otherwise."""
    start = None if spec.at is None else spec.at.resolve(anchor_wall_ns)
    if start is not None:
        if start <= now_wall_ns:
            raise _usage(
                f"--at {iso_utc(start)} is in the past by {fmt_ns(now_wall_ns - start)}; "
                f"it is {iso_utc(now_wall_ns)} now (UTC)"
            )
        if start - now_wall_ns > MAX_LEAD_NS:
            raise _usage(f"--at {iso_utc(start)} is more than a year away; check the date")
    until = None if spec.until is None else spec.until.resolve(anchor_wall_ns)
    if until is not None:
        begin = now_wall_ns if start is None else start
        assert spec.every_ns is not None, "checked by parse_spec: --until needs --every"
        if until <= begin:
            raise _usage(f"--until {iso_utc(until)} is not after the start {iso_utc(begin)} (UTC)")
        if until - begin > MAX_LEAD_NS:
            raise _usage(f"--until {iso_utc(until)} is more than a year after the start")
        if until - begin <= spec.every_ns:
            raise _usage(
                f"--until is only {fmt_ns(until - begin)} after the start, not longer than "
                f"--every {fmt_ns(spec.every_ns)}, so at most one packet would be sent"
            )
        _check_count(spec, until - begin)
    assumed = any(t is not None and t.assumed_utc for t in (spec.at, spec.until))
    return SendPlan(start, spec.repeat, spec.every_ns, spec.for_ns, until, assumed)


# -- the slots of a running series ------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Grid:
    """The slots of a series on the monotonic clock, fixed when it starts.

    ``every`` None is a burst: every packet's slot is the start, and the next one goes as soon
    as the previous one left. ``end_mono`` closes a periodic window; ``cap`` counts packets.
    """

    start_mono: int
    start_wall: int
    every: int | None
    end_mono: int | None
    cap: int | None
    at: bool  # the start is an --at time

    def __post_init__(self) -> None:
        assert self.every is None or self.every > 0
        assert self.every is not None or self.cap is not None, "a burst is counted"
        assert self.cap is None or self.cap >= 1
        assert self.cap is not None or self.end_mono is not None, "a series is bounded"

    def deadline(self, slot: int) -> int:
        return self.start_mono + slot * (self.every or 0)

    def slot_wall_ns(self, slot: int) -> int:
        """The wall-clock time of ``slot``: the time a packet sent in it carries."""
        return self.start_wall + slot * (self.every or 0)

    @property
    def total_slots(self) -> int | None:
        if self.every is None or self.end_mono is None:
            return None
        return slots_in(self.end_mono - self.start_mono, self.every)

    def in_window(self, slot: int) -> bool:
        total = self.total_slots
        return total is None or slot < total

    def nearest(self, now_mono: int) -> int:
        """The first slot not more than half a period before ``now_mono``."""
        assert self.every is not None
        return max(-(-(2 * (now_mono - self.start_mono) - self.every) // (2 * self.every)), 0)

    def too_late(self, slot: int, now_mono: int) -> bool:
        """Whether a send of ``slot`` starting at ``now_mono`` is more than half a period late."""
        return self.every is not None and 2 * (now_mono - self.deadline(slot)) > self.every

    def lost(self, slot: int, to: int) -> int:
        """Slots of the window from ``slot`` up to (not including) ``to``."""
        total = self.total_slots
        return max((to if total is None else min(to, total)) - slot, 0)

    @property
    def planned(self) -> int:
        total = self.total_slots
        if total is None:
            assert self.cap is not None
            return self.cap
        return total if self.cap is None else min(self.cap, total)


async def wait_for_wall(clock: Clock, target_wall_ns: int) -> int:
    """Wait until the wall clock is within :data:`WALL_FINE_NS` of ``target_wall_ns``; returns
    that instant on the monotonic clock, which is in the past if the target already is. Sleeps
    in slices that re-read the wall clock, so a step of it during a long wait is followed."""
    while True:
        remaining = target_wall_ns - clock.wall_ns()
        if remaining <= WALL_FINE_NS:
            return clock.mono_ns() + remaining
        await clock.sleep(min((remaining - WALL_FINE_NS // 2) / NS, WALL_SLICE_S))


async def start_grid(plan: SendPlan, clock: Clock) -> Grid:
    """Wait for the start of ``plan`` (to within a second; the pacer waits out the rest) and fix
    its slots. ``IB-CLI-001``, with nothing sent, when the link opened too late for the window
    to hold what the plan was accepted for (see the module's "The start")."""
    every = plan.every_ns
    cap = plan.repeat if plan.repeat is not None else (1 if every is None else None)
    if plan.start_wall_ns is not None:
        start_wall = plan.start_wall_ns
        start_mono = await wait_for_wall(clock, start_wall)
    else:
        start_wall = clock.wall_ns() + START_LEAD_NS
        start_mono = clock.mono_ns() + START_LEAD_NS
    window = plan.window_ns(start_wall)
    if (
        every is not None
        and plan.start_wall_ns is None
        and plan.until_wall_ns is not None
        and window is not None
        and window <= every
    ):
        raise _usage(
            f"--until {iso_utc(plan.until_wall_ns)} leaves {fmt_ns(max(window, 0))} once the "
            f"link is open, not longer than --every {fmt_ns(every)}: nothing was sent; give a "
            "later --until, or --for"
        )
    grid = Grid(
        start_mono,
        start_wall,
        every,
        None if window is None else start_mono + window,
        cap,
        plan.start_wall_ns is not None,
    )
    if every is not None and not grid.in_window(grid.nearest(clock.mono_ns())):
        raise _usage(
            "the window of the series passed while the link opened: nothing was sent; "
            "give a later --at"
        )
    return grid


# -- the account of a series ------------------------------------------------------------------


@dataclass(slots=True)
class Tally:
    """What a series did so far; filled in from the pacer's reports, read by the caller at any
    end."""

    planned: int
    sent: int = 0
    skipped: int = 0  # slots with no packet sent: late, or no packet built in time
    starved: int = 0  # of the skipped: no packet was built in time
    discarded: int = 0  # packets built for a slot that was then skipped (their counts unused)
    unsent: int = 0  # packets built ahead and never sent when the series ended
    overruns: int = 0  # send calls that took longer than the period
    late_count: int = 0
    late_sum_ns: int = 0
    late_max_ns: int = 0
    send_max_ns: int = 0
    start_late_ns: int | None = None  # --at: from the start to the return of the first send
    first_mono_ns: int = 0  # the first slot's instant
    last_mono_ns: int = 0  # when the last send returned
    first_sequence: int | None = None
    last_sequence: int | None = None
    pacer: dict[str, Any] = field(default_factory=dict)  # how the pacer ran (timer slack, spin)

    @property
    def late_mean_ns(self) -> int:
        return self.late_sum_ns // self.late_count if self.late_count else 0

    @property
    def elapsed_ns(self) -> int:
        return max(self.last_mono_ns - self.first_mono_ns, 0)

    def note_late(self, late_ns: int) -> None:
        self.late_count += 1
        self.late_sum_ns += max(late_ns, 0)
        self.late_max_ns = max(self.late_max_ns, late_ns)


def plan_fields(plan: SendPlan, planned: int) -> dict[str, Any]:
    """The ledger fields of the ``send.plan`` record."""
    return {
        "mode": plan.mode,
        "start_utc": None if plan.start_wall_ns is None else iso_utc(plan.start_wall_ns),
        "repeat": plan.repeat,
        "every_ns": plan.every_ns,
        "for_ns": plan.for_ns,
        "until_utc": None if plan.until_wall_ns is None else iso_utc(plan.until_wall_ns),
        "zone_assumed_utc": plan.assumed_utc,
        "planned": planned,
    }


def outcome_fields(tally: Tally, outcome: str) -> dict[str, Any]:
    """The ledger fields of the ``send.outcome`` record."""
    return {
        "outcome": outcome,
        "planned": tally.planned,
        "sent": tally.sent,
        "skipped": tally.skipped,
        "starved": tally.starved,
        "discarded": tally.discarded,
        "unsent": tally.unsent,
        "overruns": tally.overruns,
        "late_max_ns": tally.late_max_ns,
        "late_mean_ns": tally.late_mean_ns,
        "start_late_ns": tally.start_late_ns,
        "send_max_ns": tally.send_max_ns,
        "elapsed_ns": tally.elapsed_ns,
        "first_sequence": tally.first_sequence,
        "last_sequence": tally.last_sequence,
        "pacer": dict(tally.pacer),
    }


def timing_text(tally: Tally, every_ns: int | None) -> str:
    """``skipped 0, lateness max 0.31 ms mean 0.05 ms`` for a periodic series, and how late an
    ``--at`` start was; else ``""``."""
    parts: list[str] = []
    if tally.start_late_ns is not None:
        parts.append(f"first packet {fmt_ns(tally.start_late_ns)} after --at")
    if every_ns is not None:
        text = f"skipped {tally.skipped}"
        if tally.starved:
            text += f" ({tally.starved} with no packet ready)"
        if tally.late_count:
            text += f", lateness max {fmt_ns(tally.late_max_ns)} mean {fmt_ns(tally.late_mean_ns)}"
        parts.append(text)
    if tally.overruns:
        parts.append(f"{tally.overruns} send(s) took longer than the period")
    return ", ".join(parts)
