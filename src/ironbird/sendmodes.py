"""Send modes of ``ib send``: at a time, repeated, periodic.

The flags and how they combine (every refusal is ``IB-CLI-001``, exit 2, before anything is
opened or sent):

- ``--at T`` starts the series at T. T is an ISO 8601 time with seconds (``2026-11-04T12:00:00Z``,
  ``...+02:00``; a time with no zone is read as **UTC**, never as local time) or an offset from
  the moment the command was typed (``+10s``, ``+1m30s``, ``+250ms``). It must be in the future.
- ``--repeat N`` sends N packets. With no ``--every`` they go out back to back (a burst: the
  next one as soon as the previous send returned).
- ``--every D`` sends one packet per period D (``1ms``, ``250ms``, ``1.5s``, ``2m``; units
  ``ms s m h``, no bare numbers: ``5`` could be anything). It needs an end: ``--repeat N`` (a
  cap on the packets), ``--for D`` (a duration from the first send) or ``--until T`` (a time,
  as for ``--at``), whichever of those is given, the first to be reached ends the series.
  ``--until`` and ``--for`` together contradict each other. Both mean nothing without
  ``--every``. The window ``[start, end)`` is half open: a packet is sent at ``start + k*D``
  while that is before the end, so ``--every 1s --for 3s`` sends at 0, 1 and 2 s.
- Refused as meaningless: a window no longer than the period (one packet at most), ``--repeat 1``
  with ``--every`` (the period is never used), a period under :data:`MIN_EVERY_NS`, a series of
  more than :data:`MAX_SENDS` packets, a start more than a year ahead, an end not after the start.

Timing. One process, one open link and one codec serve the whole series. Deadlines are absolute
points of the monotonic clock (``start + k*D``), so a late send never moves the later ones and
no error accumulates. A wall-clock ``--at`` is converted to the monotonic clock one second
before it is due, so a clock step in a long wait is followed and one in the last second is not.

Overruns. When a send is not ready for its slot until more than half a period late, the slot is
**skipped**: counted and ledgered, and the series carries on at the slot nearest to now. The
alternative, catching up, sends the missed packets back to back, which turns a slow link or
vehicle into a burst and puts packets at times no one asked for; a test of a cadence wants the
cadence kept, and wants to be told how often it was not. A skipped slot does not count toward
``--repeat`` (N packets are sent), but is lost from a ``--for``/``--until`` window.

Lateness is the monotonic time between a slot's deadline and the start of its send call (the
packet reaches the socket a few tens of microseconds later); the series reports the maximum and
the mean. The wait for a deadline sleeps coarsely and then yields in a tight loop for the last
:data:`SPIN_NS`, because the event loop's sleep rounds up to a millisecond: a period under a few
milliseconds keeps one core busy.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final, Protocol

from ironbird.errors import Code, IronbirdError
from ironbird.filter.diag import FilterError
from ironbird.filter.lexer import parse_time
from ironbird.sanitize import clean

NS: Final = 1_000_000_000
MIN_EVERY_NS: Final = 1_000_000  # 1 ms: the finest period the event loop can keep
MAX_EVERY_NS: Final = 30 * 86_400 * NS
MAX_LEAD_NS: Final = 366 * 86_400 * NS  # a start further ahead than this is a typo
MAX_SENDS: Final = 100_000_000
SPIN_NS: Final = 1_500_000  # the last stretch of a wait is a yield loop, not a sleep
WALL_FINE_NS: Final = NS  # within this of a wall-clock start, switch to the monotonic clock
WALL_SLICE_S: Final = 30.0  # the longest sleep while waiting for a wall-clock start

_UNIT_NS: Final = {"ms": 1_000_000, "s": NS, "m": 60 * NS, "h": 3600 * NS}
_DURATION_RE: Final = re.compile(r"(?:[0-9]{1,9}(?:\.[0-9]{1,9})?(?:ms|s|m|h))+")
_PART_RE: Final = re.compile(r"([0-9]{1,9}(?:\.[0-9]{1,9})?)(ms|s|m|h)")
_ZONE_RE: Final = re.compile(r"(?:Z|[+-][0-9]{2}(?::?[0-9]{2})?)$")
DURATION_FORMS: Final = "a number with a unit, such as 250ms, 1.5s, 2m or 1m30s (units ms s m h)"
TIME_FORMS: Final = (
    "a UTC time such as 2026-11-04T12:00:00Z (a time without a zone is read as UTC; "
    "2026-11-04T14:00:00+02:00 names another), or an offset from now such as +10s or +1m30s"
)


class Clock(Protocol):
    """The three things the series needs from time; tests supply a simulated one."""

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


def parse_duration(text: str, option: str) -> int:
    """``250ms``, ``1.5s``, ``1m30s`` as nanoseconds; ``IB-CLI-001`` for anything else."""
    if _DURATION_RE.fullmatch(text) is None:
        raise _usage(f"{option} {clean(text, 40)!r} is not a duration: use {DURATION_FORMS}")
    total = Decimal(0)
    for number, unit in _PART_RE.findall(text):
        total += Decimal(number) * _UNIT_NS[unit]
    return int(total)


def iso_utc(wall_ns: int) -> str:
    """``2026-11-04T12:00:00.250Z`` for nanoseconds since the Unix epoch."""
    stamp = datetime.fromtimestamp(wall_ns // NS, UTC)
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
    """An ISO 8601 time (no zone means UTC) or ``+offset``; ``IB-CLI-001`` otherwise."""
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
    every_ns = None if every is None else parse_duration(every, "--every")
    if every_ns is not None and not MIN_EVERY_NS <= every_ns <= MAX_EVERY_NS:
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


def _slots(window_ns: int, every_ns: int) -> int:
    """Packets in a half-open window: those at ``k*every`` below ``window``."""
    return -(-window_ns // every_ns)


def _check_count(spec: SendSpec, window_ns: int | None) -> None:
    if spec.every_ns is None or window_ns is None:
        return
    count = _slots(window_ns, spec.every_ns)
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
        slots = None if window is None else _slots(max(window, 0), self.every_ns)
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
    """Check ``spec`` against the clock. Offsets count from ``anchor_wall_ns`` (when the command
    was typed); a start must be after ``now_wall_ns``. ``IB-CLI-001`` otherwise."""
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
        if until - begin <= spec.every_ns:
            raise _usage(
                f"--until is only {fmt_ns(until - begin)} after the start, not longer than "
                f"--every {fmt_ns(spec.every_ns)}, so at most one packet would be sent"
            )
        _check_count(spec, until - begin)
    assumed = any(t is not None and t.assumed_utc for t in (spec.at, spec.until))
    return SendPlan(start, spec.repeat, spec.every_ns, spec.for_ns, until, assumed)


# -- running a series ------------------------------------------------------------------------


@dataclass(slots=True)
class Tally:
    """What a series did so far; filled in by :func:`execute`, read by the caller at any end."""

    planned: int
    sent: int = 0
    skipped: int = 0
    overruns: int = 0  # sends that took longer than the period
    late_count: int = 0
    late_sum_ns: int = 0
    late_max_ns: int = 0
    send_max_ns: int = 0
    inflight: bool = False  # a send was begun and has not returned
    first_mono_ns: int = 0  # the first slot's instant
    last_mono_ns: int = 0  # when the last send returned

    @property
    def late_mean_ns(self) -> int:
        return self.late_sum_ns // self.late_count if self.late_count else 0

    @property
    def elapsed_ns(self) -> int:
        return max(self.last_mono_ns - self.first_mono_ns, 0)

    def note_late(self, late_ns: int) -> None:
        assert late_ns >= 0
        self.late_count += 1
        self.late_sum_ns += late_ns
        self.late_max_ns = max(self.late_max_ns, late_ns)


Send = Callable[[], Awaitable[object]]
Progress = Callable[[Tally], None]


def _ceil_div(num: int, den: int) -> int:
    return -(-num // den)


async def wait_for_wall(clock: Clock, target_wall_ns: int) -> int:
    """Wait until the wall clock reads ``target_wall_ns``; returns that instant on the monotonic
    clock. Sleeps in slices that re-read the wall clock (a step of it during a long wait is
    followed), then fixes the deadline on the monotonic clock for the last second."""
    while True:
        remaining = target_wall_ns - clock.wall_ns()
        if remaining <= WALL_FINE_NS:
            return clock.mono_ns() + max(remaining, 0)
        await clock.sleep(min((remaining - WALL_FINE_NS // 2) / NS, WALL_SLICE_S))


async def sleep_until(clock: Clock, deadline_mono_ns: int) -> None:
    """Return at the monotonic ``deadline_mono_ns``: a sleep for all but the last
    :data:`SPIN_NS` (the loop's timers round up to a millisecond), then a yield loop."""
    while True:
        remaining = deadline_mono_ns - clock.mono_ns()
        if remaining <= 0:
            return
        if remaining > SPIN_NS:
            await clock.sleep((remaining - SPIN_NS) / NS)
        else:
            await clock.sleep(0)


async def execute(
    plan: SendPlan, clock: Clock, send: Send, tally: Tally, progress: Progress | None = None
) -> None:
    """Run the series: wait for the start, then call ``send`` for each slot.

    ``tally`` is updated as it goes, so the caller can report it however this ends (finished,
    cancelled between or during a send, a send that raised - which ends the series: the link
    that failed once will fail again, and a series with holes in it is worse than a stop).
    """
    if plan.start_wall_ns is not None:
        start_wall = plan.start_wall_ns
        start_mono = await wait_for_wall(clock, start_wall)
    else:
        start_wall, start_mono = clock.wall_ns(), clock.mono_ns()
    tally.first_mono_ns = tally.last_mono_ns = start_mono
    every = plan.every_ns
    window = plan.window_ns(start_wall)
    end_mono = None if window is None else start_mono + window
    total_slots = None if (every is None or window is None) else _slots(window, every)
    cap = plan.repeat if plan.repeat is not None else (1 if every is None else None)
    tally.planned = plan.planned(start_wall)
    slot = 0
    while cap is None or tally.sent < cap:
        if every is not None:
            deadline = start_mono + slot * every
            if end_mono is not None and deadline >= end_mono:
                return
            await sleep_until(clock, deadline)
            now = clock.mono_ns()
            late = now - deadline
            if 2 * late > every:  # more than half a period behind: this slot is lost
                nearest = _ceil_div(2 * (now - start_mono) - every, 2 * every)
                assert nearest > slot, "a slot more than half late is passed over"
                lost_to = nearest if total_slots is None else min(nearest, total_slots)
                tally.skipped += max(lost_to - slot, 0)
                slot = nearest
                continue
            tally.note_late(late)
        began = clock.mono_ns()
        tally.inflight = True
        await send()
        tally.inflight = False
        done = clock.mono_ns()
        tally.sent += 1
        tally.last_mono_ns = done
        tally.send_max_ns = max(tally.send_max_ns, done - began)
        if every is not None:
            if done - began > every:
                tally.overruns += 1
            slot += 1
        else:
            await clock.sleep(0)  # a burst still lets the loop see a signal
        if progress is not None:
            progress(tally)


# -- reporting -------------------------------------------------------------------------------


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
        "overruns": tally.overruns,
        "late_max_ns": tally.late_max_ns,
        "late_mean_ns": tally.late_mean_ns,
        "send_max_ns": tally.send_max_ns,
        "elapsed_ns": tally.elapsed_ns,
    }


def timing_text(tally: Tally, every_ns: int | None) -> str:
    """``skipped 0, lateness max 0.31 ms mean 0.05 ms`` for a periodic series, else ``""``."""
    if every_ns is None:
        return ""
    text = f"skipped {tally.skipped}"
    if tally.late_count:
        text += f", lateness max {fmt_ns(tally.late_max_ns)} mean {fmt_ns(tally.late_mean_ns)}"
    if tally.overruns:
        text += f", {tally.overruns} send(s) took longer than the period"
    return text
