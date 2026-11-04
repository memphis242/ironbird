"""``ib send`` modes (--at, --repeat, --every with --until/--for) and --raw-enum.

The schedule is tested on simulated time (exact, no sleeping): the pacer thread's slot logic run
synchronously over packets built ahead. The pacer is tested with its real thread on short
periods, and the command end to end against a UDP sink on loopback (counts and timing with
tolerances meant for a loaded CI host; the timing targets are in tests/bench/test_send_timing.py).
Nothing here asserts on wording: codes, exit statuses, counts and ledger fields only.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import math
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pytest

from ironbird import engine as eng
from ironbird import pacer as pm
from ironbird import sendmodes as sm
from ironbird.config import Endpoint
from ironbird.errors import Code, IronbirdError
from ironbird.ledger import TxSeries
from ironbird.transport.udp import SEND_WAIT_S, UdpThreadSender, UdpTransport
from recutil import CLI_MAIN, EXAMPLES, Proc, run_ib
from replayutil import child_env, write_config_58k

MS = 1_000_000
SEC = 1_000_000_000


# -- the schedule on simulated time -----------------------------------------------------------


class FakeClock:
    """Wall and monotonic time that move only when something sleeps (the wait for a start)."""

    def __init__(self, wall_ns: int = 1_800_000_000 * SEC) -> None:
        self.mono = 5 * SEC
        self.offset = wall_ns - self.mono
        self.sleeps = 0

    def wall_ns(self) -> int:
        return self.mono + self.offset

    def mono_ns(self) -> int:
        return self.mono

    async def sleep(self, seconds: float) -> None:
        self.sleeps += seconds > 0.01
        self.mono += max(round(seconds * SEC), 1_000)


class FakeWaiter:
    """The pacer thread's time on the same simulated clock: a wait reaches its deadline."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock

    def setup(self) -> dict[str, Any]:
        return {}

    def mono_ns(self) -> int:
        return self.clock.mono

    def wait_until(self, deadline_ns: int, stop: threading.Event) -> bool:
        self.clock.mono = max(self.clock.mono, deadline_ns)
        return True


class _NoLoop:
    def call_soon_threadsafe(self, *_: object) -> None:
        pass


Report = pm.Report[int]


def pace(
    grid: sm.Grid,
    clock: FakeClock,
    cost: int | Callable[[int], int] = 0,
    *,
    timed: bool = False,
    slots: list[int] | None = None,
) -> tuple[list[Report], list[int]]:
    """Run the pacer thread's slot logic synchronously over packets built ahead (one per slot,
    or for ``slots``); ``cost(n)`` is the nanoseconds the n-th send takes. Returns the reports
    and the monotonic instant each send began."""
    began: list[int] = []

    def send(data: bytes, stop: threading.Event) -> bool:
        n = len(began)
        began.append(clock.mono)
        clock.mono += cost(n) if callable(cost) else cost
        return True

    pacer: pm.SeriesPacer[int] = pm.SeriesPacer(grid, send, FakeWaiter(clock), timed=timed)
    pacer._loop = _NoLoop()  # type: ignore[assignment]
    wanted = slots if slots is not None else list(range(grid.planned + 64))
    for number, slot in enumerate(wanted):
        pacer._queue.append(pm.Built(number, slot, b"", number))
    pacer._pace_slots()
    return list(pacer._reports), began


def plan_for(clock: FakeClock, **flags: Any) -> sm.SendPlan:
    spec = sm.parse_spec(
        flags.get("at"),
        flags.get("repeat"),
        flags.get("every"),
        flags.get("until"),
        flags.get("for_"),
    )
    return sm.resolve(spec, clock.wall_ns(), clock.wall_ns())


def grid_for(clock: FakeClock, opening_ns: int = 0, **flags: Any) -> sm.Grid:
    """The grid of the flags, after a link open of ``opening_ns``."""
    plan = plan_for(clock, **flags)
    clock.mono += opening_ns
    return asyncio.run(sm.start_grid(plan, clock))


def count(reports: list[Report], kind: pm.Kind) -> int:
    return sum(r.slots if kind is pm.Kind.SKIPPED else 1 for r in reports if r.kind is kind)


def lateness(reports: list[Report]) -> list[int]:
    return [r.late_ns for r in reports if r.kind is pm.Kind.SENT and r.late_ns is not None]


def test_periodic_sends_sit_on_an_absolute_grid_without_drift() -> None:
    clock = FakeClock()
    grid = grid_for(clock, every="10ms", for_="1s")
    reports, began = pace(grid, clock, cost=100_000)
    assert count(reports, pm.Kind.SENT) == 100 == grid.planned
    assert count(reports, pm.Kind.SKIPPED) == 0
    assert {b - a for a, b in itertools.pairwise(began)} == {10 * MS}  # whatever a send costs
    assert set(lateness(reports)) == {100_000}  # measured when the send returned


def test_window_is_half_open_and_repeat_caps_it() -> None:
    clock = FakeClock()
    reports, _ = pace(grid_for(clock, every="1s", for_="3s"), clock)
    assert count(reports, pm.Kind.SENT) == 3  # at 0, 1 and 2 s, not at 3 s
    clock = FakeClock()
    grid = grid_for(clock, every="1s", for_="10s", repeat=4)
    reports, _ = pace(grid, clock)
    assert count(reports, pm.Kind.SENT) == 4 == grid.planned
    clock = FakeClock()
    reports, _ = pace(grid_for(clock, every="1s", repeat=3), clock)
    assert count(reports, pm.Kind.SENT) == 3


def test_until_ends_at_an_absolute_time() -> None:
    clock = FakeClock()
    end = sm.iso_utc(clock.wall_ns() + 2 * SEC + 500 * MS)
    reports, _ = pace(grid_for(clock, every="1s", until=end), clock)
    assert count(reports, pm.Kind.SENT) == 3  # 0, 1 and 2 s are before the end


def test_a_send_that_overruns_skips_slots_instead_of_catching_up() -> None:
    clock = FakeClock()
    # the 5th send (slot 4) takes 35 ms: slots 5 and 6 are lost, slot 7 is reached 5 ms late
    # (half a period) and is sent
    grid = grid_for(clock, every="10ms", for_="200ms")
    reports, began = pace(grid, clock, cost=lambda n: 35 * MS if n == 4 else 0)
    assert count(reports, pm.Kind.SKIPPED) == 2 and count(reports, pm.Kind.SENT) == 20 - 2
    assert min(b - a for a, b in itertools.pairwise(began)) >= 5 * MS  # never a burst
    assert max(lateness(reports)) == 35 * MS  # the overrunning send itself
    assert sorted(lateness(reports))[-2] <= 5 * MS


def test_a_slow_send_never_puts_the_next_one_closer_than_half_a_period() -> None:
    clock = FakeClock()
    grid = grid_for(clock, every="10ms", for_="200ms")
    _, began = pace(grid, clock, cost=lambda n: 14 * MS if n == 5 else 20_000)
    assert min(b - a for a, b in itertools.pairwise(began)) >= 5 * MS


def test_skipped_slots_do_not_count_toward_repeat() -> None:
    clock = FakeClock()
    grid = grid_for(clock, every="10ms", repeat=10)
    reports, _ = pace(grid, clock, cost=lambda n: 35 * MS if n == 2 else 0)
    assert count(reports, pm.Kind.SENT) == 10 and count(reports, pm.Kind.SKIPPED) == 2


def test_a_stall_at_the_end_of_the_window_counts_only_slots_inside_it() -> None:
    clock = FakeClock()
    grid = grid_for(clock, every="10ms", for_="100ms")
    reports, _ = pace(grid, clock, cost=lambda n: 500 * MS if n == 8 else 0)
    assert count(reports, pm.Kind.SENT) == 9 and count(reports, pm.Kind.SKIPPED) == 1


def test_repeat_alone_is_a_burst_and_at_alone_is_one_packet() -> None:
    clock = FakeClock()
    reports, began = pace(grid_for(clock, repeat=5), clock, cost=1_000)
    assert count(reports, pm.Kind.SENT) == 5 and began == sorted(began)
    assert len(lateness(reports)) == 1  # only the first packet has a slot
    clock = FakeClock()
    grid = grid_for(clock, at="+3s")
    reports, _ = pace(grid, clock)
    assert count(reports, pm.Kind.SENT) == 1 == grid.planned


@pytest.mark.parametrize(
    "flags",
    [
        {"at": "+3s"},
        {"at": "+0.9s"},
        {"at": "+10s", "repeat": 3},
        {"at": "+3s", "every": "100ms", "repeat": 3},
    ],
)
def test_an_at_start_is_never_sent_early(flags: dict[str, Any]) -> None:
    clock = FakeClock()
    start_wall = clock.wall_ns() + sm.parse_time_arg(flags["at"], "--at").resolve(0)
    grid = grid_for(clock, **flags)
    reports, began = pace(grid, clock)
    assert began and began[0] >= grid.start_mono
    assert began[0] + clock.offset >= start_wall  # on the wall clock too
    assert lateness(reports)[0] >= 0


def test_an_at_start_waits_for_the_wall_clock_in_few_slices() -> None:
    clock = FakeClock()
    start_wall = clock.wall_ns() + 90 * SEC
    grid = grid_for(clock, at=sm.iso_utc(start_wall), every="1s", repeat=3)
    assert clock.sleeps < 20 and grid.start_mono + clock.offset == start_wall // MS * MS
    _, began = pace(grid, clock)
    assert [b - began[0] for b in began] == [0, SEC, 2 * SEC]


def test_an_at_the_link_open_missed_stays_on_its_grid_and_reports_the_slots() -> None:
    clock = FakeClock()
    grid = grid_for(clock, 3 * SEC, at="+2s", every="100ms", repeat=5)
    reports, began = pace(grid, clock)
    assert count(reports, pm.Kind.SKIPPED) == 10  # the slots of the second the open overran
    assert count(reports, pm.Kind.SENT) == 5
    assert all((b - grid.start_mono) % (100 * MS) == 0 for b in began)  # on T's grid
    clock = FakeClock()
    grid = grid_for(clock, 3 * SEC, at="+2s")
    reports, _ = pace(grid, clock)
    assert lateness(reports) == [SEC]  # sent late, and says how late


def test_an_until_window_the_open_shrank_is_refused_or_smaller() -> None:
    clock = FakeClock()
    assert grid_for(clock, 1_500 * MS, every="1s", until="+3s").planned == 2
    for flags, opening in (
        ({"every": "1s", "until": "+3s"}, 3_500 * MS),
        ({"at": "+1s", "every": "100ms", "until": "+2s"}, 3 * SEC),
        ({"at": "+1s", "every": "100ms", "for_": "1s"}, 3 * SEC),
    ):
        with pytest.raises(IronbirdError) as info:
            grid_for(FakeClock(), opening, **flags)
        assert info.value.code is Code.CLI_USAGE


def test_a_timed_packet_of_a_skipped_slot_is_discarded_and_others_are_not() -> None:
    clock = FakeClock()
    grid = grid_for(clock, every="10ms", for_="100ms")
    reports, _ = pace(grid, clock, cost=lambda n: 26 * MS if n == 2 else 0, timed=True)
    assert count(reports, pm.Kind.SKIPPED) == 2 and count(reports, pm.Kind.DISCARDED) == 2
    sent = [r.built.slot for r in reports if r.kind is pm.Kind.SENT and r.built is not None]
    assert sent == [0, 1, 2, 5, 6, 7, 8, 9]  # each in the slot it carries the time of
    clock = FakeClock()
    grid = grid_for(clock, every="10ms", for_="100ms")
    reports, _ = pace(grid, clock, cost=lambda n: 26 * MS if n == 2 else 0)
    numbers = [r.built.number for r in reports if r.kind is pm.Kind.SENT and r.built is not None]
    assert count(reports, pm.Kind.DISCARDED) == 0 and numbers == list(range(8))  # no gap


def test_a_slot_with_no_packet_built_is_skipped_as_starved() -> None:
    clock = FakeClock()
    grid = grid_for(clock, every="10ms", for_="50ms")
    reports, _ = pace(grid, clock, timed=True, slots=[0, 1, 3, 4])
    starved = [r for r in reports if r.kind is pm.Kind.SKIPPED]
    assert len(starved) == 1 and starved[0].starved and count(reports, pm.Kind.SENT) == 4


def test_starved_slots_in_a_row_are_reported_as_one_skip() -> None:
    clock = FakeClock()
    grid = grid_for(clock, every="10ms", for_="100ms")
    reports, _ = pace(grid, clock, timed=True, slots=[0, 6])  # slots 1-5 and 7-9 have no packet
    skips = [r for r in reports if r.kind is pm.Kind.SKIPPED]
    assert [r.slots for r in skips] == [5, 3] and all(r.starved for r in skips)
    assert count(reports, pm.Kind.SENT) == 2


class LoopedWaiter(FakeWaiter):
    """The pacer thread's time on the simulated clock, with the loop's timers in it: while the
    thread waits for a deadline, the loop builds what falls due, at the instants it asked for."""

    def __init__(self, clock: FakeClock, fill: Callable[[], float | None]) -> None:
        super().__init__(clock)
        self.fill = fill

    def wait_until(self, deadline_ns: int, stop: threading.Event) -> bool:
        while (delay := self.fill()) is not None:
            due = self.clock.mono + max(round(delay * SEC), 1)
            if due >= deadline_ns:
                break
            self.clock.mono = due
        self.clock.mono = max(self.clock.mono, deadline_ns)
        return True


def pace_with_loop(
    grid: sm.Grid, clock: FakeClock, *, timed: bool = False, fill_first: bool = True
) -> tuple[pm.SeriesPacer[int], list[int], list[Report]]:
    """Run the series with the loop interleaved at the worst points: its timers while the
    thread waits, right after the thread has taken a packet to send it, and once at the end.
    Returns the pacer, the slots the loop built for, and the reports."""
    slots: list[int] = []

    def build(number: int, slot: int) -> pm.Built[int]:
        slots.append(slot)
        return pm.Built(number, slot, b"", number)

    def fill() -> float | None:
        return pacer._fill(build)

    def send(data: bytes, stop: threading.Event) -> bool:
        fill()  # the loop runs while the packet is in flight
        return True

    pacer: pm.SeriesPacer[int] = pm.SeriesPacer(grid, send, LoopedWaiter(clock, fill), timed=timed)
    pacer._loop = _NoLoop()  # type: ignore[assignment]
    if fill_first:
        fill()  # the loop's first pass, before the thread has run
    pacer._pace_slots()
    fill()  # and after the thread has ended
    return pacer, slots, list(pacer._reports)


@pytest.mark.parametrize("timed", [False, True])
@pytest.mark.parametrize(
    "flags",
    [
        {"every": "10ms", "for_": "100ms"},
        {"every": "10ms", "until": "+1s"},
        {"every": "10ms", "for_": "100ms", "repeat": 7},
        {"every": "10ms", "repeat": 30},
    ],
)
def test_a_complete_series_builds_only_the_packets_it_sends(
    flags: dict[str, Any], timed: bool
) -> None:
    clock = FakeClock()
    grid = grid_for(clock, **flags)
    pacer, slots, reports = pace_with_loop(grid, clock, timed=timed)
    assert count(reports, pm.Kind.SENT) == grid.planned == len(slots)
    assert not pacer._queue  # no packet (and so no count) was built and left unsent
    assert count(reports, pm.Kind.SKIPPED) == count(reports, pm.Kind.DISCARDED) == 0


def test_the_slot_the_thread_takes_is_published_before_the_loop_can_look() -> None:
    """The last slot of a window: the loop runs between the thread's pop and the next pass of
    its loop, and must see the window used up."""
    clock = FakeClock()
    grid = grid_for(clock, every="10ms", for_="30ms")
    pacer, slots, reports = pace_with_loop(grid, clock)
    assert slots == [0, 1, 2] and not pacer._queue
    assert count(reports, pm.Kind.SENT) == 3


class _StepsAtTheSecondLengthRead(deque[pm.Built[int]]):
    """A packet queue that lets the pacer thread's step (publish the next slot, then pop) land
    at the second ``len`` of the loop's pass - the one that reads the queue's depth for a slot -
    just before it returns (``before``) or just after (``after``)."""

    def __init__(self, *, before: bool) -> None:
        super().__init__()
        self.before = before
        self.calls = 0
        self.step: Callable[[], None] = lambda: None

    def __len__(self) -> int:
        self.calls += 1
        fire = self.calls == 2
        if fire and self.before:
            self.step()
        size = super().__len__()
        if fire and not self.before:
            self.step()
        return size


@pytest.mark.parametrize("before", [True, False])
def test_a_thread_step_between_the_loops_reads_of_queue_and_slot_never_builds_one_too_many(
    before: bool,
) -> None:
    """Steady state at the end of a window: the thread waits for slot 8 and the packets for slots
    8 and 9 are queued, so nothing is left to build. The loop reads the queue's depth and the
    slot the thread publishes without a lock; the thread publishes slot 9 and pops the packet of
    slot 8 at the worst point between those two reads, whichever one the loop makes first
    (``before``: the step lands before the depth is read; else right after). The loop may build
    late, never a packet no slot is left for."""
    clock = FakeClock()
    grid = grid_for(clock, every="5ms", for_="50ms")
    assert grid.planned == 10
    built: list[int] = []

    def build(number: int, slot: int) -> pm.Built[int]:
        built.append(slot)
        return pm.Built(number, slot, b"", number)

    pacer: pm.SeriesPacer[int] = pm.SeriesPacer(
        grid, lambda d, s: True, FakeWaiter(clock), timed=False
    )
    queue = _StepsAtTheSecondLengthRead(before=before)
    pacer._queue = queue
    pacer._slot = 8
    clock.mono = grid.deadline(8)
    for slot in (8, 9):
        queue.append(pm.Built(slot, slot, b"", slot))
    pacer._built, pacer._next_slot = 2, 10

    def step() -> None:
        pacer._slot += 1
        queue.popleft()

    queue.step = step
    pacer._fill(build)
    assert queue.calls >= 2 and pacer._slot == 9  # the step did land inside the pass
    assert built == [] and [b.slot for b in queue] == [9]


def test_a_timed_series_whose_start_was_missed_builds_nothing_for_slots_already_past() -> None:
    clock = FakeClock()
    clock.mono += 3 * SEC
    grid = sm.Grid(clock.mono - SEC, clock.wall_ns() - SEC, 5 * MS, clock.mono + SEC, None, True)
    now = clock.mono
    built: list[int] = []

    def build(number: int, slot: int) -> pm.Built[int]:
        built.append(slot)
        return pm.Built(number, slot, b"", number)

    pacer: pm.SeriesPacer[int] = pm.SeriesPacer(
        grid, lambda d, s: True, FakeWaiter(clock), timed=True
    )
    pacer._loop = _NoLoop()  # type: ignore[assignment]
    pacer._fill(build)  # the loop's first pass: the thread has not published a slot yet
    assert built and min(built) == grid.nearest(now) > 0
    assert not any(grid.too_late(slot, now) for slot in built)
    assert len(built) <= pm.AHEAD_NS // (5 * MS) + 1  # no further than the build-ahead
    pacer._pace_slots()
    assert count(list(pacer._reports), pm.Kind.DISCARDED) == 0  # none built for a past slot


def test_a_timed_series_missing_its_start_sends_the_first_packet_it_built() -> None:
    clock = FakeClock()
    clock.mono += 3 * SEC
    grid = sm.Grid(clock.mono - SEC, clock.wall_ns() - SEC, 5 * MS, clock.mono + SEC, None, True)
    now = clock.mono
    _, slots, reports = pace_with_loop(grid, clock, timed=True)
    sent = [r.built for r in reports if r.kind is pm.Kind.SENT and r.built is not None]
    assert sent and sent[0].number == 0 and count(reports, pm.Kind.DISCARDED) == 0
    assert slots[0] == grid.nearest(now)


def test_a_tally_counts_a_packet_that_left_and_then_failed_like_any_other() -> None:
    tally = sm.Tally(planned=5)
    tally.count_sent(10, 100)
    tally.count_sent(11, 200)  # the second left and its send then failed
    assert (tally.sent, tally.first_sequence, tally.last_sequence, tally.last_mono_ns) == (
        2,
        10,
        11,
        200,
    )


# -- the pacer with its thread ----------------------------------------------------------------


def run_pacer(
    grid: sm.Grid,
    *,
    cancel_after_s: float | None = None,
    abort_after_s: float | None = None,
) -> tuple[list[Report], int, BaseException | None]:
    """Run a real pacer over ``grid`` with a send that only counts; returns the reports, the
    packets built, and what the run raised."""
    reports: list[Report] = []
    built: list[int] = []
    wire: list[int] = []

    def send(data: bytes, stop: threading.Event) -> bool:
        wire.append(len(data))
        return True

    def build(number: int, slot: int) -> pm.Built[int]:
        built.append(number)
        return pm.Built(number, slot, b"x", number)

    def handle(report: Report) -> BaseException | None:
        reports.append(report)
        return report.error

    async def main() -> BaseException | None:
        pacer: pm.SeriesPacer[int] = pm.SeriesPacer(
            grid, send, pm.ThreadWaiter(pm.SPIN_NS), timed=False
        )
        task = asyncio.ensure_future(pacer.run(build, handle))
        loop = asyncio.get_running_loop()
        if cancel_after_s is not None:
            loop.call_later(cancel_after_s, task.cancel)
        if abort_after_s is not None:
            refusal = IronbirdError(Code.TX_SEND, "refused")
            loop.call_later(abort_after_s, pacer.abort, refusal)
        try:
            await task
        except BaseException as exc:
            return exc
        return None

    raised = asyncio.run(main())
    assert len(wire) == sum(r.kind is pm.Kind.SENT for r in reports)
    return reports, len(built), raised


def live_grid(every_ms: int, for_ms: int) -> sm.Grid:
    now = time.monotonic_ns() + 20 * MS
    return sm.Grid(now, time.time_ns(), every_ms * MS, now + for_ms * MS, None, False)


def burst_grid(packets: int) -> sm.Grid:
    """Back to back, so no slot can be skipped on a loaded host: every send is made."""
    start = time.monotonic_ns() + 20 * MS
    return sm.Grid(start, time.time_ns(), None, None, packets, False)


def test_the_pacer_thread_runs_a_series_and_accounts_for_every_packet() -> None:
    reports, built, raised = run_pacer(live_grid(5, 300))
    assert raised is None
    sent = count(reports, pm.Kind.SENT)
    assert sent >= 1 and sent + count(reports, pm.Kind.SKIPPED) == 60  # a loaded host skips
    assert sent + count(reports, pm.Kind.UNSENT) == built  # nothing built is unaccounted for
    # (that nothing is built that cannot be sent is a property of the slot logic, tested on
    # simulated time: an overloaded host skips slots, and a skip can leave a packet unsent)


def test_a_packet_whose_count_is_awaited_is_built_again_once_the_builder_wakes_the_pacer() -> None:
    """A build that returns nothing builds and counts no packet; the pacer builds the same slot
    when woken, and every packet it then sent has a number (no gap, none twice)."""
    refused: list[int] = []
    numbers: list[int] = []
    reports: list[Report] = []
    wire: list[bytes] = []

    def send(data: bytes, stop: threading.Event) -> bool:
        wire.append(data)
        return True

    async def main() -> None:
        loop = asyncio.get_running_loop()
        pacer: pm.SeriesPacer[int] = pm.SeriesPacer(
            burst_grid(6), send, pm.ThreadWaiter(pm.SPIN_NS), timed=False
        )

        def build(number: int, slot: int) -> pm.Built[int] | None:
            if number in (0, 3) and refused.count(number) == 0:  # its count is being taken
                refused.append(number)
                loop.call_later(0.02, pacer.wake)
                return None
            numbers.append(number)
            return pm.Built(number, slot, bytes([number]), number)

        def handle(report: Report) -> BaseException | None:
            reports.append(report)
            return report.error

        await pacer.run(build, handle)

    asyncio.run(main())
    assert refused == [0, 3] and numbers == [0, 1, 2, 3, 4, 5]
    assert wire == [bytes([n]) for n in range(6)]
    assert count(reports, pm.Kind.SENT) == 6


def test_a_cancelled_pacer_stops_its_thread_and_reports_what_it_sent() -> None:
    reports, built, raised = run_pacer(live_grid(5, 60_000), cancel_after_s=0.2)
    assert isinstance(raised, asyncio.CancelledError)
    assert reports and not any(r.kind is pm.Kind.DONE for r in reports)  # handled internally
    assert count(reports, pm.Kind.SENT) + count(reports, pm.Kind.UNSENT) == built
    assert count(reports, pm.Kind.SENT) >= 5
    assert not pacer_threads()


def test_a_refusal_found_on_the_loop_ends_the_series_with_it() -> None:
    reports, built, raised = run_pacer(live_grid(5, 60_000), abort_after_s=0.2)
    assert isinstance(raised, IronbirdError) and raised.code is Code.TX_SEND
    assert count(reports, pm.Kind.SENT) + count(reports, pm.Kind.UNSENT) == built


def test_a_refusal_that_arrives_after_its_send_is_reported_not_cleared() -> None:
    """A real link's ICMP refusal arrives one round trip after the send returned; the next
    send of the series must not wipe it (it is reported as an earlier packet's)."""

    async def main() -> None:
        live = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        live.bind(("127.0.0.1", 0))
        transport = UdpTransport(remote=Endpoint("127.0.0.1", live.getsockname()[1]))
        await transport.open(None)
        sender = await transport.thread_sender()
        assert sender is not None
        try:
            sender.send(b"\x18\x90\xc0\x00\x00\x01\x00\xb6", threading.Event())
            refused = ConnectionRefusedError(111, "Connection refused")
            asyncio.get_running_loop().call_later(
                0.01, setattr, transport, "_pending_error", refused
            )
            await asyncio.sleep(0.05)
            sender.send(b"\x18\x90\xc0\x01\x00\x01\x00\xb6", threading.Event())
            with pytest.raises(IronbirdError) as info:
                transport.raise_earlier()
            assert info.value.code is Code.TX_SEND
        finally:
            sender.close()
            await transport.close()
            live.close()

    asyncio.run(main())


def pacer_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == "ib-pacer"]


def run_series(
    grid: sm.Grid,
    send: Callable[[bytes, threading.Event], bool],
    *,
    handle: Callable[[Report], BaseException | None] | None = None,
    cancel_after_s: float | None = None,
    until: Callable[[], bool] | None = None,
    late: Callable[[Report], None] | None = None,
) -> tuple[list[Report], list[int], BaseException | None]:
    """Run a real pacer over ``grid`` with ``send``; the series is cancelled after
    ``cancel_after_s``, or once ``until()`` holds. ``late`` takes a failure of the pacer thread
    after it was abandoned. Returns the reports, the slots built for, and what the run raised."""
    reports: list[Report] = []
    built: list[int] = []

    def build(number: int, slot: int) -> pm.Built[int]:
        built.append(slot)
        return pm.Built(number, slot, b"x", number)

    def handled(report: Report) -> BaseException | None:
        reports.append(report)
        return handle(report) if handle is not None else report.error

    async def main() -> BaseException | None:
        pacer: pm.SeriesPacer[int] = pm.SeriesPacer(
            grid, send, pm.ThreadWaiter(0), timed=False, late=late
        )
        task = asyncio.ensure_future(pacer.run(build, handled))
        if cancel_after_s is not None:
            asyncio.get_running_loop().call_later(cancel_after_s, task.cancel)
        if until is not None:
            while not until():
                await asyncio.sleep(0.005)
            task.cancel()
        try:
            await task
        except BaseException as exc:
            return exc
        return None

    return reports, built, asyncio.run(main())


def test_the_pacer_thread_blocks_the_shutdown_signals() -> None:
    """A signal the kernel hands to a thread that does not block it never wakes the loop."""
    seen: list[set[int]] = []

    def send(data: bytes, stop: threading.Event) -> bool:
        blocked = signal.pthread_sigmask(signal.SIG_BLOCK, [])
        seen.append(
            {int(sig) for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP) if sig in blocked}
        )
        return True

    _, _, raised = run_series(burst_grid(5), send)
    assert raised is None and len(seen) == 5
    assert all(blocked == {signal.SIGINT, signal.SIGTERM, signal.SIGHUP} for blocked in seen)


def test_a_series_lowers_the_switch_interval_and_restores_it() -> None:
    before = sys.getswitchinterval()
    seen: list[float] = []

    def send(data: bytes, stop: threading.Event) -> bool:
        seen.append(sys.getswitchinterval())
        return True

    _, _, raised = run_series(burst_grid(5), send)
    assert raised is None and len(seen) == 5 and max(seen) <= pm.SWITCH_INTERVAL_S * 1.01
    assert sys.getswitchinterval() == before
    boom = IronbirdError(Code.TX_SEND, "refused")

    def failing(data: bytes, stop: threading.Event) -> bool:
        raise boom

    _, _, raised = run_series(burst_grid(5), failing)
    assert raised is boom and sys.getswitchinterval() == before  # also when the series fails


def test_overlapping_series_share_one_lowering_and_the_last_one_restores_the_interval() -> None:
    """The switch interval is process-wide: a series that ends while another runs must leave it
    lowered for the other, and the last must restore what it was before the first."""
    before = sys.getswitchinterval()
    first_done = threading.Event()
    seen: list[float] = []

    def second_send(data: bytes, stop: threading.Event) -> bool:
        first_done.wait(10)  # the first series has ended by the time this packet is sent
        seen.append(sys.getswitchinterval())
        return True

    def build(number: int, slot: int) -> pm.Built[int]:
        return pm.Built(number, slot, b"x", number)

    async def main() -> None:
        first: pm.SeriesPacer[int] = pm.SeriesPacer(
            burst_grid(3), lambda d, s: True, pm.ThreadWaiter(0), timed=False
        )
        second: pm.SeriesPacer[int] = pm.SeriesPacer(
            burst_grid(1), second_send, pm.ThreadWaiter(0), timed=False
        )
        later = asyncio.ensure_future(second.run(build, lambda r: r.error))
        while pm._SWITCH.users == 0:
            await asyncio.sleep(0.001)
        await first.run(build, lambda r: r.error)
        assert pm._SWITCH.users == 1 and sys.getswitchinterval() <= pm.SWITCH_INTERVAL_S * 1.01
        first_done.set()
        await later

    asyncio.run(main())
    assert seen and max(seen) <= pm.SWITCH_INTERVAL_S * 1.01
    assert pm._SWITCH.users == 0 and sys.getswitchinterval() == before


def test_a_series_never_raises_an_interval_that_is_already_lower() -> None:
    before = sys.getswitchinterval()
    sys.setswitchinterval(pm.SWITCH_INTERVAL_S / 2)
    lowered = sys.getswitchinterval()  # the interpreter rounds to whole microseconds
    try:
        seen: list[float] = []

        def send(data: bytes, stop: threading.Event) -> bool:
            seen.append(sys.getswitchinterval())
            return True

        run_series(burst_grid(2), send)
        assert max(seen) == pytest.approx(lowered, rel=0.02)  # not raised to the series' own
        assert sys.getswitchinterval() == pytest.approx(lowered, rel=0.02)
    finally:
        sys.setswitchinterval(before)


def test_a_pacer_thread_that_cannot_start_is_a_coded_resource_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = threading.Thread.start

    def start(self: threading.Thread) -> None:
        if self.name == "ib-pacer":
            raise RuntimeError("can't start new thread")
        real(self)

    monkeypatch.setattr(threading.Thread, "start", start)
    _, built, raised = run_series(live_grid(5, 500), lambda data, stop: True)
    assert isinstance(raised, IronbirdError) and raised.code is Code.SYS_RESOURCE
    assert not built and not pacer_threads()


def test_a_handler_that_raises_does_not_cut_the_wind_down_short() -> None:
    sends, handled_sent = [0], [0]
    unsent_raised = [False]

    def send(data: bytes, stop: threading.Event) -> bool:
        sends[0] += 1
        if sends[0] > 2:  # the third packet waits for the stop, and is put back unsent
            stop.wait(30)
            return False
        return True

    def handle(report: Report) -> BaseException | None:
        if report.kind is pm.Kind.SENT:
            handled_sent[0] += 1
            if handled_sent[0] == 2:  # the second is handled once the rest are built ahead
                raise AssertionError("bug while handling a sent packet")
        if report.kind is pm.Kind.UNSENT and not unsent_raised[0]:
            unsent_raised[0] = True
            raise AssertionError("bug while handling an unsent packet")
        return report.error

    reports, built, raised = run_series(burst_grid(1000), send, handle=handle)
    assert isinstance(raised, AssertionError) and not pacer_threads()
    assert any("unsent" in note for note in raised.__notes__)  # the second is attached
    sent, unsent = count(reports, pm.Kind.SENT), count(reports, pm.Kind.UNSENT)
    assert sent == 2 and unsent >= 2
    assert sent + unsent == len(built)  # every packet reached the handler, in spite of both raising


def test_an_abandoned_pacer_reports_the_packet_it_was_sending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pm, "DONE_TIMEOUT_S", 0.2)
    wire: list[int] = []
    release, blocked = threading.Event(), threading.Event()

    def send(data: bytes, stop: threading.Event) -> bool:
        if len(wire) == 3:
            blocked.set()
            release.wait(5)  # a send that does not look at ``stop``
        wire.append(len(wire))
        return True

    reports, built, raised = run_series(burst_grid(1000), send, until=blocked.is_set)
    release.set()
    assert isinstance(raised, asyncio.CancelledError)
    unconfirmed = count(reports, pm.Kind.UNCONFIRMED)
    sent, unsent = count(reports, pm.Kind.SENT), count(reports, pm.Kind.UNSENT)
    assert unconfirmed == 1 and sent == 3
    assert sent + unsent + unconfirmed == len(built)  # nothing built is unaccounted for
    for thread in pacer_threads():
        thread.join(5)
    assert not pacer_threads()
    assert len(wire) - sent == unconfirmed  # and the one more on the wire is the one reported


def test_an_abandoned_pacer_is_a_coded_error_and_its_late_failure_is_not_dropped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pm, "DONE_TIMEOUT_S", 0.2)
    wire: list[int] = []
    release, blocked = threading.Event(), threading.Event()
    lateness: list[Report] = []
    late_seen = threading.Event()
    refusal = OSError(5, "late refusal")

    def send(data: bytes, stop: threading.Event) -> bool:
        if len(wire) == 3:
            blocked.set()
            release.wait(5)  # a send that does not look at ``stop``
            raise refusal
        wire.append(len(wire))
        return True

    def late(report: Report) -> None:
        lateness.append(report)
        late_seen.set()

    reports, _, raised = run_series(burst_grid(1000), send, until=blocked.is_set, late=late)
    assert isinstance(raised, asyncio.CancelledError)
    kinds = [r.kind for r in reports]
    assert kinds.count(pm.Kind.ABANDONED) == 1 and not lateness  # nothing late has come yet
    abandoned = next(r for r in reports if r.kind is pm.Kind.ABANDONED)
    assert isinstance(abandoned.error, IronbirdError) and abandoned.error.code is Code.TX_SEND
    assert abandoned.built is not None and abandoned.built.number == 3  # the packet in flight
    assert kinds.index(pm.Kind.ABANDONED) < kinds.index(pm.Kind.UNCONFIRMED)
    release.set()
    assert late_seen.wait(5)
    for thread in pacer_threads():
        thread.join(5)
    assert not pacer_threads()
    (failure,) = lateness  # the thread's own failure reached the callback, once
    assert failure.kind is pm.Kind.FAILED and failure.error is refusal
    assert failure.built is not None and failure.built.number == 3
    assert not any(r.kind is pm.Kind.FAILED for r in reports)  # and not the series' account


class _Io:
    link = object()
    end = object()


class _HopEngine:
    """The engine's send, as a series sees it through the loop: packet ``stalls`` blocks (a
    serial line that stopped taking bytes); a cancel then finds its head on the line or not."""

    def __init__(self, stalls: int, *, head_out_on_cancel: bool) -> None:
        self.stalls = stalls
        self.head_out_on_cancel = head_out_on_cancel
        self.wire: list[bytes] = []
        self.blocked = threading.Event()

    async def send(
        self,
        link: object,
        end: object,
        data: bytes,
        *,
        handed_off: Callable[[], None],
        label: str = "",
    ) -> None:
        if self.blocked.is_set() or len(self.wire) != self.stalls:
            self.wire.append(data)
            handed_off()
            return
        self.blocked.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            if self.head_out_on_cancel:
                self.wire.append(data)
                handed_off()
            raise


@pytest.mark.parametrize("head_out", [True, False])
def test_a_send_through_the_loop_ends_with_the_series_and_nothing_is_lost(head_out: bool) -> None:
    """A serial send stalled on a slow line: the stop cancels it; whether the head of the
    packet is on the line decides if it was sent, and the account has every packet."""
    engine = _HopEngine(3, head_out_on_cancel=head_out)
    reports: list[Report] = []
    built: list[int] = []

    def build(number: int, slot: int) -> pm.Built[int]:
        built.append(slot)
        return pm.Built(number, slot, bytes([number]), number)

    def handle(report: Report) -> BaseException | None:
        reports.append(report)
        return report.error

    links: list[eng.SeriesLink] = []

    async def main() -> float:
        loop = asyncio.get_running_loop()
        link = eng.SeriesLink(cast(Any, engine), cast(Any, _Io()), cast(Any, object()), None, loop)
        links.append(link)
        pacer: pm.SeriesPacer[int] = pm.SeriesPacer(
            burst_grid(1000), link.send, pm.ThreadWaiter(0), timed=False
        )
        task = asyncio.ensure_future(pacer.run(build, handle))
        while not engine.blocked.is_set():
            await asyncio.sleep(0.005)
        started = time.monotonic()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return time.monotonic() - started

    elapsed = asyncio.run(main())
    sent, unsent = count(reports, pm.Kind.SENT), count(reports, pm.Kind.UNSENT)
    assert elapsed < pm.DONE_TIMEOUT_S / 2 and not pacer_threads()
    assert sent == len(engine.wire) == 3 + head_out  # what is on the line is what is sent
    cut = links[0].cut  # a packet the stop cut short with its head on the line is marked
    assert (cut == engine.wire[-1]) if head_out else cut is None
    assert sent + unsent == len(built)
    assert not count(reports, pm.Kind.UNCONFIRMED) and not count(reports, pm.Kind.FAILED)


def test_a_send_through_a_closed_loop_leaves_no_coroutine_unawaited() -> None:
    loop = asyncio.new_event_loop()
    engine = _HopEngine(99, head_out_on_cancel=False)
    link = eng.SeriesLink(cast(Any, engine), cast(Any, _Io()), cast(Any, object()), None, loop)
    loop.close()
    assert link.send(b"x", threading.Event()) is False and not engine.wire


class _FullThenRoom:
    """A socket whose send buffer is full for its first ``full`` sends."""

    def __init__(self, full: int, on_full: Callable[[], None] = lambda: None) -> None:
        self.left = full
        self.sent: list[bytes] = []
        self.probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # for poll: writable
        self.on_full = on_full

    def fileno(self) -> int:
        return self.probe.fileno()

    def send(self, data: bytes) -> int:
        if self.left:
            self.left -= 1
            self.on_full()
            raise BlockingIOError(11, "Resource temporarily unavailable")
        self.sent.append(data)
        return len(data)

    def close(self) -> None:
        self.probe.close()


def test_a_thread_send_on_a_full_buffer_waits_for_room_instead_of_failing() -> None:
    fake = _FullThenRoom(3)
    sender = UdpThreadSender(cast(Any, fake), Endpoint("127.0.0.1", 9))
    assert sender.send(b"abc", threading.Event()) is True
    assert fake.sent == [b"abc"]
    fake.close()


def test_a_thread_send_waiting_for_room_gives_up_at_stop_and_at_the_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = threading.Event()
    fake = _FullThenRoom(10**9, on_full=lambda: stop.set() if fake.left < 10**9 - 2 else None)
    sender = UdpThreadSender(cast(Any, fake), Endpoint("127.0.0.1", 9))
    assert sender.send(b"abc", stop) is False and not fake.sent  # stopped: nothing was sent
    monkeypatch.setattr("ironbird.transport.udp.SEND_WAIT_S", 0.1)
    full = _FullThenRoom(10**9)
    with pytest.raises(BlockingIOError):  # the buffer never drains: the caller codes it
        UdpThreadSender(cast(Any, full), Endpoint("127.0.0.1", 9)).send(b"abc", threading.Event())
    fake.close()
    full.close()


def test_a_full_send_buffer_after_the_wait_is_a_coded_error_that_says_what_to_do() -> None:
    transport = UdpTransport(remote=Endpoint("127.0.0.1", 9))
    error = transport.thread_send_error(BlockingIOError(11, "Resource temporarily unavailable"))
    assert error.code is Code.TX_SEND
    assert "\n" not in error.summary
    assert f"{SEND_WAIT_S:g} s" in error.summary  # it waited, and says for how long
    assert error.summary.count(";") == 1  # and what failed is followed by what to do


# -- the ledger records of a series -----------------------------------------------------------


class FakeLedger:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, Any]]] = []

    def record_tx(self, event: str, subject: str, target: str = "", **fields: Any) -> None:
        self.records.append((event, fields))


def test_a_long_series_keeps_the_first_packets_and_counts_the_rest_in_windows() -> None:
    ledger = FakeLedger()
    now = [0.0]
    series = TxSeries(ledger, full=5, window_s=10.0, clock=lambda: now[0])  # type: ignore[arg-type]
    for n in range(1000):
        now[0] = n * 0.1
        result = "IB-TX-002" if n == 500 else ("unsent" if n == 999 else "sent")
        series.record("NOOP", "fc", result=result, sent=result == "sent", size=8, sequence=n)
    series.close()
    full = [f for e, f in ledger.records if e == "tx.command"]
    windows = [f for e, f in ledger.records if e == "tx.command.window"]
    assert len(full) == 5 + 1  # the first five, and the failure
    assert full[-1]["result"] == "IB-TX-002"
    assert 9 <= len(windows) <= 11  # one per 10 s of 100 s
    assert sum(w["sent"] for w in windows) + 5 == series.totals["sent"] == 998
    assert sum(w["packets"] for w in windows) == 1000 - 6
    assert windows[0]["first_seq"] == 5 and windows[-1]["last_seq"] == 999


# -- the flags ---------------------------------------------------------------------------------


def usage_error(**flags: Any) -> IronbirdError:
    clock = FakeClock()
    with pytest.raises(IronbirdError) as info:
        plan_for(clock, **flags)
    assert info.value.code is Code.CLI_USAGE and info.value.exit_code == 2
    return info.value


@pytest.mark.parametrize(
    "flags",
    [
        {"every": "1s"},  # unbounded
        {"until": "+10s"},  # no --every
        {"for_": "10s"},  # no --every
        {"every": "1s", "until": "+10s", "for_": "10s"},  # two ends
        {"every": "1s", "repeat": 1},  # the period is never used
        {"every": "10s", "for_": "5s"},  # at most one packet
        {"every": "100us", "repeat": 5},  # finer than the pacer is meant for
        {"every": "5", "repeat": 5},  # a bare number has no unit
        {"every": "1x", "repeat": 5},
        {"every": "-1s", "repeat": 5},
        {"every": "1s1s", "repeat": 5},  # a unit twice
        {"every": "30s1m", "repeat": 5},  # units out of order
        {"every": "1ms1h", "repeat": 5},
        {"every": "", "repeat": 5},
        {"repeat": 0},
        {"repeat": sm.MAX_SENDS + 1},
        {"every": "1ms", "for_": "1000h"},  # more packets than the limit
        {"every": "1s", "for_": "9000h"},  # longer than a year
        {"at": "yesterday"},
        {"at": "2000-01-01T00:00:00Z"},  # in the past
        {"at": "+0s"},
        {"at": "+70000000h"},  # past year 9999
        {"at": "+999999999h999999999h"},
        {"at": "+10s", "every": "1s", "until": "+5s"},  # ends before it starts
        {"at": "+10s", "every": "10s", "until": "+15s"},  # one packet at most
        {"at": "+9999h"},  # a typo, not a plan
        {"at": "2026-13-45T25:00:00Z"},
        {"every": "720h", "until": "9999-12-31T23:59:59Z"},  # an end years away
        {"every": "720h", "repeat": 2, "until": "+70000000h"},
    ],
)
def test_contradictory_or_meaningless_flags_are_coded_usage_errors(flags: dict[str, Any]) -> None:
    usage_error(**flags)


def test_times_without_a_zone_are_utc_and_the_plan_says_so() -> None:
    clock = FakeClock()
    future = clock.wall_ns() + 3600 * SEC
    bare = sm.iso_utc(future)[:-1]  # no Z
    plan = plan_for(clock, at=bare)
    assert plan.start_wall_ns == future // MS * MS and plan.assumed_utc
    assert sm.parse_time_arg(bare + "+02:00", "--at").assumed_utc is False
    zoned = sm.parse_time_arg(bare + "+02:00", "--at").absolute_ns
    assert zoned is not None and plan.start_wall_ns is not None
    assert zoned == plan.start_wall_ns - 7200 * SEC
    assert not plan_for(clock, at=sm.iso_utc(future)).assumed_utc
    assert plan_for(clock, at="+90s").start_wall_ns == clock.wall_ns() + 90 * SEC


def test_durations() -> None:
    assert sm.parse_duration("250ms", "--every") == 250 * MS
    assert sm.parse_duration("1m30s", "--every") == 90 * SEC
    assert sm.parse_duration("1.5s", "--every") == 1500 * MS
    assert sm.parse_duration("1h2m3s4ms", "--every") == (3723 * SEC) + 4 * MS


@pytest.mark.parametrize("wall_ns", [0, -1, 10**30, -(10**30), 253_402_300_800 * SEC])
def test_a_time_out_of_range_still_formats(wall_ns: int) -> None:
    assert sm.iso_utc(wall_ns)


# -- raw enum values: labels and hints --------------------------------------------------------


def _codec_with_enum_array() -> Any:
    """The mock codec with an enum on the elements of MOCK_LOAD_DATA's ``Entries`` (the JSON
    CCDD has no spelling for that today; the codec still names such values by element)."""
    from dataclasses import replace
    from types import MappingProxyType

    from ironbird.ccdd.json_adapter import parse_model
    from ironbird.ccdd.model import CcddModel
    from ironbird.codec import Codec
    from ironbird.header import load_profile

    model = parse_model(json.loads((EXAMPLES / "ccdd" / "mock.json").read_text()), "mock.json")
    enum = MappingProxyType({"A": 1, "B": 2})
    messages = tuple(
        replace(
            m,
            fields=tuple(replace(f, enum=enum) if f.name == "Entries" else f for f in m.fields),
        )
        if m.name == "MOCK_LOAD_DATA"
        else m
        for m in model.messages
    )
    return Codec(
        CcddModel.build(model.target, model.source, messages),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
    )


def test_raw_enum_values_of_an_array_are_named_by_element() -> None:
    codec = _codec_with_enum_array()
    packet = codec.encode_command(
        "MOCK_LOAD_DATA", {"Entries": [1, 9, 10], "Text": ""}, raw_enum=True
    )
    assert packet.raw_enums == (("Entries[1]", 9), ("Entries[2]", 10))


def test_the_codec_names_no_cli_flag_unless_its_caller_does() -> None:
    codec = _codec_with_enum_array()
    for hint in ("", "use --raw-enum"):
        with pytest.raises(IronbirdError) as info:
            codec.encode_command("MOCK_LOAD_DATA", {"Entries": [9], "Text": ""}, enum_hint=hint)
        assert info.value.code is Code.ENC_ARG
        assert ("--raw-enum" in info.value.summary) == bool(hint)


# -- ib send against a UDP sink ----------------------------------------------------------------


class Sink:
    """A UDP socket on the command port that timestamps what arrives."""

    def __init__(self, port: int) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 << 20)
        self.sock.bind(("127.0.0.1", port))
        self.sock.settimeout(0.05)
        self.packets: list[tuple[int, bytes]] = []
        self.walls: list[int] = []  # the wall-clock time each packet arrived
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                data, _ = self.sock.recvfrom(4096)
            except TimeoutError:
                continue
            except OSError:
                return
            self.walls.append(time.time_ns())
            self.packets.append((time.monotonic_ns(), data))

    @property
    def count(self) -> int:
        return len(self.packets)

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self.sock.close()

    def sequences(self) -> list[int]:
        return [int.from_bytes(data[2:4], "big") & 0x3FFF for _, data in self.packets]


@contextmanager
def sink_for(port: int) -> Generator[Sink]:
    sink = Sink(port)
    try:
        yield sink
    finally:
        sink.close()


@pytest.fixture
def setup(tmp_path: Path) -> tuple[Path, int, dict[str, str]]:
    config, command_port, _ = write_config_58k(tmp_path)
    return config, command_port, child_env(tmp_path)


def ledger_events(tmp_path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted((tmp_path / "state").glob("*.jsonl")):
        out += [json.loads(line) for line in path.read_text().splitlines()]
    return out


def ib_send(
    tmp_path: Path, config: Path, env: dict[str, str], *args: str
) -> tuple[Proc, list[str]]:
    argv = ["-c", str(config), "send", "fc", *args]
    return Proc(tmp_path, f"send{time.monotonic_ns()}", argv, env), argv


def consecutive(sequences: list[int]) -> bool:
    return all((b - a) % 16384 == 1 for a, b in itertools.pairwise(sequences))


def test_repeat_sends_n_packets_each_with_its_own_sequence(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        done = run_ib(
            tmp_path, ["-c", str(config), "send", "fc", "MOCK_NOOP", "--repeat", "7"], env
        )
        assert done.returncode == 0, done.stderr
        deadline = time.monotonic() + 5
        while sink.count < 7 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert sink.count == 7
        assert consecutive(sink.sequences())
    events = ledger_events(tmp_path)
    assert [e["event"] for e in events].count("tx.command") == 7
    plan = next(e for e in events if e["event"] == "send.plan")
    outcome = next(e for e in events if e["event"] == "send.outcome")
    assert plan["planned"] == 7 and plan["mode"] == "repeat"
    assert outcome["outcome"] == "complete" and outcome["sent"] == 7


def test_every_for_keeps_the_period_and_reports_lateness(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        done = run_ib(
            tmp_path,
            ["-c", str(config), "send", "fc", "MOCK_NOOP", "--every", "50ms", "--for", "2s"],
            env,
        )
        assert done.returncode == 0, done.stderr
        time.sleep(0.2)
        count = sink.count
        times = [t for t, _ in sink.packets]
    assert 1 <= count <= 40  # 40 planned; a loaded host may skip some, never add
    assert times == sorted(times)
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["sent"] == count and outcome["planned"] == 40
    assert outcome["sent"] + outcome["skipped"] == 40  # every slot is sent or counted as skipped
    assert 0 <= outcome["late_mean_ns"] <= outcome["late_max_ns"]
    if outcome["skipped"] == 0:  # only a skip can leave a built packet unsent
        assert outcome["unsent"] == 0 and outcome["totals"] == {"sent": count}


@pytest.mark.parametrize("extra", [[], ["--repeat", "3"]])
def test_at_sends_at_the_start_never_before_it(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]], extra: list[str]
) -> None:
    config, port, env = setup
    start = (math.ceil(time.time()) + 3) * SEC  # a whole second, absolute
    with sink_for(port) as sink:
        done = run_ib(
            tmp_path,
            ["-c", str(config), "send", "fc", "MOCK_NOOP", "--at", sm.iso_utc(start), *extra],
            env,
        )
        assert done.returncode == 0, done.stderr
        time.sleep(0.2)
        assert sink.count == (3 if extra else 1)
        assert sink.walls[0] >= start  # not a moment early
    events = ledger_events(tmp_path)
    plan = next(e for e in events if e["event"] == "send.plan")
    outcome = next(e for e in events if e["event"] == "send.outcome")
    assert plan["start_utc"].endswith("Z")
    assert 0 <= outcome["start_late_ns"] < 500 * MS


def test_usage_errors_exit_2_and_send_nothing(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    bad_flags = [
        ["--every", "1s"],
        ["--until", "+5s"],
        ["--every", "1s", "--for", "5s", "--until", "+9s"],
        ["--at", "2001-01-01T00:00:00Z"],
        ["--repeat", "1", "--every", "1s"],
        ["--at", "+70000000h"],
        ["--every", "720h", "--until", "9999-12-31T23:59:59Z"],
        ["--every", "1s1s", "--repeat", "2"],
    ]
    with sink_for(port) as sink:
        for flags in bad_flags:
            done = run_ib(tmp_path, ["-c", str(config), "send", "fc", "MOCK_NOOP", *flags], env)
            assert done.returncode == 2, (flags, done.stderr)
            assert Code.CLI_USAGE.value in done.stderr
        time.sleep(0.2)
        assert sink.count == 0
    assert not [e for e in ledger_events(tmp_path) if e["event"] == "tx.command"]


def test_a_bad_argument_is_refused_before_a_scheduled_start(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, _, env = setup
    started = time.monotonic()
    done = run_ib(
        tmp_path,
        ["-c", str(config), "send", "fc", "MOCK_SET_MODE", "mode=NOPE", "pad=0", "--at", "+30s"],
        env,
    )
    assert done.returncode != 0 and Code.ENC_ARG.value in done.stderr
    assert time.monotonic() - started < 15  # it did not wait for the start to find out


@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
def test_a_signal_stops_the_series_between_sends_with_a_summary(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]], sig: signal.Signals
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        proc, _ = ib_send(tmp_path, config, env, "MOCK_NOOP", "--every", "20ms", "--for", "60s")
        try:
            deadline = time.monotonic() + 20
            while sink.count < 5:
                assert proc.popen.poll() is None, proc.stderr
                assert time.monotonic() < deadline
                time.sleep(0.02)
            proc.popen.send_signal(sig)
            code = proc.finish(timeout=20)
        finally:
            proc.kill()
        assert code == 128 + int(sig), proc.stderr
        time.sleep(0.2)
        settled = sink.count
        time.sleep(0.3)
        assert sink.count == settled  # nothing after the process ended
    assert "Traceback" not in proc.stderr
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["outcome"] == "interrupted" and outcome["sent"] == settled  # exact
    assert consecutive(sink.sequences())


def test_a_signal_while_waiting_for_the_start_sends_nothing(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        proc, _ = ib_send(tmp_path, config, env, "MOCK_NOOP", "--at", "+60s")
        try:
            proc.wait_for("starting at")
            proc.popen.send_signal(signal.SIGINT)
            code = proc.finish(timeout=20)
        finally:
            proc.kill()
        assert code == 130
        assert sink.count == 0
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["outcome"] == "interrupted" and outcome["sent"] == 0


def test_a_transport_failure_mid_series_stops_it_with_the_coded_error(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    sink = Sink(port)
    proc, _ = ib_send(tmp_path, config, env, "MOCK_NOOP", "--every", "20ms", "--for", "60s")
    try:
        deadline = time.monotonic() + 20
        while sink.count < 5:
            assert proc.popen.poll() is None, proc.stderr
            assert time.monotonic() < deadline
            time.sleep(0.02)
        sink.close()  # the receiver goes away: the next sends are refused
        code = proc.finish(timeout=30)
    finally:
        proc.kill()
    assert code == 1, proc.stderr
    assert Code.TX_SEND.value in proc.stderr
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["outcome"] == "failed" and outcome["error"] == Code.TX_SEND.value
    assert outcome["sent"] >= 5


def test_a_dead_receiver_is_found_on_the_first_packet_of_a_fast_series(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, _, env = setup  # nothing listens
    done = run_ib(
        tmp_path,
        ["-c", str(config), "send", "fc", "MOCK_NOOP", "--every", "5ms", "--for", "30s"],
        env,
        timeout=20,
    )
    assert done.returncode == 1 and Code.TX_SEND.value in done.stderr


@pytest.mark.parametrize(
    "flags", [["--repeat", "2"], ["--at", "+0.5s"], ["--every", "1s", "--for", "3s"]]
)
def test_a_dead_receiver_is_found_for_any_series(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]], flags: list[str]
) -> None:
    config, _, env = setup  # nothing listens
    done = run_ib(tmp_path, ["-c", str(config), "send", "fc", "MOCK_NOOP", *flags], env)
    assert done.returncode == 1 and Code.TX_SEND.value in done.stderr
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["outcome"] == "failed" and outcome["error"] == Code.TX_SEND.value


def test_a_long_series_ledgers_its_first_packets_and_counts_the_rest(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        done = run_ib(
            tmp_path, ["-c", str(config), "send", "fc", "MOCK_NOOP", "--repeat", "700"], env
        )
        assert done.returncode == 0, done.stderr
        time.sleep(0.2)
        assert sink.count == 700
    events = ledger_events(tmp_path)
    full = [e for e in events if e["event"] == "tx.command"]
    windows = [e for e in events if e["event"] == "tx.command.window"]
    outcome = next(e for e in events if e["event"] == "send.outcome")
    assert len(full) == 600 and all(e["sent"] for e in full)
    assert sum(w["sent"] for w in windows) == 100
    assert outcome["sent"] == 700 == outcome["totals"]["sent"]
    assert outcome["last_sequence"] - outcome["first_sequence"] == 699


def test_one_millisecond_period_runs_and_accounts_for_every_slot(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    """Functional only: the timing targets are host-timing guards (tests/bench)."""
    config, port, env = setup
    with sink_for(port) as sink:
        done = run_ib(
            tmp_path,
            ["-c", str(config), "send", "fc", "MOCK_NOOP", "--every", "1ms", "--for", "1s"],
            env,
        )
        assert done.returncode == 0, done.stderr
        time.sleep(0.3)
        received = sink.count
        assert consecutive(sink.sequences())
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["planned"] == 1000 and outcome["sent"] > 0
    assert outcome["sent"] + outcome["skipped"] == 1000
    assert received == outcome["sent"]
    assert outcome["pacer"]["direct"] is True


# -- ib send: the end of a series, in a child process ----------------------------------------

#: Run before ``ib`` in a child: the session takes a while to close, as a slow ledger flush
#: would make it, so a signal can land after the series ended.
SLOW_CLOSE = (
    "import asyncio\nfrom ironbird import session\n"
    "_close = session.Session.__aexit__\n"
    "async def _slow(self, *a):\n    await asyncio.sleep(3)\n    return await _close(self, *a)\n"
    "session.Session.__aexit__ = _slow\n"
)
#: Run before ``ib`` in a child: no thread-safe send, so a series goes through the loop, and the
#: third packet's send fails after the packet left.
FAIL_AFTER_LEFT = (
    "from ironbird.transport import udp\n"
    "async def _no_thread_sender(self):\n    return None\n"
    "udp.UdpTransport.thread_sender = _no_thread_sender\n"
    "_send = udp.UdpTransport.send\n_n = [0]\n"
    "async def _failing(self, data, *, confirm=False, handed_off=None, label=''):\n"
    "    _n[0] += 1\n"
    "    await _send(self, data, confirm=confirm, handed_off=handed_off, label=label)\n"
    "    if _n[0] == 3:\n        raise OSError(5, 'fault injected after the packet left')\n"
    "udp.UdpTransport.send = _failing\n"
)


def child(
    tmp_path: Path, preface: str, args: list[str], env: dict[str, str]
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-c", preface + CLI_MAIN, *args],
        cwd=tmp_path,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def test_a_signal_after_the_series_ended_still_says_how_many_were_sent(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        proc = child(
            tmp_path,
            SLOW_CLOSE,
            ["-c", str(config), "send", "fc", "MOCK_NOOP", "--repeat", "5"],
            env,
        )
        assert proc.stdout is not None
        for line in proc.stdout:  # up to the summary: the series is over, the session closing
            if "complete" in line:
                break
        proc.send_signal(signal.SIGINT)
        _, err = proc.communicate(timeout=30)
        time.sleep(0.2)
        assert sink.count == 5
    assert proc.returncode == 128 + int(signal.SIGINT), err
    events = ledger_events(tmp_path)
    outcome = next(e for e in events if e["event"] == "send.outcome")
    notice = next(e for e in events if e["event"] == "cli.signal")
    assert outcome["outcome"] == "complete" and outcome["sent"] == 5
    assert notice["series_sent"] == 5 and notice["series_planned"] == 5
    assert notice["series_outcome"] == "complete"


def test_a_signal_notice_names_the_series_whatever_state_it_is_in() -> None:
    from ironbird.cli import main as cli

    run = cli._Run(console=cast(Any, None), verbose=False)
    for sent, outcome in ((0, ""), (3, ""), (5, "complete"), (2, "failed")):
        tally = sm.Tally(planned=5, sent=sent, outcome=outcome)
        run.series, run.series_command = tally, "MOCK_NOOP"
        notice = cli._interrupt_notice("send", int(signal.SIGINT), run)
        assert f"{sent} of 5" in notice  # the count of the series, not "nothing was built"
    for outcome in ("", "interrupted"):  # the series' end is the signal: it is said once
        run.series = sm.Tally(planned=5, sent=2, outcome=outcome)
        assert "series" not in cli._interrupt_notice("send", int(signal.SIGINT), run)
    for outcome in ("complete", "failed"):  # any other end is news
        run.series = sm.Tally(planned=5, sent=5, outcome=outcome)
        assert f"series {outcome}" in cli._interrupt_notice("send", int(signal.SIGINT), run)


def test_a_packet_that_left_and_then_failed_is_counted_as_sent(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        proc = child(
            tmp_path,
            FAIL_AFTER_LEFT,
            ["-c", str(config), "send", "fc", "MOCK_NOOP", "--repeat", "6"],
            env,
        )
        _, err = proc.communicate(timeout=30)
        time.sleep(0.2)
        wire = sink.count
    assert proc.returncode == 1 and Code.TX_SEND.value in err
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["outcome"] == "failed" and outcome["sent"] == wire == 3
    assert outcome["last_sequence"] - outcome["first_sequence"] == outcome["sent"] - 1
    assert outcome["pacer"]["direct"] is False


def test_a_thread_that_cannot_start_exits_with_the_resource_code(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    refuse = (
        "import threading\n_start = threading.Thread.start\n"
        "def _s(self):\n"
        "    if self.name == 'ib-pacer': raise RuntimeError(\"can't start new thread\")\n"
        "    return _start(self)\n"
        "threading.Thread.start = _s\n"
    )
    with sink_for(port) as sink:
        proc = child(
            tmp_path, refuse, ["-c", str(config), "send", "fc", "MOCK_NOOP", "--repeat", "3"], env
        )
        _, err = proc.communicate(timeout=30)
        assert sink.count == 0
    assert proc.returncode == 1 and Code.SYS_RESOURCE.value in err
    assert Code.INTERNAL.value not in err and "Traceback" not in err
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["outcome"] == "failed" and outcome["error"] == Code.SYS_RESOURCE.value


COUNT_MISMATCH = ["MOCK_LOAD_DATA", "Count=7", "Entries=1,2", "Text=x", "--allow-count-mismatch"]


def test_a_series_ledgers_the_faults_it_injects_once_like_a_single_send(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port):
        for extra in ([], ["--repeat", "3"]):  # one ledger: a record per invocation
            done = run_ib(tmp_path, ["-c", str(config), "send", "fc", *COUNT_MISMATCH, *extra], env)
            assert done.returncode == 0, done.stderr
    faults = [e for e in ledger_events(tmp_path) if e["event"] == "send.fault"]
    assert [f["fault"] for f in faults] == ["count_mismatch"] * 2  # not 1: not 6
    assert faults[0]["fields"] == faults[1]["fields"]
    assert faults[0]["fields"][0]["given"] != faults[0]["fields"][0]["implied"]


def test_a_series_ledgers_raw_enum_values_once(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port):
        done = run_ib(
            tmp_path,
            [
                *("-c", str(config), "send", "fc", "MOCK_SET_MODE", "mode=9", "pad=0"),
                *("--raw-enum", "--repeat", "3"),
            ],
            env,
        )
        assert done.returncode == 0, done.stderr
    faults = [e for e in ledger_events(tmp_path) if e["event"] == "send.fault"]
    assert [(f["fault"], f["fields"]) for f in faults] == [
        ("raw_enum", [{"field": "mode", "value": 9}])
    ]


def test_a_complete_untimed_window_leaves_no_packet_built_and_unsent(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        done = run_ib(
            tmp_path,
            ["-c", str(config), "send", "fc", "MOCK_NOOP", "--every", "5ms", "--for", "1s"],
            env,
        )
        assert done.returncode == 0, done.stderr
        time.sleep(0.2)
        wire = sink.count
        sequences = sink.sequences()
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["sent"] == wire and consecutive(sequences)
    if outcome["skipped"] == 0:  # a skip near the end can leave a built packet; nothing else
        assert outcome["unsent"] == 0
        assert outcome["last_sequence"] - outcome["first_sequence"] == wire - 1


def test_verbose_adds_detail_to_a_fast_series_without_a_line_for_every_packet(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port):
        done = run_ib(
            tmp_path,
            ["-c", str(config), "-v", "send", "fc", "MOCK_NOOP", "--every", "5ms", "--for", "1s"],
            env,
        )
    assert done.returncode == 0, done.stderr
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["sent"] > 40
    assert len(done.stdout.splitlines()) < outcome["sent"] / 4  # a sample, not the lot


def test_window_times_are_the_times_packets_were_sent() -> None:
    ledger = FakeLedger()
    series = TxSeries(ledger, full=0, window_s=60.0)  # type: ignore[arg-type]
    series.record("NOOP", "fc", result="sent", sent=True, size=8, sequence=1, at_ns=1_000)
    series.record("NOOP", "fc", result="sent", sent=True, size=8, sequence=2, at_ns=5_000)
    series.record("NOOP", "fc", result="unsent", sent=False, size=8, sequence=3)  # no send time
    series.close()
    window = next(f for e, f in ledger.records if e == "tx.command.window")
    assert window["first_ns"] == 1_000 and window["last_ns"] > 5_000  # the unsent: now
    series = TxSeries(ledger, full=0, window_s=60.0)  # type: ignore[arg-type]
    for n, at in enumerate((1_000, 9_000, 5_000)):
        series.record("NOOP", "fc", result="sent", sent=True, size=8, sequence=n, at_ns=at)
    series.close()
    window = [f for e, f in ledger.records if e == "tx.command.window"][-1]
    assert (window["first_ns"], window["last_ns"]) == (1_000, 5_000)


# -- --raw-enum --------------------------------------------------------------------------------


def test_raw_enum_is_refused_without_the_flag_and_sent_with_it(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    base = ["-c", str(config), "send", "fc", "MOCK_SET_MODE", "mode=9", "pad=0"]
    with sink_for(port) as sink:
        refused = run_ib(tmp_path, base, env)
        assert refused.returncode == 2 and Code.ENC_ARG.value in refused.stderr
        assert "--raw-enum" in refused.stderr  # the way to send it deliberately
        time.sleep(0.1)
        assert sink.count == 0

        sent = run_ib(tmp_path, [*base, "--raw-enum"], env)
        assert sent.returncode == 0, sent.stderr
        series = run_ib(tmp_path, [*base, "--raw-enum", "--repeat", "3"], env)
        assert series.returncode == 0, series.stderr
        deadline = time.monotonic() + 5
        while sink.count < 4 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert sink.count == 4
    events = ledger_events(tmp_path)
    faults = [e for e in events if e["event"] == "send.fault" and e["fault"] == "raw_enum"]
    assert faults and faults[0]["fields"] == [{"field": "mode", "value": 9}]
    plan = next(e for e in events if e["event"] == "send.plan")
    assert plan["request"]["raw_enums"] == [{"field": "mode", "value": 9}]


def test_raw_enum_with_nothing_to_allow_is_a_usage_error(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, _, env = setup
    done = run_ib(
        tmp_path,
        ["-c", str(config), "send", "fc", "MOCK_SET_MODE", "mode=SAFE", "pad=0", "--raw-enum"],
        env,
    )
    assert done.returncode == 2 and Code.CLI_USAGE.value in done.stderr


# -- sequence counts of a series: the state directory's, as for a single send (PR 40 with 41) --


def wait_for_packets(sink: Sink, wanted: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while sink.count < wanted and time.monotonic() < deadline:
        time.sleep(0.02)
    assert sink.count == wanted


def send_ok(tmp_path: Path, config: Path, env: dict[str, str], *args: str) -> None:
    done = run_ib(tmp_path, ["-c", str(config), "send", "fc", *args], env)
    assert done.returncode == 0, done.stderr


def test_a_series_continues_the_count_of_the_sends_before_it_and_the_next_send_goes_on(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    """B1 (PR 40 round 2): a series counted 0.. from the codec's memory whatever was sent before,
    and the single send after it reused its counts."""
    config, port, env = setup
    with sink_for(port) as sink:
        for _ in range(3):
            send_ok(tmp_path, config, env, "MOCK_NOOP")
        send_ok(tmp_path, config, env, "MOCK_NOOP", "--repeat", "5")
        send_ok(tmp_path, config, env, "MOCK_NOOP", "--every", "10ms", "--repeat", "4")
        send_ok(tmp_path, config, env, "MOCK_NOOP")
        wait_for_packets(sink, 13)
        assert sink.sequences() == list(range(13))
    spent = [e["sequence"] for e in ledger_events(tmp_path) if e["event"] == "tx.command"]
    assert sorted(spent) == list(range(13))


@pytest.mark.parametrize("mode", [["--repeat", "6"], ["--every", "10ms", "--repeat", "6"]])
def test_sequence_seats_the_first_packet_of_a_series_and_counts_on_from_it(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]], mode: list[str]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        send_ok(tmp_path, config, env, "MOCK_NOOP", "--sequence", "100", *mode)
        send_ok(tmp_path, config, env, "MOCK_NOOP")  # goes on above the series
        send_ok(tmp_path, config, env, "MOCK_NOOP", "--sequence", "16382", *mode)  # wraps
        send_ok(tmp_path, config, env, "MOCK_NOOP")
        wait_for_packets(sink, 6 + 1 + 6 + 1)
        assert sink.sequences() == [
            *range(100, 106),
            106,
            16382,
            16383,
            0,
            1,
            2,
            3,
            4,
        ]
    plan = next(e for e in ledger_events(tmp_path) if e["event"] == "send.plan")
    assert plan["request"]["sequence_set"] == 100


def test_a_sequence_that_does_not_fit_is_refused_before_a_series_starts(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        done = run_ib(
            tmp_path,
            ["-c", str(config), "send", "fc", "MOCK_NOOP", "--repeat", "3", "--sequence", "16384"],
            env,
        )
        assert done.returncode == 2 and "IB-" in done.stderr
        send_ok(tmp_path, config, env, "MOCK_NOOP")
        wait_for_packets(sink, 1)
        assert sink.sequences() == [0]  # the refused series took no count


@pytest.mark.parametrize(
    "mode", [["--every", "10ms", "--repeat", "3"], ["--at", "+30s"], ["--repeat", "3"]]
)
def test_a_length_delta_the_header_cannot_hold_is_refused_at_once_and_spends_no_count(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]], mode: list[str]
) -> None:
    """S4 (PR 40 round 3): the range of a tampered field was checked while framing, after the
    `--at` wait and with a count spent, so every refused series left a gap."""
    config, port, env = setup
    with sink_for(port) as sink:
        started = time.monotonic()
        done = run_ib(
            tmp_path,
            ["-c", str(config), "send", "fc", "MOCK_NOOP", "--length-delta", "-100", *mode],
            env,
        )
        assert done.returncode == 2 and "IB-HDR-003" in done.stderr, done.stderr
        assert time.monotonic() - started < 20  # not after a 30 s wait
        send_ok(tmp_path, config, env, "MOCK_NOOP")
        wait_for_packets(sink, 1)
        assert sink.sequences() == [0]  # the refused request took no count


def test_a_series_prepares_its_counts_before_it_waits_for_its_start(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    """S2 (PR 40 round 3): the store was opened, `--sequence` seated and the first count taken
    20 ms before the first slot, so another process's sync at that moment made the first packet
    late. The ceiling is raised while the series still waits for its start."""
    config, port, env = setup
    with sink_for(port) as sink:
        start = time.time() + 1.5
        done = run_ib(
            tmp_path,
            [
                "-c",
                str(config),
                "send",
                "fc",
                "MOCK_NOOP",
                "--at",
                sm.iso_utc(int(start * SEC)),
                "--every",
                "5ms",
                "--repeat",
                "6",
                "--sequence",
                "50",
            ],
            env,
        )
        assert done.returncode == 0, done.stderr
        wait_for_packets(sink, 6)
        assert sink.sequences() == list(range(50, 56))
    events = ledger_events(tmp_path)

    def at(event: dict[str, Any]) -> float:
        return datetime.fromisoformat(event["ts"]).timestamp()

    raised = [e for e in events if e["event"] == "sequence.extend"]
    assert raised and at(raised[0]) < start - 0.5, "the ceiling was raised at the start itself"


def test_two_series_at_once_to_one_node_and_mid_take_unique_counts_with_no_restart(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    """B1: two `ib send --repeat` processes counted from 0 each, in step, so every count was on
    the wire twice. Each process's packets now carry its own take order, and together they are
    one unbroken series."""
    config, port, env = setup
    each = 300
    with sink_for(port) as sink:
        procs = [
            ib_send(tmp_path, config, env, "MOCK_NOOP", "--repeat", str(each))[0] for _ in range(2)
        ]
        try:
            for proc in procs:
                assert proc.finish(timeout=60) == 0, proc.stderr
        finally:
            for proc in procs:
                proc.kill()
        wait_for_packets(sink, 2 * each)
        sequences = sink.sequences()
    assert sorted(sequences) == list(range(2 * each))  # unique; no restart at 0; no gap
    outcomes = [e for e in ledger_events(tmp_path) if e["event"] == "send.outcome"]
    assert [o["sent"] for o in outcomes] == [each, each]
    # a third invocation goes on above both
    with sink_for(port) as sink:
        send_ok(tmp_path, config, env, "MOCK_NOOP")
        wait_for_packets(sink, 1)
        assert sink.sequences() == [(2 * each) % 16384]


def test_two_periodic_series_at_once_interleave_without_a_repeated_count(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        procs = [
            ib_send(tmp_path, config, env, "MOCK_NOOP", "--every", "5ms", "--repeat", "60")[0]
            for _ in range(2)
        ]
        try:
            for proc in procs:
                assert proc.finish(timeout=60) == 0, proc.stderr
        finally:
            for proc in procs:
                proc.kill()
        deadline = time.monotonic() + 5
        while sink.count < 120 and time.monotonic() < deadline:
            time.sleep(0.02)
        sequences = sink.sequences()
    assert len(sequences) == len(set(sequences)) and min(sequences) == 0
    assert all(0 <= s < 130 for s in sequences)  # a skipped slot may leave a gap, never a repeat


def test_a_reset_during_a_series_restarts_its_counts_at_zero(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        proc, _ = ib_send(tmp_path, config, env, "MOCK_NOOP", "--every", "10ms", "--for", "2s")
        try:
            deadline = time.monotonic() + 20
            while sink.count < 30:
                assert proc.popen.poll() is None, proc.stderr
                assert time.monotonic() < deadline
                time.sleep(0.02)
            done = run_ib(tmp_path, ["-c", str(config), "seq", "reset", "fc"], env)
            assert done.returncode == 0, done.stderr
            assert proc.finish(timeout=30) == 0, proc.stderr
        finally:
            proc.kill()
        time.sleep(0.2)
        sequences = sink.sequences()
    drops = [i for i, (a, b) in enumerate(itertools.pairwise(sequences)) if b != (a + 1) % 16384]
    assert len(drops) == 1, sequences
    before, after = sequences[: drops[0] + 1], sequences[drops[0] + 1 :]
    assert before == list(range(len(before)))  # from 0 until the reset
    assert after[0] == 0 and consecutive(after)  # then from 0 again
    # and the series left the stream where it ended: above its last packet, by the counts it had
    # reserved and not used (fewer than a batch)
    with sink_for(port) as sink:
        send_ok(tmp_path, config, env, "MOCK_NOOP")
        wait_for_packets(sink, 1)
        (next_count,) = sink.sequences()
        assert 0 <= (next_count - after[-1] - 1) % 16384 < 32
