"""The pacer of a series of sends (``ib send --at/--repeat/--every``): a dedicated thread that
sleeps to each slot's absolute deadline and hands the packet to the link itself.

Why a thread. The event loop's timers have millisecond resolution, and its wake-up is delayed by
whatever else the loop is doing; a loop-driven series either spins (a core busy at a 1 ms
period) or is late by up to a millisecond plus the loop's backlog. The pacer thread instead:

- waits with ``threading.Event.wait`` on its stop event (a stop wakes it at once) until
  :data:`FINE_NS` before the deadline, then with ``time.sleep`` (``clock_nanosleep`` on
  ``CLOCK_MONOTONIC``, the GIL released) for the rest. It first lowers its own timer slack to
  :data:`TIMER_SLACK_NS` with ``prctl(PR_SET_TIMERSLACK)`` (unprivileged; by default the kernel
  lets a sleep overrun by 50 us). Where that is not available it runs with the default slack,
  and the series' ``send.outcome`` record says so (``pacer``);
- for a period under the configured threshold (``[send] spin_below_us``, off by default), and for
  the one wait of an ``--at`` start or a burst, ends the wait with a spin of :data:`SPIN_NS` that
  absorbs the thread's wake-up latency;
- hands the packet to the link from the thread (UDP: ``send`` on its own duplicate of the
  connected socket; a link with no thread-safe send is handed the packet through the loop),
  then measures the lateness right after the send call returns.

The loop builds the packets ahead into a bounded queue (each fully encoded, with its own count)
and handles what the pacer reports back (sent, skipped, failed): ledger records, progress lines
and errors stay on the loop, and the loop's own latency is never added to a packet. A packet is
built no earlier than :data:`AHEAD_NS` before its slot, so the ledger's transmit gate and the
time a packet carries are current. The pacer never blocks on the queue: a slot whose packet is
not there at its deadline is skipped (``starved``) and reported.

A packet whose header carries the send time (``time_seconds``/``time_subseconds``) is built for
one slot and carries that slot's time; when that slot is skipped it is discarded (its count goes
unused, and its ledger record says so). Any other packet is sent in the next slot instead.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import os
import sys
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, Generic, Protocol, TypeVar

from ironbird.sendmodes import NS, Grid

TIMER_SLACK_NS: Final = 1_000
PR_SET_TIMERSLACK: Final = 29
PR_GET_TIMERSLACK: Final = 30
FINE_NS: Final = 2_000_000  # the last stretch of a wait is a time.sleep; before it, abortable
SPIN_NS: Final = 75_000  # the final spin of a wait, for a period under the spin threshold
AHEAD_NS: Final = 20_000_000  # a packet is built no earlier than this before its slot
MAX_AHEAD: Final = 64  # packets built and waiting for their slots, at most
BURST_AHEAD: Final = 8  # ... in a back-to-back series
READY_POLL_S: Final = 0.1  # a burst's wait for its next packet re-checks the stop this often
DONE_TIMEOUT_S: Final = 5.0  # a stopped pacer that has not finished by then is abandoned

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Built(Generic[T]):
    """A packet ready for the pacer. ``item`` is the caller's, handed back in its report."""

    number: int  # 0-based, in the order built
    slot: int  # the slot it was built for (the time it carries, in a timed series)
    data: bytes
    item: T


class Kind(Enum):
    SENT = "sent"
    SKIPPED = "skipped"  # ``slots`` slots passed with nothing sent
    DISCARDED = "discarded"  # a timed packet whose slot was skipped
    UNSENT = "unsent"  # built, still queued when the series ended
    FAILED = "failed"  # the send raised (``error``); the pacer stops
    DONE = "done"  # the pacer thread is finished


@dataclass(frozen=True, slots=True)
class Report(Generic[T]):
    kind: Kind
    built: Built[T] | None = None
    slots: int = 0
    starved: bool = False  # SKIPPED: no packet was ready at the deadline
    late_ns: int | None = None  # SENT: from the slot's deadline to the return of the send
    took_ns: int = 0  # SENT: the send call
    mono_ns: int = 0  # SENT: when the send returned
    went_out: bool = False  # SENT, or FAILED after the packet left
    error: BaseException | None = None


class LeftThenFailed(Exception):
    """Raised by a thread send whose packet left and whose send then failed (``cause``)."""

    def __init__(self, cause: BaseException) -> None:
        super().__init__(str(cause))
        self.cause = cause


#: A send made in the pacer thread: True once the packet left, False when ``stop`` was set
#: before it did; it raises when the send failed (:class:`LeftThenFailed` once it had left).
ThreadSend = Callable[[bytes, threading.Event], bool]
#: Builds packet ``number`` for ``slot`` on the loop (``IronbirdError`` ends the series).
Build = Callable[[int, int], Built[T]]
#: Handles a report on the loop; for a FAILED one returns the error the series ends with.
Handle = Callable[[Report[T]], BaseException | None]


class Waiter(Protocol):
    """Time as the pacer thread sees it; tests supply a simulated one."""

    def setup(self) -> dict[str, Any]: ...

    def mono_ns(self) -> int: ...

    def wait_until(self, deadline_ns: int, stop: threading.Event) -> bool: ...


def lower_timer_slack(slack_ns: int) -> tuple[int | None, str]:
    """Set the calling thread's timer slack to ``slack_ns`` (Linux ``prctl``, unprivileged).
    Returns the slack in effect (None when unknown) and, when it could not be set, why."""
    if not sys.platform.startswith("linux"):
        return None, f"no PR_SET_TIMERSLACK on {sys.platform}"
    try:
        prctl = ctypes.CDLL(None, use_errno=True).prctl
    except (OSError, AttributeError) as exc:
        return None, f"prctl is not available: {exc}"
    prctl.restype = ctypes.c_int
    prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong]
    if prctl(PR_SET_TIMERSLACK, slack_ns, 0, 0, 0) != 0:
        return None, f"PR_SET_TIMERSLACK failed: {os.strerror(ctypes.get_errno())}"
    got = prctl(PR_GET_TIMERSLACK, 0, 0, 0, 0)
    return (got if got >= 0 else None), ""


class ThreadWaiter:
    """Sleeps the pacer thread to an absolute monotonic deadline (see the module)."""

    def __init__(self, spin_ns: int) -> None:
        assert 0 <= spin_ns <= FINE_NS
        self._spin_ns = spin_ns

    def setup(self) -> dict[str, Any]:
        slack, why = lower_timer_slack(TIMER_SLACK_NS)
        return {"timer_slack_ns": slack, "timer_slack_error": why or None, "spin_ns": self._spin_ns}

    def mono_ns(self) -> int:
        return time.monotonic_ns()

    def wait_until(self, deadline_ns: int, stop: threading.Event) -> bool:
        """Return at ``deadline_ns`` (True), or as soon as ``stop`` is set (False); a stop set
        during the last :data:`FINE_NS` is seen at the deadline."""
        spin = self._spin_ns
        while True:
            if stop.is_set():
                return False
            remaining = deadline_ns - time.monotonic_ns() - spin
            if remaining <= 0:
                break
            if remaining > FINE_NS:
                stop.wait((remaining - FINE_NS) / NS)
            else:
                time.sleep(remaining / NS)
        while time.monotonic_ns() < deadline_ns:
            pass  # the final spin: at most ``spin`` long
        return not stop.is_set()


class SeriesPacer(Generic[T]):
    """Runs one series: :meth:`run` on the loop, :meth:`_pace` in the pacer thread.

    Only the loop appends to the packet queue and only the thread pops from it (both atomic
    under the GIL); the thread's reports come back through a second queue and one
    ``call_soon_threadsafe`` per batch."""

    def __init__(self, grid: Grid, send: ThreadSend, waiter: Waiter, *, timed: bool) -> None:
        """``timed``: each packet carries its slot's time, so it is valid for that slot only."""
        self._grid = grid
        self._timed = timed
        self._send = send
        self._waiter = waiter
        self._queue: deque[Built[T]] = deque()
        self._reports: deque[Report[T]] = deque()
        self._stop = threading.Event()
        self._ready = threading.Event()  # a packet was queued (a burst waits for it)
        self._slot = 0  # the slot the thread is at: the builder does not build behind it
        self._posted = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._wake = asyncio.Event()
        self._done = False
        self._built = 0
        self._dropped = 0  # built packets that will never be sent (discarded)
        self._sent = 0
        self._next_slot = 0
        self._failure: BaseException | None = None  # found on the loop (see :meth:`abort`)
        self.facts: dict[str, Any] = {}  # how the thread ran (its waiter's setup)

    def abort(self, error: BaseException) -> None:
        """On the loop: end the series with ``error`` (a refusal the link reported for one of
        its packets); :meth:`run` raises it."""
        if self._failure is None:
            self._failure = error
        self._wake.set()

    # -- the loop side ----------------------------------------------------------------------

    async def run(self, build: Build[T], handle: Handle[T]) -> None:
        """Pace the series to its end, calling ``build`` for the packets and ``handle`` for
        each report, in order. Raises the error of a FAILED report or of ``build``; however it
        ends, the thread is stopped and every report (and every packet left unsent) is handled
        before it returns."""
        loop = asyncio.get_running_loop()
        self._loop = loop
        thread = threading.Thread(target=self._pace, name="ib-pacer", daemon=True)
        thread.start()
        timer: asyncio.TimerHandle | None = None
        try:
            while True:
                failure = self._drain(handle) or self._failure
                if failure is not None:
                    raise failure
                if self._done:
                    return
                delay = self._fill(build)
                if timer is not None:
                    timer.cancel()
                    timer = None
                if delay is not None:
                    timer = loop.call_later(delay, self._wake.set)
                await self._wake.wait()
                self._wake.clear()
        finally:
            if timer is not None:
                timer.cancel()
            self._stop.set()
            self._ready.set()
            with contextlib.suppress(TimeoutError, asyncio.CancelledError):
                async with asyncio.timeout(DONE_TIMEOUT_S):
                    while not self._done:
                        self._drain(handle)
                        if not self._done:
                            await self._wake.wait()
                            self._wake.clear()
            if self._done:
                thread.join()
            self._drain(handle)
            while self._queue:
                handle(Report(Kind.UNSENT, self._queue.popleft()))

    def _drain(self, handle: Handle[T]) -> BaseException | None:
        failure: BaseException | None = None
        while self._reports:
            report = self._reports.popleft()
            match report.kind:
                case Kind.DONE:
                    self._done = True
                    continue
                case Kind.DISCARDED:
                    self._dropped += 1
                case Kind.SENT:
                    self._sent += 1
                case _:
                    pass
            error = handle(report)
            if failure is None and report.kind is Kind.FAILED:
                failure = error if error is not None else report.error
        return failure

    def _fill(self, build: Build[T]) -> float | None:
        """Build the packets that are due; returns the seconds until the next one is, or None
        when nothing is to be built until the pacer reports again."""
        grid = self._grid
        depth = BURST_AHEAD if grid.every is None else MAX_AHEAD
        while len(self._queue) < depth:
            if grid.cap is not None and self._built - self._dropped >= grid.cap:
                return None
            now = time.monotonic_ns()
            if grid.every is not None:
                # A timed packet is built for the next slot; any other covers the next slot
                # not covered by those queued (the pacer sends it whatever slot it reaches).
                ahead = self._slot + len(self._queue)
                slot = max(self._next_slot, self._slot) if self._timed else ahead
                if not grid.in_window(slot):
                    return None
                due = grid.deadline(slot) - AHEAD_NS
            else:
                slot = 0
                if self._built and not self._sent:
                    return None  # the rest of a burst is built once its first packet left
                due = grid.start_mono - AHEAD_NS if not self._built else now
            if due > now:
                return (due - now) / NS
            self._queue.append(build(self._built, slot))
            self._ready.set()
            self._built += 1
            self._next_slot = slot + 1
        return None

    def _on_posted(self) -> None:
        self._posted = False
        self._wake.set()

    # -- the pacer thread -------------------------------------------------------------------

    def _post(self, report: Report[T]) -> None:
        self._reports.append(report)
        if self._posted:
            return
        self._posted = True
        loop = self._loop
        assert loop is not None
        with contextlib.suppress(RuntimeError):  # the loop is closed: nobody waits any more
            loop.call_soon_threadsafe(self._on_posted)

    def _pace(self) -> None:
        try:
            self.facts.update(self._waiter.setup())
            self._pace_slots()
        except BaseException as exc:  # a fault of the pacer itself: it ends the series, coded
            self._post(Report(Kind.FAILED, error=exc))
        finally:
            self._post(Report(Kind.DONE))

    def _pace_slots(self) -> None:
        grid, waiter, stop = self._grid, self._waiter, self._stop
        slot = sent = 0
        while not stop.is_set():
            if (grid.cap is not None and sent >= grid.cap) or not grid.in_window(slot):
                return
            self._slot = slot
            deadline = grid.deadline(slot) if grid.every is not None or not sent else None
            if deadline is not None and not waiter.wait_until(deadline, stop):
                return
            now = waiter.mono_ns()
            if grid.too_late(slot, now):
                to = grid.nearest(now)
                assert to > slot, "a slot more than half a period late is passed over"
                self._post(Report(Kind.SKIPPED, slots=grid.lost(slot, to)))
                slot = to
                continue
            built = self._take(slot)
            if built is None:
                if grid.every is None:
                    if not self._await_packet():
                        return
                    continue
                self._post(Report(Kind.SKIPPED, slots=1, starved=True))
                slot += 1
                continue
            try:
                went = self._send(built.data, stop)
            except LeftThenFailed as exc:
                self._post(Report(Kind.FAILED, built, went_out=True, error=exc.cause))
                return
            except Exception as exc:
                self._post(Report(Kind.FAILED, built, error=exc))
                return
            after = waiter.mono_ns()
            if not went:
                self._queue.appendleft(built)  # stopped before it left: reported as unsent
                return
            self._post(
                Report(
                    Kind.SENT,
                    built,
                    late_ns=None if deadline is None else after - deadline,
                    took_ns=after - now,
                    mono_ns=after,
                    went_out=True,
                )
            )
            sent += 1
            if grid.every is not None:
                slot += 1

    def _take(self, slot: int) -> Built[T] | None:
        """The packet for ``slot``: the next one, or for a timed series the one built for it
        (those built for earlier, skipped slots are discarded)."""
        queue = self._queue
        while queue:
            head = queue[0]
            if not self._timed or self._grid.every is None or head.slot == slot:
                return queue.popleft()
            if head.slot > slot:
                return None
            self._post(Report(Kind.DISCARDED, queue.popleft()))
        return None

    def _await_packet(self) -> bool:
        """A burst waits for its next packet; False when stopped."""
        while not self._queue:
            if self._stop.is_set():
                return False
            self._ready.wait(READY_POLL_S)
            self._ready.clear()
        return True
