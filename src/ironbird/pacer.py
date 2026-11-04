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
and errors stay on the loop, so the loop's work never delays the pacer's wait or its send call
(a link with no thread-safe send is the exception: see above). The pacer thread does share the
interpreter lock with the loop and the other helper threads, though: coming out of its sleep it
waits for the lock for as long as another thread runs Python code, up to the interpreter's
switch interval (5 ms by default, which at a 10 ms period is half a slot). For the life of a
series the switch interval is therefore lowered to :data:`SWITCH_INTERVAL_S` and restored after
(``sys.setswitchinterval``; it is process-wide, so series that overlap share one lowering: the
first saves the original and the last restores it). That bounds the wait, it does not remove it: a
thread that holds the lock through a long C call still delays the pacer, and the lateness the
series reports includes any such wait.

A packet is built no earlier than :data:`AHEAD_NS` before its slot, so the ledger's transmit
gate and the time a packet carries are current. The pacer never blocks on the queue: a slot
whose packet is not there at its deadline is skipped (``starved``) and reported, consecutive
starved slots as one report.

A packet whose header carries the send time (``time_seconds``/``time_subseconds``) is built for
one slot and carries that slot's time; when that slot is skipped it is discarded (its count goes
unused, and its ledger record says so). Any other packet is sent in the next slot instead.

The loop builds only packets that can be sent: never for a slot the thread has passed or is
passing (the thread publishes the next slot it will serve the moment it takes a packet), and,
for a packet that is not tied to a slot, never more than the slots left in the window (the loop
reads the queue before the slot the thread publishes, so a thread running between the two reads
can only make it build late, never too many). A packet is left unsent only by a skip near the end
or by a stop.

Every packet is accounted for. The thread publishes the packet it is sending; a send is asked
to return when ``stop`` is set (the loop's own stops it through the loop), and the loop joins
the thread before :meth:`SeriesPacer.run` returns. A send that ignores ``stop`` for
:data:`DONE_TIMEOUT_S` is abandoned (the one way the thread outlives ``run``): the packet it is
sending is reported as UNCONFIRMED, since whether it left is not known, after an ABANDONED report
that carries a coded error. What the abandoned thread reports from then on is not queued for the
loop (it has finished): a failure goes to the ``late`` callback, which ledgers it.
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

from ironbird.errors import Code, IronbirdError
from ironbird.sendmodes import NS, Grid
from ironbird.sigmask import start_thread

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
SWITCH_INTERVAL_S: Final = 0.0002  # the GIL switch interval for the life of a series

T = TypeVar("T")


class _SwitchInterval:
    """The interpreter's GIL switch interval, lowered while any series runs (the setting is
    process-wide, so series that overlap share it): the first to start saves and lowers it, the
    last to end restores what it was."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._users = 0
        self._saved = 0.0

    def acquire(self) -> None:
        with self._lock:
            if self._users == 0:
                self._saved = sys.getswitchinterval()
                sys.setswitchinterval(min(self._saved, SWITCH_INTERVAL_S))
            self._users += 1

    def release(self) -> None:
        with self._lock:
            assert self._users > 0, "released more often than acquired"
            self._users -= 1
            if self._users == 0:
                sys.setswitchinterval(self._saved)

    @property
    def users(self) -> int:
        with self._lock:
            return self._users


_SWITCH: Final = _SwitchInterval()


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
    UNCONFIRMED = "unconfirmed"  # in flight when the pacer was abandoned: left or not, unknown
    ABANDONED = (
        "abandoned"  # the loop gave up on the thread (``error``: coded; ``built``: in flight)
    )
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
#: Called in the abandoned pacer thread with the FAILED report it can no longer deliver. It must
#: be thread-safe and must not raise.
Late = Callable[[Report[T]], None]


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

    def __init__(
        self,
        grid: Grid,
        send: ThreadSend,
        waiter: Waiter,
        *,
        timed: bool,
        late: Late[T] | None = None,
    ) -> None:
        """``timed``: each packet carries its slot's time, so it is valid for that slot only.
        ``late``: where a failure of the thread goes once the loop has abandoned it."""
        self._grid = grid
        self._late = late
        self._timed = timed
        self._send = send
        self._waiter = waiter
        self._queue: deque[Built[T]] = deque()
        self._reports: deque[Report[T]] = deque()
        self._stop = threading.Event()
        self._ready = threading.Event()  # a packet was queued (a burst waits for it)
        self._slot = 0  # the next slot the thread will serve: the builder does not build behind it
        self._posted = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._wake = asyncio.Event()
        self._done = False
        self._built = 0
        self._dropped = 0  # built packets that will never be sent (discarded)
        self._sent = 0
        self._next_slot = 0
        self._starved_run = 0  # consecutive starved slots not yet reported (thread only)
        self._flight_lock = threading.Lock()  # guards the next three: the packet in flight
        self._inflight: Built[T] | None = None  # popped by the thread, not yet reported
        self._abandoned = False  # the loop gave up on the thread: it reports no more
        self._failure: BaseException | None = None  # found on the loop (see :meth:`abort`)
        self.facts: dict[str, Any] = {}  # how the thread ran (its waiter's setup)

    @property
    def in_flight(self) -> Built[T] | None:
        """The packet the thread is sending, if any (read in the thread that sends it, this is
        the packet of the send that is running)."""
        return self._inflight

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
        before it returns, even if ``handle`` raised on one of them (the first such error is
        raised when nothing else is, else it is attached to the error that is). ``IB-SYS-001``
        if the thread cannot be started. The interpreter's switch interval is lowered for the
        life of the series, and restored when the last series running in the process ends (see the
        module)."""
        _SWITCH.acquire()
        try:
            await self._run(build, handle)
        finally:
            _SWITCH.release()

    async def _run(self, build: Build[T], handle: Handle[T]) -> None:
        loop = asyncio.get_running_loop()
        self._loop = loop
        thread = threading.Thread(target=self._pace, name="ib-pacer", daemon=True)
        try:
            start_thread(thread)
        except RuntimeError as exc:  # the system's thread or process limit
            raise IronbirdError(
                Code.SYS_RESOURCE,
                f"could not start the pacer thread ({exc}): the limit on threads or processes "
                "may be reached; close other programs or raise it ('ulimit -u')",
                cause=exc,
            ) from exc
        timer: asyncio.TimerHandle | None = None
        raised: BaseException | None = None
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
        except BaseException as exc:
            raised = exc
            raise
        finally:
            if timer is not None:
                timer.cancel()
            errors = await self._wind_down(thread, handle)
            if errors and raised is None:
                raise errors[0]
            for error in errors:  # a bug in the handler must not replace why the series ended
                assert raised is not None
                raised.add_note(f"while the pacer was being wound down: {error!r}")

    async def _wind_down(self, thread: threading.Thread, handle: Handle[T]) -> list[Exception]:
        """Stop the thread and join it, then account for everything it did not: every report
        and every packet still queued (UNSENT) or in flight if the thread had to be abandoned
        (UNCONFIRMED, after one ABANDONED report). One report that ``handle`` raises on does not
        stop the rest being handled; the errors are returned."""
        errors: list[Exception] = []
        self._stop.set()
        self._ready.set()
        with contextlib.suppress(TimeoutError, asyncio.CancelledError):
            async with asyncio.timeout(DONE_TIMEOUT_S):
                while not self._done:
                    self._drain(handle, errors)
                    if not self._done:
                        await self._wake.wait()
                        self._wake.clear()
        flight: Built[T] | None = None
        abandoned = not self._done
        if not abandoned:
            thread.join()
        else:
            with self._flight_lock:  # what the thread reports from here on goes to ``late``
                self._abandoned = True
                flight = self._inflight
        self._drain(handle, errors)
        if abandoned:
            self._handle_guarded(
                handle, Report(Kind.ABANDONED, flight, error=self._abandon_error(flight)), errors
            )
        if flight is not None:
            self._handle_guarded(handle, Report(Kind.UNCONFIRMED, flight), errors)
        while self._queue:
            queued = self._queue.popleft()
            if queued is not flight:
                self._handle_guarded(handle, Report(Kind.UNSENT, queued), errors)
        return errors

    @staticmethod
    def _abandon_error(flight: Built[T] | None) -> IronbirdError:
        """The coded error for a pacer thread that did not stop within :data:`DONE_TIMEOUT_S`."""
        sending = (
            "its send ignored the stop, so whether the packet in flight left is not known"
            if flight is not None
            else "it did not stop"
        )
        return IronbirdError(
            Code.TX_SEND,
            f"pacer thread abandoned after {DONE_TIMEOUT_S:g} s: {sending}; check the link "
            "(a device or socket that stopped answering)",
        )

    @staticmethod
    def _handle_guarded(handle: Handle[T], report: Report[T], errors: list[Exception]) -> None:
        try:
            handle(report)
        except Exception as exc:
            errors.append(exc)

    def _drain(
        self, handle: Handle[T], errors: list[Exception] | None = None
    ) -> BaseException | None:
        """Handle the reports that came in; returns the error of the first FAILED one. With
        ``errors``, an exception of ``handle`` is collected there and the rest still handled."""
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
            error: BaseException | None = None
            if errors is None:
                error = handle(report)
            else:
                try:
                    error = handle(report)
                except Exception as exc:
                    errors.append(exc)
            if failure is None and report.kind is Kind.FAILED:
                failure = error if error is not None else report.error
        return failure

    def _fill(self, build: Build[T]) -> float | None:
        """Build the packets that are due; returns the seconds until the next one is, or None
        when nothing is to be built until the pacer reports again.

        A periodic series builds from the first slot the thread can still serve: not behind
        the slot it is at, nor behind the slot nearest to now (the thread skips to that one if
        it is late). A timed packet is built for the next such slot; any other is sent in the
        next slot not covered by those queued, so none is built that no slot is left for."""
        grid = self._grid
        depth = BURST_AHEAD if grid.every is None else MAX_AHEAD
        while len(self._queue) < depth:
            if grid.cap is not None and self._built - self._dropped >= grid.cap:
                return None
            now = self._waiter.mono_ns()
            if grid.every is not None:
                # The thread publishes ``_slot`` and then pops, so reading the queue first can
                # only see a slot too early (a deferred build), never a packet too many.
                queued = len(self._queue)
                floor = max(self._slot, grid.nearest(now))
                slot = max(self._next_slot, floor) if self._timed else floor + queued
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

    def _wake_loop(self) -> None:
        if self._posted:
            return
        self._posted = True
        loop = self._loop
        assert loop is not None
        with contextlib.suppress(RuntimeError):  # the loop is closed: nobody waits any more
            loop.call_soon_threadsafe(self._on_posted)

    def _flush_starved(self) -> None:
        """Report the consecutive starved slots so far as one SKIPPED report."""
        run, self._starved_run = self._starved_run, 0
        if run:
            self._post(Report(Kind.SKIPPED, slots=run, starved=True))

    def _post(self, report: Report[T]) -> None:
        self._flush_starved()
        self._reports.append(report)
        self._wake_loop()

    def _land(self, built: Built[T], report: Report[T] | None) -> None:
        """The packet in flight is resolved: report it, or (``None``: the send stopped before
        it left) put it back at the head of the queue. Once the loop has abandoned the thread
        the packet is the loop's to report, and a failure goes to ``late``."""
        self._flush_starved()
        with self._flight_lock:  # clearing the flight and queueing the report are one step
            self._inflight = None
            abandoned = self._abandoned
            if not abandoned:
                if report is None:
                    self._queue.appendleft(built)
                    return
                self._reports.append(report)
        if abandoned:
            self._to_late(report)
        else:
            self._wake_loop()

    def _deliver(self, report: Report[T]) -> None:
        """Hand a report that is not about a packet in flight to the loop (or to ``late``)."""
        with self._flight_lock:
            abandoned = self._abandoned
            if not abandoned:
                self._reports.append(report)
        if abandoned:
            self._to_late(report)
        else:
            self._wake_loop()

    def _to_late(self, report: Report[T] | None) -> None:
        """The loop has abandoned the thread and reported everything else it knew: a failure
        the thread reports now goes to ``late``."""
        if report is not None and report.kind is Kind.FAILED and self._late is not None:
            self._late(report)

    def _pace(self) -> None:
        try:
            self.facts.update(self._waiter.setup())
            self._pace_slots()
        except BaseException as exc:  # a fault of the pacer itself: it ends the series, coded
            flight = self._inflight
            if flight is None:
                self._flush_starved()
                self._deliver(Report(Kind.FAILED, error=exc))
            else:
                self._land(flight, Report(Kind.FAILED, flight, error=exc))
        finally:
            self._post(Report(Kind.DONE))

    def _pace_slots(self) -> None:
        try:
            self._serve_slots()
        finally:
            self._flush_starved()

    def _serve_slots(self) -> None:
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
                slot = self._slot = to
                continue
            if grid.every is not None:
                self._slot = slot + 1  # published before the packet is taken (see the module)
            built = self._take(slot)
            if built is None:
                if grid.every is None:
                    if not self._await_packet():
                        return
                    continue
                self._starved_run += 1
                slot += 1
                continue
            try:
                went = self._send(built.data, stop)
            except LeftThenFailed as exc:
                self._land(built, Report(Kind.FAILED, built, went_out=True, error=exc.cause))
                return
            except Exception as exc:
                self._land(built, Report(Kind.FAILED, built, error=exc))
                return
            after = waiter.mono_ns()
            if not went:
                self._land(built, None)  # stopped before it left: reported as unsent
                return
            self._land(
                built,
                Report(
                    Kind.SENT,
                    built,
                    late_ns=None if deadline is None else after - deadline,
                    took_ns=after - now,
                    mono_ns=after,
                    went_out=True,
                ),
            )
            sent += 1
            if grid.every is not None:
                slot += 1

    def _take(self, slot: int) -> Built[T] | None:
        """The packet for ``slot``: the next one, or for a timed series the one built for it
        (those built for earlier, skipped slots are discarded). The packet is published as in
        flight before it leaves the queue, so it is always in one or the other."""
        queue = self._queue
        while queue:
            head = queue[0]
            if not self._timed or self._grid.every is None or head.slot == slot:
                self._inflight = head
                queue.popleft()
                return head
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
