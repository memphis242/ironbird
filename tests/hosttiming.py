"""Marker shared by the host-timing guards (tests/test_benchmarks.py, tests/bench/), the host's
own wake-up reference that a pacing guard is measured against, and the wall-time deadline of the
property tests, which only an untraced run enforces."""

from __future__ import annotations

import os
import sys
import threading
import time
from dataclasses import dataclass
from typing import Final

import pytest

from ironbird import pacer

# A guard whose threshold is a rate or a cost ratio measured on the host. Under xdist the other
# workers keep every core busy, so such a measurement says more about the scheduler than about
# the tool: a listener off-CPU for a few milliseconds overruns a socket buffer capped by a small
# net.core.rmem_max (212992 on GitHub runners), and a timed loop shares its core with another
# test. These run, and are enforced, in the serial runs of CI (``pytest tests/test_benchmarks.py
# -n0`` and ``pytest tests/bench -n0``), as pytest-benchmark's own ceilings are.
needs_a_quiet_host = pytest.mark.skipif(
    "PYTEST_XDIST_WORKER" in os.environ,
    reason="host-timing guard: enforced by the serial CI runs (pytest tests/bench -n0, "
    "pytest tests/test_benchmarks.py -n0)",
)

_NS: Final = 1_000_000_000


def traced() -> bool:
    """Whether coverage measures this process (tools/coverage.sh, ``coverage run``). Its tracer
    slows Python code by a factor that depends on the code (branch-dense loops the most) and on
    how many traced processes share the host, so no wall-time bound holds under it."""
    coverage = sys.modules.get("coverage")
    return coverage is not None and coverage.Coverage.current() is not None


def hang_deadline(ms: int) -> int | None:
    """A property test's Hypothesis ``deadline``: ``ms``, the wall time past which one example is
    a hang rather than slow code, or none at all under coverage (:func:`traced`), where it would
    measure the tracer. The untraced runs of CI (the lint, types, tests jobs) run the same tests
    and enforce it; what a test asserts about the work done (a framer's own work count, a cap)
    holds traced or not."""
    assert ms > 0
    return None if traced() else ms


@dataclass(frozen=True)
class WakeReference:
    """How late a bare thread wakes on this host when it sleeps to absolute deadlines on a
    fixed grid, as the pacer thread does (``time.sleep``, timer slack lowered to
    :data:`ironbird.pacer.TIMER_SLACK_NS`) but with nothing to send and no other thread of the
    process wanting the interpreter lock. A slot woken more than half a period late is passed
    over, as the pacer passes it over, and the lateness covers the slots served, as the pacer's
    ``late_mean_ns`` does. A serial run is not a quiet host for this: on a shared VM a sleeping
    thread alone wakes 20-100 us late at a 1 ms period, which is the floor any pacer meets there."""

    served: int = 0
    skipped: int = 0
    late_sum_ns: int = 0

    @property
    def slots(self) -> int:
        return self.served + self.skipped

    @property
    def late_mean_ns(self) -> int:
        return self.late_sum_ns // self.served if self.served else 0

    @property
    def skipped_share(self) -> float:
        return self.skipped / self.slots if self.slots else 0.0

    def __add__(self, other: WakeReference) -> WakeReference:
        return WakeReference(
            self.served + other.served,
            self.skipped + other.skipped,
            self.late_sum_ns + other.late_sum_ns,
        )


def wake_reference(period_ns: int, slots: int) -> WakeReference:
    """Measure :class:`WakeReference` over ``slots`` slots of ``period_ns``, in a new thread."""
    assert period_ns > 0 and slots > 0
    found: list[WakeReference] = []
    failed: list[str] = []

    def measure() -> None:
        slack, why = pacer.lower_timer_slack(pacer.TIMER_SLACK_NS)
        if slack != pacer.TIMER_SLACK_NS:
            failed.append(f"timer slack {slack} ({why or 'not lowered'})")
            return
        served = skipped = late_sum = 0
        start = time.monotonic_ns() + 5 * period_ns
        slot = 0
        while slot < slots:
            deadline = start + slot * period_ns
            remaining = deadline - time.monotonic_ns()
            if remaining > 0:
                time.sleep(remaining / _NS)
            late = time.monotonic_ns() - deadline
            if 2 * late > period_ns:  # passed over, up to the first slot not half a period gone
                to = min(slot - (-(2 * late - period_ns) // (2 * period_ns)), slots)
                assert to > slot
                skipped += to - slot
                slot = to
                continue
            served += 1
            late_sum += max(late, 0)
            slot += 1
        found.append(WakeReference(served, skipped, late_sum))

    thread = threading.Thread(target=measure, name="wake-reference")
    thread.start()
    thread.join()
    assert not failed, failed[0]
    assert len(found) == 1 and found[0].slots == slots
    return found[0]
