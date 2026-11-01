"""Bounded waiting at exit, and work that must not run on the loop or the main thread.

- :class:`WaitBudget` - the time a command may spend waiting for a slow or hung disk at exit, shared
  by every wait (the last flush, the writer's finish and close, a scan still running) so the
  total is bounded once, not once per step; a wait ends early when a signal arrives.
- :class:`GateWorker` - one daemon thread that runs the cross-process rate limit of the
  start-of-session warnings (it reads and rewrites the notices file), so neither the event loop
  nor the main thread ever touches that file: a hung state directory delays nothing.
"""

from __future__ import annotations

import queue
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Final

from ironbird.sigmask import start_thread

SLICE_S: Final = 0.05
FINAL_WRITE_FLOOR_S: Final = 0.2  # what the ledger's last write may still wait after a signal


class WaitBudget:
    """``total_s`` seconds of waiting, spent by whichever waits draw on it.

    ``abort`` (when given) is asked every :data:`SLICE_S` during a wait; a true answer ends the
    wait at once (the caller treats it as not finished) and leaves the budget as it was spent.
    Once it has answered true it keeps answering true (:attr:`aborted`): one signal ends every
    wait of the exit, not just the one it interrupted.

    A wait may name ``keep`` seconds that it leaves unspent, for the waits that come after it
    (the ledger's last write and close must not be starved by a scan or a rate limit), and a
    ``floor``: the seconds it may still wait after the budget was aborted (a signal), counted from
    the abort - the ledger's last write is worth that much, a scan's is not.
    """

    def __init__(self, total_s: float, abort: Callable[[], bool] | None = None) -> None:
        assert total_s >= 0
        self._total = total_s
        self._left = total_s
        self._abort = abort
        self.aborted = False

    @property
    def total(self) -> float:
        return self._total

    @property
    def left(self) -> float:
        return self._left

    def wait(
        self,
        event: threading.Event,
        cap: float | None = None,
        keep: float = 0.0,
        floor: float = 0.0,
    ) -> bool:
        """Wait for ``event`` (at most ``cap`` seconds, if given, leaving ``keep`` seconds of
        the budget, and ``floor`` seconds past an abort); ``True`` when it was set."""
        return self._wait(event.wait, cap, keep, floor)

    def join(
        self,
        thread: threading.Thread,
        cap: float | None = None,
        keep: float = 0.0,
        floor: float = 0.0,
    ) -> bool:
        """Wait for ``thread`` to end (at most ``cap`` seconds, if given, leaving ``keep``
        seconds of the budget, and ``floor`` seconds past an abort); ``True`` when it has."""

        def poll(timeout: float) -> bool:
            thread.join(timeout)
            return not thread.is_alive()

        return self._wait(poll, cap, keep, floor)

    def _asked_to_stop(self) -> bool:
        if not self.aborted and self._abort is not None and self._abort():
            self.aborted = True
        return self.aborted

    def _wait(
        self,
        poll: Callable[[float], bool],
        cap: float | None = None,
        keep: float = 0.0,
        floor: float = 0.0,
    ) -> bool:
        assert floor >= 0
        start = time.monotonic()
        limit = floor if self.aborted else max(0.0, self._left - keep)
        if cap is not None:
            limit = min(cap, limit)
        try:
            while True:
                elapsed = time.monotonic() - start
                left = limit - elapsed
                if poll(max(0.0, min(SLICE_S, left)) if left > 0 else 0.0):
                    return True
                if left <= 0:
                    return False
                if not self.aborted and self._asked_to_stop():
                    if floor <= 0:
                        return False
                    limit = min(limit, time.monotonic() - start + floor)  # the floor, from now
        finally:
            self._left = max(0.0, self._left - (time.monotonic() - start))


class GateWorker:
    """Runs ``gate()`` callables one after another on a daemon thread; the results are taken by
    the owner (any thread) with :meth:`take`. A gate that raises is answered ``True``: a failing
    rate limit never causes silence."""

    def __init__(self) -> None:
        self._jobs: queue.SimpleQueue[tuple[int, Callable[[], bool]]] = queue.SimpleQueue()
        self._results: deque[tuple[int, bool]] = deque()
        self._thread = threading.Thread(target=self._run, name="gate-worker", daemon=True)
        self._started = False
        self.progress = threading.Event()  # set after each finished gate

    def submit(self, token: int, gate: Callable[[], bool]) -> None:
        if not self._started:
            self._started = True
            start_thread(self._thread)
        self._jobs.put((token, gate))

    def take(self) -> list[tuple[int, bool]]:
        done: list[tuple[int, bool]] = []
        while self._results:
            done.append(self._results.popleft())
        return done

    def _run(self) -> None:
        while True:
            token, gate = self._jobs.get()
            try:
                allowed = bool(gate())
            except BaseException:  # a rate limit that cannot be consulted displays
                allowed = True
            self._results.append((token, allowed))
            self.progress.set()
