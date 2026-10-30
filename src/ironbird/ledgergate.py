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
import signal
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Final

from ironbird.ledgerstore import SHUTDOWN_SIGNALS

SLICE_S: Final = 0.05


def start_thread(thread: threading.Thread) -> None:
    """Start a helper thread with the shutdown signals blocked, so it never receives one: they are
    handled by the main thread, which must see them all."""
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, SHUTDOWN_SIGNALS)
    try:
        thread.start()
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


class WaitBudget:
    """``total_s`` seconds of waiting, spent by whichever waits draw on it.

    ``abort`` (when given) is asked every :data:`SLICE_S` during a wait; a true answer ends the
    wait at once (the caller treats it as not finished) and leaves the budget as it was spent.
    """

    def __init__(self, total_s: float, abort: Callable[[], bool] | None = None) -> None:
        assert total_s >= 0
        self._left = total_s
        self._abort = abort

    @property
    def left(self) -> float:
        return self._left

    def wait(self, event: threading.Event, cap: float | None = None) -> bool:
        """Wait for ``event`` (at most ``cap`` seconds, if given); ``True`` when it was set."""
        return self._wait(event.wait, cap)

    def join(self, thread: threading.Thread, cap: float | None = None) -> bool:
        """Wait for ``thread`` to end (at most ``cap`` seconds, if given); ``True`` when it has."""

        def poll(timeout: float) -> bool:
            thread.join(timeout)
            return not thread.is_alive()

        return self._wait(poll, cap)

    def _wait(self, poll: Callable[[float], bool], cap: float | None = None) -> bool:
        start = time.monotonic()
        limit = self._left if cap is None else min(cap, self._left)
        try:
            while True:
                left = limit - (time.monotonic() - start)
                if poll(max(0.0, min(SLICE_S, left)) if left > 0 else 0.0):
                    return True
                if left <= 0:
                    return False
                if self._abort is not None and self._abort():
                    return False
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
