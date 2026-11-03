"""Blocking calls run off the event loop, in daemon threads that nothing waits for at exit.

The loop's default executor is joined with no time limit when ``asyncio.run`` ends (Python 3.11),
so a call there that never returns (a resolver whose DNS server does not answer, an NSS source
that hangs) would hold the process after its command is over, with the signal handlers no
longer able to end it. A call made here runs in its own daemon thread with the shutdown signals
blocked (:func:`ironbird.sigmask.start_thread`), and its outcome reaches the awaiting task through
the loop. A task that stops waiting (a timeout, a cancel) abandons the thread: it ends on its own
and its outcome is dropped. Neither ``asyncio.run`` nor the interpreter's exit waits for it.

The threads of one :class:`OffLoop` still running, abandoned ones included, are bounded: a call
beyond the bound is refused with :class:`OffLoopBusy` instead of starting yet another thread
behind ones that may never return.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import Callable
from typing import Final, Generic, TypeVar

from ironbird.sigmask import start_thread

T = TypeVar("T")


class OffLoopBusy(Exception):
    """Too many calls of one :class:`OffLoop` are still running (or never returned)."""

    def __init__(self, name: str, running: int) -> None:
        super().__init__(f"{running} earlier {name} call(s) have not returned")
        self.running = running


class OffLoop(Generic[T]):
    """Runs one kind of blocking call (named ``name``) off the loop, at most ``limit`` at once."""

    def __init__(self, name: str, limit: int) -> None:
        assert limit > 0
        self._name: Final = name
        self._limit: Final = limit
        self._lock = threading.Lock()
        self._running = 0

    @property
    def running(self) -> int:
        """Calls whose thread has not returned yet, abandoned ones included."""
        with self._lock:
            return self._running

    async def call(self, fn: Callable[[], T]) -> T:
        """Run ``fn`` in a new daemon thread and return its result (or raise what it raised).

        Raises :class:`OffLoopBusy` when ``limit`` calls are still running, and the
        ``RuntimeError`` of a thread that cannot start (the process or user thread limit).
        Cancelling the caller abandons the thread.
        """
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._running >= self._limit:
                raise OffLoopBusy(self._name, self._running)
            self._running += 1
        future: asyncio.Future[T] = loop.create_future()

        def work() -> None:
            result: T | None = None
            failure: BaseException | None = None
            try:
                result = fn()
            except BaseException as exc:  # handed to the awaiting task, never lost in the thread
                failure = exc
            finally:
                with self._lock:
                    self._running -= 1
            with contextlib.suppress(RuntimeError):  # the loop may be closed by now
                loop.call_soon_threadsafe(_settle, future, result, failure)

        try:
            start_thread(threading.Thread(target=work, name=self._name, daemon=True))
        except BaseException:
            with self._lock:
                self._running -= 1
            raise
        return await future


def _settle(future: asyncio.Future[T], result: T | None, failure: BaseException | None) -> None:
    if future.done():  # the caller stopped waiting
        return
    if failure is not None:
        future.set_exception(failure)
    else:
        future.set_result(result)  # type: ignore[arg-type]


__all__ = ["OffLoop", "OffLoopBusy"]
