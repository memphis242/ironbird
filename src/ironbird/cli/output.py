"""Terminal output that can never stall the event loop.

A reader that stops reading (an unread pipe, a paused ``less``, a slow SSH link) blocks
``write()``. Written from the event loop, that would freeze signal handling, ``--timeout``,
socket reads and the ledger. :class:`BoundedWriter` therefore hands lines to a daemon thread
through a byte-bounded queue; when the queue is full, new lines are dropped and counted, and the
owner reports the count as one coalesced coded notice. The thread writes to the raw file
descriptor, so an abandoned blocked write cannot hold a buffer lock that interpreter shutdown
would wait for.

A reader that went away (``| head -1``) is a normal end of output, not an error: the writer
records it, discards the rest and calls ``on_closed`` so the command can stop.

At the end of a command the owner drains with no deadline (a slow reader loses nothing; the owner
waits for a signal to cut that short) and then adds up :attr:`BoundedWriter.lost`: lines dropped
while the queue was full, lines abandoned when a drain was cut short, and lines that could not
be delivered at all (a closed stream, a failing descriptor). The writer threads hold the shutdown
signals blocked, so a signal never lands in a thread that cannot act on it.
"""

from __future__ import annotations

import contextlib
import errno
import os
import select
import signal
import threading
from collections import deque
from collections.abc import Callable, Generator
from typing import Final, TextIO

MAX_QUEUED_BYTES: Final = 512 * 1024
CHUNK_BYTES: Final = 64 * 1024
CLOSE_WAIT_S: Final = 1.0
_GONE: Final = frozenset({errno.EPIPE, errno.ECONNRESET, errno.EBADF})
HANDLED_SIGNALS: Final = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


@contextlib.contextmanager
def signals_held() -> Generator[None]:
    """Block :data:`HANDLED_SIGNALS` in this thread; threads started inside inherit the mask.

    A blocked signal waits as pending (see ``signal.sigtimedwait``) instead of running a handler
    at an arbitrary line. The previous mask is restored on exit, so uses nest.
    """
    if not hasattr(signal, "pthread_sigmask"):  # pragma: no cover - not POSIX
        yield
        return
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, HANDLED_SIGNALS)
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


class BoundedWriter:
    """Line writer for one stream; ``write`` never blocks."""

    def __init__(
        self,
        stream: TextIO | None,
        *,
        max_bytes: int = MAX_QUEUED_BYTES,
        on_closed: Callable[[], None] | None = None,
    ) -> None:
        assert max_bytes > 0
        self._stream = stream
        self._max_bytes = max_bytes
        self._on_closed = on_closed
        self._encoding = getattr(stream, "encoding", None) or "utf-8"
        self._cond = threading.Condition()
        self._queue: deque[bytes] = deque()
        self._queued = 0
        self._inflight = 0  # lines in the chunk the thread is writing right now
        self._closing = False
        self._reported = 0
        self.dropped_total = 0  # refused: queue full, or written after the close began
        self.abandoned_total = 0  # accepted, then given up on when a drain was cut short
        self.undelivered_total = 0  # no stream to write to, or the stream failed
        self.reader_gone = False
        self.unavailable = stream is None  # e.g. standard output closed with ``>&-``
        self.error: OSError | None = None
        self._fd: int | None = None
        self._thread: threading.Thread | None = None
        fd: int | None = None
        if stream is not None:
            try:
                fd = stream.fileno()
            except (AttributeError, OSError, ValueError):
                fd = None
        if fd is not None and stream is not None:
            with contextlib.suppress(OSError, ValueError):
                stream.flush()  # earlier buffered text goes out first
            self._fd = fd
            self._thread = threading.Thread(target=self._pump, name="ib-output", daemon=True)
            with signals_held():  # the thread starts with the shutdown signals blocked
                self._thread.start()

    def set_on_closed(self, callback: Callable[[], None] | None) -> None:
        self._on_closed = callback
        if self.reader_gone and callback is not None:
            callback()

    # -- producer (any thread, never blocks on the stream) ---------------------------

    @property
    def lost(self) -> int:
        """Lines that did not reach the reader, other than by the reader going away."""
        return self.dropped_total + self.abandoned_total + self.undelivered_total

    def write(self, text: str) -> None:
        if self.unavailable:
            self.undelivered_total += 1
            return
        if self.reader_gone:
            if self.error is not None:  # a failing descriptor, not a reader that left
                self.undelivered_total += 1
            return
        if self._fd is None:
            self._write_direct(text)
            return
        data = (text + "\n").encode(self._encoding, "backslashreplace")
        with self._cond:
            if self._closing or self._queued + len(data) > self._max_bytes:
                self.dropped_total += 1
                return
            self._queue.append(data)
            self._queued += len(data)
            self._cond.notify()

    def take_new_drops(self) -> int:
        """Lines dropped since the previous call."""
        with self._cond:
            fresh = self.dropped_total - self._reported
            self._reported = self.dropped_total
        return fresh

    @property
    def queued_lines(self) -> int:
        """Lines accepted and not yet fully written."""
        with self._cond:
            return len(self._queue) + self._inflight

    def begin_close(self) -> None:
        """Stop accepting lines; the thread ends once the queue is written."""
        with self._cond:
            self._closing = True
            self._cond.notify_all()

    def wait(self, timeout: float | None) -> bool:
        """Wait for the thread to finish; True if everything accepted was written."""
        thread = self._thread
        if thread is None:
            return True
        thread.join(timeout)
        return not thread.is_alive()

    def abandon(self) -> int:
        """Give up on lines still queued or half written; returns how many (counted as lost)."""
        with self._cond:
            count = len(self._queue) + self._inflight
            self.abandoned_total += count
            self._queue.clear()
            self._queued = 0
            self._cond.notify_all()
        return count

    def close(self, timeout: float = CLOSE_WAIT_S) -> bool:
        """Drain for up to ``timeout``, then abandon the rest; True if everything was written."""
        self.begin_close()
        if self.wait(timeout):
            return True
        self.abandon()
        return False

    # -- consumer --------------------------------------------------------------------

    def _write_direct(self, text: str) -> None:
        """Synchronous path for streams without a descriptor (in-memory capture)."""
        stream = self._stream
        assert stream is not None, "an unavailable stream never reaches the direct path"
        try:
            try:
                stream.write(text + "\n")
            except UnicodeEncodeError:
                safe = (text + "\n").encode(self._encoding, "backslashreplace")
                stream.write(safe.decode(self._encoding, "replace"))
            stream.flush()
        except (BrokenPipeError, ValueError):
            self._mark_gone()
        except OSError as exc:
            self.error = exc
            self.undelivered_total += 1
            self._mark_gone()

    def _mark_gone(self) -> None:
        self.reader_gone = True
        callback = self._on_closed
        if callback is not None:
            callback()

    def _next_chunk(self) -> bytes | None:
        with self._cond:
            while not self._queue:
                if self._closing:
                    return None
                self._cond.wait()
            parts: list[bytes] = []
            size = 0
            while self._queue and size < CHUNK_BYTES:
                part = self._queue.popleft()
                parts.append(part)
                size += len(part)
            self._queued -= size
            self._inflight = len(parts)
        return b"".join(parts)

    def _chunk_done(self) -> None:
        with self._cond:
            self._inflight = 0

    def _pump(self) -> None:
        fd = self._fd
        assert fd is not None
        if hasattr(signal, "pthread_sigmask"):  # also when started some other way
            signal.pthread_sigmask(signal.SIG_BLOCK, HANDLED_SIGNALS)
        while (chunk := self._next_chunk()) is not None:
            view = memoryview(chunk)
            try:
                while view:
                    try:
                        written = os.write(fd, view)
                    except BlockingIOError:
                        select.select([], [fd], [], 1.0)
                        continue
                    except InterruptedError:
                        continue
                    view = view[written:]
            except OSError as exc:
                with self._cond:
                    if exc.errno not in _GONE:
                        self.error = exc
                        self.undelivered_total += len(self._queue) + self._inflight
                    self._queue.clear()
                    self._queued = 0
                    self._inflight = 0
                self._mark_gone()
                return
            self._chunk_done()


class Console:
    """The two output streams of one command (stdout for results, stderr for notices)."""

    def __init__(self, stdout: TextIO | None, stderr: TextIO | None) -> None:
        self.stdout = BoundedWriter(stdout)
        self.stderr = BoundedWriter(stderr)

    def out(self, text: str) -> None:
        self.stdout.write(text)

    def err(self, text: str) -> None:
        self.stderr.write(text)

    def take_new_drops(self) -> tuple[int, int]:
        """``(stdout, stderr)`` lines dropped since the previous call."""
        return self.stdout.take_new_drops(), self.stderr.take_new_drops()

    def close(self, timeout: float = CLOSE_WAIT_S) -> None:
        """Bounded drain of both streams; whatever is left is abandoned and counted."""
        self.stdout.close(timeout)
        self.stderr.close(timeout)
