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

import bisect
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
# One ``write`` call carries at most this many bytes. A blocking write of up to ``PIPE_BUF``
# bytes to a pipe is atomic (all of it or none), so the bytes that reached a pipe are known
# to within the one slice that may be in flight when the output is abandoned (see
# :meth:`BoundedWriter.abandon`).
WRITE_SLICE: Final = getattr(select, "PIPE_BUF", 512)
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


def slice_end(chunk: bytes, offset: int) -> int:
    """End of the next slice of ``chunk`` from ``offset``: at most :data:`WRITE_SLICE` bytes.

    A slice that does not reach the end of the chunk is cut after the last newline it holds, so
    a line no longer than the slice limit is never split across two writes (and so never
    interleaved with another writer's output on a shared pipe); a longer line is cut at the limit.
    """
    assert 0 <= offset < len(chunk)
    end = min(offset + WRITE_SLICE, len(chunk))
    if end < len(chunk):
        newline = chunk.rfind(b"\n", offset, end)
        if newline >= 0:
            end = newline + 1
    assert offset < end
    return end


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
        # The chunk the thread is writing right now: the end offset of each of its lines, and
        # how many of its bytes have been written.
        self._chunk_ends: list[int] = []
        self._chunk_written = 0
        self._closing = False
        self._abandoned = False
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
        if self._fd is None and not self.unavailable:
            if not self.reader_gone:
                self._write_direct(text)
            elif self.error is not None:  # a failing descriptor, not a reader that left
                self.undelivered_total += 1
            return
        data = (text + "\n").encode(self._encoding, "backslashreplace")
        with self._cond:
            if self.unavailable:
                self.undelivered_total += 1
            elif self.reader_gone:
                if self.error is not None:  # a failing descriptor, not a reader that left
                    self.undelivered_total += 1
            elif self._closing or self._queued + len(data) > self._max_bytes:
                self.dropped_total += 1
            else:
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
            return len(self._queue) + self._unwritten_lines()

    def _unwritten_lines(self) -> int:
        """Lines of the current chunk not yet fully written (``_cond`` held)."""
        return len(self._chunk_ends) - bisect.bisect_right(self._chunk_ends, self._chunk_written)

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
        """Give up on lines still queued or only partly written; returns how many (counted lost).

        Lines that reached the descriptor are not counted. The thread writes nothing more once it
        sees the flag, which it checks before every write. The count is exact except for at most
        one in-flight slice (:data:`WRITE_SLICE` bytes): a ``write`` already blocked on a full
        pipe when this runs can still complete once the reader catches up, so those lines reach
        the reader although they were counted as abandoned. The report can exceed the true loss
        by that slice and never falls short of it.
        """
        with self._cond:
            count = len(self._queue) + self._unwritten_lines()
            self.abandoned_total += count
            self._queue.clear()
            self._queued = 0
            self._chunk_ends = []
            self._chunk_written = 0
            self._abandoned = self._closing = True
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
        with self._cond:
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
            ends: list[int] = []
            size = 0
            while self._queue and size < CHUNK_BYTES:
                part = self._queue.popleft()
                parts.append(part)
                size += len(part)
                ends.append(size)
            self._queued -= size
            self._chunk_ends = ends
            self._chunk_written = 0
        return b"".join(parts)

    def _note_written(self, written: int) -> bool:
        """Record progress on the current chunk; False once the owner abandoned the output."""
        with self._cond:
            if self._abandoned:
                return False
            self._chunk_written = written
            return True

    def _chunk_failed(self, exc: OSError) -> None:
        """The descriptor failed: nothing more will be written, whoever writes next is told."""
        with self._cond:
            if not self._abandoned:
                if exc.errno not in _GONE:
                    self.error = exc
                    self.undelivered_total += len(self._queue) + self._unwritten_lines()
                self._queue.clear()
                self._queued = 0
                self._chunk_ends = []
                self._chunk_written = 0
            self.reader_gone = True  # before the lock is released: later lines are counted
        self._mark_gone()

    def _pump(self) -> None:
        fd = self._fd
        assert fd is not None
        if hasattr(signal, "pthread_sigmask"):  # also when started some other way
            signal.pthread_sigmask(signal.SIG_BLOCK, HANDLED_SIGNALS)
        while (chunk := self._next_chunk()) is not None:
            view = memoryview(chunk)
            offset = 0
            try:
                while offset < len(chunk):
                    # Checked before every write, the retries after a refusal included: once the
                    # owner abandons the output, nothing more reaches the descriptor.
                    if self._abandoned:
                        return
                    try:
                        written = os.write(fd, view[offset : slice_end(chunk, offset)])
                    except BlockingIOError:
                        select.select([], [fd], [], 1.0)
                        continue
                    except InterruptedError:
                        continue
                    offset += written
                    if not self._note_written(offset):
                        return
            except OSError as exc:
                self._chunk_failed(exc)
                return


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
