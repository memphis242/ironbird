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
"""

from __future__ import annotations

import contextlib
import errno
import os
import select
import threading
from collections import deque
from collections.abc import Callable
from typing import Final, TextIO

MAX_QUEUED_BYTES: Final = 512 * 1024
CHUNK_BYTES: Final = 64 * 1024
CLOSE_WAIT_S: Final = 1.0
_GONE: Final = frozenset({errno.EPIPE, errno.ECONNRESET, errno.EBADF})


class BoundedWriter:
    """Line writer for one stream; ``write`` never blocks."""

    def __init__(
        self,
        stream: TextIO,
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
        self._closing = False
        self._reported = 0
        self.dropped_total = 0
        self.reader_gone = False
        self.error: OSError | None = None
        self._fd: int | None = None
        self._thread: threading.Thread | None = None
        try:
            fd = stream.fileno()
        except (AttributeError, OSError, ValueError):
            fd = None
        if fd is not None:
            with contextlib.suppress(OSError, ValueError):
                stream.flush()  # earlier buffered text goes out first
            self._fd = fd
            self._thread = threading.Thread(target=self._pump, name="ib-output", daemon=True)
            self._thread.start()

    def set_on_closed(self, callback: Callable[[], None] | None) -> None:
        self._on_closed = callback
        if self.reader_gone and callback is not None:
            callback()

    # -- producer (any thread, never blocks on the stream) ---------------------------

    def write(self, text: str) -> None:
        if self.reader_gone:
            return
        if self._fd is None:
            self._write_direct(text)
            return
        data = (text + "\n").encode(self._encoding, "backslashreplace")
        with self._cond:
            if self._closing:
                return
            if self._queued + len(data) > self._max_bytes:
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

    def close(self, timeout: float = CLOSE_WAIT_S) -> bool:
        """Let queued lines drain for up to ``timeout``; True if everything was written."""
        with self._cond:
            self._closing = True
            self._cond.notify_all()
        thread = self._thread
        if thread is not None:
            thread.join(timeout)
            return not thread.is_alive()
        return True

    # -- consumer --------------------------------------------------------------------

    def _write_direct(self, text: str) -> None:
        """Synchronous path for streams without a descriptor (in-memory capture)."""
        try:
            try:
                self._stream.write(text + "\n")
            except UnicodeEncodeError:
                safe = (text + "\n").encode(self._encoding, "backslashreplace")
                self._stream.write(safe.decode(self._encoding, "replace"))
            self._stream.flush()
        except (BrokenPipeError, ValueError):
            self._mark_gone()
        except OSError as exc:
            self.error = exc
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
        return b"".join(parts)

    def _pump(self) -> None:
        fd = self._fd
        assert fd is not None
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
                if exc.errno not in _GONE:
                    self.error = exc
                with self._cond:
                    self._queue.clear()
                    self._queued = 0
                self._mark_gone()
                return


class Console:
    """The two output streams of one command (stdout for results, stderr for notices)."""

    def __init__(self, stdout: TextIO, stderr: TextIO) -> None:
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
        self.stdout.close(timeout)
        self.stderr.close(timeout)
