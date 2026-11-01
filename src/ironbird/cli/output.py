"""Terminal output that can never stall the event loop.

A reader that stops reading (an unread pipe, a paused ``less``, a slow SSH link) blocks
``write()``. Written from the event loop, that would freeze signal handling, ``--timeout``,
socket reads and the ledger. :class:`BoundedWriter` therefore hands lines to a daemon thread
through a byte-bounded queue; when the queue is full, new lines are dropped and counted, and the
owner reports the count as one coalesced coded notice. The thread writes to the raw file
descriptor, so an abandoned blocked write cannot hold a buffer lock that interpreter shutdown
would wait for. Constructing a writer never waits on the stream: text already buffered in it is
flushed by the thread (or not at all, for a caller that cannot afford that wait).

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
import codecs
import contextlib
import errno
import os
import select
import signal
import socket
import stat
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Final, TextIO

from ironbird.sigmask import HANDLED_SIGNALS, WAIT_SLICE_S, checkpoint, start_thread

MAX_QUEUED_BYTES: Final = 512 * 1024
CHUNK_BYTES: Final = 64 * 1024
CLOSE_WAIT_S: Final = 1.0
# One ``write`` call to a pipe (or any descriptor that is not a file or a terminal) carries at
# most this many bytes. A blocking write of up to ``PIPE_BUF`` bytes to a pipe is atomic (all of
# it or none), so the bytes that reached a pipe are known to within the one slice that may be in
# flight when the output is abandoned (see :meth:`BoundedWriter.abandon`). A regular file or a
# terminal has no such atomicity to keep, and a write per slice only costs the writer thread
# (and the event loop it competes with for the interpreter lock) more wake-ups, so those take
# a whole chunk per write.
WRITE_SLICE: Final = getattr(select, "PIPE_BUF", 512)
_GONE: Final = frozenset({errno.EPIPE, errno.ECONNRESET, errno.EBADF})


# Byte order marks a codec may put at the start of what it encodes (the longer UTF-32 little
# endian mark begins with the UTF-16 one, so it is tried first).
_BOMS: Final = (
    codecs.BOM_UTF32_LE,
    codecs.BOM_UTF32_BE,
    codecs.BOM_UTF8,
    codecs.BOM_UTF16_LE,
    codecs.BOM_UTF16_BE,
)
LINE_MARKER: Final = "..."  # ends a line that :func:`fit_line` cut short
_RETRY_S: Final = 0.005  # pause between two refused writes of a non-blocking descriptor


def _without_bom(data: bytes) -> bytes:
    for bom in _BOMS:
        if data.startswith(bom):
            return data[len(bom) :]
    return data


def line_codec(encoding: str | None) -> str:
    """The codec a line is written with for a stream of ``encoding``: that one when a newline is
    a single ``\\n`` byte in it (a byte order mark aside), else UTF-8. A wide codec such as
    UTF-16 would put a mark and two-byte newlines into every line of a stream that is appended
    to line by line."""
    name = encoding or "utf-8"
    try:
        newline = _without_bom("\n".encode(name))
    except (LookupError, ValueError):
        return "utf-8"
    return name if newline == b"\n" else "utf-8"


def encode_line(text: str, codec: str) -> bytes:
    """``text`` and its newline as bytes of ``codec`` (from :func:`line_codec`), encoded in one
    call so a stateful codec stays consistent, without a leading byte order mark; what the
    codec cannot represent becomes a backslash escape."""
    return _without_bom((text + "\n").encode(codec, "backslashreplace"))


def fit_line(text: str, codec: str, limit: int) -> str:
    """``text``, cut at a character and ended with :data:`LINE_MARKER` when its line (see
    :func:`encode_line`) would be longer than ``limit`` bytes; else ``text`` itself. A character
    is never split, and neither is the escape that stands for one it cannot be encoded as."""
    assert limit >= len(encode_line(LINE_MARKER, codec))
    if len(encode_line(text, codec)) <= limit:
        return text
    low, high = 0, len(text)  # the longest prefix that fits with the marker is in [low, high]
    while low < high:
        middle = (low + high + 1) // 2
        if len(encode_line(text[:middle] + LINE_MARKER, codec)) <= limit:
            low = middle
        else:
            high = middle - 1
    return text[:low] + LINE_MARKER


def _never_blocks(mode: int, rdev: int) -> bool:
    """Whether a write to a descriptor of this kind completes without a reader: a regular file or
    the null device."""
    if stat.S_ISREG(mode):
        return True
    try:
        null = os.stat(os.devnull)
    except OSError:
        return False
    return stat.S_ISCHR(mode) and rdev == null.st_rdev


def write_within(fd: int, data: bytes, timeout: float) -> bool:
    """Write all of ``data`` to ``fd`` in at most about ``timeout`` seconds without a thread and
    without changing ``fd`` (it may be shared with other processes); True if it all went out.

    A pipe or a terminal whose reader stopped takes no bytes, and polling for room says little
    (a terminal reports room while its queue cannot take a whole line), so the bytes go through
    a second, non-blocking open file description of the same file (``/proc/self/fd``), written
    and polled until they are all out or the time is up. A socket cannot be reopened and is
    sent to with ``MSG_DONTWAIT``. A regular file and the null device never block and are
    written directly. A pipe that cannot be reopened takes one atomic write (at most
    :data:`WRITE_SLICE` bytes) once it reports room; any other descriptor that cannot be reopened
    is not written. A reader that went away ends the write quietly; the result is then False.
    """
    deadline = time.monotonic() + timeout
    try:
        info = os.fstat(fd)
        if _never_blocks(info.st_mode, info.st_rdev):
            view = memoryview(data)
            while view:
                view = view[os.write(fd, view) :]
            return True
        if stat.S_ISSOCK(info.st_mode):
            return _send_within(fd, data, deadline)
        try:
            second = os.open(f"/proc/self/fd/{fd}", os.O_WRONLY | os.O_NONBLOCK | os.O_NOCTTY)
        except OSError:
            if not stat.S_ISFIFO(info.st_mode) or len(data) > WRITE_SLICE:
                return False
            poller = select.poll()
            poller.register(fd, select.POLLOUT)
            if not poller.poll(max(0, int((deadline - time.monotonic()) * 1000))):
                return False
            os.write(fd, data)
            return True
        try:
            return _push_within(second, data, deadline)
        finally:
            os.close(second)
    except (OSError, ValueError):
        return False


def _wait_room(fd: int, deadline: float, event: int) -> bool:
    """Wait until ``fd`` reports ``event`` (``POLLOUT``) or the deadline; False on the deadline."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return False
    poller = select.poll()
    poller.register(fd, event)
    # round up: a poll of 0 ms would spin until the deadline
    return bool(poller.poll(max(1, int(remaining * 1000) + 1)))


def _push_within(fd: int, data: bytes, deadline: float) -> bool:
    """Write ``data`` to the non-blocking ``fd`` until done or the deadline."""
    view = memoryview(data)
    while view:
        try:
            view = view[os.write(fd, view) :]
        except (BlockingIOError, InterruptedError):
            if not _wait_room(fd, deadline, select.POLLOUT):
                return False
            time.sleep(_RETRY_S)  # room is reported before a whole write fits; do not spin
    return True


def _send_within(fd: int, data: bytes, deadline: float) -> bool:
    """Send ``data`` to the socket ``fd`` without blocking until done or the deadline."""
    duplicate = os.dup(fd)
    try:
        sock = socket.socket(fileno=duplicate)
    except OSError:
        os.close(duplicate)
        raise
    with sock:
        view = memoryview(data)
        while view:
            try:
                view = view[sock.send(view, socket.MSG_DONTWAIT) :]
            except (BlockingIOError, InterruptedError):
                if not _wait_room(sock.fileno(), deadline, select.POLLOUT):
                    return False
                time.sleep(_RETRY_S)
    return True


def write_slice_for(fd: int) -> int:
    """Most bytes one ``write`` to ``fd`` carries: a whole chunk for a regular file or a
    terminal, :data:`WRITE_SLICE` for a pipe, a socket or anything unknown."""
    try:
        mode = os.fstat(fd).st_mode
    except OSError:
        return WRITE_SLICE
    return CHUNK_BYTES if stat.S_ISREG(mode) or stat.S_ISCHR(mode) else WRITE_SLICE


def slice_end(chunk: bytes, offset: int, limit: int = WRITE_SLICE) -> int:
    """End of the next slice of ``chunk`` from ``offset``: at most ``limit`` bytes.

    A slice that does not reach the end of the chunk is cut after the last newline it holds, so
    a line no longer than the limit is never split across two writes (and so never
    interleaved with another writer's output on a shared pipe); a longer line is cut at the limit.
    """
    assert 0 <= offset < len(chunk)
    end = min(offset + limit, len(chunk))
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
        flush_first: bool = True,
    ) -> None:
        assert max_bytes > 0
        self._stream = stream
        self._max_bytes = max_bytes
        self._on_closed = on_closed
        self._codec = line_codec(getattr(stream, "encoding", None))
        self._flush_first = flush_first
        self._cond = threading.Condition()
        self._queue: deque[bytes] = deque()
        self._queued = 0
        # The chunk the thread is writing right now: the end offset of each of its lines, and
        # how many of its bytes have been written.
        self._chunk_ends: list[int] = []
        self._chunk_written = 0
        self._closing = False
        self._abandoned = False
        self._room_waiters = 0  # producers inside write_wait, waiting for the thread to make room
        self._reported = 0
        self.dropped_total = 0  # refused: queue full, or written after the close began
        self.abandoned_total = 0  # accepted, then given up on when a drain was cut short
        self.undelivered_total = 0  # no stream to write to, or the stream failed
        self.reader_gone = False
        self.unavailable = stream is None  # e.g. standard output closed with ``>&-``
        self.error: OSError | None = None
        self._fd: int | None = None
        self._slice = WRITE_SLICE
        self._thread: threading.Thread | None = None
        fd: int | None = None
        if stream is not None:
            try:
                fd = stream.fileno()
            except (AttributeError, OSError, ValueError):
                fd = None
        if fd is not None and stream is not None:
            self._fd = fd
            self._slice = write_slice_for(fd)
            self._thread = threading.Thread(target=self._pump, name="ib-output", daemon=True)
            start_thread(self._thread)  # it starts with the shutdown signals blocked

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
        data = encode_line(text, self._codec)
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

    def write_wait(self, text: str) -> bool:
        """Queue ``text`` as one line, waiting for room instead of dropping it when the queue is
        full (a reader that stops reading stops the producer, so nothing is lost).

        For a command that reads a file and so has no clock to keep: ``replay`` and the like
        must not drop lines the way a live listener does. The wait is a condition wait in slices
        of :data:`~ironbird.sigmask.WAIT_SLICE_S`, each followed by a
        :func:`~ironbird.sigmask.checkpoint`, so a shutdown signal ends it within a slice. Returns
        ``False`` once the output is gone for good (the reader left, the stream failed or was
        never there, or the close began) and the line was not delivered. A line longer than the
        queue is accepted into an empty one.
        """
        if self._fd is None and not self.unavailable:
            self.write(text)
            return not self.reader_gone
        data = encode_line(text, self._codec)
        with self._cond:
            while True:
                if self.unavailable:
                    self.undelivered_total += 1
                    return False
                if self.reader_gone:
                    if self.error is not None:
                        self.undelivered_total += 1
                    return False
                if self._closing:
                    self.dropped_total += 1
                    return False
                if self._queued == 0 or self._queued + len(data) <= self._max_bytes:
                    self._queue.append(data)
                    self._queued += len(data)
                    self._cond.notify_all()
                    return True
                self._room_waiters += 1
                try:
                    self._cond.wait(WAIT_SLICE_S)
                finally:
                    self._room_waiters -= 1
                checkpoint()  # an owed signal ends the wait (leaving the block frees the lock)

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
        one in-flight slice (:data:`WRITE_SLICE` bytes for a pipe; a chunk for a file or terminal):
        a ``write`` already blocked on a full pipe when this runs can still complete once the
        reader catches up, so those lines reach the reader although they were counted as
        abandoned. The report can exceed the true loss
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
                stream.write(encode_line(text, self._codec).decode(self._codec, "replace"))
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
            if self._room_waiters:
                self._cond.notify_all()
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
            self._cond.notify_all()  # a producer waiting for room learns the output is gone
        self._mark_gone()

    def _pump(self) -> None:
        fd, limit = self._fd, self._slice
        assert fd is not None
        if hasattr(signal, "pthread_sigmask"):  # also when started some other way
            signal.pthread_sigmask(signal.SIG_BLOCK, HANDLED_SIGNALS)
        stream = self._stream
        if self._flush_first and stream is not None:
            # Earlier buffered text goes out first. A full pipe can hold this flush for good,
            # which is why it is the thread's and not the constructor's.
            with contextlib.suppress(OSError, ValueError):
                stream.flush()
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
                        written = os.write(fd, view[offset : slice_end(chunk, offset, limit)])
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
