"""The engine: push ingest of raw frames, sinks, and per-consumer captures.

Data flow (one process, one event loop)::

    transport reader callback --> ingest: seq, Frame, peek MID, Codec.check
                                    |  sinks (recorder, ...) see each frame first, synchronously
                                    |  malformed or unknown: counted, ledgered, reported (coalesced)
                                    +-> each consumer's bounded queue -> filter -> decode -> render

Ingest is cheap and never decodes fields. A frame that fails ``Codec.check`` is still a frame:
it reaches the sinks and every consumer, so nothing a link carried is hidden. A frame that
passes carries the check's result, which a live consumer's decode reuses. A consumer that falls
behind loses frames from its own queue only (counted and reported as ``IB-ENG-001``); the sinks
have already seen them. Memory is bounded at every hop: a transport copies at most
``max_packet_bytes`` per datagram out of the OS, and a consumer queue holds at most
``queue_bytes`` (every frame is charged its data plus a fixed overhead), counting frames the
consumer has taken in a batch and not yet finished with.

The engine owns the transports a session has open, one per link end, shared by that end's
sends and receives.
"""

from __future__ import annotations

import asyncio
import sys
import time
import traceback
from collections import deque
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Final, Protocol

from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec, Resolved
from ironbird.config import SERIAL_TRANSPORT, Link, SystemConfig
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.frame import (
    FLAG_GARBAGE,
    FLAG_HAS_MID,
    FLAG_STALE,
    FLAG_TRUNCATED,
    FLAGS_BY_BITS,
    KNOWN_FLAG_BITS,
    MAX_STREAM_ID,
    Decoded,
    Direction,
    Frame,
    GapReason,
    Stream,
    garbage_error,
)
from ironbird.ledger import Ledger
from ironbird.notice import Level
from ironbird.transport import LinkEnd, Receiver, Transport, TransportClosed, open_transport

MAX_TRACKED_MIDS: Final = 1024
CONSUMER_QUEUE_FRAMES: Final = 16384
CONSUMER_QUEUE_BYTES: Final = 32 * 1024 * 1024
# What a queued frame costs besides its data: the Frame tuple, the bytes object header and the
# check token the frame carries. An estimate, deliberately on the high side.
FRAME_OVERHEAD_BYTES: Final = 320
_RELEASE_EVERY: Final = 64  # offers between releases of the charges of frames a consumer popped
READ_BATCH_FRAMES: Final = 512  # most frames one ``next_batch`` hands a consumer
_MAX_STREAMS: Final = MAX_STREAM_ID + 1
OPEN_TIMEOUT_S: Final = 5.0  # longest a transport may take to open (name resolution, bind)

Reporter = Callable[..., None]  # Session.report: (error, *, context, level, evidence, sample)


class FrameSink(Protocol):
    """Receives every frame, synchronously, before any consumer queue."""

    def append(self, frame: Frame) -> None:
        """Must be cheap and must not raise; a sink that does is detached and reported once."""
        ...


class GapSink(Protocol):
    """A sink that also wants to know about frames lost before they were given to it (frames the
    kernel dropped never received a sequence number). Optional: the engine calls ``gap`` on a
    sink that has it, at its once-a-second tick, and detaches a sink whose ``gap`` raises."""

    def gap(
        self,
        stream: Stream,
        reason: GapReason,
        lost: int,
        first_ns: int,
        last_ns: int,
        first_seq: int,
        last_seq: int,
    ) -> None:
        """``lost`` frames of ``stream`` went missing between wall times ``first_ns`` and
        ``last_ns``, while the engine's sequence ran from ``first_seq`` to ``last_seq`` (the
        loss lies somewhere in that range of the capture)."""
        ...


class LinkSink(Protocol):
    """A sink that also wants to know when a link end goes down or comes back. Optional: the
    engine calls ``link_event`` on a sink that has it, and detaches a sink whose method raises."""

    def link_event(self, stream: Stream, up: bool, time_ns: int, text: str) -> None:
        """The receive ``stream``'s link went down (``up`` false) or came back at ``time_ns``;
        ``text`` is a one-line description."""
        ...


@dataclass(frozen=True, slots=True)
class MidStatSnapshot:
    count: int
    last_ns: int


@dataclass(slots=True)
class _MidStat:
    """Mutable counters, owned by the engine; the outside sees :class:`MidStatSnapshot`."""

    count: int
    last_ns: int


@dataclass(frozen=True, slots=True)
class CaptureStats:
    """A frozen snapshot of what one capture was offered and what it could not hold."""

    frames: int  # offered to this capture's queue
    not_shown: int  # refused because the queue was full (sinks saw them either way)
    unread: int = 0  # queued and not yet read
    queued_bytes: int = 0  # charged to the queue now: unread frames plus a batch still held
    peak_bytes: int = 0  # most the queue ever counted as held (never below what it held)
    not_shown_bytes: int = 0  # data bytes of the frames in ``not_shown``


class _Closing(Exception):
    """The engine shut down while a transport was opening; the caller reports it in its terms."""


def _capture_after_close() -> IronbirdError:
    return IronbirdError(Code.INTERNAL, "the session is closed; it cannot capture again")


def _second_reader() -> IronbirdError:
    return IronbirdError(
        Code.INTERNAL,
        "a capture has one reader; a second consumer is reading it at the same time",
    )


class _FrameQueue:
    """Bounded FIFO between ingest (producer, event loop) and one consumer (event loop).

    Bounded by frame count and by bytes. Every queued frame is charged ``len(data) +
    FRAME_OVERHEAD_BYTES``; a frame that would take either bound over is refused (newest
    dropped, counted). The charge is released when the consumer is done with the frame:

    * a frame taken off the left of ``items`` (``popleft``, which is how a consumer reads the live
      deque) is released by a ``put`` that finds the bound reached (and every few offers), which
      compares the charges with ``items``;
    * a batch taken with :meth:`take` stays charged until the consumer asks for more
      (:meth:`wait`), so frames it is still processing count against the bound.
    """

    __slots__ = (
        "_bytes",
        "_charges",
        "_closed",
        "_held",
        "_limit",
        "_limit_bytes",
        "_waiter",
        "dropped",
        "dropped_bytes",
        "error",
        "items",
        "offered",
        "peak_bytes",
    )

    def __init__(self, limit: int, limit_bytes: int) -> None:
        assert limit > 0 and limit_bytes > 0
        self.items: deque[Frame] = deque()
        self._charges: deque[int] = deque()  # the charge of each of ``items``, same order
        self._bytes = 0  # charges of ``items`` not yet released, plus ``_held``
        self._held = 0  # charges of the batch the consumer holds
        self._limit = limit
        self._limit_bytes = limit_bytes
        self._waiter: asyncio.Future[None] | None = None
        self._closed = False
        self.error: IronbirdError | None = None  # why the queue was closed, if it failed
        self.offered = 0
        self.dropped = 0
        self.dropped_bytes = 0
        self.peak_bytes = 0

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def queued_bytes(self) -> int:
        """Bytes charged to the queue now (unread frames and the held batch)."""
        self._release_taken()
        return self._bytes

    def _release_taken(self) -> None:
        """Release the charges of frames the consumer popped off ``items`` itself."""
        charges = self._charges
        excess = len(charges) - len(self.items)
        assert excess >= 0, "frames may only leave the queue from the left"
        while excess:
            self._bytes -= charges.popleft()
            excess -= 1

    def put(self, frame: Frame) -> bool:
        """Queue ``frame``; ``False`` (newest dropped, counted) when either bound would be passed
        or the queue is closed.

        The charges of frames the consumer has popped are released lazily: when the byte bound
        would be passed, and every ``_RELEASE_EVERY`` offers so the charge list stays short. In
        between the queue counts those frames as still queued, so its figure is never below the
        truth and the bound holds."""
        if self._closed:
            return False
        self.offered += 1
        if not self.offered % _RELEASE_EVERY:
            self._release_taken()
        size = len(frame.data) + FRAME_OVERHEAD_BYTES
        total = self._bytes + size
        items = self.items
        if total > self._limit_bytes:
            self._release_taken()
            total = self._bytes + size
        if total > self._limit_bytes or len(items) >= self._limit:
            self.dropped += 1
            self.dropped_bytes += size - FRAME_OVERHEAD_BYTES
            return False
        items.append(frame)
        self._charges.append(size)
        self._bytes = total
        if total > self.peak_bytes:  # noqa: PLR1730 - the hot path avoids a call
            self.peak_bytes = total
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        return True

    def take(self, count: int) -> list[Frame]:
        """Remove up to ``count`` oldest frames for the consumer to hold. Their charge stays on
        the queue until the consumer waits for more."""
        assert not self._held, "the previous batch is released by waiting for the next"
        self._release_taken()
        taken = min(count, len(self.items))
        popleft, charge = self.items.popleft, self._charges.popleft
        batch = [popleft() for _ in range(taken)]
        self._held = sum([charge() for _ in range(taken)])
        return batch

    def close(self, error: IronbirdError | None = None) -> None:
        """No more frames; the consumer drains what is queued, then its iteration ends, or raises
        ``error`` when the queue was closed because its source failed."""
        self._closed = True
        if self.error is None:
            self.error = error
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    async def wait(self) -> None:
        """Wait until a frame is queued or the queue is closed; ``items`` is empty afterwards only
        when the queue is closed and drained. One waiter at a time: a queue has a single wake-up
        slot, so a second waiter would be starved or would split the stream, and is refused.

        Asking for more means the consumer is done with the batch it took last, so that is
        released here. Raises the error the queue was closed with once it is drained."""
        if self._waiter is not None:
            raise _second_reader()
        if self._held:
            self._bytes -= self._held
            self._held = 0
        if not self.items and not self._closed:
            self._waiter = asyncio.get_running_loop().create_future()
            try:
                await self._waiter
            finally:
                self._waiter = None
        if not self.items and self.error is not None:
            raise self.error


@dataclass(slots=True, eq=False)
class _Subscription:
    """One consumer queue attached to one link end's receive stream."""

    queue: _FrameQueue
    io: _LinkIO
    dropped: int = 0  # frames the queue refused
    dropped_to_sinks: int = 0  # of those, frames a sink had already received
    reported: int = 0


@dataclass(slots=True, eq=False)
class _SinkRef:
    """How many holders asked for a sink, and whether it receives frames (not after a failure)."""

    sink: FrameSink
    holders: int = 0
    attached: bool = False


@dataclass(slots=True)
class _KernelWatch:
    seen: int = 0
    last_ns: int = 0  # when the counter was last polled, for the GAP's time bounds
    last_seq: int = 0  # the engine's last sequence number at that poll


class _LinkIO:
    """One link end: its two streams, its transport, and the consumers fed from it."""

    def __init__(
        self,
        engine: Engine,
        link: Link,
        end: LinkEnd,
        rx_stream: Stream,
        tx_stream: Stream,
        tx_peer: str,
        label: str,
        codec: Codec,
    ) -> None:
        self.engine = engine
        self.link = link
        self.end = end
        self.rx_stream = rx_stream
        self.tx_stream = tx_stream
        self.tx_peer = tx_peer  # where our frames go, as the TX frames' ``source``
        self.label = label  # the peer's node name, for "from fc" in notices
        self.codec = codec
        self.transport: Transport | None = None
        self.receiving = False
        self.description = rx_stream.endpoint
        self.lock = asyncio.Lock()
        self.subs: list[_Subscription] = []
        self.kernel = _KernelWatch()
        self.opening: asyncio.Future[None] | None = None  # the transport open in progress
        self.lossy = False  # a consumer queue of this end has refused a frame
        self.failure: IronbirdError | None = None  # the receiving transport failed for good
        # When the link went down: (wall time, engine sequence number); None while it is up.
        self.down: tuple[int, int] | None = None

    def on_data(self, data: bytes, time_ns: int, source: str, orig_len: int, flags: int) -> None:
        """The transport's receiver. A bug in ingest is reported, never raised into the loop."""
        try:
            self.engine._ingest(self, data, time_ns, source, orig_len, flags)
        except Exception as exc:
            self.engine._ingest_failed(exc)


class Capture:
    """Async iterator of every frame a set of link ends receives: valid, malformed, unknown."""

    def __init__(
        self,
        engine: Engine,
        queue: _FrameQueue,
        sinks: Sequence[FrameSink],
    ) -> None:
        self._engine = engine
        self._queue = queue
        self._sinks = tuple(sinks)
        self._subs: list[_Subscription] = []
        self._closed = False

    @property
    def streams(self) -> tuple[Stream, ...]:
        return tuple(sub.io.rx_stream for sub in self._subs)

    @property
    def addresses(self) -> tuple[tuple[Stream, tuple[str, int] | None], ...]:
        """Each receive stream with the network address it is bound to (``None`` if none)."""
        return tuple(
            (
                sub.io.rx_stream,
                sub.io.transport.local_address if sub.io.transport is not None else None,
            )
            for sub in self._subs
        )

    def __aiter__(self) -> AsyncIterator[Frame]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Frame]:
        # The capture's queue holds the unread frames; one is taken only as it is yielded, so an
        # iteration that is abandoned (a ``break``) leaves the rest for the next one.
        queue = self._queue
        items = queue.items
        while True:
            if not items:
                await queue.wait()
                if not items:
                    return
            yield items.popleft()

    async def wait_frames(self) -> deque[Frame]:
        """Wait until a frame is unread, then return the capture's own queue of unread frames,
        oldest first (empty once the capture is closed or its engine shut down and everything was
        read).

        The consumer takes frames off the left (``popleft``) as it handles them; what it has not
        taken stays in the capture for the next read, so a consumer that stops early loses
        nothing. The deque is live: frames that arrive meanwhile are appended to it. A frame
        stops counting against the capture's byte bound once it is taken, so a consumer holds
        frames only one at a time. Raises the failure that ended the capture (``IB-TX-003``)
        once everything received before it was read.
        """
        await self._queue.wait()
        return self._queue.items

    async def next_batch(self) -> list[Frame]:
        """Up to :data:`READ_BATCH_FRAMES` unread frames, oldest first, waiting for at least one;
        empty once the capture is closed (or its engine shut down) and everything was read.

        The caller owns the frames it is given. A consumer that decodes per frame loops over the
        batch itself, which is cheaper than iterating the capture. The batch stays charged to the
        capture's byte bound until the next call, so a consumer cannot hold more than the bound
        allows by sitting on batches. A capture has one reader: a second read that waits while
        the first is waiting raises ``IB-INT-001``.
        """
        await self._queue.wait()
        return self._queue.take(READ_BATCH_FRAMES)

    def stats(self) -> CaptureStats:
        queue = self._queue
        return CaptureStats(
            queue.offered,
            queue.dropped,
            len(queue.items),
            queue.queued_bytes,
            queue.peak_bytes,
            queue.dropped_bytes,
        )

    async def close(self) -> None:
        """Stop receiving; transports no other capture needs are closed. Queued frames can still
        be drained by the iterator, which then ends."""
        if self._closed:
            return
        self._closed = True
        try:
            await self._engine._release_capture(self)
        finally:
            self._queue.close()


class FrameDecoder:
    """Decodes frames for a consumer through the codec of the frame's node.

    ``report_checked`` says whether a frame that fails ``Codec.check`` is reported here: a live
    consumer passes ``False`` because ingest already reported it, a consumer with no ingest
    (replay) passes ``True``. Either way the error is returned. ``codec_for`` gives the codec of a
    node and ``reject`` takes a reported frame: the engine's own ledger-and-notice path live, the
    consumer's tally for replay, which has no engine.

    A frame that live ingest accepted carries what its check established, so its fields are
    unpacked without checking the packet a second time; every other frame goes through the whole
    of ``Codec.decode``. A garbage frame is never unpacked: it is malformed by definition.
    """

    def __init__(
        self,
        codec_for: Callable[[str], Codec],
        reject: Callable[[Frame, DecodeError], None],
        *,
        report_checked: bool,
    ) -> None:
        self._codec_for = codec_for
        self._reject = reject
        self._report_checked = report_checked
        self._codecs: dict[str, Codec] = {}

    def decode(self, frame: Frame) -> Decoded | DecodeError:
        stream = frame.stream
        result: Decoded | DecodeError
        if int(frame.flags) & FLAG_GARBAGE:  # int(): an IntFlag operation is a Python call
            result = garbage_error(len(frame.data))
            if self._report_checked:
                self._reject(frame, result)
            return result
        codec = self._codecs.get(stream.node)
        if codec is None:
            codec = self._codecs[stream.node] = self._codec_for(stream.node)
        checked = frame.checked
        if checked is not None:
            decoded = codec.decode_resolved(stream.kind, checked, frame.data)
            if not isinstance(decoded, DecodeError):
                return Decoded(frame, decoded)
            result = decoded
        elif frame.flags & FLAG_TRUNCATED:
            result = codec.oversize_error(stream.kind, frame.data, frame.orig_len)
        else:
            decoded = codec.decode(stream.kind, frame.data)
            if not isinstance(decoded, DecodeError):
                return Decoded(frame, decoded)
            result = decoded
        if self._report_checked:
            self._reject(frame, result)
        return result


class Engine:
    """Ingest, sinks, captures and the session's transports. Owned by one session."""

    def __init__(
        self,
        config: SystemConfig,
        ledger: Ledger,
        report: Reporter,
        codec_for: Callable[[str], Codec],
        *,
        withdraw: Callable[[Code, str], None] | None = None,
    ) -> None:
        self._config = config
        self._withdraw = withdraw
        self._max_packet_bytes = config.max_packet_bytes
        self._ledger = ledger
        self._report = report
        self._codec_for = codec_for
        self._seq = 0
        self._closed = False
        self._sinks: tuple[FrameSink, ...] = ()
        self._sink_refs: dict[int, _SinkRef] = {}  # by id(sink)
        self._ios: dict[tuple[str, LinkEnd], _LinkIO] = {}
        self._labels: dict[int, str] = {}
        self._stats: dict[int, _MidStat] = {}
        self._stats_overflow = 0
        self._malformed_total = 0  # unknown-MID and malformed frames, filtered by a consumer or not
        self._resync_frames = 0  # garbage and stale frames a stream framer produced
        self._rcvbuf_warned = False

    # -- read-only state ---------------------------------------------------------------

    @property
    def last_seq(self) -> int:
        """Sequence number of the last frame built (0 before the first)."""
        return self._seq

    @property
    def stats_overflow(self) -> int:
        """Known-MID packets not counted because the per-MID table was full."""
        return self._stats_overflow

    @property
    def malformed_total(self) -> int:
        return self._malformed_total

    @property
    def resync_frames(self) -> int:
        """Frames of skipped bytes (``GARBAGE``) and flushed partial packets (``STALE``) that
        serial framing produced; the transport reports them as ``IB-TX-007``, not per frame."""
        return self._resync_frames

    def stats_snapshot(self) -> dict[int, MidStatSnapshot]:
        return {mid: MidStatSnapshot(s.count, s.last_ns) for mid, s in self._stats.items()}

    # -- sinks -----------------------------------------------------------------------

    def add_sink(self, sink: FrameSink) -> None:
        """Attach ``sink``. A sink is attached once however many holders ask (a session-level
        attach and several captures, say) and stays until the last holder removes it, so it sees
        each frame once. Asking for a sink that failed and was detached attaches it again."""
        ref = self._sink_refs.get(id(sink))
        if ref is None:
            ref = self._sink_refs[id(sink)] = _SinkRef(sink)
        ref.holders += 1
        if not ref.attached:
            ref.attached = True
            self._sinks = (*self._sinks, sink)

    def remove_sink(self, sink: FrameSink) -> None:
        """Release one holder of ``sink``; it detaches when the last holder lets go. A sink that
        was never added is ignored; one that failed is still held until each holder releases it."""
        ref = self._sink_refs.get(id(sink))
        if ref is None:
            return
        ref.holders -= 1
        if ref.holders > 0:
            return
        del self._sink_refs[id(sink)]
        self._drop_from_ingest(sink)

    def _drop_from_ingest(self, sink: FrameSink) -> None:
        self._sinks = tuple(s for s in self._sinks if s is not sink)

    def _sink_failed(self, sink: FrameSink, seq: int, exc: Exception) -> None:
        """A sink raised on the frame (or gap) at sequence ``seq``: it is detached at once (it
        would fail on every later one as well), and the failure is reported and ledgered this one
        time."""
        name = type(sink).__name__
        ref = self._sink_refs.get(id(sink))
        if ref is not None:
            ref.attached = False  # its holders still count, so a later holder cannot be undone
        self._drop_from_ingest(sink)
        self._ledger.record("state", "sink.detached", sink=name, seq=seq, error=type(exc).__name__)
        self._report(
            IronbirdError(
                Code.INTERNAL,
                f"frame sink {name} failed on frame {seq} and was detached: "
                f"{type(exc).__name__}: {exc}",
                details="".join(traceback.format_exception(exc)),
                cause=exc,
            ),
            context=f"sink {name}",
        )

    def _ingest_failed(self, exc: Exception) -> None:
        self._report(
            IronbirdError(
                Code.INTERNAL,
                f"frame ingest failed: {type(exc).__name__}: {exc}",
                details="".join(traceback.format_exception(exc)),
                cause=exc,
            ),
            context="ingest",
        )

    # -- link ends ---------------------------------------------------------------------

    def streams_for(self, ends: Sequence[tuple[Link, LinkEnd]]) -> tuple[Stream, ...]:
        """The receive and send stream of each of ``ends``, in order. Stream ids are assigned
        the first time a link end is named, so a recorder can declare its streams before the
        capture that feeds it opens."""
        found: list[Stream] = []
        for link, end in ends:
            io = self._io(link, end)
            found += (io.rx_stream, io.tx_stream)
        return tuple(found)

    def _io(self, link: Link, end: LinkEnd) -> _LinkIO:
        existing = self._ios.get((link.name, end))
        if existing is not None:
            return existing
        if len(self._labels) + 2 > _MAX_STREAMS:
            raise IronbirdError(Code.CFG_LINK, "too many link ends in use (stream ids exhausted)")
        match link.transport:
            case "udp":
                spec = link.udp
                assert spec is not None
                if end is LinkEnd.SOURCE:
                    rx_where, tx_where = spec.telemetry_endpoint, spec.command_endpoint
                else:
                    rx_where, tx_where = spec.command_endpoint, spec.telemetry_endpoint
                rx_text, tx_text, tx_peer = f"udp {rx_where}", f"udp {tx_where}", str(tx_where)
            case "serial":
                serial = link.serial
                assert serial is not None
                rx_text = tx_text = f"serial {serial.device}"  # one device carries both ways
                tx_peer = ""
            case _:  # pragma: no cover - the config accepts only transports with a branch here
                raise AssertionError(f"no stream description for transport {link.transport}")
        if end is LinkEnd.SOURCE:
            rx_kind, tx_kind, peer = MessageKind.TELEMETRY, MessageKind.COMMAND, link.destination
        else:
            rx_kind, tx_kind, peer = MessageKind.COMMAND, MessageKind.TELEMETRY, link.source
        base = len(self._labels)
        rx = Stream(
            base, link.name, link.destination, rx_kind, Direction.RX, link.transport, rx_text
        )
        tx = Stream(
            base + 1, link.name, link.destination, tx_kind, Direction.TX, link.transport, tx_text
        )
        self._labels[rx.id] = peer
        self._labels[tx.id] = peer
        io = _LinkIO(self, link, end, rx, tx, tx_peer, peer, self._codec_for(link.destination))
        self._ios[(link.name, end)] = io
        return io

    def _transport_report(self, err: IronbirdError, label: str) -> None:
        if err.code is Code.TX_RCVBUF:  # one notice per session, not per listener
            if self._rcvbuf_warned:
                return
            self._rcvbuf_warned = True
        if err.code is Code.TX_RESYNC:  # the line's bytes cause it at will: evidence, sampled
            self._report(err, context=label, evidence=True, sample=f"{err.code.value}:{label}")
            return
        self._report(err, context=label)

    def _new_transport(self, io: _LinkIO) -> Transport:
        return open_transport(
            io.link,
            io.end,
            lambda err: self._transport_report(err, io.label),
            failed=lambda err: self._receive_failed(io, err),
            max_packet_bytes=self._max_packet_bytes,
            codec=io.codec,
            link_state=lambda up, time_ns, err: self._link_state(io, up, time_ns, err),
        )

    def _link_state(self, io: _LinkIO, up: bool, time_ns: int, error: IronbirdError | None) -> None:
        """A link end that was up lost its device, or got it back (serial reconnect).

        Going down is ledgered, shown once (coalesced) as ``IB-TX-008`` and recorded as a MARK
        by sinks that take them. Coming back is ledgered, shown as an info notice, marked, and
        the time in between becomes one ``link_down`` GAP on the receive stream (UDP has no link
        to lose, so it never gets one).
        """
        label = io.description
        if not up:
            if io.down is not None:
                return
            assert error is not None, "a link goes down with a reason"
            io.down = (time_ns, self._seq)
            self._ledger.record(
                "state",
                "link.down",
                label=label,
                code=error.code.value,
                summary=error.summary,
                seq=self._seq,
            )
            self._notify_link(io, False, time_ns, f"{label}: link lost")
            self._report(error, context=f"link down {label}")
            return
        started = io.down
        if started is None:
            return
        io.down = None
        seconds = max(0, time_ns - started[0]) / 1e9
        self._ledger.record(
            "state", "link.up", label=label, down_s=round(seconds, 3), seq=self._seq
        )
        self._notify_link(io, True, time_ns, f"{label}: link restored after {seconds:.1f} s")
        self._link_down_gap(io, started, time_ns)
        self._report(
            IronbirdError(
                Code.TX_DOWN, f"{label}: link restored after {seconds:.1f} s; a gap is recorded"
            ),
            context=f"link up {label}",
            level=Level.INFO,
        )

    def _link_down_gap(self, io: _LinkIO, started: tuple[int, int], end_ns: int) -> None:
        """Tell the gap sinks the receive stream of ``io`` was down from ``started`` until
        ``end_ns``. How many frames that cost is unknown, so the GAP counts the interruption (1)."""
        if not io.receiving:
            return
        down_ns, down_seq = started
        self._notify_gap(
            io.rx_stream, GapReason.LINK_DOWN, 1, down_ns, max(down_ns, end_ns), down_seq, self._seq
        )

    def _close_down_link(self, io: _LinkIO) -> None:
        """The transport of ``io`` is closing: a link still down ends its gap now."""
        started, io.down = io.down, None
        if started is None:
            return
        now_ns = time.time_ns()
        self._ledger.record(
            "state", "link.down_at_close", label=io.description, since_ns=started[0], seq=self._seq
        )
        self._link_down_gap(io, started, now_ns)

    def _notify_link(self, io: _LinkIO, up: bool, time_ns: int, text: str) -> None:
        for sink in self._sinks:
            event = getattr(sink, "link_event", None)
            if event is None:
                continue
            try:
                event(io.rx_stream, up, time_ns, text)
            except Exception as exc:
                self._sink_failed(sink, self._seq, exc)

    def _receive_failed(self, io: _LinkIO, error: IronbirdError) -> None:
        """The receiving transport of ``io`` cannot receive any more (``IB-TX-003``).

        Every capture fed from it ends once it has read what arrived before the failure, by
        raising ``error``; the failure is ledgered here and again by whoever handles the raised
        error, so the trace shows both when it happened and where it surfaced. A capture opened
        on the end afterwards is refused with the same error until the transport is replaced.
        """
        if io.failure is not None:
            return
        io.failure = error
        if self._withdraw is not None:  # the raised error is the last word on this failure
            self._withdraw(error.code, io.label)
        self._ledger.record(
            "state",
            "rx.failed",
            label=io.description,
            code=error.code.value,
            summary=error.summary,
        )
        for sub in list(io.subs):
            self._poll_sub(sub)
            sub.queue.close(error)

    async def _open_transport(
        self, io: _LinkIO, transport: Transport, receiver: Receiver | None
    ) -> None:
        """Open ``transport``, bounded in time and cut short by :meth:`close`.

        Runs under the link end's lock, which a close (and every other user of the end) waits
        for, so a resolver that never answers must not hold it for long. Raises
        :class:`_Closing` when the engine shut down meanwhile.
        """
        task = asyncio.ensure_future(transport.open(receiver))
        io.opening = task
        me = asyncio.current_task()
        try:
            async with asyncio.timeout(OPEN_TIMEOUT_S):
                await task
        except TimeoutError as exc:
            raise IronbirdError(
                Code.TX_OPEN,
                f"cannot open {transport.description}: no answer within {OPEN_TIMEOUT_S:g} s "
                "(is the host name resolvable?)",
                cause=exc,
            ) from exc
        except asyncio.CancelledError:
            if task.cancelled() and self._closed and me is not None and me.cancelling() == 0:
                raise _Closing from None  # close() cut the open short; our caller is not cancelled
            raise
        finally:
            io.opening = None
        if self._closed:
            await transport.close()
            raise _Closing

    async def _attach(self, io: _LinkIO, sub: _Subscription) -> None:
        """Subscribe ``sub`` to ``io``, opening its receiving transport if none is open.

        The subscription is added under the link end's lock, so a close that is deciding whether
        the transport is still needed sees it. A closed engine opens nothing.
        """
        async with io.lock:
            if self._closed:
                raise _capture_after_close()
            if io.failure is not None:
                raise io.failure
            if not io.receiving:
                transport = self._new_transport(io)
                if io.transport is not None and io.link.transport == SERIAL_TRANSPORT:
                    # A serial device is locked by its opener, so the send-only transport lets go
                    # of it before the receiving one opens (a UDP socket can share its port).
                    stale, io.transport = io.transport, None
                    await stale.close()
                try:
                    await self._open_transport(io, transport, io.on_data)
                except _Closing:
                    raise _capture_after_close() from None
                previous, io.transport, io.receiving = io.transport, transport, True
                io.description = transport.description
                io.kernel = _KernelWatch(0, time.time_ns(), self._seq)
                facts: dict[str, Any] = dict(transport.facts())
                self._ledger.record("state", f"{io.link.transport}.open", **facts)
                io.subs.append(sub)
                if previous is not None:  # a send-only transport the receiving one replaces
                    await previous.close()
            else:
                io.subs.append(sub)

    async def _stop_if_idle(self, io: _LinkIO) -> None:
        """Close the receiving transport of ``io`` unless a capture subscribed in the meantime.

        The decision and the close happen under the link end's lock, and the end's state changes
        only once the transport is closed, so an open of the same end waits for the port to be
        free instead of racing the close for it.
        """
        async with io.lock:
            transport = io.transport
            if io.subs or not io.receiving or transport is None:
                return
            try:
                await transport.close()
            finally:  # drops counted while closing are part of the final report
                self._poll_kernel(io, transport)
                self._note_unread(io, transport)
                self._close_down_link(io)
                io.transport, io.receiving, io.failure = None, False, None

    async def open_capture(
        self,
        ends: Sequence[tuple[Link, LinkEnd]],
        *,
        sinks: Sequence[FrameSink] = (),
        queue_frames: int | None = None,
        queue_bytes: int | None = None,
    ) -> Capture:
        """Start receiving on ``ends``; every frame they receive reaches ``sinks`` and the
        returned capture. ``sinks`` stay attached to the whole engine until the capture closes.

        The capture's queue holds at most ``queue_frames`` frames and ``queue_bytes`` bytes
        (frame data plus ``FRAME_OVERHEAD_BYTES`` each); frames past either are not shown to this
        capture's consumer, and counted."""
        assert len({(link.name, end) for link, end in ends}) == len(ends), "duplicate link end"
        if self._closed:
            raise _capture_after_close()
        room = self._max_packet_bytes + FRAME_OVERHEAD_BYTES
        assert queue_bytes is None or queue_bytes >= room, (
            f"a queue of {queue_bytes} bytes could not hold one frame of {room}"
        )
        queue = _FrameQueue(
            CONSUMER_QUEUE_FRAMES if queue_frames is None else queue_frames,
            CONSUMER_QUEUE_BYTES if queue_bytes is None else queue_bytes,
        )
        capture = Capture(self, queue, sinks)
        for sink in sinks:
            self.add_sink(sink)
        try:
            for link, end in ends:
                io = self._io(link, end)
                sub = _Subscription(queue, io)
                capture._subs.append(sub)
                await self._attach(io, sub)
        except BaseException:
            await capture.close()
            raise
        return capture

    async def _release_capture(self, capture: Capture) -> None:
        failure: BaseException | None = None
        try:
            ios: dict[int, _LinkIO] = {}
            for sub in capture._subs:
                self._poll_sub(sub)
                if sub in sub.io.subs:
                    sub.io.subs.remove(sub)
                ios[id(sub.io)] = sub.io
            for io in ios.values():
                try:
                    await self._stop_if_idle(io)
                except BaseException as exc:  # the other link ends are still released
                    failure = failure or exc
        finally:
            for sink in capture._sinks:
                self.remove_sink(sink)
        if failure is not None:
            raise failure

    async def send(
        self,
        link: Link,
        end: LinkEnd,
        data: bytes,
        *,
        confirm: bool = False,
        handed_off: Callable[[], None] | None = None,
    ) -> None:
        """Send ``data`` out of ``end`` of ``link``. The TX frame enters ingest the moment the
        bytes leave the process (``handed_off`` runs right after), so sinks see what was sent.

        The transport is shared with the end's captures, so closing the last one can close it
        under a send that has not yet left the process. That send is made again on the transport
        that replaces it; a send is never reported as a link failure for that.
        """
        io = self._io(link, end)
        went_out = False

        def out() -> None:
            nonlocal went_out
            went_out = True
            self._ingest_tx(io, data)
            if handed_off is not None:
                handed_off()

        closed: TransportClosed | None = None
        for _ in range(2):
            async with io.lock:
                if self._closed:
                    raise IronbirdError(
                        Code.TX_SEND, "not sent: the session is closing (not a link failure)"
                    )
                if io.transport is None:
                    transport = self._new_transport(io)
                    try:
                        await self._open_transport(io, transport, None)
                    except _Closing:
                        raise IronbirdError(
                            Code.TX_SEND, "not sent: the session is closing (not a link failure)"
                        ) from None
                    io.transport = transport
                transport = io.transport
            try:
                await transport.send(data, confirm=confirm, handed_off=out)
            except TransportClosed as exc:
                if went_out:
                    raise
                closed = exc
                continue
            return
        reason = (
            "the session is closing"
            if self._closed
            else "another consumer of this link end closed it twice under the send; send again"
        )
        raise IronbirdError(
            Code.TX_SEND,
            f"not sent: {io.description} was closed while sending ({reason}; not a link failure)",
            cause=closed,
        )

    async def close(self) -> None:
        """Close every transport still open (a capture its owner never closed, a send-only one)
        and end every capture's iteration, so a consumer still waiting does not wait forever."""
        self._closed = True
        failure: BaseException | None = None
        ios = list(self._ios.values())
        for io in ios:
            if io.opening is not None:  # an open in progress must not hold the lock for its timeout
                io.opening.cancel()
            self._end_subscriptions(io)
        for io in ios:
            try:
                async with io.lock:
                    self._end_subscriptions(io)  # a capture that joined while the lock was held
                    transport = io.transport
                    if transport is None:
                        continue
                    try:
                        await transport.close()
                    finally:
                        if io.receiving:
                            self._poll_kernel(io, transport)
                        self._note_unread(io, transport)
                        self._close_down_link(io)
                        io.transport, io.receiving = None, False
            except BaseException as exc:  # the other link ends are still closed
                failure = failure or exc
        if failure is not None:
            raise failure

    def _end_subscriptions(self, io: _LinkIO) -> None:
        for sub in io.subs:
            self._poll_sub(sub)
            sub.queue.close()
        io.subs.clear()

    # -- ingest (event-loop thread, per frame) -----------------------------------------------

    def _count(self, mid: int, time_ns: int) -> None:
        """Per-MID statistics; only called for MIDs the CCDD defines, so garbage cannot fill it."""
        stat = self._stats.get(mid)
        if stat is not None:
            stat.count += 1
            stat.last_ns = time_ns
        elif len(self._stats) < MAX_TRACKED_MIDS:
            self._stats[mid] = _MidStat(1, time_ns)
            self._ledger.record("state", "rx.first", mid=mid)
        else:
            self._stats_overflow += 1

    def decoder(self, *, report_checked: bool) -> FrameDecoder:
        """A decoder through this engine's codecs that reports what it rejects here."""
        return FrameDecoder(self._codec_for, self.reject, report_checked=report_checked)

    def reject(self, frame: Frame, error: DecodeError) -> None:
        """Count, ledger (raw bytes) and report (coalesced) one frame that failed the boundary."""
        self._malformed_total += 1
        label = self._labels[frame.stream.id]
        # One evidence-sampling key per (code, MID, stream): the first eight of a window are kept
        # in full, then one a second; the window summary stands for the rest.
        key = f"{error.code.value}:{error.mid if error.mid is not None else '-'}:{label}"
        self._ledger.record(
            "packet",
            "rx.malformed",
            sample=key,
            raw=frame.data,
            wire_len=frame.orig_len,
            source=frame.source,
            seq=frame.seq,
        )
        self._report(error, context=f"from {label}", evidence=True, sample=f"{key}|error")

    def _ingest(
        self,
        io: _LinkIO,
        data: bytes,
        time_ns: int,
        source: str,
        orig_len: int,
        flags: int,
    ) -> None:
        self._ledger.mark_busy()
        size = len(data)
        assert orig_len >= size, "a transport reports the size on the wire, never less"
        bits = int(flags)
        assert not bits & ~KNOWN_FLAG_BITS, f"a transport passed unknown frame flag bits {bits:#x}"
        if size > self._max_packet_bytes:
            # A transport cuts at the edge (UDP does, with MSG_TRUNC), so no copy of the whole
            # datagram reaches here; this keeps a transport that does not from breaking the cap.
            data = data[: self._max_packet_bytes]
            bits |= FLAG_TRUNCATED
        stream, codec = io.rx_stream, io.codec
        kind = stream.kind
        checked: Resolved | None = None
        error: DecodeError | None = None
        skipped = bits & (FLAG_GARBAGE | FLAG_STALE)
        if skipped:
            # Bytes a stream framer skipped or flushed are not a packet to check; the transport
            # reports them as one resynchronisation notice (IB-TX-007), not one error each.
            mid = None if bits & FLAG_GARBAGE else codec.peek_mid(kind, data)
        elif bits & FLAG_TRUNCATED:
            error = codec.oversize_error(kind, data, orig_len)
            mid = codec.peek_mid(kind, data)
        else:
            verdict = codec.resolve(kind, data)
            if isinstance(verdict, DecodeError):
                error = verdict
                mid = codec.peek_mid(kind, data)
            else:
                error, checked = None, verdict
                mid = verdict.message.mid  # a packet that passed the check names a CCDD message
        if mid is not None:
            bits |= FLAG_HAS_MID
        self._seq += 1
        frame = Frame(
            self._seq,
            time_ns,
            stream,
            sys.intern(source),
            mid,
            FLAGS_BY_BITS[bits],
            orig_len,
            data,
            checked,
        )
        for sink in self._sinks:
            try:
                sink.append(frame)
            except Exception as exc:
                self._sink_failed(sink, frame.seq, exc)
        if checked is not None:
            self._count(checked.message.mid, time_ns)
        elif skipped:
            self._resync_frames += 1
        else:
            assert error is not None
            self.reject(frame, error)
        for sub in io.subs:
            queue = sub.queue
            if not queue.put(frame):
                if queue.closed:  # ended (a failed link end of the same capture): not a lag
                    continue
                io.lossy = True
                sub.dropped += 1
                if self._sinks:
                    sub.dropped_to_sinks += 1

    def _ingest_tx(self, io: _LinkIO, data: bytes) -> None:
        """A frame this process sent. Sinks see it; consumers of received frames do not."""
        stream = io.tx_stream
        mid = io.codec.peek_mid(stream.kind, data)
        bits = FLAG_HAS_MID if mid is not None else 0
        assert len(data) <= self._max_packet_bytes, "the codec keeps encoded packets under the cap"
        self._seq += 1
        frame = Frame(
            self._seq,
            time.time_ns(),
            stream,
            sys.intern(io.tx_peer),
            mid,
            FLAGS_BY_BITS[bits],
            len(data),
            data,
        )
        for sink in self._sinks:
            try:
                sink.append(frame)
            except Exception as exc:
                self._sink_failed(sink, frame.seq, exc)

    # -- periodic work and drop accounting -----------------------------------------------

    async def tick(self) -> None:
        """About once a second: refresh OS-side counters (off the loop) and report new drops."""
        receiving = [io for io in self._ios.values() if io.receiving and io.transport is not None]
        transports = [(io, io.transport) for io in receiving if io.transport is not None]
        await asyncio.gather(*(transport.poll() for _, transport in transports))
        for io, transport in transports:
            if io.transport is transport:  # a replaced transport was accounted for as it closed
                self._poll_kernel(io, transport)
            for sub in io.subs:
                self._poll_sub(sub)

    def _poll_sub(self, sub: _Subscription) -> None:
        """Report frames this consumer's queue refused since the last poll (coalesced, ledgered)."""
        total = sub.dropped
        if total == sub.reported:
            return
        delta, sub.reported = total - sub.reported, total
        label = sub.io.description
        kept = sub.dropped_to_sinks
        queue = sub.queue
        self._ledger.record(
            "state",
            "rx.queue_drops",
            label=label,
            delta=delta,
            total=total,
            seen_by_sinks=kept,
            queue_dropped_bytes=queue.dropped_bytes,
            queue_peak_bytes=queue.peak_bytes,
        )
        tail = f"; {kept} of them still reached a sink" if kept else ""
        self._report(
            IronbirdError(
                Code.ENG_LAG,
                f"{label}: consumer fell behind, {total} frame(s) not shown so far{tail}",
            ),
            context=f"queue {label}",
        )

    def _note_unread(self, io: _LinkIO, transport: Transport) -> None:
        """Ledger the packets a framing transport emitted although the CCDD disagrees with them,
        and account for datagrams still in the socket buffer when ``transport`` closed (they are
        discarded unread). Always ledgered; reported only on a link end that was already losing
        frames, where the count completes the picture rather than adding noise."""
        stats = transport.stats()
        label = io.description
        if stats.ccdd_mismatched:  # the decode reports each one; this is the total for the ledger
            self._ledger.record(
                "state", "rx.ccdd_mismatch", label=label, packets=stats.ccdd_mismatched
            )
        left = stats.unread_at_close
        if not left:
            return
        self._ledger.record("state", "rx.unread_at_close", label=label, datagrams=left)
        # Datagrams the OS discards unread are lost before the transport read them, like a kernel
        # drop: sinks that record gaps get one, bracketing the loss by the last poll and now.
        watch, now_ns = io.kernel, time.time_ns()
        self._notify_gap(
            io.rx_stream,
            GapReason.KERNEL_DROP,
            left,
            watch.last_ns,
            now_ns,
            watch.last_seq,
            self._seq,
        )
        if io.lossy or stats.kernel_dropped:
            self._report(
                IronbirdError(
                    Code.TX_OVERRUN,
                    f"{label}: {left} more datagram(s) were still in the socket buffer when it "
                    "closed and were not read",
                ),
                context=f"close {label}",
            )

    def _notify_gap(
        self,
        stream: Stream,
        reason: GapReason,
        lost: int,
        first_ns: int,
        last_ns: int,
        first_seq: int,
        last_seq: int,
    ) -> None:
        for sink in self._sinks:
            gap = getattr(sink, "gap", None)
            if gap is None:
                continue
            try:
                gap(stream, reason, lost, first_ns, last_ns, first_seq, last_seq)
            except Exception as exc:
                self._sink_failed(sink, last_seq, exc)

    def _poll_kernel(self, io: _LinkIO, transport: Transport) -> None:
        """Report new kernel-side drops of ``transport`` since the last poll."""
        total = transport.stats().kernel_dropped
        watch = io.kernel
        now_ns, seq = time.time_ns(), self._seq
        first_ns, first_seq = watch.last_ns, watch.last_seq
        watch.last_ns, watch.last_seq = now_ns, seq
        if total == watch.seen:
            return
        delta, watch.seen = total - watch.seen, total
        self._notify_gap(
            io.rx_stream, GapReason.KERNEL_DROP, delta, first_ns, now_ns, first_seq, seq
        )
        label = io.description
        self._ledger.record("state", "rx.kernel_drops", label=label, delta=delta, total=total)
        self._report(
            IronbirdError(
                Code.TX_OVERRUN,
                f"{label}: kernel dropped {total} datagram(s) so far "
                "(socket buffer full; see net.core.rmem_max)",
            ),
            context=f"kernel {label}",
        )


__all__ = [
    "CONSUMER_QUEUE_BYTES",
    "CONSUMER_QUEUE_FRAMES",
    "FRAME_OVERHEAD_BYTES",
    "MAX_TRACKED_MIDS",
    "Capture",
    "CaptureStats",
    "Engine",
    "FrameDecoder",
    "FrameSink",
    "GapSink",
    "LinkSink",
    "MidStatSnapshot",
]
