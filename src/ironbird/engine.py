"""The engine: push ingest of raw frames, sinks, and per-consumer captures.

Data flow (one process, one event loop)::

    transport reader callback --> ingest: seq, Frame, peek MID, Codec.check
                                    |  sinks (recorder, ...) see each frame first, synchronously
                                    |  malformed or unknown: counted, ledgered, reported (coalesced)
                                    +-> each consumer's bounded queue -> filter -> decode -> render

Ingest is cheap and never decodes fields. A frame that fails ``Codec.check`` is still a frame:
it reaches the sinks and every consumer, so nothing a link carried is hidden. A consumer that
falls behind loses frames from its own queue only (counted and reported); the sinks have
already seen them.

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
from ironbird.codec import Codec
from ironbird.config import Link, SystemConfig
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.frame import Decoded, Direction, Frame, FrameFlag, Stream
from ironbird.ledger import Ledger
from ironbird.transport import LinkEnd, Transport, open_transport

MAX_TRACKED_MIDS: Final = 1024
CONSUMER_QUEUE_FRAMES: Final = 16384
_MAX_STREAMS: Final = 0x10000

Reporter = Callable[..., None]  # Session.report: (error, *, context, level, evidence)


class FrameSink(Protocol):
    """Receives every frame, synchronously, before any consumer queue."""

    def append(self, frame: Frame) -> None:
        """Must be cheap and must not raise; a sink that does is reported as an internal error."""
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
    not_shown: int  # refused because the queue was full (still seen by every sink)


class _FrameQueue:
    """Bounded FIFO between ingest (producer, event loop) and one consumer (event loop)."""

    __slots__ = ("_closed", "_limit", "_waiter", "dropped", "items", "offered")

    def __init__(self, limit: int) -> None:
        assert limit > 0
        self.items: deque[Frame] = deque()
        self._limit = limit
        self._waiter: asyncio.Future[None] | None = None
        self._closed = False
        self.offered = 0
        self.dropped = 0

    @property
    def closed(self) -> bool:
        return self._closed

    def put(self, frame: Frame) -> bool:
        """Queue ``frame``; ``False`` (newest dropped, counted) when the queue is full."""
        self.offered += 1
        if len(self.items) >= self._limit:
            self.dropped += 1
            return False
        self.items.append(frame)
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        return True

    def close(self) -> None:
        """No more frames; the consumer drains what is queued, then its iteration ends."""
        self._closed = True
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    async def wait(self) -> None:
        """Return when a frame is queued or the queue is closed."""
        if self.items or self._closed:
            return
        self._waiter = asyncio.get_running_loop().create_future()
        try:
            await self._waiter
        finally:
            self._waiter = None


@dataclass(slots=True)
class _Subscription:
    """One consumer queue attached to one link end's receive stream."""

    queue: _FrameQueue
    io: _LinkIO
    dropped: int = 0
    reported: int = 0


@dataclass(slots=True)
class _KernelWatch:
    seen: int = 0


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

    def on_data(
        self, data: bytes, time_ns: int, source: str, orig_len: int, flags: FrameFlag
    ) -> None:
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
        queue = self._queue
        items = queue.items
        while True:
            while items:
                yield items.popleft()
            if queue.closed:
                return
            await queue.wait()

    def stats(self) -> CaptureStats:
        return CaptureStats(self._queue.offered, self._queue.dropped)

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
    (replay) passes ``True``. Either way the error is returned.
    """

    def __init__(self, engine: Engine, *, report_checked: bool) -> None:
        self._engine = engine
        self._report_checked = report_checked

    def decode(self, frame: Frame) -> Decoded | DecodeError:
        codec = self._engine._codec_for(frame.stream.node)
        kind = frame.stream.kind
        if FrameFlag.TRUNCATED in frame.flags:
            result: Decoded | DecodeError = codec.oversize_error(kind, frame.data, frame.orig_len)
        else:
            decoded = codec.decode(kind, frame.data)
            result = decoded if isinstance(decoded, DecodeError) else Decoded(frame, decoded)
        if isinstance(result, DecodeError) and self._report_checked:
            self._engine.reject(frame, result)
        return result


class Engine:
    """Ingest, sinks, captures and the session's transports. Owned by one session."""

    def __init__(
        self,
        config: SystemConfig,
        ledger: Ledger,
        report: Reporter,
        codec_for: Callable[[str], Codec],
    ) -> None:
        self._config = config
        self._max_packet_bytes = config.max_packet_bytes
        self._ledger = ledger
        self._report = report
        self._codec_for = codec_for
        self._seq = 0
        self._sinks: tuple[FrameSink, ...] = ()
        self._ios: dict[tuple[str, LinkEnd], _LinkIO] = {}
        self._labels: dict[int, str] = {}
        self._stats: dict[int, _MidStat] = {}
        self._stats_overflow = 0
        self._malformed_total = 0  # unknown-MID and malformed frames, filtered by a consumer or not
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

    def stats_snapshot(self) -> dict[int, MidStatSnapshot]:
        return {mid: MidStatSnapshot(s.count, s.last_ns) for mid, s in self._stats.items()}

    # -- sinks -----------------------------------------------------------------------

    def add_sink(self, sink: FrameSink) -> None:
        self._sinks = (*self._sinks, sink)

    def remove_sink(self, sink: FrameSink) -> None:
        remaining = list(self._sinks)
        if sink in remaining:
            remaining.remove(sink)
        self._sinks = tuple(remaining)

    def _sink_failed(self, sink: FrameSink, exc: Exception) -> None:
        self._report(
            IronbirdError(
                Code.INTERNAL,
                f"frame sink {type(sink).__name__} failed: {type(exc).__name__}: {exc}",
                details="".join(traceback.format_exception(exc)),
                cause=exc,
            ),
            context=f"sink {type(sink).__name__}",
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
        self._report(err, context=label)

    def _new_transport(self, io: _LinkIO) -> Transport:
        return open_transport(io.link, io.end, lambda err: self._transport_report(err, io.label))

    async def _start_receiving(self, io: _LinkIO) -> None:
        async with io.lock:
            if io.receiving:
                return
            transport = self._new_transport(io)
            await transport.open(io.on_data)
            previous, io.transport, io.receiving = io.transport, transport, True
            io.description = transport.description
            io.kernel = _KernelWatch()
            facts: dict[str, Any] = dict(transport.facts())
            self._ledger.record("state", f"{io.link.transport}.open", **facts)
            if previous is not None:  # a send-only transport the receiving one replaces
                await previous.close()

    async def _stop_receiving(self, io: _LinkIO) -> None:
        transport, io.transport, io.receiving = io.transport, None, False
        if transport is None:
            return
        try:
            await transport.close()
        finally:  # drops counted while closing are part of the final report
            self._poll_kernel(io, transport)

    async def open_capture(
        self,
        ends: Sequence[tuple[Link, LinkEnd]],
        *,
        sinks: Sequence[FrameSink] = (),
        queue_frames: int | None = None,
    ) -> Capture:
        """Start receiving on ``ends``; every frame they receive reaches ``sinks`` and the
        returned capture. ``sinks`` stay attached to the whole engine until the capture closes."""
        assert len({(link.name, end) for link, end in ends}) == len(ends), "duplicate link end"
        queue = _FrameQueue(CONSUMER_QUEUE_FRAMES if queue_frames is None else queue_frames)
        capture = Capture(self, queue, sinks)
        for sink in sinks:
            self.add_sink(sink)
        try:
            for link, end in ends:
                io = self._io(link, end)
                await self._start_receiving(io)
                sub = _Subscription(queue, io)
                io.subs.append(sub)
                capture._subs.append(sub)
        except BaseException:
            await capture.close()
            raise
        return capture

    async def _release_capture(self, capture: Capture) -> None:
        try:
            for sub in capture._subs:
                self._poll_sub(sub)
                if sub in sub.io.subs:
                    sub.io.subs.remove(sub)
            for io in {sub.io for sub in capture._subs}:
                if not io.subs and io.receiving:
                    await self._stop_receiving(io)
        finally:
            for sink in capture._sinks:
                self.remove_sink(sink)

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
        bytes leave the process (``handed_off`` runs right after), so sinks see what was sent."""
        io = self._io(link, end)
        async with io.lock:
            if io.transport is None:
                transport = self._new_transport(io)
                await transport.open(None)
                io.transport = transport
            transport = io.transport

        def out() -> None:
            self._ingest_tx(io, data)
            if handed_off is not None:
                handed_off()

        await transport.send(data, confirm=confirm, handed_off=out)

    async def close(self) -> None:
        """Close every transport still open (a capture its owner never closed, a send-only one)."""
        for io in list(self._ios.values()):
            io.subs.clear()
            if io.transport is not None:
                if io.receiving:
                    await self._stop_receiving(io)
                else:
                    transport, io.transport = io.transport, None
                    await transport.close()

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

    def reject(self, frame: Frame, error: DecodeError) -> None:
        """Count, ledger (raw bytes) and report (coalesced) one frame that failed the boundary."""
        self._malformed_total += 1
        self._ledger.record("packet", "rx.malformed", raw=frame.data, source=frame.source)
        self._report(error, context=f"from {self._labels[frame.stream.id]}", evidence=True)

    def _ingest(
        self,
        io: _LinkIO,
        data: bytes,
        time_ns: int,
        source: str,
        orig_len: int,
        flags: FrameFlag,
    ) -> None:
        self._ledger.mark_busy()
        size = len(data)
        assert orig_len >= size, "a transport reports the size on the wire, never less"
        if size > self._max_packet_bytes:
            data = data[: self._max_packet_bytes]
            flags |= FrameFlag.TRUNCATED
        stream, codec = io.rx_stream, io.codec
        kind = stream.kind
        mid = codec.peek_mid(kind, data)
        if mid is not None:
            flags |= FrameFlag.HAS_MID
        self._seq += 1
        frame = Frame(self._seq, time_ns, stream, sys.intern(source), mid, flags, orig_len, data)
        for sink in self._sinks:
            try:
                sink.append(frame)
            except Exception as exc:
                self._sink_failed(sink, exc)
        if FrameFlag.TRUNCATED in flags:
            error = codec.oversize_error(kind, data, orig_len)
        else:
            if mid is not None and codec.is_known_mid(kind, mid):
                self._count(mid, time_ns)
            error = codec.check(kind, data)
        if error is not None:
            self.reject(frame, error)
        for sub in io.subs:
            if not sub.queue.put(frame):
                sub.dropped += 1

    def _ingest_tx(self, io: _LinkIO, data: bytes) -> None:
        """A frame this process sent. Sinks see it; consumers of received frames do not."""
        stream = io.tx_stream
        mid = io.codec.peek_mid(stream.kind, data)
        flags = FrameFlag.HAS_MID if mid is not None else FrameFlag.NONE
        assert len(data) <= self._max_packet_bytes, "the codec keeps encoded packets under the cap"
        self._seq += 1
        frame = Frame(
            self._seq, time.time_ns(), stream, sys.intern(io.tx_peer), mid, flags, len(data), data
        )
        for sink in self._sinks:
            try:
                sink.append(frame)
            except Exception as exc:
                self._sink_failed(sink, exc)

    # -- periodic work and drop accounting -----------------------------------------------

    async def tick(self) -> None:
        """About once a second: refresh OS-side counters (off the loop) and report new drops."""
        receiving = [io for io in self._ios.values() if io.receiving and io.transport is not None]
        transports = [(io, io.transport) for io in receiving if io.transport is not None]
        await asyncio.gather(*(transport.poll() for _, transport in transports))
        for io, transport in transports:
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
        self._ledger.record("state", "rx.queue_drops", label=label, delta=delta, total=total)
        self._report(
            IronbirdError(
                Code.TX_OVERRUN,
                f"{label}: consumer queue overflow, {total} frame(s) not shown so far "
                "(still captured)",
            ),
            context=f"queue {label}",
        )

    def _poll_kernel(self, io: _LinkIO, transport: Transport) -> None:
        """Report new kernel-side drops of ``transport`` since the last poll."""
        total = transport.stats().kernel_dropped
        watch = io.kernel
        if total == watch.seen:
            return
        delta, watch.seen = total - watch.seen, total
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
    "CONSUMER_QUEUE_FRAMES",
    "MAX_TRACKED_MIDS",
    "Capture",
    "CaptureStats",
    "Engine",
    "FrameDecoder",
    "FrameSink",
    "MidStatSnapshot",
]
