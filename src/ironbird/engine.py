"""The engine: push ingest of raw frames, sinks, and per-consumer captures.

Data flow (one process, one event loop)::

    transport reader callback --> ingest: seq, Frame, peek MID, Codec.check
                                    |  sinks (recorder, ...) see each frame first, synchronously
                                    |  malformed or unknown: counted, ledgered, reported (coalesced)
                                    +-> each consumer's bounded queue -> filter -> decode -> render

Ingest is cheap and never decodes fields. A frame that fails ``Codec.check`` is still a frame:
it reaches the sinks and every consumer, so nothing a link carried is hidden. A frame that
passes carries the check's result, which a live consumer's decode reuses. A consumer that falls
behind loses frames from its own queue only (counted and reported); the sinks have already seen
them.

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
from ironbird.config import Link, SystemConfig
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.frame import (
    FLAG_HAS_MID,
    FLAG_TRUNCATED,
    FLAGS_BY_BITS,
    KNOWN_FLAG_BITS,
    Decoded,
    Direction,
    Frame,
    GapReason,
    Stream,
)
from ironbird.ledger import Ledger
from ironbird.transport import LinkEnd, Receiver, Transport, TransportClosed, open_transport

MAX_TRACKED_MIDS: Final = 1024
CONSUMER_QUEUE_FRAMES: Final = 16384
READ_BATCH_FRAMES: Final = 512  # most frames one ``next_batch`` hands a consumer
_MAX_STREAMS: Final = 0x10000
OPEN_TIMEOUT_S: Final = 5.0  # longest a transport may take to open (name resolution, bind)

Reporter = Callable[..., None]  # Session.report: (error, *, context, level, evidence)


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
        """Wait until a frame is queued or the queue is closed; ``items`` is empty afterwards only
        when the queue is closed and drained. One waiter at a time: a queue has a single wake-up
        slot, so a second waiter would be starved or would split the stream, and is refused."""
        if self._waiter is not None:
            raise _second_reader()
        if not self.items and not self._closed:
            self._waiter = asyncio.get_running_loop().create_future()
            try:
                await self._waiter
            finally:
                self._waiter = None


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
        nothing. The deque is live: frames that arrive meanwhile are appended to it.
        """
        await self._queue.wait()
        return self._queue.items

    async def next_batch(self) -> list[Frame]:
        """Up to :data:`READ_BATCH_FRAMES` unread frames, oldest first, waiting for at least one;
        empty once the capture is closed (or its engine shut down) and everything was read.

        The caller owns the frames it is given. A consumer that decodes per frame loops over the
        batch itself, which is cheaper than iterating the capture. A capture has one reader: a
        second read that waits while the first is waiting raises ``IB-INT-001``.
        """
        items = await self.wait_frames()
        popleft = items.popleft
        return [popleft() for _ in range(min(len(items), READ_BATCH_FRAMES))]

    @property
    def overloaded(self) -> bool:
        """Whether this capture has lost frames: its queue refused some, or the kernel dropped
        datagrams of a link end it receives from."""
        return self._queue.dropped > 0 or any(sub.io.kernel.seen for sub in self._subs)

    def stats(self) -> CaptureStats:
        queue = self._queue
        return CaptureStats(queue.offered, queue.dropped, len(queue.items))

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

    A frame that live ingest accepted carries what its check established, so its fields are
    unpacked without checking the packet a second time; every other frame goes through the whole
    of ``Codec.decode``.
    """

    def __init__(self, engine: Engine, *, report_checked: bool) -> None:
        self._engine = engine
        self._report_checked = report_checked
        self._codecs: dict[str, Codec] = {}

    def decode(self, frame: Frame) -> Decoded | DecodeError:
        stream = frame.stream
        codec = self._codecs.get(stream.node)
        if codec is None:
            codec = self._codecs[stream.node] = self._engine._codec_for(stream.node)
        checked = frame.checked
        result: Decoded | DecodeError
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
        self._closed = False
        self._sinks: tuple[FrameSink, ...] = ()
        self._sink_refs: dict[int, _SinkRef] = {}  # by id(sink)
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
            if not io.receiving:
                transport = self._new_transport(io)
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
                io.transport, io.receiving = None, False

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
        if self._closed:
            raise _capture_after_close()
        queue = _FrameQueue(CONSUMER_QUEUE_FRAMES if queue_frames is None else queue_frames)
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
        flags: int,
    ) -> None:
        self._ledger.mark_busy()
        size = len(data)
        assert orig_len >= size, "a transport reports the size on the wire, never less"
        bits = int(flags)
        assert not bits & ~KNOWN_FLAG_BITS, f"a transport passed unknown frame flag bits {bits:#x}"
        if size > self._max_packet_bytes:
            data = data[: self._max_packet_bytes]
            bits |= FLAG_TRUNCATED
        stream, codec = io.rx_stream, io.codec
        kind = stream.kind
        checked: Resolved | None = None
        error: DecodeError | None
        if bits & FLAG_TRUNCATED:
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
        else:
            assert error is not None
            self.reject(frame, error)
        for sub in io.subs:
            if not sub.queue.put(frame):
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
        self._ledger.record(
            "state", "rx.queue_drops", label=label, delta=delta, total=total, seen_by_sinks=kept
        )
        tail = f"; {kept} of them still reached a sink" if kept else ""
        self._report(
            IronbirdError(
                Code.TX_OVERRUN,
                f"{label}: consumer queue overflow, {total} frame(s) dropped from the "
                f"consumer view so far{tail}",
            ),
            context=f"queue {label}",
        )

    def _note_unread(self, io: _LinkIO, transport: Transport) -> None:
        """Account for datagrams still in the socket buffer when ``transport`` closed (they are
        discarded unread). Always ledgered; reported only on a link end that was already losing
        frames, where the count completes the picture rather than adding noise."""
        left = transport.stats().unread_at_close
        if not left:
            return
        label = io.description
        self._ledger.record("state", "rx.unread_at_close", label=label, datagrams=left)
        if io.lossy or transport.stats().kernel_dropped:
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
    "CONSUMER_QUEUE_FRAMES",
    "MAX_TRACKED_MIDS",
    "Capture",
    "CaptureStats",
    "Engine",
    "FrameDecoder",
    "FrameSink",
    "GapSink",
    "MidStatSnapshot",
]
