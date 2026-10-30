"""The API both front-ends use. It runs in-process; the :mod:`ironbird.engine` it wraps does the
ingest.

A :class:`Session` is one invocation of ironbird *acting as* one node. Commander side
(ground): encode commands, decode telemetry. Responder side (a flight node): decode commands,
answer with telemetry. The same codec serves both. Requests and results are plain frozen
values so the surface can later be exposed over a control socket unchanged.

Receiving is a :class:`~ironbird.engine.Capture` of raw frames (``Session.capture``). The
:class:`Listener`, :class:`EventStream` and :class:`Responder` are consumers of one that decode
what they show; malformed frames are reported once at ingest, not again here.
"""

from __future__ import annotations

import asyncio
import os
import traceback
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final

from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import CcddModel, EventSeverity, Message, MessageKind, TypeClass
from ironbird.coalesce import Coalescer
from ironbird.codec import Codec, DecodedPacket, EncodedPacket
from ironbird.config import Link, Node, ResponseRule, SystemConfig
from ironbird.engine import (
    Capture,
    Engine,
    FrameDecoder,
    FrameSink,
    MidStatSnapshot,
)
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.events import Event, extract_event
from ironbird.frame import Decoded
from ironbird.header import load_profile
from ironbird.ledger import Ledger
from ironbird.notice import Level, Notice
from ironbird.transport import LinkEnd

TICK_S: Final = 1.0
EVENT_QUEUE_SIZE: Final = 1024


class Side(StrEnum):
    COMMANDER = "commander"  # sends commands, receives telemetry
    RESPONDER = "responder"  # receives commands, sends telemetry


@dataclass(frozen=True, slots=True)
class Route:
    link: Link
    side: Side
    peer: Node
    server: Node  # owns the CCDD and header profiles (always link.destination)


def link_end(side: Side) -> LinkEnd:
    """The end of its link a process on ``side`` is."""
    return LinkEnd.SOURCE if side is Side.COMMANDER else LinkEnd.DESTINATION


@dataclass(frozen=True, slots=True)
class SentCommand:
    node: str
    command: str
    mid: int
    function_code: int
    sequence: int
    size: int
    data: bytes
    endpoint: str


@dataclass(frozen=True, slots=True)
class Handled:
    """One command processed by a responder."""

    time_ns: int
    command: DecodedPacket | None  # None when the datagram was rejected
    error: DecodeError | None
    reply: EncodedPacket | None


@dataclass(frozen=True, slots=True)
class TxAttempt:
    """The most recent command send. Replaced (never mutated) as the send progresses, so a
    reader of :attr:`Session.last_tx` sees one consistent value (signal handling reads it)."""

    command: str
    went_out: bool = False


@dataclass(frozen=True, slots=True)
class Match:
    tier: str  # "exact" | "name" | "description"
    node: str
    message: Message


def _is_decimal(text: str) -> bool:
    return text.isascii() and text.isdigit()


class Session:
    def __init__(
        self,
        config: SystemConfig,
        *,
        role: str | None = None,
        ledger: Ledger,
        notify: Callable[[Notice], None],
    ) -> None:
        self._config: SystemConfig = config
        self._node: Node = config.resolve(role) if role else config.nodes[config.acts_as]
        self._ledger: Ledger = ledger
        ledger.set_max_raw_bytes(config.max_packet_bytes)
        self._coalescer: Coalescer = Coalescer(notify)
        self._codecs: dict[str, Codec] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._last_tx: TxAttempt | None = None
        self._engine: Engine = Engine(config, ledger, self.report, self.codec_for)

    # -- read-only state -----------------------------------------------------------

    @property
    def config(self) -> SystemConfig:
        return self._config

    @property
    def node(self) -> Node:
        """The node ironbird is acting as."""
        return self._node

    @property
    def ledger(self) -> Ledger:
        return self._ledger

    @property
    def last_tx(self) -> TxAttempt | None:
        """The most recent command send; the session updates it as the send progresses."""
        return self._last_tx

    @property
    def stats_overflow(self) -> int:
        """Known-MID packets not counted because the per-MID table was full."""
        return self._engine.stats_overflow

    @property
    def malformed_total(self) -> int:
        """Unknown-MID and malformed frames seen, whether a consumer filtered them or not."""
        return self._engine.malformed_total

    def stats_snapshot(self) -> Mapping[int, MidStatSnapshot]:
        """Frozen copy of the per-MID statistics, safe to hold while packets keep arriving."""
        return MappingProxyType(self._engine.stats_snapshot())

    # -- lifecycle -----------------------------------------------------------------

    async def __aenter__(self) -> Session:
        loop = asyncio.get_running_loop()
        self._tasks = [
            loop.create_task(self.ledger.drain_task(), name="ledger-drain"),
            loop.create_task(self._ticker(), name="coalescer-tick"),
        ]
        for task in self._tasks:
            task.add_done_callback(self._background_done)
        self.ledger.record(
            "action",
            "session.start",
            role=self.node.name,
            config=self.config.path,
            pid=os.getpid(),
            ledger=self.ledger.path,
        )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.ledger.record("action", "session.end", role=self.node.name)
        try:
            for task in self._tasks:
                task.cancel()
            # A task that died is already reported by its done-callback. Collecting the results
            # (rather than suppressing CancelledError around each await) lets a cancellation of
            # this very task, such as a signal, propagate.
            await asyncio.gather(*self._tasks, return_exceptions=True)
            await self._engine.close()
        finally:
            try:
                self.flush_notices()
            finally:
                self.ledger.flush()

    def _background_done(self, task: asyncio.Task[None]) -> None:
        """Report a background task that died instead of being cancelled."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        self.report(
            IronbirdError(
                Code.INTERNAL,
                f"background task {task.get_name()} stopped: {type(exc).__name__}: {exc}",
                details="".join(traceback.format_exception(exc)),
                cause=exc,
            )
        )

    async def _ticker(self) -> None:
        while True:
            await asyncio.sleep(TICK_S)
            self._coalescer.tick()
            await self._engine.tick()

    # -- error reporting ---------------------------------------------------------------

    def flush_notices(self) -> None:
        """Emit every pending coalesced summary now (shutdown, or before reading the output)."""
        self._coalescer.flush()

    def fatal(self, error: IronbirdError) -> None:
        """Ledger an error that is leaving the session (the caller prints it)."""
        self.ledger.record_error(error, role=self.node.name)

    def report(
        self,
        error: IronbirdError | DecodeError,
        *,
        context: str = "",
        level: Level = Level.ERROR,
        evidence: bool = False,
    ) -> None:
        """Ledger in full, notify the user coalesced (one notice per key per window).

        ``evidence`` marks an error that untrusted wire input can produce at will; its ledger
        record then shares the capped evidence tier instead of the essential one.
        """
        if isinstance(error, DecodeError):
            raw_mid = error.mid
            err = error.to_error(context)
            offset: int | None = error.offset
        else:
            raw_mid = None
            err = error
            offset = None
        ledger_n = self.ledger.record_error(
            err, evidence=evidence, role=self.node.name, mid=raw_mid, offset=offset
        )
        key = f"{err.code.value}:{raw_mid}:{context}"
        self._coalescer.report(err.code, level, key, err.summary, ledger_n=ledger_n, group=err.code)

    # -- loading -------------------------------------------------------------------

    def codec_for(self, node_name: str) -> Codec:
        """Load (once) the CCDD and header profiles of ``node_name``."""
        cached = self._codecs.get(node_name)
        if cached is not None:
            return cached
        node = self.config.nodes[node_name]
        if node.ccdd is None or node.command_header is None or node.telemetry_header is None:
            raise IronbirdError(
                Code.CFG_REF,
                f"node {node_name!r} has no ccdd/command_header/telemetry_header in "
                f"{self.config.path}",
            )
        model = load_ccdd(node.ccdd, node.ccdd_adapter)
        codec = Codec(
            model,
            load_profile(node.command_header),
            load_profile(node.telemetry_header),
            max_packet_bytes=self.config.max_packet_bytes,
        )
        self.ledger.record(
            "state",
            "codec.loaded",
            node=node_name,
            ccdd=node.ccdd,
            messages=len(model.messages),
        )
        self._codecs[node_name] = codec
        return codec

    def models(self) -> Mapping[str, CcddModel]:
        return {
            name: self.codec_for(name).model
            for name, node in self.config.nodes.items()
            if node.ccdd is not None
        }

    # -- routing -------------------------------------------------------------------

    def route(
        self, peer: str, *, require: Side | None = None, link_name: str | None = None
    ) -> Route:
        peer_node = self.config.resolve(peer)
        me = self.node.name
        if peer_node.name == me:
            raise IronbirdError(
                Code.CFG_LINK, f"{me!r} is the node ironbird is acting as; name the other end"
            )
        links = self.config.links_between(me, peer_node.name)
        if link_name is not None:
            links = tuple(link for link in links if link.name == link_name)
        if not links:
            raise IronbirdError(
                Code.CFG_LINK,
                f"no link between {me!r} and {peer_node.name!r}"
                + (f" named {link_name!r}" if link_name else ""),
                details=f"links: {', '.join(link.name for link in self.config.links) or 'none'}",
            )

        def side_of(link: Link) -> Side:
            return Side.COMMANDER if link.source == me else Side.RESPONDER

        usable = [link for link in links if require is None or side_of(link) is require]
        if not usable:
            other = "commander" if require is Side.RESPONDER else "responder"
            hint = f"; try --as {peer_node.name}" if require is not None else ""
            raise IronbirdError(
                Code.CFG_LINK,
                f"{me!r} is the {other} on its link to {peer_node.name!r}, "
                f"not the {require.value if require else '?'}{hint}",
            )
        if len(usable) > 1:
            raise IronbirdError(
                Code.CFG_LINK,
                f"several links between {me!r} and {peer_node.name!r}; pick one with --link",
                details="links: " + ", ".join(link.name for link in usable),
            )
        link = usable[0]
        return Route(link, side_of(link), peer_node, self.config.nodes[link.destination])

    # -- search --------------------------------------------------------------------

    def find(self, query: str) -> tuple[Match, ...]:
        """Tiered lookup: exact name/MID, then name substring, then description/field text."""
        needle = query.strip().casefold()
        if not needle:
            return ()
        mid: int | None = None
        if needle.startswith("0x") or _is_decimal(needle):
            try:
                mid = int(needle, 0) if needle.startswith("0x") else int(needle, 10)
            except ValueError:
                mid = None
        found: list[Match] = []
        seen: set[tuple[str, str]] = set()
        models = self.models()

        def add(tier: str, node: str, msg: Message) -> None:
            key = (node, msg.name)
            if key not in seen:
                seen.add(key)
                found.append(Match(tier, node, msg))

        for node, model in models.items():
            for msg in model.messages:
                if msg.name.casefold() == needle or (mid is not None and msg.mid == mid):
                    add("exact", node, msg)
        for node, model in models.items():
            for msg in model.messages:
                if needle in msg.name.casefold():
                    add("name", node, msg)
        for node, model in models.items():
            for msg in model.messages:
                haystack = [
                    msg.description,
                    *(f.name for f in msg.fields),
                    *(f.description for f in msg.fields),
                ]
                if any(needle in text.casefold() for text in haystack):
                    add("description", node, msg)
        return tuple(found)

    # -- commander: send -----------------------------------------------------------

    async def send_command(
        self,
        peer: str,
        command: str,
        args: Mapping[str, object] | None = None,
        *,
        tamper: Mapping[str, int] | None = None,
        link: str | None = None,
    ) -> SentCommand:
        route = self.route(peer, require=Side.COMMANDER, link_name=link)
        codec = self.codec_for(route.server.name)
        endpoint = route.link.command_endpoint
        self.ledger.record(
            "action",
            "send.request",
            node=route.server.name,
            command=command,
            args=dict(args or {}),
            tamper=dict(tamper or {}),
            link=route.link.name,
        )
        encoded = codec.encode_command(command, args, tamper=tamper)
        attempt = TxAttempt(encoded.message.name)
        self._last_tx = attempt
        result = "not_sent"

        def handed_off() -> None:
            nonlocal attempt, result
            attempt = replace(attempt, went_out=True)
            self._last_tx = attempt
            result = "sent"

        try:
            await self._engine.send(
                route.link, link_end(route.side), encoded.data, confirm=True, handed_off=handed_off
            )
        except IronbirdError as exc:
            result = exc.code.value
            raise
        except asyncio.CancelledError:
            result = "sent" if attempt.went_out else "cancelled"
            raise
        except BaseException as exc:
            result = type(exc).__name__
            raise
        finally:
            self.ledger.record(
                "command",
                "tx.command",
                node=route.server.name,
                command=encoded.message.name,
                mid=encoded.mid,
                function_code=encoded.function_code,
                sequence=encoded.sequence,
                size=len(encoded.data),
                endpoint=str(endpoint),
                result=result,
                sent=attempt.went_out,
                raw=encoded.data,
            )
        assert encoded.function_code is not None
        return SentCommand(
            node=route.server.name,
            command=encoded.message.name,
            mid=encoded.mid,
            function_code=encoded.function_code,
            sequence=encoded.sequence,
            size=len(encoded.data),
            data=encoded.data,
            endpoint=str(endpoint),
        )

    # -- capture (raw frames) and decoding ---------------------------------------------

    async def capture(
        self,
        routes: Sequence[Route],
        *,
        sinks: Sequence[FrameSink] = (),
        queue_frames: int | None = None,
    ) -> Capture:
        """Start receiving on ``routes``: an async iterator of every frame they carry.

        ``sinks`` see each frame, synchronously, before any consumer queue (and also the frames
        this session sends); a consumer that falls behind loses frames from its own queue only.
        Close the capture when done.
        """
        ends = [(route.link, link_end(route.side)) for route in routes]
        return await self._engine.open_capture(ends, sinks=sinks, queue_frames=queue_frames)

    def add_sink(self, sink: FrameSink) -> None:
        """Attach ``sink`` to every frame this session ingests or sends until it is removed."""
        self._engine.add_sink(sink)

    def remove_sink(self, sink: FrameSink) -> None:
        self._engine.remove_sink(sink)

    def decoder(self, *, report_checked: bool) -> FrameDecoder:
        """A decoder for frames of this session's streams.

        ``report_checked`` is ``False`` for frames that came through live ingest, which already
        reported what the check found, and ``True`` where nothing did (replay).
        """
        return FrameDecoder(self._engine, report_checked=report_checked)

    # -- listen (either side) -----------------------------------------------------

    async def listen(
        self, peer: str, *, mids: frozenset[int] | None = None, link: str | None = None
    ) -> Listener:
        route = self.route(peer, link_name=link)
        return await self._open_listener(route, mids)

    def _listen_endpoint(self, route: Route) -> str:
        link = route.link
        if route.side is Side.COMMANDER:
            return str(link.telemetry_endpoint)
        return str(link.command_endpoint)

    async def _open_listener(self, route: Route, mids: frozenset[int] | None) -> Listener:
        capture = await self.capture([route])
        self._record_listen_start(route, mids)
        return Listener(self, route, capture, mids)

    def _record_listen_start(self, route: Route, mids: frozenset[int] | None) -> None:
        self.ledger.record(
            "action",
            "listen.start",
            peer=route.peer.name,
            side=route.side.value,
            link=route.link.name,
            endpoint=self._listen_endpoint(route),
            mids=sorted(mids) if mids else None,
        )

    # -- events (all telemetry links of the selected nodes) ---------------------------

    def _event_routes(self, nodes: Sequence[str] | None) -> list[Route]:
        """Commander-side routes whose telemetry carries events, for ``nodes`` (default all)."""
        me = self.node.name
        outgoing = [lk for lk in self.config.links if lk.source == me]
        wanted: set[str] | None = None
        if nodes:
            wanted = {self.config.resolve(name).name for name in nodes}
            for name in sorted(wanted - {lk.destination for lk in outgoing}):
                raise IronbirdError(
                    Code.CFG_LINK,
                    f"{me!r} has no telemetry link from {name!r}",
                    details="links: " + ", ".join(link.name for link in self.config.links),
                )
        routes = [
            Route(
                lk,
                Side.COMMANDER,
                self.config.nodes[lk.destination],
                self.config.nodes[lk.destination],
            )
            for lk in outgoing
            if wanted is None or lk.destination in wanted
        ]
        if not routes:
            raise IronbirdError(
                Code.CFG_LINK,
                f"{me!r} has no telemetry links to listen on; try --as with the ground node",
                details="links: " + ", ".join(link.name for link in self.config.links),
            )
        seen: dict[str, Route] = {}
        for route in routes:
            clash = seen.setdefault(str(route.link.telemetry_endpoint), route)
            if clash is not route:
                raise IronbirdError(
                    Code.CFG_LINK,
                    f"links {clash.link.name!r} and {route.link.name!r} both receive telemetry "
                    f"on {route.link.telemetry_endpoint}; select one node with --node",
                )
        return routes

    async def events(
        self, nodes: Sequence[str] | None = None, *, min_severity: EventSeverity | None = None
    ) -> EventStream:
        """Listen on every telemetry link of ``nodes`` (default: all) for event packets.

        Every received event is ledgered in full; ``min_severity`` only limits what the
        stream yields. Nodes whose CCDD defines no event messages are skipped with a notice;
        if none is left, that is an error.
        """
        routes: list[Route] = []
        event_mids: dict[str, frozenset[int]] = {}
        for route in self._event_routes(nodes):
            codec = self.codec_for(route.server.name)
            mids = frozenset(m.mid for m in codec.model.event_messages())
            if not mids:
                self.report(
                    IronbirdError(
                        Code.EVT_NONE,
                        f"node {route.server.name!r} defines no event telemetry in "
                        f"{codec.model.source}; skipped",
                    )
                )
                continue
            routes.append(route)
            event_mids[route.server.name] = mids
        if not routes:
            raise IronbirdError(
                Code.EVT_NONE,
                "no selected node defines event telemetry; mark event messages with "
                '"category": "event" and an "event" mapping in the CCDD',
            )
        capture = await self.capture(routes)
        for route in routes:
            self._record_listen_start(route, event_mids[route.server.name])
        self.ledger.record(
            "action",
            "events.start",
            nodes=[route.server.name for route in routes],
            min_severity=min_severity.name if min_severity else None,
        )
        return EventStream(self, routes, capture, event_mids, min_severity)

    # -- responder ------------------------------------------------------------------

    async def respond(self, *, peer: str | None = None, link: str | None = None) -> Responder:
        me = self.node.name
        candidates = [
            lk
            for lk in self.config.links
            if lk.destination == me
            and (peer is None or lk.source == self.config.resolve(peer).name)
            and (link is None or lk.name == link)
        ]
        if not candidates:
            raise IronbirdError(
                Code.CFG_LINK,
                f"{me!r} is not the flight-side end of any matching link; "
                f"use --as with a node that receives commands",
            )
        if len(candidates) > 1:
            raise IronbirdError(
                Code.CFG_LINK,
                f"several links end at {me!r}; pick one with --peer or --link",
                details="links: " + ", ".join(lk.name for lk in candidates),
            )
        chosen = candidates[0]
        route = Route(chosen, Side.RESPONDER, self.config.nodes[chosen.source], self.node)
        codec = self.codec_for(me)
        rules = _validate_rules(self.node, codec, self.config)
        capture = await self.capture([route])
        self.ledger.record(
            "action",
            "respond.start",
            link=chosen.name,
            listen=str(chosen.command_endpoint),
            reply_to=str(chosen.telemetry_endpoint),
            rules=len(rules),
        )
        return Responder(self, route, codec, capture, rules)


class Listener:
    """Opened receive endpoint; iterate for decoded packets. Close when done."""

    def __init__(
        self,
        session: Session,
        route: Route,
        capture: Capture,
        mids: frozenset[int] | None,
    ) -> None:
        self._session = session
        self._route = route
        self._capture = capture
        self._decoder = session.decoder(report_checked=False)
        self._mids = mids

    @property
    def node(self) -> str:
        """Node whose CCDD decodes what arrives here."""
        return self._route.server.name

    @property
    def capture(self) -> Capture:
        return self._capture

    @property
    def local_address(self) -> tuple[str, int] | None:
        return self._capture.addresses[0][1]

    def __aiter__(self) -> AsyncIterator[Decoded]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Decoded]:
        mids = self._mids
        async for frame in self._capture:
            if mids is not None and frame.mid not in mids:
                continue  # a MID filter hides valid packets only; ingest reported the rest
            result = self._decoder.decode(frame)
            if not isinstance(result, DecodeError):
                yield result

    async def close(self) -> None:
        await self._capture.close()
        self._session.ledger.record("action", "listen.stop", peer=self._route.peer.name)


class EventStream:
    """Events from several links merged into one stream. Close when done.

    One pump task decodes the event frames of one capture, ledgers every event in full, and
    queues those at or above the severity floor; the bounded queue applies back-pressure to the
    pump, and from there to the capture's own bounded queue, whose overflow is reported as one
    coalesced error.
    """

    def __init__(
        self,
        session: Session,
        routes: Sequence[Route],
        capture: Capture,
        event_mids: Mapping[str, frozenset[int]],
        min_severity: EventSeverity | None,
    ) -> None:
        assert routes
        self._session = session
        self._routes = tuple(routes)
        self._capture = capture
        self._decoder = session.decoder(report_checked=False)
        self._event_mids = event_mids
        self._min_severity = min_severity
        self._queue: asyncio.Queue[Event | BaseException] = asyncio.Queue(EVENT_QUEUE_SIZE)
        loop = asyncio.get_running_loop()
        self._pump_task = loop.create_task(self._pump(), name="events")

    @property
    def endpoints(self) -> tuple[tuple[str, tuple[str, int] | None], ...]:
        """``(node, bound address)`` per link."""
        return tuple((stream.node, addr) for stream, addr in self._capture.addresses)

    def __aiter__(self) -> AsyncIterator[Event]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Event]:
        while True:
            item = await self._queue.get()
            if isinstance(item, BaseException):
                raise item
            yield item

    async def _pump(self) -> None:
        ledger = self._session.ledger
        try:
            async for frame in self._capture:
                node = frame.stream.node
                mids = self._event_mids.get(node)
                if mids is None or frame.mid not in mids:
                    continue
                decoded = self._decoder.decode(frame)
                if isinstance(decoded, DecodeError):
                    continue
                event = extract_event(node, frame.time_ns, decoded.packet)
                if event is None:  # the filter above keeps event MIDs; stay defensive
                    continue
                ledger.record(
                    "event",
                    "rx.event",
                    node=event.node,
                    app=event.app,
                    event_id=event.event_id,
                    type=event.type_label,
                    type_value=event.type_value,
                    severity=event.severity.name if event.severity else None,
                    text=event.text,
                    message=event.name,
                    mid=event.mid,
                    sequence=event.sequence,
                    source=frame.source,
                    raw=frame.data,
                )
                floor = self._min_severity
                if floor is not None and event.severity is not None and event.severity < floor:
                    continue
                await self._queue.put(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # surfaced to the consumer, never lost with the task
            await self._queue.put(exc)

    async def close(self) -> None:
        self._pump_task.cancel()
        await asyncio.gather(self._pump_task, return_exceptions=True)
        try:
            await self._capture.close()
        finally:
            for route in self._routes:
                self._session.ledger.record("action", "listen.stop", peer=route.peer.name)
            self._session.ledger.record("action", "events.stop")


def _validate_rules(node: Node, codec: Codec, config: SystemConfig) -> Mapping[str, ResponseRule]:
    """Check every response rule against the CCDD up front; trial-encode its literals."""
    model = codec.model
    rules: dict[str, ResponseRule] = {}
    for i, rule in enumerate(node.responses):
        where = f"{config.path}: nodes.{node.name}.responses[{i}]"
        if rule.on in rules:
            raise IronbirdError(Code.CFG_SCHEMA, f"{where}.on: duplicate rule for {rule.on!r}")
        command = model.by_name.get(rule.on) if rule.on != "*" else None
        if rule.on != "*" and (command is None or command.kind is not MessageKind.COMMAND):
            raise IronbirdError(
                Code.CFG_REF, f"{where}.on: {rule.on!r} is not a command in {model.source}"
            )
        reply = model.by_name.get(rule.reply)
        if reply is None or reply.kind is not MessageKind.TELEMETRY:
            raise IronbirdError(
                Code.CFG_REF, f"{where}.reply: {rule.reply!r} is not telemetry in {model.source}"
            )
        names = {f.name for f in reply.fields}
        referenced = [*rule.fields, *rule.echo]
        for counter in (rule.accepted_counter, rule.rejected_counter):
            if counter is not None:
                referenced.append(counter)
                field = reply.field(counter)
                if field is not None and field.type.type_class is not TypeClass.INT:
                    raise IronbirdError(
                        Code.CFG_SCHEMA, f"{where}: counter {counter} must be an integer field"
                    )
        for name in referenced:
            if name not in names:
                raise IronbirdError(
                    Code.CFG_REF, f"{where}: reply {reply.name} has no field {name!r}"
                )
        if command is None and rule.echo:
            raise IronbirdError(Code.CFG_SCHEMA, f"{where}.echo: not allowed on a '*' rule")
        if command is not None:
            for arg in rule.echo.values():
                if command.field(arg) is None:
                    raise IronbirdError(
                        Code.CFG_REF,
                        f"{where}.echo: command {command.name} has no argument {arg!r}",
                    )
        codec.encode_telemetry(rule.reply, rule.fields)  # surfaces bad literal values now
        rules[rule.on] = rule
    codec.reset_sequences()
    return rules


class Responder:
    """Opened command endpoint of a flight-side node; iterate to process commands."""

    def __init__(
        self,
        session: Session,
        route: Route,
        codec: Codec,
        capture: Capture,
        rules: Mapping[str, ResponseRule],
    ) -> None:
        self._session = session
        self._route = route
        self._codec = codec
        self._capture = capture
        self._decoder = session.decoder(report_checked=False)
        self._rules = rules
        self._accepted = 0
        self._rejected = 0

    @property
    def accepted(self) -> int:
        return self._accepted

    @property
    def rejected(self) -> int:
        return self._rejected

    @property
    def local_address(self) -> tuple[str, int] | None:
        return self._capture.addresses[0][1]

    def __aiter__(self) -> AsyncIterator[Handled]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Handled]:
        session = self._session
        async for frame in self._capture:
            result = self._decoder.decode(frame)
            if isinstance(result, DecodeError):  # reported once, at ingest
                self._rejected += 1
                yield Handled(frame.time_ns, None, result, None)
                continue
            command = result.packet
            self._accepted += 1
            session.ledger.record(
                "command",
                "rx.command",
                command=command.message.name,
                mid=command.message.mid,
                function_code=command.message.function_code,
                sequence=command.header.sequence,
                args=command.as_dict(),
                source=frame.source,
            )
            reply = await self._answer(command)
            yield Handled(frame.time_ns, command, None, reply)

    async def _answer(self, command: DecodedPacket) -> EncodedPacket | None:
        session = self._session
        rule = self._rules.get(command.message.name) or self._rules.get("*")
        if rule is None:
            return None
        values: dict[str, Any] = dict(rule.fields)
        args = command.as_dict()
        for reply_field, arg in rule.echo.items():
            if arg not in args:
                session.report(
                    IronbirdError(
                        Code.RSP_RULE,
                        f"rule {rule.on!r}: command {command.message.name} has no argument "
                        f"{arg!r} to echo into {reply_field}; no reply sent",
                    )
                )
                return None
            values[reply_field] = args[arg]
        reply = self._codec.model.by_name[rule.reply]
        for counter, count in (
            (rule.accepted_counter, self._accepted),
            (rule.rejected_counter, self._rejected),
        ):
            if counter:
                field = reply.field(counter)
                assert field is not None and field.type.type_class is TypeClass.INT
                lo, hi = field.type.bit_range
                values[counter] = lo + (count - lo) % (hi - lo + 1)  # flight counters wrap
        try:
            encoded = self._codec.encode_telemetry(rule.reply, values)
            await session._engine.send(self._route.link, link_end(self._route.side), encoded.data)
        except IronbirdError as exc:
            session.report(exc)
            return None
        session.ledger.record(
            "command",
            "tx.telemetry",
            message=encoded.message.name,
            mid=encoded.mid,
            sequence=encoded.sequence,
            size=len(encoded.data),
            raw=encoded.data,
        )
        return encoded

    async def close(self) -> None:
        await self._capture.close()
        self._session.ledger.record(
            "action", "respond.stop", accepted=self._accepted, rejected=self._rejected
        )
