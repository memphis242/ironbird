"""The engine API both front-ends use. In Stage 1 it runs in-process.

A :class:`Session` is one invocation of ironbird *acting as* one node. Commander side
(ground): encode commands, decode telemetry. Responder side (a flight node): decode commands,
answer with telemetry. The same codec serves both. Requests and results are plain frozen
values so the surface can later be exposed over a control socket unchanged.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import CcddModel, EventSeverity, Message, MessageKind, TypeClass
from ironbird.coalesce import Coalescer
from ironbird.codec import Codec, DecodedPacket, EncodedPacket
from ironbird.config import Link, Node, ResponseRule, SystemConfig
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.events import Event, extract_event
from ironbird.header import load_profile
from ironbird.ledger import Ledger
from ironbird.transport import Datagram
from ironbird.transport.udp import UdpTransport

MAX_TRACKED_MIDS: Final = 1024
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
class Received:
    time_ns: int
    node: str  # node whose CCDD decoded the packet
    packet: DecodedPacket
    source: str
    raw: bytes = b""


@dataclass(frozen=True, slots=True)
class Handled:
    """One command processed by a responder."""

    time_ns: int
    command: DecodedPacket | None  # None when the datagram was rejected
    error: DecodeError | None
    reply: EncodedPacket | None


@dataclass(slots=True)
class MidStat:
    count: int
    last_ns: int


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
        notify: Callable[[str], None],
    ) -> None:
        self.config: SystemConfig = config
        self.node: Node = config.resolve(role) if role else config.nodes[config.acts_as]
        self.ledger: Ledger = ledger
        self.coalescer: Coalescer = Coalescer(notify)
        self._codecs: dict[str, Codec] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self.stats: dict[int, MidStat] = {}
        self.stats_overflow = 0

    # -- lifecycle -----------------------------------------------------------------

    async def __aenter__(self) -> Session:
        loop = asyncio.get_running_loop()
        self._tasks = [
            loop.create_task(self.ledger.drain_task(), name="ledger-drain"),
            loop.create_task(self._ticker(), name="coalescer-tick"),
        ]
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
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self.coalescer.flush()
        self.ledger.flush()

    async def _ticker(self) -> None:
        while True:
            await asyncio.sleep(TICK_S)
            self.coalescer.tick()

    # -- error reporting ---------------------------------------------------------------

    def fatal(self, error: IronbirdError) -> None:
        """Ledger an error that is leaving the session (the caller prints it)."""
        self.ledger.record_error(error, role=self.node.name)

    def report(self, error: IronbirdError | DecodeError, *, context: str = "") -> None:
        """Ledger in full, notify the user coalesced (one line per key per window)."""
        if isinstance(error, DecodeError):
            raw_mid = error.mid
            err = error.to_error(context)
            offset: int | None = error.offset
        else:
            raw_mid = None
            err = error
            offset = None
        self.ledger.record_error(err, role=self.node.name, mid=raw_mid, offset=offset)
        key = f"{err.code.value}:{raw_mid}:{context}"
        self.coalescer.report(key, err.user_line(), group=err.code.value)

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

    def find(self, query: str) -> list[Match]:
        """Tiered lookup: exact name/MID, then name substring, then description/field text."""
        needle = query.strip().casefold()
        if not needle:
            return []
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
        return found

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
        transport = UdpTransport(remote=endpoint)
        result = "sent"
        try:
            await transport.open()
            await transport.send(encoded.data, confirm=True)
        except IronbirdError as exc:
            result = exc.code.value
            raise
        finally:
            await transport.close()
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

    # -- shared ingest (peek, filter, count, decode through the boundary) -------------

    def _count(self, mid: int, time_ns: int) -> None:
        stat = self.stats.get(mid)
        if stat is not None:
            stat.count += 1
            stat.last_ns = time_ns
        elif len(self.stats) < MAX_TRACKED_MIDS:
            self.stats[mid] = MidStat(1, time_ns)
            self.ledger.record("state", "rx.first", mid=mid)
        else:
            self.stats_overflow += 1

    def _ingest(
        self,
        codec: Codec,
        kind: MessageKind,
        datagram: Datagram,
        mids: frozenset[int] | None,
        source_label: str,
    ) -> DecodedPacket | DecodeError | None:
        """``None`` means filtered out; errors are already reported (coalesced)."""
        self.ledger.mark_busy()
        data = datagram.data
        mid = codec.peek_mid(kind, data)
        if mid is not None:
            if mids is not None and mid not in mids:
                self._count(mid, datagram.time_ns)
                return None
            self._count(mid, datagram.time_ns)
            if not codec.is_known_mid(kind, mid):
                error = DecodeError(
                    Code.DEC_UNKNOWN, 0, f"unknown {kind.value} MID 0x{mid:04X}", mid
                )
                self.ledger.record("packet", "rx.malformed", raw=data, source=datagram.source)
                self.report(error, context=f"from {source_label}")
                return error
        result = codec.decode(kind, data)
        if isinstance(result, DecodeError):
            self.ledger.record("packet", "rx.malformed", raw=data, source=datagram.source)
            self.report(result, context=f"from {source_label}")
        return result

    def _check_drops(self, transport: UdpTransport, seen: int, label: str) -> int:
        dropped = transport.dropped
        if dropped != seen:
            self.report(
                IronbirdError(
                    Code.TX_OVERRUN,
                    f"{label}: receive queue overflow, {dropped - seen} datagram(s) dropped",
                )
            )
        return dropped

    # -- listen (either side) -----------------------------------------------------

    async def listen(
        self, peer: str, *, mids: frozenset[int] | None = None, link: str | None = None
    ) -> Listener:
        route = self.route(peer, link_name=link)
        return await self._open_listener(route, mids)

    async def _open_listener(self, route: Route, mids: frozenset[int] | None) -> Listener:
        codec = self.codec_for(route.server.name)
        if route.side is Side.COMMANDER:
            kind, endpoint = MessageKind.TELEMETRY, route.link.telemetry_endpoint
        else:
            kind, endpoint = MessageKind.COMMAND, route.link.command_endpoint
        transport = UdpTransport(local=endpoint)
        await transport.open()
        self.ledger.record(
            "action",
            "listen.start",
            peer=route.peer.name,
            side=route.side.value,
            link=route.link.name,
            endpoint=str(endpoint),
            mids=sorted(mids) if mids else None,
        )
        return Listener(self, route, codec, kind, transport, mids)

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
        listeners: list[Listener] = []
        try:
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
                listeners.append(await self._open_listener(route, mids))
            if not listeners:
                raise IronbirdError(
                    Code.EVT_NONE,
                    "no selected node defines event telemetry; mark event messages with "
                    '"category": "event" and an "event" mapping in the CCDD',
                )
        except BaseException:
            for opened in listeners:
                await opened.close()
            raise
        self.ledger.record(
            "action",
            "events.start",
            nodes=[lst.node for lst in listeners],
            min_severity=min_severity.name if min_severity else None,
        )
        return EventStream(self, listeners, min_severity)

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
        transport = UdpTransport(local=chosen.command_endpoint, reply_to=chosen.telemetry_endpoint)
        await transport.open()
        self.ledger.record(
            "action",
            "respond.start",
            link=chosen.name,
            listen=str(chosen.command_endpoint),
            reply_to=str(chosen.telemetry_endpoint),
            rules=len(rules),
        )
        return Responder(self, route, codec, transport, rules)


class Listener:
    """Opened receive endpoint; iterate for decoded packets. Close when done."""

    def __init__(
        self,
        session: Session,
        route: Route,
        codec: Codec,
        kind: MessageKind,
        transport: UdpTransport,
        mids: frozenset[int] | None,
    ) -> None:
        self._session = session
        self._route = route
        self._codec = codec
        self._kind = kind
        self._transport = transport
        self._mids = mids

    @property
    def node(self) -> str:
        """Node whose CCDD decodes what arrives here."""
        return self._route.server.name

    @property
    def local_address(self) -> tuple[str, int] | None:
        return self._transport.local_address

    def __aiter__(self) -> AsyncIterator[Received]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Received]:
        seen_drops = 0
        label = self._route.peer.name
        async for datagram in self._transport.receive():
            result = self._session._ingest(self._codec, self._kind, datagram, self._mids, label)
            seen_drops = self._session._check_drops(self._transport, seen_drops, "listen")
            if isinstance(result, DecodedPacket):
                yield Received(
                    datagram.time_ns,
                    self._route.server.name,
                    result,
                    datagram.source,
                    datagram.data,
                )

    async def close(self) -> None:
        await self._transport.close()
        self._session.ledger.record("action", "listen.stop", peer=self._route.peer.name)


class EventStream:
    """Events from several listeners merged into one stream. Close when done.

    One pump task per listener decodes, ledgers (every event, in full) and queues; the
    bounded queue applies back-pressure to a pump, and from there to its socket queue, whose
    overflow the session already reports as one coalesced error.
    """

    def __init__(
        self,
        session: Session,
        listeners: Sequence[Listener],
        min_severity: EventSeverity | None,
    ) -> None:
        assert listeners
        self._session = session
        self._listeners = tuple(listeners)
        self._min_severity = min_severity
        self._queue: asyncio.Queue[Event | BaseException] = asyncio.Queue(EVENT_QUEUE_SIZE)
        loop = asyncio.get_running_loop()
        self._pumps = [
            loop.create_task(self._pump(listener), name=f"events-{listener.node}")
            for listener in self._listeners
        ]

    @property
    def endpoints(self) -> list[tuple[str, tuple[str, int] | None]]:
        """``(node, bound address)`` per listener."""
        return [(lst.node, lst.local_address) for lst in self._listeners]

    def __aiter__(self) -> AsyncIterator[Event]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Event]:
        while True:
            item = await self._queue.get()
            if isinstance(item, BaseException):
                raise item
            yield item

    async def _pump(self, listener: Listener) -> None:
        ledger = self._session.ledger
        try:
            async for rx in listener:
                event = extract_event(rx.node, rx.time_ns, rx.packet)
                if event is None:  # the listener filters on event MIDs; stay defensive
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
                    source=rx.source,
                    raw=rx.raw,
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
        for pump in self._pumps:
            pump.cancel()
        await asyncio.gather(*self._pumps, return_exceptions=True)
        for listener in self._listeners:
            await listener.close()
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
        transport: UdpTransport,
        rules: Mapping[str, ResponseRule],
    ) -> None:
        self._session = session
        self._route = route
        self._codec = codec
        self._transport = transport
        self._rules = rules
        self.accepted = 0
        self.rejected = 0

    @property
    def local_address(self) -> tuple[str, int] | None:
        return self._transport.local_address

    def __aiter__(self) -> AsyncIterator[Handled]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Handled]:
        session = self._session
        seen_drops = 0
        label = self._route.peer.name
        async for datagram in self._transport.receive():
            result = session._ingest(self._codec, MessageKind.COMMAND, datagram, None, label)
            seen_drops = session._check_drops(self._transport, seen_drops, "respond")
            if result is None:  # pragma: no cover - no filter is set
                continue
            if isinstance(result, DecodeError):
                self.rejected += 1
                yield Handled(datagram.time_ns, None, result, None)
                continue
            self.accepted += 1
            session.ledger.record(
                "command",
                "rx.command",
                command=result.message.name,
                mid=result.message.mid,
                function_code=result.message.function_code,
                sequence=result.header.sequence,
                args=result.as_dict(),
                source=datagram.source,
            )
            reply = await self._answer(result)
            yield Handled(datagram.time_ns, result, None, reply)

    async def _answer(self, command: DecodedPacket) -> EncodedPacket | None:
        session = self._session
        rule = self._rules.get(command.message.name) or self._rules.get("*")
        if rule is None:
            return None
        values: dict[str, Any] = dict(rule.fields)
        args = command.as_dict()
        for reply_field, arg in rule.echo.items():
            values[reply_field] = args[arg]
        reply = self._codec.model.by_name[rule.reply]
        for counter, count in (
            (rule.accepted_counter, self.accepted),
            (rule.rejected_counter, self.rejected),
        ):
            if counter:
                field = reply.field(counter)
                assert field is not None and field.type.type_class is TypeClass.INT
                lo, hi = field.type.bit_range
                values[counter] = lo + (count - lo) % (hi - lo + 1)  # flight counters wrap
        try:
            encoded = self._codec.encode_telemetry(rule.reply, values)
            await self._transport.send(encoded.data)
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
        await self._transport.close()
        self._session.ledger.record(
            "action", "respond.stop", accepted=self.accepted, rejected=self.rejected
        )
