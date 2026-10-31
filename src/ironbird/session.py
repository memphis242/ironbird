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
from ironbird.ccdd.model import (
    CcddModel,
    EventSeverity,
    Message,
    MessageKind,
    TypeClass,
    meets_floor,
)
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
from ironbird.filter import Filter, FilterError, Matcher, compile_filter
from ironbird.frame import Decoded, Frame, Stream
from ironbird.header import load_profile
from ironbird.ledger import Ledger
from ironbird.ledgertiers import Tier
from ironbird.notice import Level, Notice
from ironbird.transport import LinkEnd

TICK_S: Final = 1.0
EVENT_QUEUE_SIZE: Final = 1024
LEDGER_FILTER_MIDS: Final = 32  # MIDs of a filter's candidate set a ledger record lists
SHOWN_MIDS: Final = 4  # MIDs named in a one-line error


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


def _notice_key(code: Code, mid: int | None, context: str) -> str:
    return f"{code.value}:{mid}:{context}"


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
        ledger.set_notifier(notify)
        self._codecs: dict[str, Codec] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._last_tx: TxAttempt | None = None
        self._engine: Engine = Engine(
            config, ledger, self.report, self.codec_for, withdraw=self.withdraw_notices
        )

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
                self.ledger.settle()
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
            self.ledger.tick()
            await self._engine.tick()

    # -- error reporting ---------------------------------------------------------------

    def flush_notices(self) -> None:
        """Emit every pending coalesced summary now (shutdown, or before reading the output)."""
        self._coalescer.flush()
        self.ledger.flush_notices()

    def withdraw_notices(self, code: Code, context: str) -> None:
        """Drop the pending coalesced summary of ``code`` reported with ``context``, because a
        fatal error about the same thing is being raised to the user and ends it."""
        self._coalescer.discard(_notice_key(code, None, context))

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
        sample: str | None = None,
    ) -> None:
        """Ledger in full, notify the user coalesced (one notice per key per window).

        ``evidence`` marks an error that untrusted wire input can produce at will; its ledger
        record then belongs to the evidence tier instead of the essential one, and ``sample``
        names its evidence-sampling key.
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
            err,
            tier=Tier.EVIDENCE if evidence else None,
            sample=sample,
            role=self.node.name,
            mid=raw_mid,
            offset=offset,
        )
        key = _notice_key(err.code, raw_mid, context)
        self._coalescer.report(
            err.code,
            level,
            key,
            err.summary,
            ledger_n=ledger_n,
            group=err.code,
            ledger_part=self.ledger.part_number,
            ledger_file=self.ledger.part_file,
        )

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
        header: Mapping[str, int] | None = None,
    ) -> SentCommand:
        route = self.route(peer, require=Side.COMMANDER, link_name=link)
        codec = self.codec_for(route.server.name)
        endpoint = route.link.command_endpoint
        extra: dict[str, Any] = {"header": dict(header)} if header else {}
        self.ledger.record(
            "action",
            "send.request",
            node=route.server.name,
            command=command,
            args=dict(args or {}),
            tamper=dict(tamper or {}),
            link=route.link.name,
            **extra,
        )
        encoded = codec.encode_command(command, args, tamper=tamper, header=header)
        self.ledger.check_transmit(encoded.message.name, route.server.name)
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
            self.ledger.record_tx(
                "tx.command",
                encoded.message.name,
                route.server.name,
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
        queue_bytes: int | None = None,
    ) -> Capture:
        """Start receiving on ``routes``: an async iterator of every frame they carry.

        ``sinks`` see each frame, synchronously, before any consumer queue (and also the frames
        this session sends); a consumer that falls behind loses frames from its own queue only
        (``IB-ENG-001``). The queue is bounded by ``queue_frames`` and ``queue_bytes``. Iterating
        a capture raises the ``IB-TX-003`` error that ends it if a link end can no longer
        receive. Close the capture when done.
        """
        ends = [(route.link, link_end(route.side)) for route in routes]
        return await self._engine.open_capture(
            ends, sinks=sinks, queue_frames=queue_frames, queue_bytes=queue_bytes
        )

    def streams_for(self, routes: Sequence[Route]) -> tuple[Stream, ...]:
        """The receive and send streams of ``routes``, in order (ids are fixed from here on), so a
        recorder can declare them before the capture that feeds it opens."""
        return self._engine.streams_for([(route.link, link_end(route.side)) for route in routes])

    def record_routes(
        self, nodes: Sequence[str] | None = None, *, link_name: str | None = None
    ) -> list[Route]:
        """Every link of the node ironbird acts as (or those to ``nodes`` / named ``link_name``),
        each as the route a capture on this side of it receives from, for ``ib record``."""
        me = self.node.name
        wanted: set[str] | None = None
        if nodes:
            wanted = {self.config.resolve(name).name for name in nodes}
        routes: list[Route] = []
        for lk in self.config.links:
            if me not in (lk.source, lk.destination):
                continue
            peer = lk.destination if lk.source == me else lk.source
            if (wanted is not None and peer not in wanted) or (
                link_name is not None and lk.name != link_name
            ):
                continue
            side = Side.COMMANDER if lk.source == me else Side.RESPONDER
            routes.append(
                Route(lk, side, self.config.nodes[peer], self.config.nodes[lk.destination])
            )
        if not routes:
            raise IronbirdError(
                Code.CFG_LINK,
                f"{me!r} has no link to record"
                + (f" named {link_name!r}" if link_name else "")
                + (f" to {', '.join(sorted(wanted))}" if wanted else ""),
                details="links: " + ", ".join(link.name for link in self.config.links),
            )
        seen: dict[str, Route] = {}
        for route in routes:
            end = link_end(route.side)
            rx = (
                route.link.telemetry_endpoint
                if end is LinkEnd.SOURCE
                else route.link.command_endpoint
            )
            clash = seen.setdefault(str(rx), route)
            if clash is not route:
                raise IronbirdError(
                    Code.CFG_LINK,
                    f"links {clash.link.name!r} and {route.link.name!r} both receive on {rx}; "
                    "select one with --link",
                )
        return routes

    def add_sink(self, sink: FrameSink) -> None:
        """Attach ``sink`` to every frame this session ingests or sends until it is removed.

        A sink attached by several holders (this call, captures naming it) sees each frame once
        and stays until each has removed it; one that raises is detached and reported once.
        """
        self._engine.add_sink(sink)

    def remove_sink(self, sink: FrameSink) -> None:
        """Release one ``add_sink`` of ``sink``."""
        self._engine.remove_sink(sink)

    def decoder(self, *, report_checked: bool) -> FrameDecoder:
        """A decoder for frames of this session's streams.

        ``report_checked`` is ``False`` for frames that came through live ingest, which already
        reported what the check found, and ``True`` where nothing did (replay).
        """
        return self._engine.decoder(report_checked=report_checked)

    def compile_filter(self, text: str, nodes: Sequence[str]) -> Filter:
        """Compile the ``--where`` expression ``text`` against the definitions of ``nodes`` (the
        nodes whose frames it will test) and this configuration's link names. Raises
        ``IB-FLT-*`` (exit 2) before anything is opened."""
        codecs = {name: self.codec_for(name) for name in dict.fromkeys(nodes)}
        return compile_filter(
            text,
            codecs,
            links=[link.name for link in self.config.links],
            aliases={node.alias: node.name for node in self.config.nodes.values()},
        )

    @staticmethod
    def _require_overlap(flt: Filter, allowed: frozenset[int] | None, what: str) -> None:
        """Refuse a filter whose candidate MIDs share none with ``allowed`` (``what`` names that
        set for the message): no frame could pass both, so the run would show nothing."""
        if flt.mids is None or not allowed or not flt.mids.isdisjoint(allowed):
            return
        raise FilterError(
            Code.FLT_TYPE,
            flt.source,
            0,
            f"the filter can never match: it selects MID {_mids_text(flt.mids)} "
            f"but {what} {_mids_text(allowed)}",
        )

    def matcher(self, flt: Filter, decoder: FrameDecoder) -> Matcher:
        """A matcher of ``flt`` that decodes through ``decoder`` (the consumer's own, so a frame
        the filter decodes is not decoded again)."""
        return flt.bind(decoder.decode, self.codec_for)

    # -- listen (either side) -----------------------------------------------------

    async def listen(
        self,
        peer: str,
        *,
        mids: frozenset[int] | None = None,
        link: str | None = None,
        where: str | None = None,
    ) -> Listener:
        """Open a listener for ``peer``. ``where`` is a filter expression (S4) that frames must
        match to be yielded; it is compiled first, so a bad one fails before any socket opens."""
        route, flt = self.plan_listen(peer, mids=mids, link=link, where=where)
        return await self._open_listener(route, mids, flt)

    def plan_listen(
        self,
        peer: str,
        *,
        mids: frozenset[int] | None = None,
        link: str | None = None,
        where: str | None = None,
    ) -> tuple[Route, Filter | None]:
        """Everything :meth:`listen` decides before it opens a socket: the route and the compiled
        filter. Raises the usage errors (``IB-CFG-*``, ``IB-FLT-*``) without opening anything or
        writing a ledger record, so a front end can refuse a bad invocation up front."""
        route = self.route(peer, link_name=link)
        flt = self.compile_filter(where, [route.server.name]) if where is not None else None
        if flt is not None:
            self._require_overlap(flt, mids, "--mid allows")
        return route, flt

    def _listen_endpoint(self, route: Route) -> str:
        link = route.link
        if route.side is Side.COMMANDER:
            return str(link.telemetry_endpoint)
        return str(link.command_endpoint)

    async def _open_listener(
        self, route: Route, mids: frozenset[int] | None, flt: Filter | None
    ) -> Listener:
        capture = await self.capture([route])
        self._record_listen_start(route, mids, flt)
        return Listener(self, route, capture, mids, flt)

    def _record_listen_start(
        self, route: Route, mids: frozenset[int] | None, flt: Filter | None = None
    ) -> None:
        # A filter is recorded only when there is one; its candidate MIDs by count and the
        # first few, because the set can be as large as the CCDD.
        where: dict[str, Any] = {}
        if flt is not None:
            where["where"] = flt.source
            if flt.mids is not None:
                ordered = sorted(flt.mids)
                where["where_mid_count"] = len(ordered)
                where["where_mids"] = ordered[:LEDGER_FILTER_MIDS]
        self.ledger.record(
            "action",
            "listen.start",
            peer=route.peer.name,
            side=route.side.value,
            link=route.link.name,
            endpoint=self._listen_endpoint(route),
            mids=sorted(mids) if mids else None,
            **where,
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

    def event_routes(self, nodes: Sequence[str] | None = None) -> list[Route]:
        """The routes :meth:`events` captures: those of ``nodes`` (default: all) whose CCDD
        defines event telemetry. Empty when none does (``events`` then explains)."""
        return [
            route
            for route in self._event_routes(nodes)
            if self.codec_for(route.server.name).model.event_messages()
        ]

    def plan_events(
        self, nodes: Sequence[str] | None = None, *, where: str | None = None
    ) -> tuple[list[Route], dict[str, frozenset[int]], Filter | None]:
        """Everything :meth:`events` decides before it opens a socket: the routes that carry
        event telemetry, their event MIDs and the compiled filter. Raises the usage errors
        without opening anything (see :meth:`plan_listen`)."""
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
        flt = (
            self.compile_filter(where, [route.server.name for route in routes])
            if where is not None
            else None
        )
        if flt is not None:
            self._require_overlap(flt, frozenset().union(*event_mids.values()), "events carry")
        return routes, event_mids, flt

    async def events(
        self,
        nodes: Sequence[str] | None = None,
        *,
        min_severity: EventSeverity | None = None,
        where: str | None = None,
    ) -> EventStream:
        """Listen on every telemetry link of ``nodes`` (default: all) for event packets.

        Every received event is ledgered in full; ``min_severity`` and ``where`` (a filter
        expression, S4, tested on the event's frame) only limit what the stream yields. Nodes
        whose CCDD defines no event messages are skipped with a notice; if none is left, that
        is an error.
        """
        routes, event_mids, flt = self.plan_events(nodes, where=where)
        capture = await self.capture(routes)
        for route in routes:
            self._record_listen_start(route, event_mids[route.server.name], flt)
        recorded: dict[str, Any] = {"where": flt.source} if flt is not None else {}
        self.ledger.record(
            "action",
            "events.start",
            nodes=[route.server.name for route in routes],
            min_severity=min_severity.name if min_severity else None,
            **recorded,
        )
        return EventStream(self, routes, capture, event_mids, min_severity, flt)

    # -- responder ------------------------------------------------------------------

    def respond_route(self, *, peer: str | None = None, link: str | None = None) -> Route:
        """The link :meth:`respond` answers on (``peer``/``link`` narrow it when several end at
        this node)."""
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
        return Route(chosen, Side.RESPONDER, self.config.nodes[chosen.source], self.node)

    async def respond(self, *, peer: str | None = None, link: str | None = None) -> Responder:
        route = self.respond_route(peer=peer, link=link)
        chosen = route.link
        codec = self.codec_for(self.node.name)
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


def _mids_text(mids: frozenset[int]) -> str:
    """Up to :data:`SHOWN_MIDS` of ``mids`` in hex, for a one-line message."""
    ordered = sorted(mids)
    shown = ", ".join(f"0x{mid:04X}" for mid in ordered[:SHOWN_MIDS])
    return f"{shown}, ... ({len(ordered)})" if len(ordered) > SHOWN_MIDS else shown


class Listener:
    """Opened receive endpoint; iterate for decoded packets. Close when done."""

    def __init__(
        self,
        session: Session,
        route: Route,
        capture: Capture,
        mids: frozenset[int] | None,
        where: Filter | None = None,
    ) -> None:
        self._session = session
        self._route = route
        self._capture = capture
        self._decoder = session.decoder(report_checked=False)
        self._mids = mids
        self._matcher: Matcher | None = (
            session.matcher(where, self._decoder) if where is not None else None
        )
        self._undecodable = 0  # frames the filter matched that do not decode (shown nowhere)

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
        # Frames are taken off the capture's own queue as they are handled, so an iteration that
        # is abandoned (a ``break``) leaves the rest for the next one.
        mids, decode, wait_frames = self._mids, self._decoder.decode, self._capture.wait_frames
        matcher = self._matcher
        while unread := await wait_frames():
            popleft = unread.popleft
            while unread:
                frame = popleft()
                if mids is not None and frame.mid not in mids:
                    continue  # a MID filter hides valid packets only; ingest reported the rest
                if matcher is not None:
                    if not matcher(frame):
                        continue
                    result = matcher.decoded(frame)  # the filter's decode, if it made one
                else:
                    result = decode(frame)
                if not isinstance(result, DecodeError):
                    yield result
                elif matcher is not None:
                    self._undecodable += 1

    async def close(self) -> None:
        await self._capture.close()
        stats = self._capture.stats()
        undecodable: dict[str, Any] = (
            {"where_undecodable": self._undecodable} if self._matcher is not None else {}
        )
        self._session.ledger.record(
            "action", "listen.stop", peer=self._route.peer.name, unread=stats.unread, **undecodable
        )
        if stats.unread and stats.not_shown:
            # The queue refused frames, so the consumer was behind and what is still queued was
            # never shown. Frames merely in flight when listening ended, or lost to the kernel
            # (reported as IB-TX-004), are not a consumer lag.
            self._session.report(
                IronbirdError(
                    Code.ENG_LAG,
                    f"listening ended with {stats.unread} frame(s) received but not shown",
                )
            )


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
        where: Filter | None = None,
    ) -> None:
        assert routes
        self._session = session
        self._routes = tuple(routes)
        self._capture = capture
        self._decoder = session.decoder(report_checked=False)
        self._matcher: Matcher | None = (
            session.matcher(where, self._decoder) if where is not None else None
        )
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
                decoded = (
                    self._matcher.decoded(frame)
                    if self._matcher is not None
                    else self._decoder.decode(frame)
                )
                if isinstance(decoded, DecodeError):
                    continue
                event = extract_event(node, frame.time_ns, decoded.packet)
                if event is None:  # the filter above keeps event MIDs; stay defensive
                    continue
                ledger.record("event", "rx.event", **event_record_fields(event, frame))
                floor = self._min_severity
                if floor is not None and not meets_floor(event.severity, floor):
                    continue
                if self._matcher is not None and not self._matcher(frame):
                    continue  # ledgered above; the filter only limits what is shown
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


def event_record_fields(event: Event, frame: Frame) -> dict[str, Any]:
    """The fields of the ``rx.event`` ledger record: the event in full, raw packet included
    (its size is what the event allowance is sized by)."""
    return {
        "node": event.node,
        "app": event.app,
        "event_id": event.event_id,
        "type": event.type_label,
        "type_value": event.type_value,
        "severity": event.severity.name if event.severity else None,
        "text": event.text,
        "message": event.name,
        "mid": event.mid,
        "sequence": event.sequence,
        "source": frame.source,
        "raw": frame.data,
    }


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
                sample=f"rx.command:{command.message.name}",
                seq=frame.seq,
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
        if session.ledger.withhold_reply():
            return None  # on_full = "refuse_commands" and the ledger is stalled: counted, not sent
        try:
            encoded = self._codec.encode_telemetry(rule.reply, values)
            await session._engine.send(self._route.link, link_end(self._route.side), encoded.data)
        except IronbirdError as exc:
            session.report(exc)
            return None
        session.ledger.record_reply(
            encoded.message.name,
            size=len(encoded.data),
            seq=encoded.sequence,
            mid=encoded.mid,
            raw=encoded.data,
        )
        return encoded

    async def close(self) -> None:
        await self._capture.close()
        self._session.ledger.record(
            "action", "respond.stop", accepted=self._accepted, rejected=self._rejected
        )
