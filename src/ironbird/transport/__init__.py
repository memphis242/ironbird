"""Transports move raw bytes; they never interpret them.

``TRANSPORTS`` maps a link's ``transport`` name to the factory that builds one link end, so the
session picks the transport from the configuration alone.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final

from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec
from ironbird.config import SERIAL_TRANSPORT, SUPPORTED_TRANSPORTS, UDP_TRANSPORT, Link
from ironbird.errors import Code, IronbirdError
from ironbird.header import DEFAULT_MAX_PACKET_BYTES
from ironbird.transport.base import (
    ErrorReporter,
    FailureHandler,
    LinkEnd,
    LinkStateHandler,
    Receiver,
    Transport,
    TransportClosed,
    TransportStats,
)
from ironbird.transport.framing import (
    NS_PER_MS,
    HeaderLengthFramer,
    validate_stream_profile,
)
from ironbird.transport.serial import SerialTransport
from ironbird.transport.udp import UdpTransport


@dataclass(frozen=True, slots=True)
class TransportParams:
    """What the session hands every transport factory besides the link and its end."""

    report: ErrorReporter
    failed: FailureHandler | None
    max_packet_bytes: int
    codec: Codec | None  # the codec of the link's destination node; a stream transport needs it
    link_state: LinkStateHandler | None


TransportFactory = Callable[[Link, LinkEnd, TransportParams], Transport]


def _udp(link: Link, end: LinkEnd, params: TransportParams) -> Transport:
    spec = link.udp
    assert spec is not None, f"link {link.name} has no udp table"
    match end:
        case LinkEnd.SOURCE:  # commands out to the destination, telemetry in
            return UdpTransport(
                local=spec.telemetry_endpoint,
                remote=spec.command_endpoint,
                report=params.report,
                failed=params.failed,
                max_packet_bytes=params.max_packet_bytes,
            )
        case LinkEnd.DESTINATION:  # commands in, telemetry back to the source
            return UdpTransport(
                local=spec.command_endpoint,
                reply_to=spec.telemetry_endpoint,
                report=params.report,
                failed=params.failed,
                max_packet_bytes=params.max_packet_bytes,
            )
    raise AssertionError(f"unhandled link end {end}")  # pragma: no cover


def _serial(link: Link, end: LinkEnd, params: TransportParams) -> Transport:
    spec, framing = link.serial, link.framing
    assert spec is not None and framing is not None, f"link {link.name} has no serial table"
    codec = params.codec
    if codec is None:
        raise IronbirdError(
            Code.INTERNAL, f"link {link.name!r}: a serial transport needs the codec of its node"
        )
    # The end receives what the other end sends: telemetry at the source, commands at the
    # destination, so that is the header profile its stream is framed with.
    kind = MessageKind.TELEMETRY if end is LinkEnd.SOURCE else MessageKind.COMMAND
    profile = codec.profile(kind)
    validate_stream_profile(profile, params.max_packet_bytes, f"link {link.name!r}")

    def bounds(mid: int) -> tuple[int, int] | None:
        return codec.payload_bounds(kind, mid)

    framer = HeaderLengthFramer(
        profile,
        max_packet_bytes=params.max_packet_bytes,
        sync=framing.sync,
        max_idle_ns=framing.max_idle_ms * NS_PER_MS,
        payload_bounds=bounds if framing.require_known_mid else None,
        largest_payload=codec.largest_payload(kind) if framing.require_known_mid else None,
    )
    return SerialTransport(
        spec,
        framing,
        framer,
        report=params.report,
        link_state=params.link_state,
        max_packet_bytes=params.max_packet_bytes,
    )


TRANSPORTS: Final[Mapping[str, TransportFactory]] = MappingProxyType(
    {UDP_TRANSPORT: _udp, SERIAL_TRANSPORT: _serial}
)

# config cannot import the transports (they import config), so the two lists are tied here:
# a transport the config accepts always has a factory.
assert set(TRANSPORTS) == set(SUPPORTED_TRANSPORTS), "config and transport registry disagree"


def open_transport(
    link: Link,
    end: LinkEnd,
    report: ErrorReporter,
    *,
    failed: FailureHandler | None = None,
    max_packet_bytes: int = DEFAULT_MAX_PACKET_BYTES,
    codec: Codec | None = None,
    link_state: LinkStateHandler | None = None,
) -> Transport:
    """Build the transport for one end of ``link``; the caller awaits ``open(receiver)``.

    ``report`` receives problems found away from any caller, such as a short receive buffer or a
    failed read. ``failed`` is called once if the transport can no longer receive (``IB-TX-003``);
    the transport then delivers nothing more. ``max_packet_bytes`` is the most a transport copies
    out of the OS for one frame; it cuts longer ones at the edge and reports the size on the wire.
    ``codec`` (of the link's destination node) is what a stream transport frames with; it is
    ignored by UDP. ``link_state`` hears a link that was up go down and come back (serial).
    """
    factory = TRANSPORTS.get(link.transport)
    if factory is None:  # config validation makes this unreachable for loaded configs
        raise IronbirdError(
            Code.CFG_SCHEMA, f"link {link.name!r}: no transport named {link.transport[:20]!r}"
        )
    return factory(link, end, TransportParams(report, failed, max_packet_bytes, codec, link_state))


__all__ = [
    "TRANSPORTS",
    "ErrorReporter",
    "FailureHandler",
    "LinkEnd",
    "LinkStateHandler",
    "Receiver",
    "SerialTransport",
    "Transport",
    "TransportClosed",
    "TransportFactory",
    "TransportParams",
    "TransportStats",
    "UdpTransport",
    "open_transport",
]
