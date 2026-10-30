"""Transports move raw bytes; they never interpret them.

``TRANSPORTS`` maps a link's ``transport`` name to the factory that builds one link end, so the
session picks the transport from the configuration alone.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Final

from ironbird.config import SUPPORTED_TRANSPORTS, Link
from ironbird.errors import Code, IronbirdError
from ironbird.header import DEFAULT_MAX_PACKET_BYTES
from ironbird.transport.base import (
    ErrorReporter,
    FailureHandler,
    LinkEnd,
    Receiver,
    Transport,
    TransportClosed,
    TransportStats,
)
from ironbird.transport.udp import UdpTransport

TransportFactory = Callable[[Link, LinkEnd, ErrorReporter, FailureHandler | None, int], Transport]


def _udp(
    link: Link,
    end: LinkEnd,
    report: ErrorReporter,
    failed: FailureHandler | None,
    max_packet_bytes: int,
) -> Transport:
    spec = link.udp
    assert spec is not None, f"link {link.name} has no udp table"
    match end:
        case LinkEnd.SOURCE:  # commands out to the destination, telemetry in
            return UdpTransport(
                local=spec.telemetry_endpoint,
                remote=spec.command_endpoint,
                report=report,
                failed=failed,
                max_packet_bytes=max_packet_bytes,
            )
        case LinkEnd.DESTINATION:  # commands in, telemetry back to the source
            return UdpTransport(
                local=spec.command_endpoint,
                reply_to=spec.telemetry_endpoint,
                report=report,
                failed=failed,
                max_packet_bytes=max_packet_bytes,
            )
    raise AssertionError(f"unhandled link end {end}")  # pragma: no cover


TRANSPORTS: Final[Mapping[str, TransportFactory]] = MappingProxyType({"udp": _udp})

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
) -> Transport:
    """Build the transport for one end of ``link``; the caller awaits ``open(receiver)``.

    ``report`` receives problems found away from any caller, such as a short receive buffer or a
    failed read. ``failed`` is called once if the transport can no longer receive (``IB-TX-003``);
    the transport then delivers nothing more. ``max_packet_bytes`` is the most a transport copies
    out of the OS for one frame; it cuts longer ones at the edge and reports the size on the wire.
    """
    factory = TRANSPORTS.get(link.transport)
    if factory is None:  # config validation makes this unreachable for loaded configs
        raise IronbirdError(
            Code.CFG_SCHEMA, f"link {link.name!r}: no transport named {link.transport[:20]!r}"
        )
    return factory(link, end, report, failed, max_packet_bytes)


__all__ = [
    "TRANSPORTS",
    "ErrorReporter",
    "FailureHandler",
    "LinkEnd",
    "Receiver",
    "Transport",
    "TransportClosed",
    "TransportFactory",
    "TransportStats",
    "UdpTransport",
    "open_transport",
]
