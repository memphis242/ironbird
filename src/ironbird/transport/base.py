"""The transport contract: what the engine needs from UDP now and serial / 1553 later.

A transport moves raw bytes and never interprets them. Receiving is push based: the transport
calls the :class:`Receiver` it was opened with from the event loop's reader callback, once per
datagram (or recovered frame), so the engine can hand every frame to its sinks before any
consumer queue is touched.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Final, Protocol

from ironbird.errors import Code, IronbirdError


class LinkEnd(StrEnum):
    """Which end of a link this process is."""

    SOURCE = "source"  # sends commands, receives telemetry
    DESTINATION = "destination"  # receives commands, sends telemetry


# Called on the event-loop thread for each received frame as (data, time_ns, source, orig_len,
# flags), where ``flags`` is the transport's ``FrameFlag`` bits as a plain int (0 for none); it
# must be cheap and must never raise.
Receiver = Callable[[bytes, int, str, int, int], None]

NO_FLAGS: Final = 0


class TransportClosed(IronbirdError):
    """A send reached a transport that was closed before the bytes left the process.

    Nothing was sent. The engine sends again on the transport that replaces it; this is not a
    failure of the link.
    """

    def __init__(self) -> None:
        super().__init__(Code.TX_SEND, "send on a closed transport")


@dataclass(frozen=True, slots=True)
class TransportStats:
    """A frozen snapshot of a transport's counters (cumulative since it opened)."""

    rx_frames: int = 0
    rx_bytes: int = 0
    tx_frames: int = 0
    tx_bytes: int = 0
    kernel_dropped: int = 0  # lost to the OS before the transport saw them (buffer overflow)
    unread_at_close: int = 0  # still in the OS buffer when the transport closed, discarded


# What a factory may call to surface a problem found away from any caller (e.g. at open time).
ErrorReporter = Callable[[IronbirdError], None]


class Transport(Protocol):
    """One link end. A transport open for a link is owned by the session and shared by its
    sends and receives."""

    @property
    def description(self) -> str:
        """Sanitized display text, e.g. ``udp 127.0.0.1:2234``."""
        ...

    @property
    def local_address(self) -> tuple[str, int] | None:
        """Bound network address once receiving (UDP); ``None`` for other transports."""
        ...

    async def open(self, receiver: Receiver | None) -> None:
        """Open the link end. ``None`` makes it send-only. Raises a coded ``IB-TX-001``."""
        ...

    async def send(
        self,
        data: bytes,
        *,
        confirm: bool = False,
        handed_off: Callable[[], None] | None = None,
    ) -> None:
        """Send one packet. ``handed_off`` is called as soon as the bytes have left the
        process; ``confirm`` then also waits for the transport to report a refusal."""
        ...

    def stats(self) -> TransportStats: ...

    def facts(self) -> Mapping[str, object]:
        """Settings in effect after open, for the ledger (e.g. the receive buffer granted)."""
        ...

    async def poll(self) -> None:
        """Refresh counters that cost a file read or syscall; about once a second, off the loop."""
        ...

    async def close(self) -> None: ...
