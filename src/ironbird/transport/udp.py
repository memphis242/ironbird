"""UDP transport on asyncio datagram endpoints.

Two shapes, chosen by which endpoints are given:

* sender (``remote`` only): a connected socket, so ICMP "port unreachable" surfaces as a
  coded send error instead of vanishing;
* listener (``local``, optional ``reply_to``): a bound, unconnected socket that accepts
  datagrams from any peer and sends replies with ``sendto``.

The receive path never blocks: when the bounded queue is full, datagrams are counted in
``dropped`` and discarded, and the session reports that as one coalesced error.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import time
from collections.abc import AsyncIterator
from typing import Any, Final

from ironbird.config import Endpoint
from ironbird.errors import Code, IronbirdError
from ironbird.transport import Datagram

QUEUE_SIZE: Final = 16384
RCVBUF_BYTES: Final = 4 * 1024 * 1024
CONFIRM_WAIT_S: Final = 0.05


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, owner: UdpTransport) -> None:
        self._owner = owner

    def datagram_received(self, data: bytes, addr: Any) -> None:
        self._owner._on_datagram(data, addr)

    def error_received(self, exc: Exception) -> None:
        self._owner._on_error(exc)

    def connection_lost(self, exc: Exception | None) -> None:
        self._owner._on_lost()


class UdpTransport:
    def __init__(
        self,
        *,
        local: Endpoint | None = None,
        remote: Endpoint | None = None,
        reply_to: Endpoint | None = None,
        queue_size: int = QUEUE_SIZE,
    ) -> None:
        assert local is not None or remote is not None, "need a local or remote endpoint"
        assert remote is None or reply_to is None, "connected sockets cannot also reply_to"
        self._local = local
        self._remote = remote
        self._reply_to = reply_to
        self._queue: asyncio.Queue[Datagram | None] = asyncio.Queue(maxsize=queue_size)
        self._transport: asyncio.DatagramTransport | None = None
        self._reply_addr: tuple[Any, ...] | None = None
        self._dropped = 0
        self._pending_error: OSError | None = None
        self._closed = False

    @property
    def dropped(self) -> int:
        return self._dropped

    @property
    def local_address(self) -> tuple[str, int] | None:
        """Bound ``(host, port)`` once open; useful when binding to port 0 in tests."""
        if self._transport is None:
            return None
        name = self._transport.get_extra_info("sockname")
        return (str(name[0]), int(name[1])) if name else None

    # -- callbacks from the protocol (event-loop thread) -------------------------------

    def _on_datagram(self, data: bytes, addr: Any) -> None:
        stamp = time.time_ns()
        source = f"{addr[0]}:{addr[1]}" if addr else "?"
        try:
            self._queue.put_nowait(Datagram(data, stamp, source))
        except asyncio.QueueFull:
            self._dropped += 1

    def _on_error(self, exc: Exception) -> None:
        if isinstance(exc, OSError):
            self._pending_error = exc
        else:  # pragma: no cover - asyncio only reports OSError here
            self._pending_error = OSError(str(exc))

    def _on_lost(self) -> None:
        self._enqueue_sentinel()

    def _enqueue_sentinel(self) -> None:
        if self._queue.full():
            self._queue.get_nowait()  # losing the oldest datagram beats never terminating
            self._dropped += 1
        self._queue.put_nowait(None)

    # -- Transport API ---------------------------------------------------------------

    async def open(self) -> None:
        loop = asyncio.get_running_loop()
        where = self._local or self._remote
        assert where is not None
        try:
            if self._reply_to is not None:
                infos = await loop.getaddrinfo(
                    self._reply_to.host, self._reply_to.port, type=socket.SOCK_DGRAM
                )
                self._reply_addr = infos[0][4]
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _Protocol(self),
                local_addr=(self._local.host, self._local.port) if self._local else None,
                remote_addr=(self._remote.host, self._remote.port) if self._remote else None,
            )
        except (OSError, UnicodeError) as exc:
            action = "bind" if self._local else "connect to"
            raise IronbirdError(
                Code.TX_OPEN,
                f"cannot {action} udp {where}: {getattr(exc, 'strerror', None) or exc}",
                cause=exc,
            ) from exc
        self._transport = transport  # type: ignore[assignment]
        sock = transport.get_extra_info("socket")
        if sock is not None:
            with contextlib.suppress(OSError):  # best effort; the kernel keeps its default
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RCVBUF_BYTES)

    async def send(self, data: bytes, *, confirm: bool = False) -> None:
        """Send one datagram.

        With ``confirm`` the call waits briefly for an ICMP error (e.g. connection refused
        on a closed port) and raises it as a coded error. Fire-and-forget otherwise.
        """
        transport = self._transport
        if transport is None or self._closed:
            raise IronbirdError(Code.TX_SEND, "send on a closed transport")
        target = self._remote or self._reply_to
        assert target is not None, "transport has no destination"
        self._pending_error = None
        try:
            if self._remote is not None:
                transport.sendto(data)
            else:
                transport.sendto(data, self._reply_addr)
        except OSError as exc:
            raise self._send_error(target, exc) from exc
        if confirm:
            await asyncio.sleep(CONFIRM_WAIT_S)
        error = self._pending_error
        if error is not None:
            self._pending_error = None
            raise self._send_error(target, error) from error

    @staticmethod
    def _send_error(target: Endpoint, exc: OSError) -> IronbirdError:
        if isinstance(exc, ConnectionRefusedError):
            summary = f"nothing is listening at udp {target} (connection refused)"
        else:
            summary = f"send to udp {target} failed: {exc.strerror or exc}"
        return IronbirdError(Code.TX_SEND, summary, cause=exc)

    async def receive(self) -> AsyncIterator[Datagram]:
        while True:
            item = await self._queue.get()
            if item is None:
                return
            yield item

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._transport is not None:
            self._transport.close()
        self._enqueue_sentinel()
