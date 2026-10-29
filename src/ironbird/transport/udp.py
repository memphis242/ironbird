"""UDP transport on a non-blocking socket driven by the asyncio selector.

Two shapes, chosen by which endpoints are given:

* sender (``remote`` only): a connected socket, so ICMP "port unreachable" surfaces as a
  coded send error instead of vanishing;
* listener (``local``, optional ``reply_to``): a bound, unconnected socket that accepts
  datagrams from any peer and sends replies with ``sendto``.

The receive path never blocks. Each wakeup reads a batch of datagrams with ``recvmsg``; when
the bounded queue is full, datagrams are counted in ``dropped``. Datagrams the *kernel*
discarded because the socket buffer was full never reach user space, so they are counted from
the kernel itself: the ``SO_RXQ_OVFL`` ancillary counter on every received datagram, and the
``drops`` column of ``/proc/net/udp`` (which also covers the tail of a flood, after which no
datagram arrives to carry the counter). The session reports both as coalesced errors.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import os
import socket
import struct
import sys
import time
from collections.abc import AsyncIterator, Collection, Iterable
from typing import Any, Final

from ironbird.config import Endpoint
from ironbird.errors import Code, IronbirdError
from ironbird.transport import Datagram

QUEUE_SIZE: Final = 16384
RCVBUF_BYTES: Final = 4 * 1024 * 1024
CONFIRM_WAIT_S: Final = 0.05
BATCH: Final = 256  # datagrams read per wakeup before yielding to the loop
MAX_DATAGRAM: Final = 65536
PROC_TABLES: Final = ("/proc/net/udp", "/proc/net/udp6")
PROC_INODE_COLUMN: Final = 9
PROC_MIN_COLUMNS: Final = 13  # the last one is "drops"
# Not exported by Python's socket module on every version; the value is Linux ABI.
SO_RXQ_OVFL: Final = getattr(socket, "SO_RXQ_OVFL", 40)
_U32: Final = 0xFFFFFFFF

_OPEN_REMEDY: Final = {
    errno.EAFNOSUPPORT: (
        "this host does not support that address family (is IPv6 disabled?) - use an "
        "address of another family in the config"
    ),
    errno.EMFILE: "too many open files - raise the limit with 'ulimit -n' or close other files",
    errno.ENFILE: "the system is out of file descriptors - close other programs, then retry",
    errno.ENOBUFS: "the system is out of socket buffer memory - free memory, then retry",
    errno.ENOMEM: "the system is out of memory - free memory, then retry",
    errno.EADDRINUSE: (
        "another listener (ib listen/respond, cFS) may own the port - stop it or change "
        "the port in the config"
    ),
    errno.EADDRNOTAVAIL: "that address is not on this host - fix the host in the config",
    errno.EACCES: "ports below 1024 need privileges - use a higher port in the config",
}


def read_proc_drops(inodes: Iterable[int]) -> dict[int, int]:
    """Kernel drop counters of the UDP sockets with these inodes, in one pass over the tables.

    Blocking file reads: call from a worker thread when the event loop is running. A socket
    that is not in the tables (already closed) is absent from the result.
    """
    wanted = {str(inode): inode for inode in inodes}
    found: dict[int, int] = {}
    for table in PROC_TABLES:
        if len(found) == len(wanted):
            break
        try:
            with open(table, encoding="ascii", errors="replace") as handle:
                next(handle, None)  # header
                for row in handle:
                    parts = row.split()
                    if len(parts) < PROC_MIN_COLUMNS:
                        continue
                    inode = wanted.get(parts[PROC_INODE_COLUMN])
                    if inode is not None:
                        with contextlib.suppress(ValueError):
                            found[inode] = int(parts[-1])
        except OSError:
            continue
    return found


async def scan_kernel_drops(transports: Collection[UdpTransport]) -> None:
    """Refresh the kernel drop counters of ``transports`` with one scan, off the event loop."""
    inodes = {t.inode: t for t in transports if t.inode is not None}
    if not inodes:
        return
    found = await asyncio.to_thread(read_proc_drops, tuple(inodes))
    for inode, transport in inodes.items():
        value = found.get(inode)
        if value is not None:
            transport.note_proc_drops(value)


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
        self._sock: socket.socket | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._reply_addr: tuple[Any, ...] | None = None
        self._buf = bytearray(MAX_DATAGRAM)
        self._view = memoryview(self._buf)
        self._dropped = 0
        self._ovfl_last = 0
        self._ovfl_total = 0
        self._proc_drops = 0
        self._ovfl_active = False
        self._rx_total = 0  # datagrams read from the socket
        self._rx_seen = 0  # ``_rx_total`` at the previous :meth:`kernel_scan_due`
        self._tail_unchecked = False
        self._inode: int | None = None
        self._pending_error: OSError | None = None
        self._closed = False
        self.rcvbuf_requested: int = 0
        self.rcvbuf_effective: int = 0

    @property
    def dropped(self) -> int:
        """Datagrams discarded in user space because the consumer fell behind."""
        return self._dropped

    @property
    def kernel_dropped(self) -> int:
        """Datagrams the kernel discarded because the socket buffer was full (cumulative)."""
        return max(self._ovfl_total, self._proc_drops)

    @property
    def overflow_counter_active(self) -> bool:
        """True when the kernel reports drops per datagram (``SO_RXQ_OVFL``)."""
        return self._ovfl_active

    @property
    def rcvbuf_below_target(self) -> bool:
        """The kernel granted less receive buffer than requested (``net.core.rmem_max``).

        Linux reports twice the usable size, which accounts for per-datagram overhead.
        """
        factor = 2 if sys.platform.startswith("linux") else 1
        return self.rcvbuf_effective < factor * self.rcvbuf_requested

    @property
    def local_address(self) -> tuple[str, int] | None:
        """Bound ``(host, port)`` once open; useful when binding to port 0 in tests."""
        if self._sock is None:
            return None
        name = self._sock.getsockname()
        return (str(name[0]), int(name[1])) if name else None

    @property
    def inode(self) -> int | None:
        """Socket inode (the key of its row in ``/proc/net/udp``), once open."""
        return self._inode

    def note_proc_drops(self, value: int) -> None:
        """Record a drop count read from ``/proc/net/udp`` (it only ever grows)."""
        self._proc_drops = max(self._proc_drops, value)

    def kernel_scan_due(self) -> bool:
        """Whether the periodic tick should read ``/proc/net/udp`` for this socket now.

        Without ``SO_RXQ_OVFL`` the table is the only source, so every tick. With it, drops ride
        on the datagrams that arrive; only the tail of a burst (datagrams dropped after the last
        one that got through) is invisible, so one scan is due on the first tick that finds the
        socket quiet after traffic. Call once per tick.
        """
        if not self._ovfl_active:
            return True
        busy = self._rx_total != self._rx_seen
        self._rx_seen = self._rx_total
        if busy:
            self._tail_unchecked = True
            return False
        due, self._tail_unchecked = self._tail_unchecked, False
        return due

    def refresh_kernel_drops(self) -> int:
        """Re-read the kernel's drop counter now (blocking); returns the cumulative count.

        The running session uses :func:`scan_kernel_drops` instead, which does not block the loop.
        """
        if self._inode is not None:
            found = read_proc_drops((self._inode,)).get(self._inode)
            if found is not None:
                self.note_proc_drops(found)
        return self.kernel_dropped

    # -- receive (event-loop thread) ----------------------------------------------------

    def _on_readable(self) -> None:
        sock = self._sock
        if sock is None:
            return
        queue = self._queue
        space = socket.CMSG_SPACE(4) if self._ovfl_active else 0
        for _ in range(BATCH):
            try:
                size, ancillary, _flags, addr = sock.recvmsg_into([self._buf], space)
            except (BlockingIOError, InterruptedError):
                return
            except OSError as exc:  # e.g. ICMP "connection refused" on a connected socket
                self._pending_error = exc
                return
            stamp = time.time_ns()
            self._rx_total += 1
            for level, kind, data in ancillary:
                if level == socket.SOL_SOCKET and kind == SO_RXQ_OVFL and len(data) >= 4:
                    counter = struct.unpack_from("=I", data)[0]
                    self._ovfl_total += (counter - self._ovfl_last) & _U32
                    self._ovfl_last = counter
            source = f"{addr[0]}:{addr[1]}" if addr else "?"
            try:
                queue.put_nowait(Datagram(bytes(self._view[:size]), stamp, source))
            except asyncio.QueueFull:
                self._dropped += 1

    def _enqueue_sentinel(self) -> None:
        if self._queue.full():
            self._queue.get_nowait()  # losing the oldest datagram beats never terminating
            self._dropped += 1
        self._queue.put_nowait(None)

    # -- Transport API ---------------------------------------------------------------

    @staticmethod
    def _open_error(what: str, endpoint: Endpoint, exc: BaseException) -> IronbirdError:
        reason = getattr(exc, "strerror", None) or str(exc)
        code = exc.errno if isinstance(exc, OSError) else None
        remedy = _OPEN_REMEDY.get(code, "") if code else ""
        suffix = f"; {remedy}" if remedy else ""
        return IronbirdError(
            Code.TX_OPEN, f"cannot {what} udp {endpoint}: {reason}{suffix}", cause=exc
        )

    async def open(self) -> None:
        assert self._sock is None, "transport already open"
        loop = asyncio.get_running_loop()
        self._loop = loop
        if self._reply_to is not None:
            try:
                reply_infos = await loop.getaddrinfo(
                    self._reply_to.host, self._reply_to.port, type=socket.SOCK_DGRAM
                )
                self._reply_addr = reply_infos[0][4]
            except (OSError, UnicodeError) as exc:
                raise self._open_error("resolve reply host", self._reply_to, exc) from exc
        endpoint = self._local or self._remote
        assert endpoint is not None
        try:
            infos = await loop.getaddrinfo(endpoint.host, endpoint.port, type=socket.SOCK_DGRAM)
        except (OSError, UnicodeError) as exc:
            raise self._open_error("resolve host of", endpoint, exc) from exc
        last: OSError = OSError("no usable address")
        step = "open a socket for"
        sock: socket.socket | None = None
        for family, kind, proto, _canon, address in infos:
            try:
                candidate = socket.socket(family, kind, proto)
            except OSError as exc:  # e.g. EAFNOSUPPORT for IPv6 on a host without it, EMFILE
                last, step = exc, "open a socket for"
                continue
            try:
                candidate.setblocking(False)
                if self._local is not None:
                    self._tune_listener(candidate)
                    step = "bind"
                    candidate.bind(address)
                else:
                    step = "connect to"
                    candidate.connect(address)
            except OSError as exc:
                candidate.close()
                last = exc
                continue
            sock = candidate
            break
        if sock is None:
            raise self._open_error(step, endpoint, last) from last
        try:
            self._inode = os.fstat(sock.fileno()).st_ino
            if self._local is not None:
                self.rcvbuf_effective = int(sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF))
            loop.add_reader(sock.fileno(), self._on_readable)
        except OSError as exc:
            sock.close()
            raise self._open_error("open a socket for", endpoint, exc) from exc
        self._sock = sock

    def _tune_listener(self, sock: socket.socket) -> None:
        """Ask for a big receive buffer and the kernel's drop counter (both best effort)."""
        self.rcvbuf_requested = RCVBUF_BYTES
        with contextlib.suppress(OSError):  # the kernel keeps its default
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RCVBUF_BYTES)
        if sys.platform.startswith("linux"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, SO_RXQ_OVFL, 1)
                self._ovfl_active = True
            except OSError:
                self._ovfl_active = False  # the /proc table still counts

    def send_nowait(self, data: bytes) -> None:
        """Hand one datagram to the kernel. Raises a coded error if that fails."""
        sock = self._sock
        if sock is None or self._closed:
            raise IronbirdError(Code.TX_SEND, "send on a closed transport")
        target = self._remote or self._reply_to
        assert target is not None, "transport has no destination"
        self._pending_error = None
        try:
            if self._remote is not None:
                sock.send(data)
            else:
                assert self._reply_addr is not None, "listener without a reply address"
                sock.sendto(data, self._reply_addr)
        except BlockingIOError as exc:
            raise IronbirdError(
                Code.TX_SEND, f"send to udp {target} failed: kernel send buffer full", cause=exc
            ) from exc
        except OSError as exc:
            raise self._send_error(target, exc) from exc

    def _raise_pending(self) -> None:
        target = self._remote or self._reply_to
        assert target is not None
        error = self._pending_error
        self._pending_error = None
        sock = self._sock
        if error is None and sock is not None and not self._closed:
            code = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if code:
                error = OSError(code, os.strerror(code))
        if error is not None:
            raise self._send_error(target, error) from error

    async def confirm(self) -> None:
        """Wait briefly for an ICMP error (e.g. connection refused) and raise it, coded."""
        await asyncio.sleep(CONFIRM_WAIT_S)
        self._raise_pending()

    async def send(self, data: bytes, *, confirm: bool = False) -> None:
        """Send one datagram; with ``confirm`` also wait for a refusal. Fire-and-forget else."""
        self.send_nowait(data)
        if confirm:
            await self.confirm()
        else:
            self._raise_pending()

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
        sock = self._sock
        try:
            if sock is not None:
                if self._loop is not None:
                    with contextlib.suppress(ValueError, OSError):
                        self._loop.remove_reader(sock.fileno())
                if self._local is not None:
                    # The socket's counter goes away with it; this also catches the tail of a
                    # burst that no datagram reported.
                    await scan_kernel_drops((self,))
        finally:
            if sock is not None:
                sock.close()
            self._enqueue_sentinel()
