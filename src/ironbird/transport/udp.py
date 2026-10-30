"""UDP transport on non-blocking sockets driven by the asyncio selector.

One transport is one link end. Up to two sockets, chosen by which endpoints are given:

* receive (``local``, when opened with a receiver): a bound, unconnected socket that accepts
  datagrams from any peer;
* send, to ``remote``: a connected socket, so ICMP "port unreachable" surfaces as a coded send
  error instead of vanishing;
* send, to ``reply_to``: ``sendto`` on the receive socket (or on a bare socket when the
  transport is send-only).

The receive path never blocks and never queues: each wakeup reads a batch of datagrams with
``recvmsg`` and hands each one to the receiver the transport was opened with. Datagrams the
*kernel* discarded because the socket buffer was full never reach user space, so they are
counted from the kernel itself: the ``SO_RXQ_OVFL`` ancillary counter on every received
datagram, and the ``drops`` column of ``/proc/net/udp`` (which also covers the tail of a flood,
after which no datagram arrives to carry the counter). The engine reports both as coalesced
errors.
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
from collections.abc import Callable, Collection, Iterable, Mapping
from types import MappingProxyType
from typing import Any, Final

from ironbird.config import Endpoint, format_hostport
from ironbird.errors import Code, IronbirdError
from ironbird.transport.base import NO_FLAGS, Receiver, TransportClosed, TransportStats

RCVBUF_BYTES: Final = 4 * 1024 * 1024
CONFIRM_WAIT_S: Final = 0.05
BATCH: Final = 256  # datagrams read per wakeup before yielding to the loop
SOURCE_CACHE_SIZE: Final = 64  # peers whose text form is remembered (bounded: peers are untrusted)
MAX_DATAGRAM: Final = 65536
UNREAD_COUNT_LIMIT: Final = 1 << 20  # datagrams counted at close before giving up
PROC_TABLES: Final = ("/proc/net/udp", "/proc/net/udp6")
PROC_INODE_COLUMN: Final = 9
PROC_MIN_COLUMNS: Final = 13  # the last one is "drops"
# Not exported by Python's socket module on every version; the value is Linux ABI.
SO_RXQ_OVFL: Final = getattr(socket, "SO_RXQ_OVFL", 40)
_U32: Final = 0xFFFFFFFF

_OPEN_REMEDY: Final[Mapping[int, str]] = MappingProxyType(
    {
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
)


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
    """One UDP link end (see the module docstring)."""

    def __init__(
        self,
        *,
        local: Endpoint | None = None,
        remote: Endpoint | None = None,
        reply_to: Endpoint | None = None,
        report: Callable[[IronbirdError], None] | None = None,
    ) -> None:
        assert local is not None or remote is not None, "need a local or remote endpoint"
        assert remote is None or reply_to is None, "connected sockets cannot also reply_to"
        self._local = local
        self._remote = remote
        self._reply_to = reply_to
        self._report = report
        self._receiver: Receiver | None = None
        self._rx: socket.socket | None = None
        self._tx: socket.socket | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._reply_addr: tuple[Any, ...] | None = None
        self._buf = bytearray(MAX_DATAGRAM)
        self._view = memoryview(self._buf)
        self._sources: dict[Any, str] = {}
        self._ovfl_last = 0
        self._ovfl_total = 0
        self._proc_drops = 0
        self._ovfl_active = False
        self._rx_total = 0  # datagrams read from the socket
        self._rx_bytes = 0
        self._rx_seen = 0  # ``_rx_total`` at the previous :meth:`kernel_scan_due`
        self._tx_total = 0
        self._tx_bytes = 0
        self._tail_unchecked = False
        self._unread_at_close = 0
        self._inode: int | None = None
        self._pending_error: OSError | None = None
        self._opened = False
        self._closed = False
        self._tx_lock = asyncio.Lock()
        self.rcvbuf_requested: int = 0
        self.rcvbuf_effective: int = 0

    @property
    def description(self) -> str:
        return f"udp {self._local or self._remote or self._reply_to}"

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
        """Bound ``(host, port)`` once receiving; useful when binding to port 0 in tests."""
        if self._rx is None:
            return None
        name = self._rx.getsockname()
        return (str(name[0]), int(name[1])) if name else None

    @property
    def inode(self) -> int | None:
        """Receive socket inode (the key of its row in ``/proc/net/udp``), once open."""
        return self._inode

    def stats(self) -> TransportStats:
        return TransportStats(
            rx_frames=self._rx_total,
            rx_bytes=self._rx_bytes,
            tx_frames=self._tx_total,
            tx_bytes=self._tx_bytes,
            kernel_dropped=self.kernel_dropped,
            unread_at_close=self._unread_at_close,
        )

    def facts(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "endpoint": str(self._local),
                "rcvbuf_requested": self.rcvbuf_requested,
                "rcvbuf_effective": self.rcvbuf_effective,
                "drop_counter": "SO_RXQ_OVFL" if self._ovfl_active else "/proc/net/udp",
            }
        )

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

    async def poll(self) -> None:
        """Refresh the kernel drop counter when due, off the event loop."""
        if self._rx is not None and not self._closed and self.kernel_scan_due():
            await scan_kernel_drops((self,))

    def refresh_kernel_drops(self) -> int:
        """Re-read the kernel's drop counter now (blocking); returns the cumulative count.

        The running engine uses :meth:`poll` instead, which does not block the loop.
        """
        if self._inode is not None:
            found = read_proc_drops((self._inode,)).get(self._inode)
            if found is not None:
                self.note_proc_drops(found)
        return self.kernel_dropped

    # -- receive (event-loop thread) ----------------------------------------------------

    def _source_text(self, addr: Any) -> str:
        if not addr:
            return "?"
        text = self._sources.get(addr)
        if text is None:
            if len(self._sources) >= SOURCE_CACHE_SIZE:
                self._sources.clear()
            text = format_hostport(addr[0], addr[1])
            self._sources[addr] = text
        return text

    def _on_readable(self) -> None:
        sock, receiver = self._rx, self._receiver
        if sock is None or receiver is None:
            return
        buf, view = self._buf, self._view
        space = socket.CMSG_SPACE(4) if self._ovfl_active else 0
        for _ in range(BATCH):
            try:
                size, ancillary, _flags, addr = sock.recvmsg_into([buf], space)
            except (BlockingIOError, InterruptedError):
                return
            except OSError as exc:  # e.g. ICMP "connection refused" on a connected socket
                self._pending_error = exc
                return
            stamp = time.time_ns()
            self._rx_total += 1
            self._rx_bytes += size
            for level, kind, data in ancillary:
                if level == socket.SOL_SOCKET and kind == SO_RXQ_OVFL and len(data) >= 4:
                    counter = struct.unpack_from("=I", data)[0]
                    self._ovfl_total += (counter - self._ovfl_last) & _U32
                    self._ovfl_last = counter
            receiver(bytes(view[:size]), stamp, self._source_text(addr), size, NO_FLAGS)

    def _on_tx_readable(self) -> None:
        """The connected send socket is never read for data; reading it surfaces ICMP errors."""
        sock = self._tx
        if sock is None:
            return
        for _ in range(BATCH):
            try:
                sock.recv(MAX_DATAGRAM)
            except (BlockingIOError, InterruptedError):
                return
            except OSError as exc:
                self._pending_error = exc
                return

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

    async def _attach(
        self, loop: asyncio.AbstractEventLoop, endpoint: Endpoint, *, bind: bool
    ) -> socket.socket:
        """A non-blocking socket bound to (``bind``) or connected to ``endpoint``."""
        try:
            infos = await loop.getaddrinfo(endpoint.host, endpoint.port, type=socket.SOCK_DGRAM)
        except (OSError, UnicodeError) as exc:
            raise self._open_error("resolve host of", endpoint, exc) from exc
        # A socket() failure (this family is unsupported here) says less than a bind/connect
        # failure on an address that exists, so the latter is the error to report.
        socket_failure: OSError | None = None
        attach_failure: tuple[str, OSError] | None = None
        for family, kind, proto, _canon, address in infos:
            try:
                candidate = socket.socket(family, kind, proto)
            except OSError as exc:  # e.g. EAFNOSUPPORT for IPv6 on a host without it, EMFILE
                socket_failure = socket_failure or exc
                continue
            step = "bind" if bind else "connect to"
            try:
                candidate.setblocking(False)
                if bind:
                    self._tune_listener(candidate)
                    candidate.bind(address)
                else:
                    candidate.connect(address)
            except OSError as exc:
                candidate.close()
                attach_failure = attach_failure or (step, exc)
                continue
            return candidate
        if attach_failure is not None:
            step, last = attach_failure
        else:
            step, last = "open a socket for", socket_failure or OSError("no usable address")
        raise self._open_error(step, endpoint, last) from last

    async def open(self, receiver: Receiver | None) -> None:
        assert not self._opened and not self._closed, "transport already opened"
        assert receiver is None or self._local is not None, "receiving needs a local endpoint"
        assert receiver is not None or self._remote or self._reply_to, "send-only needs a target"
        self._opened = True
        loop = asyncio.get_running_loop()
        self._loop = loop
        try:
            reply_family: socket.AddressFamily | None = None
            if self._reply_to is not None:
                try:
                    reply_infos = await loop.getaddrinfo(
                        self._reply_to.host, self._reply_to.port, type=socket.SOCK_DGRAM
                    )
                    reply_family, self._reply_addr = reply_infos[0][0], reply_infos[0][4]
                except (OSError, UnicodeError) as exc:
                    raise self._open_error("resolve reply host", self._reply_to, exc) from exc
            if receiver is not None:
                assert self._local is not None
                sock = await self._attach(loop, self._local, bind=True)
                self._rx = sock
                try:
                    self._inode = os.fstat(sock.fileno()).st_ino
                    self.rcvbuf_effective = int(
                        sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
                    )
                    loop.add_reader(sock.fileno(), self._on_readable)
                except OSError as exc:
                    raise self._open_error("open a socket for", self._local, exc) from exc
                self._receiver = receiver
                if self.rcvbuf_below_target and self._report is not None:
                    self._report(
                        IronbirdError(
                            Code.TX_RCVBUF,
                            f"{self.description}: kernel receive buffer is "
                            f"{self.rcvbuf_effective} bytes, below the {self.rcvbuf_requested} "
                            "requested; bursts may be lost - raise net.core.rmem_max (sysctl)",
                        )
                    )
            if self._remote is not None:
                if receiver is None:
                    await self._open_tx()
                # else: the connected socket is opened by the first send; a listener whose
                # command endpoint does not resolve can still listen.
            elif self._reply_to is not None:
                if self._rx is not None:
                    self._tx = self._rx
                else:
                    assert reply_family is not None
                    try:
                        bare = socket.socket(reply_family, socket.SOCK_DGRAM)
                        bare.setblocking(False)
                    except OSError as exc:
                        raise self._open_error("open a socket for", self._reply_to, exc) from exc
                    self._tx = bare
        except BaseException:
            self._release()
            raise

    async def _open_tx(self) -> None:
        """Open the connected send socket (to ``remote``)."""
        loop, remote = self._loop, self._remote
        assert loop is not None and remote is not None and self._tx is None
        tx = await self._attach(loop, remote, bind=False)
        try:
            loop.add_reader(tx.fileno(), self._on_tx_readable)
        except OSError as exc:
            tx.close()
            raise self._open_error("open a socket for", remote, exc) from exc
        if self._closed:  # closed while resolving
            with contextlib.suppress(ValueError, OSError):
                loop.remove_reader(tx.fileno())
            tx.close()
            raise TransportClosed
        self._tx = tx

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
        sock = self._tx
        if sock is None or self._closed:
            raise TransportClosed
        target = self._remote or self._reply_to
        assert target is not None, "transport has no destination"
        self._pending_error = None
        try:
            if self._remote is not None:
                # An ICMP error left by an earlier send must not be blamed on this one.
                sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                sock.send(data)
            else:
                assert self._reply_addr is not None, "reply_to without a resolved address"
                sock.sendto(data, self._reply_addr)
        except BlockingIOError as exc:
            raise IronbirdError(
                Code.TX_SEND, f"send to udp {target} failed: kernel send buffer full", cause=exc
            ) from exc
        except OSError as exc:
            raise self._send_error(target, exc) from exc
        self._tx_total += 1
        self._tx_bytes += len(data)

    def _raise_pending(self) -> None:
        target = self._remote or self._reply_to
        assert target is not None
        error = self._pending_error
        self._pending_error = None
        sock = self._tx
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

    async def send(
        self,
        data: bytes,
        *,
        confirm: bool = False,
        handed_off: Callable[[], None] | None = None,
    ) -> None:
        """Send one datagram; with ``confirm`` also wait for a refusal. Fire-and-forget else."""
        if self._tx is None and self._remote is not None and not self._closed:
            async with self._tx_lock:
                if self._tx is None:
                    await self._open_tx()
        self.send_nowait(data)
        if handed_off is not None:
            handed_off()
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

    def _release(self) -> None:
        """Close the sockets (detaching their readers) without any final accounting."""
        loop, rx, tx = self._loop, self._rx, self._tx
        self._closed = True
        self._receiver = None
        self._rx = self._tx = None
        for sock in (rx, tx) if tx is not rx else (rx,):
            if sock is None:
                continue
            if loop is not None:
                with contextlib.suppress(ValueError, OSError):
                    loop.remove_reader(sock.fileno())
            sock.close()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        rx, loop = self._rx, self._loop
        try:
            if rx is not None:
                self._receiver = None  # nothing more is delivered
                if loop is not None:
                    with contextlib.suppress(ValueError, OSError):
                        loop.remove_reader(rx.fileno())
                # The socket's counter goes away with it; this also catches the tail of a
                # burst that no datagram reported.
                await scan_kernel_drops((self,))
                self._unread_at_close = self._count_unread(rx)
        finally:
            self._release()

    def _count_unread(self, sock: socket.socket) -> int:
        """Datagrams waiting in the receive buffer, read and discarded so they can be counted."""
        count = 0
        while count < UNREAD_COUNT_LIMIT:
            try:
                sock.recvmsg_into([self._buf])
            except (BlockingIOError, InterruptedError):
                break
            except OSError:
                break
            count += 1
        return count
