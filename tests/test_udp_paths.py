"""UDP transport error and edge paths: unresolvable hosts, sends on closed or failing sockets,
a fatal receive error with no engine handler, send-only reply links, and the kernel drop scan.

Everything stays on loopback; hosts that must not resolve are malformed names (an over-long
DNS label), so no test depends on a resolver.
"""

from __future__ import annotations

import asyncio
import errno
import socket
from collections.abc import Awaitable
from pathlib import Path
from typing import Any

import pytest

from ironbird.config import Endpoint
from ironbird.errors import Code, IronbirdError
from ironbird.transport import udp
from ironbird.transport.base import TransportClosed
from ironbird.transport.udp import UdpTransport, read_proc_drops, scan_kernel_drops

LOOP = Endpoint("127.0.0.1", 0)
ROW = (
    "  0: 0100007F:1F90 00000000:0000 07 00000000:00000000 00:00000000 00000000"
    "  0  0 4242 2 0 {drops}"
)
BAD_HOST = Endpoint("a" * 70 + ".example", 9)  # a DNS label over 63 characters never resolves


def run(coro: Awaitable[Any]) -> Any:
    return asyncio.run(asyncio.wait_for(coro, 20))  # type: ignore[arg-type]


def sink() -> tuple[socket.socket, int]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    return sock, int(sock.getsockname()[1])


# -- open errors --


@pytest.mark.parametrize("role", ["local", "remote", "reply_to"])
def test_an_unresolvable_host_is_a_coded_open_error(role: str) -> None:
    async def main() -> None:
        match role:
            case "local":
                transport = UdpTransport(local=BAD_HOST)
                receiver: Any = lambda *_a: None  # noqa: E731
            case "remote":
                transport = UdpTransport(remote=BAD_HOST)
                receiver = None
            case _:
                transport = UdpTransport(local=LOOP, reply_to=BAD_HOST)
                receiver = None
        with pytest.raises(IronbirdError) as info:
            await transport.open(receiver)
        assert info.value.code is Code.TX_OPEN
        assert "udp" in info.value.summary
        await transport.close()

    run(main())


def test_a_listener_still_listens_when_its_command_endpoint_does_not_resolve() -> None:
    async def main() -> None:
        transport = UdpTransport(local=LOOP, remote=BAD_HOST)
        await transport.open(lambda *_a: None)
        try:
            assert transport.local_address is not None
            with pytest.raises(IronbirdError) as info:
                await transport.send(b"x")  # the lazy send socket cannot be made
            assert info.value.code is Code.TX_OPEN
        finally:
            await transport.close()

    run(main())


def test_a_bind_to_an_address_not_on_this_host_names_the_remedy() -> None:
    async def main() -> None:
        transport = UdpTransport(local=Endpoint("192.0.2.77", 40123))
        with pytest.raises(IronbirdError) as info:
            await transport.open(lambda *_a: None)
        assert info.value.code is Code.TX_OPEN
        assert "not on this host" in info.value.summary
        await transport.close()

    run(main())


def test_a_port_already_taken_names_the_remedy() -> None:
    async def main() -> None:
        taken, port = sink()
        try:
            transport = UdpTransport(local=Endpoint("127.0.0.1", port))
            with pytest.raises(IronbirdError) as info:
                await transport.open(lambda *_a: None)
            assert info.value.code is Code.TX_OPEN and "port" in info.value.summary
            await transport.close()
        finally:
            taken.close()

    run(main())


def test_the_listening_address_is_known_only_while_open() -> None:
    async def main() -> None:
        transport = UdpTransport(local=LOOP)
        assert transport.local_address is None
        await transport.open(lambda *_a: None)
        address = transport.local_address
        assert address is not None and address[0] == "127.0.0.1" and address[1] > 0
        await transport.close()

    run(main())


# -- sending --


def test_a_send_on_a_closed_transport_is_the_closed_signal_and_sends_nothing() -> None:
    async def main() -> None:
        target, port = sink()
        transport = UdpTransport(remote=Endpoint("127.0.0.1", port))
        await transport.open(None)
        await transport.close()
        with pytest.raises(TransportClosed):
            await transport.send(b"late")
        with pytest.raises(TransportClosed):
            transport.send_nowait(b"late")
        target.settimeout(0.2)
        with pytest.raises(TimeoutError):
            target.recv(10)
        assert transport.stats().tx_frames == 0
        target.close()
        await transport.close()  # closing twice is harmless

    run(main())


def test_a_reply_only_link_sends_from_a_bare_socket() -> None:
    async def main() -> None:
        target, port = sink()
        transport = UdpTransport(local=LOOP, reply_to=Endpoint("127.0.0.1", port))
        await transport.open(None)
        try:
            await transport.send(b"hello")
            target.settimeout(2)
            assert target.recv(100) == b"hello"
            assert transport.stats().tx_frames == 1
        finally:
            await transport.close()
            target.close()

    run(main())


class BrokenSend:
    """A send socket whose send fails with the given error (or reports a pending SO_ERROR)."""

    def __init__(self, real: socket.socket, error: BaseException | None, so_error: int = 0) -> None:
        self._real, self._error, self._so_error = real, error, so_error
        self.error_after_send = 0

    def send(self, data: bytes) -> int:
        if self._error is not None:
            raise self._error
        sent = self._real.send(data)
        self._so_error = self.error_after_send
        return sent

    def getsockopt(self, level: int, option: int, *args: Any) -> int:
        if option == socket.SO_ERROR and self._so_error:
            code, self._so_error = self._so_error, 0
            return code
        return int(self._real.getsockopt(level, option, *args))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


async def open_sender() -> tuple[UdpTransport, socket.socket]:
    target, port = sink()
    transport = UdpTransport(remote=Endpoint("127.0.0.1", port))
    await transport.open(None)
    assert transport._tx is not None
    return transport, target


def test_a_full_kernel_send_buffer_is_a_coded_send_error_not_a_crash() -> None:
    async def main() -> None:
        transport, target = await open_sender()
        try:
            transport._tx = BrokenSend(transport._tx, BlockingIOError(errno.EAGAIN, "full"))  # type: ignore[assignment]
            with pytest.raises(IronbirdError) as info:
                await transport.send(b"x")
            assert info.value.code is Code.TX_SEND and "buffer full" in info.value.summary
            assert transport.stats().tx_frames == 0
        finally:
            await transport.close()
            target.close()

    run(main())


@pytest.mark.parametrize(
    ("error", "fragment"),
    [
        (OSError(errno.ENETUNREACH, "Network is unreachable"), "unreachable"),
        (ConnectionRefusedError(errno.ECONNREFUSED, "refused"), "nothing is listening"),
        (OSError("no errno at all"), "failed"),
    ],
)
def test_a_failed_send_names_the_target(error: OSError, fragment: str) -> None:
    async def main() -> None:
        transport, target = await open_sender()
        try:
            transport._tx = BrokenSend(transport._tx, error)  # type: ignore[assignment]
            with pytest.raises(IronbirdError) as info:
                await transport.send(b"x")
            assert info.value.code is Code.TX_SEND
            assert fragment in info.value.summary and "udp 127.0.0.1" in info.value.summary
        finally:
            await transport.close()
            target.close()

    run(main())


def test_an_error_the_socket_reports_after_the_send_is_raised_from_so_error() -> None:
    async def main() -> None:
        transport, target = await open_sender()
        try:
            real = transport._tx
            assert real is not None
            # The check before the send sees nothing; the one after it sees ECONNREFUSED.
            wrapper = BrokenSend(real, None)
            wrapper.error_after_send = errno.ECONNREFUSED
            transport._tx = wrapper  # type: ignore[assignment]
            with pytest.raises(IronbirdError) as info:
                await transport.send(b"first")
            assert info.value.code is Code.TX_SEND and "nothing is listening" in info.value.summary
        finally:
            await transport.close()
            target.close()

    run(main())


def test_the_send_socket_drain_keeps_a_read_error_for_the_next_send() -> None:
    async def main() -> None:
        transport, target = await open_sender()
        real = transport._tx
        assert real is not None
        try:

            class Refusing:
                def recv_into(self, _buf: Any) -> int:
                    raise ConnectionRefusedError(errno.ECONNREFUSED, "refused")

                def __getattr__(self, name: str) -> Any:
                    return getattr(real, name)

            transport._tx = Refusing()  # type: ignore[assignment]
            transport._on_tx_readable()
            assert isinstance(transport._pending_error, ConnectionRefusedError)
            transport._tx = None
            transport._on_tx_readable()  # no socket: nothing to read, nothing raised
        finally:
            transport._tx = real
            await transport.close()
            target.close()

    run(main())


# -- fatal receive errors --


@pytest.mark.parametrize("handler", ["failed", "report", "none"])
def test_a_fatal_receive_error_stops_receiving_and_is_surfaced_once(handler: str) -> None:
    async def main() -> None:
        failed: list[IronbirdError] = []
        reported: list[IronbirdError] = []
        kwargs: dict[str, Any] = {}
        if handler == "failed":
            kwargs["failed"] = failed.append
        if handler in ("failed", "report"):
            kwargs["report"] = reported.append
        transport = UdpTransport(local=LOOP, **kwargs)
        await transport.open(lambda *_a: None)
        try:
            transport._receive_failed(OSError(errno.ENOTSOCK, "Socket operation on non-socket"))
            assert transport._rx_dead
            shown = failed if handler == "failed" else reported
            if handler == "none":
                assert not failed and not reported
            else:
                assert len(shown) == 1 and shown[0].code is Code.TX_RECV
                assert "no longer receive" in shown[0].summary
            assert transport.stats().rx_errors == 1
        finally:
            await transport.close()

    run(main())


def test_a_transient_receive_error_is_reported_but_keeps_receiving() -> None:
    async def main() -> None:
        reported: list[IronbirdError] = []
        clock = [0.0]
        transport = UdpTransport(local=LOOP, report=reported.append, clock=lambda: clock[0])
        await transport.open(lambda *_a: None)
        try:
            transport._receive_failed(OSError(errno.ENOBUFS, "No buffer space"))
            assert not transport._rx_dead
            assert len(reported) == 1 and reported[0].code is Code.TX_RECV
            # Failing continuously past the window turns the same error fatal.
            clock[0] += udp.RECV_FAIL_WINDOW_S - 0.5
            transport._receive_failed(OSError(errno.ENOBUFS, "No buffer space"))
            assert not transport._rx_dead
            clock[0] += 0.4
            transport._receive_failed(OSError(errno.ENOBUFS, "No buffer space"))
            clock[0] += 0.4
            transport._receive_failed(OSError(errno.ENOBUFS, "No buffer space"))
            assert transport._rx_dead
        finally:
            await transport.close()

    run(main())


# -- kernel drop scan --


def test_read_proc_drops_skips_short_rows_and_unreadable_tables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wide = ROW.format(drops="7")
    table = tmp_path / "udp"
    table.write_text("header\n  1: too short\n" + wide + "\n" + wide.replace("4242", "4243") + "\n")
    monkeypatch.setattr(udp, "PROC_TABLES", (str(tmp_path / "missing"), str(table)))
    assert read_proc_drops([4242]) == {4242: 7}
    assert read_proc_drops([99999]) == {}


def test_read_proc_drops_ignores_a_non_numeric_drop_column(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    row = ROW.format(drops="zz")
    table = tmp_path / "udp"
    table.write_text("header\n" + row + "\n")
    monkeypatch.setattr(udp, "PROC_TABLES", (str(table),))
    assert read_proc_drops([4242]) == {}


def test_scan_kernel_drops_with_nothing_open_is_a_no_op() -> None:
    async def main() -> None:
        await scan_kernel_drops([])
        await scan_kernel_drops([UdpTransport(local=LOOP)])  # not opened: no inode yet

    run(main())


def test_scan_kernel_drops_updates_an_open_transport() -> None:
    async def main() -> None:
        transport = UdpTransport(local=LOOP)
        await transport.open(lambda *_a: None)
        try:
            await scan_kernel_drops([transport])
            assert transport.kernel_dropped >= 0
        finally:
            await transport.close()

    run(main())


def test_empty_source_address_is_shown_as_unknown() -> None:
    transport = UdpTransport(local=LOOP)
    assert transport._source_text(None) == "?"
    assert transport._source_text(("127.0.0.1", 5)) == "127.0.0.1:5"
