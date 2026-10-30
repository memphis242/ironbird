"""Stage 2c: byte-bounded memory and receive-side errors.

Covers the edge cap of the UDP transport (``MSG_TRUNC``, no whole-datagram copy), the consumer
queue's byte bound (including a batch the consumer holds), the ledger's truncation at record
creation and its byte bound, receive errors (``IB-TX-003``), consumer lag (``IB-ENG-001``), and
an end-to-end flood of maximum-size datagrams against a stalled consumer.

Malformed-input coverage: the corpus datagrams and arbitrary sizes go through the real UDP edge.
"""

from __future__ import annotations

import asyncio
import errno
import json
import os
import socket
import subprocess
import sys
from collections import deque
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any, TypeVar

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import CORPUS
from ironbird.ccdd.model import MessageKind
from ironbird.config import Endpoint, load_config
from ironbird.engine import (
    _RELEASE_EVERY,
    CONSUMER_QUEUE_BYTES,
    FRAME_OVERHEAD_BYTES,
    Capture,
    _FrameQueue,
)
from ironbird.errors import Code, IronbirdError
from ironbird.frame import MAX_FRAME_BYTES, Direction, Frame, FrameFlag, Stream
from ironbird.ledger import ENTRY_OVERHEAD_BYTES, Ledger
from ironbird.notice import Notice
from ironbird.session import Session
from ironbird.transport import LinkEnd, UdpTransport, udp
from ironbird.transport.udp import RECV_FAIL_WINDOW_S

T = TypeVar("T")
TRUNCATED_BIT = int(FrameFlag.TRUNCATED)
FUZZ = settings(max_examples=200, deadline=2000, suppress_health_check=[HealthCheck.too_slow])


def _run(coro: Coroutine[Any, Any, T], timeout: float = 60) -> T:
    async def bounded() -> T:
        async with asyncio.timeout(timeout):
            return await coro

    return asyncio.run(bounded())


async def _until(predicate: Callable[[], bool], timeout: float = 10.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


# -- the UDP edge: MSG_TRUNC, nothing but the first max_packet_bytes is copied --------------

Received = tuple[bytes, int, int]  # data, orig_len, flags


async def _open_edge(cap: int) -> tuple[UdpTransport, list[Received], tuple[str, int]]:
    got: list[Received] = []

    def receiver(data: bytes, _time_ns: int, _source: str, orig_len: int, flags: int) -> None:
        got.append((data, orig_len, flags))

    transport = UdpTransport(local=Endpoint("127.0.0.1", 0), max_packet_bytes=cap)
    await transport.open(receiver)
    address = transport.local_address
    assert address is not None
    return transport, got, address


def _send(address: tuple[str, int], *datagrams: bytes) -> None:
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for datagram in datagrams:
            sender.sendto(datagram, address)
    finally:
        sender.close()


@pytest.mark.parametrize("size", [0, 1, 63, 64, 65, 2047, 2048, 2049, 4096, 60000, 65507])
def test_the_edge_cuts_a_datagram_at_the_cap_and_reports_its_size_on_the_wire(size: int) -> None:
    cap = 2048
    datagram = bytes(range(256)) * (size // 256) + bytes(range(size % 256))
    assert len(datagram) == size

    async def main() -> None:
        transport, got, address = await _open_edge(cap)
        try:
            assert len(transport._buf) == cap  # the read buffer, hence the copy, is the cap
            _send(address, datagram)
            await _until(lambda: len(got) == 1)
            data, orig_len, flags = got[0]
            assert data == datagram[:cap] and orig_len == size
            assert bool(flags & TRUNCATED_BIT) == (size > cap)
            assert transport.stats().rx_bytes == size  # the wire size, not the kept size
        finally:
            await transport.close()

    _run(main())


def test_a_cut_datagram_does_not_leak_into_the_next_one() -> None:
    """The read buffer is reused; a short datagram after a long one is not padded with its tail."""

    async def main() -> None:
        transport, got, address = await _open_edge(64)
        try:
            _send(address, b"\xaa" * 500, b"\x01\x02\x03")
            await _until(lambda: len(got) == 2)
            assert got[0] == (b"\xaa" * 64, 500, TRUNCATED_BIT)
            assert got[1] == (b"\x01\x02\x03", 3, 0)
        finally:
            await transport.close()

    _run(main())


@pytest.mark.parametrize("cap", [64, 2048])
def test_corpus_datagrams_go_through_the_edge_intact_or_cut(cap: int) -> None:
    corpus = sorted(CORPUS.glob("packet-*.bin"))
    assert any(len(p.read_bytes()) > cap for p in corpus) or cap != 64

    async def main() -> None:
        transport, got, address = await _open_edge(cap)
        try:
            for path in corpus:
                _send(address, path.read_bytes())
            await _until(lambda: len(got) == len(corpus))
            for path, (data, orig_len, flags) in zip(corpus, got, strict=True):
                wire = path.read_bytes()
                assert data == wire[:cap] and orig_len == len(wire), path.name
                assert bool(flags & TRUNCATED_BIT) == (len(wire) > cap), path.name
        finally:
            await transport.close()

    _run(main())


@FUZZ
@given(sizes=st.lists(st.integers(0, 6000), min_size=1, max_size=8), cap=st.integers(1, 3000))
def test_edge_invariants_hold_for_arbitrary_datagram_sizes(sizes: list[int], cap: int) -> None:
    async def main() -> None:
        transport, got, address = await _open_edge(cap)
        try:
            datagrams = [bytes([i % 251]) * size for i, size in enumerate(sizes)]
            _send(address, *datagrams)
            await _until(lambda: len(got) == len(datagrams))
            for datagram, (data, orig_len, flags) in zip(datagrams, got, strict=True):
                assert len(data) <= cap and data == datagram[:cap]
                assert orig_len == len(datagram) and orig_len >= len(data)
                assert bool(flags & TRUNCATED_BIT) == (orig_len > len(data))
        finally:
            await transport.close()

    _run(main())


def test_the_send_socket_reads_a_peers_datagrams_without_keeping_them() -> None:
    """A peer can send to the ephemeral port of the connected send socket; it is drained, and
    only an ICMP error is kept."""

    async def main() -> None:
        sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sink.bind(("127.0.0.1", 0))
        port = sink.getsockname()[1]
        transport = UdpTransport(remote=Endpoint("127.0.0.1", port))
        try:
            await transport.open(None)
            tx = transport._tx
            assert tx is not None
            peer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            peer.sendto(b"x" * 60000, tx.getsockname())
            peer.close()
            transport._on_tx_readable()
            with pytest.raises(BlockingIOError):
                tx.recv(1)  # the datagram was consumed by the drain
            assert transport._pending_error is None
        finally:
            await transport.close()
            sink.close()

    _run(main())


# -- receive errors: IB-TX-003 ----------------------------------------------------------


class FlakySocket:
    """Fails its next reads with the given errors, then reads the real socket."""

    def __init__(self, real: socket.socket, errors: list[OSError]) -> None:
        self._real = real
        self.errors = errors

    def recvmsg_into(self, *args: Any) -> Any:
        if self.errors:
            raise self.errors.pop(0)
        return self._real.recvmsg_into(*args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


def _flaky(transport: UdpTransport, errors: list[OSError]) -> FlakySocket:
    assert transport._rx is not None
    flaky = FlakySocket(transport._rx, errors)
    transport._rx = flaky  # type: ignore[assignment]
    return flaky


def test_a_failed_read_is_counted_reported_and_receiving_continues() -> None:
    reports: list[IronbirdError] = []
    failures: list[IronbirdError] = []

    async def main() -> None:
        got: list[Received] = []
        transport = UdpTransport(
            local=Endpoint("127.0.0.1", 0),
            report=reports.append,
            failed=failures.append,
        )
        await transport.open(lambda data, _t, _s, orig, flags: got.append((data, orig, flags)))
        address = transport.local_address
        assert address is not None
        _flaky(transport, [OSError(errno.ENOMEM, "Cannot allocate memory")] * 3)
        try:
            _send(address, b"after")
            await _until(lambda: len(got) == 1)
            assert got[0][0] == b"after"
            assert transport.stats().rx_errors == 3
        finally:
            await transport.close()

    _run(main())
    assert [e.code for e in reports] == [Code.TX_RECV] * 3 and failures == []
    assert "ENOMEM" in reports[0].summary and "3 read error" in reports[-1].summary


@pytest.mark.parametrize("number", [errno.EBADF, errno.ENOTSOCK, errno.EINVAL])
def test_an_unusable_socket_fails_the_transport_at_once(number: int) -> None:
    reports: list[IronbirdError] = []
    failures: list[IronbirdError] = []

    async def main() -> None:
        transport = UdpTransport(
            local=Endpoint("127.0.0.1", 0), report=reports.append, failed=failures.append
        )
        await transport.open(lambda *_: None)
        address = transport.local_address
        assert address is not None
        _flaky(transport, [OSError(number, os.strerror(number))])
        try:
            _send(address, b"x")
            await _until(lambda: bool(failures))
            assert transport.stats().rx_errors == 1
            _send(address, b"y")
            await asyncio.sleep(0.05)
            assert transport.stats().rx_errors == 1  # the reader is gone; nothing spins
        finally:
            await transport.close()

    _run(main())
    assert reports == [] and len(failures) == 1 and failures[0].code is Code.TX_RECV
    assert "restart" in failures[0].summary


class ReceiveClock:
    """A clock the receive path reads once per failed read: each failure sees ``times`` in turn
    (the last one repeats), so a test fixes exactly when each failure happens."""

    def __init__(self, times: list[float]) -> None:
        self._times = times
        self.reads = 0

    def __call__(self) -> float:
        value = self._times[min(self.reads, len(self._times) - 1)]
        self.reads += 1
        return value


async def _failing_transport(
    clock: ReceiveClock,
    reports: list[IronbirdError],
    failures: list[IronbirdError],
    got: list[Received],
) -> tuple[UdpTransport, tuple[str, int]]:
    transport = UdpTransport(
        local=Endpoint("127.0.0.1", 0),
        report=reports.append,
        failed=failures.append,
        clock=clock,
    )
    await transport.open(lambda data, _t, _s, orig, flags: got.append((data, orig, flags)))
    address = transport.local_address
    assert address is not None
    return transport, address


def test_reads_still_failing_after_the_window_end_the_transport() -> None:
    reports: list[IronbirdError] = []
    failures: list[IronbirdError] = []
    step = 0.2
    last = round(RECV_FAIL_WINDOW_S / step)  # the failure that finds the window elapsed
    clock = ReceiveClock([step * n for n in range(2 * last)])

    async def main() -> None:
        transport, address = await _failing_transport(clock, reports, failures, [])
        _flaky(transport, [OSError(errno.EIO, "I/O error")] * (3 * last))
        try:
            _send(address, b"x")
            await _until(lambda: bool(failures))
            assert transport.stats().rx_errors == last + 1
        finally:
            await transport.close()

    _run(main())
    assert len(reports) == last and len(failures) == 1
    assert "restart" in failures[0].summary


def test_a_burst_of_failures_inside_the_window_never_ends_the_transport() -> None:
    reports: list[IronbirdError] = []
    failures: list[IronbirdError] = []
    burst = 3000  # far more than any count-only rule would tolerate, all within 3 s
    clock = ReceiveClock([n * 0.001 for n in range(burst)])

    async def main() -> None:
        got: list[Received] = []
        transport, address = await _failing_transport(clock, reports, failures, got)
        _flaky(transport, [OSError(errno.EIO, "I/O error")] * burst)
        try:
            _send(address, b"after")
            await _until(lambda: len(got) == 1)
            assert transport.stats().rx_errors == burst
        finally:
            await transport.close()

    _run(main())
    assert len(reports) == burst and failures == []


def test_a_flood_of_failures_ends_the_transport_at_the_count_backstop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backstop = 500
    monkeypatch.setattr(udp, "RECV_FAIL_BACKSTOP", backstop)
    reports: list[IronbirdError] = []
    failures: list[IronbirdError] = []
    clock = ReceiveClock([0.0])  # the clock never moves: only the count can end it

    async def main() -> None:
        transport, address = await _failing_transport(clock, reports, failures, [])
        _flaky(transport, [OSError(errno.EIO, "I/O error")] * (2 * backstop))
        try:
            _send(address, b"x")
            await _until(lambda: bool(failures))
            assert transport.stats().rx_errors == backstop
        finally:
            await transport.close()

    _run(main())
    assert len(reports) == backstop - 1 and len(failures) == 1


def test_a_successful_read_resets_the_failing_window() -> None:
    failures: list[IronbirdError] = []
    window = RECV_FAIL_WINDOW_S
    # Each round fails at its start and 0.8 of a window later, then a good read arrives. Rounds
    # begin 0.9 of a window apart, so without the reset the third round would be 2+ windows old.
    times: list[float] = []
    for round_ in range(4):
        base = round_ * 0.9 * window
        times += [base, base + 0.8 * window]
    clock = ReceiveClock(times)

    async def main() -> None:
        got: list[Received] = []
        transport, address = await _failing_transport(clock, [], failures, got)
        flaky = _flaky(transport, [])
        try:
            for round_ in range(4):
                flaky.errors.extend([OSError(errno.EIO, "I/O error")] * 2)
                _send(address, b"ok")
                await _until(lambda expected=round_ + 1: len(got) == expected)
        finally:
            await transport.close()

    _run(main())
    assert failures == [] and clock.reads == len(times)


def test_a_successful_read_resets_the_failure_count_toward_the_backstop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(udp, "RECV_FAIL_BACKSTOP", 10)
    failures: list[IronbirdError] = []
    clock = ReceiveClock([0.0])

    async def main() -> None:
        got: list[Received] = []
        transport, address = await _failing_transport(clock, [], failures, got)
        flaky = _flaky(transport, [])
        try:
            for round_ in range(4):  # 4 x 6 failures in all, but never 10 in a row
                flaky.errors.extend([OSError(errno.EIO, "I/O error")] * 6)
                _send(address, b"ok")
                await _until(lambda expected=round_ + 1: len(got) == expected)
        finally:
            await transport.close()

    _run(main())
    assert failures == []


def test_failures_further_apart_than_the_window_are_separate_episodes() -> None:
    failures: list[IronbirdError] = []
    # Nothing is read in between (so no good read resets anything), but each failure comes more
    # than a window after the one before: the condition cleared in the meantime.
    clock = ReceiveClock([n * 1.5 * RECV_FAIL_WINDOW_S for n in range(6)])

    async def main() -> None:
        transport, _address = await _failing_transport(clock, [], failures, [])
        try:
            for _ in range(6):
                transport._receive_failed(OSError(errno.EIO, "I/O error"))
        finally:
            await transport.close()

    _run(main())
    assert failures == [] and clock.reads == 6


# -- through the engine: sessions on the mock config ------------------------------------------


def _ground(
    config_path: Path,
    tmp_path: Path,
    notices: list[Notice],
    text: str | None = None,
    *,
    ledger: bool = True,
) -> Session:
    if text is not None:
        config_path.write_text(text)
    return Session(
        load_config(config_path),
        role="ground",
        ledger=Ledger(tmp_path / "ledger", session_id="2c", enabled=ledger),
        notify=notices.append,
    )


def _source_io(ground: Session) -> Any:
    link = ground.route("fc").link
    return ground._engine._ios[(link.name, LinkEnd.SOURCE)]


def test_a_failed_read_is_reported_once_and_the_listener_keeps_listening(
    mock_config_path: Path, tmp_path: Path
) -> None:
    notices: list[Notice] = []

    async def main() -> None:
        async with _ground(mock_config_path, tmp_path, notices) as ground:
            hk = ground.codec_for("fc").encode_telemetry("MOCK_HK").data
            listener = await ground.listen("fc")
            address = listener.local_address
            assert address is not None
            _flaky(_source_io(ground).transport, [OSError(errno.ENOMEM, "no memory")] * 5)
            _send(address, hk)
            iterator = aiter(listener)
            rx = await anext(iterator)
            assert rx.packet.message.name == "MOCK_HK"
            await listener.close()
            ground.flush_notices()

    _run(main())
    recv = [n for n in notices if n.code is Code.TX_RECV]
    assert len(recv) == 2, recv  # the first occurrence, then one summary of the other four
    assert recv[1].count == 5 and "5 read error" in recv[1].summary


def test_a_dead_socket_ends_the_capture_with_the_error_after_what_it_received(
    mock_config_path: Path, tmp_path: Path
) -> None:
    notices: list[Notice] = []

    async def main() -> None:
        async with _ground(mock_config_path, tmp_path, notices) as ground:
            hk = ground.codec_for("fc").encode_telemetry("MOCK_HK").data
            listener = await ground.listen("fc")
            address = listener.local_address
            assert address is not None
            io = _source_io(ground)
            _send(address, hk, hk)
            await _until(lambda: listener.capture.stats().frames == 2)
            _flaky(io.transport, [OSError(errno.EBADF, "Bad file descriptor")])
            _send(address, hk)
            seen = 0
            with pytest.raises(IronbirdError) as info:
                async for _rx in listener:
                    seen += 1
            assert seen == 2  # everything received before the failure was delivered first
            assert info.value.code is Code.TX_RECV and "EBADF" in info.value.summary
            assert io.failure is info.value
            with pytest.raises(IronbirdError) as again:  # a new capture on the dead end is refused
                await ground.listen("fc")
            assert again.value.code is Code.TX_RECV
            await listener.close()
            # closing the end replaces the transport: listening works again
            fresh = await ground.listen("fc")
            await fresh.close()
            ledger = ground.ledger
            ledger.flush()
            events = [json.loads(line)["event"] for line in ledger.path.read_text().splitlines()]
            assert "rx.failed" in events

    _run(main())


def test_a_fatal_failure_is_the_last_word_no_stale_coalesced_summary_follows_it(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    failures_before_fatal = 150
    monkeypatch.setattr(udp, "RECV_FAIL_BACKSTOP", failures_before_fatal)
    notices: list[Notice] = []

    async def main() -> None:
        async with _ground(mock_config_path, tmp_path, notices) as ground:
            listener = await ground.listen("fc")
            address = listener.local_address
            assert address is not None
            errors = [OSError(errno.ENOMEM, "no memory")] * failures_before_fatal
            _flaky(_source_io(ground).transport, errors)
            _send(address, b"x")
            with pytest.raises(IronbirdError) as info:
                async for _rx in listener:
                    pass
            assert info.value.code is Code.TX_RECV
            await listener.close()
            ground.flush_notices()  # what shutdown does after the error is printed

    _run(main())
    recv = [n for n in notices if n.code is Code.TX_RECV]
    # The first occurrence was shown; the fatal error (raised, not noticed) is the only thing
    # after it, so no "(x149 in ...)" summary of the failures it ends may follow.
    assert len(recv) == 1 and recv[0].count == 1, recv


def test_frames_a_failed_captures_other_link_end_still_receives_are_not_counted_as_lag(
    mock_config_path: Path, tmp_path: Path
) -> None:
    notices: list[Notice] = []

    async def main() -> None:
        async with _ground(mock_config_path, tmp_path, notices) as ground:
            engine = ground._engine
            link = ground.route("fc").link
            capture = await engine.open_capture(
                [(link, LinkEnd.SOURCE), (link, LinkEnd.DESTINATION)]
            )
            source = engine._ios[(link.name, LinkEnd.SOURCE)]
            other = engine._ios[(link.name, LinkEnd.DESTINATION)]
            assert isinstance(source.transport, UdpTransport)
            assert isinstance(other.transport, UdpTransport)
            source_address = source.transport.local_address
            other_address = other.transport.local_address
            assert source_address is not None and other_address is not None
            _flaky(source.transport, [OSError(errno.EBADF, "Bad file descriptor")])
            _send(source_address, b"x")
            await _until(lambda: source.failure is not None)
            assert capture._queue.closed  # one queue serves both link ends: it ended with the first
            before = engine.last_seq
            _send(other_address, b"y", b"z")  # the other end keeps receiving into the closed queue
            await _until(lambda: engine.last_seq == before + 2)
            await engine.tick()
            ground.flush_notices()
            assert capture.stats().not_shown == 0 and not other.lossy
            assert all(sub.dropped == 0 for sub in other.subs)
            await capture.close()

    _run(main())
    assert not [n for n in notices if n.code is Code.ENG_LAG], notices


def test_frames_left_unread_at_close_are_not_a_consumer_lag_when_only_the_kernel_dropped(
    mock_config_path: Path, tmp_path: Path
) -> None:
    notices: list[Notice] = []

    async def main() -> None:
        async with _ground(mock_config_path, tmp_path, notices) as ground:
            hk = ground.codec_for("fc").encode_telemetry("MOCK_HK").data
            listener = await ground.listen("fc")
            address = listener.local_address
            assert address is not None
            _send(address, hk, hk, hk)
            await _until(lambda: listener.capture.stats().frames == 3)
            _source_io(ground).kernel.seen = 5  # the kernel dropped datagrams; the queue did not
            await listener.close()
            assert listener.capture.stats().unread == 3 and listener.capture.stats().not_shown == 0

    _run(main())
    assert not [n for n in notices if n.code is Code.ENG_LAG], notices


def test_the_events_stream_surfaces_a_failed_link_end(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> None:
        async with _ground(mock_config_path, tmp_path, []) as ground:
            stream = await ground.events()
            address = stream.endpoints[0][1]
            assert address is not None
            _flaky(_source_io(ground).transport, [OSError(errno.ENOTSOCK, "Not a socket")])
            _send(address, b"x")
            with pytest.raises(IronbirdError) as info:
                async for _event in stream:
                    pass
            assert info.value.code is Code.TX_RECV
            await stream.close()

    _run(main())


CLI_WITH_DEAD_SOCKET = """
import errno, socket, sys, threading, time
from ironbird.transport import udp
port = int(sys.argv[2])
class Dead:
    armed = False
    def __init__(self, real):
        self._real = real
    def recvmsg_into(self, *args):
        if self.armed:
            raise OSError(errno.EBADF, "Bad file descriptor")
        return self._real.recvmsg_into(*args)
    def __getattr__(self, name):
        return getattr(self._real, name)
original = udp.UdpTransport.open
async def patched(self, receiver):
    await original(self, receiver)
    if self._rx is not None:
        dead = self._rx = Dead(self._rx)
        def later():
            time.sleep(0.3)
            dead.armed = True
            socket.socket(socket.AF_INET, socket.SOCK_DGRAM).sendto(b"x", ("127.0.0.1", port))
        threading.Thread(target=later, daemon=True).start()
udp.UdpTransport.open = patched
from ironbird.cli.main import main
sys.argv = ["ib", "-c", sys.argv[1], "listen", "fc", "--timeout", "30"]
sys.exit(main())
"""


def test_the_cli_exits_with_the_coded_error_when_the_receive_socket_dies(
    mock_config_path: Path, tmp_path: Path
) -> None:
    port = load_config(mock_config_path).links[0].telemetry_endpoint.port
    done = subprocess.run(
        [sys.executable, "-c", CLI_WITH_DEAD_SOCKET, str(mock_config_path), str(port)],
        capture_output=True,
        check=False,
        text=True,
        timeout=60,
        env={**os.environ, "IRONBIRD_STATE_DIR": str(tmp_path / "state")},
    )
    assert done.returncode == 1, done.stderr
    errors = [line for line in done.stderr.splitlines() if line.startswith("error ")]
    assert len(errors) == 1 and "IB-TX-003" in errors[0] and "EBADF" in errors[0], done.stderr


# -- the consumer queue: bytes, held batches -------------------------------------------------

STREAM = Stream(1, "l", "fc", MessageKind.TELEMETRY, Direction.RX, "udp", "udp 0.0.0.0:1")


def _frame(size: int, seq: int = 1) -> Frame:
    return Frame(seq, 1, STREAM, "s", None, FrameFlag.NONE, size, bytes(size))


def _charge(size: int) -> int:
    return size + FRAME_OVERHEAD_BYTES


def test_the_queue_refuses_the_newest_frame_once_the_byte_bound_would_be_passed() -> None:
    queue = _FrameQueue(1000, 10 * _charge(100) + 5)
    accepted = [queue.put(_frame(100, seq)) for seq in range(1, 21)]
    assert accepted == [True] * 10 + [False] * 10
    assert queue.dropped == 10 and queue.dropped_bytes == 1000 and queue.offered == 20
    assert queue.peak_bytes == queue.queued_bytes == 10 * _charge(100)
    assert [f.seq for f in queue.items] == list(range(1, 11))  # oldest kept


def test_the_frame_count_bound_still_applies_to_small_frames() -> None:
    queue = _FrameQueue(3, 1 << 30)
    assert [queue.put(_frame(0, seq)) for seq in range(1, 6)] == [True, True, True, False, False]


def test_frames_the_consumer_pops_free_their_bytes_for_the_next_put() -> None:
    queue = _FrameQueue(1000, 4 * _charge(100))
    for seq in range(1, 5):
        assert queue.put(_frame(100, seq))
    assert not queue.put(_frame(100))
    queue.items.popleft()
    queue.items.popleft()
    assert queue.queued_bytes == 2 * _charge(100)
    assert queue.put(_frame(100)) and queue.put(_frame(100)) and not queue.put(_frame(100))


def test_a_put_at_the_byte_bound_releases_popped_frames_without_anyone_asking_the_size() -> None:
    # Nothing reads ``queued_bytes`` here: the put itself must find the room the consumer freed.
    queue = _FrameQueue(1000, 4 * _charge(100))
    for seq in range(1, 5):
        assert queue.put(_frame(100, seq))
    assert not queue.put(_frame(100))
    queue.items.popleft()
    assert queue.put(_frame(100)), "the freed room must be found by the put that needs it"
    assert not queue.put(_frame(100))
    assert queue.dropped == 2


def test_a_consumer_that_pops_never_makes_the_charge_list_outgrow_the_queue() -> None:
    # The byte and count bounds are never reached, so only the periodic release trims the list
    # of charges; it must stay a few dozen entries long however long the consumer keeps popping.
    queue = _FrameQueue(1 << 30, 1 << 40)
    longest = 0
    for seq in range(1, 20 * _RELEASE_EVERY + 1):
        assert queue.put(_frame(10, seq))
        queue.items.popleft()
        longest = max(longest, len(queue._charges))
    assert longest <= _RELEASE_EVERY, longest
    assert queue._bytes <= _RELEASE_EVERY * _charge(10)  # never below the truth, but not runaway


def test_a_batch_the_consumer_holds_stays_charged_until_it_asks_for_more() -> None:
    async def main() -> None:
        queue = _FrameQueue(1000, 10 * _charge(100))
        for seq in range(1, 11):
            assert queue.put(_frame(100, seq))
        await queue.wait()
        batch = queue.take(4)
        assert [f.seq for f in batch] == [1, 2, 3, 4] and len(queue.items) == 6
        assert queue.queued_bytes == 10 * _charge(100)  # the held four still count
        assert not queue.put(_frame(100))
        await queue.wait()  # asking for more releases the batch
        assert queue.queued_bytes == 6 * _charge(100)
        assert all(queue.put(_frame(100)) for _ in range(4)) and not queue.put(_frame(100))

    _run(main())


def test_a_closed_queue_drains_then_raises_the_error_it_was_closed_with() -> None:
    async def main() -> None:
        queue = _FrameQueue(10, 1 << 20)
        queue.put(_frame(1))
        error = IronbirdError(Code.TX_RECV, "gone")
        queue.close(error)
        assert not queue.put(_frame(1)) and queue.offered == 1  # a closed queue is offered nothing
        await queue.wait()
        assert len(queue.items) == 1
        queue.items.popleft()
        with pytest.raises(IronbirdError) as info:
            await queue.wait()
        assert info.value is error
        queue.close(IronbirdError(Code.TX_RECV, "later"))  # the first failure stands
        assert queue.error is error

    _run(main())


@FUZZ
@given(
    ops=st.lists(
        st.one_of(
            st.tuples(st.just("put"), st.integers(0, 3000)),
            st.tuples(st.just("pop"), st.just(0)),
            st.tuples(st.just("take"), st.integers(0, 6)),
        ),
        max_size=120,
    ),
    room=st.integers(2, 12),
)
def test_queue_accounting_matches_a_model_for_any_interleaving(
    ops: list[tuple[str, int]], room: int
) -> None:
    limit = room * _charge(1500)

    async def main() -> None:
        queue = _FrameQueue(10_000, limit)
        model: deque[int] = deque()  # sizes queued
        held: list[int] = []
        for op, arg in ops:
            if op == "put":
                fits = sum(map(_charge, model)) + sum(map(_charge, held)) + _charge(arg) <= limit
                assert queue.put(_frame(arg)) is fits
                if fits:
                    model.append(arg)
            elif op == "pop" and model:
                assert len(queue.items.popleft().data) == model.popleft()
            elif op == "take":
                if not queue.items:
                    continue  # a consumer would block in wait(); the held batch stays charged
                await queue.wait()  # asking for more releases the previous batch
                held = []
                batch = queue.take(arg)
                held = [model.popleft() for _ in batch]
                assert [len(f.data) for f in batch] == held
            charged = sum(map(_charge, model)) + sum(map(_charge, held))
            assert queue.queued_bytes == charged <= limit
            assert queue.peak_bytes <= limit

    _run(main())


# -- the ledger: truncation at creation, byte bound --------------------------------------


def _ledger(tmp_path: Path, **kwargs: Any) -> Ledger:
    return Ledger(tmp_path / "ledger", session_id="2c", **kwargs)


def test_raw_bytes_are_cut_when_the_record_is_created_not_when_it_is_written(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path, max_raw_bytes=100)
    ledger.record("packet", "rx.malformed", raw=bytes(60000))
    ledger.record("packet", "rx.malformed", raw=bytearray(60000))
    ledger.record("packet", "rx.malformed", raw=memoryview(bytes(60000)))
    assert ledger.pending_bytes == 3 * (ENTRY_OVERHEAD_BYTES + 100) + 3 * 0  # no 60000 B queued
    ledger.flush()
    for line in ledger.path.read_text().splitlines():
        record = json.loads(line)
        assert record["raw_len"] == 60000 and record["raw"].endswith("...(+59900B)")
        assert len(bytes.fromhex(record["raw"].split(" ...")[0])) == 100
    assert ledger.pending_bytes == 0


def test_a_buffer_the_producer_reuses_cannot_change_a_queued_record(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    buffer = bytearray(b"\x01\x02\x03")
    ledger.record("packet", "rx", raw=buffer)
    view = memoryview(bytearray(b"\x0a\x0b"))
    ledger.record("packet", "rx", raw=view)
    buffer[:] = b"\xff\xff\xff"
    view[:] = b"\xff\xff"
    ledger.flush()
    first, second = (json.loads(x) for x in ledger.path.read_text().splitlines())
    assert first["raw"] == "01 02 03" and first["raw_len"] == 3
    assert second["raw"] == "0a 0b" and second["raw_len"] == 2


def test_wide_and_strided_memoryviews_are_measured_in_bytes(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path, max_raw_bytes=8)
    ledger.record("packet", "rx", raw=memoryview(bytes(range(20))).cast("H"))  # 10 items, 20 bytes
    ledger.record("packet", "rx", raw=memoryview(bytes(range(20)))[::2])  # not contiguous
    ledger.flush()
    wide, strided = (json.loads(x) for x in ledger.path.read_text().splitlines())
    assert wide["raw_len"] == 20 and wide["raw"].startswith("00 01 02 03 04 05 06 07 ...")
    assert strided["raw_len"] == 10 and strided["raw"].startswith("00 02 04 06 08 0a 0c 0e ...")


def test_the_queue_is_bounded_by_bytes_and_refused_records_are_counted(tmp_path: Path) -> None:
    per_record = ENTRY_OVERHEAD_BYTES + 1000
    ledger = _ledger(
        tmp_path,
        max_raw_bytes=1000,
        max_queue_bytes=10 * per_record + 5,
        max_evidence_queue_bytes=6 * per_record,
    )
    packets = [ledger.record("packet", "rx", raw=bytes(5000)) for _ in range(10)]
    assert packets.count(None) == 4 and ledger.dropped == 4  # evidence stops at its share
    essential = [ledger.record("action", "x", note="y" * 1000) for _ in range(6)]
    assert essential.count(None) == 2  # the rest of the byte bound, then refused
    assert ledger.pending_bytes <= 10 * per_record + 5
    ledger.flush()
    assert ledger.pending_bytes == 0
    last = json.loads(ledger.path.read_text().splitlines()[-1])
    assert last["event"] == "ledger.dropped" and last["count"] == 6


@FUZZ
@given(
    records=st.lists(
        st.tuples(
            st.sampled_from(["packet", "action", "state"]),
            st.integers(0, 70000),
            st.sampled_from([bytes, bytearray, memoryview]),
        ),
        max_size=60,
    ),
    raw_cap=st.integers(1, 3000),
)
def test_ledger_memory_stays_under_its_byte_bound_for_any_record_sizes(
    records: list[tuple[str, int, type]], raw_cap: int
) -> None:
    bound = 20 * (ENTRY_OVERHEAD_BYTES + raw_cap)
    with pytest.MonkeyPatch.context() as _:
        ledger = Ledger(
            Path(os.devnull).parent / "ib-2c-unused",  # never written: nothing is flushed
            max_raw_bytes=raw_cap,
            max_queue_bytes=bound,
            max_evidence_queue_bytes=bound // 2,
        )
        for kind, size, container in records:
            ledger.record(kind, "e", raw=container(bytes(size)))
            assert ledger.pending_bytes <= bound
        queued = sum(entry[6] for entry in ledger._queue)
        assert queued == ledger.pending_bytes
        assert all(
            len(entry[4]["raw"].head if hasattr(entry[4]["raw"], "head") else entry[4]["raw"])
            <= raw_cap
            for entry in ledger._queue
        )
        ledger.disable()
        assert ledger.pending_bytes == 0


# -- end to end: a flood of maximum-size datagrams against a consumer that never reads ---------

FLOOD_SENDER = """
import socket, sys, time
port, count, size = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
data = bytes(size)
for i in range(count):
    sock.sendto(data, ("127.0.0.1", port))
    if i % 8 == 7:
        time.sleep(0.0005)
"""


def _rss_kib() -> int:
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    raise AssertionError("no VmRSS in /proc/self/status")


def test_a_flood_of_64k_datagrams_with_a_stalled_consumer_keeps_memory_bounded(
    mock_config_path: Path, tmp_path: Path
) -> None:
    """The bug this guards: with ``max_packet_bytes`` at 65535 and a consumer that does not read,
    a queue bounded only by frame count held 16384 frames of 64 KiB (1 GiB)."""
    text = mock_config_path.read_text().replace(
        'acts_as = "ground"', 'acts_as = "ground"\nmax_packet_bytes = 65535'
    )
    notices: list[Notice] = []
    datagram, count, queue_bytes = 60000, 4000, 4 * 1024 * 1024

    class CountingSink:
        frames = 0
        wire_bytes = 0
        longest = 0

        def append(self, frame: Frame) -> None:
            self.frames += 1
            self.wire_bytes += frame.orig_len
            self.longest = max(self.longest, len(frame.data))

    async def main() -> dict[str, int]:
        # No ledger: every flooded frame is malformed here (zeros), and a ledger would write the
        # raw bytes of each to disk. Its memory bound is covered by its own tests.
        async with _ground(mock_config_path, tmp_path, notices, text, ledger=False) as ground:
            sink = CountingSink()
            capture: Capture = await ground.capture(
                [ground.route("fc")], sinks=[sink], queue_bytes=queue_bytes
            )
            [(_, address)] = capture.addresses
            assert address is not None
            io = _source_io(ground)
            before = _rss_kib()
            sender = await asyncio.create_subprocess_exec(
                sys.executable, "-c", FLOOD_SENDER, str(address[1]), str(count), str(datagram)
            )
            await sender.wait()
            transport = io.transport
            await _until(lambda: transport.stats().rx_frames == sink.frames)
            await asyncio.sleep(0.2)  # the socket buffer is empty and ingest has caught up
            transport.refresh_kernel_drops()
            stats = capture.stats()
            rx_frames = transport.stats().rx_frames
            kernel = transport.stats().kernel_dropped
            result = {
                "rss_growth_kib": _rss_kib() - before,
                "sent": count,
                "received": rx_frames,
                "kernel_dropped": kernel,
                "sink_frames": sink.frames,
                "offered": stats.frames,
                "not_shown": stats.not_shown,
                "unread": stats.unread,
                "peak_bytes": stats.peak_bytes,
                "longest": sink.longest,
            }
            shown = 0
            async for _ in capture:
                shown += 1
                if shown == stats.unread:
                    break
            result["shown"] = shown
            await capture.close()
            return result

    r = _run(main(), timeout=120)
    print(f"\nflood: {r}")
    assert r["longest"] == datagram  # the sinks got whole frames: the cap was 65535
    assert r["received"] >= 500, f"the flood did not get through ({r['received']} of {count})"
    assert r["sent"] == r["received"] + r["kernel_dropped"], r  # every datagram is accounted for
    assert r["sink_frames"] == r["offered"] == r["received"]  # the sink saw them all, recorded
    assert r["offered"] == r["shown"] + r["not_shown"], r  # shown + not_shown, nothing else
    assert r["not_shown"] > 0 and r["peak_bytes"] <= queue_bytes, r
    assert r["peak_bytes"] > queue_bytes - 2 * (datagram + FRAME_OVERHEAD_BYTES)  # it was full
    # The queue held at most 4 MiB. The flood moved far more through, so a leak of even a
    # fraction of it would show; the allowance covers allocator and interpreter noise.
    assert r["received"] * datagram > 10 * queue_bytes
    assert r["rss_growth_kib"] < (queue_bytes // 1024) + 16 * 1024, r
    assert [n for n in notices if n.code is Code.ENG_LAG], "the lag was not reported"


def test_the_default_queue_bytes_admit_the_largest_frame() -> None:
    assert CONSUMER_QUEUE_BYTES >= MAX_FRAME_BYTES + FRAME_OVERHEAD_BYTES
