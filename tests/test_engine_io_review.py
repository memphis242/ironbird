"""Regression tests for the engine / transport / signal review (F1-F8).

F1  a resolver that does not answer held the process after IB-TX-001, deaf to signals, and the
    error named the telemetry endpoint instead of the one being resolved;
F2  a serial send cancelled or timed out with part of its packet on the line was reported (and
    resent) as not sent; a cancelled send none of whose bytes went out still went out;
F3  a signal while a recording closed abandoned its backlog and its trailer;
F4  a serial close dropped, uncounted, the bytes the device already held;
F5  one failing tick silenced drop reporting for the rest of the session;
F6  ``SerialTransport.close`` swallowed a cancellation of its caller;
F7  a cancellation during ``Session.__aexit__`` skipped ``Engine.close``;
F8  an ``EventStream`` consumer waited forever after its capture ended cleanly.

The CLI cases (F1) run the real entry point in a subprocess with a patched resolver; the rest
drive the module API on a pty or loopback.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import pytest

import test_review2_engine as r2
from conftest import EXAMPLES, free_udp_ports
from ironbird.config import Endpoint, load_config
from ironbird.engine import Engine
from ironbird.errors import Code, IronbirdError
from ironbird.frame import GapReason
from ironbird.ledger import Ledger
from ironbird.notice import Notice
from ironbird.recording import inspect as rec_inspect
from ironbird.recording.writer import Recorder, RecorderOptions
from ironbird.session import Session
from ironbird.transport import LinkEnd, TransportClosed, TransportStats
from ironbird.transport import serial as serial_module
from ironbird.transport import udp as udp_module
from ironbird.transport.serial import SerialTransport
from ironbird.transport.udp import UdpTransport
from recutil import RX, TX, frame, meta
from serialutil import Line, hk, wait_for
from support.ptybridge import PtyDevice
from test_engine import _Probe

state_dir = r2.state_dir

PACKET = b"\xa5" * 51200  # more than a pty takes at once, less than TX_BUFFER_BYTES


def _mock_config(tmp_path: Path, *, command_host: str = "127.0.0.1") -> Path:
    cmd, tlm = free_udp_ports(2)
    text = (EXAMPLES / "mock.toml").read_text()
    text = text.replace("127.0.0.1:41234", f"{command_host}:{cmd}")
    text = text.replace("127.0.0.1:41235", f"127.0.0.1:{tlm}")
    text = text.replace('"ccdd/', f'"{EXAMPLES}/ccdd/')
    text = text.replace('"headers/', f'"{EXAMPLES}/headers/')
    path = tmp_path / "m.toml"
    path.write_text(text)
    return path


def _session(tmp_path: Path, notify: Any = lambda _n: None) -> Session:
    config = load_config(_mock_config(tmp_path))
    return Session(
        config, role="ground", ledger=Ledger(tmp_path / "l", session_id="x"), notify=notify
    )


# -- F1: a resolver that does not answer -------------------------------------------------------

HUNG_RESOLVER = """
    import socket, sys, time
    import ironbird.engine
    ironbird.engine.OPEN_TIMEOUT_S = {timeout}
    _real = socket.getaddrinfo
    def _getaddrinfo(host, *args, **kwargs):
        if host == "hang.invalid":
            sys.stderr.write("RESOLVING hang.invalid\\n")
            sys.stderr.flush()
            time.sleep(120)  # a DNS server that does not answer
        return _real(host, *args, **kwargs)
    socket.getaddrinfo = _getaddrinfo
"""


def _hung_send(tmp_path: Path, timeout: float) -> subprocess.Popen[str]:
    config = _mock_config(tmp_path, command_host="hang.invalid")
    return subprocess.Popen(
        r2.patched_argv(HUNG_RESOLVER.format(timeout=timeout), config, "send", "fc", "MOCK_NOOP"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def test_f1_a_hung_resolver_does_not_hold_the_process_after_the_open_times_out(
    tmp_path: Path, state_dir: Path
) -> None:
    started = time.monotonic()
    proc = _hung_send(tmp_path, 0.5)
    try:
        _, err = proc.communicate(timeout=20)  # the resolver would answer after 120 s
    finally:
        r2.kill_quietly(proc)
    took = time.monotonic() - started
    assert proc.returncode == 1, err
    assert took < 15, f"the process waited {took:.1f} s for the resolver"
    [line] = [ln for ln in err.splitlines() if Code.TX_OPEN.value in ln]
    assert "hang.invalid" in line, line  # the endpoint being resolved, not the telemetry one


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_f1_a_signal_during_a_hung_lookup_ends_the_command_at_once(
    tmp_path: Path, state_dir: Path, signum: signal.Signals
) -> None:
    proc = _hung_send(tmp_path, 60)
    try:
        r2.wait_for_text(proc, "RESOLVING hang.invalid", timeout=20)
        sent = time.monotonic()
        proc.send_signal(signum)
        _, err = proc.communicate(timeout=20)
        took = time.monotonic() - sent
    finally:
        r2.kill_quietly(proc)
    assert proc.returncode == 128 + signum, err
    assert took < 10, f"the signal took {took:.1f} s to end the command"
    assert "was not sent" in err, err
    [event] = r2.events_named(state_dir, "cli.signal")
    assert event["signum"] == int(signum)


def test_f1_lookups_that_never_answer_are_bounded_then_refused_with_a_coded_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()
    real = socket.getaddrinfo

    def hanging(host: Any, *args: Any, **kwargs: Any) -> Any:
        if host == "hang.invalid":
            release.wait(30)
        return real(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", hanging)

    async def main() -> IronbirdError:
        for _ in range(udp_module.RESOLVER_THREADS):  # each one cut short, its thread left
            transport = UdpTransport(remote=Endpoint("hang.invalid", 9))
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.05):
                    await transport.open(None)
            assert transport.opening_step == "resolve host of udp hang.invalid:9"
        with pytest.raises(IronbirdError) as info:
            await UdpTransport(remote=Endpoint("hang.invalid", 9)).open(None)
        return info.value

    try:
        refused = asyncio.run(main())
    finally:
        release.set()
    assert refused.code is Code.TX_OPEN and "never answered" in refused.summary
    wait_until = time.monotonic() + 10
    while udp_module._RESOLVER.running and time.monotonic() < wait_until:
        time.sleep(0.01)
    assert udp_module._RESOLVER.running == 0  # released lookups give their slots back


def test_s1_an_ip_literal_opens_while_every_resolver_slot_hangs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()
    real = socket.getaddrinfo

    def hanging(host: Any, *args: Any, **kwargs: Any) -> Any:
        if host == "hang.invalid":
            release.wait(30)
        return real(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", hanging)
    (port,) = free_udp_ports(1)

    async def main() -> None:
        for _ in range(udp_module.RESOLVER_THREADS):
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.05):
                    await UdpTransport(remote=Endpoint("hang.invalid", 9)).open(None)
        transport = UdpTransport(remote=Endpoint("127.0.0.1", port))
        await transport.open(None)
        await transport.close()

    try:
        asyncio.run(main())
    finally:
        release.set()


# -- F2: serial sends with part of the packet on the line -----------------------------------------


async def _drain_device(device: PtyDevice, into: bytearray, until: int, timeout: float) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while len(into) < until and loop.time() < deadline:
        into += device.read_available()
        await asyncio.sleep(0.002)


def test_f2_a_send_cancelled_after_its_head_went_out_is_sent_and_handed_off(
    tmp_path: Path,
) -> None:
    device = PtyDevice(tmp_path / "tty")
    handed: list[bool] = []

    async def body() -> bytes:
        line = Line(device)
        await line.open(receiving=False)
        try:
            send = asyncio.ensure_future(
                line.transport.send(PACKET, handed_off=lambda: handed.append(True))
            )
            await wait_for(lambda: bool(line.transport._txbuf), "a partial write")
            send.cancel()
            outcome = (await asyncio.gather(send, return_exceptions=True))[0]
            assert isinstance(outcome, asyncio.CancelledError)  # the cancel is not absorbed
            got = bytearray()
            await _drain_device(device, got, len(PACKET), 10)
            return bytes(got)
        finally:
            await line.transport.close()

    try:
        got = asyncio.run(asyncio.wait_for(body(), 30))
    finally:
        device.close()
    assert got == PACKET and handed == [True]


def test_f2_a_cancelled_send_none_of_whose_bytes_went_out_is_taken_back(tmp_path: Path) -> None:
    device = PtyDevice(tmp_path / "tty")
    first, second, third = b"\x01" * 30000, b"\x02" * 1000, b"\x03" * 1000
    handed: list[str] = []

    async def body() -> bytes:
        line = Line(device)
        await line.open(receiving=False)
        try:
            transport = line.transport
            sends = [
                asyncio.ensure_future(transport.send(data, handed_off=lambda n=n: handed.append(n)))
                for n, data in (("first", first), ("second", second), ("third", third))
            ]
            await asyncio.sleep(0)
            await wait_for(lambda: len(transport._tx_waiters) == 3, "three waiting sends")
            sends[1].cancel()
            got = bytearray()
            await _drain_device(device, got, len(first) + len(third), 10)
            outcomes = await asyncio.gather(*sends, return_exceptions=True)
            assert outcomes[0] is None and outcomes[2] is None
            assert isinstance(outcomes[1], asyncio.CancelledError)
            return bytes(got)
        finally:
            await line.transport.close()

    try:
        got = asyncio.run(asyncio.wait_for(body(), 30))
    finally:
        device.close()
    assert got == first + third  # the cancelled one never reached the line
    assert handed == ["first", "third"]


def test_f2_a_send_that_times_out_part_way_says_how_much_went_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(SerialTransport, "_drain_timeout", lambda self, pending: 0.2)
    device = PtyDevice(tmp_path / "tty")

    async def body() -> tuple[IronbirdError, int]:
        line = Line(device)
        await line.open(receiving=False)
        try:
            with pytest.raises(IronbirdError) as info:
                await line.transport.send(PACKET)
            return info.value, len(device.read_available())
        finally:
            await line.transport.close()

    try:
        error, on_wire = asyncio.run(asyncio.wait_for(body(), 30))
    finally:
        device.close()
    assert on_wire > 0
    assert not isinstance(error, TransportClosed) and error.code is Code.TX_SEND
    assert not error.summary.startswith("not sent")
    assert f"of {len(PACKET)} bytes" in error.summary and "torn" in error.summary


def test_f2_close_lets_the_tail_of_a_packet_already_on_the_line_follow(tmp_path: Path) -> None:
    device = PtyDevice(tmp_path / "tty")

    async def body() -> tuple[bytes, list[IronbirdError]]:
        line = Line(device)
        await line.open(receiving=False)
        send = asyncio.ensure_future(line.transport.send(PACKET))
        await wait_for(lambda: bool(line.transport._txbuf), "a partial write")
        send.cancel()  # the sender is gone; the packet is past the point of no return
        await asyncio.gather(send, return_exceptions=True)
        got = bytearray()
        reader = asyncio.ensure_future(_drain_device(device, got, len(PACKET), 10))
        await line.transport.close()  # waits for the tail
        await reader
        got += device.read_available()
        return bytes(got), line.reports

    try:
        got, reports = asyncio.run(asyncio.wait_for(body(), 30))
    finally:
        device.close()
    assert got == PACKET and reports == []


def test_f2_a_waiting_send_whose_tail_cannot_follow_at_close_is_torn_not_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    device = PtyDevice(tmp_path / "tty")

    async def body() -> BaseException | None:
        line = Line(device)
        await line.open(receiving=False)
        send = asyncio.ensure_future(line.transport.send(PACKET))
        await wait_for(lambda: bool(line.transport._txbuf), "a partial write")
        monkeypatch.setattr(SerialTransport, "_drain_timeout", lambda self, pending: 0.1)
        await line.transport.close()  # nobody reads the device: the tail cannot follow
        return (await asyncio.gather(send, return_exceptions=True))[0]

    try:
        outcome = asyncio.run(asyncio.wait_for(body(), 30))
    finally:
        device.close()
    # TransportClosed would make Engine.send send the whole packet again after its head
    assert isinstance(outcome, IronbirdError) and not isinstance(outcome, TransportClosed)
    assert "torn" in outcome.summary and "transport closed" in outcome.summary


# -- F3: a signal while the recording closes ---------------------------------------------------


def test_f3_a_cancel_during_the_recorder_close_still_writes_the_backlog_and_trailer(
    tmp_path: Path,
) -> None:
    path = tmp_path / "slow.ibrec"
    count = 2000

    def slow_write(fd: int, view: memoryview) -> int:  # a disk that takes 4 MiB/s
        time.sleep(len(view) / (4 * 1024 * 1024))
        return os.write(fd, view)

    async def main() -> tuple[bool, bool]:
        loop = asyncio.get_running_loop()
        rec = Recorder.open(
            path,
            meta(),
            streams=(RX, TX),
            options=RecorderOptions(min_free_bytes=0),
            loop=loop,
            write=slow_write,
        )
        for i in range(1, count + 1):
            rec.append(frame(i, data=bytes([i % 251]) * 1000))
        me = asyncio.current_task()
        assert me is not None
        loop.call_later(0.1, me.cancel)  # what the CLI's handler does on SIGINT/SIGTERM
        try:
            await rec.close()
        except asyncio.CancelledError:
            return True, rec._writer.is_alive()
        return False, rec._writer.is_alive()

    cancelled, writer_alive = asyncio.run(main())
    assert cancelled, "the cancellation must still propagate"
    assert not writer_alive
    report = rec_inspect.info(path)
    assert report.status == "closed" and report.frames == count


def test_s2_a_cancelled_close_on_a_slow_but_moving_disk_is_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ironbird.recording import writer as writer_module

    monkeypatch.setattr(writer_module, "CLOSE_STALL_S", 0.3)
    path = tmp_path / "crawl.ibrec"

    def crawl(fd: int, view: memoryview) -> int:  # always progressing, never finishing soon
        time.sleep(0.01)
        return os.write(fd, view[:64])

    async def main() -> float:
        loop = asyncio.get_running_loop()
        rec = Recorder.open(
            path,
            meta(),
            streams=(RX, TX),
            options=RecorderOptions(min_free_bytes=0),
            loop=loop,
            write=crawl,
        )
        for i in range(1, 2001):
            rec.append(frame(i, data=bytes([i % 251]) * 1000))
        me = asyncio.current_task()
        assert me is not None
        loop.call_later(0.05, me.cancel)
        start = time.monotonic()
        with pytest.raises(asyncio.CancelledError):
            await rec.close()
        return time.monotonic() - start

    assert asyncio.run(main()) < 3.0


# -- F4: bytes the serial device holds at close --------------------------------------------------


def test_f4_a_packet_waiting_in_the_device_at_close_is_delivered(tmp_path: Path) -> None:
    device = PtyDevice(tmp_path / "tty")

    async def body() -> Line:
        line = Line(device, idle_ms=10_000)
        await line.open()
        device.write(hk(9))
        time.sleep(0.05)  # in the tty input queue; the loop has not read it yet
        await line.transport.close()
        return line

    try:
        line = asyncio.run(asyncio.wait_for(body(), 30))
    finally:
        device.close()
    assert line.data() == [hk(9)]


def test_f4_bytes_left_in_the_device_beyond_the_close_reads_are_counted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(serial_module, "READ_BYTES", len(hk(1)))
    monkeypatch.setattr(serial_module, "READS_PER_WAKEUP", 1)
    device = PtyDevice(tmp_path / "tty")

    async def body() -> tuple[Line, TransportStats]:
        line = Line(device, idle_ms=10_000)
        await line.open()
        device.write(hk(1) + hk(2) + hk(3))
        time.sleep(0.05)
        await line.transport.close()
        return line, line.transport.stats()

    try:
        line, stats = asyncio.run(asyncio.wait_for(body(), 30))
    finally:
        device.close()
    assert line.data() == [hk(1)]
    assert stats.unread_bytes_at_close == len(hk(2)) + len(hk(3))


def test_f4_stream_bytes_unread_at_close_are_ledgered_and_one_gap(tmp_path: Path) -> None:
    probe = _Probe(tmp_path / "probe")
    ground = probe.io[LinkEnd.SOURCE]
    gaps: list[tuple[GapReason, int]] = []

    class GapProbe:
        def append(self, frame: Any) -> None:
            del frame

        def gap(self, _stream: Any, reason: GapReason, lost: int, *_: int) -> None:
            gaps.append((reason, lost))

    class Closed:
        def stats(self) -> TransportStats:
            return TransportStats(unread_bytes_at_close=56)

    probe.engine.add_sink(GapProbe())
    probe.engine._note_unread(ground, Closed())  # type: ignore[arg-type]
    assert gaps == [(GapReason.KERNEL_DROP, 1)]  # how many packets 56 bytes held is unknown


# -- F5: one failing tick ---------------------------------------------------------------------


def test_f5_a_failed_poll_is_reported_and_drop_reporting_goes_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ironbird.session.TICK_S", 0.05)
    real = udp_module.scan_kernel_drops
    calls = {"n": 0}

    async def flaky(transports: Any) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("can't start new thread")
        await real(transports)

    monkeypatch.setattr(udp_module, "scan_kernel_drops", flaky)
    notices: list[Notice] = []

    async def main() -> None:
        async with _session(tmp_path, notices.append) as session:
            capture = await session.capture([session.route("fc")], queue_frames=4)
            data = session.codec_for("fc").encode_telemetry("MOCK_HK").data
            [(_, addr)] = capture.addresses
            assert addr is not None
            raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            raw.sendto(data, addr)  # traffic, then quiet: the next tick scans /proc and fails
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 10
            while calls["n"] < 1 and loop.time() < deadline:
                await asyncio.sleep(0.01)
            assert calls["n"] >= 1
            for _ in range(50):  # the consumer never reads: frames not shown
                raw.sendto(data, addr)
            raw.close()
            deadline = loop.time() + 10
            while Code.ENG_LAG not in [n.code for n in notices] and loop.time() < deadline:
                await asyncio.sleep(0.02)
            await capture.close()

    asyncio.run(main())
    codes = [n.code for n in notices]
    assert Code.ENG_LAG in codes, codes  # reported while the session ran
    failed = [n for n in notices if n.code is Code.INTERNAL]
    assert failed and "drop counters" in failed[0].summary
    assert not any("background task" in n.summary for n in notices)


def test_f5_a_tick_step_that_raises_does_not_stop_the_ticker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ironbird.session.TICK_S", 0.01)
    ticks = {"engine": 0}
    notices: list[Notice] = []

    async def counting_tick(self: Engine) -> None:
        ticks["engine"] += 1

    monkeypatch.setattr(Engine, "tick", counting_tick)

    async def main() -> None:
        async with _session(tmp_path, notices.append) as session:
            broken = {"left": 3}
            real_tick = session._coalescer.tick

            def tick() -> None:
                if broken["left"]:
                    broken["left"] -= 1
                    raise ValueError("boom")
                real_tick()

            session._coalescer.tick = tick  # type: ignore[method-assign]
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 10
            while ticks["engine"] < 10 and loop.time() < deadline:
                await asyncio.sleep(0.01)

    asyncio.run(main())
    assert ticks["engine"] >= 10  # the engine still ticked after each coalescer failure
    failed = [n for n in notices if n.code is Code.INTERNAL]
    assert failed and all("periodic coalescer work failed" in n.summary for n in failed)


# -- F6: SerialTransport.close and its caller's cancellation -------------------------------------


def test_f6_a_cancel_that_lands_while_close_waits_for_the_reconnect_task_propagates(
    tmp_path: Path,
) -> None:
    device = PtyDevice(tmp_path / "tty")
    after_close: list[str] = []

    async def caller(line: Line) -> None:
        await line.transport.close()
        after_close.append("ran on after close")

    async def body() -> asyncio.Task[None]:
        line = Line(device, fast_reconnect=False)  # the reconnect task sleeps 0.5 s
        await line.open()
        device.unplug()
        await wait_for(lambda: line.transport.is_down, "the link down")
        assert line.transport._reconnect is not None
        task = asyncio.ensure_future(caller(line))
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return task

    try:
        task = asyncio.run(asyncio.wait_for(body(), 30))
    finally:
        device.close()
    assert task.cancelled() and after_close == []


# -- F7: a cancel during Session.__aexit__ ---------------------------------------------------


def test_f7_the_engine_is_closed_even_when_the_session_exit_is_cancelled(tmp_path: Path) -> None:
    seen: dict[str, Any] = {}

    async def main() -> None:
        session = _session(tmp_path)
        seen["session"] = session
        async with session:
            seen["capture"] = await session.capture([session.route("fc")])
            me = asyncio.current_task()
            assert me is not None
            asyncio.get_running_loop().call_soon(me.cancel)  # lands in __aexit__'s gather

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(main())
    engine = seen["session"]._engine
    [io] = engine._ios.values()
    assert engine._closed and io.transport is None
    assert seen["capture"]._queue.closed


# -- F8: EventStream after a clean end ----------------------------------------------------


def test_f8_event_iteration_ends_when_the_engine_closes(tmp_path: Path) -> None:
    async def main() -> object:
        async with _session(tmp_path) as session:
            stream = await session.events()
            waiting = asyncio.ensure_future(anext(aiter(stream), "ended"))
            await asyncio.sleep(0.05)
            await session._engine.close()  # what a long-lived owner does on shutdown
            try:
                return await asyncio.wait_for(waiting, 5)
            finally:
                await stream.close()

    assert asyncio.run(main()) == "ended"


def test_f8_a_second_consumer_ends_when_the_stream_is_closed(tmp_path: Path) -> None:
    async def main() -> object:
        async with _session(tmp_path) as session:
            stream = await session.events()
            waiting = asyncio.ensure_future(anext(aiter(stream), "ended"))
            await asyncio.sleep(0.05)
            await stream.close()
            return await asyncio.wait_for(waiting, 5)

    assert asyncio.run(main()) == "ended"
