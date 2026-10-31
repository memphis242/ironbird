"""Stage 2k: the serial transport on a pseudo-terminal.

A pty slave is opened by path exactly as a serial device is; closing the pty master is the
unplug (reads end, writes fail with ``EIO``). Nothing here sleeps for a fixed time: each test waits
for the condition it needs, with a generous timeout.
"""

from __future__ import annotations

import asyncio
import errno
import os
import termios
import threading
import tomllib
from collections.abc import Iterator
from pathlib import Path

import pytest
import serial

import ser_corpus
from conftest import EXAMPLES
from ironbird.config import load_config
from ironbird.errors import Code, IronbirdError
from ironbird.frame import FLAG_GARBAGE, FLAG_STALE
from ironbird.transport import LinkEnd, SerialTransport, TransportClosed, open_transport
from ironbird.transport import serial as serial_module
from ironbird.transport.serial import (
    DRAIN_FLOOR_S,
    READ_BYTES,
    TX_BUFFER_BYTES,
    errno_of,
    open_error,
)
from serialutil import CODEC, Line, hk, serial_config, wait_for
from support.ptybridge import PtyDevice


@pytest.fixture
def device(tmp_path: Path) -> Iterator[PtyDevice]:
    dev = PtyDevice(tmp_path / "tty")
    yield dev
    dev.close()


def run(coro: object) -> None:
    asyncio.run(asyncio.wait_for(coro, 60))  # type: ignore[arg-type]


# -- receiving ----------------------------------------------------------------------------


def test_packets_written_to_the_line_arrive_as_frames_stamped_when_read(device: PtyDevice) -> None:
    async def body() -> None:
        line = Line(device)
        await line.open()
        try:
            device.write(hk(1) + hk(2))
            await wait_for(lambda: len(line.frames) >= 1, "the first packet")
            device.write(hk(3))
            await wait_for(lambda: len(line.frames) == 3, "all three packets (silence confirms)")
        finally:
            await line.transport.close()
        assert line.data() == [hk(1), hk(2), hk(3)]
        times = [t for _, t, _ in line.frames]
        assert times == sorted(times) and times[0] > 0
        stats = line.transport.stats()
        assert stats.rx_frames == 3 and stats.rx_bytes == 3 * len(hk(1))

    run(body())


def test_the_idle_timer_confirms_a_packet_the_line_does_not_follow(device: PtyDevice) -> None:
    async def body() -> None:
        line = Line(device, idle_ms=30)
        await line.open()
        try:
            device.write(hk(5))
            await wait_for(lambda: line.data() == [hk(5)], "the packet after the idle time")
        finally:
            await line.transport.close()

    run(body())


def test_garbage_between_packets_is_framed_and_reported_once_per_read(device: PtyDevice) -> None:
    async def body() -> None:
        line = Line(device)
        await line.open()
        try:
            device.write(hk(1))
            await wait_for(lambda: len(line.data()) == 1, "the first packet")
            device.write(b"\x00\x01\x02garbage")
            await wait_for(lambda: len(line.data(FLAG_GARBAGE)) == 1, "the garbage frame")
            device.write(hk(2))
            await wait_for(lambda: len(line.data()) == 2, "the next packet")
        finally:
            await line.transport.close()
        assert line.data(FLAG_GARBAGE) == [b"\x00\x01\x02garbage"]
        [report] = line.reports
        assert report.code is Code.TX_RESYNC and "10 byte(s)" in report.summary
        assert "00 01 02" in report.detail_text() or "00 01 02" in report.details

    run(body())


def test_a_read_larger_than_one_chunk_is_framed_whole(device: PtyDevice) -> None:
    async def body() -> None:
        line = Line(device)
        await line.open()
        count = 3 * READ_BYTES // len(hk(1))
        try:
            device.write(b"".join(hk(i % 200) for i in range(count)))
            await wait_for(lambda: len(line.data()) >= count - 1, "nearly all of the packets")
        finally:
            await line.transport.close()
        assert line.data() == [hk(i % 200) for i in range(count)]

    run(body())


def test_the_last_bytes_before_a_close_are_delivered_not_lost(device: PtyDevice) -> None:
    async def body() -> None:
        line = Line(device, idle_ms=10_000)  # the idle timer cannot be what delivers it
        await line.open()
        device.write(hk(9))
        await wait_for(lambda: line.transport.stats().rx_bytes == len(hk(9)), "the read")
        assert line.frames == []
        await line.transport.close()
        assert line.data() == [hk(9)]

    run(body())


# -- sending --------------------------------------------------------------------------------


def test_send_writes_the_packet_and_calls_handed_off_when_it_has_left(device: PtyDevice) -> None:
    async def body() -> None:
        line = Line(device)
        await line.open(receiving=False)
        out: list[int] = []
        try:
            await line.transport.send(
                b"\x08\x90\xc0\x00\x00\x01\x00\xb6",
                confirm=True,
                handed_off=lambda: out.append(len(device.read_available())),
            )
        finally:
            await line.transport.close()
        assert out == [8]
        stats = line.transport.stats()
        assert (stats.tx_frames, stats.tx_bytes) == (1, 8)

    run(body())


def test_a_send_that_does_not_fit_the_buffer_is_refused_with_a_coded_error(
    device: PtyDevice,
) -> None:
    async def body() -> None:
        line = Line(device)
        await line.open(receiving=False)
        chunk = b"\xa5" * 4096
        try:
            sends = [
                asyncio.ensure_future(line.transport.send(chunk))
                for _ in range(TX_BUFFER_BYTES // 4096 + 12)
            ]
            await wait_for(
                lambda: any(s.done() for s in sends), "a refusal while the peer is not reading"
            )
            refused = [e for s in sends if s.done() and (e := s.exception()) is not None]
            assert refused and all(isinstance(e, IronbirdError) for e in refused)
            assert all(isinstance(e, IronbirdError) and e.code is Code.TX_SEND for e in refused)
            assert "refused" in str(refused[0])
            # the peer reads at last: the sends that were waiting finish
            drained = 0
            while not all(s.done() for s in sends):
                drained += len(device.read_available())
                await asyncio.sleep(0.005)
            ok = [s for s in sends if s.exception() is None]
            assert ok and drained > 0
        finally:
            await line.transport.close()

    run(body())


def test_bytes_that_do_not_leave_the_uart_in_time_are_reported_and_discarded(
    device: PtyDevice, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = threading.Event()
    flushed: list[int] = []
    monkeypatch.setattr(serial_module.termios, "tcdrain", lambda fd: release.wait(30))
    monkeypatch.setattr(serial_module.termios, "tcflush", lambda fd, how: flushed.append(how))
    monkeypatch.setattr(SerialTransport, "_drain_timeout", lambda self, pending: 0.1)

    async def body() -> None:
        line = Line(device)
        await line.open(receiving=False)
        try:
            with pytest.raises(IronbirdError) as info:
                await line.transport.send(b"\x08\x90\xc0\x00\x00\x01\x00\xb6", confirm=True)
            assert info.value.code is Code.TX_SEND and "did not leave the UART" in str(info.value)
            assert termios.TCOFLUSH in flushed  # pyserial also flushes input when it opens
        finally:
            release.set()
            await line.transport.close()

    run(body())


def test_a_peer_that_never_reads_makes_a_waiting_send_fail_not_hang(
    device: PtyDevice, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(SerialTransport, "_drain_timeout", lambda self, pending: 0.2)

    async def body() -> None:
        line = Line(device)
        await line.open(receiving=False)
        try:
            results = await asyncio.gather(
                *(line.transport.send(b"\xa5" * 4096) for _ in range(40)), return_exceptions=True
            )
        finally:
            await line.transport.close()
        failures = [r for r in results if isinstance(r, IronbirdError)]
        assert failures and all(f.code is Code.TX_SEND for f in failures)
        assert any("accepted no bytes" in str(f) for f in failures)

    run(body())


def test_a_send_after_close_says_the_transport_is_closed(device: PtyDevice) -> None:
    async def body() -> None:
        line = Line(device)
        await line.open(receiving=False)
        await line.transport.close()
        await line.transport.close()  # closing twice is fine
        with pytest.raises(TransportClosed):
            await line.transport.send(b"x")

    run(body())


def test_the_drain_timeout_scales_with_bytes_and_baud(device: PtyDevice) -> None:
    transport = Line(device).transport
    assert transport._drain_timeout(1) == DRAIN_FLOOR_S
    assert transport._drain_timeout(100_000) > transport._drain_timeout(10_000) > DRAIN_FLOOR_S


# -- losing and regaining the device ----------------------------------------------------------


def test_an_unplugged_device_puts_the_link_down_and_a_replug_brings_it_back(
    device: PtyDevice,
) -> None:
    async def body() -> None:
        line = Line(device)
        await line.open()
        try:
            device.write(hk(1))
            await wait_for(lambda: len(line.data()) == 1, "a packet before the unplug")
            device.unplug()
            await wait_for(lambda: [s[0] for s in line.states] == [False], "the link down event")
            _, down_ns, error = line.states[0]
            assert error is not None and error.code is Code.TX_DOWN and "reconnecting" in str(error)
            assert line.transport.is_down
            with pytest.raises(IronbirdError) as info:
                await line.transport.send(b"x")
            assert info.value.code is Code.TX_DOWN
            device.plug()
            await wait_for(lambda: [s[0] for s in line.states] == [False, True], "the link up")
            assert line.states[1][1] >= down_ns and not line.transport.is_down
            device.write(hk(2))
            await wait_for(lambda: len(line.data()) == 2, "a packet after the replug")
        finally:
            await line.transport.close()
        assert line.data() == [hk(1), hk(2)]

    run(body())


def test_what_was_read_before_the_unplug_is_delivered_before_the_link_down_event(
    device: PtyDevice,
) -> None:
    async def body() -> None:
        line = Line(device, idle_ms=10_000)
        order: list[str] = []
        await line.open()
        line.transport._link_state = lambda up, t, err: order.append(
            f"{'up' if up else 'down'} after {len(line.frames)} frame(s)"
        )
        try:
            device.write(hk(1) + b"\x00\x00\x00")
            await wait_for(lambda: line.transport.stats().rx_bytes > 0, "the read")
            assert line.frames == []  # pending: nothing proves the packet yet
            device.unplug()
            await wait_for(lambda: bool(order), "the link down event")
        finally:
            await line.transport.close()
        assert order == ["down after 2 frame(s)"]  # the packet and the three bytes after it
        assert line.data() == [hk(1)] and line.data(FLAG_GARBAGE) == [b"\x00\x00\x00"]

    run(body())


def test_a_partial_packet_cut_by_the_unplug_is_delivered_as_a_stale_frame(
    device: PtyDevice,
) -> None:
    async def body() -> None:
        line = Line(device, idle_ms=10_000)
        await line.open()
        try:
            device.write(hk(1)[:20])
            await wait_for(lambda: line.transport.stats().rx_bytes == 20, "the read")
            device.unplug()
            await wait_for(lambda: bool(line.states), "the link down event")
        finally:
            await line.transport.close()
        assert line.data(FLAG_STALE) == [hk(1)[:20]]

    run(body())


def test_closing_while_the_link_is_down_stops_the_reconnect(device: PtyDevice) -> None:
    async def body() -> None:
        line = Line(device)
        await line.open()
        device.unplug()
        await wait_for(lambda: bool(line.states), "the link down event")
        task = line.transport._reconnect
        assert task is not None
        await line.transport.close()
        assert task.done()
        assert [s[0] for s in line.states] == [False]  # no up after the close

    run(body())


def test_failed_reconnect_attempts_are_reported_rarely_and_say_why(device: PtyDevice) -> None:
    async def body() -> None:
        line = Line(device)
        await line.open()
        device.unplug()
        try:
            await wait_for(
                lambda: any("still down" in r.summary for r in line.reports), "a retry report"
            )
            await asyncio.sleep(0.4)  # many attempts at 0.02-0.1 s
        finally:
            await line.transport.close()
        still = [r for r in line.reports if "still down" in r.summary]
        assert 1 <= len(still) <= 4  # attempts 1, 2, 3 and every 12th
        assert "no such device" in still[0].summary

    run(body())


def test_a_handler_that_raises_is_reported_and_does_not_end_the_reconnect(
    device: PtyDevice,
) -> None:
    async def body() -> None:
        line = Line(device)

        def failing(up: bool, t: int, err: IronbirdError | None) -> None:
            raise RuntimeError("bug in a handler")

        line.transport._link_state = failing
        await line.open()
        device.unplug()
        try:
            await wait_for(
                lambda: any(r.code is Code.INTERNAL for r in line.reports), "the handler report"
            )
            device.plug()
            await wait_for(lambda: not line.transport.is_down, "the reconnect")
        finally:
            await line.transport.close()

    run(body())


# -- opening ----------------------------------------------------------------------------------


def test_a_missing_device_is_a_coded_error_that_says_what_to_check(tmp_path: Path) -> None:
    async def body() -> None:
        line = Line(PtyDevice(tmp_path / "gone"))
        line.device.unplug()
        with pytest.raises(IronbirdError) as info:
            await line.open()
        assert info.value.code is Code.TX_OPEN
        assert "check the cable" in str(info.value) and "/dev/serial/by-id" in str(info.value)
        assert line.states == []  # a device that never opened is not a link going down

    run(body())


def test_a_device_held_by_another_process_is_a_busy_error(device: PtyDevice) -> None:
    async def body() -> None:
        first = Line(device)
        await first.open()
        try:
            with pytest.raises(IronbirdError) as info:
                await Line(device).open()
            assert info.value.code is Code.TX_BUSY and "locked by another process" in str(
                info.value
            )
        finally:
            await first.transport.close()
        again = Line(device)  # released on close
        await again.open()
        await again.transport.close()

    run(body())


def test_a_path_that_is_not_a_serial_device_is_refused_as_such(tmp_path: Path) -> None:
    async def body() -> None:
        plain = tmp_path / "plain"
        plain.write_bytes(b"x")
        line = Line(PtyDevice(tmp_path / "unused"))
        line.transport._spec = type(line.transport._spec)(str(plain), 115200)
        with pytest.raises(IronbirdError) as info:
            await line.open()
        assert info.value.code is Code.TX_OPEN and "not a serial device" in str(info.value)

    run(body())


@pytest.mark.parametrize(
    ("code", "expected_code", "needle"),
    [
        (errno.ENOENT, Code.TX_OPEN, "check the cable"),
        (errno.EACCES, Code.TX_OPEN, "usually dialout"),
        (errno.EPERM, Code.TX_OPEN, "never needs root"),
        (errno.EAGAIN, Code.TX_BUSY, "locked by another process"),
        (errno.EBUSY, Code.TX_BUSY, "locked by another process"),
        (errno.ENOTTY, Code.TX_OPEN, "not a serial device"),
        (errno.EIO, Code.TX_OPEN, "not responding"),
        (errno.ENXIO, Code.TX_OPEN, "not responding"),
        (errno.EMFILE, Code.TX_OPEN, "Too many open files"),
    ],
)
def test_open_errors_map_to_coded_messages_with_a_remedy(
    code: int, expected_code: Code, needle: str
) -> None:
    for exc in (
        serial.SerialException(code, os.strerror(code)),
        OSError(code, os.strerror(code)),
    ):
        err = open_error("/dev/ttyUSB0", exc)
        assert err.code is expected_code and needle in err.summary
        assert "/dev/ttyUSB0" in err.summary and err.cause is exc


def test_the_errno_behind_a_pyserial_failure_is_found_through_its_cause() -> None:
    inner = termios.error(errno.ENOTTY, "Inappropriate ioctl for device")
    outer = serial.SerialException("Could not configure port: ...")
    outer.__context__ = inner
    assert errno_of(outer) == errno.ENOTTY
    assert errno_of(ValueError("x")) is None


# -- the registry ---------------------------------------------------------------------------


def _config(tmp_path: Path, device: Path) -> Path:
    return serial_config(tmp_path, "ground.toml", device, "ground")


def test_open_transport_builds_a_serial_transport_framed_for_the_end(
    tmp_path: Path, device: PtyDevice
) -> None:
    link = load_config(_config(tmp_path, device.path)).links[0]
    errors: list[IronbirdError] = []
    source = open_transport(link, LinkEnd.SOURCE, errors.append, codec=CODEC)
    destination = open_transport(link, LinkEnd.DESTINATION, errors.append, codec=CODEC)
    assert isinstance(source, SerialTransport) and isinstance(destination, SerialTransport)
    assert source.description == f"serial {device.path}" == destination.description
    assert source.facts()["baud"] == 115200 and source.facts()["max_idle_ms"] == 50
    assert source.local_address is None


def test_a_serial_link_cannot_be_built_without_the_codec_of_its_node(
    tmp_path: Path, device: PtyDevice
) -> None:
    link = load_config(_config(tmp_path, device.path)).links[0]
    with pytest.raises(IronbirdError) as info:
        open_transport(link, LinkEnd.SOURCE, lambda e: None)
    assert info.value.code is Code.INTERNAL


def test_a_header_profile_without_a_length_field_is_refused_when_the_link_is_built(
    tmp_path: Path, device: PtyDevice
) -> None:
    from ironbird import header

    data = tomllib.loads((EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml").read_text())
    for field in data["fields"]:
        if field.get("computed") == "length":
            field.pop("computed")
            field.pop("bias", None)
            field["value"] = 0
    no_length = header.parse_profile(data, "tlm-no-length.toml")
    link = load_config(_config(tmp_path, device.path)).links[0]
    broken = type(CODEC)(
        CODEC.model,
        CODEC.profile(ser_corpus.MessageKind.COMMAND),
        no_length,
        max_packet_bytes=ser_corpus.CAP,
    )
    with pytest.raises(IronbirdError) as info:
        open_transport(link, LinkEnd.SOURCE, lambda e: None, codec=broken)
    assert info.value.code is Code.CFG_LINK and "length field" in info.value.summary
