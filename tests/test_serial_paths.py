"""Serial transport failure paths on a pty: read and write errors that mean the device is gone
versus ones that do not, a stalled writer, and links with no state handler.

Faults are injected by wrapping ``os.read`` / ``os.write`` for the transport's own descriptor
only, so the event loop and everything else keep the real calls.
"""

from __future__ import annotations

import asyncio
import errno
import os
import select
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from ironbird.errors import Code, IronbirdError
from ironbird.transport import serial as serial_module
from serialutil import Line, hk, wait_for
from support.ptybridge import PtyDevice


@pytest.fixture
def device(tmp_path: Path) -> Iterator[PtyDevice]:
    dev = PtyDevice(tmp_path / "tty")
    yield dev
    dev.close()


def run(coro: object) -> None:
    asyncio.run(asyncio.wait_for(coro, 60))  # type: ignore[arg-type]


def fail_on(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    fd_of: Callable[[], int],
    errors: list[BaseException],
) -> None:
    """Make ``os.<name>`` raise the queued errors, one per call, for the transport's fd only."""
    real = getattr(os, name)

    def wrapper(fd: int, *args: Any) -> Any:
        if fd == fd_of() and errors:
            raise errors.pop(0)
        return real(fd, *args)

    monkeypatch.setattr(os, name, wrapper)


# -- reading --


def test_a_read_error_that_is_not_an_unplug_still_puts_the_link_down_and_it_recovers(
    device: PtyDevice, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def body() -> None:
        line = Line(device)
        await line.open()
        try:
            errors: list[BaseException] = [OSError(errno.EIO, "Input/output error")]
            fail_on(monkeypatch, "read", lambda: line.transport._fd, errors)
            device.write(hk(1))
            await wait_for(lambda: len(line.states) >= 2, "link down, then back up")
            assert [up for up, _, _ in line.states[:2]] == [False, True]
            down = line.states[0][2]
            assert down is not None and down.code is Code.TX_DOWN and "EIO" in down.summary
            assert line.transport.stats().rx_errors == 1
            device.write(hk(2))
            await wait_for(lambda: hk(2) in line.data(), "traffic after the reconnect")
        finally:
            await line.transport.close()

    run(body())


def test_a_link_with_no_state_handler_reports_the_loss_as_a_coded_error(
    device: PtyDevice,
) -> None:
    async def body() -> None:
        line = Line(device, fast_reconnect=False)
        line.transport._link_state = None
        await line.open()
        try:
            device.unplug()
            await wait_for(lambda: any(r.code is Code.TX_DOWN for r in line.reports), "the loss")
            assert line.transport.is_down
        finally:
            await line.transport.close()

    run(body())


# -- writing --


def test_a_write_to_a_vanished_device_puts_the_link_down(
    device: PtyDevice, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def body() -> None:
        line = Line(device, fast_reconnect=False)
        await line.open(receiving=False)
        try:
            fail_on(monkeypatch, "write", lambda: line.transport._fd, [OSError(errno.EIO, "gone")])
            with pytest.raises(IronbirdError) as info:
                await line.transport.send(hk(1))
            assert info.value.code is Code.TX_DOWN
            assert line.transport.is_down
            assert line.transport.stats().rx_errors == 1
            assert line.transport.stats().tx_frames == 0
        finally:
            await line.transport.close()

    run(body())


def test_a_write_the_device_refuses_is_a_send_error_and_the_link_stays_up(
    device: PtyDevice, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def body() -> None:
        line = Line(device)
        await line.open(receiving=False)
        try:
            error = OSError(errno.ENOSPC, "No space left on device")
            fail_on(monkeypatch, "write", lambda: line.transport._fd, [error])
            with pytest.raises(IronbirdError) as info:
                await line.transport.send(hk(1))
            assert info.value.code is Code.TX_SEND and info.value.cause is error
            assert not line.transport.is_down
            await line.transport.send(hk(2), confirm=True)  # the accounting stayed consistent
            assert line.transport.stats().tx_frames == 1
            assert device.read_available() == hk(2)
        finally:
            await line.transport.close()

    run(body())


@pytest.mark.parametrize(
    ("error", "code", "link_down"),
    [
        (OSError(errno.EIO, "Input/output error"), Code.TX_DOWN, True),
        (OSError(errno.ENOSPC, "No space left on device"), Code.TX_SEND, False),
    ],
    ids=["device-lost", "write-refused"],
)
def test_a_buffered_send_fails_with_the_cause_when_the_writer_hits_an_error(
    device: PtyDevice,
    monkeypatch: pytest.MonkeyPatch,
    error: OSError,
    code: Code,
    link_down: bool,
) -> None:
    async def body() -> None:
        line = Line(device, fast_reconnect=False)
        await line.open(receiving=False)
        try:
            # The first write on the device would block, so the send is buffered; the write the
            # loop's writer callback then makes fails with `error`.
            real_write = os.write
            calls: list[int] = []

            def writer_fails(fd: int, data: Any) -> int:
                if fd != line.transport._fd:
                    return real_write(fd, data)
                calls.append(fd)
                if len(calls) == 1:
                    raise BlockingIOError(errno.EAGAIN, "x")
                raise error

            monkeypatch.setattr(os, "write", writer_fails)
            with pytest.raises(IronbirdError) as info:
                await line.transport.send(hk(1))
            assert info.value.code is code
            assert line.transport.is_down is link_down
        finally:
            monkeypatch.undo()
            await line.transport.close()

    run(body())


def test_a_writer_that_would_block_again_keeps_the_send_waiting_until_the_device_drains(
    device: PtyDevice, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def body() -> None:
        line = Line(device)
        await line.open(receiving=False)
        try:
            real_write = os.write
            blocked = [BlockingIOError(errno.EAGAIN, "x"), BlockingIOError(errno.EAGAIN, "x")]

            def write(fd: int, data: Any) -> int:
                if fd == line.transport._fd and blocked:
                    raise blocked.pop(0)
                return real_write(fd, data)

            monkeypatch.setattr(os, "write", write)
            await line.transport.send(hk(3))  # blocks twice (send, first writer wake), then goes
            assert not blocked
            assert line.transport.stats().tx_frames == 1
            assert device.read_available() == hk(3)
        finally:
            monkeypatch.undo()
            await line.transport.close()

    run(body())


# -- the hang-up probe --


def test_a_descriptor_the_kernel_will_not_poll_counts_as_gone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Failing:
        def register(self, fd: int, mask: int) -> None:
            pass

        def poll(self, timeout: float) -> list[tuple[int, int]]:
            raise OSError(errno.EBADF, "bad descriptor")

    monkeypatch.setattr(select, "poll", Failing)
    assert serial_module._hung_up(0) is True


def test_a_descriptor_that_is_not_a_terminal_counts_as_gone() -> None:
    read_end, write_end = os.pipe()
    try:
        assert serial_module._hung_up(read_end) is True  # a pipe has no terminal attributes
    finally:
        os.close(read_end)
        os.close(write_end)


def test_a_live_terminal_is_not_hung_up(device: PtyDevice) -> None:
    fd = os.open(str(device.path), os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
    try:
        assert serial_module._hung_up(fd) is False
    finally:
        os.close(fd)


# -- group lookups in the permission advice --


def test_a_device_owned_by_an_unknown_user_and_group_still_gets_advice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import grp
    import pwd

    path = tmp_path / "tty"
    path.write_bytes(b"")
    path.chmod(0o600)

    def no_group(_gid: int) -> Any:
        raise KeyError("no such group")

    def no_user(_uid: int) -> Any:
        raise KeyError("no such user")

    monkeypatch.setattr(grp, "getgrgid", no_group)
    monkeypatch.setattr(pwd, "getpwuid", no_user)
    error = serial_module.open_error(str(path), PermissionError(errno.EACCES, "Permission denied"))
    assert error.code is Code.TX_OPEN
    assert str(path) in error.summary
