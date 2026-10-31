"""Serial transport: a UART or USB serial device driven by the asyncio selector.

pyserial opens and configures the port (line settings, custom baud rates, raw mode, and
``exclusive=True``, which is an ``flock`` on the device). From then on the event loop drives the
file descriptor itself with ``add_reader``/``add_writer`` and non-blocking ``os.read``/
``os.write``, like the UDP transport: no thread reads the device. One device carries both
directions of the link.

Receiving: each read goes to a :class:`~ironbird.transport.framing.Framer`, which cuts the stream
into packets, garbage runs and stale partial packets; every one of them is handed to the receiver
as a frame, stamped when the read that completed it returned (a USB adapter adds its own latency
timer, 16 ms for an FTDI by default; that is documented, not corrected). A timer on the loop
expires what the line leaves idle.

Sending goes through a bounded buffer (``TX_BUFFER_BYTES``): bytes the device does not take at
once wait for the writable callback, and a send that cannot fit is refused. ``confirm`` also waits
for the bytes to leave the UART (``tcdrain``, in a helper thread that blocks the shutdown signals,
with a timeout from the byte count and the baud rate).

Losing the device (``EIO``/``ENXIO`` and the like on read or write, or end of file: an unplugged
adapter) puts the link down, not the command. The pending bytes are flushed as frames, the port
is closed, ``IB-TX-008`` is reported, and the transport reopens the device with a backoff of
``RECONNECT_MIN_S`` to ``RECONNECT_MAX_S`` until it succeeds or the transport is closed. The link
state handler is told of each change so the engine can ledger it and record the gap. A device that
cannot be opened at first is a coded error and nothing is retried.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import grp
import os
import pwd
import select
import stat
import termios
import threading
import time
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Final

import serial

from ironbird.config import DATA_BITS, PARITIES, STOP_BITS, FramingSpec, SerialLinkSpec
from ironbird.errors import Code, IronbirdError
from ironbird.ledgergate import start_thread
from ironbird.transport.base import (
    LinkStateHandler,
    Receiver,
    TransportClosed,
    TransportStats,
)
from ironbird.transport.framing import Emit, Framer

READ_BYTES: Final = 4096  # most one read takes from the device
_HANGUP_EVENTS: Final = select.POLLHUP | select.POLLERR | select.POLLNVAL
READS_PER_WAKEUP: Final = 16  # reads before the loop gets a turn (level-triggered: called again)
TX_BUFFER_BYTES: Final = 64 * 1024
RECONNECT_MIN_S: Final = 0.5
RECONNECT_MAX_S: Final = 5.0
DRAIN_FLOOR_S: Final = 1.0  # the least a confirm waits for the UART to empty
RESYNC_SAMPLES: Final = 4  # skipped runs whose first bytes a notice's details show
REPORT_EVERY_ATTEMPTS: Final = 12  # a failed reconnect attempt is reported 1, 2, 3 and every 12th
# Errors that mean the device is gone, not that one call failed.
_LOST_ERRNOS: Final = frozenset(
    {errno.EIO, errno.ENXIO, errno.ENODEV, errno.EBADF, errno.EPIPE, errno.ESHUTDOWN}
)
_BUSY_ERRNOS: Final = frozenset({errno.EAGAIN, errno.EWOULDBLOCK, errno.EBUSY})
_DENIED_ERRNOS: Final = frozenset({errno.EACCES, errno.EPERM})

_PARITY: Final[Mapping[str, str]] = MappingProxyType(
    {
        "none": serial.PARITY_NONE,
        "even": serial.PARITY_EVEN,
        "odd": serial.PARITY_ODD,
        "mark": serial.PARITY_MARK,
        "space": serial.PARITY_SPACE,
    }
)
_DATA_BITS: Final[Mapping[int, int]] = MappingProxyType(
    {5: serial.FIVEBITS, 6: serial.SIXBITS, 7: serial.SEVENBITS, 8: serial.EIGHTBITS}
)
_STOP_BITS: Final[Mapping[float, float]] = MappingProxyType(
    {
        1.0: serial.STOPBITS_ONE,
        1.5: serial.STOPBITS_ONE_POINT_FIVE,  # Linux has no 1.5: the device sends 2
        2.0: serial.STOPBITS_TWO,
    }
)
assert set(_PARITY) == set(PARITIES) and set(_DATA_BITS) == set(DATA_BITS)
assert set(_STOP_BITS) == set(STOP_BITS), "every setting the config accepts has a pyserial value"


def errno_of(exc: BaseException) -> int | None:
    """The errno behind an exception pyserial or the OS raised (``None``: not an errno error).

    pyserial reports an ``OSError`` from ``open`` as a ``SerialException`` that carries the errno,
    and a ``termios.error`` (not a tty) as one that only chains it.
    """
    seen: BaseException | None = exc
    for _ in range(4):
        if seen is None:
            break
        if isinstance(seen, OSError) and seen.errno is not None:
            return seen.errno
        if isinstance(seen, termios.error) and seen.args and isinstance(seen.args[0], int):
            return seen.args[0]
        seen = seen.__cause__ or seen.__context__
    return None


def _denied_advice(device: str) -> str:
    """What to do about a device the user may not open, from who owns it and who the user is.

    Group membership only helps when the device grants its group read and write; a user who was
    added to the group after logging in needs a new login before the process has it.
    """
    try:
        info = os.stat(device)
    except OSError:
        return "ask an admin to give your user access to the device (usually group dialout)"
    try:
        group = grp.getgrgid(info.st_gid)
        group_name, members = group.gr_name, group.gr_mem
    except KeyError:
        group_name, members = str(info.st_gid), []
    try:
        owner = pwd.getpwuid(info.st_uid).pw_name
    except KeyError:
        owner = str(info.st_uid)
    mode = stat.S_IMODE(info.st_mode)
    if mode & 0o060 != 0o060:
        return (
            f"the device is mode {mode:04o} owned by {owner}:{group_name}, so group membership "
            "cannot help; ask an admin to change its permissions (a udev rule)"
        )
    if info.st_gid in os.getgroups():
        return (
            f"your user is already in group {group_name}, which may use the device; ask an "
            "admin to check ACLs and mount options"
        )
    try:
        me = pwd.getpwuid(os.getuid()).pw_name
    except KeyError:
        me = ""
    if me and me in members:
        return (
            f"you were added to group {group_name} after this login; log in again so the "
            "new group takes effect"
        )
    return f"ask an admin to add your user to group {group_name}, then log in again"


_OPEN_DEVICES: Final[dict[int, SerialTransport]] = {}  # device number -> its transport
_OPEN_DEVICES_LOCK: Final = threading.Lock()


def open_error(device: str, exc: BaseException) -> IronbirdError:
    """The coded error for a device that could not be opened, with what to do about it."""
    code = errno_of(exc)
    reason = getattr(exc, "strerror", None) or str(exc)
    if code in _BUSY_ERRNOS:
        return IronbirdError(
            Code.TX_BUSY,
            f"serial {device} is locked by another process (another ib command or a terminal "
            "program); close it, then retry",
            cause=exc,
        )
    if code in _DENIED_ERRNOS:
        return IronbirdError(
            Code.TX_OPEN,
            f"cannot open serial {device}: permission denied; {_denied_advice(device)}; "
            "ironbird never needs root",
            cause=exc,
        )
    match code:
        case errno.ENOENT:
            advice = "no such device; check the cable, and list /dev/serial/by-id"
        case errno.ENOTTY | errno.EINVAL:
            advice = "that is not a serial device; fix the device path in the config"
        case errno.ENODEV | errno.ENXIO | errno.EIO:
            advice = "the device is not responding; check the cable and the adapter"
        case _:
            advice = reason
    return IronbirdError(Code.TX_OPEN, f"cannot open serial {device}: {advice}", cause=exc)


class SerialTransport:
    """One serial link end (see the module docstring)."""

    def __init__(
        self,
        spec: SerialLinkSpec,
        framing: FramingSpec,
        framer: Framer,
        *,
        report: Callable[[IronbirdError], None] | None = None,
        link_state: LinkStateHandler | None = None,
        max_packet_bytes: int,
        reconnect_min_s: float | None = None,
        reconnect_max_s: float | None = None,
        wall_ns: Callable[[], int] = time.time_ns,
        mono_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        # The backoff defaults are the module constants as they are when the transport is built.
        if reconnect_min_s is None:
            reconnect_min_s = RECONNECT_MIN_S
        if reconnect_max_s is None:
            reconnect_max_s = RECONNECT_MAX_S
        assert 0 < reconnect_min_s <= reconnect_max_s
        self._spec = spec
        self._framing = framing
        self._framer = framer
        self._report = report
        self._link_state = link_state
        self._cap = max_packet_bytes
        self._backoff = (reconnect_min_s, reconnect_max_s)
        self._wall_ns = wall_ns
        self._mono_ns = mono_ns
        self._receiver: Receiver | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._port: serial.Serial | None = None
        self._fd = -1
        self._rdev: int | None = None  # device number registered in _OPEN_DEVICES while open
        self._timer: asyncio.TimerHandle | None = None
        self._reconnect: asyncio.Task[None] | None = None
        self._txbuf = bytearray()
        self._tx_enqueued = 0  # bytes ever accepted for sending
        self._tx_written = 0  # bytes ever written to the device
        self._tx_waiters: list[tuple[int, asyncio.Future[None]]] = []
        self._writer_on = False
        self._opened = False
        self._closed = False
        self._down = False
        self._rx_frames = self._rx_bytes = self._rx_errors = 0
        self._tx_frames = self._tx_bytes = 0

    # -- Transport API: state ---------------------------------------------------------

    @property
    def description(self) -> str:
        return f"serial {self._spec.device}"

    @property
    def local_address(self) -> tuple[str, int] | None:
        return None

    @property
    def is_down(self) -> bool:
        """True while the device is lost and being reopened."""
        return self._down

    def stats(self) -> TransportStats:
        return TransportStats(
            rx_frames=self._rx_frames,
            rx_bytes=self._rx_bytes,
            tx_frames=self._tx_frames,
            tx_bytes=self._tx_bytes,
            rx_errors=self._rx_errors,
            ccdd_mismatched=self._framer.stats().mismatched,
        )

    def facts(self) -> Mapping[str, object]:
        spec, framing = self._spec, self._framing
        return MappingProxyType(
            {
                "device": spec.device,
                "baud": spec.baud,
                "data_bits": spec.data_bits,
                "parity": spec.parity,
                "stop_bits": spec.stop_bits,
                "flow": spec.flow,
                "max_packet_bytes": self._cap,
                "framing": framing.kind,
                "sync": framing.sync.hex(),
                "max_idle_ms": framing.max_idle_ms,
                "require_known_mid": framing.require_known_mid,
                "tx_buffer_bytes": TX_BUFFER_BYTES,
            }
        )

    async def poll(self) -> None:
        """Nothing to refresh: every counter is kept in process."""

    # -- opening ----------------------------------------------------------------------

    def _open_port(self) -> serial.Serial:
        """Open and configure the device (non-blocking calls only). Raises a coded error."""
        spec = self._spec
        self._refuse_own_device()
        port = serial.Serial(
            port=None,
            baudrate=spec.baud,
            bytesize=_DATA_BITS[spec.data_bits],
            parity=_PARITY[spec.parity],
            stopbits=_STOP_BITS[spec.stop_bits],
            xonxoff=spec.flow == "xonxoff",
            rtscts=spec.flow == "rtscts",
            timeout=0,
            exclusive=True,
        )
        port.port = spec.device
        try:
            port.open()
        except (serial.SerialException, OSError, ValueError) as exc:
            with contextlib.suppress(Exception):
                port.close()
            raise open_error(spec.device, exc) from exc
        return port

    def _refuse_own_device(self) -> None:
        """Refuse a device another link of this process already holds, whatever path names it.

        The device lock would refuse it too, but as if another program held it; a symlink such as
        ``/dev/serial/by-id/...`` and the ``/dev/ttyUSB0`` it points to are one device.
        """
        try:
            info = os.stat(self._spec.device)
        except OSError:
            return  # the open reports it
        if not stat.S_ISCHR(info.st_mode):
            return
        with _OPEN_DEVICES_LOCK:
            holder = _OPEN_DEVICES.get(info.st_rdev)
        if holder is not None and holder is not self:
            raise IronbirdError(
                Code.TX_BUSY,
                f"{self.description} is the same device as {holder.description}, held by "
                "another link of this process; one device carries one link, so fix the "
                "device paths in the config",
            )

    def _register_device(self, port: serial.Serial) -> None:
        """Note which device number ``port`` is, so another link of this process is refused it."""
        try:
            info = os.fstat(port.fileno())
        except OSError:
            return
        with _OPEN_DEVICES_LOCK:
            _OPEN_DEVICES[info.st_rdev] = self
        self._rdev = info.st_rdev

    def _attach(self, port: serial.Serial) -> None:
        """Make ``port`` the live device: the loop watches its descriptor."""
        loop = self._loop
        assert loop is not None
        self._port, self._fd = port, port.fileno()
        self._register_device(port)
        try:
            if self._receiver is not None:
                loop.add_reader(self._fd, self._on_readable)
        except OSError as exc:
            self._detach(port)
            raise open_error(self._spec.device, exc) from exc

    def _detach(self, port: serial.Serial | None = None) -> None:
        """Stop watching the descriptor and close the port, whatever state it is in."""
        loop, fd = self._loop, self._fd
        port = port if port is not None else self._port
        if loop is not None and fd >= 0:
            with contextlib.suppress(ValueError, OSError):
                loop.remove_reader(fd)
            with contextlib.suppress(ValueError, OSError):
                loop.remove_writer(fd)
        self._writer_on = False
        self._port, self._fd = None, -1
        rdev, self._rdev = self._rdev, None
        if rdev is not None:
            with _OPEN_DEVICES_LOCK:
                if _OPEN_DEVICES.get(rdev) is self:
                    del _OPEN_DEVICES[rdev]
        if port is not None:
            with contextlib.suppress(Exception):
                port.close()

    async def open(self, receiver: Receiver | None) -> None:
        assert not self._opened and not self._closed, "transport already opened"
        self._opened = True
        self._loop = asyncio.get_running_loop()
        self._receiver = receiver
        self._attach(self._open_port())

    # -- receiving (event-loop thread) --------------------------------------------------

    def _deliver(self, emits: list[Emit]) -> None:
        receiver = self._receiver
        episodes = self._framer.take_episodes()
        if receiver is not None:
            for emit in emits:
                size = len(emit.data)
                self._rx_frames += 1
                receiver(emit.data, emit.time_ns, "", size, emit.flags)
        if episodes and self._report is not None:
            skipped = sum(e.skipped for e in episodes)
            samples = "; ".join(e.sample.hex(" ") for e in episodes[:RESYNC_SAMPLES])
            self._report(
                IronbirdError(
                    Code.TX_RESYNC,
                    f"{self.description}: resynchronised, {skipped} byte(s) of the stream skipped",
                    details=(
                        f"{len(episodes)} run(s) skipped, first bytes of up to {RESYNC_SAMPLES} "
                        f"of them: {samples}"
                    ),
                )
            )

    def _arm_idle_timer(self) -> None:
        loop, deadline = self._loop, self._framer.deadline_ns()
        if self._timer is not None or deadline is None or loop is None:
            return
        if self._closed or self._down:
            return
        delay = max(0.0, (deadline - self._mono_ns()) / 1e9)
        self._timer = loop.call_later(delay, self._on_idle)

    def _on_idle(self) -> None:
        self._timer = None
        if self._closed or self._down:
            return
        self._deliver(self._framer.expire(self._mono_ns(), self._wall_ns()))
        self._arm_idle_timer()

    def _on_readable(self) -> None:
        try:
            self._read_available()
        finally:
            self._arm_idle_timer()  # whatever ended the reads, a partial packet must expire

    def _read_available(self) -> None:
        """Read what the device has, up to ``READS_PER_WAKEUP`` reads.

        An empty read means the device closed only when it is the first of the wakeup, the loop
        having been told the descriptor is readable, and the line then confirms a hang-up. Later
        in the wakeup it means the device has nothing more: a tty set to return at once
        (``VMIN=0``) answers an empty queue with an empty read, not ``EAGAIN``.
        """
        fd = self._fd
        for attempt in range(READS_PER_WAKEUP):
            if fd < 0 or fd != self._fd:
                return  # lost while delivering
            try:
                chunk = os.read(fd, READ_BYTES)
            except (BlockingIOError, InterruptedError):
                return
            except OSError as exc:
                self._rx_errors += 1
                self._lost(exc)
                return
            if not chunk:
                if attempt == 0 and _hung_up(fd):
                    self._lost(None, "end of file: the device closed")
                return
            self._rx_bytes += len(chunk)
            self._deliver(self._framer.feed(chunk, self._wall_ns(), self._mono_ns()))
            if len(chunk) < READ_BYTES:
                return

    # -- losing and regaining the device -------------------------------------------------

    def _lost(self, exc: BaseException | None, reason: str | None = None) -> None:
        """The device went away: flush what was read, release the port, and start reopening."""
        if self._down or self._closed:
            return
        self._down = True
        now = self._wall_ns()
        if reason is None:
            code = errno_of(exc) if exc is not None else None
            name = errno.errorcode.get(code, "error") if code is not None else "error"
            text = getattr(exc, "strerror", None) or str(exc)
            reason = f"{text} ({name})"
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._deliver(self._framer.flush(now))
        self._detach()
        self._fail_sends(self._down_error(f"link lost: {reason}"))
        error = IronbirdError(
            Code.TX_DOWN,
            f"{self.description}: link lost ({reason}); reconnecting, retry every "
            f"{self._backoff[0]:g}-{self._backoff[1]:g} s",
            cause=exc,
        )
        self._announce(False, now, error)
        loop = self._loop
        assert loop is not None
        self._reconnect = loop.create_task(self._reopen(), name="serial-reconnect")

    def _announce(self, up: bool, time_ns: int, error: IronbirdError | None) -> None:
        """Tell the link state handler (it must not raise; a bug in it is reported, not fatal to
        the link)."""
        if self._link_state is None:
            if error is not None and self._report is not None:
                self._report(error)
            return
        try:
            self._link_state(up, time_ns, error)
        except Exception as exc:
            if self._report is not None:
                self._report(
                    IronbirdError(
                        Code.INTERNAL,
                        f"{self.description}: link state handler failed: "
                        f"{type(exc).__name__}: {exc}",
                        cause=exc,
                    )
                )

    def _down_error(self, why: str) -> IronbirdError:
        return IronbirdError(
            Code.TX_DOWN, f"{self.description}: not sent, the link is down ({why}); reconnecting"
        )

    async def _reopen(self) -> None:
        """Retry opening the device until it works or the transport is closed."""
        low, high = self._backoff
        delay, attempt = low, 0
        while not self._closed:
            await asyncio.sleep(delay)
            if self._closed:
                return
            attempt += 1
            try:
                self._attach(self._open_port())
            except (IronbirdError, OSError, serial.SerialException, ValueError) as raised:
                err = (
                    raised
                    if isinstance(raised, IronbirdError)
                    else open_error(self._spec.device, raised)
                )
                if self._report is not None and (
                    attempt <= 3 or attempt % REPORT_EVERY_ATTEMPTS == 0
                ):
                    self._report(
                        IronbirdError(
                            Code.TX_DOWN,
                            f"{self.description}: still down after {attempt} attempt(s): "
                            f"{err.summary}",
                            cause=err,
                        )
                    )
                delay = min(high, delay * 2)
                continue
            self._down = False
            self._reconnect = None
            self._announce(True, self._wall_ns(), None)
            return

    # -- sending ----------------------------------------------------------------------

    def _fail_sends(self, error: IronbirdError | TransportClosed) -> None:
        """Drop buffered bytes (they were never sent) and fail the sends waiting for them."""
        self._txbuf.clear()
        waiters, self._tx_waiters = self._tx_waiters, []
        for _, future in waiters:
            if not future.done():
                future.set_exception(error)
        self._tx_written = self._tx_enqueued

    def _drain_timeout(self, pending: int) -> float:
        """Seconds the line needs for ``pending`` bytes, doubled, with a floor."""
        seconds = pending * self._spec.bits_per_char / self._spec.baud
        return max(DRAIN_FLOOR_S, 2 * seconds + 0.5)

    def _write_lost(self, exc: OSError) -> IronbirdError:
        self._rx_errors += 1
        self._lost(exc)
        return self._down_error("write failed")

    def _on_writable(self) -> None:
        fd = self._fd
        if fd < 0:
            return
        if self._txbuf:
            try:
                written = os.write(fd, self._txbuf)
            except (BlockingIOError, InterruptedError):
                return
            except OSError as exc:
                if exc.errno in _LOST_ERRNOS:
                    self._lost(exc)
                else:  # the bytes cannot go out: refuse them rather than retry forever
                    self._fail_sends(
                        IronbirdError(
                            Code.TX_SEND,
                            f"send to {self.description} failed: {exc.strerror or exc}",
                            cause=exc,
                        )
                    )
                    self._stop_writer()
                return
            del self._txbuf[:written]
            self._tx_written += written
            self._tx_bytes += written
        self._release_waiters()
        if not self._txbuf:
            self._stop_writer()

    def _release_waiters(self) -> None:
        done = self._tx_written
        keep: list[tuple[int, asyncio.Future[None]]] = []
        for offset, future in self._tx_waiters:
            if offset <= done:
                if not future.done():
                    future.set_result(None)
            else:
                keep.append((offset, future))
        self._tx_waiters = keep

    def _stop_writer(self) -> None:
        if self._writer_on and self._loop is not None and self._fd >= 0:
            with contextlib.suppress(ValueError, OSError):
                self._loop.remove_writer(self._fd)
        self._writer_on = False

    async def send(
        self,
        data: bytes,
        *,
        confirm: bool = False,
        handed_off: Callable[[], None] | None = None,
    ) -> None:
        """Send one packet: the bytes are written to the device (``handed_off`` runs when they
        all are), and ``confirm`` also waits for the UART to empty."""
        if self._closed:
            raise TransportClosed
        if self._down or self._fd < 0:
            raise self._down_error("no device")
        size = len(data)
        if len(self._txbuf) + size > TX_BUFFER_BYTES:
            raise IronbirdError(
                Code.TX_SEND,
                f"send to {self.description} refused: {len(self._txbuf)} byte(s) are still "
                f"waiting in the {TX_BUFFER_BYTES}-byte buffer (is the peer reading? flow "
                "control?)",
            )
        loop, fd = self._loop, self._fd
        assert loop is not None
        self._tx_enqueued += size
        written = 0
        if not self._txbuf:
            try:
                written = os.write(fd, data)
            except (BlockingIOError, InterruptedError):
                written = 0
            except OSError as exc:
                self._tx_enqueued -= size
                if exc.errno in _LOST_ERRNOS:
                    raise self._write_lost(exc) from exc
                raise IronbirdError(
                    Code.TX_SEND,
                    f"send to {self.description} failed: {exc.strerror or exc}",
                    cause=exc,
                ) from exc
        self._tx_written += written
        self._tx_bytes += written
        if written < size:
            self._txbuf += data[written:]
            future: asyncio.Future[None] = loop.create_future()
            self._tx_waiters.append((self._tx_enqueued, future))
            if not self._writer_on:
                loop.add_writer(fd, self._on_writable)
                self._writer_on = True
            try:
                async with asyncio.timeout(self._drain_timeout(len(self._txbuf))):
                    await future
            except TimeoutError as exc:
                self._fail_sends(IronbirdError(Code.TX_SEND, f"send to {self.description} stalled"))
                self._stop_writer()
                raise IronbirdError(
                    Code.TX_SEND,
                    f"not sent: {self.description} accepted no bytes for "
                    f"{self._drain_timeout(size):.1f} s (is the peer reading? flow control?)",
                    cause=exc,
                ) from exc
        self._tx_frames += 1
        if handed_off is not None:
            handed_off()
        if confirm:
            await self._drain(size)

    async def _drain(self, pending: int) -> None:
        """Wait for the UART to send what was written (``tcdrain`` in a helper thread).

        The thread works on a duplicate of the descriptor, so closing the port (or the number
        being reused) while it runs cannot send it to another device. A drain that fails because
        the device went away puts the link down; any other failure is a send error.
        """
        loop, fd = self._loop, self._fd
        if loop is None or fd < 0:
            return
        try:
            helper = _DrainFd(fd)
        except OSError as exc:
            raise self._drain_failed(exc) from exc
        future: asyncio.Future[None] = loop.create_future()
        timeout = self._drain_timeout(pending)

        def drain() -> None:
            outcome: BaseException | None = None
            try:
                termios.tcdrain(helper.fd)
            except (termios.error, OSError) as exc:
                outcome = exc
            finally:
                helper.close()
            with contextlib.suppress(RuntimeError):  # the loop may be gone
                loop.call_soon_threadsafe(_finish, future, outcome)

        try:
            start_thread(threading.Thread(target=drain, name="serial-drain", daemon=True))
        except BaseException:
            helper.close()
            raise
        try:
            async with asyncio.timeout(timeout):
                await future
        except TimeoutError as exc:
            helper.flush()  # also wakes the thread
            raise IronbirdError(
                Code.TX_SEND,
                f"{self.description}: sent bytes did not leave the UART within {timeout:.1f} s "
                "(is the peer reading? flow control?); the rest was discarded",
                cause=exc,
            ) from exc
        except (termios.error, OSError) as exc:
            raise self._drain_failed(exc) from exc

    def _drain_failed(self, exc: BaseException) -> IronbirdError:
        """The coded error for a drain that failed with ``exc`` (a lost device ends the link)."""
        if errno_of(exc) in _LOST_ERRNOS:
            self._rx_errors += 1
            self._lost(exc)
            return IronbirdError(
                Code.TX_DOWN,
                f"{self.description}: not sent, the link is down (the device went away while "
                "the UART was sending); reconnecting",
                cause=exc,
            )
        reason = getattr(exc, "strerror", None) or str(exc)
        return IronbirdError(
            Code.TX_SEND,
            f"{self.description}: waiting for the UART to empty failed: {reason}",
            cause=exc,
        )

    # -- closing ---------------------------------------------------------------------

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        task, self._reconnect = self._reconnect, None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if not self._down:
            self._deliver(self._framer.flush(self._wall_ns()))  # the line's last bytes
        self._receiver = None
        self._detach()
        self._fail_sends(TransportClosed())


def _hung_up(fd: int) -> bool:
    """Whether the device behind ``fd`` is gone: the kernel reports a hang-up, or the descriptor
    no longer answers as a terminal."""
    poller = select.poll()
    poller.register(fd, select.POLLIN)  # POLLHUP, POLLERR and POLLNVAL are always reported
    try:
        events = poller.poll(0)
    except OSError:
        return True
    if any(mask & _HANGUP_EVENTS for _, mask in events):
        return True
    try:
        termios.tcgetattr(fd)
    except (termios.error, OSError):
        return True
    return False


class _DrainFd:
    """A duplicate of the port's descriptor for the helper thread that drains it.

    The thread closes it when its drain returns; ``flush`` (event-loop side) discards the output
    queue to wake a drain that never ends, and does nothing once the descriptor is closed.
    """

    def __init__(self, fd: int) -> None:
        self.fd = os.dup(fd)
        self._open = True
        self._lock = threading.Lock()

    def flush(self) -> None:
        with self._lock:
            if self._open:
                with contextlib.suppress(termios.error, OSError):
                    termios.tcflush(self.fd, termios.TCOFLUSH)

    def close(self) -> None:
        with self._lock:
            if self._open:
                self._open = False
                with contextlib.suppress(OSError):
                    os.close(self.fd)


def _finish(future: asyncio.Future[None], outcome: BaseException | None) -> None:
    if future.done():
        return
    if outcome is None:
        future.set_result(None)
    else:
        future.set_exception(outcome)


__all__ = ["SerialTransport", "errno_of", "open_error"]
