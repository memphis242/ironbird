"""Pseudo-terminal pairs standing in for serial hardware, and a relay that joins two of them.

``PtyDevice`` is one end of a serial cable as the kernel sees it: a pty master the test holds and
a slave the code under test opens by path (through a stable symlink, so an unplugged device that
is plugged in again keeps its path, as ``/dev/serial/by-id/...`` does).

``PtyBridge`` is a cable between two such devices. A relay thread copies the bytes each master
receives to the other one, and the test can inject garbage into either side, drop or split
what is relayed, pace it at a baud rate, and unplug and re-plug a side. Every byte the relay
delivered to a side is logged, so a test can compare what a recording holds with what the line
carried.
"""

from __future__ import annotations

import contextlib
import errno
import os
import pty
import selectors
import threading
import time
import tty
from pathlib import Path

CHUNK = 4096


class PtyDevice:
    """One end of a serial cable: ``path`` is a symlink to the current slave."""

    def __init__(self, link: Path) -> None:
        self.path = link
        self._lock = threading.Lock()
        self._master = -1
        self._holder = -1  # a slave descriptor kept open so the master never sees a hang-up
        self.plug()

    @property
    def master(self) -> int:
        return self._master

    @property
    def plugged(self) -> bool:
        return self._master >= 0

    def plug(self) -> None:
        """Create a fresh pty and point ``path`` at its slave."""
        with self._lock:
            assert self._master < 0, "already plugged"
            master, slave = pty.openpty()
            tty.setraw(slave)
            name = os.ttyname(slave)
            os.set_blocking(master, False)
            self._master, self._holder = master, slave
            tmp = self.path.with_name(self.path.name + ".new")
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()
            tmp.symlink_to(name)
            tmp.replace(self.path)

    def unplug(self) -> None:
        """Close the master (the slave's reads then end and its writes fail with ``EIO``) and
        remove the path, as an unplugged adapter's device node goes away."""
        with self._lock:
            master, holder = self._master, self._holder
            self._master = self._holder = -1
        for fd in (master, holder):
            if fd >= 0:
                with contextlib.suppress(OSError):
                    os.close(fd)
        with contextlib.suppress(FileNotFoundError):
            self.path.unlink()

    def write(self, data: bytes) -> None:
        """Send ``data`` towards the code under test (it arrives on the slave)."""
        view = memoryview(data)
        deadline = time.monotonic() + 10
        while view:
            fd = self._master
            if fd < 0:
                return
            try:
                view = view[os.write(fd, view) :]
            except BlockingIOError:
                if time.monotonic() > deadline:
                    raise TimeoutError("the slave is not reading") from None
                time.sleep(0.001)
            except OSError as exc:
                if exc.errno in (errno.EIO, errno.EBADF):
                    return
                raise

    def read_available(self) -> bytes:
        """What the code under test has written and not yet been read; ``b''`` if nothing."""
        fd = self._master
        if fd < 0:
            return b""
        try:
            return os.read(fd, CHUNK)
        except (BlockingIOError, OSError):
            return b""

    def close(self) -> None:
        self.unplug()


class PtyBridge:
    """Two devices and the relay between them (see the module docstring).

    ``a`` and ``b`` are the two ends. ``log(side)`` is every byte the relay delivered to that
    end, in order. Faults apply to the bytes relayed *towards* a side and are consumed once.
    """

    def __init__(self, directory: Path, *, baud: int | None = None) -> None:
        self.a = PtyDevice(directory / "tty-a")
        self.b = PtyDevice(directory / "tty-b")
        self._baud = baud
        self._ends = {"a": self.a, "b": self.b}
        self._logs: dict[str, bytearray] = {"a": bytearray(), "b": bytearray()}
        self._drop: dict[str, int] = {"a": 0, "b": 0}
        self._split: dict[str, int] = {"a": 0, "b": 0}
        self._pause: dict[str, float] = {"a": 0.0, "b": 0.0}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="ptybridge", daemon=True)
        self._thread.start()

    def __enter__(self) -> PtyBridge:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # -- faults and observation ----------------------------------------------------------

    def log(self, side: str) -> bytes:
        with self._lock:
            return bytes(self._logs[side])

    def drop_next(self, side: str, count: int) -> None:
        """Discard the next ``count`` bytes relayed towards ``side``."""
        with self._lock:
            self._drop[side] += count

    def split_into(self, side: str, size: int) -> None:
        """Deliver bytes towards ``side`` in pieces of ``size`` with a short pause between."""
        with self._lock:
            self._split[side] = size

    def pause_after(self, side: str, seconds: float) -> None:
        """Hold the relay for ``seconds`` before the next delivery towards ``side``."""
        with self._lock:
            self._pause[side] = seconds

    def inject(self, side: str, data: bytes) -> None:
        """Put ``data`` on the line towards ``side`` as if the peer had sent it."""
        self._deliver(side, data)

    def unplug(self, side: str) -> None:
        self._ends[side].unplug()

    def plug(self, side: str) -> None:
        self._ends[side].plug()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self.a.close()
        self.b.close()

    # -- the relay -----------------------------------------------------------------------

    def _deliver(self, side: str, data: bytes) -> None:
        with self._lock:
            dropped = min(self._drop[side], len(data))
            self._drop[side] -= dropped
            data = data[dropped:]
            piece = self._split[side]
            pause, self._pause[side] = self._pause[side], 0.0
        if pause:
            time.sleep(pause)
        end = self._ends[side]
        for start in range(0, len(data), piece or max(1, len(data))):
            part = data[start : start + piece] if piece else data
            if not part:
                continue
            if self._baud:
                time.sleep(len(part) * 10 / self._baud)
            if not end.plugged:
                return  # nothing is there to receive it: lost, and not logged as delivered
            with self._lock:
                self._logs[side] += part
            end.write(part)
            if piece:
                time.sleep(0.002)

    def _run(self) -> None:
        with selectors.DefaultSelector() as selector:
            registered: dict[str, int] = {}
            while not self._stop.is_set():
                for side, end in self._ends.items():
                    fd = end.master
                    if registered.get(side, -1) != fd:
                        if registered.get(side, -1) >= 0:
                            with contextlib.suppress(KeyError, ValueError, OSError):
                                selector.unregister(registered[side])
                        registered[side] = -1
                        if fd >= 0:
                            try:
                                selector.register(fd, selectors.EVENT_READ, side)
                                registered[side] = fd
                            except (ValueError, OSError):
                                registered[side] = -1
                for key, _ in selector.select(timeout=0.02):
                    side = str(key.data)
                    other = "b" if side == "a" else "a"
                    try:
                        data = os.read(key.fd, CHUNK)
                    except (BlockingIOError, OSError):
                        continue
                    if data:
                        self._deliver(other, data)
