#!/usr/bin/env python3
"""Join a pseudo-terminal to cFS's UDP lab apps: a "serial" link to a real cFS.

    tools/cfs/serial_bridge.py --link /tmp/ib-cfs-uart
    ib -c examples/cfs-serial.toml listen fc

Commands ironbird writes to the pty are cut into packets and sent, one per datagram, to CI_LAB
(default 127.0.0.1:1234). Telemetry datagrams TO_LAB sends to UDP 2234 are written to the pty as a
continuous byte stream, back to back, as a UART would carry them.

The command direction is framed here with the CCSDS primary header's length field alone (the
packet is 7 + the length field bytes), written independently of ironbird's header-profile framer, so
the two disagree loudly if either is wrong. It is stdlib only and needs no privileges.

``--garbage-every N`` injects line noise into the telemetry stream after every N packets (with a
``--garbage-gap-ms`` silence on each side, so a packet with no checksum can be confirmed by the
reader): ironbird must report a resynchronisation and lose none of the packets around it.

Stop with Ctrl-C or SIGTERM; the symlink is removed on the way out.
"""

from __future__ import annotations

import argparse
import contextlib
import errno
import os
import pty
import selectors
import signal
import socket
import sys
import time
import tty

HEADER = 6  # CCSDS primary header
MIN_PACKET = 7  # the length field counts the bytes after the header, less one: total = 7 + field
MIN_COMMAND = 8  # a command has a primary header and a two-byte secondary header
COMMAND_MASK = 0xF8  # version 0, packet type command, secondary header present
COMMAND_BITS = 0x18
NOISE = bytes(range(0xF0, 0x100))  # no CCSDS version 0 header starts with these


def cut_packets(buf: bytearray, max_packet: int) -> tuple[list[bytes], int]:
    """Whole packets at the front of ``buf`` (removed from it), and the bytes dropped to find a
    plausible header: a cFS command's first byte is 0x18..0x1F, and its length is between the
    smallest command and ``max_packet``."""
    out: list[bytes] = []
    dropped = 0
    while len(buf) >= HEADER:
        total = MIN_PACKET + int.from_bytes(buf[4:6], "big")
        if buf[0] & COMMAND_MASK != COMMAND_BITS or not MIN_COMMAND <= total <= max_packet:
            del buf[0]
            dropped += 1
            continue
        if len(buf) < total:
            break
        out.append(bytes(buf[:total]))
        del buf[:total]
    return out, dropped


def parse_hostport(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    return host or "127.0.0.1", int(port)


class Bridge:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.master, self.holder = pty.openpty()
        tty.setraw(self.holder)  # a UART passes bytes through untouched
        os.set_blocking(self.master, False)
        self.slave_name = os.ttyname(self.holder)
        self.link = os.path.abspath(args.link)
        tmp = self.link + ".new"
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)
        os.symlink(self.slave_name, tmp)
        os.replace(tmp, self.link)
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        self.udp.bind(("127.0.0.1", args.to_lab_port))
        self.udp.setblocking(False)
        self.ci_lab = parse_hostport(args.ci_lab)
        self.commands = bytearray()
        self.telemetry = 0
        self.sent = 0
        self.dropped_in = 0
        self.dropped_out = 0
        self.running = True

    def close(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(self.link)
        for fd in (self.master, self.holder):
            with contextlib.suppress(OSError):
                os.close(fd)
        self.udp.close()

    def write_line(self, data: bytes) -> None:
        """Write to the pty without blocking forever: a reader that is absent loses the bytes
        once the pty buffer is full (a UART's fate too)."""
        view = memoryview(data)
        deadline = time.monotonic() + 1.0
        while view and time.monotonic() < deadline:
            try:
                view = view[os.write(self.master, view) :]
            except BlockingIOError:
                time.sleep(0.001)
            except OSError as exc:
                if exc.errno in (errno.EIO, errno.EBADF):
                    break
                raise
        self.dropped_out += len(view)

    def on_commands(self) -> None:
        try:
            chunk = os.read(self.master, 4096)
        except (BlockingIOError, OSError):
            return
        self.commands += chunk
        packets, dropped = cut_packets(self.commands, self.args.max_packet)
        self.dropped_in += dropped
        for packet in packets:
            self.udp.sendto(packet, self.ci_lab)
            self.sent += 1

    def on_telemetry(self) -> None:
        for _ in range(64):
            try:
                data, _ = self.udp.recvfrom(65536)
            except BlockingIOError:
                return
            every = self.args.garbage_every
            if every and self.telemetry and self.telemetry % every == 0:
                gap = self.args.garbage_gap_ms / 1000
                time.sleep(gap)
                self.write_line((NOISE * 4)[: self.args.garbage_bytes])
                time.sleep(gap)
            self.telemetry += 1
            self.write_line(data)

    def run(self) -> None:
        selector = selectors.DefaultSelector()
        selector.register(self.master, selectors.EVENT_READ, self.on_commands)
        selector.register(self.udp, selectors.EVENT_READ, self.on_telemetry)
        print(f"serial_bridge: {self.link} -> {self.slave_name} ready", flush=True)
        while self.running:
            for key, _ in selector.select(timeout=0.2):
                key.data()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--link", required=True, help="path of the symlink to the pty slave")
    parser.add_argument("--ci-lab", default="127.0.0.1:1234", help="where commands go (UDP)")
    parser.add_argument(
        "--to-lab-port", type=int, default=2234, help="UDP port telemetry is read on"
    )
    parser.add_argument("--max-packet", type=int, default=2048, help="longest command accepted")
    parser.add_argument("--garbage-every", type=int, default=0, help="noise after every N packets")
    parser.add_argument("--garbage-bytes", type=int, default=7, help="bytes of noise to inject")
    parser.add_argument("--garbage-gap-ms", type=int, default=120, help="silence around the noise")
    args = parser.parse_args()
    bridge = Bridge(args)

    def stop(*_: object) -> None:
        bridge.running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        bridge.run()
    finally:
        bridge.close()
        print(
            f"serial_bridge: {bridge.sent} command(s) sent, {bridge.telemetry} telemetry "
            f"packet(s) relayed, {bridge.dropped_in} byte(s) skipped in, "
            f"{bridge.dropped_out} byte(s) lost out",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
