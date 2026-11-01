"""Loopback blaster: sends a fixed number of UDP datagrams at a fixed rate, each a copy of one
template whose little-endian ``uint32`` at ``--serial-offset`` is the datagram's index (0, 1, ...).

Run as a script (``python blaster.py --port P --rate R --count N --template-hex H
--serial-offset O``) from the throughput gate. The schedule is absolute, ``index / rate`` after the
start, so a datagram late by a scheduler hiccup is followed by catch-up bursts instead of
shifting every later one, and the run takes ``count / rate`` seconds whenever the host keeps up.
No more than ``MAX_BATCH`` datagrams go out between two sleeps (the next ones wait a tick), so a
blaster that was descheduled catches up at a bounded rate and its bursts stay within what the
receiver's socket buffer holds.
One JSON object is printed at the end: ``sent``, ``elapsed_s`` and ``max_lag_ms`` (how far
behind its slot the latest datagram ever went out).
"""

from __future__ import annotations

import argparse
import json
import socket
import struct
import sys
import time

TICK_S = 0.001  # datagrams are sent in batches, one batch per tick of the schedule
# At most this many datagrams per tick (64k/s): about 50 KiB of kernel memory, within the 208 KiB
# socket buffer a runner may cap net.core.rmem_max at.
MAX_BATCH = 64


def blast(
    port: int, rate: int, count: int, template: bytes, serial_offset: int
) -> dict[str, float]:
    assert rate > 0 and count > 0
    assert 0 <= serial_offset <= len(template) - 4
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    target = ("127.0.0.1", port)
    packet = bytearray(template)
    pack_into = struct.pack_into
    send = sock.sendto
    clock = time.perf_counter
    sleep = time.sleep
    sent = 0
    burst = 0  # datagrams sent since the last sleep, never more than MAX_BATCH
    max_lag = 0.0
    start = clock()
    while sent < count:
        now = clock() - start
        due = min(count, int(now * rate) + 1)
        if due > sent:
            max_lag = max(max_lag, now - sent / rate)
            batch_end = min(due, sent + MAX_BATCH - burst)
            for index in range(sent, batch_end):
                pack_into("<I", packet, serial_offset, index)
                send(packet, target)
            burst += batch_end - sent
            sent = batch_end
            if burst >= MAX_BATCH:  # the rest of what is due goes out after the next tick
                sleep(TICK_S)
                burst = 0
        else:
            sleep(min(TICK_S, max(0.0, sent / rate - now)))
            burst = 0
    elapsed = clock() - start
    sock.close()
    return {"sent": sent, "elapsed_s": elapsed, "max_lag_ms": max_lag * 1000.0}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--rate", type=int, required=True, help="datagrams per second")
    parser.add_argument("--count", type=int, required=True, help="datagrams to send")
    parser.add_argument("--template-hex", required=True)
    parser.add_argument("--serial-offset", type=int, required=True)
    args = parser.parse_args()
    template = bytes.fromhex(args.template_hex)
    if args.rate < 1 or args.count < 1 or not 0 <= args.serial_offset <= len(template) - 4:
        parser.error("rate and count must be positive and the serial must fit in the template")
    print(json.dumps(blast(args.port, args.rate, args.count, template, args.serial_offset)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
