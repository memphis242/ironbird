"""Stopping a UDP capture: what the socket holds at the stop is recorded, what arrives after it
is not a loss.

A capture stops at one instant: the close of its receiving transport hands the receiver what the
socket already holds (one read batch, as a wakeup would), stops reading, and counts what is left
as unread (``rx.unread_at_close``, a ``kernel_drop`` GAP for a recorder). Only then does it read
the kernel's drop counter, off the loop. A datagram that reaches the socket while that read is in
flight arrived after the stop: it is neither recorded nor a gap.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from ironbird.codec import Codec
from ironbird.config import Endpoint, load_config
from ironbird.engine import CaptureStats
from ironbird.errors import Code
from ironbird.ledger import Ledger
from ironbird.notice import Notice
from ironbird.session import Session
from ironbird.transport import LinkEnd, UdpTransport, udp
from recutil import write_config

# ``ib`` with the UDP stop under traffic: ``early`` datagrams reach the receiving socket right
# before its close starts (they arrived since the loop last read it), and ``late`` more while
# the close reads the kernel's drop counter (they arrive after the stop). The real-cFS failure
# this reproduces was one HK packet landing in that read.
CLI_WITH_TRAFFIC_AT_THE_STOP = r"""
import socket, sys
from ironbird.transport import udp
data, early, late = bytes.fromhex(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])

def deliver(transport, count):
    address = transport.local_address
    if address is None or not count:
        return
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        for _ in range(count):
            sock.sendto(data, address)

close = udp.UdpTransport.close

async def closing(self):
    deliver(self, early)
    await close(self)

scan = udp.scan_kernel_drops

async def scanning(transports):
    for transport in transports:
        if transport._closed:
            deliver(transport, late)
    await scan(transports)

udp.UdpTransport.close = closing
udp.scan_kernel_drops = scanning
from ironbird.cli.main import main
sys.argv = ["ib", *sys.argv[4:]]
sys.exit(main())
"""


def _ib(
    args: list[str], env: dict[str, str], data: bytes, early: int, late: int
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-c",
            CLI_WITH_TRAFFIC_AT_THE_STOP,
            data.hex(),
            str(early),
            str(late),
            *args,
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
        check=False,
    )


def _info(path: Path, env: dict[str, str]) -> dict[str, Any]:
    done = subprocess.run(
        [sys.executable, "-m", "ironbird.cli.main", "rec", "info", str(path), "--format", "json"],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    report: dict[str, Any] = json.loads(done.stdout)
    return report


def _ledger(state: Path, event: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted(state.glob("*.jsonl")):
        out += [json.loads(line) for line in path.read_text().splitlines()]
    return [e for e in out if e["event"] == event]


@pytest.mark.parametrize(("early", "late"), [(0, 1), (3, 0), (3, 2)])
def test_ib_record_stopped_under_traffic_is_a_clean_recording(
    tmp_path: Path, mock_codec: Codec, early: int, late: int
) -> None:
    config, _, _ = write_config(tmp_path)
    state = tmp_path / "state"
    env = {**os.environ, "IRONBIRD_CONFIG": str(config), "IRONBIRD_STATE_DIR": str(state)}
    out = tmp_path / "stop.ibrec"
    data = mock_codec.encode_telemetry("MOCK_HK", {"cmd_counter": 1}).data
    done = _ib(["record", "-o", str(out), "--duration", "0.5", "--quiet"], env, data, early, late)
    assert done.returncode == 0, done.stderr

    report = _info(out, env)
    gaps = report["gaps"]
    assert gaps["total"] == 0 and report["missing_frames"] == 0, (gaps, report["holes"])
    assert report["frames"] == early  # what the socket held at the stop; nothing after it
    assert _ledger(state, "rx.unread_at_close") == []


# -- the transport ----------------------------------------------------------------------------


def _run(coro: Any) -> Any:
    return asyncio.run(asyncio.wait_for(coro, 30))


def _send(address: tuple[str, int], count: int) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        for _ in range(count):
            sock.sendto(b"x" * 20, address)


def test_udp_close_delivers_one_batch_of_what_the_socket_holds_and_counts_the_rest() -> None:
    async def main() -> tuple[int, int]:
        received: list[bytes] = []
        transport = UdpTransport(local=Endpoint("127.0.0.1", 0))
        await transport.open(lambda data, *_: received.append(data))
        address = transport.local_address
        assert address is not None
        _send(address, udp.BATCH + 50)
        await transport.close()  # the loop never got to read them
        return len(received), transport.stats().unread_at_close

    assert _run(main()) == (udp.BATCH, 50)


def test_a_datagram_arriving_while_the_close_reads_the_drop_counter_is_not_unread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scan = udp.scan_kernel_drops

    async def main() -> tuple[int, int, int]:
        received: list[bytes] = []
        transport = UdpTransport(local=Endpoint("127.0.0.1", 0))
        await transport.open(lambda data, *_: received.append(data))
        address = transport.local_address
        assert address is not None

        async def scanning(transports: Any) -> None:
            _send(address, 4)  # after the stop
            await scan(transports)

        monkeypatch.setattr(udp, "scan_kernel_drops", scanning)
        _send(address, 2)  # before the stop
        await transport.close()
        return len(received), transport.stats().unread_at_close, transport.stats().rx_frames

    assert _run(main()) == (2, 0, 2)


# -- the engine: what the close hands over is counted for the consumer -------------------------


@pytest.mark.parametrize("closer", ["listener", "engine"])
def test_frames_the_close_hands_over_are_queued_for_the_consumer(
    mock_config_path: Path, tmp_path: Path, closer: str
) -> None:
    """``sent == shown + unread in the queue``: none of the datagrams buffered at the stop goes
    to no one, whether the consumer closes its capture or the engine shuts down under it."""
    notices: list[Notice] = []

    async def main() -> CaptureStats:
        ground = Session(
            load_config(mock_config_path),
            role="ground",
            ledger=Ledger(tmp_path / "ledger", session_id="stop"),
            notify=notices.append,
        )
        async with ground:
            data = ground.codec_for("fc").encode_telemetry("MOCK_HK").data
            listener = await ground.listen("fc")
            address = listener.local_address
            assert address is not None
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                for _ in range(5):
                    sock.sendto(data, address)
            if closer == "listener":
                await listener.close()
            else:
                await ground._engine.close()  # what the session's exit does, with no wait first
        return listener.capture.stats()

    stats = _run(main())
    assert (stats.frames, stats.unread, stats.not_shown) == (5, 5, 0)
    assert not [n for n in notices if n.code is Code.TX_OVERRUN], notices


def test_engine_close_of_a_multi_end_capture_queues_what_every_end_hands_over(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A capture's queue is shared by its link ends. Datagrams reach the second end after the
    first end's close finished (during that close's drop-counter read, so no loop turn in
    between); the second end's close hands them over, and the queue must still take them:
    ``sent == shown + unread in the queue``."""
    scan = udp.scan_kernel_drops
    count = 5

    async def main() -> tuple[CaptureStats, int, list[dict[str, Any]]]:
        ground = Session(
            load_config(mock_config_path),
            role="ground",
            ledger=Ledger(tmp_path / "ledger", session_id="stop"),
            notify=lambda _notice: None,
        )
        async with ground:
            data = ground.codec_for("fc").encode_telemetry("MOCK_HK").data
            link = ground.route("fc").link
            engine = ground._engine
            capture = await engine.open_capture(
                [(link, LinkEnd.SOURCE), (link, LinkEnd.DESTINATION)]
            )
            first, second = list(engine._ios.values())[:2]
            assert first.transport is not None and second.transport is not None
            address = second.transport.local_address
            assert address is not None
            scans = 0

            async def scanning(transports: Any) -> None:
                nonlocal scans
                await scan(transports)
                scans += 1
                if scans == 1:  # the first end has stopped; the second has not
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                        for _ in range(count):
                            sock.sendto(data, address)

            monkeypatch.setattr(udp, "scan_kernel_drops", scanning)
            await engine.close()
            monkeypatch.undo()
            stats = capture.stats()
            read = 0
            while batch := await capture.next_batch():
                read += len(batch)
            ledger = ground.ledger
            ledger.flush()
            records = [json.loads(line) for line in ledger.path.read_text().splitlines()]
        return stats, read, records

    stats, read, records = _run(main())
    assert (stats.frames, stats.unread, stats.not_shown, stats.after_close) == (count, count, 0, 0)
    assert read == count
    assert not [rec for rec in records if rec["event"] == "rx.closed_drops"], records
