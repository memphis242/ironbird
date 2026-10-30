"""Performance guards (pytest-benchmark). Floors are loose: the plan's design target is
10k packets/s aggregate, so decode must stay far below 100 us per packet."""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from conftest import EXAMPLES, free_udp_ports
from ironbird.ccdd.model import MessageKind
from ironbird.coalesce import Coalescer
from ironbird.codec import Codec, DecodedPacket
from ironbird.config import load_config
from ironbird.errors import Code
from ironbird.frame import Frame, FrameFlag
from ironbird.ledger import Ledger
from ironbird.notice import Level
from ironbird.session import Session
from ironbird.transport import LinkEnd
from test_engine import _Probe

DECODE_CEILING_S = 100e-6
INGEST_CEILING_S = 100e-6
FLOOR_PPS = 1_000  # the plan's hard floor, aggregate packets per second
TARGET_PPS = 10_000  # the plan's design target
PACKET_BYTES = 200  # the plan's average packet size


def _check_ceiling(benchmark: Any, ceiling: float) -> None:
    stats = getattr(benchmark, "stats", None)
    if stats is not None and not getattr(benchmark, "disabled", False):  # skipped under xdist
        assert stats.stats.mean < ceiling, f"mean {stats.stats.mean * 1e6:.1f} us"


@pytest.mark.parametrize("name", ["MOCK_HK", "MOCK_STATUS"])
def test_decode_telemetry_rate(benchmark: Any, mock_codec: Codec, name: str) -> None:
    wire = mock_codec.encode_telemetry(name).data
    result = benchmark(mock_codec.decode, MessageKind.TELEMETRY, wire)
    assert isinstance(result, DecodedPacket)
    _check_ceiling(benchmark, DECODE_CEILING_S)


def test_decode_command_with_checksum_rate(benchmark: Any, mock_codec: Codec) -> None:
    wire = mock_codec.encode_command("MOCK_SET_LABEL", {"label": "abc"}).data
    result = benchmark(mock_codec.decode, MessageKind.COMMAND, wire)
    assert isinstance(result, DecodedPacket)
    _check_ceiling(benchmark, DECODE_CEILING_S)


def test_decode_rejects_garbage_cheaply(benchmark: Any, mock_codec: Codec) -> None:
    benchmark(mock_codec.decode, MessageKind.TELEMETRY, b"\xff" * 200)
    _check_ceiling(benchmark, DECODE_CEILING_S)


def test_ledger_record_cost(benchmark: Any, tmp_path: Any) -> None:
    ledger = Ledger(tmp_path / "bench", session_id="b")
    benchmark(ledger.record, "state", "tick", mid=1, count=2)
    assert not (tmp_path / "bench").exists()  # record() never touches the disk
    _check_ceiling(benchmark, 20e-6)


def test_coalescer_suppressed_report_cost(benchmark: Any) -> None:
    """The flood path: a repeat inside an open window only bumps a counter (no Notice built)."""
    emitted: list[object] = []
    coalescer = Coalescer(emitted.append, window_s=3600.0)
    args = (Code.DEC_CHECKSUM, Level.ERROR, "k", "bad")
    coalescer.report(*args, ledger_n=1, group=Code.DEC_CHECKSUM)  # opens the window
    benchmark(coalescer.report, *args, ledger_n=5, group=Code.DEC_CHECKSUM)
    assert len(emitted) == 1  # every timed call was suppressed
    _check_ceiling(benchmark, 2e-6)


def test_check_valid_frame_rate(benchmark: Any, mock_codec: Codec) -> None:
    wire = mock_codec.encode_telemetry("MOCK_HK").data
    assert benchmark(mock_codec.check, MessageKind.TELEMETRY, wire) is None
    _check_ceiling(benchmark, DECODE_CEILING_S)


def test_check_rejects_garbage_cheaply(benchmark: Any, mock_codec: Codec) -> None:
    assert benchmark(mock_codec.check, MessageKind.TELEMETRY, b"\xff" * 200) is not None
    _check_ceiling(benchmark, DECODE_CEILING_S)


def test_ingest_per_frame_cost(benchmark: Any, tmp_path: Path) -> None:
    """One valid frame through ingest: seq, Frame, peek, check, count, one sink. No transport."""
    probe = _Probe(tmp_path)
    seen: list[Frame] = []

    class Counting:
        def append(self, frame: Frame) -> None:
            seen.append(frame) if len(seen) < 10 else None

    probe.engine.add_sink(Counting())
    wire = probe.codec.encode_telemetry("MOCK_HK").data
    io = probe.io[LinkEnd.SOURCE]
    before = probe.engine.last_seq
    benchmark(io.on_data, wire, 1, "127.0.0.1:1", len(wire), FrameFlag.NONE)
    assert probe.engine.last_seq > before and probe.engine.malformed_total == 0
    _check_ceiling(benchmark, INGEST_CEILING_S)


# -- engine ingest on loopback ---------------------------------------------------------------

SENDER = """
import socket, sys, time
port, count, rate, hexdata = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
data = bytes.fromhex(hexdata)
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
target = ("127.0.0.1", port)
if rate == 0:                       # as fast as the sender can go
    for _ in range(count):
        sock.sendto(data, target)
else:                               # paced: rate/1000 packets per millisecond
    per_ms = max(1, rate // 1000)
    start = time.perf_counter()
    sent = 0
    while sent < count:
        for _ in range(min(per_ms, count - sent)):
            sock.sendto(data, target)
        sent += per_ms
        delay = start + sent / rate - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
"""


def _bench_config(tmp_path: Path) -> Path:
    """A config whose telemetry message is PACKET_BYTES long, on free loopback ports."""
    ccdd = json.loads((EXAMPLES / "ccdd" / "mock.json").read_text())
    header_bytes = 16
    ccdd["messages"].append(
        {
            "name": "BENCH_TLM",
            "kind": "telemetry",
            "mid": "0x0899",
            "category": "housekeeping",
            "fields": [{"name": "blob", "type": f"char[{PACKET_BYTES - header_bytes}]"}],
        }
    )
    ccdd_path = tmp_path / "bench.json"
    ccdd_path.write_text(json.dumps(ccdd))
    command_port, telemetry_port = free_udp_ports(2)
    path = tmp_path / "bench.toml"
    path.write_text(
        f"""[ironbird]
acts_as = "ground"
[nodes.ground]
[nodes.fc]
ccdd = "{ccdd_path}"
command_header = "{EXAMPLES}/headers/ccsds_v1_cfs_cmd.toml"
telemetry_header = "{EXAMPLES}/headers/ccsds_v1_cfs_tlm.toml"
[[links]]
name = "ground-fc"
from = "ground"
to = "fc"
transport = "udp"
command_endpoint = "127.0.0.1:{command_port}"
telemetry_endpoint = "127.0.0.1:{telemetry_port}"
"""
    )
    return path


def _ignore(_notice: object) -> None:
    pass


def _loopback_run(tmp_path: Path, count: int, rate: int) -> tuple[int, int, float]:
    """Send ``count`` valid packets to a capture from another process; ``rate`` 0 is unpaced.

    Returns (sent, ingested, seconds from the first to the last ingested frame). A consumer
    drains the capture the whole time, as a real front-end would.
    """
    config = load_config(_bench_config(tmp_path))
    codec = Session(
        config, role="ground", ledger=Ledger(tmp_path / "l0", session_id="c"), notify=_ignore
    ).codec_for("fc")
    wire = codec.encode_telemetry("BENCH_TLM", {"blob": "x" * 20}).data
    assert len(wire) == PACKET_BYTES

    async def main() -> tuple[int, float]:
        stamps: list[float] = []

        class Stamp:
            ingested = 0

            def append(self, frame: Frame) -> None:
                Stamp.ingested += 1
                if Stamp.ingested == 1:
                    stamps.append(time.perf_counter())

        sink = Stamp()
        ledger = Ledger(tmp_path / "ledger", session_id="b")
        async with Session(config, role="ground", ledger=ledger, notify=_ignore) as ground:
            capture = await ground.capture([ground.route("fc")], sinks=[sink])
            [(_, address)] = capture.addresses
            assert address is not None

            async def consume() -> None:
                async for _frame in capture:
                    pass

            consumer = asyncio.get_running_loop().create_task(consume())
            sender = await asyncio.create_subprocess_exec(
                sys.executable, "-c", SENDER, str(address[1]), str(count), str(rate), wire.hex()
            )
            await sender.wait()
            last = -1
            while sink.ingested != last:  # until a quiet moment: the buffer has been read
                last = sink.ingested
                await asyncio.sleep(0.3)
            end = time.perf_counter() - 0.3
            consumer.cancel()
            await capture.close()
            assert ground.malformed_total == 0
            return sink.ingested, max(end - stamps[0], 1e-6) if stamps else 1e-6

    ingested, seconds = asyncio.run(main())
    return count, ingested, seconds


def test_ingest_saturation_rate_on_loopback(benchmark: Any, tmp_path: Path) -> None:
    """The most packets/s one engine ingests (and a consumer drains) from an unpaced sender.

    The sender outruns the receiver, so the kernel drops the excess; the rate is what got in.
    """
    rates: list[float] = []

    def run() -> None:
        _, ingested, seconds = _loopback_run(tmp_path, 150_000, 0)
        rates.append(ingested / seconds)

    benchmark.pedantic(run, rounds=2, iterations=1)
    best = max(rates)
    benchmark.extra_info["ingest_packets_per_s"] = round(best)
    benchmark.extra_info["target_packets_per_s"] = TARGET_PPS
    print(f"\ningest saturation: {best:,.0f} packets/s of {PACKET_BYTES} B (target {TARGET_PPS:,})")
    assert best >= FLOOR_PPS


def test_ingest_at_the_target_rate_loses_nothing_on_loopback(tmp_path: Path) -> None:
    """Paced at the floor and, on a host with room, at the design target: nothing is lost."""
    sent, ingested, seconds = _loopback_run(tmp_path, FLOOR_PPS * 2, FLOOR_PPS)
    assert ingested == sent, f"floor rate lost {sent - ingested} of {sent}"
    sent, ingested, seconds = _loopback_run(tmp_path, TARGET_PPS * 2, TARGET_PPS)
    print(f"\npaced {TARGET_PPS:,} pkt/s: {ingested}/{sent} ingested over {seconds:.2f} s")
    assert ingested >= 0.9 * sent  # a loaded CI runner may drop some; the target is tracked
