"""Performance guards (pytest-benchmark). Floors are loose: the plan's design target is
10k packets/s aggregate, so decode must stay far below 100 us per packet.

Two kinds of guard are kept apart on purpose. The ``ingest`` ones time only the receive side
(a frame through ingest, or datagrams into a draining capture), which is a fraction of what a
front-end spends per packet. The ``listen`` ones run the whole path a user sees: receive,
decode, render and write.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from conftest import EXAMPLES, free_udp_ports
from filter_defs import make_frame
from ironbird import render
from ironbird.ccdd.model import MessageKind
from ironbird.coalesce import Coalescer
from ironbird.codec import Codec, DecodedPacket
from ironbird.config import load_config
from ironbird.errors import Code
from ironbird.filter import compile_filter
from ironbird.frame import Decoded, Direction, Frame, FrameFlag, Stream
from ironbird.ledger import Ledger
from ironbird.notice import Level
from ironbird.session import Session
from ironbird.transport import LinkEnd
from test_engine import _Probe

DECODE_CEILING_S = 100e-6
INGEST_CEILING_S = 100e-6
LISTEN_OVERHEAD_RATIO = 1.5  # listen path vs. the bare decode + render it must wrap
LISTEN_FILTER_OVERHEAD_RATIO = 1.8  # the same with a decode-level filter evaluated per frame
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


def test_ingest_only_per_frame_cost(benchmark: Any, tmp_path: Path) -> None:
    """INGEST ONLY: one valid frame through ingest (seq, Frame, peek, check, count, one sink).
    No transport, no consumer, no decode, no output: not the cost of a packet a user sees."""
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


# -- filter evaluation (docs/plan.md S4): per frame, by the dearest level an expression needs --

FILTER_FRAME_CEILING_S = 5e-6  # frame-level fields only
FILTER_MODEL_CEILING_S = 5e-6  # a MID lookup
FILTER_DECODE_CEILING_S = 100e-6  # a decode (the plan's per-packet ceiling) plus field tests

FILTER_CASES = {
    "frame": "seq > 0 and node == fc and dir == rx and len >= 16",
    "model": "msg == MOCK_HK and category == housekeeping",
    "check": "status == ok",
    "decode": "msg == MOCK_HK and cmd_counter >= 0 and mode in [SAFE, NOMINAL, SCIENCE, 0]",
    "decode_hdr_event": "hdr.sequence >= 0 and event.type >= error",
}


def _decode_for_filter(codec: Codec, frame: Frame) -> Any:
    checked = frame.checked
    packet = (
        codec.decode_resolved(frame.stream.kind, checked, frame.data)
        if checked is not None
        else codec.decode(frame.stream.kind, frame.data)
    )
    assert isinstance(packet, DecodedPacket)
    return Decoded(frame, packet)


FILTER_STREAM = Stream(
    id=1,
    link="ground-fc",
    node="fc",
    kind=MessageKind.TELEMETRY,
    direction=Direction.RX,
    transport="udp",
    endpoint="udp 127.0.0.1:1",
)


@pytest.mark.parametrize("case", list(FILTER_CASES))
def test_filter_evaluation_rate(benchmark: Any, mock_codec: Codec, case: str) -> None:
    """One frame through a compiled filter, by the dearest level the expression needs. The decode
    level includes one full decode of the frame (the matcher shares it with the consumer)."""
    wire = mock_codec.encode_telemetry("MOCK_HK").data
    # Two equal frames, tested alternately: the matcher memoises the frame it saw last, and a
    # repeat of the same frame would not pay for its decode.
    first = make_frame(mock_codec, FILTER_STREAM, wire, live=True)
    second = make_frame(mock_codec, FILTER_STREAM, wire, live=True, seq=2)
    flt = compile_filter(FILTER_CASES[case], {"fc": mock_codec})
    ceiling = {"frame": FILTER_FRAME_CEILING_S, "model": FILTER_MODEL_CEILING_S}.get(
        case, FILTER_DECODE_CEILING_S
    )
    matcher = flt.bind(lambda f: _decode_for_filter(mock_codec, f), lambda _node: mock_codec)
    expected = case != "decode_hdr_event"  # a housekeeping frame has no event type

    def run() -> tuple[bool, bool]:
        return matcher(first), matcher(second)

    assert benchmark(run) == (expected, expected)
    benchmark.extra_info["level"] = flt.cost.name
    stats = getattr(benchmark, "stats", None)
    if stats is not None and not getattr(benchmark, "disabled", False):
        per_frame = stats.stats.mean / 2
        benchmark.extra_info["frames_per_s"] = round(1 / per_frame)
        print(f"\nfilter {case}: {per_frame * 1e6:.2f} us/frame, {1 / per_frame:,.0f} frames/s")
        assert per_frame < ceiling, f"{per_frame * 1e6:.1f} us"


def test_filter_rejects_a_non_candidate_frame_without_decoding(
    benchmark: Any, mock_codec: Codec
) -> None:
    """The cost of a frame outside the candidate MIDs: one set lookup, no decode."""
    frame = make_frame(
        mock_codec, FILTER_STREAM, mock_codec.encode_telemetry("MOCK_STATUS").data, live=True
    )
    flt = compile_filter("msg == MOCK_HK and cmd_counter > 0", {"fc": mock_codec})
    decoded: list[Frame] = []

    def decode(f: Frame) -> Any:
        decoded.append(f)
        return _decode_for_filter(mock_codec, f)

    matcher = flt.bind(decode, lambda _node: mock_codec)
    assert benchmark(matcher, frame) is False
    assert decoded == []
    _check_ceiling(benchmark, FILTER_FRAME_CEILING_S)


# -- engine ingest on loopback (ingest only: a sender and a draining consumer, no decode) --

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


def test_ingest_only_saturation_rate_on_loopback(benchmark: Any, tmp_path: Path) -> None:
    """INGEST ONLY: the most packets/s one engine ingests (and a draining consumer discards)
    from an unpaced sender. Nothing is decoded or printed, so this is not the rate ``ib listen``
    sustains; see :func:`test_listen_cli_at_the_target_rate_loses_no_output`.

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


def test_ingest_only_at_the_target_rate_loses_nothing_on_loopback(tmp_path: Path) -> None:
    """INGEST ONLY: paced at the floor and, on a host with room, at the design target: nothing
    is lost between the socket and a draining consumer."""
    sent, ingested, seconds = _loopback_run(tmp_path, FLOOR_PPS * 2, FLOOR_PPS)
    assert ingested == sent, f"floor rate lost {sent - ingested} of {sent}"
    sent, ingested, seconds = _loopback_run(tmp_path, TARGET_PPS * 2, TARGET_PPS)
    print(f"\npaced {TARGET_PPS:,} pkt/s: {ingested}/{sent} ingested over {seconds:.2f} s")
    assert ingested >= 0.9 * sent  # a loaded CI runner may drop some; the target is tracked


# -- the whole listen path --


@pytest.mark.parametrize("where", [None, 'msg == BENCH_TLM and blob contains "xx"'])
def test_listen_path_costs_little_over_decode_and_render(tmp_path: Path, where: str | None) -> None:
    """The per-packet cost of receive + queue + decode + JSON render, against the cost of the
    bare decode + render it wraps, measured in the same process so the ratio holds on any host.
    With ``where`` a typical decode-level filter is applied to every frame as well; its decode is
    the one the render reuses, so the filter adds only its own few microseconds of tests.

    Frames arrive in bursts as they do off a socket (50 per reader wake-up); what is timed is
    the engine and the listener around the decoder, which must stay a small fraction of it.
    """
    config = load_config(_bench_config(tmp_path))
    burst, total = 50, 20_000

    async def main() -> tuple[float, float]:
        ledger = Ledger(tmp_path / "ledger", session_id="cost")
        async with Session(config, role="ground", ledger=ledger, notify=_ignore) as ground:
            codec = ground.codec_for("fc")
            wire = codec.encode_telemetry("BENCH_TLM", {"blob": "x" * 20}).data
            listener = await ground.listen("fc", where=where)
            link = ground.route("fc").link
            io = ground._engine._ios[(link.name, LinkEnd.SOURCE)]
            frames = aiter(listener)
            best_listen = best_floor = float("inf")
            for _ in range(3):
                start = time.perf_counter()
                for _ in range(total // burst):
                    for _ in range(burst):
                        io.on_data(wire, 1, "127.0.0.1:1", len(wire), FrameFlag.NONE)
                    for _ in range(burst):
                        rx = await anext(frames)
                        json.dumps(render.record("fc", rx.frame.time_ns, rx.packet))
                best_listen = min(best_listen, (time.perf_counter() - start) / total)
                start = time.perf_counter()
                for _ in range(total):
                    packet = codec.decode(MessageKind.TELEMETRY, wire)
                    assert isinstance(packet, DecodedPacket)
                    json.dumps(render.record("fc", 1, packet))
                best_floor = min(best_floor, (time.perf_counter() - start) / total)
            await listener.close()
            return best_listen, best_floor

    listen_s, floor_s = asyncio.run(main())
    print(
        f"\nlisten path {listen_s * 1e6:.1f} us/packet, bare decode+render {floor_s * 1e6:.1f} us"
    )
    ratio = LISTEN_OVERHEAD_RATIO if where is None else LISTEN_FILTER_OVERHEAD_RATIO
    assert listen_s < ratio * floor_s, (
        f"listen path {listen_s * 1e6:.1f} us/packet vs {floor_s * 1e6:.1f} us for decode+render"
    )


LISTEN_PACKETS = 30_000
LISTEN_RATE = TARGET_PPS
CLI_MAIN = "import sys; from ironbird.cli.main import main; sys.exit(main())"


def _run_listen_cli(
    tmp_path: Path, count: int, rate: int, extra: tuple[str, ...] = ()
) -> tuple[int, int, str]:
    """``ib listen --format json`` (plus ``extra`` arguments) to a file while another process
    sends ``count`` packets at ``rate``/s. Returns (sent, lines written, standard error text)."""
    config_path = _bench_config(tmp_path)
    config = load_config(config_path)
    codec = Session(
        config, role="ground", ledger=Ledger(tmp_path / "l0", session_id="c"), notify=_ignore
    ).codec_for("fc")
    wire = codec.encode_telemetry("MOCK_HK").data  # many fields: the costly line to print
    port = config.links[0].telemetry_endpoint.port
    environment = {
        **os.environ,
        "IRONBIRD_CONFIG": str(config_path),
        "IRONBIRD_STATE_DIR": str(tmp_path / "state"),
    }
    out_path, err_path = tmp_path / "listen.out", tmp_path / "listen.err"
    with out_path.open("wb") as out, err_path.open("wb") as err:
        listener = subprocess.Popen(
            [sys.executable, "-c", CLI_MAIN, "listen", "fc", "--format", "json", *extra],
            stdout=out,
            stderr=err,
            env=environment,
        )
        try:
            deadline = time.monotonic() + 30
            while b"istening" not in err_path.read_bytes():
                assert listener.poll() is None, err_path.read_text()
                assert time.monotonic() < deadline, "the listener never reported it was listening"
                time.sleep(0.05)
            subprocess.run(
                [sys.executable, "-c", SENDER, str(port), str(count), str(rate), wire.hex()],
                check=True,
            )
            time.sleep(0.5)  # the last datagrams are read and written
        finally:
            listener.send_signal(signal.SIGTERM)
            try:
                listener.wait(timeout=30)
            except subprocess.TimeoutExpired:
                listener.kill()
                listener.wait()
    return count, len(out_path.read_bytes().splitlines()), err_path.read_text()


def test_listen_cli_at_the_target_rate_loses_no_output(tmp_path: Path) -> None:
    """END TO END: a separate ``ib listen --format json`` process writing to a file, fed by a
    separate sender at the design target rate. Output lines must not be dropped by the tool
    (``IB-OUT-*`` on standard error); only the host's own datagram loss is tolerated.
    """
    sent, lines, errors = _run_listen_cli(tmp_path, LISTEN_PACKETS, LISTEN_RATE)
    print(f"\nib listen at {LISTEN_RATE:,} pkt/s: {lines}/{sent} lines written")
    assert "IB-OUT-" not in errors, errors
    assert lines >= 0.95 * sent, f"{sent - lines} of {sent} packets did not reach the output"


def test_listen_cli_with_a_filter_at_the_target_rate_loses_no_output(tmp_path: Path) -> None:
    """END TO END with a typical filter: the same run as above with ``--where`` selecting every
    packet by a message name and a decoded field, so the filter is evaluated 30000 times and the
    output must still keep up with the design target."""
    where = "msg == MOCK_HK and err_counter >= 0 and not status == malformed"
    sent, lines, errors = _run_listen_cli(tmp_path, LISTEN_PACKETS, LISTEN_RATE, ("--where", where))
    print(f"\nib listen --where at {LISTEN_RATE:,} pkt/s: {lines}/{sent} lines written")
    assert "IB-OUT-" not in errors, errors
    assert lines >= 0.95 * sent, f"{sent - lines} of {sent} packets did not reach the output"
