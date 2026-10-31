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
import yaml

from conftest import EXAMPLES, REPO
from filter_defs import make_frame
from ironbird import render
from ironbird.ccdd.json_adapter import parse_model
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.coalesce import Coalescer
from ironbird.codec import Codec, DecodedPacket
from ironbird.config import load_config
from ironbird.errors import Code
from ironbird.errors import DecodeError as DecodeErrorType
from ironbird.filter import compile_filter
from ironbird.frame import Decoded, Direction, Frame, FrameFlag, Stream
from ironbird.header import load_profile
from ironbird.ledger import Ledger
from ironbird.notice import Level
from ironbird.session import Session
from ironbird.transport import LinkEnd
from recutil import high_ports
from test_engine import _Probe

DECODE_CEILING_S = 100e-6
INGEST_CEILING_S = 100e-6
LISTEN_OVERHEAD_RATIO = 1.5  # listen path vs. the bare decode + render it must wrap
LISTEN_FILTER_OVERHEAD_RATIO = 1.8  # the same with a decode-level filter evaluated per frame
FLOOR_PPS = 1_000  # the plan's hard floor, aggregate packets per second
TARGET_PPS = 10_000  # the plan's design target
PACKET_BYTES = 200  # the plan's average packet size

# A guard whose threshold is a rate or a cost ratio measured on the host. Under xdist the other
# workers keep every core busy, so such a measurement says more about the scheduler than about
# the tool: a listener off-CPU for a few milliseconds overruns a socket buffer capped by a small
# net.core.rmem_max (212992 on GitHub runners), and a timed loop shares its core with another
# test. These run, and are enforced, in the serial run of this file (CI's "Benchmarks" step,
# ``pytest tests/test_benchmarks.py -n0``), as pytest-benchmark's own ceilings are.
needs_a_quiet_host = pytest.mark.skipif(
    "PYTEST_XDIST_WORKER" in os.environ,
    reason="host-timing guard: enforced by the serial run (pytest tests/test_benchmarks.py -n0)",
)


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


def test_decode_and_check_variable_length_telemetry_rate(benchmark: Any, mock_codec: Codec) -> None:
    """A counted array of 64 words and a tail string: the check reads the counts, the decode
    unpacks the elements with one single-element struct."""
    wire = mock_codec.encode_telemetry(
        "MOCK_DATA", {"Entries": list(range(64)), "Text": "x" * 100}
    ).data
    assert mock_codec.check(MessageKind.TELEMETRY, wire) is None
    result = benchmark(mock_codec.decode, MessageKind.TELEMETRY, wire)
    assert isinstance(result, DecodedPacket) and len(result.values[1]) == 64  # type: ignore[arg-type]
    _check_ceiling(benchmark, DECODE_CEILING_S)


def test_check_variable_length_frame_rate(benchmark: Any, mock_codec: Codec) -> None:
    wire = mock_codec.encode_telemetry("MOCK_DATA", {"Entries": list(range(64))}).data
    assert benchmark(mock_codec.check, MessageKind.TELEMETRY, wire) is None
    _check_ceiling(benchmark, DECODE_CEILING_S)


def test_check_rejects_a_variable_length_count_cheaply(benchmark: Any, mock_codec: Codec) -> None:
    kind, wire = MessageKind.TELEMETRY, mock_codec.encode_telemetry("MOCK_DATA").data
    bad = wire[:16] + b"\xff\xff" + wire[18:]
    result = benchmark(mock_codec.check, kind, bad)
    assert result is not None and result.code is Code.DEC_COUNT
    _check_ceiling(benchmark, DECODE_CEILING_S)


@pytest.fixture
def v2_codec() -> Codec:
    """The mock CCDD on the cFE MsgId V2 header: the MID is read from three places in the
    header (``examples/headers/cfe_msgid_v2_*.toml``)."""
    doc = json.loads((EXAMPLES / "ccdd" / "mock.json").read_text())
    for message in doc["messages"]:
        if message["kind"] == "command":
            message["mid"] = 0x2A90
        else:
            message["mid"] = 0x2A10 + (int(message["mid"], 0) & 0xF)
    return Codec(
        parse_model(doc, "mock-msgid-v2.json"),
        load_profile(EXAMPLES / "headers" / "cfe_msgid_v2_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "cfe_msgid_v2_tlm.toml"),
        clock=lambda: 1_700_000_000.0,
    )


@pytest.fixture
def pec_codec() -> Codec:
    """A telemetry message on a header with a CRC-16 trailer (packet error control)."""
    model = parse_model(
        {
            "schema": "ironbird-ccdd/0",
            "target": "pec",
            "messages": [
                {
                    "name": "PEC_HK",
                    "kind": "telemetry",
                    "mid": 0x0123,
                    "category": "housekeeping",
                    "fields": [
                        {"name": "counter", "type": "uint32"},
                        {"name": "volts", "type": "float32"},
                        {"name": "label", "type": "char[16]"},
                    ],
                }
            ],
        },
        "pec.json",
    )
    return Codec(
        model,
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "ccsds_pec_tlm.toml"),
    )


def test_decode_msgid_v2_telemetry_rate(benchmark: Any, v2_codec: Codec) -> None:
    wire = v2_codec.encode_telemetry("MOCK_HK").data
    result = benchmark(v2_codec.decode, MessageKind.TELEMETRY, wire)
    assert isinstance(result, DecodedPacket) and result.header.mid == 0x2A10
    _check_ceiling(benchmark, DECODE_CEILING_S)


def test_decode_msgid_v2_command_with_checksum_rate(benchmark: Any, v2_codec: Codec) -> None:
    wire = v2_codec.encode_command("MOCK_SET_LABEL", {"label": "abc"}).data
    result = benchmark(v2_codec.decode, MessageKind.COMMAND, wire)
    assert isinstance(result, DecodedPacket) and result.header.mid == 0x2A90
    _check_ceiling(benchmark, DECODE_CEILING_S)


def test_check_msgid_v2_frame_rate(benchmark: Any, v2_codec: Codec) -> None:
    wire = v2_codec.encode_telemetry("MOCK_HK").data
    assert benchmark(v2_codec.check, MessageKind.TELEMETRY, wire) is None
    _check_ceiling(benchmark, DECODE_CEILING_S)


def test_peek_msgid_v2_mid_rate(benchmark: Any, v2_codec: Codec) -> None:
    wire = v2_codec.encode_telemetry("MOCK_HK").data
    assert benchmark(v2_codec.peek_mid, MessageKind.TELEMETRY, wire) == 0x2A10
    _check_ceiling(benchmark, 5e-6)


def test_check_and_decode_with_a_crc_trailer_rate(benchmark: Any, pec_codec: Codec) -> None:
    wire = pec_codec.encode_telemetry("PEC_HK", {"counter": 7, "label": "x"}).data
    assert pec_codec.check(MessageKind.TELEMETRY, wire) is None
    result = benchmark(pec_codec.decode, MessageKind.TELEMETRY, wire)
    assert isinstance(result, DecodedPacket) and result.values[0] == 7
    _check_ceiling(benchmark, DECODE_CEILING_S)


def _per_call(function: Any, *args: Any, calls: int = 20_000) -> float:
    best = float("inf")
    for _ in range(5):
        start = time.perf_counter()
        for _ in range(calls):
            function(*args)
        best = min(best, (time.perf_counter() - start) / calls)
    return best


@needs_a_quiet_host
def test_the_new_header_features_cost_little_over_the_plain_header(
    mock_codec: Codec, v2_codec: Codec, pec_codec: Codec
) -> None:
    """Relative guards (any host): a three-window MID, and a CRC trailer, check a frame at no
    more than 2.5x the cost of the plain 16-bit MID with no trailer."""
    tlm = MessageKind.TELEMETRY
    plain = _per_call(mock_codec.check, tlm, mock_codec.encode_telemetry("MOCK_HK").data)
    v2 = _per_call(v2_codec.check, tlm, v2_codec.encode_telemetry("MOCK_HK").data)
    pec = _per_call(pec_codec.check, tlm, pec_codec.encode_telemetry("PEC_HK").data)
    assert v2 < 2.5 * plain, f"msgid v2 {v2 * 1e6:.2f} us vs plain {plain * 1e6:.2f} us"
    assert pec < 2.5 * plain, f"crc trailer {pec * 1e6:.2f} us vs plain {plain * 1e6:.2f} us"


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


# Cost of one more predicate: a chain of N predicates that are all true, so each one runs.
FILTER_PREDICATE_CEILING_S = 3e-6  # per predicate at frame, model and check level
FILTER_FIELD_PREDICATE_CEILING_S = 6e-6  # per decoded-field test, the decode itself excluded

PREDICATE_CASES = {
    "frame": (16, " and ".join(f"seq >= {i % 2}" for i in range(16))),
    "model": (8, " and ".join(["msg == MOCK_HK", "category == housekeeping"] * 4)),
    "check": (8, " and ".join(["status == ok", "status != malformed"] * 4)),
    "field": (8, " and ".join(["cmd_counter >= 0", "err_counter >= 0"] * 4)),
}


@needs_a_quiet_host
@pytest.mark.parametrize("case", list(PREDICATE_CASES))
def test_filter_cost_per_predicate(benchmark: Any, mock_codec: Codec, case: str) -> None:
    """The marginal cost of a predicate, by level: a chain of all-true predicates divided by its
    length (the single decode a field test needs is taken out by timing a one-predicate chain
    of the same level)."""
    count, text = PREDICATE_CASES[case]
    wire = mock_codec.encode_telemetry("MOCK_HK").data
    first = make_frame(mock_codec, FILTER_STREAM, wire, live=True)
    second = make_frame(mock_codec, FILTER_STREAM, wire, live=True, seq=2)

    def bound(expression: str) -> Any:
        flt = compile_filter(expression, {"fc": mock_codec})
        return flt.bind(lambda f: _decode_for_filter(mock_codec, f), lambda _node: mock_codec)

    matcher = bound(text)

    def run() -> tuple[bool, bool]:
        return matcher(first), matcher(second)

    assert benchmark(run) == (True, True)
    stats = getattr(benchmark, "stats", None)
    if stats is not None and not getattr(benchmark, "disabled", False):
        single = bound(text.split(" and ")[0])
        started = time.perf_counter()
        for _ in range(20_000):
            single(first)
            single(second)
        base = (time.perf_counter() - started) / 40_000
        per_predicate = (stats.stats.mean / 2 - base) / max(count - 1, 1)
        print(f"\nfilter {case}: {per_predicate * 1e6:.2f} us per extra predicate")
        ceiling = (
            FILTER_FIELD_PREDICATE_CEILING_S if case == "field" else FILTER_PREDICATE_CEILING_S
        )
        assert per_predicate < ceiling, f"{per_predicate * 1e6:.2f} us"


COMPILE_MESSAGES = 2048
COMPILE_ALTERNATIVES = 256
FILTER_COMPILE_CEILING_S = 0.5  # a 256-way `or` over a 2048-message CCDD (0.03-0.11 s measured)


@needs_a_quiet_host
def test_filter_compile_time_does_not_grow_with_the_message_count(tmp_path: Path) -> None:
    """Fields of one type are judged once however many messages have them, so a wide `or` over a
    large CCDD compiles in milliseconds (it took seconds when each message was judged alone)."""
    mids = [m for m in range(0x10000) if m & 0x1800 == 0x0800][:COMPILE_MESSAGES]
    assert len(mids) == COMPILE_MESSAGES
    messages = [
        {
            "name": f"M{i}",
            "kind": "telemetry",
            "mid": hex(mid),
            "category": "housekeeping",
            "fields": [
                {"name": "x", "type": "uint16" if i % 2 else "uint8"},
                {"name": "mode", "type": "uint8", "enum": {"SAFE": 1, "NOMINAL": 2}},
            ],
        }
        for i, mid in enumerate(mids)
    ]
    path = tmp_path / "wide.json"
    path.write_text(
        json.dumps({"schema": "ironbird-ccdd/0", "target": "wide", "messages": messages})
    )
    codec = Codec(
        load_ccdd(path),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
    )
    texts = {
        "or": " or ".join(f"x == {i}" for i in range(COMPILE_ALTERNATIVES)),
        "in": "x in [" + ", ".join(str(i) for i in range(500)) + "]",
        "and": " and ".join(f"x != {i}" for i in range(COMPILE_ALTERNATIVES)),
        "labels": " or ".join(["mode == SAFE", "mode in [NOMINAL, SAFE]"] * 60),
    }
    for kind, text in texts.items():
        started = time.perf_counter()
        flt = compile_filter(text, {"wide": codec})
        took = time.perf_counter() - started
        print(f"\ncompile {kind} ({len(text)} chars): {took * 1e3:.1f} ms")
        assert flt.mids is not None and len(flt.mids) == COMPILE_MESSAGES
        assert took < FILTER_COMPILE_CEILING_S, f"{kind}: {took:.2f} s"


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
    command_port, telemetry_port = high_ports(2)
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
[recording]
min_free_bytes = 0
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


@needs_a_quiet_host
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


@needs_a_quiet_host
def test_ingest_only_at_the_target_rate_loses_nothing_on_loopback(tmp_path: Path) -> None:
    """INGEST ONLY: paced at the floor and, on a host with room, at the design target: nothing
    is lost between the socket and a draining consumer."""
    sent, ingested, seconds = _loopback_run(tmp_path, FLOOR_PPS * 2, FLOOR_PPS)
    assert ingested == sent, f"floor rate lost {sent - ingested} of {sent}"
    sent, ingested, seconds = _loopback_run(tmp_path, TARGET_PPS * 2, TARGET_PPS)
    print(f"\npaced {TARGET_PPS:,} pkt/s: {ingested}/{sent} ingested over {seconds:.2f} s")
    assert ingested >= 0.9 * sent  # a loaded CI runner may drop some; the target is tracked


# -- the whole listen path --


@needs_a_quiet_host
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


@needs_a_quiet_host
def test_listen_cli_at_the_target_rate_loses_no_output(tmp_path: Path) -> None:
    """END TO END: a separate ``ib listen --format json`` process writing to a file, fed by a
    separate sender at the design target rate. Output lines must not be dropped by the tool
    (``IB-OUT-*`` on standard error); only the host's own datagram loss is tolerated.
    """
    sent, lines, errors = _run_listen_cli(tmp_path, LISTEN_PACKETS, LISTEN_RATE)
    print(f"\nib listen at {LISTEN_RATE:,} pkt/s: {lines}/{sent} lines written")
    assert "IB-OUT-" not in errors, errors
    assert lines >= 0.95 * sent, (
        f"{sent - lines} of {sent} packets did not reach the output; stderr: {errors}"
    )


@needs_a_quiet_host
def test_listen_cli_with_a_filter_at_the_target_rate_loses_no_output(tmp_path: Path) -> None:
    """END TO END with a typical filter: the same run as above with ``--where`` selecting every
    packet by a message name and a decoded field, so the filter is evaluated 30000 times and the
    output must still keep up with the design target."""
    where = "msg == MOCK_HK and err_counter >= 0 and not status == malformed"
    sent, lines, errors = _run_listen_cli(tmp_path, LISTEN_PACKETS, LISTEN_RATE, ("--where", where))
    print(f"\nib listen --where at {LISTEN_RATE:,} pkt/s: {lines}/{sent} lines written")
    assert "IB-OUT-" not in errors, errors
    assert lines >= 0.95 * sent, (
        f"{sent - lines} of {sent} packets did not reach the output; stderr: {errors}"
    )


# -- recording --


def _record_frames(count: int) -> list[Frame]:
    from recutil import RX, frame

    data = bytes(PACKET_BYTES)
    return [frame(i, stream=RX, data=data, mid=0x899) for i in range(1, count + 1)]


def test_recorder_append_cost(benchmark: Any, tmp_path: Path) -> None:
    """The per-frame cost of the recorder sink: encode one 200-byte frame (CRC, chunk summary).
    Flushes to the writer thread are amortised in; the disk is the thread's problem."""
    from ironbird.recording.writer import Recorder
    from recutil import FAST, RX, frame, meta

    recorder = Recorder.open(tmp_path / "b.ibrec", meta(), streams=[RX], options=FAST)
    item = frame(1, data=bytes(PACKET_BYTES), mid=0x899)
    benchmark(recorder.append, item)  # the recorder does not look at sequence order
    asyncio.run(recorder.close())
    benchmark.extra_info["frames_per_s"] = round(1 / benchmark.stats.stats.mean) if getattr(
        benchmark, "stats", None) else None  # fmt: skip
    _check_ceiling(benchmark, 100e-6)


@needs_a_quiet_host
def test_recorder_writer_throughput_and_reader_rate(tmp_path: Path) -> None:
    """WRITER: frames/s and MB/s from append to a closed, fsynced file; READER: frames/s with
    every CRC and rule checked."""
    from ironbird.recording.reader import RecordingReader
    from ironbird.recording.writer import Recorder
    from recutil import FAST, RX, meta

    total = 150_000
    frames = _record_frames(total)
    recorder = Recorder.open(tmp_path / "w.ibrec", meta(), streams=[RX], options=FAST)
    start = time.perf_counter()
    for item in frames:
        recorder.append(item)
    appended = time.perf_counter() - start
    asyncio.run(recorder.close())
    written = time.perf_counter() - start
    size = (tmp_path / "w.ibrec").stat().st_size
    start = time.perf_counter()
    with RecordingReader(tmp_path / "w.ibrec") as reader:
        read = sum(1 for _ in reader.frames())
    reading = time.perf_counter() - start
    assert read == total
    print(
        f"\nrecorder append {total / appended:,.0f} frames/s "
        f"({appended / total * 1e6:.1f} us each); "
        f"to a closed file {total / written:,.0f} frames/s, {size / written / 1e6:.0f} MB/s; "
        f"reader {total / reading:,.0f} frames/s ({size / reading / 1e6:.0f} MB/s)"
    )
    assert total / appended >= TARGET_PPS * 3  # the sink alone leaves most of a core free
    assert total / reading >= FLOOR_PPS * 10


RECORD_PACKETS = 30_000


def _run_record_cli(tmp_path: Path, count: int, rate: int) -> tuple[int, Any, str, float]:
    """``ib record`` in its own process while another sends ``count`` packets at ``rate``/s.
    Returns (sent, the recording's report, stderr, CPU seconds the recorder used)."""
    import resource

    from ironbird.recording import inspect as rec_inspect

    config_path = _bench_config(tmp_path)
    config = load_config(config_path)
    codec = Session(
        config, role="ground", ledger=Ledger(tmp_path / "l0", session_id="c"), notify=_ignore
    ).codec_for("fc")
    wire = codec.encode_telemetry("BENCH_TLM", {"blob": "x" * 20}).data
    port = config.links[0].telemetry_endpoint.port
    environment = {
        **os.environ,
        "IRONBIRD_CONFIG": str(config_path),
        "IRONBIRD_STATE_DIR": str(tmp_path / "state"),
    }
    out_path, err_path = tmp_path / "bench.ibrec", tmp_path / "record.err"
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    with err_path.open("wb") as err:
        recorder = subprocess.Popen(
            [sys.executable, "-c", CLI_MAIN, "record", "-o", str(out_path), "--quiet"],
            stdout=subprocess.DEVNULL,
            stderr=err,
            env=environment,
        )
        try:
            deadline = time.monotonic() + 30
            while b"istening" not in err_path.read_bytes():
                assert recorder.poll() is None, err_path.read_text()
                assert time.monotonic() < deadline, "the recorder never reported it was listening"
                time.sleep(0.05)
            subprocess.run(
                [sys.executable, "-c", SENDER, str(port), str(count), str(rate), wire.hex()],
                check=True,
            )
            time.sleep(0.7)
        finally:
            recorder.send_signal(signal.SIGTERM)
            try:
                recorder.wait(timeout=30)
            except subprocess.TimeoutExpired:
                recorder.kill()
                recorder.wait()
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu = (after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime)
    return count, rec_inspect.scan(out_path), err_path.read_text(), cpu


@needs_a_quiet_host
def test_record_cli_at_the_target_rate_loses_no_frame(tmp_path: Path) -> None:
    """END TO END: a separate ``ib record`` process, a separate sender at the design target rate.
    Every datagram that reached the socket is in the recording: recorded frames plus the kernel's
    own drops (GAP records) equal what was sent, nothing is dropped by the recorder, and the file
    is closed cleanly."""
    sent, report, errors, cpu = _run_record_cli(tmp_path, RECORD_PACKETS, TARGET_PPS)
    kernel = report.lost_by_reason.get("kernel_drop", 0)
    print(
        f"\nib record at {TARGET_PPS:,} pkt/s: {report.frames}/{sent} recorded, {kernel} lost in "
        f"the kernel, {cpu:.2f} s CPU ({cpu / max(report.frames, 1) * 1e6:.0f} us per frame)"
    )
    assert "IB-REC-" not in errors.replace("IB-REC-007", ""), errors
    assert report.status == "closed" and not report.holes and not report.findings
    assert report.frames + kernel == sent, f"{sent - report.frames - kernel} frames unaccounted for"
    assert report.frames >= 0.95 * sent  # the host's own datagram loss is tolerated, not hidden
    assert report.lost_by_reason.get("recorder_overrun", 0) == 0


# -- replay --

REPLAY_FRAMES = 60_000
REPLAY_OVERHEAD_RATIO = 1.5  # replay vs. the reader + decode + render it must wrap


def _replay_rate(path: Path, **kwargs: Any) -> tuple[int, int, float]:
    """``(frames shown, frames read, seconds)`` for one in-process replay to a null output."""
    from replayutil import replay_in_process

    start = time.perf_counter()
    run = replay_in_process(path, **kwargs)
    took = time.perf_counter() - start
    assert run.replay is not None
    return run.result.shown, run.replay.stats.frames_read, took


@pytest.fixture(scope="module")
def replay_file(tmp_path_factory: pytest.TempPathFactory) -> Path:
    from ironbird.recording.writer import RecorderOptions
    from replayutil import codec_status_frames, hk_frames, write_recording

    path = tmp_path_factory.mktemp("replay-bench") / "bench.ibrec"
    frames = hk_frames(REPLAY_FRAMES - 20_000) + codec_status_frames(20_000, REPLAY_FRAMES - 19_999)
    write_recording(path, frames, options=RecorderOptions(min_free_bytes=0))
    return path


@needs_a_quiet_host
def test_replay_decodes_and_renders_at_the_rate_of_the_bare_path(replay_file: Path) -> None:
    """Replay's own cost on top of reading, decoding and rendering stays small, in every format."""
    from ironbird.recording.player import OutputFormat
    from ironbird.recording.reader import RecordingReader
    from replayutil import mock_codec

    codec = mock_codec()
    start = time.perf_counter()
    with RecordingReader(replay_file) as reader:
        for item in reader.frames():
            decoded = codec.decode(item.stream.kind, item.data)
            assert not isinstance(decoded, DecodeErrorType)
            render.line("fc", item.time_ns, decoded)
    bare = time.perf_counter() - start
    rates = {}
    for fmt_ in OutputFormat:
        shown, _, took = _replay_rate(replay_file, fmt_=fmt_)
        assert shown == REPLAY_FRAMES
        rates[fmt_.value] = REPLAY_FRAMES / took
    print(
        f"\nreplay {rates['line']:,.0f} frames/s as lines, {rates['json']:,.0f} as JSON, "
        f"{rates['hex']:,.0f} as hex dumps; the bare read + decode + render is "
        f"{REPLAY_FRAMES / bare:,.0f} frames/s"
    )
    assert rates["line"] >= TARGET_PPS * 1.5  # the design target leaves most of a core free
    assert REPLAY_FRAMES / rates["line"] <= bare * REPLAY_OVERHEAD_RATIO


@needs_a_quiet_host
@pytest.mark.parametrize(
    "where, selected",
    [
        ("seq > 59990", 10),
        ("msg == MOCK_HK and cmd_counter > 250", 780),
        ("msg == MOCK_STATUS", 20_000),
    ],
)
def test_filtered_replay_is_cheaper_than_showing_everything(
    replay_file: Path, where: str, selected: int
) -> None:
    """A filter that selects little costs less than printing everything, and one the index can
    answer (a message that is in few chunks) does not read the rest of the file."""
    _, _, everything = _replay_rate(replay_file)
    shown, read, took = _replay_rate(replay_file, where=where)
    print(
        f"\nreplay --where {where!r}: {shown:,} shown of {REPLAY_FRAMES:,}, {read:,} read, "
        f"{REPLAY_FRAMES / took:,.0f} frames/s ({took / everything:.2f} of the unfiltered time)"
    )
    assert shown == selected
    assert took < everything * 0.9


@needs_a_quiet_host
def test_a_narrow_time_range_reads_a_fraction_of_the_file(replay_file: Path) -> None:
    from ironbird.recording.replay import TimeSpec
    from replayutil import STEP_NS, T0

    start = TimeSpec(absolute_ns=T0 + 30_000 * STEP_NS)
    end = TimeSpec(absolute_ns=T0 + 30_500 * STEP_NS)
    shown, read, took = _replay_rate(replay_file, start=start, end=end)
    _, _, everything = _replay_rate(replay_file)
    print(f"\nreplay of a 500 frame range: {read:,} read of {REPLAY_FRAMES:,}, {took * 1e3:.1f} ms")
    assert shown == 500 and read < REPLAY_FRAMES * 0.1
    assert took < everything * 0.2


@needs_a_quiet_host
def test_replay_cli_writes_every_line_at_the_target_rate(replay_file: Path, tmp_path: Path) -> None:
    """The whole command (start-up, read, decode, render, the output thread) to a file."""
    from recutil import CLI_MAIN
    from replayutil import child_env

    out = tmp_path / "out.txt"
    start = time.perf_counter()
    with out.open("wb") as sink:
        done = subprocess.run(
            [sys.executable, "-c", CLI_MAIN, "replay", str(replay_file)],
            stdout=sink,
            stderr=subprocess.PIPE,
            env=child_env(tmp_path),
            check=False,
            timeout=300,
        )
    took = time.perf_counter() - start
    lines = out.read_bytes().count(b"\n")
    print(f"\nib replay: {lines:,} lines in {took:.2f} s, {lines / took:,.0f} lines/s")
    assert done.returncode == 0, done.stderr
    assert lines == REPLAY_FRAMES and lines / took >= TARGET_PPS


# -- export --

EXPORT_OVERHEAD_RATIO = 2.5  # export (decode + format + write) vs. the bare reader + decode


@needs_a_quiet_host
def test_export_writes_every_row_at_the_target_rate_in_every_format(
    replay_file: Path, tmp_path: Path
) -> None:
    """Reading, decoding, formatting and writing a file: CSV tables, text lines and JSON records,
    each to a real temporary file that is committed at the end."""
    from exportutil import export_in_process
    from ironbird.recording.export import ExportFormat
    from ironbird.recording.reader import RecordingReader
    from replayutil import mock_codec

    codec = mock_codec()
    start = time.perf_counter()
    with RecordingReader(replay_file) as reader:
        for item in reader.frames():
            assert not isinstance(codec.decode(item.stream.kind, item.data), DecodeErrorType)
    bare = time.perf_counter() - start
    rates = {}
    for fmt_ in ExportFormat:
        out = tmp_path / f"out-{fmt_.value}"
        start = time.perf_counter()
        result, _ = export_in_process(replay_file, out, fmt_)
        took = time.perf_counter() - start
        assert result.rows == REPLAY_FRAMES
        rates[fmt_.value] = REPLAY_FRAMES / took
        if fmt_ is ExportFormat.CSV:
            assert took <= bare * EXPORT_OVERHEAD_RATIO
    print(
        f"\nexport {rates['csv']:,.0f} rows/s as CSV tables, {rates['text']:,.0f} as text, "
        f"{rates['jsonl']:,.0f} as JSON Lines; the bare read + decode is "
        f"{REPLAY_FRAMES / bare:,.0f} frames/s"
    )
    assert min(rates.values()) >= TARGET_PPS * 1.5  # the design target leaves most of a core free


@needs_a_quiet_host
def test_export_cli_writes_a_csv_directory_at_the_target_rate(
    replay_file: Path, tmp_path: Path
) -> None:
    """The whole command: start-up, definitions, read, decode, CSV, sync and commit."""
    from recutil import CLI_MAIN
    from replayutil import child_env

    out = tmp_path / "tables"
    start = time.perf_counter()
    done = subprocess.run(
        [sys.executable, "-c", CLI_MAIN, "export", str(replay_file), "-o", str(out)],
        capture_output=True,
        text=True,
        env=child_env(tmp_path),
        check=False,
        timeout=300,
    )
    took = time.perf_counter() - start
    rows = sum(p.read_bytes().count(b"\n") - 1 for p in out.iterdir())
    print(f"\nib export: {rows:,} rows in {took:.2f} s, {rows / took:,.0f} rows/s")
    assert done.returncode == 0, done.stderr
    assert rows == REPLAY_FRAMES and rows / took >= TARGET_PPS


# -- the serial run that enforces the guards above --

WORKFLOW = REPO / ".github" / "workflows" / "ci.yml"
PARALLEL_RUN = "pytest -n auto"
SERIAL_RUN = "pytest tests/test_benchmarks.py -n0"


def _unenforced_jobs(workflow_text: str) -> list[str]:
    """Jobs that run the suite under xdist (where the host-timing guards skip) without a later,
    failure-reporting serial run of this file; empty if every such job enforces them."""
    document = yaml.safe_load(workflow_text)
    assert isinstance(document, dict) and isinstance(document.get("jobs"), dict)
    problems: list[str] = []
    for name, job in document["jobs"].items():
        steps = job.get("steps", []) if isinstance(job, dict) else []
        runs = [(str(step.get("run", "")), step) for step in steps if isinstance(step, dict)]
        parallel = [i for i, (run, _) in enumerate(runs) if PARALLEL_RUN in run]
        if not parallel:
            continue
        serial = [
            i
            for i, (run, step) in enumerate(runs)
            if SERIAL_RUN in run and not step.get("continue-on-error")
        ]
        if not any(i > parallel[-1] for i in serial):
            problems.append(f"{name}: runs '{PARALLEL_RUN}' but no later '{SERIAL_RUN}' step")
    return problems


def test_every_parallel_ci_job_enforces_the_host_timing_guards_serially() -> None:
    assert _unenforced_jobs(WORKFLOW.read_text()) == []


@pytest.mark.parametrize(
    "old, new",
    [
        (SERIAL_RUN, "pytest tests/test_benchmarks.py -n auto"),
        (f"run: {SERIAL_RUN}\n", f"run: {SERIAL_RUN}\n        continue-on-error: true\n"),
    ],
    ids=["serial run made parallel", "serial run allowed to fail"],
)
def test_the_enforcement_check_catches_a_lost_serial_run(old: str, new: str) -> None:
    text = WORKFLOW.read_text()
    assert old in text, f"mutation anchor not found: {old!r}"
    assert _unenforced_jobs(text.replace(old, new))
