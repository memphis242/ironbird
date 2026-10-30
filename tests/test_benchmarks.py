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
from ironbird import render
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
from recutil import high_ports
from test_engine import _Probe

DECODE_CEILING_S = 100e-6
INGEST_CEILING_S = 100e-6
LISTEN_OVERHEAD_RATIO = 1.5  # listen path vs. the bare decode + render it must wrap
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
def test_listen_path_costs_little_over_decode_and_render(tmp_path: Path) -> None:
    """The per-packet cost of receive + queue + decode + JSON render, against the cost of the
    bare decode + render it wraps, measured in the same process so the ratio holds on any host.

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
            listener = await ground.listen("fc")
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
    assert listen_s < LISTEN_OVERHEAD_RATIO * floor_s, (
        f"listen path {listen_s * 1e6:.1f} us/packet vs {floor_s * 1e6:.1f} us for decode+render"
    )


LISTEN_PACKETS = 30_000
LISTEN_RATE = TARGET_PPS
CLI_MAIN = "import sys; from ironbird.cli.main import main; sys.exit(main())"


def _run_listen_cli(tmp_path: Path, count: int, rate: int) -> tuple[int, int, str]:
    """``ib listen --format json`` to a file while another process sends ``count`` packets at
    ``rate``/s. Returns (sent, lines written, standard error text)."""
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
            [sys.executable, "-c", CLI_MAIN, "listen", "fc", "--format", "json"],
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
