"""The frame-yielding engine: ingest order, sinks, captures, TX frames, the transport registry.

Malformed-input coverage of the ingest path and of ``Frame`` construction (Hypothesis) lives
here too: whatever bytes a transport delivers, ingest produces a valid frame, never raises,
and never loses the frame.
"""

from __future__ import annotations

import asyncio
import dataclasses
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import EXAMPLES
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec
from ironbird.config import Link, SystemConfig, UdpLinkSpec, load_config
from ironbird.engine import Capture, Engine
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.frame import (
    MAX_FRAME_BYTES,
    Decoded,
    Direction,
    Frame,
    FrameFlag,
    Stream,
)
from ironbird.header import load_profile
from ironbird.ledger import Ledger
from ironbird.notice import Notice
from ironbird.session import Session
from ironbird.transport import TRANSPORTS, LinkEnd, UdpTransport, open_transport

TLM, CMD = MessageKind.TELEMETRY, MessageKind.COMMAND


def run_ground_and_fc(
    config: SystemConfig,
    tmp_path: Path,
    scenario: Callable[[Session, Session, list[Notice]], Awaitable[None]],
) -> list[Notice]:
    notices: list[Notice] = []

    async def main() -> None:
        async with (
            Session(
                config,
                role="ground",
                ledger=Ledger(tmp_path / "ground", session_id="g"),
                notify=notices.append,
            ) as ground,
            Session(
                config, role="fc", ledger=Ledger(tmp_path / "fc", session_id="f"), notify=print
            ) as fc,
        ):
            await asyncio.wait_for(scenario(ground, fc, notices), timeout=20)

    asyncio.run(main())
    return notices


class ListSink:
    """Records frames, and whether the consumer queue could already hold each one."""

    def __init__(self, capture: Any = None) -> None:
        self.frames: list[Frame] = []
        self.capture = capture
        self.queued_when_seen: list[bool] = []

    def append(self, frame: Frame) -> None:
        self.frames.append(frame)
        if self.capture is not None:
            self.queued_when_seen.append(frame in self.capture._queue.items)


def blast(addr: tuple[str, int], datagrams: list[bytes]) -> None:
    raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for data in datagrams:
            raw.sendto(data, addr)
    finally:
        raw.close()


async def until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


# -- ingest order and sinks ---------------------------------------------------------------


def test_sinks_see_every_frame_before_the_consumer_queue_even_when_it_is_full(
    mock_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(mock_config_path)
    total, room = 30, 4

    async def scenario(ground: Session, fc: Session, notices: list[Notice]) -> None:
        sink = ListSink()
        capture = await ground.capture([ground.route("fc")], sinks=[sink], queue_frames=room)
        sink.capture = capture
        [(_, addr)] = capture.addresses
        assert addr is not None
        hk = fc.codec_for("fc").encode_telemetry("MOCK_HK").data
        blast(addr, [hk] * total)  # the capture is never read while this happens
        await until(lambda: len(sink.frames) == total)
        stats = capture.stats()
        assert stats.frames == total and stats.not_shown == total - room
        assert sink.queued_when_seen == [False] * total  # the sink ran before any queue
        kept = [frame async for frame in _drain(capture, room)]
        assert [f.seq for f in kept] == [f.seq for f in sink.frames[:room]]  # newest dropped
        await capture.close()

    run_ground_and_fc(config, tmp_path, scenario)


async def _drain(capture: Capture, count: int) -> AsyncIterator[Frame]:
    seen = 0
    async for frame in capture:
        yield frame
        seen += 1
        if seen == count:
            return


def test_a_consumer_that_falls_behind_is_reported_once_and_loses_nothing_from_the_sinks(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ironbird.session.TICK_S", 0.05)
    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, notices: list[Notice]) -> None:
        sink = ListSink()
        capture = await ground.capture([ground.route("fc")], sinks=[sink], queue_frames=2)
        [(_, addr)] = capture.addresses
        assert addr is not None
        hk = fc.codec_for("fc").encode_telemetry("MOCK_HK").data
        blast(addr, [hk] * 50)
        await until(lambda: len(sink.frames) == 50)
        await asyncio.sleep(0.2)
        overflow = [n for n in notices if n.code is Code.TX_OVERRUN]
        assert len(overflow) == 1 and "dropped from the consumer view" in overflow[0].summary
        await capture.close()
        assert len(sink.frames) == 50

    run_ground_and_fc(config, tmp_path, scenario)


def test_seq_is_session_wide_and_strictly_increasing_across_two_links(
    two_node_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(two_node_config_path)

    async def scenario(ground: Session, fc: Session, notices: list[Notice]) -> None:
        routes = [ground.route("fc"), ground.route("fc2")]
        sink = ListSink()
        capture = await ground.capture(routes, sinks=[sink])
        assert len({s.id for s in capture.streams}) == 2
        addrs = {stream.node: addr for stream, addr in capture.addresses}
        hk = fc.codec_for("fc").encode_telemetry("MOCK_HK").data
        for _ in range(10):
            for node in ("fc", "fc2"):
                address = addrs[node]
                assert address is not None
                blast(address, [hk])
                await asyncio.sleep(0.002)
        await until(lambda: len(sink.frames) == 20)
        assert [f.seq for f in sink.frames] == list(range(1, 21))
        assert {f.stream.node for f in sink.frames} == {"fc", "fc2"}
        frames = [frame async for frame in _drain(capture, 20)]
        assert [f.seq for f in frames] == list(range(1, 21))
        await capture.close()

    run_ground_and_fc(config, tmp_path, scenario)


@pytest.fixture
def two_node_config_path(mock_config_path: Path) -> Path:
    from conftest import free_udp_ports

    command_port, telemetry_port = free_udp_ports(2)
    text = mock_config_path.read_text()
    second = (
        f'\n[nodes.fc2]\nalias = "flight2"\nccdd = "{EXAMPLES}/ccdd/mock.json"\n'
        f'command_header = "{EXAMPLES}/headers/ccsds_v1_cfs_cmd.toml"\n'
        f'telemetry_header = "{EXAMPLES}/headers/ccsds_v1_cfs_tlm.toml"\n\n'
        f'[[links]]\nname = "ground-fc2"\nfrom = "ground"\nto = "fc2"\ntransport = "udp"\n'
        f'command_endpoint = "127.0.0.1:{command_port}"\n'
        f'telemetry_endpoint = "127.0.0.1:{telemetry_port}"\n'
    )
    path = mock_config_path.with_name("two.toml")
    path.write_text(text + second)
    return path


# -- malformed and unknown frames reach the consumers -----------------------------------------


def test_malformed_unknown_and_oversize_frames_are_yielded_with_their_flags(
    mock_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, notices: list[Notice]) -> None:
        codec = fc.codec_for("fc")
        sink = ListSink()
        capture = await ground.capture([ground.route("fc")], sinks=[sink])
        [(_, addr)] = capture.addresses
        assert addr is not None
        hk = codec.encode_telemetry("MOCK_HK").data
        unknown = bytearray(hk)
        unknown[1] ^= 0x7F
        cap = config.max_packet_bytes
        datagrams = [hk, hk[:1], hk + b"\x00", bytes(unknown), bytes(cap + 500), hk]
        blast(addr, datagrams)
        frames = [frame async for frame in _drain(capture, len(datagrams))]
        assert [f.seq for f in frames] == [1, 2, 3, 4, 5, 6]  # nothing was hidden
        assert [f.data for f in frames[:4]] == datagrams[:4]
        short, lying, unknown_frame, oversize = frames[1:5]
        assert FrameFlag.HAS_MID not in short.flags and short.mid is None
        assert lying.mid is not None and FrameFlag.HAS_MID in lying.flags
        assert unknown_frame.mid is not None and FrameFlag.HAS_MID in unknown_frame.flags
        assert FrameFlag.TRUNCATED in oversize.flags
        assert oversize.orig_len == cap + 500 and len(oversize.data) == cap
        decoder = ground.decoder(report_checked=False)
        verdicts = [decoder.decode(f) for f in frames]
        assert isinstance(verdicts[0], Decoded) and isinstance(verdicts[5], Decoded)
        errors = [v for v in verdicts if isinstance(v, DecodeError)]
        assert [e.code for e in errors] == [
            Code.DEC_SHORT,
            Code.DEC_LENGTH,
            Code.DEC_UNKNOWN,
            Code.DEC_OVERSIZE,
        ]
        assert ground.malformed_total == 4  # counted once, at ingest; the decoder added none
        assert {n.code for n in notices} >= {Code.DEC_SHORT, Code.DEC_OVERSIZE}
        again = ground.decoder(report_checked=True)  # replay has no ingest: it reports
        assert isinstance(again.decode(frames[1]), DecodeError)
        assert ground.malformed_total == 5
        await capture.close()

    run_ground_and_fc(config, tmp_path, scenario)


# -- TX frames ------------------------------------------------------------------------------


def test_sent_commands_and_replies_pass_through_ingest_as_tx_frames(
    mock_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, notices: list[Notice]) -> None:
        ground_sink, fc_sink = ListSink(), ListSink()
        fc.add_sink(fc_sink)
        ground.add_sink(ground_sink)
        responder = await fc.respond()
        listener = await ground.listen("fc")
        sent = await ground.send_command("fc", "MOCK_NOOP")
        handled = await asyncio.wait_for(aiter(responder).__anext__(), timeout=5)
        assert handled.reply is not None
        [tx] = [f for f in ground_sink.frames if f.stream.direction is Direction.TX]
        assert tx.stream.direction is Direction.TX and tx.stream.kind is CMD
        assert tx.data == sent.data and tx.mid == sent.mid and FrameFlag.HAS_MID in tx.flags
        await until(lambda: len(fc_sink.frames) >= 1)
        reply_frames = [f for f in fc_sink.frames if f.stream.direction is Direction.TX]
        assert [f.data for f in reply_frames] == [handled.reply.data]
        assert reply_frames[0].stream.kind is TLM
        received = await asyncio.wait_for(aiter(listener).__anext__(), timeout=5)
        assert received.frame.stream.direction is Direction.RX
        assert received.frame.seq > tx.seq  # one numbering for what went out and what came in
        await responder.close()
        await listener.close()

    run_ground_and_fc(config, tmp_path, scenario)


def test_a_sink_that_raises_is_reported_and_does_not_stop_ingest(
    mock_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(mock_config_path)

    class Broken:
        def append(self, frame: Frame) -> None:
            raise RuntimeError("sink bug")

    async def scenario(ground: Session, fc: Session, notices: list[Notice]) -> None:
        good = ListSink()
        capture = await ground.capture([ground.route("fc")], sinks=[Broken(), good])
        [(_, addr)] = capture.addresses
        assert addr is not None
        hk = fc.codec_for("fc").encode_telemetry("MOCK_HK").data
        blast(addr, [hk] * 5)
        await until(lambda: len(good.frames) == 5)
        internal = [n for n in notices if n.code is Code.INTERNAL]
        assert len(internal) == 1 and "Broken" in internal[0].summary  # coalesced, not 5 lines
        assert len([f async for f in _drain(capture, 5)]) == 5
        await capture.close()

    run_ground_and_fc(config, tmp_path, scenario)


# -- shared transports -----------------------------------------------------------------------


def test_two_captures_of_one_link_end_share_the_transport_and_free_the_port_when_done(
    mock_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, notices: list[Notice]) -> None:
        route = ground.route("fc")
        first = await ground.capture([route])
        second = await ground.capture([route])
        ((_, addr),) = first.addresses
        assert addr is not None
        hk = fc.codec_for("fc").encode_telemetry("MOCK_HK").data
        blast(addr, [hk])
        one = await asyncio.wait_for(aiter(first).__anext__(), timeout=5)
        two = await asyncio.wait_for(aiter(second).__anext__(), timeout=5)
        assert one == two  # the same frame, delivered to both
        await first.close()
        blast(addr, [hk])  # the transport stays up for the capture that remains
        assert (await asyncio.wait_for(aiter(second).__anext__(), timeout=5)).seq == 2
        await second.close()
        again = await ground.capture([route])  # the port is free again
        await again.close()

    run_ground_and_fc(config, tmp_path, scenario)


def test_a_send_only_transport_is_replaced_when_a_capture_starts_and_sends_still_work(
    mock_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, notices: list[Notice]) -> None:
        command = ground.route("fc").link.command_endpoint
        target = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # so no send is refused
        target.bind((command.host, command.port))
        target.setblocking(False)
        sink = ListSink()
        ground.add_sink(sink)
        try:
            await ground.send_command("fc", "MOCK_NOOP")  # opens a send-only transport
            capture = await ground.capture([ground.route("fc")])  # replaces it with a receiving one
            await ground.send_command("fc", "MOCK_NOOP")
            await capture.close()
            await ground.send_command("fc", "MOCK_NOOP")  # and a capture's end leaves sends working
            await until(lambda: len(sink.frames) == 3)
            assert [f.seq for f in sink.frames] == [1, 2, 3]
            assert all(f.stream.direction is Direction.TX for f in sink.frames)
            received = [target.recv(2048) for _ in range(3)]
            assert received == [f.data for f in sink.frames]  # what left is what was recorded
        finally:
            target.close()
            ground.remove_sink(sink)

    run_ground_and_fc(config, tmp_path, scenario)


def test_a_failed_open_leaves_no_capture_and_no_sink_behind(
    mock_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, notices: list[Notice]) -> None:
        route = ground.route("fc")
        telemetry = route.link.telemetry_endpoint
        squatter = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        squatter.bind((telemetry.host, telemetry.port))
        sink = ListSink()
        try:
            with pytest.raises(IronbirdError) as info:
                await ground.capture([route], sinks=[sink])
        finally:
            squatter.close()
        assert info.value.code is Code.TX_OPEN
        capture = await ground.capture([route])  # the port is usable once the squatter is gone
        [(_, addr)] = capture.addresses
        assert addr is not None
        blast(addr, [fc.codec_for("fc").encode_telemetry("MOCK_HK").data])
        await asyncio.wait_for(aiter(capture).__anext__(), timeout=5)
        assert sink.frames == []  # the failed capture's sink was detached
        await capture.close()

    run_ground_and_fc(config, tmp_path, scenario)


# -- the transport registry ------------------------------------------------------------------


def test_registry_has_a_factory_for_every_transport_the_config_accepts() -> None:
    from ironbird.config import SUPPORTED_TRANSPORTS

    assert set(TRANSPORTS) == set(SUPPORTED_TRANSPORTS)
    with pytest.raises(TypeError):
        TRANSPORTS["serial"] = TRANSPORTS["udp"]  # type: ignore[index]


def test_open_transport_builds_the_transport_the_link_names(mock_config_path: Path) -> None:
    config = load_config(mock_config_path)
    link = config.links[0]
    errors: list[IronbirdError] = []
    source = open_transport(link, LinkEnd.SOURCE, errors.append)
    destination = open_transport(link, LinkEnd.DESTINATION, errors.append)
    assert isinstance(source, UdpTransport) and isinstance(destination, UdpTransport)
    assert source.description == f"udp {link.telemetry_endpoint}"  # the source receives telemetry
    assert destination.description == f"udp {link.command_endpoint}"


def test_a_link_holds_exactly_the_spec_its_transport_names(mock_config_path: Path) -> None:
    link = load_config(mock_config_path).links[0]
    assert link.udp is not None and link.serial is None
    with pytest.raises(AssertionError):
        Link("l", "a", "b", "udp")  # no spec
    with pytest.raises(AssertionError):
        Link("l", "a", "b", "serial", udp=link.udp)  # the wrong spec
    with pytest.raises(dataclasses.FrozenInstanceError):
        link.udp.command_endpoint = link.udp.telemetry_endpoint  # type: ignore[misc]
    assert isinstance(link.udp, UdpLinkSpec)


def test_a_serial_link_is_still_refused_by_the_config(
    mock_config_path: Path, tmp_path: Path
) -> None:
    text = mock_config_path.read_text().replace('transport = "udp"', 'transport = "serial"')
    path = tmp_path / "serial.toml"
    path.write_text(text)
    with pytest.raises(IronbirdError) as info:
        load_config(path)
    assert info.value.code is Code.CFG_SCHEMA and "unsupported transport" in info.value.summary


# -- Frame invariants -----------------------------------------------------------------------

STREAM = Stream(1, "l", "fc", TLM, Direction.RX, "udp", "udp 0.0.0.0:1")
SERIAL_STREAM = dataclasses.replace(STREAM, transport="serial", endpoint="serial /dev/x")


def make_frame(**changes: Any) -> Frame:
    fields: dict[str, Any] = {
        "seq": 1,
        "time_ns": 5,
        "stream": STREAM,
        "source": "127.0.0.1:1",
        "mid": 0x890,
        "flags": FrameFlag.HAS_MID,
        "orig_len": 3,
        "data": b"abc",
    }
    fields.update(changes)
    return Frame(**fields)


def test_frame_is_immutable_and_accepts_a_consistent_description() -> None:
    frame = make_frame()
    with pytest.raises(AttributeError):
        frame.seq = 2  # type: ignore[misc]
    assert make_frame(mid=None, flags=FrameFlag.NONE).mid is None
    cut = make_frame(flags=FrameFlag.HAS_MID | FrameFlag.TRUNCATED, orig_len=70000)
    assert cut.orig_len == 70000
    garbage = make_frame(
        stream=SERIAL_STREAM, mid=None, flags=FrameFlag.GARBAGE, data=b"\xff\xfe", orig_len=2
    )
    assert FrameFlag.GARBAGE in garbage.flags


@pytest.mark.parametrize(
    "changes",
    [
        {"mid": None},  # HAS_MID without a mid
        {"flags": FrameFlag.NONE},  # a mid without HAS_MID
        {"mid": -1},
        {"mid": 1 << 32},
        {"seq": 0},
        {"orig_len": 2},  # below the data
        {"orig_len": 4},  # longer on the wire but not marked cut
        {"flags": FrameFlag.HAS_MID | FrameFlag.TRUNCATED},  # marked cut but not longer
        {"flags": FrameFlag.HAS_MID | FrameFlag(0x40)},  # an undefined bit
        {"data": bytes(MAX_FRAME_BYTES + 1), "orig_len": MAX_FRAME_BYTES + 1},
        {"flags": FrameFlag.GARBAGE, "mid": None},  # garbage on a UDP stream
        {"flags": FrameFlag.STALE | FrameFlag.HAS_MID},  # stale on a UDP stream
    ],
    ids=lambda c: next(iter(c)) + "=" + str(next(iter(c.values())))[:12],
)
def test_frame_rejects_inconsistent_descriptions(changes: dict[str, Any]) -> None:
    with pytest.raises(AssertionError):
        make_frame(**changes)


@pytest.mark.parametrize(
    "flags",
    [
        FrameFlag.GARBAGE | FrameFlag.STALE,
        FrameFlag.GARBAGE | FrameFlag.HAS_MID,
        FrameFlag.GARBAGE | FrameFlag.TRUNCATED,
    ],
)
def test_frame_rejects_meaningless_serial_flag_combinations(flags: FrameFlag) -> None:
    mid = 1 if FrameFlag.HAS_MID in flags else None
    orig = 5 if FrameFlag.TRUNCATED in flags else 3
    with pytest.raises(AssertionError):
        make_frame(stream=SERIAL_STREAM, flags=flags, mid=mid, orig_len=orig)


FUZZ = settings(max_examples=300, deadline=1000, suppress_health_check=[HealthCheck.too_slow])


@FUZZ
@given(
    seq=st.integers(-5, 1 << 64),
    time_ns=st.integers(-5, 1 << 64),
    stream=st.sampled_from([STREAM, SERIAL_STREAM]),
    mid=st.none() | st.integers(-5, 1 << 33),
    flags=st.integers(0, 0xFF),
    orig_len=st.integers(-5, 1 << 20),
    data=st.binary(max_size=64),
)
def test_frame_construction_from_arbitrary_values_asserts_or_is_consistent(
    seq: int, time_ns: int, stream: Stream, mid: int | None, flags: int, orig_len: int, data: bytes
) -> None:
    try:
        frame = Frame(seq, time_ns, stream, "src", mid, FrameFlag(flags), orig_len, data)
    except AssertionError:
        return  # an inconsistent description is refused, never silently kept
    assert frame.seq >= 1 and frame.orig_len >= len(frame.data)
    assert (FrameFlag.HAS_MID in frame.flags) == (frame.mid is not None)
    assert (FrameFlag.TRUNCATED in frame.flags) == (frame.orig_len > len(frame.data))
    if frame.flags & (FrameFlag.GARBAGE | FrameFlag.STALE):
        assert frame.stream.transport == "serial"


# -- ingest of arbitrary bytes ----------------------------------------------------------------


class _Probe:
    """An engine on the example config with no event loop: ingest is synchronous."""

    def __init__(self, directory: Path) -> None:
        config = load_config(EXAMPLES / "mock.toml")
        self.config = config
        self.codec = Codec(
            load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
            load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
            load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
            max_packet_bytes=config.max_packet_bytes,
        )
        self.reports: list[DecodeError | IronbirdError] = []
        self.engine = Engine(
            config,
            Ledger(directory, session_id="probe"),
            lambda error, **_: self.reports.append(error),
            lambda _node: self.codec,
        )
        self.sink = ListSink()
        self.engine.add_sink(self.sink)
        self.io = {end: self.engine._io(config.links[0], end) for end in LinkEnd}


@pytest.fixture(scope="module")
def probe(tmp_path_factory: pytest.TempPathFactory) -> _Probe:
    return _Probe(tmp_path_factory.mktemp("probe"))


@FUZZ
@given(data=st.binary(max_size=3 * 2048), end=st.sampled_from(list(LinkEnd)))
def test_ingest_of_arbitrary_bytes_always_yields_one_valid_frame(
    probe: _Probe, data: bytes, end: LinkEnd
) -> None:
    cap = probe.config.max_packet_bytes
    before_seq, before_bad = probe.engine.last_seq, probe.engine.malformed_total
    before_frames = len(probe.sink.frames)
    probe.io[end].on_data(data, 7, "peer", len(data), FrameFlag.NONE)
    assert probe.engine.last_seq == before_seq + 1  # one frame, numbered in order
    assert len(probe.sink.frames) == before_frames + 1
    frame = probe.sink.frames[-1]
    assert frame.seq == before_seq + 1 and frame.stream is probe.io[end].rx_stream
    assert frame.data == data[:cap] and frame.orig_len == len(data)
    assert (FrameFlag.TRUNCATED in frame.flags) == (len(data) > cap)
    verdict = probe.codec.check(frame.stream.kind, frame.data)
    expected_bad = verdict is not None or len(data) > cap
    assert probe.engine.malformed_total == before_bad + (1 if expected_bad else 0)
    assert frame.mid == probe.codec.peek_mid(frame.stream.kind, frame.data)


def test_a_transport_that_already_cut_the_datagram_is_still_one_oversize_frame(
    probe: _Probe,
) -> None:
    cap = probe.config.max_packet_bytes
    before = probe.engine.malformed_total
    probe.io[LinkEnd.SOURCE].on_data(bytes(cap), 1, "peer", cap + 1000, FrameFlag.TRUNCATED)
    frame = probe.sink.frames[-1]
    assert FrameFlag.TRUNCATED in frame.flags and frame.orig_len == cap + 1000
    assert probe.engine.malformed_total == before + 1
    report = probe.reports[-1]
    assert isinstance(report, DecodeError) and report.code is Code.DEC_OVERSIZE


def test_ingest_failure_is_reported_not_raised(tmp_path: Path) -> None:
    probe = _Probe(tmp_path)

    class Raises:
        def append(self, frame: Frame) -> None:
            raise RuntimeError("x")

    probe.engine.add_sink(Raises())
    probe.io[LinkEnd.SOURCE].on_data(b"\x01", 1, "peer", 1, FrameFlag.NONE)
    assert any(isinstance(r, IronbirdError) and r.code is Code.INTERNAL for r in probe.reports)
    assert probe.engine.last_seq == 1 and len(probe.sink.frames) == 1
