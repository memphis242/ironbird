"""Stage 2k: what the engine does with a serial link end.

Skipped bytes and flushed partial packets are frames but not malformed packets; a link that
goes down and comes back is a ledger state change, two marks for the sinks that take them, and
one ``link_down`` gap on the receive stream; a link that is still down when its transport closes
ends the gap there. UDP never has any of it.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from ironbird.config import load_config
from ironbird.errors import Code
from ironbird.frame import FLAG_GARBAGE, FLAG_STALE, GapReason, Stream
from ironbird.ledger import Ledger
from ironbird.notice import Level, Notice
from ironbird.session import Session
from ironbird.transport import LinkEnd
from serialutil import hk, serial_config, wait_for
from support.ptybridge import PtyDevice


class Sink:
    """A frame sink with the two optional hooks: gaps and link events."""

    def __init__(self) -> None:
        self.frames: list[Any] = []
        self.gaps: list[tuple[Stream, GapReason, int, int, int, int, int]] = []
        self.links: list[tuple[Stream, bool, int, str]] = []

    def append(self, frame: Any) -> None:
        self.frames.append(frame)

    def gap(self, *args: Any) -> None:
        self.gaps.append(args)  # type: ignore[arg-type]

    def link_event(self, stream: Stream, up: bool, time_ns: int, text: str) -> None:
        self.links.append((stream, up, time_ns, text))


def run_ground(
    tmp_path: Path,
    device: Path,
    scenario: Any,
    *,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[Notice], Ledger]:
    from ironbird.transport import serial as serial_module

    monkeypatch.setattr(serial_module, "RECONNECT_MIN_S", 0.02)
    monkeypatch.setattr(serial_module, "RECONNECT_MAX_S", 0.1)
    # the factory builds transports with the module defaults, which the patch above replaced
    config = load_config(serial_config(tmp_path, "ground.toml", device, "ground"))
    notices: list[Notice] = []
    ledger = Ledger(tmp_path / "ledger", session_id="g")

    async def main() -> None:
        async with Session(config, role="ground", ledger=ledger, notify=notices.append) as ground:
            await asyncio.wait_for(scenario(ground), timeout=30)

    asyncio.run(main())
    return notices, ledger


def events(ledger: Ledger) -> list[dict[str, Any]]:
    ledger.flush()
    return [json.loads(line) for line in ledger.path.read_text().splitlines()]


def test_skipped_bytes_are_frames_but_not_malformed_packets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    device = PtyDevice(tmp_path / "tty")

    async def scenario(ground: Session) -> None:
        sink = Sink()
        capture = await ground.capture([ground.route("fc")], sinks=[sink])
        engine = ground._engine
        try:
            device.write(hk(1))
            await wait_for(lambda: len(sink.frames) == 1, "the packet, settled by the idle time")
            device.write(b"\x00\x01\x02" + hk(2)[:20])  # garbage, then a packet cut short
            await wait_for(lambda: len(sink.frames) == 3, "garbage, packet and stale frames")
        finally:
            await capture.close()
        flags = [int(f.flags) & (FLAG_GARBAGE | FLAG_STALE) for f in sink.frames]
        assert sink.frames[0].data == hk(1) and flags[0] == 0
        assert sink.frames[1].flags & FLAG_GARBAGE and sink.frames[1].mid is None
        assert sink.frames[2].flags & FLAG_STALE and sink.frames[2].mid is not None
        assert engine.malformed_total == 0 and engine.resync_frames == 2
        assert sorted(engine.stats_snapshot()) == [0x890]  # only the packet is counted

    try:
        notices, ledger = run_ground(tmp_path, device.path, scenario, monkeypatch=monkeypatch)
    finally:
        device.close()
    resync = [n for n in notices if n.code is Code.TX_RESYNC]
    assert 1 <= len(resync) <= 2 and not any(n.code.value.startswith("IB-DEC") for n in notices)
    assert not [e for e in events(ledger) if e["event"] == "rx.malformed"]


def test_bytes_flagged_garbage_are_never_taken_for_a_packet_even_if_they_look_like_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    device = PtyDevice(tmp_path / "tty")

    async def scenario(ground: Session) -> None:
        sink = Sink()
        capture = await ground.capture([ground.route("fc")], sinks=[sink])
        try:
            io = ground._engine._io(ground.config.links[0], LinkEnd.SOURCE)
            io.on_data(hk(3), 5, "", len(hk(3)), FLAG_GARBAGE)
        finally:
            await capture.close()
        [frame] = sink.frames
        assert frame.flags == FLAG_GARBAGE and frame.checked is None and frame.mid is None
        assert ground._engine.stats_snapshot() == {} and ground._engine.malformed_total == 0

    try:
        run_ground(tmp_path, device.path, scenario, monkeypatch=monkeypatch)
    finally:
        device.close()


def test_a_link_that_goes_down_and_comes_back_is_a_gap_two_marks_and_ledger_states(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    device = PtyDevice(tmp_path / "tty")
    sink = Sink()

    async def scenario(ground: Session) -> None:
        capture = await ground.capture([ground.route("fc")], sinks=[sink])
        try:
            device.write(hk(1))
            await wait_for(lambda: len(sink.frames) == 1, "a packet")
            seq_before = ground._engine.last_seq
            device.unplug()
            await wait_for(lambda: len(sink.links) == 1, "the down mark")
            assert sink.gaps == []  # the gap is written when its end is known
            device.plug()
            await wait_for(lambda: len(sink.links) == 2, "the up mark")
        finally:
            await capture.close()
        (stream, down, down_ns, down_text), (_, up, up_ns, up_text) = sink.links
        assert (down, up) == (False, True) and up_ns >= down_ns
        assert "link lost" in down_text and "restored" in up_text
        [(gap_stream, reason, lost, first_ns, last_ns, first_seq, last_seq)] = sink.gaps
        assert gap_stream == stream and stream.direction.value == "rx"
        assert reason is GapReason.LINK_DOWN and lost == 1
        assert (first_ns, last_ns) == (down_ns, up_ns)
        assert first_seq == seq_before <= last_seq

    try:
        notices, ledger = run_ground(tmp_path, device.path, scenario, monkeypatch=monkeypatch)
    finally:
        device.close()
    states = [e for e in events(ledger) if e["event"] in ("link.down", "link.up")]
    assert [e["event"] for e in states] == ["link.down", "link.up"]
    assert states[0]["code"] == Code.TX_DOWN.value and states[1]["down_s"] >= 0
    down = [n for n in notices if n.code is Code.TX_DOWN]
    assert [n.level for n in down if "link lost" in n.summary] == [Level.ERROR]
    assert [n.level for n in down if "restored" in n.summary] == [Level.INFO]


def test_a_link_still_down_when_the_capture_closes_ends_its_gap_there(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    device = PtyDevice(tmp_path / "tty")
    sink = Sink()

    async def scenario(ground: Session) -> None:
        capture = await ground.capture([ground.route("fc")], sinks=[sink])
        device.unplug()
        await wait_for(lambda: len(sink.links) == 1, "the down mark")
        await capture.close()
        [(_, reason, lost, first_ns, last_ns, _, _)] = sink.gaps
        assert reason is GapReason.LINK_DOWN and lost == 1 and last_ns >= first_ns
        assert [up for _, up, _, _ in sink.links] == [False]  # it never came back

    try:
        _, ledger = run_ground(tmp_path, device.path, scenario, monkeypatch=monkeypatch)
    finally:
        device.close()
    assert [e["event"] for e in events(ledger) if e["event"].startswith("link.")] == [
        "link.down",
        "link.down_at_close",
    ]


def test_a_sink_that_raises_on_a_link_event_is_detached_and_the_link_carries_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    device = PtyDevice(tmp_path / "tty")

    class Bad(Sink):
        def link_event(self, stream: Stream, up: bool, time_ns: int, text: str) -> None:
            raise RuntimeError("sink bug")

    sink = Bad()

    async def scenario(ground: Session) -> None:
        capture = await ground.capture([ground.route("fc")], sinks=[sink])
        try:
            device.unplug()
            await wait_for(
                lambda: bool(ground._engine._ios) and not ground._engine._sinks,
                "the sink to be detached",
            )
            device.plug()
            await wait_for(
                lambda: not any(io.down for io in ground._engine._ios.values()), "the link back"
            )
            device.write(hk(5))
        finally:
            await capture.close()

    try:
        notices, _ = run_ground(tmp_path, device.path, scenario, monkeypatch=monkeypatch)
    finally:
        device.close()
    assert any(n.code is Code.INTERNAL and "Bad" in n.summary for n in notices)


def test_an_udp_link_end_never_reports_a_link_state(mock_config_path: Path) -> None:
    from ironbird.errors import IronbirdError
    from ironbird.transport import UdpTransport, open_transport

    link = load_config(mock_config_path).links[0]
    seen: list[tuple[bool, int, IronbirdError | None]] = []
    transport = open_transport(
        link, LinkEnd.SOURCE, lambda e: None, link_state=lambda *a: seen.append(a)
    )
    assert isinstance(transport, UdpTransport)
    assert not hasattr(transport, "is_down") and seen == []
