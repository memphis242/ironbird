"""Engine fault isolation and bounds that need a hand-driven link end: sinks that raise on a
sent frame, a gap or a link event; link up/down bookkeeping; the per-MID table cap; a transport
that hands over more than the packet cap; and an ingest that fails outright.

Frames are injected with the link end's own receive callback, so no socket is involved.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Coroutine
from pathlib import Path
from typing import Any, TypeVar

import pytest

from ironbird.config import load_config
from ironbird.errors import Code, IronbirdError
from ironbird.frame import Frame, FrameFlag, GapReason, Stream
from ironbird.ledger import Ledger
from ironbird.notice import Notice
from ironbird.session import Session
from ironbird.transport import LinkEnd

T = TypeVar("T")


def run(coro: Coroutine[Any, Any, T]) -> T:
    async def bounded() -> T:
        async with asyncio.timeout(30):
            return await coro

    return asyncio.run(bounded())


def ground_session(mock_config_path: Path, tmp_path: Path, notices: list[Notice]) -> Session:
    return Session(
        load_config(mock_config_path),
        role="ground",
        ledger=Ledger(tmp_path / "ledger", session_id="paths"),
        notify=notices.append,
    )


def source_io(ground: Session) -> Any:
    link = ground.route("fc").link
    return ground._engine._ios[(link.name, LinkEnd.SOURCE)]


class Collector:
    def __init__(self) -> None:
        self.frames: list[Frame] = []

    def append(self, frame: Frame) -> None:
        self.frames.append(frame)


class RaisesOnTx:
    """Fine on received frames, broken on frames this process sent."""

    def __init__(self) -> None:
        self.calls = 0

    def append(self, frame: Frame) -> None:
        self.calls += 1
        if frame.stream.direction.value == "tx":
            raise RuntimeError("tx sink bug")


class RaisesOnGap(Collector):
    def gap(self, *args: Any) -> None:
        raise RuntimeError("gap bug")


class RaisesOnLink(Collector):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[tuple[bool, str]] = []

    def link_event(self, stream: Stream, up: bool, time_ns: int, text: str) -> None:
        self.events.append((up, text))
        raise RuntimeError("link bug")


def internal(notices: list[Notice]) -> list[Notice]:
    return [n for n in notices if n.code is Code.INTERNAL]


def test_a_sink_that_fails_on_a_sent_frame_is_detached_once_and_the_send_succeeds(
    mock_config_path: Path, tmp_path: Path
) -> None:
    notices: list[Notice] = []

    async def main() -> tuple[RaisesOnTx, Collector]:
        async with ground_session(mock_config_path, tmp_path, notices) as ground:
            bad, good = RaisesOnTx(), Collector()
            ground.add_sink(bad)
            ground.add_sink(good)
            target = ground.route("fc").link.command_endpoint
            listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            listener.bind((target.host, target.port))  # nothing answers with ICMP refusals
            try:
                for _ in range(3):
                    await ground.send_command("fc", "MOCK_NOOP")
            finally:
                listener.close()
            ground.flush_notices()
            return bad, good

    bad, good = run(main())
    assert bad.calls == 1  # detached by the first failure
    assert len(good.frames) == 3  # the healthy sink saw every sent frame
    assert len(internal(notices)) == 1


def test_a_sink_whose_gap_hook_raises_is_detached_and_others_still_get_the_gap(
    mock_config_path: Path, tmp_path: Path
) -> None:
    notices: list[Notice] = []

    async def main() -> None:
        async with ground_session(mock_config_path, tmp_path, notices) as ground:
            bad = RaisesOnGap()
            ground.add_sink(bad)
            capture = await ground.capture([ground.route("fc")])
            io = source_io(ground)
            ground._engine._notify_gap(io.rx_stream, GapReason.QUEUE_DROP, 4, 1, 2, 1, 2)
            ground._engine._notify_gap(io.rx_stream, GapReason.QUEUE_DROP, 4, 1, 2, 1, 2)
            ground.flush_notices()
            await capture.close()

    run(main())
    assert len(internal(notices)) == 1  # one report, however many gaps follow


def test_link_down_and_up_are_bookkept_and_a_failing_link_hook_is_detached(
    mock_config_path: Path, tmp_path: Path
) -> None:
    notices: list[Notice] = []

    async def main() -> tuple[RaisesOnLink, Collector]:
        async with ground_session(mock_config_path, tmp_path, notices) as ground:
            engine = ground._engine
            capture = await ground.capture([ground.route("fc")])
            io = source_io(ground)
            hooked, plain = RaisesOnLink(), Collector()
            ground.add_sink(hooked)
            ground.add_sink(plain)  # no link_event at all: skipped, not an error
            error = IronbirdError(Code.TX_DOWN, "fake: link lost")
            engine._link_state(io, True, 5, None)  # up while never down: nothing to do
            engine._link_state(io, False, 10, error)
            engine._link_state(io, False, 11, error)  # a second down changes nothing
            assert io.down is not None and io.down[0] == 10
            engine._link_state(io, True, 2_000_000_010, None)
            assert io.down is None
            ground.flush_notices()
            await capture.close()
            return hooked, plain

    hooked, _plain = run(main())
    assert [up for up, _ in hooked.events] == [False]  # detached after the first failure
    codes = [n.code for n in notices]
    assert Code.INTERNAL in codes and Code.TX_DOWN in codes


def test_a_link_still_down_when_the_session_closes_ends_its_gap(
    mock_config_path: Path, tmp_path: Path
) -> None:
    notices: list[Notice] = []
    gaps: list[Any] = []

    class GapCollector(Collector):
        def gap(self, *args: Any) -> None:
            gaps.append(args)

    async def main() -> None:
        async with ground_session(mock_config_path, tmp_path, notices) as ground:
            capture = await ground.capture([ground.route("fc")], sinks=[GapCollector()])
            io = source_io(ground)
            ground._engine._link_state(io, False, 10, IronbirdError(Code.TX_DOWN, "lost"))
            await capture.close()

    run(main())
    assert [g[1] for g in gaps] == [GapReason.LINK_DOWN]


def test_the_per_mid_table_is_capped_and_counts_what_did_not_fit(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ironbird.engine.MAX_TRACKED_MIDS", 1)
    notices: list[Notice] = []

    async def main() -> tuple[int, int]:
        async with ground_session(mock_config_path, tmp_path, notices) as ground:
            capture = await ground.capture([ground.route("fc")])
            codec = ground.codec_for("fc")
            io = source_io(ground)
            hk = codec.encode_telemetry("MOCK_HK").data
            status = codec.encode_telemetry("MOCK_STATUS").data
            for i, data in enumerate([hk, status, status, hk]):
                io.on_data(data, i + 1, "127.0.0.1:1", len(data), FrameFlag.NONE)
            result = (len(ground._engine._stats), ground._engine.stats_overflow)
            await capture.close()
            return result

    tracked, overflow = run(main())
    assert tracked == 1 and overflow == 2


def test_a_transport_that_hands_over_more_than_the_cap_is_cut_and_flagged(
    mock_config_path: Path, tmp_path: Path
) -> None:
    notices: list[Notice] = []

    async def main() -> Frame:
        async with ground_session(mock_config_path, tmp_path, notices) as ground:
            sink = Collector()
            capture = await ground.capture([ground.route("fc")], sinks=[sink])
            io = source_io(ground)
            cap = ground._engine._max_packet_bytes
            io.on_data(b"\xa5" * (cap + 500), 1, "127.0.0.1:1", cap + 500, FrameFlag.NONE)
            await capture.close()
            return sink.frames[0]

    frame = run(main())
    assert frame.flags & FrameFlag.TRUNCATED
    assert frame.orig_len > len(frame.data)


def test_an_ingest_that_fails_outright_is_reported_and_the_next_frame_still_arrives(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notices: list[Notice] = []

    async def main() -> int:
        async with ground_session(mock_config_path, tmp_path, notices) as ground:
            sink = Collector()
            capture = await ground.capture([ground.route("fc")], sinks=[sink])
            engine = ground._engine
            io = source_io(ground)
            real = engine._ingest
            calls = []

            def flaky(*args: Any) -> None:
                calls.append(1)
                if len(calls) == 1:
                    raise ValueError("ingest bug")
                real(*args)

            monkeypatch.setattr(engine, "_ingest", flaky)
            io.on_data(b"\x00", 1, "p", 1, FrameFlag.NONE)
            io.on_data(b"\x00", 2, "p", 1, FrameFlag.NONE)
            ground.flush_notices()
            await capture.close()
            return len(sink.frames)

    assert run(main()) == 1
    [notice] = internal(notices)
    assert "ingest bug" in notice.summary
