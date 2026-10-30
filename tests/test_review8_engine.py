"""Review 8 regressions for the frame engine: link-end open/close races, kernel-drop accounting
across a re-open, sink fault isolation and de-duplication, engine shutdown, per-MID statistics,
overflow wording and concurrent readers of one capture."""

from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import Callable, Coroutine, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeVar

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ironbird.ccdd.model import MessageKind
from ironbird.cli.output import CHUNK_BYTES, WRITE_SLICE, BoundedWriter, write_slice_for
from ironbird.config import load_config
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.frame import Decoded, Frame, FrameFlag
from ironbird.ledger import Ledger
from ironbird.notice import Notice
from ironbird.session import Session
from ironbird.transport import LinkEnd, TransportStats
from ironbird.transport.base import Receiver
from test_engine import _Probe

T = TypeVar("T")
ROUNDS = 100


def _run(coro: Coroutine[Any, Any, T]) -> T:
    """Run a scenario, failing it instead of hanging if it never finishes."""

    async def bounded() -> T:
        async with asyncio.timeout(30):
            return await coro

    return asyncio.run(bounded())


def _ground(mock_config_path: Path, tmp_path: Path, notices: list[Notice] | None = None) -> Session:
    return Session(
        load_config(mock_config_path),
        role="ground",
        ledger=Ledger(tmp_path / "ledger", session_id="r8"),
        notify=(notices.append if notices is not None else lambda _: None),
    )


def _source_io(ground: Session) -> Any:
    link = ground.route("fc").link
    return ground._engine._ios[(link.name, LinkEnd.SOURCE)]


class FakeTransport:
    """A transport with no sockets: its counters and its close/poll timing are test-controlled."""

    def __init__(self, kernel_dropped: int = 0) -> None:
        self.kernel_dropped = kernel_dropped
        self.receiver: Receiver | None = None
        self.closed = False
        self.poll_gate: asyncio.Event | None = None
        self.close_gate: asyncio.Event | None = None

    @property
    def description(self) -> str:
        return "fake link"

    @property
    def local_address(self) -> tuple[str, int] | None:
        return None

    async def open(self, receiver: Receiver | None) -> None:
        self.receiver = receiver

    async def send(
        self,
        data: bytes,
        *,
        confirm: bool = False,
        handed_off: Callable[[], None] | None = None,
    ) -> None:
        if handed_off is not None:
            handed_off()

    def stats(self) -> TransportStats:
        return TransportStats(kernel_dropped=self.kernel_dropped)

    def facts(self) -> Mapping[str, object]:
        return MappingProxyType({})

    async def poll(self) -> None:
        if self.poll_gate is not None:
            await self.poll_gate.wait()

    async def close(self) -> None:
        if self.close_gate is not None:
            await self.close_gate.wait()
        self.closed = True


def _use_fakes(ground: Session, fakes: list[FakeTransport]) -> None:
    """The engine's next transports are ``fakes``, in order."""
    queue = list(fakes)
    ground._engine._new_transport = lambda io: queue.pop(0)  # type: ignore[method-assign,assignment]


# -- B3: close and open race on one link end --


def test_closing_and_reopening_the_same_link_end_never_loses_the_port(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> list[str]:
        failures: list[str] = []
        async with _ground(mock_config_path, tmp_path) as ground:
            route = ground.route("fc")
            current = await ground.capture([route])
            for _ in range(ROUNDS):
                closed, opened = await asyncio.gather(
                    current.close(), ground.capture([route]), return_exceptions=True
                )
                assert closed is None
                if isinstance(opened, BaseException):
                    failures.append(str(opened))
                    current = await ground.capture([route])
                else:
                    current = opened
            await current.close()
        return failures

    assert _run(main()) == []


def test_a_capture_that_joins_a_closing_link_end_keeps_its_transport(
    mock_config_path: Path, tmp_path: Path
) -> None:
    """The closer re-checks the subscribers under the link end's lock: a capture that attached
    while the close was in progress still receives."""

    async def main() -> int:
        async with _ground(mock_config_path, tmp_path) as ground:
            route = ground.route("fc")
            first = await ground.capture([route])
            second, _ = await asyncio.gather(ground.capture([route]), first.close())
            io = _source_io(ground)
            assert io.receiving and io.transport is not None and io.subs
            [(_, address)] = second.addresses
            assert address is not None
            raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            raw.sendto(b"\x00", address)
            raw.close()
            got = 0
            async with asyncio.timeout(5):
                async for _frame in second:
                    got += 1
                    break
            await second.close()
            return got

    assert _run(main()) == 1


def test_kernel_drops_of_a_replaced_transport_are_not_charged_to_its_successor(
    mock_config_path: Path, tmp_path: Path
) -> None:
    """A tick that was polling the old transport when it was closed and replaced must not report
    the old transport's total against the new transport's counters."""
    notices: list[Notice] = []

    async def main() -> None:
        async with _ground(mock_config_path, tmp_path, notices) as ground:
            engine = ground._engine
            old, new = FakeTransport(kernel_dropped=7), FakeTransport(kernel_dropped=0)
            old.poll_gate = asyncio.Event()
            _use_fakes(ground, [old, new])
            route = ground.route("fc")
            first = await ground.capture([route])
            tick = asyncio.ensure_future(engine.tick())  # parked inside old.poll()
            await asyncio.sleep(0)
            await first.close()  # the final accounting reports old's 7 drops
            notices.clear()
            second = await ground.capture([route])
            old.poll_gate.set()
            await tick
            ground.flush_notices()
            assert [n for n in notices if n.code is Code.TX_OVERRUN] == []
            new.kernel_dropped = 2
            await engine.tick()
            ground.flush_notices()
            assert [n for n in notices if n.code is Code.TX_OVERRUN]
            await second.close()

    _run(main())


def test_the_link_end_keeps_its_transport_until_the_close_completes(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> None:
        async with _ground(mock_config_path, tmp_path) as ground:
            transport = FakeTransport()
            transport.close_gate = asyncio.Event()
            _use_fakes(ground, [transport])
            capture = await ground.capture([ground.route("fc")])
            io = _source_io(ground)
            closing = asyncio.ensure_future(capture.close())
            await asyncio.sleep(0.01)
            assert io.transport is transport and io.receiving, "state swapped before the close"
            transport.close_gate.set()
            await closing
            assert io.transport is None and not io.receiving and transport.closed

    _run(main())


# -- F1: a sink that raises is detached, once --


class _Raises:
    def __init__(self) -> None:
        self.calls = 0

    def append(self, frame: Frame) -> None:
        self.calls += 1
        raise ValueError("boom")


class _Collects:
    def __init__(self) -> None:
        self.seqs: list[int] = []

    def append(self, frame: Frame) -> None:
        self.seqs.append(frame.seq)


def test_a_sink_that_raises_is_detached_after_one_report(
    mock_config_path: Path, tmp_path: Path
) -> None:
    notices: list[Notice] = []

    async def main() -> tuple[_Raises, _Collects]:
        async with _ground(mock_config_path, tmp_path, notices) as ground:
            bad, good = _Raises(), _Collects()
            capture = await ground.capture([ground.route("fc")], sinks=[bad, good])
            io = _source_io(ground)
            for i in range(50):
                io.on_data(b"\x00", i, "127.0.0.1:1", 1, FrameFlag.NONE)
            await capture.close()
            ground.flush_notices()
            return bad, good

    bad, good = _run(main())
    assert bad.calls == 1, "a failing sink is called again after it failed"
    assert good.seqs == list(range(1, 51)), "a healthy sink lost frames to the failing one"
    internal = [n for n in notices if n.code is Code.INTERNAL]
    assert len(internal) == 1, f"{len(internal)} notices for one failing sink"
    ledger_text = "".join(p.read_text() for p in (tmp_path / "ledger").rglob("*") if p.is_file())
    assert ledger_text.count("sink.detached") == 1


# -- F2: one sink, several captures --


def test_a_sink_on_two_captures_sees_each_frame_once_and_stays_until_the_last_closes(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> tuple[list[int], list[int], list[int]]:
        async with _ground(mock_config_path, tmp_path) as ground:
            route = ground.route("fc")
            sink = _Collects()
            first = await ground.capture([route], sinks=[sink])
            second = await ground.capture([route], sinks=[sink])
            io = _source_io(ground)
            io.on_data(b"\x00", 1, "p", 1, FrameFlag.NONE)
            both = list(sink.seqs)
            await second.close()
            io.on_data(b"\x00", 2, "p", 1, FrameFlag.NONE)
            after_one = list(sink.seqs)
            await first.close()
            io.on_data(b"\x00", 3, "p", 1, FrameFlag.NONE)
            return both, after_one, list(sink.seqs)

    both, after_one, final = _run(main())
    assert both == [1]
    assert after_one == [1, 2]
    assert final == [1, 2]


def test_a_session_level_sink_survives_a_capture_that_also_named_it(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> list[int]:
        async with _ground(mock_config_path, tmp_path) as ground:
            sink = _Collects()
            ground.add_sink(sink)
            ground.add_sink(sink)
            capture = await ground.capture([ground.route("fc")], sinks=[sink])
            io = _source_io(ground)
            io.on_data(b"\x00", 1, "p", 1, FrameFlag.NONE)
            await capture.close()
            io.on_data(b"\x00", 2, "p", 1, FrameFlag.NONE)
            ground.remove_sink(sink)
            ground.remove_sink(sink)
            io.on_data(b"\x00", 3, "p", 1, FrameFlag.NONE)
            return sink.seqs

    assert _run(main()) == [1, 2]


# -- F3: engine close ends iterators --


def test_closing_the_session_ends_a_capture_iterator_that_is_waiting(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> str:
        ground = _ground(mock_config_path, tmp_path)
        async with ground:
            capture = await ground.capture([ground.route("fc")])

            async def consume() -> str:
                async for _frame in capture:
                    pass
                return "ended"

            consumer = asyncio.ensure_future(consume())
            await asyncio.sleep(0.05)
        async with asyncio.timeout(2):
            result = await consumer
        await capture.close()  # a late close of an already released capture is harmless
        return result

    assert _run(main()) == "ended"


# -- F4: per-MID statistics count only frames that pass the check --


def test_malformed_frames_with_a_known_mid_are_not_counted_in_the_statistics(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> tuple[dict[int, int], int]:
        async with _ground(mock_config_path, tmp_path) as ground:
            wire = ground.codec_for("fc").encode_telemetry("MOCK_HK").data
            capture = await ground.capture([ground.route("fc")])
            io = _source_io(ground)
            bad = [wire[:8], wire + b"\0", wire[:-1], wire[:4] + b"\xff" * 3 + wire[7:]]
            for data in bad:
                io.on_data(data, 1, "p", len(data), FrameFlag.NONE)
            counts = {mid: s.count for mid, s in ground.stats_snapshot().items()}
            assert counts == {}, "malformed frames were counted"
            io.on_data(wire, 2, "p", len(wire), FrameFlag.NONE)
            counts = {mid: s.count for mid, s in ground.stats_snapshot().items()}
            await capture.close()
            return counts, ground.malformed_total

    counts, malformed = _run(main())
    assert list(counts.values()) == [1] and malformed >= 1


# -- F5: overflow wording --


def _overflow_notice(
    mock_config_path: Path, tmp_path: Path, sinks: list[Any], total: int = 12
) -> str:
    notices: list[Notice] = []

    async def main() -> None:
        async with _ground(mock_config_path, tmp_path, notices) as ground:
            capture = await ground.capture([ground.route("fc")], sinks=sinks, queue_frames=2)
            io = _source_io(ground)
            for i in range(total):
                io.on_data(b"\x00", i, "p", 1, FrameFlag.NONE)
            await ground._engine.tick()
            await capture.close()
            ground.flush_notices()

    _run(main())
    [notice] = [n for n in notices if n.code is Code.ENG_LAG]
    return notice.summary


def test_overflow_does_not_claim_capture_when_no_sink_received_the_frames(
    mock_config_path: Path, tmp_path: Path
) -> None:
    text = _overflow_notice(mock_config_path, tmp_path, sinks=[])
    assert "captured" not in text and "sink" not in text and "10" in text


def test_overflow_says_a_sink_received_the_frames_when_one_did(
    mock_config_path: Path, tmp_path: Path
) -> None:
    text = _overflow_notice(mock_config_path, tmp_path / "s", sinks=[_Collects()])
    assert "sink" in text


# -- F6: concurrent readers and send racing close --


def test_a_second_concurrent_reader_of_one_capture_is_a_coded_error(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> IronbirdError:
        async with _ground(mock_config_path, tmp_path) as ground:
            capture = await ground.capture([ground.route("fc")])

            async def read() -> None:
                async for _frame in capture:
                    pass

            first = asyncio.ensure_future(read())
            await asyncio.sleep(0.02)  # parked waiting for frames
            try:
                await read()
            except IronbirdError as exc:
                error = exc
            else:
                raise AssertionError("a second reader was accepted")
            finally:
                await capture.close()
                await first
            return error

    error = _run(main())
    assert error.code is Code.INTERNAL and "one reader" in error.summary


def test_a_send_racing_the_close_of_the_capture_never_reads_as_a_wire_failure(
    mock_config_path: Path, tmp_path: Path
) -> None:
    """The commanded node is up (its port is bound), so a send either goes out or fails for a
    reason that is not the link: the shared transport being closed under it must not surface."""
    config = load_config(mock_config_path)
    link = config.links[0]
    host, port = link.command_endpoint.host, link.command_endpoint.port
    fc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    fc.bind((host, port))

    async def main() -> list[str]:
        failures: list[str] = []
        async with _ground(mock_config_path, tmp_path) as ground:
            route = ground.route("fc")
            for _ in range(ROUNDS):
                capture = await ground.capture([route])
                results = await asyncio.gather(
                    ground.send_command("fc", "MOCK_NOOP"), capture.close(), return_exceptions=True
                )
                failures += [str(r) for r in results if isinstance(r, BaseException)]
        return failures

    try:
        failures = _run(main())
    finally:
        fc.close()
    assert failures == []


def test_a_send_after_the_engine_closed_is_a_clear_error(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> IronbirdError:
        ground = _ground(mock_config_path, tmp_path)
        async with ground:
            pass
        try:
            await ground.send_command("fc", "MOCK_NOOP")
        except IronbirdError as exc:
            return exc
        raise AssertionError("sent on a closed session")

    error = _run(main())
    assert error.code is Code.TX_SEND and "closing" in error.summary


# -- the live decode reuses ingest's check --


@pytest.fixture(scope="module")
def parity_probe(tmp_path_factory: pytest.TempPathFactory) -> _Probe:
    return _Probe(tmp_path_factory.mktemp("parity"))


@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    data=st.binary(max_size=64),
    flips=st.lists(st.tuples(st.integers(0, 47), st.integers(0, 255)), max_size=3),
    cut=st.integers(0, 3),
)
def test_decoding_a_live_frame_gives_what_the_whole_decode_gives(
    parity_probe: _Probe,
    data: bytes,
    flips: list[tuple[int, int]],
    cut: int,
) -> None:
    probe = parity_probe
    wire = bytearray(probe.codec.encode_telemetry("MOCK_HK").data)
    for index, value in flips:
        if index < len(wire):
            wire[index] = value
    candidates = [bytes(wire[: len(wire) - cut]), data, bytes(wire) + data[:cut]]
    decoder = probe.engine.decoder(report_checked=False)
    for candidate in candidates:
        probe.io[LinkEnd.SOURCE].on_data(candidate, 1, "p", len(candidate), FrameFlag.NONE)
        frame = probe.sink.frames[-1]
        expected = probe.codec.decode(MessageKind.TELEMETRY, frame.data)
        live = decoder.decode(frame)
        replayed = decoder.decode(frame._replace(checked=None))
        if isinstance(expected, DecodeError):
            assert live == expected and replayed == expected
        else:
            assert isinstance(live, Decoded) and live.packet == expected
            assert isinstance(replayed, Decoded) and replayed.packet == expected
            assert frame.checked is not None


# -- output slices --


def test_a_file_takes_whole_chunks_and_a_pipe_keeps_atomic_slices(tmp_path: Path) -> None:
    read_end, write_end = os.pipe()
    try:
        assert write_slice_for(write_end) == WRITE_SLICE
    finally:
        os.close(read_end)
        os.close(write_end)
    with (tmp_path / "out").open("wb") as handle:
        assert write_slice_for(handle.fileno()) == CHUNK_BYTES
    assert write_slice_for(-1) == WRITE_SLICE  # an unusable descriptor takes the safe size
    text = [f"line {i}" for i in range(5000)]
    path = tmp_path / "lines"
    with path.open("w") as stream:
        writer = BoundedWriter(stream, max_bytes=1 << 22)
        for line in text:
            writer.write(line)
        assert writer.close(timeout=10) and writer.lost == 0
    assert path.read_text().splitlines() == text
