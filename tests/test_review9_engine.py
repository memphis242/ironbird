"""Review 9 regressions for the frame engine: batches survive an abandoned iteration, a close
racing an open ends every capture, the check token is bound to its codec and bytes, frame
equality and pickling ignore the token, sink holder counts across a failure, the first reading
task owns a capture, and a bounded transport open."""

from __future__ import annotations

import asyncio
import copy
import pickle
import socket
import time
from collections.abc import AsyncIterator, Callable, Coroutine
from pathlib import Path
from typing import Any, TypeVar

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import EXAMPLES
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec, Resolved
from ironbird.config import Endpoint, load_config
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.frame import Decoded, Direction, Frame, FrameFlag, Stream
from ironbird.header import load_profile
from ironbird.ledger import Ledger
from ironbird.notice import Notice
from ironbird.session import Session
from ironbird.transport import LinkEnd, UdpTransport
from test_engine import _Probe

T = TypeVar("T")
TLM = MessageKind.TELEMETRY
BURST = 200  # more than one frame, fewer than a read batch


def _run(coro: Coroutine[Any, Any, T]) -> T:
    async def bounded() -> T:
        async with asyncio.timeout(30):
            return await coro

    return asyncio.run(bounded())


def _ground(mock_config_path: Path, tmp_path: Path, role: str = "ground") -> Session:
    return Session(
        load_config(mock_config_path),
        role=role,
        ledger=Ledger(tmp_path / f"ledger-{role}", session_id="r9"),
        notify=lambda _: None,
    )


async def _burst(address: tuple[str, int], data: bytes, count: int) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for _ in range(count):
            sock.sendto(data, address)
    finally:
        sock.close()


async def _until(predicate: Callable[[], bool]) -> None:
    deadline = time.monotonic() + 10
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached"
        await asyncio.sleep(0.005)


async def _drain(iterator: Any, idle_s: float = 0.2) -> int:
    """Items an async iterator yields until it stays quiet for ``idle_s``."""
    count = 0
    try:
        while True:
            await asyncio.wait_for(iterator.__anext__(), idle_s)
            count += 1
    except (TimeoutError, StopAsyncIteration):
        return count


# -- 1: abandoning an iteration keeps the unread frames --


def test_breaking_out_of_a_capture_iteration_loses_no_frames(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> tuple[int, int, list[int]]:
        async with _ground(mock_config_path, tmp_path) as ground:
            capture = await ground.capture([ground.route("fc")])
            address = capture.addresses[0][1]
            assert address is not None
            await _burst(address, ground.codec_for("fc").encode_telemetry("MOCK_HK").data, BURST)
            await _until(lambda: capture.stats().frames == BURST)
            first = 0
            async for frame in capture:
                first = frame.seq
                break
            rest = [f.seq for f in await capture.next_batch()]
            again = await _drain(capture.__aiter__())
            await capture.close()
            return first, again, rest

    first, again, rest = _run(main())
    assert first == 1
    assert rest == list(range(2, BURST + 1)) and again == 0


def test_breaking_out_of_a_capture_and_iterating_again_continues_in_order(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> list[int]:
        async with _ground(mock_config_path, tmp_path) as ground:
            capture = await ground.capture([ground.route("fc")])
            address = capture.addresses[0][1]
            assert address is not None
            await _burst(address, ground.codec_for("fc").encode_telemetry("MOCK_HK").data, BURST)
            await _until(lambda: capture.stats().frames == BURST)
            seqs: list[int] = []
            for _ in range(5):  # a "wait for X" loop that breaks at once, many times over
                async for frame in capture:
                    seqs.append(frame.seq)
                    break
            async for frame in capture:
                seqs.append(frame.seq)
                if len(seqs) == BURST:
                    break
            await capture.close()
            return seqs

    assert _run(main()) == list(range(1, BURST + 1))


def test_breaking_out_of_a_listener_iteration_loses_no_packets(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> tuple[int, int]:
        async with _ground(mock_config_path, tmp_path) as ground:
            listener = await ground.listen("fc")
            address = listener.local_address
            assert address is not None
            await _burst(address, ground.codec_for("fc").encode_telemetry("MOCK_HK").data, BURST)
            await _until(lambda: listener.capture.stats().frames == BURST)
            async for _packet in listener:
                break
            rest = await _drain(listener.__aiter__())
            stats = listener.capture.stats()
            await listener.close()
            return rest, stats.not_shown

    rest, not_shown = _run(main())
    assert (rest, not_shown) == (BURST - 1, 0)


def test_breaking_out_of_a_responder_iteration_loses_no_commands(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> int:
        async with _ground(mock_config_path, tmp_path, role="fc") as flight:
            responder = await flight.respond()
            address = responder.local_address
            assert address is not None
            await _burst(address, b"\x00\x01\x02", BURST)  # malformed: handled, no reply
            await _until(lambda: responder._capture.stats().frames == BURST)
            async for _handled in responder:
                break
            rest = await _drain(responder.__aiter__())
            await responder.close()
            return rest

    assert _run(main()) == BURST - 1


# -- 2: a close racing an open ends the capture and leaves no transport --


def test_a_close_racing_a_capture_open_never_leaves_an_endless_capture(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def trial(steps: int) -> str:
        ground = _ground(mock_config_path, tmp_path)
        opened = asyncio.ensure_future(ground.capture([ground.route("fc")]))
        for _ in range(steps):
            await asyncio.sleep(0)
        await ground._engine.close()
        try:
            capture = await opened
        except IronbirdError:
            capture = None
        if capture is not None:
            try:
                batch = await asyncio.wait_for(capture.next_batch(), 1.0)
            except TimeoutError:
                return f"steps={steps}: iteration of a capture opened across close never ends"
            assert batch == []
            await capture.close()
        leaked = [io.transport for io in ground._engine._ios.values() if io.transport is not None]
        if leaked:
            return f"steps={steps}: a transport is still open after close"
        return ""

    async def main() -> list[str]:
        return [verdict for steps in range(8) if (verdict := await trial(steps))]

    assert _run(main()) == []


def test_a_capture_opened_after_the_engine_closed_is_a_coded_error(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> IronbirdError:
        ground = _ground(mock_config_path, tmp_path)
        await ground._engine.close()
        try:
            await ground.capture([ground.route("fc")])
        except IronbirdError as exc:
            return exc
        raise AssertionError("captured on a closed engine")

    error = _run(main())
    assert error.code is Code.INTERNAL and "closed" in error.summary


# -- 3 and 4: the check token and frame identity --


def _codec(max_packet_bytes: int = 4096) -> Codec:
    return Codec(
        load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
        max_packet_bytes=max_packet_bytes,
    )


def _stream() -> Stream:
    return Stream(0, "l", "fc", TLM, Direction.RX, "udp", "udp x")


def _live_frame(codec: Codec, data: bytes) -> Frame:
    token = codec.resolve(TLM, data)
    assert isinstance(token, Resolved)
    return Frame(
        1, 1, _stream(), "src", token.message.mid, FrameFlag.HAS_MID, len(data), data, token
    )


@pytest.fixture(scope="module")
def token_probe(tmp_path_factory: pytest.TempPathFactory) -> _Probe:
    return _Probe(tmp_path_factory.mktemp("token"))


def _expected(codec: Codec, data: bytes) -> object:
    return codec.decode(TLM, data)


@settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    payload=st.binary(max_size=40),
    op=st.sampled_from(["replace_data", "replace_other", "make", "make_short", "foreign", "kind"]),
    tail=st.integers(0, 3),
)
def test_a_tampered_or_foreign_token_always_decodes_like_the_whole_decode(
    token_probe: _Probe, payload: bytes, op: str, tail: int
) -> None:
    probe = token_probe
    codec = probe.codec
    hk = codec.encode_telemetry("MOCK_HK").data
    status = codec.encode_telemetry("MOCK_STATUS").data
    live = _live_frame(codec, hk)
    other = _codec(max_packet_bytes=2048)  # a second codec with its own identity
    decoder = probe.engine.decoder(report_checked=False)
    frame: Frame
    match op:
        case "replace_data":
            frame = live._replace(data=status, orig_len=len(status))
        case "replace_other":
            cut = payload[: max(0, len(payload) - tail)]
            frame = live._replace(data=cut, orig_len=len(cut), mid=None, flags=FrameFlag.NONE)
        case "make":
            frame = Frame._make([*live[:7], hk[:-1] + b"\x00", live.checked])
        case "make_short":
            frame = Frame._make(list(live)[:8])
        case "foreign":
            token = other.resolve(TLM, hk)
            assert isinstance(token, Resolved)
            frame = Frame(1, 1, live.stream, "s", token.message.mid, live.flags, len(hk), hk, token)
        case _:
            frame = live
    # the token never outlives a rebuilt frame; a foreign one is refused by the decode itself
    result = decoder.decode(frame)
    expected = _expected(codec, frame.data)
    if isinstance(expected, DecodeError):
        assert result == expected
    else:
        assert isinstance(result, Decoded) and result.packet == expected
    if frame.checked is not None:
        direct = codec.decode_resolved(TLM, frame.checked, frame.data)
        assert direct == expected or (
            isinstance(direct, DecodeError) and isinstance(expected, DecodeError)
        )


def test_decode_resolved_refuses_a_token_from_another_codec_or_other_bytes() -> None:
    first, second = _codec(), _codec()
    hk = first.encode_telemetry("MOCK_HK").data
    token = first.resolve(TLM, hk)
    assert isinstance(token, Resolved)
    assert second.decode_resolved(TLM, token, hk) == second.decode(TLM, hk)
    twin = bytes(bytearray(hk))  # equal bytes, other object: the token is not theirs
    assert twin == hk and twin is not hk
    assert first.decode_resolved(TLM, token, twin) == first.decode(TLM, twin)
    status = first.encode_telemetry("MOCK_STATUS").data
    assert first.decode_resolved(TLM, token, status) == first.decode(TLM, status)
    assert first.decode_resolved(MessageKind.COMMAND, token, hk) == first.decode(
        MessageKind.COMMAND, hk
    )
    assert first.decode_resolved(TLM, token, bytearray(hk)) == first.decode(TLM, hk)
    assert first.decode_resolved(TLM, token, hk[:-1]) == first.decode(TLM, hk[:-1])


def test_replace_and_make_go_through_the_frame_checks_and_drop_the_token() -> None:
    codec = _codec()
    hk = codec.encode_telemetry("MOCK_HK").data
    live = _live_frame(codec, hk)
    assert live.checked is not None
    assert live._replace(source="elsewhere").checked is None
    assert Frame._make(list(live)).checked is None
    if __debug__:
        with pytest.raises(AssertionError):
            live._replace(seq=0)
        with pytest.raises(AssertionError):
            live._replace(flags=FrameFlag.GARBAGE)
        with pytest.raises(AssertionError):
            Frame._make([0, -1, live.stream, "s", None, FrameFlag.TRUNCATED, 0, b""])
    with pytest.raises(ValueError):
        live._replace(nonsense=1)


def test_a_live_frame_is_hashable_picklable_and_equals_its_replay_built_twin() -> None:
    codec = _codec()
    hk = codec.encode_telemetry("MOCK_HK").data
    live = _live_frame(codec, hk)
    replayed = Frame(1, 1, _stream(), "src", live.mid, live.flags, len(hk), hk)
    assert replayed.checked is None and live.checked is not None
    assert live == replayed and hash(live) == hash(replayed)
    assert {live, replayed} == {live}
    assert live != live._replace(seq=2)
    for clone in (
        pickle.loads(pickle.dumps(live)),  # noqa: S301 - our own bytes
        copy.copy(live),
        copy.deepcopy(live),
    ):
        assert clone == live and clone.checked is None
    assert pickle.loads(pickle.dumps(replayed)) == replayed  # noqa: S301 - our own bytes


@settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(seq=st.integers(1, 10**6), source=st.text(max_size=8), flip=st.booleans())
def test_frame_equality_depends_on_the_packet_not_on_the_token(
    seq: int, source: str, flip: bool
) -> None:
    codec = _codec()
    hk = codec.encode_telemetry("MOCK_HK").data
    token = codec.resolve(TLM, hk)
    assert isinstance(token, Resolved)
    args = (seq, 5, _stream(), source, token.message.mid, FrameFlag.HAS_MID, len(hk), hk)
    a, b = Frame(*args, token), Frame(*args)
    assert (a == b) and (hash(a) == hash(b)) and (a != b) is False
    if flip:
        assert a != Frame(*(seq + 1, *args[1:]))


# -- follow-ups --


class _Flaky:
    def __init__(self) -> None:
        self.seen = 0
        self.fail_next = True

    def append(self, frame: Frame) -> None:
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("disk hiccup")
        self.seen += 1


def test_a_sink_re_added_after_a_failure_survives_the_old_holders_release(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> tuple[bool, bool, int]:
        async with _ground(mock_config_path, tmp_path) as ground:
            data = ground.codec_for("fc").encode_telemetry("MOCK_HK").data
            sink = _Flaky()
            ground.add_sink(sink)  # holder A
            first = await ground.capture([ground.route("fc")])
            address = first.addresses[0][1]
            assert address is not None
            await _burst(address, data, 1)
            await _until(lambda: first.stats().frames == 1)
            failed_out = sink not in ground._engine._sinks
            await first.close()
            second = await ground.capture([ground.route("fc")], sinks=[sink])  # holder B
            ground.remove_sink(sink)  # holder A lets go
            address = second.addresses[0][1]
            assert address is not None
            await _burst(address, data, 5)
            await _until(lambda: second.stats().frames == 5)
            attached = sink in ground._engine._sinks
            await second.close()
            return failed_out, attached, sink.seen

    failed_out, attached, seen = _run(main())
    assert failed_out and attached and seen == 5


def test_the_waiting_reader_keeps_the_capture_and_the_second_one_is_refused(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> tuple[IronbirdError | None, int]:
        async with _ground(mock_config_path, tmp_path) as ground:
            capture = await ground.capture([ground.route("fc")])
            address = capture.addresses[0][1]
            assert address is not None

            async def first() -> int:
                return len([f async for f in _take(capture, 3)])

            owner = asyncio.ensure_future(first())
            await asyncio.sleep(0.05)  # parked, waiting for frames
            late: IronbirdError | None = None
            try:
                await capture.__aiter__().__anext__()
            except IronbirdError as exc:
                late = exc
            await _burst(address, ground.codec_for("fc").encode_telemetry("MOCK_HK").data, 3)
            got = await owner  # the waiting reader got the frames; it was never the one refused
            await capture.close()
            return late, got

    late, got = _run(main())
    assert late is not None and late.code is Code.INTERNAL and "one reader" in late.summary
    assert got == 3


async def _take(capture: Any, count: int) -> AsyncIterator[Frame]:
    seen = 0
    async for frame in capture:
        yield frame
        seen += 1
        if seen == count:
            return


def test_a_slow_transport_open_is_bounded_and_close_does_not_wait_for_it(
    mock_config_path: Path, tmp_path: Path
) -> None:
    async def main() -> tuple[float, str, str]:
        ground = _ground(mock_config_path, tmp_path)
        loop = asyncio.get_running_loop()
        real = loop.getaddrinfo

        async def slow(*args: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(3)  # a resolver that does not answer in time
            return await real(*args, **kwargs)

        loop.getaddrinfo = slow  # type: ignore[method-assign,assignment]
        route = ground.route("fc")
        data = ground.codec_for("fc").encode_command("MOCK_NOOP").data
        sending = asyncio.ensure_future(ground._engine.send(route.link, LinkEnd.SOURCE, data))
        await asyncio.sleep(0.05)
        capturing = asyncio.ensure_future(ground.capture([route]))
        await asyncio.sleep(0.05)
        started = time.perf_counter()
        await ground._engine.close()
        took = time.perf_counter() - started
        outcomes = await asyncio.gather(sending, capturing, return_exceptions=True)
        texts = []
        for outcome in outcomes:
            assert isinstance(outcome, IronbirdError), outcome
            texts.append(outcome.summary)
        return took, texts[0], texts[1]

    took, send_text, capture_text = _run(main())
    assert took < 1.0
    assert "closing" in send_text or "closed" in send_text
    assert "closed" in capture_text or "closing" in capture_text


def test_a_live_frame_decodes_through_its_token_without_checking_again(
    token_probe: _Probe, monkeypatch: pytest.MonkeyPatch
) -> None:
    probe = token_probe
    data = probe.codec.encode_telemetry("MOCK_HK").data
    probe.io[LinkEnd.SOURCE].on_data(data, 1, "p", len(data), FrameFlag.NONE)
    frame = probe.sink.frames[-1]
    assert frame.checked is not None
    expected = probe.codec.decode(TLM, frame.data)

    def refuse(*_: object) -> None:
        raise AssertionError("the whole decode ran for a frame that carries its check")

    monkeypatch.setattr(probe.codec, "resolve", refuse)
    live = probe.engine.decoder(report_checked=False).decode(frame)
    assert isinstance(live, Decoded) and live.packet == expected


# -- datagrams buffered when a link end closes are counted --


def test_udp_close_counts_the_datagrams_still_in_its_socket_buffer() -> None:
    async def main() -> int:
        transport = UdpTransport(local=Endpoint("127.0.0.1", 0))
        await transport.open(lambda *_: None)
        address = transport.local_address
        assert address is not None
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for _ in range(50):
            sock.sendto(b"x" * 20, address)
        sock.close()
        await transport.close()  # the loop never got to read them
        return transport.stats().unread_at_close

    assert _run(main()) == 50


def test_closing_an_overloaded_listener_reports_what_it_left_unread(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ironbird.engine.CONSUMER_QUEUE_FRAMES", 4)
    notices: list[Notice] = []

    async def main() -> None:
        ground = Session(
            load_config(mock_config_path),
            role="ground",
            ledger=Ledger(tmp_path / "ledger", session_id="r9"),
            notify=notices.append,
        )
        async with ground:
            data = ground.codec_for("fc").encode_telemetry("MOCK_HK").data
            listener = await ground.listen("fc")
            address = listener.local_address
            assert address is not None
            await _burst(address, data, 10)
            await _until(lambda: listener.capture.stats().frames == 10)
            await _burst(address, data, 7)  # still in the socket buffer when the end closes
            await listener.close()

    _run(main())
    lag = [n.summary for n in notices if n.code is Code.ENG_LAG]
    assert any("received but not shown" in line and "4 frame" in line for line in lag), lag
    lines = [n.summary for n in notices if n.code is Code.TX_OVERRUN]
    assert any("7 more datagram" in line and "socket buffer" in line for line in lines), lines
