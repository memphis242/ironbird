"""Differential test of the filter compiler against an independently written reference.

Random expressions over a small CCDD are compiled and run against a pool of random frames
(valid, mutated, truncated, cut, garbage; with and without the live check token); each verdict
must equal :class:`~filter_ref_independent.IndependentReference`'s. The candidate MID set must
never exclude a match, and a frame outside it, or any frame for a filter that needs no decode,
must never reach the decoder.
"""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import pytest

from conftest import EXAMPLES
from filter_ref_independent import IndependentReference
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec
from ironbird.errors import DecodeError
from ironbird.filter import FilterError, compile_filter, parse
from ironbird.frame import Decoded, Direction, Frame, FrameFlag, Stream
from ironbird.header import load_profile

NODE: Final = "s"


def _tlm(name: str, mid: int, category: str, fields: list[dict[str, Any]], **extra: Any) -> Any:
    return {
        "name": name,
        "kind": "telemetry",
        "mid": hex(mid),
        "category": category,
        "fields": fields,
        **extra,
    }


def _cmd(name: str, fc: int, fields: list[dict[str, Any]]) -> Any:
    return {
        "name": name,
        "kind": "command",
        "mid": "0x1880",
        "function_code": fc,
        "fields": fields,
    }


CCDD: Final = {
    "schema": "ironbird-ccdd/0",
    "target": NODE,
    "messages": [
        _tlm(
            "A",
            0x0800,
            "housekeeping",
            [
                {"name": "cnt", "type": "uint8"},
                {"name": "temp", "type": "float32"},
                {"name": "mode", "type": "uint8", "enum": {"SAFE": 1, "NOMINAL": 2}},
                {"name": "lvl", "type": "int16"},
                {"name": "big", "type": "uint64"},
                {"name": "name", "type": "char[8]"},
                {"name": "d", "type": "float64"},
                {"name": "blob", "type": "bytes[2]"},
            ],
        ),
        _tlm(
            "B",
            0x0801,
            "status",
            [
                {"name": "cnt", "type": "uint16"},
                {"name": "mode", "type": "uint8", "enum": {"SAFE": 1, "SCIENCE": 3}},
            ],
        ),
        _tlm("Cm", 0x0802, "housekeeping", [{"name": "other", "type": "uint8"}]),
        _tlm(
            "EV",
            0x0803,
            "event",
            [
                {"name": "app", "type": "char[8]"},
                {"name": "eid", "type": "uint16"},
                {
                    "name": "etype",
                    "type": "uint16",
                    "enum": {"DEBUG": 1, "INFORMATION": 2, "ERROR": 3, "CRITICAL": 4},
                },
                {"name": "text", "type": "char[16]"},
            ],
            event={"app": "app", "id": "eid", "type": "etype", "message": "text"},
        ),
        _cmd("NOOP", 0, []),
        _cmd("SET", 1, [{"name": "cnt", "type": "uint8"}]),
    ],
}

ATOMS: Final = [
    "seq > {i}", "seq == {i}", "len >= {i}", "len < {i}", "seq >= {i}.5", "seq < {i}.5",
    "mid == {m}", "mid != {m}", "mid in [{m}, {m}]",
    "node == s", "link == l1", "link != l2", "dir == rx", "dir == tx",
    "kind == command", "kind == telemetry",
    'source contains "127"', "truncated == true", "garbage == false", "garbage == true",
    "status == ok", "status == malformed", "status in [unknown, malformed]", "status != ok",
    "category == housekeeping", "category in [status, event]", "category != housekeeping",
    "msg == A", "msg == B", "msg != A", "msg in [NOOP, SET]", 'msg contains "E"', "msg == EV",
    "fc == 1", "fc != 0", "fc >= 0",
    "cnt == {s}", "cnt != {s}", "cnt > {s}", "cnt in [{s}, 300, 7]", "cnt >= 255.5",
    "mode == SAFE", "mode != SCIENCE", "mode in [SAFE, SCIENCE]", "mode == 2", "mode < 3",
    "mode in [NOMINAL, SCIENCE]", "mode != NOMINAL", "not mode == SCIENCE",
    "A.cnt == {s}", "B.cnt != {s}", "A.temp < 0.5", "temp >= -0.0", "temp != 1", "temp == 0.1",
    "temp <= 0.1", "temp in [0.1, 0.25]", "temp > 1" + "0" * 30,
    "lvl < -{s}", "lvl == -1", "lvl > -0.5", "lvl <= -0.5",
    'name == "abc"', 'name contains "b"', 'name != "abc"', 'blob == "0aff"', 'blob contains "ff"',
    'blob != "00"',
    "big > {s}", "big > 18446744073709551614.5", "big < 18446744073709551615.5",
    "big >= 18446744073709551615.0", "big == 18446744073709551615", "d <= 0.25", "d == 0.1",
    "other == 1",
    "hdr.apid == {a}", "hdr.sequence < {s}", "hdr.length > 10",
    "event.id == {s}", "event.type >= error", "event.type > info", "event.type == information",
    "event.type != error", "event.type < critical", "event.type in [debug, critical]",
    'event.app == "APP"', 'event.text contains "x"',
    "time >= 2027-01-15T08:00:00Z", "time < 2027-01-15T09:00:00+01:00",
    "SET.cnt == {s}",
]  # fmt: skip
MIDS: Final = [0x800, 0x801, 0x802, 0x803, 0x1880, 0x804, 0x7FF]
EPOCH_NS: Final = int(datetime(2027, 1, 15, 8, tzinfo=UTC).timestamp()) * 10**9
BASE_NS: Final = 1_800_000_000_000_000_000


@pytest.fixture(scope="module")
def codec(tmp_path_factory: pytest.TempPathFactory) -> Codec:
    path: Path = tmp_path_factory.mktemp("filter-diff") / "s.json"
    path.write_text(json.dumps(CCDD))
    return Codec(
        load_ccdd(path),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
        clock=lambda: 1000.5,
    )


def stream(kind: MessageKind, **changes: Any) -> Stream:
    fields: dict[str, Any] = {
        "id": 0 if kind is MessageKind.TELEMETRY else 1,
        "link": "l1",
        "node": NODE,
        "kind": kind,
        "direction": Direction.RX,
        "transport": "udp",
        "endpoint": "udp x",
    }
    return Stream(**{**fields, **changes})


def frame_of(
    codec: Codec,
    st: Stream,
    data: bytes,
    *,
    seq: int,
    time_ns: int,
    live: bool,
    cut: int = 0,
    garbage: bool = False,
) -> Frame:
    mid = None if garbage else codec.peek_mid(st.kind, data)
    flags = FrameFlag.NONE
    if mid is not None:
        flags |= FrameFlag.HAS_MID
    if cut:
        flags |= FrameFlag.TRUNCATED
    if garbage:
        flags |= FrameFlag.GARBAGE
    checked = None
    if live and not cut and not garbage:
        resolved = codec.resolve(st.kind, data)
        checked = None if isinstance(resolved, DecodeError) else resolved
    return Frame(seq, time_ns, st, "127.0.0.1:9", mid, flags, len(data) + cut, data, checked)


def decode(codec: Codec, frame: Frame) -> Decoded | DecodeError:
    if frame.flags & FrameFlag.TRUNCATED:
        return codec.oversize_error(frame.stream.kind, frame.data, frame.orig_len)
    result = codec.decode(frame.stream.kind, frame.data)
    return result if isinstance(result, DecodeError) else Decoded(frame, result)


def random_frames(codec: Codec, rng: random.Random, count: int) -> list[Frame]:
    frames: list[Frame] = []
    for _ in range(count):
        kind = rng.random()
        seq = rng.randint(1, 60)
        time_ns = rng.choice([BASE_NS, BASE_NS + 10**13, EPOCH_NS, EPOCH_NS - 1])
        live = rng.random() < 0.5
        if kind < 0.55:
            name = rng.choice(["A", "B", "Cm", "EV"])
            values: dict[str, dict[str, Any]] = {
                "A": {
                    "cnt": rng.choice([0, 5, 7, 255]),
                    "temp": rng.choice([0.1, -0.0, 0.0, 1.0, float("nan"), float("inf")]),
                    "mode": rng.choice([1, 2, 3, 0]),
                    "lvl": rng.choice([-1, -300, 5]),
                    "big": rng.choice([0, 7, 2**64 - 1]),
                    "name": rng.choice(["abc", "xbz", ""]),
                    "d": rng.choice([0.25, 0.1]),
                    "blob": rng.choice([b"\x0a\xff", b"\x00\x00"]),
                },
                "B": {"cnt": rng.choice([5, 300, 7]), "mode": rng.choice([1, 3, 2])},
                "Cm": {"other": rng.choice([0, 1])},
                "EV": {
                    "app": rng.choice(["APP", "X"]),
                    "eid": rng.choice([5, 7]),
                    "etype": rng.choice([1, 2, 3, 4, 0, 7]),
                    "text": rng.choice(["x", "y"]),
                },
            }
            data = bytearray(codec.encode_telemetry(name, values[name]).data)
            if rng.random() < 0.2:
                data[rng.randrange(len(data))] ^= 1 << rng.randrange(8)
            if rng.random() < 0.1:
                del data[rng.randrange(len(data)) :]
            cut = rng.choice([0, 0, 0, 5])
            if cut:
                data += bytes(codec.max_packet_bytes - len(data))
            st = stream(
                MessageKind.TELEMETRY,
                direction=rng.choice(list(Direction)),
                link=rng.choice(["l1", "l2"]),
            )
            frames.append(
                frame_of(codec, st, bytes(data), seq=seq, time_ns=time_ns, live=live, cut=cut)
            )
        elif kind < 0.8:
            encoded = (
                codec.encode_command("SET", {"cnt": rng.choice([0, 5, 7])})
                if rng.random() < 0.6
                else codec.encode_command("NOOP")
            )
            data = bytearray(encoded.data)
            if rng.random() < 0.25:
                data[rng.randrange(len(data))] ^= 1 << rng.randrange(8)
            st = stream(MessageKind.COMMAND, link=rng.choice(["l1", "l2"]))
            frames.append(frame_of(codec, st, bytes(data), seq=seq, time_ns=time_ns, live=live))
        elif kind < 0.9:
            data = bytes(rng.randrange(256) for _ in range(rng.randint(0, 30)))
            st = stream(rng.choice(list(MessageKind)))
            frames.append(frame_of(codec, st, data, seq=seq, time_ns=time_ns, live=live))
        else:
            # Serial garbage; half of it spells a valid packet, which must not matter.
            if rng.random() < 0.5:
                data = codec.encode_telemetry("A", {"cnt": 5, "mode": 1}).data
            else:
                data = bytes(rng.randrange(256) for _ in range(rng.randint(1, 30)))
            st = stream(MessageKind.TELEMETRY, transport="serial")
            frames.append(
                frame_of(codec, st, data, seq=seq, time_ns=time_ns, live=False, garbage=True)
            )
    return frames


def random_expression(rng: random.Random, depth: int = 0) -> str:
    roll = rng.random()
    if depth > 3 or roll < 0.35:
        return rng.choice(ATOMS).format(
            i=rng.randint(0, 60),
            m=hex(rng.choice(MIDS)),
            s=rng.choice([0, 1, 5, 7, 255, 300]),
            a=rng.choice([0, 1, 2, 3]),
        )
    if roll < 0.5:
        return "not " + random_expression(rng, depth + 1)
    joiner = " and " if roll < 0.75 else " or "
    parts = (random_expression(rng, depth + 1) for _ in range(rng.randint(2, 4)))
    return "(" + joiner.join(parts) + ")"


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_compiled_filters_equal_the_independent_reference(codec: Codec, seed: int) -> None:
    rng = random.Random(seed)
    codecs = {NODE: codec}
    reference = IndependentReference(codecs)
    frames = random_frames(codec, rng, 200)
    compiled = refused = 0
    for _ in range(400):
        text = random_expression(rng)
        try:
            flt = compile_filter(text, codecs, links=["l1", "l2"])
        except FilterError:
            refused += 1  # e.g. a filter that can never match; the compiler's refusal
            continue
        compiled += 1
        tree = parse(text)
        seen: list[Frame] = []

        def recording(frame: Frame, seen: list[Frame] = seen) -> Decoded | DecodeError:
            seen.append(frame)
            return decode(codec, frame)

        matcher = flt.bind(recording, lambda _node: codec)
        for frame in frames:
            seen.clear()
            got = matcher(frame)
            assert got == reference.matches(tree, frame), (text, frame)
            if flt.mids is not None:
                assert frame.mid in flt.mids or not got, (text, "a match outside the candidates")
                assert all(f.mid in flt.mids for f in seen), (text, "decoded a non-candidate")
            if not flt.needs_decode:
                assert not seen, (text, "decoded though the filter needs no decode")
            assert not any(f.flags & FrameFlag.GARBAGE for f in seen), (text, "decoded garbage")
    assert compiled > refused, f"{refused} of {compiled + refused} expressions refused"
