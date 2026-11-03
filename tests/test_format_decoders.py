"""The body decoders of the recording format: every defect in an untrusted record body is a
``FormatError`` (never another exception), and every valid body round-trips.

The reader bounds each body's length before it calls a decoder, so the bodies here are the
shapes the reader lets through: the right size for the type, wrong inside.
"""

from __future__ import annotations

import json
import struct
from hashlib import sha256

import pytest

from ironbird.ccdd.model import MessageKind
from ironbird.frame import Direction, GapReason, Stream
from ironbird.recording import format as fmt
from ironbird.recording.format import (
    BlobKind,
    Chunk,
    FormatError,
    Gap,
    MarkKind,
)

STREAM = Stream(3, "ground-fc", "fc", MessageKind.TELEMETRY, Direction.RX, "udp", "udp 1.2.3.4:5")


def good_chunk(**changes: object) -> Chunk:
    base: dict[str, object] = {
        "chunk_start": 64,
        "first_seq": 1,
        "last_seq": 10,
        "min_time_ns": 100,
        "max_time_ns": 200,
        "first_mono_ns": 1,
        "last_mono_ns": 2,
        "frames": 10,
        "bytes": 400,
        "frames_without_mid": 1,
        "flags": 0,
        "mids": ((0x0801, 4), (0x0802, 5)),
    }
    base.update(changes)
    return Chunk(**base)  # type: ignore[arg-type]


def blob_body(
    *,
    node: bytes = b"fc",
    adapter: bytes = b"ccdd_json",
    content: bytes = b"{}",
    digest: bytes = b"",
) -> bytes:
    digest = digest or sha256(content).digest()
    return b"".join(
        (
            bytes([BlobKind.CCDD, len(node)]),
            node,
            bytes([len(adapter)]),
            adapter,
            digest,
            content,
        )
    )


# -- META --


@pytest.mark.parametrize(
    "body",
    [
        b"\xff\xfe not json",
        b"[1, 2]",
        b'"text"',
        json.dumps({"schema": "something-else/9", "max_packet_bytes": 64}).encode(),
        json.dumps({"max_packet_bytes": 64}).encode(),
        json.dumps({"schema": fmt.META_SCHEMA}).encode(),
        json.dumps({"schema": fmt.META_SCHEMA, "max_packet_bytes": True}).encode(),
        json.dumps({"schema": fmt.META_SCHEMA, "max_packet_bytes": 0}).encode(),
        json.dumps({"schema": fmt.META_SCHEMA, "max_packet_bytes": "64"}).encode(),
        json.dumps({"schema": fmt.META_SCHEMA, "max_packet_bytes": 64, "part": 0}).encode(),
        json.dumps({"schema": fmt.META_SCHEMA, "max_packet_bytes": 64, "part": 2**32}).encode(),
        json.dumps(
            {"schema": fmt.META_SCHEMA, "max_packet_bytes": 64, "previous_last_seq": -1}
        ).encode(),
        b"[" * 5000 + b"]" * 5000,
    ],
)
def test_meta_defects_are_format_errors(body: bytes) -> None:
    with pytest.raises(FormatError):
        fmt.decode_meta(body)


def test_meta_nested_past_the_limit_is_refused_and_at_the_limit_accepted() -> None:
    def nest(levels: int) -> dict[str, object]:
        value: object = 1
        for _ in range(levels):
            value = {"k": value}
        return {"schema": fmt.META_SCHEMA, "max_packet_bytes": 64, "deep": value}

    # The top-level object is level 1, so "deep" adds `levels` more.
    fmt.decode_meta(fmt.encode_meta(nest(fmt.MAX_META_DEPTH - 1)))
    with pytest.raises(FormatError, match="nested"):
        fmt.decode_meta(fmt.encode_meta(nest(fmt.MAX_META_DEPTH + 1)))


def test_meta_round_trips_with_defaults() -> None:
    meta = fmt.decode_meta(
        fmt.encode_meta({"schema": fmt.META_SCHEMA, "max_packet_bytes": 1500, "extra": [1]})
    )
    assert (meta.max_packet_bytes, meta.part, meta.previous_last_seq) == (1500, 1, 0)
    assert meta.data["extra"] == [1]


# -- BLOB --


def test_blob_round_trips() -> None:
    content = b"a,b,c"
    encoded = fmt.encode_blob(BlobKind.CCDD, "fc", "csv", sha256(content).digest(), content)
    blob = fmt.decode_blob(encoded, sha256)
    assert (blob.kind, blob.node, blob.adapter, blob.content) == (
        BlobKind.CCDD,
        "fc",
        "csv",
        content,
    )


@pytest.mark.parametrize(
    "body",
    [
        b"\x01\x05fc",  # node length beyond the body
        b"\x01",  # nothing after the kind byte
        b"",  # not even a kind
        blob_body(node=b"f\xffc"),
        blob_body(node=b"bad node"),
        blob_body(adapter=b"bad adapter"),
        b"\x01\x02fc\x03abc" + b"\x00" * 31,  # one byte short of a digest
        blob_body(content=b"x", digest=b"\x00" * 32),  # digest does not match
        bytes([9]) + blob_body()[1:],  # undefined kind
    ],
)
def test_blob_defects_are_format_errors(body: bytes) -> None:
    with pytest.raises(FormatError):
        fmt.decode_blob(body, sha256)


def test_blob_content_over_the_cap_is_refused_even_with_a_valid_digest() -> None:
    content = b"\x00" * (fmt.MAX_BLOB_CONTENT + 1)
    body = blob_body(content=content)
    with pytest.raises(FormatError, match="larger"):
        fmt.decode_blob(body, sha256)


# -- STREAM --


def test_stream_round_trips() -> None:
    assert fmt.decode_stream(fmt.encode_stream(STREAM)) == STREAM


def stream_body(
    *,
    link: bytes = b"ground-fc",
    node: bytes = b"fc",
    transport: bytes = b"udp",
    endpoint: bytes = b"udp 1.2.3.4:5",
    kind: int = 1,
    direction: int = 1,
    sid: int = 3,
    endpoint_len: int | None = None,
) -> bytes:
    return b"".join(
        (
            struct.pack("<HBB", sid, kind, direction),
            bytes([len(link)]),
            link,
            bytes([len(node)]),
            node,
            bytes([len(transport)]),
            transport,
            struct.pack("<H", len(endpoint) if endpoint_len is None else endpoint_len),
            endpoint,
        )
    )


def test_stream_body_helper_is_valid() -> None:
    # The fixture itself decodes, so each defect below is the only thing wrong with its body.
    assert fmt.decode_stream(stream_body()).transport == "udp"


@pytest.mark.parametrize(
    "body",
    [
        b"\x00\x00",  # shorter than the fixed head
        stream_body(kind=0x7F),
        stream_body(direction=0x7F),
        stream_body(sid=fmt.NO_STREAM),
        stream_body(transport=b"carrier-pigeon"),
        stream_body(endpoint=b"\xff\xfe"),
        stream_body(endpoint_len=3),  # length disagrees with what follows
        stream_body(link=b"Bad Link"),
        stream_body(node=b"n\xe9"),
        stream_body()[:21],  # ends before the endpoint length field
        stream_body(link=b"x" * 200)[:8],  # a length-prefixed string running off the record
    ],
)
def test_stream_defects_are_format_errors(body: bytes) -> None:
    with pytest.raises(FormatError):
        fmt.decode_stream(body)


def test_stream_endpoint_is_sanitized_and_bounded() -> None:
    nasty = "udp \x1b[31mred\x07" + "z" * 1000
    stream = fmt.decode_stream(stream_body(endpoint=nasty.encode()))
    assert "\x1b" not in stream.endpoint and "\x07" not in stream.endpoint
    assert len(stream.endpoint) < fmt.MAX_TEXT * 2  # escapes may lengthen the kept text a little


# -- SOURCE --


def test_source_round_trips_and_sanitizes() -> None:
    assert fmt.decode_source(fmt.encode_source(7, "10.0.0.1:99")) == (7, "10.0.0.1:99")
    _, text = fmt.decode_source(struct.pack("<H", 7) + b"a\x1b[2Jb")
    assert "\x1b" not in text


@pytest.mark.parametrize(
    "body", [struct.pack("<H", fmt.MAX_SOURCES) + b"x", struct.pack("<H", 1) + b"\xff\xfe"]
)
def test_source_defects_are_format_errors(body: bytes) -> None:
    with pytest.raises(FormatError):
        fmt.decode_source(body)


# -- GAP --


def gap_body(**changes: int) -> bytes:
    fields = {
        "stream": 1,
        "reason": int(GapReason.KERNEL_DROP),
        "zero8": 0,
        "zero32": 0,
        "count": 5,
        "first_ns": 10,
        "last_ns": 20,
        "first_seq": 3,
        "last_seq": 7,
    }
    fields.update(changes)
    return struct.pack("<HBBIQQQQQ", *fields.values())


def test_gap_round_trips() -> None:
    gap = Gap(2, GapReason.LINK_DOWN, 9, 5, 6, 1, 9)
    assert fmt.decode_gap(fmt.encode_gap(gap)) == gap
    assert fmt.decode_gap(gap_body()).reason is GapReason.KERNEL_DROP


@pytest.mark.parametrize(
    "changes",
    [{"zero8": 1}, {"zero32": 1}, {"reason": 99}, {"reason": 0}, {"count": 0}, {"first_seq": 8}],
)
def test_gap_defects_are_format_errors(changes: dict[str, int]) -> None:
    with pytest.raises(FormatError):
        fmt.decode_gap(gap_body(**changes))


# -- MARK --


def test_mark_round_trips_and_keeps_unknown_kinds() -> None:
    mark = fmt.decode_mark(fmt.encode_mark(123, MarkKind.NOTE, "hello"))
    assert (mark.time_ns, mark.kind, mark.text) == (123, MarkKind.NOTE, "hello")
    future = fmt.decode_mark(struct.pack("<QBxxx", 1, 200) + b"later minor version")
    assert future.kind == 200


@pytest.mark.parametrize(
    "body",
    [
        struct.pack("<QB", 1, 7) + b"\x01\x00\x00",  # reserved bytes set
        struct.pack("<QBxxx", 1, 7) + b"\xff\xfe",  # text is not UTF-8
    ],
)
def test_mark_defects_are_format_errors(body: bytes) -> None:
    with pytest.raises(FormatError):
        fmt.decode_mark(body)


def test_mark_text_is_sanitized() -> None:
    mark = fmt.decode_mark(fmt.encode_mark(1, MarkKind.NOTE, "ok\x1b[2J\x00gone"))
    assert "\x1b" not in mark.text and "\x00" not in mark.text


# -- CHUNK --


def test_chunk_round_trips() -> None:
    chunk = good_chunk()
    assert fmt.decode_chunk(fmt.encode_chunk(chunk)) == chunk


def test_chunk_with_mid_overflow_may_count_fewer_frames_than_it_holds() -> None:
    chunk = good_chunk(flags=fmt.CHUNK_FLAG_MID_OVERFLOW, frames=50)
    decoded = fmt.decode_chunk(fmt.encode_chunk(chunk))
    assert decoded.mid_overflow


@pytest.mark.parametrize(
    "changes",
    [
        {"mids": ((5, 1), (5, 1)), "frames": 3, "frames_without_mid": 1},  # not strictly sorted
        {"mids": ((9, 1), (5, 1)), "frames": 3, "frames_without_mid": 1},  # descending
        {"frames": 3},  # MID counts exceed the frames the chunk holds
        {"frames": 11},  # MID counts do not add up
        {"flags": 0x8000},  # unknown flag bits
        {"first_seq": 11, "last_seq": 10},
        {"min_time_ns": 300, "max_time_ns": 200},
    ],
)
def test_chunk_defects_are_format_errors(changes: dict[str, object]) -> None:
    body = fmt.encode_chunk(good_chunk(**changes))
    with pytest.raises(FormatError):
        fmt.decode_chunk(body)


def test_chunk_table_length_must_match_its_count() -> None:
    body = fmt.encode_chunk(good_chunk())
    with pytest.raises(FormatError, match="MID table"):
        fmt.decode_chunk(body + b"\x00" * 8)
    with pytest.raises(FormatError, match="MID table"):
        fmt.decode_chunk(body[:-8])
    huge = bytearray(body[: fmt.CHUNK_FIXED_SIZE])
    struct.pack_into("<H", huge, fmt.CHUNK_FIXED_SIZE - 2, fmt.MAX_CHUNK_MIDS + 1)
    with pytest.raises(FormatError, match="MID table"):
        fmt.decode_chunk(bytes(huge))
