"""Stage 2i: header profile flexibility. Per-field byte order, a trailer, checksum spans, a MID
spread over the header, header values per message and per send, the time epoch, framing by the
length field, and (first of all) proof that the shipped profiles did not change."""

from __future__ import annotations

import binascii
import hashlib
import json
import re
import socket
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from typer.testing import CliRunner

import golden_2i
from conftest import CORPUS, EXAMPLES
from filter_defs import CCDD_N1, STREAMS, count_decodes, make_frame
from ironbird import checksum, header
from ironbird.ccdd.json_adapter import parse_model
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.cli.main import _parse_header_values, app
from ironbird.codec import Codec, DecodedPacket
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.filter import FilterError, compile_filter
from ironbird.header import Header, HeaderProfile
from test_header_props import crc16_bitwise

CMD, TLM = MessageKind.COMMAND, MessageKind.TELEMETRY
HEADERS = EXAMPLES / "headers"
UNIX = datetime(1970, 1, 1, tzinfo=UTC)


def prof(
    fields: list[dict[str, Any]],
    *,
    kind: str = "telemetry",
    mid: tuple[str, ...] = ("mid",),
    trailer: list[dict[str, Any]] | None = None,
    time: dict[str, Any] | None = None,
    fc: str | None = None,
) -> HeaderProfile:
    doc: dict[str, Any] = {"name": "t", "kind": kind, "mid_fields": list(mid), "fields": fields}
    if trailer is not None:
        doc["trailer"] = {"fields": trailer}
    if time is not None:
        doc["time"] = time
    if fc is not None:
        doc["function_code_field"] = fc
    return header.parse_profile(doc, "t.toml")


def rejected(doc_edit: Any, needle: str, base: dict[str, Any] | None = None) -> None:
    doc = base or {
        "name": "t",
        "kind": "telemetry",
        "mid_fields": ["mid"],
        "fields": [
            {"name": "mid", "bits": 16, "computed": "from_mid"},
            {"name": "length", "bits": 16, "computed": "length"},
        ],
    }
    doc = json.loads(json.dumps(doc))
    doc_edit(doc)
    with pytest.raises(IronbirdError) as info:
        header.parse_profile(doc, "bad.toml")
    assert info.value.code is Code.HDR_SCHEMA
    assert needle in info.value.summary, info.value.summary
    assert info.value.summary.startswith("bad.toml: ")


# -- the shipped profiles did not change -----------------------------------------------------


def test_shipped_profiles_encode_and_decode_byte_for_byte_as_before_2i() -> None:
    """``tests/golden/pre2i_header_vectors.json`` was produced by the code as it stood before
    Stage 2i (``python tests/golden_2i.py``): every message of both shipped CCDD files encoded
    twice, then tens of thousands of decode, check, unpack and peek results over valid packets,
    truncations, extensions, bit flips and the malformed-packet corpus."""
    golden = json.loads(golden_2i.GOLDEN.read_text())
    assert golden_2i.encodes() == golden["encodes"]
    lines = golden_2i.results()
    assert len(lines) == golden["result_lines"] > 20_000
    assert hashlib.sha256("\n".join(lines).encode()).hexdigest() == golden["result_sha256"]
    assert lines[:12] + lines[-6:] == golden["result_sample"]


def test_shipped_profiles_use_none_of_the_new_features() -> None:
    for name in ("ccsds_v1_cfs_cmd", "ccsds_v1_cfs_tlm"):
        profile = header.load_profile(HEADERS / f"{name}.toml")
        assert profile.trailer_size == 0 and not profile.swapped_fields
        assert len(profile.mid_windows) == 1 and profile.mid_single == (0, 2, 0, 0xFFFF, "big")
        assert all(not f.little and f.span == header.Span() for f in profile.fields)
    tlm = header.load_profile(HEADERS / "ccsds_v1_cfs_tlm.toml")
    assert tlm.time is not None and tlm.time.epoch_s == 0 and tlm.time.leap_seconds == 0


# -- per-field byte order ---------------------------------------------------------------------

LE_FIELDS: list[dict[str, Any]] = [
    {"name": "mid", "bits": 16, "computed": "from_mid"},
    {"name": "length", "bits": 16, "computed": "length", "endian": "little"},
    {"name": "function_code", "bits": 8},
    {"name": "counter", "bits": 32, "default": 0x01020304, "endian": "little"},
    {"name": "checksum", "bits": 8, "computed": "checksum", "algorithm": "xor8_cfs"},
]


def test_little_endian_fields_are_stored_least_significant_byte_first() -> None:
    profile = prof(LE_FIELDS, kind="command", fc="function_code")
    packet = header.pack(profile, b"", mid=0x1234, inputs={"function_code": 7})
    body = bytes.fromhex("12340a000704030201")
    xor = 0xFF
    for byte in body:
        xor ^= byte
    assert packet == body + bytes([xor])
    parsed = header.unpack(profile, packet)
    assert isinstance(parsed, Header)
    assert parsed.values["length"] == 10 and parsed.values["counter"] == 0x01020304
    assert parsed.mid == 0x1234


def test_a_little_endian_length_field_frames_a_stream() -> None:
    profile = prof(LE_FIELDS, kind="command", fc="function_code")
    packet = header.pack(profile, b"abc", mid=0x1234, inputs={"function_code": 1})
    assert profile.length_window == 4
    assert profile.frame_length(packet[:3]) is None
    assert profile.frame_length(packet[:4]) == len(packet) == 13
    wrong = packet[:2] + b"\x00\x0a" + packet[4:]  # the same value, big-endian: 0x000a -> 0x0a00
    assert profile.frame_length(wrong) == 0x0A00


def test_a_little_endian_mid_field_is_read_by_peek_and_split() -> None:
    profile = prof(
        [
            {"name": "app", "bits": 16, "computed": "from_mid", "endian": "little"},
            {"name": "kind", "bits": 8, "value": 0xAB},
        ],
        mid=("kind", "app"),
    )
    packet = header.pack(profile, b"", mid=0xAB1234)
    assert packet == bytes.fromhex("3412ab")
    assert header.peek_mid(profile, packet) == 0xAB1234
    assert profile.mid_windows == ((2, 1, 0, 0xFF, 16, "big"), (0, 2, 0, 0xFFFF, 0, "little"))
    parsed = header.unpack(profile, packet)
    assert isinstance(parsed, Header) and parsed.values["app"] == 0x1234


def test_a_little_endian_fixed_field_is_checked_in_wire_form() -> None:
    profile = prof(
        [
            {"name": "mid", "bits": 16, "computed": "from_mid"},
            {"name": "magic", "bits": 16, "value": 0xCAFE, "endian": "little"},
        ]
    )
    packet = header.pack(profile, b"", mid=1)
    assert packet[2:] == b"\xfe\xca"
    bad = header.check(profile, packet[:2] + b"\xca\xfe")
    assert isinstance(bad, DecodeError) and bad.code is Code.DEC_FIXED and bad.offset == 2
    assert bad.reason == "magic is 65226, expected 51966"


def test_little_endian_on_one_byte_is_the_same_as_big_endian() -> None:
    little = prof(
        [{"name": "mid", "bits": 8, "computed": "from_mid", "endian": "little"}], mid=("mid",)
    )
    assert not little.fields[0].swap and little.fields[0].little
    assert header.pack(little, b"", mid=0x5A) == b"\x5a"


@pytest.mark.parametrize(
    ("edit", "needle"),
    [
        (
            lambda d: (
                d["fields"].insert(0, {"name": "a", "bits": 4, "endian": "little"})
                or d["fields"].insert(1, {"name": "b", "bits": 4})
            ),
            "little-endian field must start on a byte boundary",
        ),
        (lambda d: d["fields"][0].update(endian="middle"), "'big' or 'little'"),
        (lambda d: d["fields"][0].update(endian=1), "must be a string"),
        (lambda d: d.update(byte_order="little"), "only 'big'"),
    ],
)
def test_contradictory_or_ambiguous_byte_order_is_refused(edit: Any, needle: str) -> None:
    rejected(edit, needle)


def test_a_little_endian_field_off_a_byte_boundary_is_refused() -> None:
    def edit(d: dict[str, Any]) -> None:
        d["fields"] = [
            {"name": "flags", "bits": 4},
            {"name": "mid", "bits": 16, "computed": "from_mid", "endian": "little"},
            {"name": "pad", "bits": 4},
        ]

    rejected(edit, "fields[1].endian: a little-endian field must start on a byte boundary")


# -- trailer and checksum span ------------------------------------------------------------------

TRAILER_HEAD: list[dict[str, Any]] = [
    {"name": "mid", "bits": 16, "computed": "from_mid"},
    {"name": "length", "bits": 16, "computed": "length"},
]


def test_a_trailer_follows_the_payload_and_the_crc_covers_the_packet_with_itself_zeroed() -> None:
    profile = prof(
        TRAILER_HEAD,
        trailer=[
            {"name": "marker", "bits": 16, "value": 0xA55A},
            {"name": "crc", "bits": 16, "computed": "checksum", "algorithm": "crc16_ccitt",
             "endian": "little"},
        ],
    )  # fmt: skip
    packet = header.pack(profile, b"hello", mid=0x0102)
    assert len(packet) == 4 + 5 + 4 and packet[:4] == bytes.fromhex("0102000d")
    assert packet[4:9] == b"hello" and packet[9:11] == b"\xa5\x5a"
    crc = crc16_bitwise(packet[:11] + b"\x00\x00")
    assert packet[11:] == crc.to_bytes(2, "little")
    parsed = header.unpack(profile, packet)
    assert isinstance(parsed, Header)
    assert parsed.values["marker"] == 0xA55A and parsed.values["crc"] == crc
    assert parsed.size == 4 and profile.trailer_size == 4 and profile.min_bytes == 8

    flipped = bytearray(packet)
    flipped[9] ^= 0x01  # the marker
    result = header.check(profile, bytes(flipped))
    assert isinstance(result, DecodeError) and result.code is Code.DEC_FIXED
    assert result.offset == len(packet) - 4

    flipped = bytearray(packet)
    flipped[5] ^= 0x01  # the payload
    result = header.check(profile, bytes(flipped))
    assert isinstance(result, DecodeError) and result.code is Code.DEC_CHECKSUM
    assert result.offset == len(packet) - 2
    assert "crc is 0x" in result.reason


def test_a_packet_shorter_than_header_and_trailer_is_short() -> None:
    profile = prof(
        TRAILER_HEAD,
        trailer=[{"name": "end", "bits": 8, "value": 0x7E}],
    )
    for n in range(5):
        result = header.check(profile, bytes(n))
        assert isinstance(result, DecodeError) and result.code is Code.DEC_SHORT
        assert "trailer" in result.reason
    assert header.pack(profile, b"", mid=1)[-1] == 0x7E


def test_packet_error_control_is_the_crc_of_everything_before_it() -> None:
    profile = header.load_profile(HEADERS / "ccsds_pec_tlm.toml")
    assert profile.trailer_size == 2 and profile.size_bytes == 6
    packet = header.pack(profile, b"\x01\x02\x03", mid=0x0123, sequence=5)
    assert packet[:6] == bytes.fromhex("0123c0050004")  # length = 11 - 7
    assert packet[6:9] == b"\x01\x02\x03"
    assert packet[9:] == crc16_bitwise(packet[:9]).to_bytes(2, "big")
    assert packet[9:] == binascii.crc_hqx(packet[:9], 0xFFFF).to_bytes(2, "big")
    parsed = header.unpack(profile, packet)
    assert isinstance(parsed, Header) and parsed.values["pec"] == int.from_bytes(packet[9:], "big")
    bad = header.check(profile, packet[:-1] + bytes([packet[-1] ^ 1]))
    assert isinstance(bad, DecodeError) and bad.code is Code.DEC_CHECKSUM and bad.offset == 9
    # The PEC is outside its own span, so changing the span's last byte changes the PEC.
    other = header.pack(profile, b"\x01\x02\x04", mid=0x0123, sequence=5)
    assert other[9:] != packet[9:]


def test_the_crc16_check_value_through_a_payload_span() -> None:
    profile = prof(
        [{"name": "mid", "bits": 16, "computed": "from_mid"}],
        trailer=[
            {"name": "crc", "bits": 16, "computed": "checksum", "algorithm": "crc16_ccitt",
             "span": "payload"},
        ],
    )  # fmt: skip
    packet = header.pack(profile, b"123456789", mid=0xFFFF)
    assert packet[-2:] == b"\x29\xb1"  # the standard CRC-16/CCITT-FALSE check value
    assert header.check(profile, packet) is None
    assert checksum.REGISTRY["crc16_ccitt"].compute(b"123456789") == 0x29B1


@given(st.binary(min_size=1, max_size=300), st.data())
def test_xor8_without_a_copy_equals_xor8_of_the_packet_with_the_byte_zeroed(
    data: bytes, draw: st.DataObject
) -> None:
    algorithm = checksum.REGISTRY["xor8_cfs"]
    assert algorithm.compute_zeroed is not None
    assert checksum.REGISTRY["crc16_ccitt"].compute_zeroed is None
    at = draw.draw(st.integers(0, len(data) - 1))
    zeroed = bytearray(data)
    zeroed[at] = 0
    want = algorithm.compute(zeroed)
    for buffer in (data, bytearray(data), memoryview(data)):
        assert algorithm.compute_zeroed(buffer, at) == want


def test_crc16_is_the_same_function_as_the_table_driven_one_it_replaced() -> None:
    compute = checksum.REGISTRY["crc16_ccitt"].compute
    for data in (b"", b"\x00", b"\xff" * 300, bytes(range(256)) * 3, b"123456789"):
        assert compute(data) == crc16_bitwise(data)
        assert compute(bytearray(data)) == compute(memoryview(data)) == compute(data)


def test_header_and_payload_spans_leave_the_other_part_out() -> None:
    def build(span: str) -> HeaderProfile:
        return prof(
            [
                {"name": "mid", "bits": 16, "computed": "from_mid"},
                {"name": "length", "bits": 16, "computed": "length"},
                {"name": "ck", "bits": 8, "computed": "checksum", "algorithm": "xor8_cfs",
                 "span": span},
                {"name": "pad", "bits": 8, "value": 0, "check": False},
            ],
            trailer=[{"name": "end", "bits": 8, "value": 0x7E}],
        )  # fmt: skip

    for span, same in (("header", True), ("payload", False)):
        profile = build(span)
        one = header.pack(profile, b"\x01\x02", mid=7)
        two = header.pack(profile, b"\x01\x03", mid=7)
        assert (one[4] == two[4]) is same, span
        assert header.check(profile, one) is None and header.check(profile, two) is None
    only_header = build("header")
    packet = header.pack(only_header, b"\x01\x02", mid=7)
    assert packet[4] == 0xFF ^ 7 ^ 9  # header bytes 00 07 00 09, the field itself zero, pad 00
    only_payload = build("payload")
    packet = header.pack(only_payload, b"\x01\x02", mid=7)
    assert packet[4] == 0xFF ^ 1 ^ 2


def test_a_range_span_counts_the_end_from_the_packet_end() -> None:
    profile = prof(
        [
            {"name": "mid", "bits": 16, "computed": "from_mid"},
            {"name": "ck", "bits": 8, "computed": "checksum", "algorithm": "xor8_cfs",
             "span": {"start": 1, "end": -2}},
        ],
        trailer=[{"name": "tail", "bits": 16, "value": 0x1234, "check": False}],
    )  # fmt: skip
    packet = header.pack(profile, b"\x10\x20\x30", mid=0x0A0B)
    covered = packet[1:-2]  # byte 1 to 2 before the end: includes the checksum field itself
    zeroed = bytearray(covered)
    zeroed[1] = 0  # the checksum field (packet byte 2) is inside the span: zeroed
    expected = 0xFF
    for byte in zeroed:
        expected ^= byte
    assert packet[2] == expected
    assert header.check(profile, packet) is None
    changed = packet[:-1] + b"\x99"  # inside the trailer: not covered, and unchecked
    assert header.check(profile, changed) is None
    outside = packet[:0] + b"\x0b" + packet[1:]  # byte 0 is before the span
    assert header.check(profile, outside) is None
    inside = packet[:3] + b"\x11" + packet[4:]
    assert isinstance(header.check(profile, inside), DecodeError)


def test_a_span_that_does_not_fit_the_packet_is_a_coded_error_never_an_exception() -> None:
    profile = prof(
        [
            {"name": "mid", "bits": 16, "computed": "from_mid"},
            {"name": "ck", "bits": 8, "computed": "checksum", "algorithm": "xor8_cfs",
             "span": {"start": 3, "end": -3}},
            {"name": "pad", "bits": 8, "value": 0, "check": False},
        ],
    )  # fmt: skip
    with pytest.raises(IronbirdError) as info:
        header.pack(profile, b"", mid=1)  # 4 bytes: span 3..1
    assert info.value.code is Code.ENC_FIT and "lies outside" in info.value.summary
    packet = header.pack(profile, b"\x01\x02\x03\x04", mid=1)  # 3..5 fits
    assert header.check(profile, packet) is None
    short = header.check(profile, bytes(4))
    assert isinstance(short, DecodeError) and short.code is Code.DEC_CHECKSUM
    assert short.offset == 2 and "lies outside the 4-byte packet" in short.reason


def test_a_span_boundary_through_the_checksum_field_is_refused_at_load_or_at_check() -> None:
    def header_start(d: dict[str, Any]) -> None:
        d["fields"] = [
            {"name": "mid", "bits": 16, "computed": "from_mid"},
            {"name": "ck", "bits": 16, "computed": "checksum", "algorithm": "crc16_ccitt",
             "span": {"start": 3, "end": 0}},
        ]  # fmt: skip

    rejected(header_start, "starts at byte 3, inside the checksum field itself")

    def trailer_end(d: dict[str, Any]) -> None:
        d["trailer"] = {
            "fields": [
                {"name": "ck", "bits": 16, "computed": "checksum", "algorithm": "crc16_ccitt",
                 "span": {"start": 0, "end": -3}},
                {"name": "x", "bits": 16, "value": 0},
            ]
        }  # fmt: skip

    rejected(trailer_end, "ends 3 bytes before the end, inside the checksum field")


@pytest.mark.parametrize(
    ("span", "needle"),
    [
        ("everything", "is not a span"),
        (5, "must be 'packet'"),
        ({"start": 0}, "span.end: required key is missing"),
        ({"start": 0, "end": 1}, "must be in -65535..0"),
        ({"start": -1, "end": 0}, "must be in 0..65535"),
        ({"start": 65535, "end": -1}, "leave no room"),
        ({"start": 0, "end": 0, "step": 2}, "span.step: unknown key"),
        ({"start": True, "end": 0}, "must be an integer"),
    ],
)
def test_bad_spans_are_refused_with_the_field_path(span: Any, needle: str) -> None:
    def edit(d: dict[str, Any]) -> None:
        d["fields"].append(
            {"name": "ck", "bits": 8, "computed": "checksum", "algorithm": "xor8_cfs", "span": span}
        )

    rejected(edit, needle)


def test_span_on_a_field_that_is_not_a_checksum_is_refused() -> None:
    rejected(
        lambda d: d["fields"][0].update(span="header"), "fields[0].span: only valid on computed"
    )


def _fixed(name: str, bits: int = 8) -> dict[str, Any]:
    return {"name": name, "bits": bits, "value": 1}


@pytest.mark.parametrize(
    ("trailer", "mid_fields", "needle"),
    [
        ([], None, "trailer: must be a table"),
        ({}, None, "trailer.fields: required"),
        ({"fields": [], "x": 1}, None, "trailer.x: unknown key"),
        ({"fields": []}, None, "non-empty array"),
        ({"fields": [{"name": "a", "bits": 4}]}, None, "whole number of bytes"),
        ({"fields": [{"name": "a", "bits": 8}]}, None, "must be fixed"),
        ({"fields": [{"name": "a", "bits": 8, "computed": "length"}]}, None, "must be fixed"),
        ({"fields": [_fixed("mid")]}, None, "duplicate field name"),
        ({"fields": [_fixed("a"), _fixed("a")]}, None, "duplicate field name"),
        ({"fields": [_fixed(f"a{i}") for i in range(65)]}, None, "too many fields"),
        ({"fields": [_fixed("a")]}, ["mid", "a"], "in the trailer, not the header"),
    ],
)
def test_bad_trailers_are_refused(trailer: Any, mid_fields: Any, needle: str) -> None:
    def edit(d: dict[str, Any]) -> None:
        d["trailer"] = trailer
        if mid_fields is not None:
            d["mid_fields"] = mid_fields

    rejected(edit, needle)


def test_one_checksum_per_profile_header_or_trailer() -> None:
    def edit(d: dict[str, Any]) -> None:
        d["fields"].append(
            {"name": "ck", "bits": 8, "computed": "checksum", "algorithm": "xor8_cfs"}
        )
        d["trailer"] = {
            "fields": [{"name": "ck2", "bits": 8, "computed": "checksum", "algorithm": "xor8_cfs"}]
        }

    rejected(edit, "at most one computed = 'checksum' field")


# -- a message ID spread over the header ---------------------------------------------------------


def test_cfe_msgid_v2_builds_the_mid_from_three_places() -> None:
    cmd = header.load_profile(HEADERS / "cfe_msgid_v2_cmd.toml")
    tlm = header.load_profile(HEADERS / "cfe_msgid_v2_tlm.toml")
    assert cmd.size_bytes == 16 and tlm.size_bytes == 16 and cmd.mid_bits == 16
    assert cmd.mid_windows == (
        (7, 1, 0, 0xFF, 8, "big"),  # subsystem low byte: extended header byte 7
        (0, 1, 4, 0x1, 7, "big"),  # packet type: primary header bit 3
        (1, 1, 0, 0x7F, 0, "big"),  # APID low 7 bits: primary header byte 1
    )
    assert cmd.mid_single is None
    packet = header.pack(cmd, b"", mid=0x2A85, inputs={"function_code": 3})
    # version 0, type 1, secondary header 1, apid high 0 | apid low 5 | seq | length 9
    assert packet[:6] == bytes.fromhex("1805c0000009")
    # cFE defaults: EDS version 1 (0x08), little-endian flag (0x04), subsystem low byte 0x2A, then
    # the system id 0x0042 (the sample spacecraft id)
    assert packet[6:10] == bytes.fromhex("0c2a0042")
    assert packet[10] == 3  # function code
    parsed = header.unpack(cmd, packet)
    assert isinstance(parsed, Header) and parsed.mid == 0x2A85 == header.peek_mid(cmd, packet)
    assert parsed.values["subsystem_lo"] == 0x2A and parsed.values["apid_lo"] == 5
    telemetry = header.pack(tlm, b"", mid=0x2A05, now=1e9)
    assert telemetry[:2] == bytes.fromhex("0805") and telemetry[7] == 0x2A
    assert header.peek_mid(tlm, telemetry) == 0x2A05
    for bad in (header.peek_mid(cmd, packet[:7]), header.peek_mid(cmd, b"")):
        assert bad is None


def test_the_mid_must_agree_with_the_fixed_fields_wherever_they_sit() -> None:
    cmd = header.load_profile(HEADERS / "cfe_msgid_v2_cmd.toml")
    with pytest.raises(IronbirdError) as info:
        header.pack(cmd, b"", mid=0x2A05, inputs={"function_code": 3})  # type bit 0: telemetry
    assert info.value.code is Code.ENC_FIT and "packet_type" in info.value.summary


@settings(max_examples=200, deadline=1000)
@given(st.integers(0, 255), st.integers(0, 127), st.integers(0, 127))
def test_msgid_v2_round_trips_every_mid_and_the_unused_apid_bits_do_not_matter(
    subsystem: int, apid: int, spare: int
) -> None:
    cmd = header.load_profile(HEADERS / "cfe_msgid_v2_cmd.toml")
    mid = subsystem << 8 | 0x80 | apid
    packet = bytearray(header.pack(cmd, b"xy", mid=mid, inputs={"function_code": 1}))
    assert header.peek_mid(cmd, packet) == mid
    # apid_hi (not part of the MID, check = false) takes any value: the packet still decodes.
    packet[0] |= spare & 0x07
    packet[1] |= (spare & 0x08) << 4
    packet[11] = 0  # the checksum field
    fixed = 0xFF
    for byte in packet:
        fixed ^= byte
    packet[11] = fixed
    assert header.peek_mid(cmd, packet) == mid
    parsed = header.unpack(cmd, bytes(packet))
    assert isinstance(parsed, Header) and parsed.mid == mid


def _wide_mid_profile() -> dict[str, Any]:
    return {
        "name": "p",
        "kind": "telemetry",
        "mid_fields": ["hi", "lo"],
        "fields": [
            {"name": "lo", "bits": 20, "computed": "from_mid"},
            {"name": "gap", "bits": 8, "value": 0},
            {"name": "hi", "bits": 20, "computed": "from_mid"},
            {"name": "pad", "bits": 8, "value": 0},
        ],
    }


def test_a_mid_wider_than_32_bits_over_several_fields_is_refused_by_the_codec() -> None:
    profile = header.parse_profile(_wide_mid_profile(), "wide.toml")
    assert profile.mid_bits == 40
    model = parse_model({"schema": "ironbird-ccdd/0", "target": "t", "messages": []}, "t.json")
    cmd = header.load_profile(HEADERS / "ccsds_v1_cfs_cmd.toml")
    with pytest.raises(IronbirdError) as info:
        Codec(model, cmd, profile)
    assert info.value.code is Code.HDR_SCHEMA and "40-bit message ID" in info.value.summary


@pytest.mark.parametrize(
    ("edit", "needle"),
    [
        (lambda d: d.update(mid_fields=["hi", "hi"]), "listed twice"),
        (
            lambda d: d["fields"].append({"name": "inp", "bits": 8})
            or d.update(mid_fields=["hi", "inp"]),
            "must be fixed or computed",
        ),
        (lambda d: d.update(mid_fields=["hi", "nope"]), "unknown field"),
        (lambda d: d.update(mid_fields=["hi"]), "needs the field in mid_fields"),
    ],
)  # fmt: skip
def test_bad_mid_field_lists_are_refused(edit: Any, needle: str) -> None:
    rejected(edit, needle, _wide_mid_profile())


def test_a_mid_wider_than_64_bits_is_refused() -> None:
    def edit(d: dict[str, Any]) -> None:
        d["fields"] = [
            {"name": "a", "bits": 64, "computed": "from_mid"},
            {"name": "b", "bits": 8, "computed": "from_mid"},
        ]
        d["mid_fields"] = ["a", "b"]

    rejected(edit, "wider than 64 bits", _wide_mid_profile())


# -- header values per message and per send ------------------------------------------------------

HV_CMD = {
    "name": "hv_cmd",
    "kind": "command",
    "function_code_field": "function_code",
    "mid_fields": ["mid"],
    "fields": [
        {"name": "mid", "bits": 16, "computed": "from_mid"},
        {"name": "length", "bits": 16, "computed": "length"},
        {"name": "function_code", "bits": 8},
        {"name": "priority", "bits": 4, "default": 2},
        {"name": "dest", "bits": 4, "default": 0},
        {"name": "checksum", "bits": 8, "computed": "checksum", "algorithm": "xor8_cfs"},
    ],
}
HV_TLM = {
    "name": "hv_tlm",
    "kind": "telemetry",
    "mid_fields": ["mid"],
    "fields": [
        {"name": "mid", "bits": 16, "computed": "from_mid"},
        {"name": "length", "bits": 16, "computed": "length"},
        {"name": "priority", "bits": 8, "default": 1},
    ],
}


def hv_codec(
    cmd_header: Any = None, tlm_header: Any = None, *, clock: float = 1_000_000.25
) -> Codec:
    def msg(name: str, kind: str, mid: int, header_values: Any) -> dict[str, Any]:
        m: dict[str, Any] = {"name": name, "kind": kind, "mid": mid, "fields": []}
        if kind == "command":
            m["function_code"] = 1
        else:
            m["category"] = "status"
        if header_values is not None:
            m["header"] = header_values
        return m

    model = parse_model(
        {
            "schema": "ironbird-ccdd/0",
            "target": "hv",
            "messages": [
                msg("PLAIN", "command", 0x1801, None),
                msg("HIGH", "command", 0x1802, cmd_header),
                msg("STATUS", "telemetry", 0x0801, tlm_header),
            ],
        },
        "hv.json",
    )
    return Codec(
        model,
        header.parse_profile(HV_CMD, "hv_cmd.toml"),
        header.parse_profile(HV_TLM, "hv_tlm.toml"),
        clock=lambda: clock,
    )


def decoded(codec: Codec, kind: MessageKind, data: bytes) -> DecodedPacket:
    result = codec.decode(kind, data)
    assert isinstance(result, DecodedPacket), result
    return result


def test_header_values_follow_send_then_message_then_profile_default() -> None:
    codec = hv_codec({"priority": 9, "dest": 3})
    plain = decoded(codec, CMD, codec.encode_command("PLAIN").data).header.values
    assert (plain["priority"], plain["dest"]) == (2, 0)  # the profile defaults
    high = decoded(codec, CMD, codec.encode_command("HIGH").data).header.values
    assert (high["priority"], high["dest"]) == (9, 3)  # the message's values
    sent = codec.encode_command("HIGH", header={"priority": 15}).data
    values = decoded(codec, CMD, sent).header.values
    assert (values["priority"], values["dest"]) == (15, 3)  # send wins, the rest is the message
    sent = decoded(codec, CMD, codec.encode_command("PLAIN", header={"dest": 7}).data)
    assert (sent.header.values["priority"], sent.header.values["dest"]) == (2, 7)
    assert codec.check(CMD, codec.encode_command("HIGH", header={"dest": 1}).data) is None


def test_telemetry_takes_message_and_send_header_values_too() -> None:
    codec = hv_codec(tlm_header={"priority": 200})
    assert (
        decoded(codec, TLM, codec.encode_telemetry("STATUS").data).header.values["priority"] == 200
    )
    again = codec.encode_telemetry("STATUS", header={"priority": 7}).data
    assert decoded(codec, TLM, again).header.values["priority"] == 7


@pytest.mark.parametrize(
    ("bad", "needle"),
    [
        ({"nope": 1}, "'nope' is not an input field of profile hv_cmd (settable: priority, dest)"),
        ({"mid": 1}, "'mid' is not an input field"),
        ({"length": 1}, "'length' is not an input field"),
        ({"checksum": 1}, "'checksum' is not an input field"),
        ({"function_code": 1}, "function code comes from the command definition"),
        ({"priority": 16}, "priority=16 does not fit 4 bits (max 15)"),
        ({"priority": -1}, "priority=-1 does not fit 4 bits"),
        ({"priority": True}, "does not fit"),
        ({"priority": 1.5}, "does not fit"),
        ({"priority": "3"}, "does not fit"),
        ({"priority": None}, "does not fit"),
        ({1: 2}, "field names must be strings, not int"),
        ({None: 1}, "field names must be strings, not NoneType"),
        ({b"priority": 1}, "field names must be strings, not bytes"),
        ({("priority",): 1}, "field names must be strings, not tuple"),
        ([("priority", 1)], "must be a mapping of field name to value, not a list"),
        ("priority=1", "must be a mapping of field name to value, not a str"),
    ],
)
def test_per_send_header_values_are_checked_against_the_profile(bad: Any, needle: str) -> None:
    codec = hv_codec()
    with pytest.raises(IronbirdError) as info:
        codec.encode_command("PLAIN", header=bad)
    assert info.value.code is Code.HDR_PACK and needle in info.value.summary
    assert codec.encode_command("PLAIN").data  # and the codec is unharmed


@pytest.mark.parametrize(
    ("bad", "needle"),
    [
        ({"nope": 1}, "header 'nope' is not an input field of profile hv_cmd"),
        ({"function_code": 1}, "function code comes from the command definition"),
        ({"dest": 99}, "header dest=99 does not fit 4 bits"),
    ],
)
def test_per_message_header_values_are_checked_when_the_codec_is_built(
    bad: Any, needle: str
) -> None:
    with pytest.raises(IronbirdError) as info:
        hv_codec(bad)
    assert info.value.code is Code.ENC_FIT
    assert "command HIGH" in info.value.summary and needle in info.value.summary


@pytest.mark.parametrize(
    ("value", "needle"),
    [
        ([], "must be an object"),
        ({"a b": 1}, "must be a header field name"),
        ({"priority": -1}, "must be in 0.."),
        ({"priority": 1.5}, "must be an integer"),
        ({"priority": True}, "must be an integer"),
        ({"priority": "1"}, "must be an integer"),
        ({"priority": 2**64}, "must be in 0.."),
        ({f"f{i}": 1 for i in range(65)}, "more than 64 header values"),
    ],
)
def test_the_ccdd_header_key_is_validated(value: Any, needle: str) -> None:
    with pytest.raises(IronbirdError) as info:
        hv_codec(value)
    assert info.value.code is Code.CCD_SCHEMA and needle in info.value.summary
    assert "messages[1](HIGH).header" in info.value.summary


def test_the_ccdd_header_key_is_part_of_the_model_and_read_only() -> None:
    codec = hv_codec({"priority": 9})
    message = codec.model.by_name["HIGH"]
    assert dict(message.header) == {"priority": 9}
    with pytest.raises(TypeError):
        message.header["priority"] = 1  # type: ignore[index]
    assert dict(codec.model.by_name["PLAIN"].header) == {}


def test_parse_header_option_values() -> None:
    assert _parse_header_values(None) == {}
    assert _parse_header_values(["priority=3", "dest=0x2"]) == {"priority": 3, "dest": 2}
    for bad, needle in (
        (["priority"], "field=N"),
        (["=3"], "field=N"),
        (["priority=x"], "must be an integer"),
        (["priority="], "must be an integer"),
        (["priority=1", "priority=2"], "more than once"),
    ):
        with pytest.raises(IronbirdError) as info:
            _parse_header_values(bad)
        assert info.value.code is Code.CLI_USAGE and needle in info.value.summary


def test_send_header_option_reaches_the_wire(mock_config_path: Path, tmp_path: Path) -> None:
    cmd_toml = (HEADERS / "ccsds_v1_cfs_cmd.toml").read_text()
    old = '[[fields]]\nname = "reserved"\nbits = 1\nvalue = 0\ncheck = false\n'
    assert old in cmd_toml
    custom = tmp_path / "prio_cmd.toml"
    custom.write_text(
        cmd_toml.replace(old, '[[fields]]\nname = "priority"\nbits = 1\ndefault = 0\n')
    )
    text = mock_config_path.read_text().replace(f'"{HEADERS}/ccsds_v1_cfs_cmd.toml"', f'"{custom}"')
    assert str(custom) in text
    config = tmp_path / "prio.toml"
    config.write_text(text)
    port = int(re.search(r'command_endpoint = "127\.0\.0\.1:(\d+)"', text).group(1))  # type: ignore[union-attr]
    runner = CliRunner()
    base = ["-c", str(config)]
    codec = Codec(
        load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
        header.load_profile(custom),
        header.load_profile(HEADERS / "ccsds_v1_cfs_tlm.toml"),
    )
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sink:
        sink.bind(("127.0.0.1", port))
        sink.settimeout(5)
        done = runner.invoke(app, [*base, "send", "fc", "MOCK_NOOP", "--header", "priority=1"])
        assert done.exit_code == 0, done.output
        data = sink.recv(2048)
        assert decoded(codec, CMD, data).header.values["priority"] == 1
        done = runner.invoke(app, [*base, "send", "fc", "MOCK_NOOP"])
        assert done.exit_code == 0, done.output
        assert decoded(codec, CMD, sink.recv(2048)).header.values["priority"] == 0
        for args, code in (
            (["--header", "nope=1"], "IB-HDR-003"),
            (["--header", "priority=2"], "IB-HDR-003"),
            (["--header", "function_code=2"], "IB-HDR-003"),
            (["--header", "priority"], "IB-CLI-001"),
            (["--header", "priority=1", "--header", "priority=0"], "IB-CLI-001"),
        ):
            bad = runner.invoke(app, [*base, "send", "fc", "MOCK_NOOP", *args])
            assert bad.exit_code == 2 and code in bad.output, (args, bad.output)
        sink.settimeout(0.2)
        with pytest.raises(TimeoutError):
            sink.recv(2048)  # nothing was sent for the refused ones


# -- time epoch and header time ------------------------------------------------------------------


def to_ns(moment: datetime) -> int:
    delta = moment - UNIX
    return (delta.days * 86400 + delta.seconds) * 10**9 + delta.microseconds * 1000


def test_the_cfe_time_profile_reads_tai_since_1980_as_utc() -> None:
    profile = header.load_profile(HEADERS / "ccsds_v1_cfs_tlm_cfe_time.toml")
    assert profile.time is not None
    assert profile.time.epoch_s == 315_532_800 and profile.time.leap_seconds == 37
    # What cFE stamps 1000000 s after its epoch, half a second in: TAI 1980-01-12T13:46:40.5,
    # UTC 37 s earlier.
    packet = bytearray(header.pack(profile, b"", mid=0x0805, now=1e9))
    packet[6:10] = (1_000_000).to_bytes(4, "big")
    packet[10:12] = (0x8000).to_bytes(2, "big")
    parsed = header.unpack(profile, bytes(packet))
    assert isinstance(parsed, Header)
    utc = datetime(1980, 1, 1, tzinfo=UTC) + timedelta(seconds=1_000_000 - 37, microseconds=500_000)
    assert parsed.time_ns == to_ns(utc) == 316_532_763_500_000_000
    assert (utc.year, utc.month, utc.day, utc.hour, utc.minute, utc.second) == (
        1980, 1, 12, 13, 46, 3,
    )  # fmt: skip


def test_packing_with_an_epoch_writes_the_counter_the_unpacker_reads_back() -> None:
    profile = header.load_profile(HEADERS / "ccsds_v1_cfs_tlm_cfe_time.toml")
    now = 1_700_000_000.5
    packet = header.pack(profile, b"", mid=0x0805, now=now)
    assert int.from_bytes(packet[6:10], "big") == 1_700_000_000 - 315_532_800 + 37
    assert int.from_bytes(packet[10:12], "big") == 0x8000
    parsed = header.unpack(profile, packet)
    assert isinstance(parsed, Header) and parsed.time_ns == 1_700_000_000_500_000_000
    early = datetime(1975, 1, 1, tzinfo=UTC).timestamp()
    with pytest.raises(IronbirdError) as info:
        header.pack(profile, b"", mid=0x0805, now=early)
    assert info.value.code is Code.HDR_PACK and "precedes the time epoch" in info.value.summary


def test_without_a_time_table_the_counter_is_unix_seconds() -> None:
    profile = header.load_profile(HEADERS / "ccsds_v1_cfs_tlm.toml")
    packet = header.pack(profile, b"", mid=0x0805, now=1_700_000_000.25)
    parsed = header.unpack(profile, packet)
    assert isinstance(parsed, Header) and parsed.time_ns == 1_700_000_000_250_000_000


def test_a_profile_without_a_seconds_field_has_no_header_time() -> None:
    profile = header.load_profile(HEADERS / "ccsds_v1_cfs_cmd.toml")
    assert profile.time is None
    packet = header.pack(profile, b"", mid=0x1805, inputs={"function_code": 0})
    parsed = header.unpack(profile, packet)
    assert isinstance(parsed, Header) and parsed.time_ns is None
    bare = Header(values={}, mid=None, size=0, sequence=None)
    assert bare.time_ns is None


def test_seconds_only_and_subseconds_widths() -> None:
    coarse = prof(
        [
            {"name": "mid", "bits": 16, "computed": "from_mid"},
            {"name": "s", "bits": 32, "computed": "time_seconds", "endian": "little"},
        ],
        time={"epoch": "2000-01-01T12:00:00Z"},
    )
    packet = header.pack(coarse, b"", mid=1, now=946_728_000 + 86400.9)
    assert packet[2:] == (86400).to_bytes(4, "little")
    parsed = header.unpack(coarse, packet)
    assert isinstance(parsed, Header) and parsed.time_ns == (946_728_000 + 86400) * 10**9
    fine = prof(
        [
            {"name": "mid", "bits": 16, "computed": "from_mid"},
            {"name": "s", "bits": 32, "computed": "time_seconds"},
            {"name": "ss", "bits": 32, "computed": "time_subseconds"},
        ],
    )
    packet = bytearray(header.pack(fine, b"", mid=1))
    packet[6:10] = (0xFFFFFFFF).to_bytes(4, "big")  # the subseconds field
    parsed = header.unpack(fine, bytes(packet))
    assert isinstance(parsed, Header) and parsed.time_ns == 999_999_999


@pytest.mark.parametrize(
    "epoch", ["1980-01-01T00:00:00Z", "1980-01-01T01:00:00+01:00", "1979-12-31T19:00:00-05:00"]
)
def test_epochs_in_any_zone_name_the_same_instant(epoch: str) -> None:
    profile = prof(
        [
            {"name": "mid", "bits": 16, "computed": "from_mid"},
            {"name": "s", "bits": 32, "computed": "time_seconds"},
        ],
        time={"epoch": epoch},
    )
    assert profile.time is not None and profile.time.epoch_s == 315_532_800


def test_a_toml_date_time_works_as_the_epoch() -> None:
    import tomllib

    doc = tomllib.loads(
        'name = "t"\nkind = "telemetry"\nmid_fields = ["mid"]\n'
        "[time]\nepoch = 1980-01-06T00:00:00Z\nleap_seconds = 18\n"
        '[[fields]]\nname = "mid"\nbits = 16\ncomputed = "from_mid"\n'
        '[[fields]]\nname = "s"\nbits = 32\ncomputed = "time_seconds"\n'
    )
    profile = header.parse_profile(doc, "t.toml")
    assert profile.time is not None
    assert profile.time.epoch_s == 315_964_800 and profile.time.leap_seconds == 18


@pytest.mark.parametrize(
    ("time", "needle"),
    [
        ({"epoch": "1980-01-01T00:00:00"}, "needs a zone"),
        ({"epoch": "1980-01-01"}, "needs a zone"),
        ({"epoch": "yesterday"}, "is not an ISO 8601 time"),
        ({"epoch": 5}, "must be an ISO 8601 time string"),
        ({"epoch": "1980-01-01T00:00:00.5Z"}, "must be a whole second"),
        ({"epoch": "1980-01-01T00:00:00+00:00:30"}, "whole number of minutes"),
        ({"epoch": "1980-01-01T00:00:00-05:30:15"}, "whole number of minutes"),
        ({"epoch": "1899-12-31T23:59:59Z"}, "between the years 1900 and 2200"),
        ({"epoch": "2200-01-01T00:00:01Z"}, "between the years 1900 and 2200"),
        ({"epoch": "0001-01-01T00:00:00+23:59"}, "between the years 1900 and 2200"),
        ({"epoch": "9" * 200}, "longer than 64"),
        ({"leap_seconds": 1001}, "must be in -1000..1000"),
        ({"leap_seconds": 1.5}, "must be an integer"),
        ({"leap_seconds": True}, "must be an integer"),
        ({"zone": "utc"}, "time.zone: unknown key"),
    ],
)
def test_bad_time_tables_are_refused(time: Any, needle: str) -> None:
    def edit(d: dict[str, Any]) -> None:
        d["fields"].append({"name": "s", "bits": 32, "computed": "time_seconds"})
        d["time"] = time

    rejected(edit, needle)


def test_a_time_table_needs_a_seconds_field_and_must_be_a_table() -> None:
    rejected(
        lambda d: d.update(time={"leap_seconds": 1}), "time: needs a field with computed = 'time_se"
    )
    rejected(lambda d: d.update(time=5), "time: must be a table")


# -- framing by the length field -----------------------------------------------------------------


@pytest.mark.parametrize(
    "name", ["ccsds_v1_cfs_cmd", "ccsds_v1_cfs_tlm", "cfe_msgid_v2_cmd", "cfe_msgid_v2_tlm"]
)
def test_frame_length_on_every_prefix_of_the_example_profiles(name: str) -> None:
    profile = header.load_profile(HEADERS / f"{name}.toml")
    inputs = {"function_code": 2} if profile.kind is header.ProfileKind.COMMAND else {}
    mid = {
        "ccsds_v1_cfs_cmd": 0x1805,
        "ccsds_v1_cfs_tlm": 0x0805,
        "cfe_msgid_v2_cmd": 0x2A85,
        "cfe_msgid_v2_tlm": 0x2A05,
    }[name]
    packet = header.pack(profile, b"payload", mid=mid, inputs=inputs, now=1e9)
    assert profile.length_window == 6  # the length field ends at byte 6 in all four
    for cut in range(len(packet) + 1):
        want = None if cut < 6 else len(packet)
        assert profile.frame_length(packet[:cut]) == want
    garbage = bytes([0xFF] * 6)
    assert profile.frame_length(garbage) == 0xFFFF + 7  # untrusted: judged by the caller
    assert profile.frame_length(memoryview(packet)) == len(packet)
    assert profile.frame_length(bytearray(packet[:5])) is None


def test_frame_length_with_a_bit_aligned_length_field_and_a_bias() -> None:
    profile = prof(
        [
            {"name": "a", "bits": 4, "value": 0xA},
            {"name": "n", "bits": 12, "computed": "length", "bias": 3},
            {"name": "mid", "bits": 8, "computed": "from_mid"},
        ]
    )
    assert profile.length_window == 2
    packet = header.pack(profile, b"abcd", mid=9)
    assert packet[:3] == b"\xa0\x0a\x09" and len(packet) == 7  # length field: 7 + bias 3
    assert profile.frame_length(packet[:1]) is None
    assert profile.frame_length(packet[:2]) == 7
    assert header.check(profile, packet) is None


def test_frame_length_returns_negative_and_implausible_values_unjudged() -> None:
    """The framer (2k) must range-check the result: it is the field minus the bias, as claimed."""
    biased = prof(
        [
            {"name": "a", "bits": 4, "value": 0xA},
            {"name": "n", "bits": 12, "computed": "length", "bias": 3},
            {"name": "mid", "bits": 8, "computed": "from_mid"},
        ]
    )
    assert biased.frame_length(b"\xa0\x00") == -3  # a field smaller than the bias
    assert biased.frame_length(b"\xa0\x02") == -1
    assert biased.frame_length(b"\xaf\xff") == 0xFFF - 3
    shipped = header.load_profile(HEADERS / "ccsds_v1_cfs_tlm.toml")  # bias -7
    assert shipped.frame_length(bytes(6)) == 7 < shipped.min_bytes  # shorter than its header
    assert shipped.frame_length(b"\x00" * 4 + b"\xff\xff") == 0xFFFF + 7  # past any cap


def test_a_profile_without_a_length_field_cannot_frame() -> None:
    profile = header.load_profile(HEADERS / "ccsds_pec_tlm.toml")
    assert profile.length_window == 6
    bare = prof([{"name": "mid", "bits": 16, "computed": "from_mid"}])
    assert bare.length_window is None
    with pytest.raises(AssertionError):
        bare.frame_length(b"\x00\x00")


# -- through the codec ---------------------------------------------------------------------------


def test_the_codec_counts_the_trailer_in_the_payload_arithmetic() -> None:
    cmd = header.load_profile(HEADERS / "ccsds_v1_cfs_cmd.toml")
    tlm = header.load_profile(HEADERS / "ccsds_pec_tlm.toml")
    model = parse_model(
        {
            "schema": "ironbird-ccdd/0",
            "target": "p",
            "messages": [
                {
                    "name": "T",
                    "kind": "telemetry",
                    "mid": 0x0123,
                    "category": "status",
                    "fields": [{"name": "x", "type": "uint16", "endian": "big"}],
                }
            ],
        },
        "p.json",
    )
    codec = Codec(model, cmd, tlm)
    data = codec.encode_telemetry("T", {"x": 0xBEEF}).data
    assert len(data) == 6 + 2 + 2 and data[6:8] == b"\xbe\xef"
    assert decoded(codec, TLM, data).as_dict() == {"x": 0xBEEF}
    assert decoded(codec, TLM, data).size == 10
    # One byte short or long is a payload (or length, or checksum) error, never a crash.
    for cut in (data[:-1], data + b"\x00", data[:9], data[:7]):
        assert isinstance(codec.decode(TLM, cut), DecodeError)
    tail_only = bytearray(data)
    tail_only[-1] ^= 1
    result = codec.decode(TLM, bytes(tail_only))
    assert isinstance(result, DecodeError) and result.code is Code.DEC_CHECKSUM
    # A packet whose length field and PEC are right but payload is one byte too big: DEC_PAYLOAD.
    wide = header.pack(tlm, b"\xbe\xef\x00", mid=0x0123)
    result = codec.decode(TLM, wide)
    assert isinstance(result, DecodeError) and result.code is Code.DEC_PAYLOAD
    assert "payload is 3 bytes, CCDD says 2" in result.reason and result.offset == 6


def test_a_message_whose_checksum_span_cannot_fit_it_is_refused_when_the_codec_is_built() -> None:
    cmd = header.load_profile(HEADERS / "ccsds_v1_cfs_cmd.toml")
    tlm = prof(
        [
            {"name": "mid", "bits": 16, "computed": "from_mid"},
            {"name": "ck", "bits": 8, "computed": "checksum", "algorithm": "xor8_cfs",
             "span": {"start": 6, "end": 0}},
            {"name": "pad", "bits": 8, "value": 0},
        ]
    )  # fmt: skip

    def model(fields: list[dict[str, Any]]) -> Any:
        return parse_model(
            {
                "schema": "ironbird-ccdd/0",
                "target": "p",
                "messages": [
                    {
                        "name": "T",
                        "kind": "telemetry",
                        "mid": 1,
                        "category": "status",
                        "fields": fields,
                    }
                ],
            },
            "p.json",
        )

    with pytest.raises(IronbirdError) as info:
        Codec(model([]), cmd, tlm)  # a 4-byte packet: the span would start at 6
    assert info.value.code is Code.ENC_FIT
    assert "telemetry T: checksum span 6..4 lies outside the 4-byte packet" in info.value.summary
    codec = Codec(model([{"name": "x", "type": "uint16"}]), cmd, tlm)  # 6 bytes: 6..6, empty
    data = codec.encode_telemetry("T", {"x": 1}).data
    assert data[2] == 0xFF and codec.check(TLM, data) is None


# -- the filter's htime ----------------------------------------------------------------------


def htime_codecs(tmp_path: Path, tlm_name: str) -> dict[str, Codec]:
    path = tmp_path / "n1.json"
    path.write_text(json.dumps(CCDD_N1))
    return {
        "n1": Codec(
            load_ccdd(path),
            header.load_profile(HEADERS / "ccsds_v1_cfs_cmd.toml"),
            header.load_profile(HEADERS / tlm_name),
            clock=lambda: 1_700_000_000.5,
        )
    }


def test_htime_compares_the_header_time_with_a_time(tmp_path: Path) -> None:
    codecs = htime_codecs(tmp_path, "ccsds_v1_cfs_tlm_cfe_time.toml")
    frame = make_frame(
        codecs["n1"], STREAMS[("n1", TLM)], codecs["n1"].encode_telemetry("TLM_A").data
    )
    decode, seen = count_decodes(codecs)

    def match(text: str) -> bool:
        return compile_filter(text, codecs).bind(decode, lambda node: codecs[node])(frame)

    assert match("htime == 2023-11-14T22:13:20.5Z")
    assert match("htime > 2023-11-14T22:13:20Z and htime < 2023-11-14T22:13:21Z")
    assert match("htime >= 2023-11-14T22:13:20.5Z and htime <= 2023-11-14T22:13:20.5Z")
    assert match("not htime < 2023-01-01T00:00:00Z")
    assert not match("htime < 2023-01-01T00:00:00Z")
    assert not match("htime != 2023-11-14T22:13:20.5Z")
    assert match("htime in [2023-11-14T22:13:20.5Z, 2024-01-01T00:00:00Z]")
    assert match("htime > 2023-11-14T23:13:20+01:00")  # the same second, one hour of offset
    assert match("msg == TLM_A and htime > 2000-01-01T00:00:00Z")
    assert len(seen) > 0
    # The Unix-epoch profile reads the same send time as the same UTC; only the stamp differs.
    shipped = htime_codecs(tmp_path, "ccsds_v1_cfs_tlm.toml")
    other = make_frame(
        shipped["n1"], STREAMS[("n1", TLM)], shipped["n1"].encode_telemetry("TLM_A").data
    )
    dec2, _ = count_decodes(shipped)
    flt = compile_filter("htime == 2023-11-14T22:13:20.5Z", shipped)
    assert flt.bind(dec2, lambda node: shipped[node])(other)
    assert frame.data[6:10] != other.data[6:10]


def test_htime_is_false_for_a_frame_that_does_not_decode(tmp_path: Path) -> None:
    codecs = htime_codecs(tmp_path, "ccsds_v1_cfs_tlm_cfe_time.toml")
    good = codecs["n1"].encode_telemetry("TLM_A").data
    bad = make_frame(codecs["n1"], STREAMS[("n1", TLM)], good[:-1])
    decode, _ = count_decodes(codecs)
    flt = compile_filter("htime > 1970-01-01T00:00:00Z", codecs)
    assert not flt.bind(decode, lambda node: codecs[node])(bad)
    assert flt.needs_decode


@pytest.mark.parametrize(
    ("text", "code"),
    [
        ("htime > 1", Code.FLT_TYPE),
        ("htime > 5.5", Code.FLT_TYPE),
        ("htime == abc", Code.FLT_TYPE),
        ("htime contains 'x'", Code.FLT_TYPE),
        ("htime > 2023-01-01T00:00:00", Code.FLT_SYNTAX),
    ],
)
def test_htime_wants_a_time_literal(tmp_path: Path, text: str, code: Code) -> None:
    codecs = htime_codecs(tmp_path, "ccsds_v1_cfs_tlm_cfe_time.toml")
    with pytest.raises(FilterError) as info:
        compile_filter(text, codecs)
    assert info.value.code is code, info.value


def test_htime_without_any_time_field_in_use_is_a_name_error(tmp_path: Path) -> None:
    path = tmp_path / "n1.json"
    path.write_text(json.dumps(CCDD_N1))
    command = header.load_profile(HEADERS / "ccsds_v1_cfs_cmd.toml")
    plain = prof(
        [
            {"name": "mid", "bits": 16, "computed": "from_mid"},
            {"name": "length", "bits": 16, "computed": "length", "bias": -7},
        ]
    )
    codecs = {"n1": Codec(load_ccdd(path), command, plain)}
    with pytest.raises(FilterError) as info:
        compile_filter("htime > 2023-01-01T00:00:00Z", codecs)
    assert info.value.code is Code.FLT_NAME
    assert "no header profile in use has a time" in str(info.value)


def test_trailer_fields_are_header_fields_for_the_filter(tmp_path: Path) -> None:
    path = tmp_path / "n1.json"
    path.write_text(json.dumps(CCDD_N1))
    command = header.load_profile(HEADERS / "ccsds_v1_cfs_cmd.toml")
    with_trailer = prof(
        [
            {"name": "mid", "bits": 16, "computed": "from_mid"},
            {"name": "length", "bits": 16, "computed": "length", "bias": -7},
        ],
        trailer=[{"name": "crc", "bits": 16, "computed": "checksum", "algorithm": "crc16_ccitt"}],
    )
    codec = Codec(load_ccdd(path), command, with_trailer, clock=lambda: 1.0)
    codecs = {"n1": codec}
    data = codec.encode_telemetry("TLM_A").data
    frame = make_frame(codec, STREAMS[("n1", TLM)], data)
    value = int.from_bytes(data[-2:], "big")
    decode, _ = count_decodes(codecs)
    matcher = compile_filter(f"hdr.crc == {value}", codecs).bind(decode, lambda node: codecs[node])
    assert matcher(frame)
    other = compile_filter(f"hdr.crc != {value}", codecs).bind(decode, lambda node: codecs[node])
    assert not other(frame)


# -- corpus of bad profiles -----------------------------------------------------------------------

NEW_CORPUS = [
    "hdr-endian-little-bitfield.toml",
    "hdr-endian-little-unaligned.toml",
    "hdr-endian-bad-value.toml",
    "hdr-byte-order-little.toml",
    "hdr-span-cuts-own-checksum.toml",
    "hdr-span-cuts-trailer-checksum.toml",
    "hdr-span-too-large.toml",
    "hdr-span-unknown-name.toml",
    "hdr-span-on-non-checksum.toml",
    "hdr-span-end-positive.toml",
    "hdr-mid-field-twice.toml",
    "hdr-mid-field-in-trailer.toml",
    "hdr-trailer-input-field.toml",
    "hdr-trailer-duplicate-name.toml",
    "hdr-trailer-odd-width.toml",
    "hdr-two-checksums.toml",
    "hdr-time-without-seconds.toml",
    "hdr-time-epoch-naive.toml",
    "hdr-time-epoch-offset-seconds.toml",
    "hdr-time-epoch-year-3000.toml",
    "hdr-time-leap-huge.toml",
]


@pytest.mark.parametrize("name", NEW_CORPUS)
def test_new_corpus_profiles_exist_and_are_refused_by_the_loader(name: str) -> None:
    path = CORPUS / name
    with pytest.raises(IronbirdError) as info:
        header.load_profile(path)
    assert info.value.code is Code.HDR_SCHEMA
    assert info.value.summary.startswith(f"{path}: ")
    assert "\n" not in info.value.user_line()
    with pytest.raises(IronbirdError) as again:  # an embedded copy is judged the same way
        header.load_profile_bytes(path.read_bytes(), "recording:fc:telemetry")
    assert again.value.code is Code.HDR_SCHEMA


def test_the_codec_corpus_for_a_mid_over_32_bits_spread_over_fields() -> None:
    path = CORPUS / "codec-hdr-mid-wider-than-32-noncontiguous.toml"
    profile = header.load_profile(path)  # a valid profile on its own
    assert profile.mid_bits == 40 and len(profile.mid_windows) == 2
    model = parse_model({"schema": "ironbird-ccdd/0", "target": "t", "messages": []}, "t.json")
    with pytest.raises(IronbirdError) as info:
        Codec(model, header.load_profile(HEADERS / "ccsds_v1_cfs_cmd.toml"), profile)
    assert info.value.code is Code.HDR_SCHEMA and "40-bit" in info.value.summary


@pytest.mark.parametrize(
    "path", sorted(CORPUS.glob("codec-ccdd-header-*.json")), ids=lambda p: p.stem
)
def test_corpus_ccdd_header_values_the_profile_cannot_take_are_refused_by_the_codec(
    path: Path,
) -> None:
    model = load_ccdd(path)  # a valid CCDD on its own
    assert any(m.header for m in model.messages)
    with pytest.raises(IronbirdError) as info:
        Codec(
            model,
            header.load_profile(HEADERS / "ccsds_v1_cfs_cmd.toml"),
            header.load_profile(HEADERS / "ccsds_v1_cfs_tlm.toml"),
        )
    assert info.value.code is Code.ENC_FIT and "\n" not in info.value.user_line()


def test_the_codec_ccdd_header_corpus_is_not_empty() -> None:
    assert len(list(CORPUS.glob("codec-ccdd-header-*.json"))) == 4


# -- fuzzing the new inputs ----------------------------------------------------------------------

from test_malformed_input import FUZZ, MOCK_JSON, _mutants  # noqa: E402

NEW_EXAMPLES = [
    "cfe_msgid_v2_cmd",
    "cfe_msgid_v2_tlm",
    "ccsds_v1_cfs_tlm_cfe_time",
    "ccsds_pec_tlm",
]


@pytest.mark.parametrize("name", NEW_EXAMPLES)
def test_the_new_example_profiles_load_and_round_trip(name: str) -> None:
    profile = header.load_profile(HEADERS / f"{name}.toml")
    inputs = {f.name: f.value or 0 for f in profile.input_fields}
    mid = {
        "cfe_msgid_v2_cmd": 0x2A85,
        "cfe_msgid_v2_tlm": 0x2A05,
        "ccsds_v1_cfs_tlm_cfe_time": 0x0805,
    }.get(name, 0x0123)
    packet = header.pack(profile, b"abc", mid=mid, inputs=inputs, now=1_700_000_000.0)
    parsed = header.unpack(profile, packet)
    assert isinstance(parsed, Header) and parsed.mid == mid


def _example_docs(name: str) -> dict[str, Any]:
    import tomllib

    return tomllib.loads((HEADERS / f"{name}.toml").read_text())


@FUZZ
@given(st.sampled_from(NEW_EXAMPLES).flatmap(lambda n: _mutants(_example_docs(n))))
def test_mutated_new_example_profiles_never_raise_anything_but_a_coded_error(doc: Any) -> None:
    try:
        profile = header.parse_profile(doc, "fuzz.toml")
    except IronbirdError as err:
        assert err.code is Code.HDR_SCHEMA
        return
    inputs = {f.name: f.value or 0 for f in profile.input_fields}
    for mid in (0x2A85, 0x0123, 0):
        try:
            packet = header.pack(profile, b"\x00\x01", mid=mid, inputs=inputs, now=1_700_000_000.0)
        except IronbirdError as err:
            assert err.code in (Code.ENC_FIT, Code.HDR_PACK)
            continue
        assert isinstance(header.unpack(profile, packet), Header | DecodeError)
        assert isinstance(header.unpack(profile, packet[:-1]), DecodeError | Header)
        if profile.length_window:
            assert isinstance(profile.frame_length(packet), int)


@FUZZ
@given(
    st.sampled_from(range(len(MOCK_JSON["messages"]))),
    st.recursive(
        st.none() | st.booleans() | st.integers() | st.floats() | st.text(max_size=12),
        lambda inner: (
            st.lists(inner, max_size=3) | st.dictionaries(st.text(max_size=12), inner, max_size=3)
        ),
        max_leaves=10,
    ),
)
def test_arbitrary_header_values_in_a_ccdd_message_are_coded_errors(index: int, value: Any) -> None:
    doc = json.loads(json.dumps(MOCK_JSON))
    doc["messages"][index]["header"] = value
    try:
        model = parse_model(doc, "fuzz.json")
    except IronbirdError as err:
        assert err.code is Code.CCD_SCHEMA
        return
    try:
        Codec(
            model,
            header.load_profile(HEADERS / "ccsds_v1_cfs_cmd.toml"),
            header.load_profile(HEADERS / "ccsds_v1_cfs_tlm.toml"),
        )
    except IronbirdError as err:
        assert err.code is Code.ENC_FIT
