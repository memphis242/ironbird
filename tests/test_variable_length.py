"""Stage 2j: variable-length payloads - the CCDD schema, encode, the decode boundary (untrusted),
rendering and export, the filter, the corpus, and the mock end to end."""

from __future__ import annotations

import asyncio
import copy
import csv
import io
import json
import math
import re
import socket
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from typer.testing import CliRunner

import var_corpus
from conftest import CORPUS, EXAMPLES
from filter_defs import LINKS, STREAMS, count_decodes, make_frame
from ironbird import header, render
from ironbird.ccdd.json_adapter import JsonAdapter, parse_model
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind, TypeClass, VarUnit
from ironbird.cli.main import app
from ironbird.codec import Codec, DecodedPacket, Resolved
from ironbird.config import load_config
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.filter import FilterError, compile_filter
from ironbird.frame import Decoded
from ironbird.recording.export import csv_line
from ironbird.session import Handled, Session

CMD, TLM = MessageKind.COMMAND, MessageKind.TELEMETRY

# -- a CCDD with every kind of variable-length field ---------------------------------------------

VAR_CCDD: dict[str, Any] = {
    "schema": "ironbird-ccdd/1",
    "target": "var",
    "messages": [
        {
            # counts of both widths and byte orders, a byte length, a tail of 2-byte elements
            "name": "ALL",
            "kind": "telemetry",
            "mid": "0x0900",
            "fields": [
                {"name": "nf", "type": "uint8"},
                {"name": "ni", "type": "uint16", "endian": "big"},
                {"name": "blen", "type": "uint8"},
                {
                    "name": "Floats",
                    "type": "float32[]",
                    "count": "nf",
                    "min": 1,
                    "max": 5,
                    "endian": "big",
                },
                {"name": "Ints", "type": "int16[]", "count": "ni", "max": 10, "endian": "big"},
                {"name": "Blob", "type": "bytes[]", "length": "blen", "max": 20},
                {"name": "Words", "type": "uint16[]", "length": "remaining", "max": 8},
            ],
        },
        {
            # a byte length over 2-byte elements
            "name": "LEN",
            "kind": "telemetry",
            "mid": "0x0901",
            "fields": [
                {"name": "wlen", "type": "uint8"},
                {"name": "Ws", "type": "uint16[]", "length": "wlen", "min": 1, "max": 100},
            ],
        },
        {
            # data after the last field is not allowed
            "name": "TRAIL",
            "kind": "telemetry",
            "mid": "0x0902",
            "fields": [
                {"name": "n", "type": "uint8"},
                {"name": "Items", "type": "uint8[]", "count": "n", "max": 4},
            ],
        },
        {
            # a fixed field with an explicit offset and a gap, then the variable section
            "name": "GAP",
            "kind": "telemetry",
            "mid": "0x0903",
            "fields": [
                {"name": "a", "type": "uint8", "offset": 4},
                {"name": "n", "type": "uint8", "offset": 0},
                {"name": "Items", "type": "uint16[]", "count": "n", "max": 3},
            ],
        },
        {
            "name": "CMD",
            "kind": "command",
            "mid": "0x1900",
            "function_code": 0,
            "fields": [
                {"name": "n", "type": "uint8"},
                {"name": "Data", "type": "bytes[]", "count": "n", "max": 8},
                {"name": "Name", "type": "char[]", "length": "remaining", "min": 1, "max": 10},
            ],
        },
        {
            "name": "EVT",
            "kind": "telemetry",
            "mid": "0x0904",
            "category": "event",
            "event": {"app": "app", "id": "id", "type": "type", "message": "Text"},
            "fields": [
                {"name": "app", "type": "char[8]"},
                {"name": "id", "type": "uint16"},
                {"name": "type", "type": "uint16", "enum": {"INFORMATION": 2, "ERROR": 3}},
                {"name": "Text", "type": "char[]", "length": "remaining", "max": 40},
            ],
        },
    ],
}


def build_codec(data: Mapping[str, Any] = VAR_CCDD) -> Codec:
    return Codec(
        parse_model(copy.deepcopy(dict(data)), "var.json"),
        header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
        clock=lambda: 1_000_000.25,
    )


@pytest.fixture(scope="module")
def codec() -> Codec:
    return build_codec()


def wrap(c: Codec, name: str, payload: bytes) -> tuple[MessageKind, bytes]:
    """Message ``name`` around ``payload`` with a right header (length, checksum)."""
    msg = c.model.by_name[name]
    kind = msg.kind
    profile = c.profile(kind)
    inputs: dict[str, int] = {}
    if kind is CMD:
        assert profile.function_code_field is not None and msg.function_code is not None
        inputs[profile.function_code_field] = msg.function_code
    return kind, header.pack(profile, payload, mid=msg.mid, inputs=inputs, now=1_000_000.25)


def decoded(c: Codec, name: str, payload: bytes) -> DecodedPacket:
    kind, data = wrap(c, name, payload)
    result = c.decode(kind, data)
    assert isinstance(result, DecodedPacket), result
    return result


def error_of(c: Codec, name: str, payload: bytes) -> DecodeError:
    kind, data = wrap(c, name, payload)
    result = c.decode(kind, data)
    assert isinstance(result, DecodeError), result
    assert c.check(kind, data) == result  # the cheap half reports the very same error
    return result


# -- the CCDD: schema and layout -----------------------------------------------------------------


def test_variable_fields_in_the_model(codec: Codec) -> None:
    allm = codec.model.by_name["ALL"]
    assert allm.is_variable and allm.fixed_extent == 4
    assert (allm.min_payload, allm.max_payload, allm.payload_size) == (8, 80, 80)
    floats = allm.field("Floats")
    assert floats is not None and floats.var is not None
    assert floats.type.type_class is TypeClass.ARRAY and floats.type.text == "float32[]"
    assert (floats.var.unit, floats.var.count_field, floats.var.min, floats.var.max) == (
        VarUnit.COUNT,
        "nf",
        1,
        5,
    )
    assert floats.offset == allm.fixed_extent  # the start of the variable-length section
    blob, words = allm.field("Blob"), allm.field("Words")
    assert blob is not None and blob.type.type_class is TypeClass.BYTES and blob.var is not None
    assert blob.var.unit is VarUnit.LENGTH
    assert words is not None and words.var is not None
    assert words.var.unit is VarUnit.REMAINING and words.var.count_field is None
    fixed = codec.model.by_name["GAP"]
    assert fixed.fixed_extent == 5 and fixed.min_payload == 5 and fixed.payload_size == 11
    plain = codec.model.by_name["EVT"]
    assert plain.is_variable
    assert not load_ccdd(EXAMPLES / "ccdd" / "mock.json").by_name["MOCK_HK"].is_variable


def test_schema_0_still_loads_and_may_not_use_variable_fields() -> None:
    old: dict[str, Any] = {
        "schema": "ironbird-ccdd/0",
        "target": "t",
        "messages": [
            {
                "name": "A",
                "kind": "telemetry",
                "mid": "0x0801",
                "fields": [{"name": "x", "type": "uint8"}],
            }
        ],
    }
    model = parse_model(old, "old.json")
    assert not model.by_name["A"].is_variable
    assert model.by_name["A"].min_payload == model.by_name["A"].payload_size == 1
    old["messages"][0]["fields"].append(
        {"name": "v", "type": "uint8[]", "length": "remaining", "max": 4}
    )
    with pytest.raises(IronbirdError) as info:
        parse_model(old, "old.json")
    assert info.value.code is Code.CCD_SCHEMA and "ironbird-ccdd/1" in info.value.summary
    old["messages"][0]["fields"].pop()
    old["messages"][0]["min_payload"] = 1  # a /1 key
    with pytest.raises(IronbirdError) as info:
        parse_model(old, "old.json")
    assert info.value.code is Code.CCD_SCHEMA
    old["schema"] = "ironbird-ccdd/2"
    with pytest.raises(IronbirdError, match="ironbird-ccdd/0' or 'ironbird-ccdd/1"):
        parse_model(old, "old.json")


def test_declared_payload_bounds_that_agree_are_accepted() -> None:
    data = copy.deepcopy(VAR_CCDD)
    data["messages"][2].update(min_payload=1, max_payload=5)  # TRAIL: n + 0..4 items
    assert build_codec(data).model.by_name["TRAIL"].max_payload == 5


def test_shipped_examples_use_the_current_schema() -> None:
    for name in ("mock", "cfs"):
        text = (EXAMPLES / "ccdd" / f"{name}.json").read_text()
        assert json.loads(text)["schema"] == "ironbird-ccdd/1"
        model = load_ccdd(EXAMPLES / "ccdd" / f"{name}.json")
        assert sum(m.is_variable for m in model.messages) == 2  # one command or request, one reply


def test_a_message_must_fit_the_cap_and_the_length_field_at_its_largest(codec: Codec) -> None:
    # LEN is 19 bytes at its smallest and 217 at its largest: the largest is what the cap is for
    with pytest.raises(IronbirdError) as info:
        Codec(codec.model, codec.profile(CMD), codec.profile(TLM), max_packet_bytes=100)
    assert info.value.code is Code.ENC_FIT and "exceeds the 100-byte cap" in info.value.summary
    huge = copy.deepcopy(VAR_CCDD)
    huge["messages"] = [
        {
            "name": "HUGE",
            "kind": "telemetry",
            "mid": "0x0910",
            "fields": [{"name": "Raw", "type": "uint8[]", "length": "remaining", "max": 65535}],
        }
    ]
    model = parse_model(huge, "huge.json")
    assert model.by_name["HUGE"].max_payload == 65535
    with pytest.raises(IronbirdError) as info:  # 65551 bytes cannot be counted in 16 bits
        Codec(model, codec.profile(CMD), codec.profile(TLM), max_packet_bytes=70_000)
    assert info.value.code is Code.ENC_FIT and "16-bit" in info.value.summary


@pytest.mark.parametrize("path", sorted(CORPUS.glob("ccdd-var-*.json")), ids=lambda p: p.stem)
def test_var_ccdd_corpus_is_refused_with_a_coded_one_line_error(path: Path) -> None:
    with pytest.raises(IronbirdError) as info:
        load_ccdd(path)
    assert info.value.code.value.startswith("IB-CCD-")
    assert "\n" not in info.value.user_line()


def test_var_corpus_files_are_what_the_builder_builds() -> None:
    on_disk = {p.name: p.read_bytes() for p in CORPUS.glob("ccdd-var-*.json")}
    on_disk |= {p.name: p.read_bytes() for p in CORPUS.glob("varpacket-*.bin")}
    built = {name: (text + "\n").encode() for name, text in var_corpus.ccdd_cases().items()}
    built |= {name: packet.data for name, packet in var_corpus.packet_cases().items()}
    assert on_disk == built


def test_the_named_layout_mistakes_are_each_refused_for_that_reason() -> None:
    reasons = {
        "ccdd-var-count-after-array.json": "declared before",
        "ccdd-var-missing-max.json": "required key is missing",
        "ccdd-var-signed-count.json": "unsigned integer",
        "ccdd-var-remaining-not-last.json": "last field",
        "ccdd-var-remaining-is-a-field.json": "rename that field",
        "ccdd-var-count-too-narrow.json": "cannot be expressed",
        "ccdd-var-fixed-after-variable.json": "declare every fixed field first",
        "ccdd-var-count-shared.json": "share the count field",
        "ccdd-var-schema-0.json": "ironbird-ccdd/1",
        "ccdd-var-nine-fields.json": "more than 8",
    }
    for name, needle in reasons.items():
        with pytest.raises(IronbirdError) as info:
            load_ccdd(CORPUS / name)
        assert needle in info.value.summary, (name, info.value.summary)


# -- encode --------------------------------------------------------------------------------------


def test_every_kind_of_field_round_trips(codec: Codec) -> None:
    values: dict[str, object] = {
        "Floats": [1.5, -2.0],
        "Ints": (-3, 4, 5),
        "Blob": "00ff10",
        "Words": [7, 8, 9],
    }
    wire = codec.encode_telemetry("ALL", values).data
    got = codec.decode(TLM, wire)
    assert isinstance(got, DecodedPacket)
    assert got.as_dict() == {
        "nf": 2,
        "ni": 3,
        "blen": 3,
        "Floats": (1.5, -2.0),
        "Ints": (-3, 4, 5),
        "Blob": b"\x00\xff\x10",
        "Words": (7, 8, 9),
    }
    assert got.size == len(wire)
    # byte orders: the count ni is big-endian, the Ints are big-endian, the Words little-endian
    payload = wire[codec.profile(TLM).size_bytes :]
    assert payload[1:3] == b"\x00\x03"
    assert payload[4 + 8 : 4 + 8 + 6] == struct.pack(">3h", -3, 4, 5)
    assert payload[-6:] == struct.pack("<3H", 7, 8, 9)


def test_text_list_spellings(codec: Codec) -> None:
    for spelling in ("1,2,3", "[1,2,3]", "[1, 2, 3]", " 1 2  3 ", "1, 2,3"):
        wire = codec.encode_command("CMD", {"Data": "010203", "Name": spelling}).data
        assert codec.decode(CMD, wire).as_dict()["Name"] == spelling  # type: ignore[union-attr]
        t = codec.encode_telemetry("TRAIL", {"Items": spelling}).data
        assert codec.decode(TLM, t).as_dict() == {"n": 3, "Items": (1, 2, 3)}  # type: ignore[union-attr]
    empty = codec.encode_telemetry("TRAIL", {"Items": "[]"}).data
    assert codec.decode(TLM, empty).as_dict() == {"n": 0, "Items": ()}  # type: ignore[union-attr]
    blank = codec.encode_telemetry("TRAIL", {"Items": ""}).data
    assert blank[8:] == empty[8:]  # the payload; the header holds the sequence counter


def test_text_with_a_nul_is_refused_since_it_would_decode_cut_short(codec: Codec) -> None:
    for text in ("ab\x00cd", "\x00", "abc\x00"):
        with pytest.raises(IronbirdError) as info:
            codec.encode_telemetry("EVT", {"Text": text})
        assert info.value.code is Code.ENC_ARG and "NUL" in info.value.summary
        assert "\n" not in info.value.user_line()


def test_the_count_is_filled_in_and_a_disagreeing_one_is_refused(codec: Codec) -> None:
    ok = codec.encode_command("CMD", {"n": 2, "Data": "0102", "Name": "ab"})
    filled = codec.encode_command("CMD", {"Data": "0102", "Name": "ab"})
    assert ok.data[8:] == filled.data[8:]  # the payload; the header holds the sequence counter
    with pytest.raises(IronbirdError) as info:
        codec.encode_command("CMD", {"n": 5, "Data": "0102", "Name": "ab"})
    assert info.value.code is Code.ENC_ARG
    assert "--allow-count-mismatch" in info.value.summary and "n" in info.value.summary
    # fault injection: the count goes out as given, and the receiver refuses the packet
    bad = codec.encode_command(
        "CMD", {"n": 5, "Data": "0102", "Name": "ab"}, allow_count_mismatch=True
    )
    result = codec.decode(CMD, bad.data)
    assert isinstance(result, DecodeError) and result.code is Code.DEC_OVERRUN
    # a count above what the CCDD allows is a count that fits its field: injectable too
    over = codec.encode_command(
        "CMD", {"n": 200, "Data": "0102", "Name": "ab"}, allow_count_mismatch=True
    )
    result = codec.decode(CMD, over.data)
    assert isinstance(result, DecodeError) and result.code is Code.DEC_COUNT
    # the flag does not make a value that does not fit the count field acceptable
    with pytest.raises(IronbirdError) as info:
        codec.encode_command(
            "CMD", {"n": 300, "Data": "0102", "Name": "ab"}, allow_count_mismatch=True
        )
    assert info.value.code is Code.ENC_ARG


def test_a_length_field_counts_bytes(codec: Codec) -> None:
    wire = codec.encode_telemetry("LEN", {"Ws": [1, 2, 3]}).data
    assert wire[codec.profile(TLM).size_bytes] == 6
    assert codec.decode(TLM, wire).as_dict() == {"wlen": 6, "Ws": (1, 2, 3)}  # type: ignore[union-attr]
    with pytest.raises(IronbirdError, match="disagrees with the 6 bytes"):
        codec.encode_telemetry("LEN", {"wlen": 3, "Ws": [1, 2, 3]})


def test_a_missing_array_is_the_smallest_one_for_telemetry_and_an_error_for_a_command(
    codec: Codec,
) -> None:
    wire = codec.encode_telemetry("ALL").data  # Floats has min 1: one zero element
    got = codec.decode(TLM, wire)
    assert isinstance(got, DecodedPacket)
    assert got.as_dict() == {
        "nf": 1,
        "ni": 0,
        "blen": 0,
        "Floats": (0.0,),
        "Ints": (),
        "Blob": b"",
        "Words": (),
    }
    with pytest.raises(IronbirdError) as info:
        codec.encode_command("CMD", {"Name": "x"})
    assert info.value.code is Code.ENC_ARGSET and "Data" in info.value.summary
    assert (
        "'n'" not in info.value.summary and "n," not in info.value.summary
    )  # the count is not asked for


@pytest.mark.parametrize(
    ("name", "args", "needle"),
    [
        ("TRAIL", {"Items": "1,2,3,4,5"}, "5 elements exceed 4"),
        ("TRAIL", {"Items": "1,x"}, "Items[1]"),
        ("TRAIL", {"Items": "1,300"}, "Items[1]"),
        ("TRAIL", {"Items": "[1,2"}, "must end with ]"),
        ("TRAIL", {"Items": 5}, "unsupported value"),
        ("TRAIL", {"Items": b"\x01"}, "unsupported value"),
        ("TRAIL", {"Items": [1, True]}, "Items[1]"),
        ("TRAIL", {"Items": "1," * 600_000}, "longer than"),
        ("ALL", {"Floats": "nan,inf,1e999"}, "overflows"),
        ("ALL", {"Floats": []}, "0 elements; the CCDD allows 1 to 5"),
        ("ALL", {"Floats": [1], "Blob": "0"}, "hex digits"),
        ("ALL", {"Floats": [1], "Blob": "00" * 21}, "allows 0 to 20"),
        ("ALL", {"Floats": [1], "Blob": 7}, "unsupported value"),
        ("EVT", {"Text": "é"}, "ASCII"),
        ("EVT", {"Text": "x" * 41}, "exceed 40"),
        ("EVT", {"Text": 5}, "unsupported value"),
    ],
)
def test_bad_variable_arguments_are_coded_one_line_errors(
    codec: Codec, name: str, args: dict[str, object], needle: str
) -> None:
    with pytest.raises(IronbirdError) as info:
        codec.encode_telemetry(name, args)
    assert info.value.code is Code.ENC_ARG
    assert needle in info.value.summary and "\n" not in info.value.user_line()


def test_non_finite_floats_are_values_in_a_float_array(codec: Codec) -> None:
    wire = codec.encode_telemetry("ALL", {"Floats": "nan,inf,-inf"}).data
    got = codec.decode(TLM, wire)
    assert isinstance(got, DecodedPacket)
    floats = got.as_dict()["Floats"]
    assert (
        isinstance(floats, tuple)
        and floats[0] != floats[0]
        and floats[1:] == (float("inf"), float("-inf"))
    )


# -- decode (untrusted) --------------------------------------------------------------------------


def test_decode_reports_each_way_a_packet_disagrees_with_the_ccdd(codec: Codec) -> None:
    base = codec.profile(TLM).size_bytes
    # count above the maximum: IB-DEC-008 at the count field
    err = error_of(codec, "TRAIL", bytes([5, 1, 2, 3, 4]))
    assert (err.code, err.offset) == (Code.DEC_COUNT, base)
    # count beyond the packet: IB-DEC-009 at the start of the data
    err = error_of(codec, "TRAIL", bytes([4, 1, 2]))
    assert (err.code, err.offset) == (Code.DEC_OVERRUN, base + 1)
    # bytes after the last field: IB-DEC-007 where they start
    err = error_of(codec, "TRAIL", bytes([2, 1, 2, 9]))
    assert (err.code, err.offset) == (Code.DEC_PAYLOAD, base + 3) and "follow" in err.reason
    # a count of 0 is a valid empty array
    assert decoded(codec, "TRAIL", bytes([0])).as_dict() == {"n": 0, "Items": ()}
    # the shortest and the longest payload of the message are in range, one more or less is not
    assert decoded(codec, "TRAIL", bytes([4, 1, 2, 3, 4])).values == (4, (1, 2, 3, 4))
    for payload in (b"", bytes(6)):
        assert error_of(codec, "TRAIL", payload).code is Code.DEC_PAYLOAD
    # a byte length that is not a whole number of elements
    err = error_of(codec, "LEN", bytes([3, 1, 0, 2]))
    assert (err.code, err.offset) == (Code.DEC_PAYLOAD, base) and "whole number" in err.reason
    # a byte length above the maximum in bytes, then below the minimum
    assert error_of(codec, "LEN", bytes([202]) + bytes(100)).code is Code.DEC_COUNT
    assert error_of(codec, "LEN", bytes([0])).code is Code.DEC_PAYLOAD
    # "remaining" that is not whole elements, then above the maximum
    odd = bytes([1, 0, 0, 0]) + bytes(4) + b"\x01\x02\x03"
    err = error_of(codec, "ALL", odd)
    assert err.code is Code.DEC_PAYLOAD and "whole number" in err.reason
    err = error_of(codec, "ALL", bytes([1, 0, 0, 0]) + bytes(4) + bytes(18))
    assert err.code is Code.DEC_COUNT and err.offset == base + 8
    # below the field's own minimum (Floats needs 1) although the message payload is in range
    err = error_of(codec, "ALL", bytes([0, 0, 0, 0]) + b"\x00\x01\x02\x03\x04\x05\x06\x07")
    assert err.code is Code.DEC_PAYLOAD and "minimum" in err.reason
    # a command's counted bytes then its tail string
    got = decoded(codec, "CMD", bytes([2, 0xAA, 0xBB]) + b"hey")
    assert got.as_dict() == {"n": 2, "Data": b"\xaa\xbb", "Name": "hey"}
    assert error_of(codec, "CMD", bytes([2, 0xAA, 0xBB])).code is Code.DEC_PAYLOAD  # Name needs 1
    assert error_of(codec, "CMD", bytes([9]) + bytes(9) + b"x").code is Code.DEC_COUNT


def test_a_gap_between_fixed_fields_and_the_data_after_them(codec: Codec) -> None:
    payload = bytes([2, 0, 0, 0, 77]) + struct.pack("<2H", 5, 6)
    assert decoded(codec, "GAP", payload).values == (77, 2, (5, 6))
    assert error_of(codec, "GAP", bytes([4, 0, 0, 0, 77]) + bytes(6)).code is Code.DEC_COUNT


def test_text_stops_at_a_nul_and_controls_stay_data(codec: Codec) -> None:
    got = decoded(codec, "CMD", bytes([0]) + b"ab\0cd\x1b[2J")
    assert got.as_dict()["Name"] == "ab"
    # the raw escape is data until it is displayed: the renderer sanitizes it
    got = decoded(codec, "CMD", bytes([0]) + b"a\x1b[2Jb")
    assert "\x1b" not in render.line("n", 0, got)


def test_decode_resolved_takes_the_counts_the_check_read(codec: Codec) -> None:
    kind, data = wrap(codec, "TRAIL", bytes([3, 1, 2, 3]))
    resolved = codec.resolve(kind, data)
    assert not isinstance(resolved, DecodeError)
    assert resolved.counts == (3,)
    via_token = codec.decode_resolved(kind, resolved, data)
    assert via_token == codec.decode(kind, data)
    # a token for other bytes is not believed; the whole check runs and answers for them
    other = wrap(codec, "TRAIL", bytes([1, 9]))[1]
    assert codec.decode_resolved(kind, resolved, other) == codec.decode(kind, other)
    fixed = codec.resolve(TLM, codec.encode_telemetry("EVT", {"Text": ""}).data)
    assert not isinstance(fixed, DecodeError) and fixed.counts == (0,)


def test_a_resolved_cannot_be_forged_and_a_mispaired_one_is_never_believed() -> None:
    """Only ``resolve`` makes a token and its fields are read-only; every other pairing of a
    token (or look-alike) with bytes answers exactly what ``decode`` does, and never raises."""
    c = var_corpus.codec()
    data = c.encode_telemetry("MOCK_DATA", {"Entries": [1, 2, 3], "Text": "abc"}).data
    token = c.resolve(TLM, data)
    assert isinstance(token, Resolved) and token.counts == (3, 3)
    expected = c.decode(TLM, data)
    assert isinstance(expected, DecodedPacket)
    assert c.decode_resolved(TLM, token, data) == expected

    # no way to construct one, change one or copy one through the public surface
    msg = token.message
    for key in (None, 0, object(), ()):
        with pytest.raises(TypeError):
            Resolved(key, msg, token.word, c, data, token.counts)  # type: ignore[arg-type]
    for name, value in (("word", 0), ("counts", ()), ("message", msg), ("data", b""), ("x", 1)):
        with pytest.raises(AttributeError):
            setattr(token, name, value)
    with pytest.raises(AttributeError):
        del token.word
    assert not hasattr(token, "_replace")
    with pytest.raises(TypeError):
        copy.copy(token)
    with pytest.raises(TypeError):
        copy.deepcopy(token)
    assert c.decode_resolved(TLM, token, data) == expected  # still the one the check made

    # look-alikes that are not a Resolved
    @dataclass
    class Lookalike:
        message: Any
        word: Any
        counts: Any
        codec: Any
        data: Any

    lookalikes: list[object] = [
        (msg, token.word, c, data, token.counts),
        Lookalike(msg, token.word, token.counts, c, data),
        Lookalike(msg, None, (3.0, 3), c, data),
        Lookalike(msg, "x", (-1, 3), c, data),
        None,
        "MOCK_DATA",
        3.5,
    ]
    for fake in lookalikes:
        assert c.decode_resolved(TLM, fake, data) == expected, fake  # type: ignore[arg-type]

    # a real token paired with other bytes, a mutable copy, another kind or another codec
    flipped = bytearray(data)
    flipped[5] ^= 1  # the length byte
    last = bytearray(data)
    last[-1] ^= 0xFF  # the last byte (checksum or text)
    for other in (bytes(flipped), bytes(last), bytes(data), data[:-1], data + b"\0", b""):
        assert c.decode_resolved(TLM, token, other) == c.decode(TLM, other)
    for mutable in (bytearray(data), memoryview(data), memoryview(bytearray(data))):
        assert c.decode_resolved(TLM, token, mutable) == expected
    assert c.decode_resolved(CMD, token, data) == c.decode(CMD, data)
    twin = var_corpus.codec()
    assert twin.decode_resolved(TLM, token, data) == twin.decode(TLM, data)

    # a command token against a different command of the same MID
    kind, mode = wrap(c, "MOCK_SET_MODE", bytes(2))
    mode_token = c.resolve(kind, mode)
    assert isinstance(mode_token, Resolved)
    assert c.decode_resolved(kind, mode_token, mode) == c.decode(kind, mode)
    for name in ("MOCK_SET_LABEL", "MOCK_NOOP", "MOCK_LOAD_DATA"):
        other = wrap(c, name, bytes(2))[1]
        assert c.decode_resolved(kind, mode_token, other) == c.decode(kind, other), name


def test_the_checksum_span_is_checked_at_every_size_it_can_cut_the_field(tmp_path: Path) -> None:
    """A span that ends 10 bytes before the packet end leaves the 2-byte checksum field at bytes
    6..7 outside it at 16 bytes and inside it at 18, but cuts it at 17: the sizes between the
    smallest and the largest packet are checked, not just those two."""
    profile = tmp_path / "cut.toml"
    profile.write_text(
        'name = "cut_tlm"\nkind = "telemetry"\nbyte_order = "big"\n'
        'mid_fields = ["version", "packet_type", "sec_hdr_flag", "apid"]\n'
        '[[fields]]\nname = "version"\nbits = 3\nvalue = 0\n'
        '[[fields]]\nname = "packet_type"\nbits = 1\nvalue = 0\n'
        '[[fields]]\nname = "sec_hdr_flag"\nbits = 1\nvalue = 1\n'
        '[[fields]]\nname = "apid"\nbits = 11\ncomputed = "from_mid"\n'
        '[[fields]]\nname = "seq_flags"\nbits = 2\nvalue = 3\ncheck = false\n'
        '[[fields]]\nname = "sequence"\nbits = 14\ncomputed = "sequence"\n'
        '[[fields]]\nname = "length"\nbits = 16\ncomputed = "length"\nbias = -7\n'
        '[[fields]]\nname = "ck"\nbits = 16\ncomputed = "checksum"\n'
        'algorithm = "crc16_ccitt"\nspan = { start = 0, end = -10 }\n'
    )
    cut = header.load_profile(profile)
    ck = cut.checksum_field
    assert ck is not None and cut.min_bytes == 8
    assert not isinstance(header.checksum_plan(cut, ck, 16), str)
    assert not isinstance(header.checksum_plan(cut, ck, 18), str)
    assert "cuts through" in str(header.checksum_plan(cut, ck, 17))
    assert 17 in header.checksum_probe_sizes(cut, ck, 16, 18)

    def build(low: int, high: int) -> Codec:
        message = {
            "name": "V",
            "kind": "telemetry",
            "mid": "0x0801",
            "fields": [
                {"name": "D", "type": "uint8[]", "length": "remaining", "min": low, "max": high}
            ],
        }
        raw = {"schema": "ironbird-ccdd/1", "target": "t", "messages": [message]}
        cmd = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
        return Codec(parse_model(raw, "t"), cmd, cut)

    with pytest.raises(IronbirdError) as info:
        build(8, 10)
    assert info.value.code is Code.ENC_FIT and "cuts through" in info.value.summary
    build(8, 8)  # one size, no cut
    build(10, 10)


def test_truncation_at_every_offset_is_typed_and_check_agrees(codec: Codec) -> None:
    wire = codec.encode_telemetry(
        "ALL", {"Floats": [1.0, 2.0], "Ints": [1, 2, 3], "Blob": "aabb", "Words": [1, 2]}
    ).data
    profile = codec.profile(TLM)
    payload = wire[profile.size_bytes :]
    kind = TLM
    for cut in range(len(wire)):  # the whole packet cut anywhere: always an error
        result = codec.decode(kind, wire[:cut])
        assert isinstance(result, DecodeError)
        assert codec.check(kind, wire[:cut]) == result
    for cut in range(len(payload)):  # a payload cut anywhere under a right header
        _, data = wrap(codec, "ALL", payload[:cut])
        result = codec.decode(kind, data)
        assert codec.check(kind, data) == (result if isinstance(result, DecodeError) else None)
        if cut < len(payload) - 4:  # the counts say more than is there
            assert isinstance(result, DecodeError), cut
    for extra in (1, 2, 3, 64):  # and extended
        _, data = wrap(codec, "ALL", payload + bytes(extra))
        result = codec.decode(kind, data)
        assert codec.check(kind, data) == (result if isinstance(result, DecodeError) else None)


# Arbitrary bytes, and bytes that start like a packet (small counts) so later checks are reached.
PAYLOADS = st.one_of(
    st.binary(max_size=300),
    st.builds(
        lambda head, tail: bytes(head) + tail,
        st.lists(st.integers(0, 6), max_size=6),
        st.binary(max_size=100),
    ),
)


@settings(max_examples=600, deadline=1000, suppress_health_check=[HealthCheck.too_slow])
@given(name=st.sampled_from(["ALL", "LEN", "TRAIL", "GAP", "CMD", "EVT"]), payload=PAYLOADS)
def test_arbitrary_payloads_never_raise_and_check_equals_decode(name: str, payload: bytes) -> None:
    c = _CODEC
    kind, data = wrap(c, name, payload)
    error = c.check(kind, data)
    result = c.decode(kind, data)
    if error is None:
        assert isinstance(result, DecodedPacket)
        if not any(f.type.type_class is TypeClass.CHAR for f in result.message.fields):
            # what decoded encodes back to a packet that decodes to the same values
            again = c.decode(kind, _reencode(c, result))
            assert isinstance(again, DecodedPacket)
            assert _stable(again.values) == _stable(result.values)
    else:
        assert result == error
        assert error.code in {Code.DEC_PAYLOAD, Code.DEC_COUNT, Code.DEC_OVERRUN}
        assert 0 <= error.offset <= len(data)


@settings(max_examples=300, deadline=1000, suppress_health_check=[HealthCheck.too_slow])
@given(
    count=st.integers(0, 0xFFFF),
    entries=st.integers(0, 70),
    text=st.binary(max_size=140),
)
def test_counts_beyond_the_buffer_and_the_maximum_are_rejected_on_the_mock(
    count: int, entries: int, text: bytes
) -> None:
    c = var_corpus.codec()
    payload = struct.pack("<H", count) + bytes(4 * entries) + text
    kind, data = wrap(c, "MOCK_LOAD_DATA", payload)
    result = c.decode(kind, data)
    tail = len(payload) - 2 - 4 * count  # what is left for Text after Count entries
    if len(payload) > 380:
        want: Code | None = Code.DEC_PAYLOAD
    elif count > 64:
        want = Code.DEC_COUNT
    elif tail < 0:
        want = Code.DEC_OVERRUN
    elif tail > 122:
        want = Code.DEC_COUNT
    else:
        want = None
    if want is None:
        assert isinstance(result, DecodedPacket)
        assert result.values[0] == count and len(result.values[1]) == count  # type: ignore[arg-type]
    else:
        assert isinstance(result, DecodeError) and result.code is want, (result, want)


_CODEC = build_codec()


def _stable(values: tuple[object, ...]) -> tuple[object, ...]:
    """Values compared across a re-encode: NaN is not equal to itself, so it is spelled."""
    return tuple(
        tuple("nan" if isinstance(v, float) and math.isnan(v) else v for v in x)
        if isinstance(x, tuple)
        else x
        for x in values
    )


def _reencode(c: Codec, packet: DecodedPacket) -> bytes:
    """The packet re-encoded from its own values (counts filled in)."""
    msg = packet.message
    counts = {f.var.count_field for f in msg.fields if f.var is not None and f.var.count_field}
    args = {
        f.name: v for f, v in zip(msg.fields, packet.values, strict=True) if f.name not in counts
    }
    if msg.kind is CMD:
        return c.encode_command(msg.name, args).data
    return c.encode_telemetry(msg.name, args).data


@settings(max_examples=200, deadline=1000, suppress_health_check=[HealthCheck.too_slow])
@given(
    floats=st.lists(st.floats(width=32, allow_nan=False), min_size=1, max_size=5),
    ints=st.lists(st.integers(-(2**15), 2**15 - 1), max_size=10),
    blob=st.binary(max_size=20),
    words=st.lists(st.integers(0, 65535), max_size=8),
)
def test_encode_then_decode_returns_the_values(
    floats: list[float], ints: list[int], blob: bytes, words: list[int]
) -> None:
    c = _CODEC
    wire = c.encode_telemetry(
        "ALL", {"Floats": floats, "Ints": ints, "Blob": blob, "Words": words}
    ).data
    got = c.decode(TLM, wire)
    assert isinstance(got, DecodedPacket)
    assert got.as_dict() == {
        "nf": len(floats),
        "ni": len(ints),
        "blen": len(blob),
        "Floats": tuple(floats),
        "Ints": tuple(ints),
        "Blob": blob,
        "Words": tuple(words),
    }


# -- the wire corpus -------------------------------------------------------------------------


@pytest.mark.parametrize("path", sorted(CORPUS.glob("varpacket-*.bin")), ids=lambda p: p.stem)
def test_var_packet_corpus_is_rejected_with_the_expected_error(path: Path) -> None:
    c = var_corpus.codec()
    kind = CMD if "-cmd-" in path.name else TLM
    result = c.decode(kind, path.read_bytes())
    expected = var_corpus.packet_cases()[path.name]
    assert isinstance(result, DecodeError)
    assert (result.code, result.offset) == (expected.code, expected.offset)
    assert c.check(kind, path.read_bytes()) == result
    assert isinstance(c.decode(TLM if kind is CMD else CMD, path.read_bytes()), DecodeError)


# -- rendering and export ------------------------------------------------------------------------


def test_line_shows_sixteen_elements_and_how_many_more(codec: Codec) -> None:
    few = decoded(codec, "TRAIL", bytes([3, 1, 2, 3]))
    assert "Items=[1 2 3]" in render.line("n", 0, few)
    mock = var_corpus.codec()
    wire = mock.encode_telemetry("MOCK_DATA", {"Entries": list(range(40)), "Text": "hi"}).data
    got = mock.decode(TLM, wire)
    assert isinstance(got, DecodedPacket)
    line = render.line("n", 0, got)
    assert "Entries=[0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 ...(+24)]" in line
    assert "Text=hi" in line
    sixteen = mock.encode_telemetry("MOCK_DATA", {"Entries": list(range(16))}).data
    exact = mock.decode(TLM, sixteen)
    assert isinstance(exact, DecodedPacket) and "...(" not in render.line("n", 0, exact)


def test_line_shows_bytes_as_hex_with_a_count_of_the_rest(codec: Codec) -> None:
    short = decoded(codec, "CMD", bytes([2, 0xAA, 0xBB]) + b"x")
    assert "Data=aabb" in render.line("n", 0, short)
    many = decoded(codec, "ALL", bytes([1, 0, 0, 20]) + bytes(4) + bytes(range(20)))
    assert "Blob=" + bytes(range(16)).hex() + " ...(+4)" in render.line("n", 0, many)


def test_json_record_has_full_lists_and_finite_numbers_only(codec: Codec) -> None:
    mock = var_corpus.codec()
    wire = mock.encode_telemetry("MOCK_DATA", {"Entries": list(range(40)), "Text": "hi"}).data
    got = mock.decode(TLM, wire)
    assert isinstance(got, DecodedPacket)
    record = json.loads(json.dumps(render.record("n", 0, got)))
    assert record["fields"]["Entries"] == list(range(40))
    assert record["fields"]["Count"] == 40 and record["fields"]["Text"] == "hi"
    floats = codec.decode(TLM, codec.encode_telemetry("ALL", {"Floats": "nan,inf,2"}).data)
    assert isinstance(floats, DecodedPacket)
    out = render.record("n", 0, floats)["fields"]["Floats"]
    assert out[0] == "nan" and out[1] == "inf" and out[2] == 2.0
    blob = decoded(codec, "ALL", bytes([1, 0, 0, 20]) + bytes(4) + bytes(range(20)))
    assert render.record("n", 0, blob)["fields"]["Blob"] == bytes(range(20)).hex()  # not cut


def test_a_csv_cell_holds_the_whole_array(codec: Codec) -> None:
    from ironbird.recording.export import _converter

    mock = var_corpus.codec()
    msg = mock.model.by_name["MOCK_DATA"]
    entries = msg.field("Entries")
    assert entries is not None
    cell = _converter(entries, False)((1, 2, 3))
    assert cell == "[1 2 3]" and _converter(entries, False)(()) == "[]"
    assert csv_line(["x", cell]) == "x,[1 2 3]\n"
    floats = codec.model.by_name["ALL"].field("Floats")
    assert floats is not None
    assert _converter(floats, False)((1.5, float("nan"))) == "[1.5 nan]"
    text = codec.model.by_name["EVT"].field("Text")
    assert text is not None
    assert _converter(text, False)("=cmd()") == "'=cmd()"  # spreadsheet formulas stay defused
    row = next(csv.reader(io.StringIO(csv_line([cell, "a,b"]))))
    assert row == ["[1 2 3]", "a,b"]


# -- events --------------------------------------------------------------------------------------


def test_an_event_message_may_be_a_variable_length_char_field(codec: Codec) -> None:
    from ironbird.events import extract_event

    wire = codec.encode_telemetry(
        "EVT", {"app": "APP", "id": 9, "type": "ERROR", "Text": "bus voltage low"}
    ).data
    got = codec.decode(TLM, wire)
    assert isinstance(got, DecodedPacket)
    event = extract_event("n", 0, got)
    assert event is not None and event.text == "bus voltage low" and event.event_id == 9


# -- the filter ----------------------------------------------------------------------------------


def _filter_frame(codec: Codec, name: str, values: dict[str, object]) -> Any:
    return make_frame(codec, STREAMS[("n1", TLM)], codec.encode_telemetry(name, values).data)


def _matches(codec: Codec, text: str, frame: Any) -> bool:
    decode, _ = count_decodes({"n1": codec})
    matcher = compile_filter(text, {"n1": codec}, links=LINKS).bind(decode, lambda node: codec)
    return bool(matcher(frame))


def test_the_filter_takes_variable_text_and_bytes_but_not_a_number_array(codec: Codec) -> None:
    evt = _filter_frame(codec, "EVT", {"app": "A", "Text": "bus voltage low"})
    assert _matches(codec, 'Text contains "voltage"', evt)
    assert not _matches(codec, 'Text contains "current"', evt)
    assert _matches(codec, 'Text == "bus voltage low"', evt)
    all_ = _filter_frame(codec, "ALL", {"Floats": [1], "Blob": "aabb"})
    assert _matches(codec, 'Blob == "aabb"', all_)  # any length up to the maximum
    assert not _matches(codec, 'Blob == "aabbcc"', all_)
    assert _matches(codec, 'Blob contains "bb"', all_)
    empty = _filter_frame(codec, "ALL", {"Floats": [1], "Blob": ""})
    assert _matches(codec, 'Blob == ""', empty) and not _matches(codec, 'Blob != ""', empty)
    assert not _matches(codec, 'Blob == ""', all_) and _matches(codec, 'Blob != ""', all_)
    for bad in ('Blob contains ""', 'Floats == ""'):
        with pytest.raises(FilterError):
            compile_filter(bad, {"n1": codec}, links=LINKS)
    with pytest.raises(FilterError) as info:
        compile_filter('Blob == "' + "aa" * 21 + '"', {"n1": codec}, links=LINKS)
    assert info.value.code is Code.FLT_TYPE and "at most 20" in info.value.summary
    with pytest.raises(FilterError) as info:
        compile_filter("Floats == 1", {"n1": codec}, links=LINKS)
    assert info.value.code is Code.FLT_TYPE and "array" in info.value.summary


# -- engine, session and CLI ---------------------------------------------------------------------


async def _next(iterator: Any) -> Any:
    return await asyncio.wait_for(iterator.__anext__(), timeout=5)


def test_a_variable_length_command_and_its_reply_through_respond(
    mock_config_path: Path, tmp_path: Path
) -> None:
    from test_e2e_mock import _events, _run

    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, *_: Any) -> None:
        responder = await fc.respond()
        listener = await ground.listen("fc")
        handled, received = aiter(responder), aiter(listener)
        try:
            sent = await ground.send_command(
                "fc", "MOCK_LOAD_DATA", {"Entries": "10,20,30", "Text": "hello"}
            )
            assert sent.size == 8 + 2 + 12 + 5  # header, Count, three entries, the string
            event: Handled = await _next(handled)
            assert event.command is not None
            assert event.command.as_dict() == {"Count": 3, "Entries": (10, 20, 30), "Text": "hello"}
            reply: Decoded = await _next(received)
            assert reply.packet.message.name == "MOCK_DATA"
            assert reply.packet.as_dict() == {"Count": 3, "Entries": (10, 20, 30), "Text": "hello"}
            await ground.send_command("fc", "MOCK_LOAD_DATA", {"Entries": [], "Text": ""})
            await _next(handled)
            empty: Decoded = await _next(received)
            assert empty.packet.as_dict() == {"Count": 0, "Entries": (), "Text": ""}
            full = {"Entries": list(range(64)), "Text": "x" * 122}
            big = await ground.send_command("fc", "MOCK_LOAD_DATA", full)
            assert big.size == 8 + 380
            await _next(handled)
            last: Decoded = await _next(received)
            assert last.packet.as_dict()["Entries"] == tuple(range(64))
            assert last.packet.size == 16 + 380
        finally:
            await responder.close()
            await listener.close()

    ground_ledger, fc_ledger = _run(config, tmp_path, scenario)
    sent = [e for e in _events(ground_ledger) if e["event"] == "tx.command"]
    assert [e["size"] for e in sent] == [27, 10, 388]
    received = [e for e in _events(fc_ledger) if e["event"] == "rx.command"]
    assert received[0]["args"]["Entries"] == [10, 20, 30]


def test_a_malformed_variable_length_packet_is_reported_once_and_counted(
    mock_config_path: Path, tmp_path: Path
) -> None:
    from test_e2e_mock import _run

    config = load_config(mock_config_path)
    flood = 150

    async def scenario(
        ground: Session, fc: Session, ground_notes: list[str], fc_notes: list[str]
    ) -> None:
        responder = await fc.respond()
        listener = await ground.listen("fc")
        received = aiter(listener)
        addr = listener.local_address
        assert addr is not None
        c = fc.codec_for("fc")
        _, over = wrap_mock(c, TLM, "MOCK_DATA", struct.pack("<H", 65) + bytes(4 * 65))
        _, overrun = wrap_mock(c, TLM, "MOCK_DATA", struct.pack("<H", 9) + bytes(8))
        good = c.encode_telemetry("MOCK_DATA", {"Entries": [1, 2], "Text": "ok"}).data
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            for i in range(flood):
                raw.sendto(over, addr)
                raw.sendto(overrun, addr)
                if i % 10 == 9:
                    await asyncio.sleep(0.002)
            raw.sendto(good, addr)
            rx: Decoded = await _next(received)
            assert rx.packet.as_dict()["Text"] == "ok"  # nothing malformed reached the consumer
            assert ground._engine.malformed_total == 2 * flood
        finally:
            raw.close()
            await responder.close()
            await listener.close()
        ground.flush_notices()
        counted = [n for n in ground_notes if "IB-DEC-008" in n]
        overran = [n for n in ground_notes if "IB-DEC-009" in n]
        assert 1 <= len(counted) <= 3 and 1 <= len(overran) <= 3, ground_notes
        assert any(f"x{flood}" in n for n in counted), counted
        assert len(ground_notes) < 12  # alert fatigue: not one line per packet

    _run(config, tmp_path, scenario)


def wrap_mock(c: Codec, kind: MessageKind, name: str, payload: bytes) -> tuple[MessageKind, bytes]:
    msg = c.model.by_name[name]
    profile = c.profile(kind)
    inputs: dict[str, int] = {}
    if kind is CMD:
        assert profile.function_code_field is not None and msg.function_code is not None
        inputs[profile.function_code_field] = msg.function_code
    return kind, header.pack(profile, payload, mid=msg.mid, inputs=inputs, now=1.0)


def test_count_mismatch_is_refused_before_anything_is_sent_unless_asked_for(
    mock_config_path: Path, tmp_path: Path
) -> None:
    from test_e2e_mock import _events, _run

    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, *_: Any) -> None:
        responder = await fc.respond()
        handled = aiter(responder)
        try:
            with pytest.raises(IronbirdError) as info:
                await ground.send_command(
                    "fc", "MOCK_LOAD_DATA", {"Count": 9, "Entries": "1,2", "Text": "x"}
                )
            assert info.value.code is Code.ENC_ARG
            await ground.send_command(
                "fc",
                "MOCK_LOAD_DATA",
                {"Count": 9, "Entries": "1,2", "Text": "x"},
                allow_count_mismatch=True,
            )
            event: Handled = await _next(handled)
            assert event.command is None and event.error is not None
            assert event.error.code is Code.DEC_OVERRUN
        finally:
            await responder.close()

    ground_ledger, fc_ledger = _run(config, tmp_path, scenario)
    ground_events = _events(ground_ledger)
    requests = [e for e in ground_events if e["event"] == "send.request"]
    assert [bool(e.get("allow_count_mismatch")) for e in requests] == [False, True]
    faults = [e for e in ground_events if e["event"] == "send.fault"]
    assert len(faults) == 1 and faults[0]["fault"] == "count_mismatch"
    assert faults[0]["fields"] == [{"field": "Count", "given": 9, "implied": 2, "unit": "elements"}]
    assert len([e for e in ground_events if e["event"] == "tx.command"]) == 1
    assert any(e["event"] == "rx.malformed" for e in _events(fc_ledger))


def test_a_reply_rule_with_a_wrong_count_is_refused_without_naming_a_send_option(
    codec: Codec,
) -> None:
    with pytest.raises(IronbirdError) as info:
        codec.encode_telemetry("TRAIL", {"n": 3, "Items": [1]})
    assert info.value.code is Code.ENC_ARG and "disagrees" in info.value.summary
    assert "--allow-count-mismatch" not in info.value.summary
    packet = codec.encode_telemetry("TRAIL", {"n": 3, "Items": [1]}, allow_count_mismatch=True)
    assert [(f.field, f.given, f.implied) for f in packet.count_faults] == [("n", 3, 1)]
    args = {"n": 2, "Data": "0102", "Name": "ab"}
    same = codec.encode_command("CMD", args, allow_count_mismatch=True)
    assert same.count_faults == ()  # a value equal to the implied one is no fault
    assert codec.count_field_names("CMD", CMD) == ("n",)
    assert codec.count_field_names("EVT", TLM) == ()


def test_cli_send_takes_lists_and_the_fault_injection_flag(mock_config_path: Path) -> None:
    port = int(re.search(r"127\.0\.0\.1:(\d+)", mock_config_path.read_text()).group(1))  # type: ignore[union-attr]
    sink = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sink.bind(("127.0.0.1", port))
    sink.settimeout(2)
    runner = CliRunner()
    base = ["-c", str(mock_config_path), "send", "fc", "MOCK_LOAD_DATA"]
    try:
        ok = runner.invoke(app, [*base, "Entries=[1,2,3]", "Text=hello world"])
        assert ok.exit_code == 0, ok.output
        assert len(sink.recv(2048)) == 8 + 2 + 12 + 11
        refused = runner.invoke(app, [*base, "Count=7", "Entries=1,2", "Text=x"])
        assert refused.exit_code == 1 and "IB-ENC-002" in refused.output
        assert "--allow-count-mismatch" in refused.output
        injected = runner.invoke(
            app, [*base, "Count=7", "Entries=1,2", "Text=x", "--allow-count-mismatch"]
        )
        assert injected.exit_code == 0, injected.output
        wire = sink.recv(2048)  # only the injected one went out: the refused one sent nothing
        assert wire[8:10] == b"\x07\x00" and len(wire) == 8 + 2 + 8 + 1
        over = runner.invoke(app, [*base, "Entries=" + ",".join(["1"] * 65), "Text=x"])
        assert over.exit_code == 1 and "65 elements exceed 64" in over.output
        # the flag needs a count or length field to be wrong about
        none = runner.invoke(app, ["-c", str(mock_config_path), "send", "fc", "MOCK_NOOP"])
        assert none.exit_code == 0, none.output
        sink.recv(2048)
        fixed = runner.invoke(
            app, ["-c", str(mock_config_path), "send", "fc", "MOCK_NOOP", "--allow-count-mismatch"]
        )
        assert fixed.exit_code == 2 and "IB-CLI-" in fixed.output, fixed.output
        sink.settimeout(0.3)
        with pytest.raises(TimeoutError):  # nothing went out
            sink.recv(2048)
    finally:
        sink.close()


def test_adapter_rejects_what_it_cannot_describe_without_raising() -> None:
    adapter = JsonAdapter()
    for text in (
        '{"schema":"ironbird-ccdd/1","target":"t","messages":[{"name":"A","kind":"telemetry",'
        '"mid":"0x801","fields":[{"name":"c","type":"uint8"},{"name":"v","type":"uint8[]",'
        '"count":"c","max":1e3}]}]}',
        '{"schema":"ironbird-ccdd/1","target":"t","messages":[{"name":"A","kind":"telemetry",'
        '"mid":"0x801","fields":[{"name":"v","type":"[]","length":"remaining","max":3}]}]}',
        '{"schema":"ironbird-ccdd/1","target":"t","messages":[{"name":"A","kind":"telemetry",'
        '"mid":"0x801","fields":[{"name":"v","type":"char[]","length":["remaining"],"max":3}]}]}',
        '{"schema":"ironbird-ccdd/1","target":"t","messages":[{"name":"A","kind":"telemetry",'
        '"mid":"0x801","fields":[{"name":"v","type":"char[]","length":"remaining","max":3,'
        '"endian":"middle"}]}]}',
    ):
        with pytest.raises(IronbirdError) as info:
            adapter.load_bytes(text.encode(), "x.json")
        assert info.value.code.value.startswith("IB-CCD-")
