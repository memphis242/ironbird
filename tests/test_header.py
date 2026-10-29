from __future__ import annotations

import copy
from typing import Any

import pytest

from conftest import EXAMPLES
from ironbird import checksum, header
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.header import Header, HeaderProfile

CMD = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
TLM = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml")


def _unpacked(profile: HeaderProfile, data: bytes) -> Header:
    result = header.unpack(profile, data)
    assert isinstance(result, Header), result
    return result


def test_profile_shapes() -> None:
    assert CMD.size_bytes == 8
    assert TLM.size_bytes == 16
    assert CMD.mid_bits == 16
    assert CMD.function_code_field == "function_code"
    assert CMD.checksum_field is not None and CMD.checksum_field.algorithm == "xor8_cfs"


def test_command_pack_matches_hand_built_bytes() -> None:
    # mid 0x1805 -> stream id word 0x1805; seq flags 3 + seq 0 -> c000; length 8+2-7 = 3
    packet = header.pack(CMD, b"\x01\x00", mid=0x1805, inputs={"function_code": 1})
    assert packet[:6] == bytes.fromhex("1805c0000003")
    assert packet[6] == 0x01  # reserved bit 0, function code 1
    xor = 0
    for byte in packet:
        xor ^= byte
    assert xor == 0xFF  # cFS rule: XOR over the whole packet is 0xFF


def test_command_round_trip_with_checksum_and_sequence() -> None:
    packet = header.pack(
        CMD, b"\xaa\xbb\xcc", mid=0x1805, inputs={"function_code": 99}, sequence=1234
    )
    parsed = _unpacked(CMD, packet)
    assert parsed.mid == 0x1805
    assert parsed.sequence == 1234
    assert parsed.values["function_code"] == 99
    assert parsed.values["apid"] == 5
    assert parsed.size == 8


def test_peek_mid_matches_unpack_and_handles_short_input() -> None:
    packet = header.pack(CMD, b"", mid=0x18FF, inputs={"function_code": 0})
    assert header.peek_mid(CMD, packet) == 0x18FF
    assert header.peek_mid(CMD, packet[:1]) is None
    assert header.peek_mid(CMD, b"") is None


def test_telemetry_time_fields() -> None:
    packet = header.pack(TLM, b"\x00\x00", mid=0x0805, now=1_000_000.25)
    parsed = _unpacked(TLM, packet)
    assert parsed.values["seconds"] == 1_000_000
    assert parsed.values["subseconds"] == 0x4000
    assert packet[:6] == bytes.fromhex("0805c000000b")  # 16 + 2 - 7


def test_mid_conflicting_with_profile_is_rejected() -> None:
    with pytest.raises(IronbirdError) as info:
        header.pack(CMD, b"", mid=0x0805, inputs={"function_code": 0})  # telemetry-type MID
    assert info.value.code is Code.ENC_FIT


def test_input_out_of_range_is_rejected() -> None:
    with pytest.raises(IronbirdError) as info:
        header.pack(CMD, b"", mid=0x1805, inputs={"function_code": 128})
    assert info.value.code is Code.HDR_PACK


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda p: p[:7], Code.DEC_SHORT),
        (lambda p: p + b"\x00", Code.DEC_LENGTH),
        (lambda p: p[:4] + b"\x00\x09" + p[6:], Code.DEC_LENGTH),
        (lambda p: p[:7] + bytes([p[7] ^ 0x01]) + p[8:], Code.DEC_CHECKSUM),
        (lambda p: bytes([p[0] | 0x20]) + p[1:], Code.DEC_FIXED),  # version bits
    ],
)
def test_unpack_failures_are_typed(mutate: Any, code: Code) -> None:
    good = header.pack(CMD, b"\x01\x02", mid=0x1805, inputs={"function_code": 1})
    result = header.unpack(CMD, mutate(good))
    assert isinstance(result, DecodeError)
    assert result.code is code
    assert result.offset >= 0


def test_oversize_rejected_before_anything_else() -> None:
    result = header.unpack(CMD, bytes(5000), max_size=1500)
    assert isinstance(result, DecodeError) and result.code is Code.DEC_OVERSIZE


def test_tamper_checksum_and_length() -> None:
    good = header.pack(CMD, b"\x01", mid=0x1805, inputs={"function_code": 1})
    bad_sum = header.pack(
        CMD, b"\x01", mid=0x1805, inputs={"function_code": 1}, tamper={"checksum": 1}
    )
    bad_len = header.pack(
        CMD, b"\x01", mid=0x1805, inputs={"function_code": 1}, tamper={"length": 2}
    )
    assert good not in {bad_sum, bad_len}
    assert isinstance(header.unpack(CMD, bad_sum), DecodeError)
    result = header.unpack(CMD, bad_len)
    assert isinstance(result, DecodeError) and result.code is Code.DEC_LENGTH


def test_crc16_ccitt_profile_round_trip() -> None:
    data = _profile_dict()
    data["fields"][-1] = {
        "name": "checksum",
        "bits": 16,
        "computed": "checksum",
        "algorithm": "crc16_ccitt",
    }
    profile = header.parse_profile(data, "crc.toml")
    packet = header.pack(profile, b"123456789", mid=0x1805, inputs={"function_code": 3})
    assert isinstance(header.unpack(profile, packet), Header)
    flipped = packet[:-1] + bytes([packet[-1] ^ 0x80])
    result = header.unpack(profile, flipped)
    assert isinstance(result, DecodeError) and result.code is Code.DEC_CHECKSUM


def test_checksum_algorithms_known_values() -> None:
    assert checksum.REGISTRY["crc16_ccitt"].compute(b"123456789") == 0x29B1
    assert checksum.REGISTRY["xor8_cfs"].compute(b"\x00") == 0xFF


def _profile_dict() -> dict[str, Any]:
    return {
        "name": "p",
        "kind": "command",
        "function_code_field": "function_code",
        "mid_fields": ["mid"],
        "fields": [
            {"name": "mid", "bits": 16, "computed": "from_mid"},
            {"name": "length", "bits": 16, "computed": "length", "bias": 0},
            {"name": "function_code", "bits": 8},
            {"name": "checksum", "bits": 8, "computed": "checksum", "algorithm": "xor8_cfs"},
        ],
    }


def test_minimal_profile_parses() -> None:
    profile = header.parse_profile(_profile_dict(), "p.toml")
    packet = header.pack(profile, b"", mid=0x1234, inputs={"function_code": 7})
    assert _unpacked(profile, packet).mid == 0x1234


@pytest.mark.parametrize(
    ("edit", "needle"),
    [
        (lambda d: d.update(bogus=1), "bogus: unknown key"),
        (lambda d: d["fields"][0].update(bits=0), "fields[0].bits"),
        (lambda d: d["fields"][2].update(bits=7), "whole number of bytes"),
        (lambda d: d["fields"][1].update(value=1), "mutually exclusive"),
        (lambda d: d["fields"][3].update(algorithm="nope"), "unknown algorithm"),
        (lambda d: d["fields"][3].update(bits=16), "needs 8 bits"),
        (lambda d: d["fields"][1].update(computed="python:evil.func"), "escape hatch"),
        (lambda d: d["fields"][2].update(name="mid"), "duplicate field name"),
        (lambda d: d.update(mid_fields=["length"]), "from_mid"),
        (lambda d: d.update(kind="sideways"), "kind"),
    ],
)
def test_profile_schema_errors_name_file_and_path(edit: Any, needle: str) -> None:
    data = copy.deepcopy(_profile_dict())
    edit(data)
    with pytest.raises(IronbirdError) as info:
        header.parse_profile(data, "bad.toml")
    assert info.value.code is Code.HDR_SCHEMA
    assert info.value.summary.startswith("bad.toml: ")
    assert needle in info.value.summary


def test_load_profile_reports_syntax_and_missing_file(tmp_path: Any) -> None:
    broken = tmp_path / "broken.toml"
    broken.write_text("name = [unclosed")
    with pytest.raises(IronbirdError) as info:
        header.load_profile(broken)
    assert info.value.code is Code.HDR_FILE
    with pytest.raises(IronbirdError) as info2:
        header.load_profile(tmp_path / "missing.toml")
    assert info2.value.code is Code.HDR_FILE
