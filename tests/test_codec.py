from __future__ import annotations

import pytest

from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec, DecodedPacket
from ironbird.errors import Code, DecodeError, IronbirdError

CMD, TLM = MessageKind.COMMAND, MessageKind.TELEMETRY


def test_encode_command_hand_built_fixture(mock_codec: Codec) -> None:
    encoded = mock_codec.encode_command("MOCK_SET_MODE", {"mode": "SAFE", "pad": "0"})
    # 0x1890 | c000 (flags+seq 0) | length 3 | fc 1 | checksum | mode=1 pad=0
    assert encoded.data == bytes.fromhex("1890c000000301b40100")
    assert (encoded.mid, encoded.function_code, encoded.sequence) == (0x1890, 1, 0)


def test_command_sequence_counts_per_mid(mock_codec: Codec) -> None:
    first = mock_codec.encode_command("MOCK_NOOP")
    second = mock_codec.encode_command("MOCK_NOOP")
    assert (first.sequence, second.sequence) == (0, 1)


def test_command_decode_round_trip(mock_codec: Codec) -> None:
    wire = mock_codec.encode_command("MOCK_SET_LABEL", {"label": "hello"}).data
    decoded = mock_codec.decode(CMD, wire)
    assert isinstance(decoded, DecodedPacket)
    assert decoded.message.name == "MOCK_SET_LABEL"
    assert decoded.as_dict() == {"label": "hello"}


def test_telemetry_hand_built_fixture_and_decode(mock_codec: Codec) -> None:
    encoded = mock_codec.encode_telemetry(
        "MOCK_HK", {"cmd_counter": 1, "mode": "NOMINAL", "label": "hi"}
    )
    expected = bytes.fromhex(
        "0890c0000015"  # mid 0x0890, flags+seq 0, length 28-7
        "000f4240"
        "4000"  # seconds 1_000_000, subseconds .25
        "00000000"  # spare
        "010002"
        "00"
        "6869000000000000"  # cmd=1 err=0 mode=2 pad label "hi"
    )
    assert encoded.data == expected
    decoded = mock_codec.decode(TLM, expected)
    assert isinstance(decoded, DecodedPacket)
    assert decoded.as_dict() == {
        "cmd_counter": 1,
        "err_counter": 0,
        "mode": 2,
        "pad": 0,
        "label": "hi",
    }
    assert decoded.message.field("mode").enum_label(2) == "NOMINAL"  # type: ignore[union-attr]


def test_mixed_endian_float_telemetry(mock_codec: Codec) -> None:
    encoded = mock_codec.encode_telemetry(
        "MOCK_STATUS", {"temperature_c": 21.5, "bus_voltage_v": "28.0", "uptime_s": 258}
    )
    assert encoded.data.endswith(bytes.fromhex("00000102"))  # big-endian uptime
    decoded = mock_codec.decode(TLM, encoded.data)
    assert isinstance(decoded, DecodedPacket)
    assert decoded.as_dict() == {"temperature_c": 21.5, "bus_voltage_v": 28.0, "uptime_s": 258}


@pytest.mark.parametrize(
    ("name", "args", "code"),
    [
        ("NOPE", {}, Code.ENC_UNKNOWN),
        ("MOCK_HK", {}, Code.ENC_UNKNOWN),  # not a command
        ("MOCK_NOOP", {"x": "1"}, Code.ENC_ARGSET),
        ("MOCK_SET_MODE", {"mode": "SAFE"}, Code.ENC_ARGSET),
        ("MOCK_SET_MODE", {"mode": "BOGUS", "pad": "0"}, Code.ENC_ARG),
        ("MOCK_SET_MODE", {"mode": "300", "pad": "0"}, Code.ENC_ARG),
        ("MOCK_SET_MODE", {"mode": "-1", "pad": "0"}, Code.ENC_ARG),
        ("MOCK_SET_LABEL", {"label": "waytoolongtext"}, Code.ENC_ARG),
        ("MOCK_SET_LABEL", {"label": "café"}, Code.ENC_ARG),
    ],
)
def test_encode_errors_are_coded(
    mock_codec: Codec, name: str, args: dict[str, str], code: Code
) -> None:
    with pytest.raises(IronbirdError) as info:
        mock_codec.encode_command(name, args)
    assert info.value.code is code
    assert "\n" not in info.value.user_line()


def test_decode_unknown_mid_and_function_code(mock_codec: Codec) -> None:
    other_mid = mock_codec.encode_command("MOCK_NOOP").data
    # same bytes, different MID: needs a valid checksum, so rebuild with the header module
    from ironbird import header

    wire = header.pack(mock_codec.profile(CMD), b"", mid=0x18AA, inputs={"function_code": 0})
    result = mock_codec.decode(CMD, wire)
    assert (
        isinstance(result, DecodeError) and result.code is Code.DEC_UNKNOWN and result.mid == 0x18AA
    )
    wire = header.pack(mock_codec.profile(CMD), b"", mid=0x1890, inputs={"function_code": 99})
    result = mock_codec.decode(CMD, wire)
    assert isinstance(result, DecodeError) and "function code 99" in result.reason
    assert other_mid  # first fixture still valid


def test_decode_payload_size_mismatch(mock_codec: Codec) -> None:
    from ironbird import header

    wire = header.pack(
        mock_codec.profile(CMD), b"\x01", mid=0x1890, inputs={"function_code": 1}
    )  # MOCK_SET_MODE needs 2 payload bytes
    result = mock_codec.decode(CMD, wire)
    assert isinstance(result, DecodeError) and result.code is Code.DEC_PAYLOAD


def test_decode_wrong_direction_is_an_error_not_a_crash(mock_codec: Codec) -> None:
    telemetry = mock_codec.encode_telemetry("MOCK_HK").data
    assert isinstance(mock_codec.decode(CMD, telemetry), DecodeError)
