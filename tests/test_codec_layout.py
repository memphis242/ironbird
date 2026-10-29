"""Round-trip property over generated payload layouts (gaps, leading offset, mixed endian)."""

from __future__ import annotations

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import EXAMPLES
from ironbird import header
from ironbird.ccdd.model import (
    CcddModel,
    Endian,
    FieldDef,
    Message,
    MessageKind,
    TypeClass,
    parse_type,
)
from ironbird.codec import Codec, DecodedPacket

TLM_PROFILE = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml")
CMD_PROFILE = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
TYPE_TEXTS = [
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "int8",
    "int16",
    "int32",
    "int64",
    "float32",
    "float64",
    "char[3]",
    "bytes[5]",
]


def _codec(message: Message) -> Codec:
    model = CcddModel.build("T", "test", (message,))
    return Codec(model, CMD_PROFILE, TLM_PROFILE, clock=lambda: 1.0)


def _value(draw: st.DrawFn, field: FieldDef) -> object:
    ftype = field.type
    match ftype.type_class:
        case TypeClass.INT:
            low, high = ftype.bit_range
            return draw(st.integers(low, high))
        case TypeClass.FLOAT:
            return draw(st.floats(width=ftype.size * 8, allow_nan=False))  # type: ignore[arg-type]
        case TypeClass.CHAR:
            alphabet = st.characters(min_codepoint=33, max_codepoint=126)
            return draw(st.text(alphabet, max_size=ftype.size))
        case TypeClass.BYTES:
            return draw(st.binary(min_size=ftype.size, max_size=ftype.size))
    raise AssertionError("unreachable")


@st.composite
def _layouts(draw: st.DrawFn) -> tuple[Message, dict[str, object]]:
    count = draw(st.integers(1, 6))
    cursor = draw(st.integers(0, 6))  # leading offset
    fields: list[FieldDef] = []
    for i in range(count):
        parsed = parse_type(draw(st.sampled_from(TYPE_TEXTS)))
        assert not isinstance(parsed, str)
        endian = draw(st.sampled_from(list(Endian)))
        fields.append(FieldDef(f"f{i}", parsed, cursor, endian))
        cursor += parsed.size + draw(st.integers(0, 3))  # gap
    size = cursor + draw(st.integers(0, 2))
    message = Message("GEN", MessageKind.TELEMETRY, 0x0890, None, "", tuple(fields), size)
    values = {f.name: _value(draw, f) for f in fields}
    return message, values


@settings(max_examples=400, deadline=1000, suppress_health_check=[HealthCheck.too_slow])
@given(_layouts())
def test_generated_layout_round_trips(case: tuple[Message, dict[str, object]]) -> None:
    message, values = case
    codec = _codec(message)
    wire = codec.encode_telemetry("GEN", values).data
    decoded = codec.decode(MessageKind.TELEMETRY, wire)
    assert isinstance(decoded, DecodedPacket), decoded
    assert decoded.as_dict() == values


def test_first_field_not_at_payload_start_decodes_correctly() -> None:
    parsed = parse_type("uint16")
    assert not isinstance(parsed, str)
    fields = (FieldDef("x", parsed, 4), FieldDef("y", parsed, 6))
    message = Message("GEN", MessageKind.TELEMETRY, 0x0890, None, "", fields, 8)
    codec = _codec(message)
    wire = codec.encode_telemetry("GEN", {"x": 0x1234, "y": 0x5678}).data
    decoded = codec.decode(MessageKind.TELEMETRY, wire)
    assert isinstance(decoded, DecodedPacket)
    assert decoded.as_dict() == {"x": 0x1234, "y": 0x5678}
