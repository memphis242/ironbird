"""Properties of generated header profiles (Stage 2i): pack then unpack round-trips, the wire
equals an independent model of the profile, no input makes a decoder raise, a bit flipped under a
checksum is caught, ``frame_length`` answers on every prefix, and profiles that use nothing new
agree with the pre-2i implementation packet for packet."""

from __future__ import annotations

import random
from typing import Any

import pytest
from hypothesis import HealthCheck, find, given, settings
from hypothesis import strategies as st

import header_ref_pre2i as ref
from conftest import EXAMPLES
from headergen import (
    EPOCH_SECONDS,
    Spec,
    expect_wire,
    expected_mid,
    offset_of,
    span_of,
    specs,
)
from ironbird import header
from ironbird.ccdd.json_adapter import parse_model
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec, DecodedPacket
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.header import Header, HeaderProfile

FUZZ = settings(
    max_examples=300,
    deadline=2000,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)


def crc16_bitwise(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def checksum_model(algorithm: str, data: bytes) -> int:
    if algorithm == "xor8_cfs":
        out = 0xFF
        for byte in data:
            out ^= byte
        return out
    assert algorithm == "crc16_ccitt"
    return crc16_bitwise(data)


def build(spec: Spec) -> HeaderProfile:
    return header.parse_profile(spec.doc(), "gen.toml")


class Case:
    """One packet's worth of draws for a spec and the logical value of every field."""

    def __init__(self, spec: Spec, data: st.DataObject) -> None:
        self.spec = spec
        self.payload = data.draw(st.binary(max_size=40))
        self.sequence = data.draw(st.integers(0, 70000))
        self.now = data.draw(st.floats(0, 4e9, allow_nan=False))
        by = spec.by_name()
        self.parts: dict[str, int] = {}
        for name in spec.mid_order:
            f = by[name]
            self.parts[name] = (
                f.value if f.kind == "fixed" else data.draw(st.integers(0, (1 << f.bits) - 1))
            )
        self.mid = expected_mid(spec, self.parts)
        self.inputs: dict[str, int] = {}
        for f in spec.header:
            if f.kind == "input" and data.draw(st.booleans()):
                self.inputs[f.name] = data.draw(st.integers(0, (1 << f.bits) - 1))
        self.total = spec.header_bytes + len(self.payload) + spec.trailer_bytes

    def values(self) -> dict[str, int] | tuple[Code, str]:
        """Logical value of each field except the checksum, or the error pack must raise."""
        spec = self.spec
        out: dict[str, int] = {}
        for f in [*spec.header, *spec.trailer]:
            mask = (1 << f.bits) - 1
            match f.kind:
                case "fixed":
                    out[f.name] = f.value
                case "input":
                    out[f.name] = self.inputs.get(f.name, f.value)
                case "from_mid":
                    out[f.name] = self.parts[f.name]
                case "sequence":
                    out[f.name] = self.sequence & mask
                case "length":
                    value = self.total + f.bias
                    if not 0 <= value <= mask:
                        return Code.ENC_FIT, "length"
                    out[f.name] = value
                case "time_seconds":
                    counter = int(self.now) + spec.epoch_offset()
                    if counter < 0:
                        return Code.HDR_PACK, "epoch"
                    out[f.name] = counter & mask
                case "time_sub":
                    out[f.name] = int((self.now - int(self.now)) * (1 << f.bits)) & mask
                case "checksum":
                    out[f.name] = 0
                case _:
                    raise AssertionError(f.kind)
        return out


def pack_case(profile: HeaderProfile, case: Case) -> bytes:
    return header.pack(
        profile,
        case.payload,
        mid=case.mid,
        inputs=case.inputs,
        sequence=case.sequence,
        now=case.now,
    )


def fill_checksum(spec: Spec, values: dict[str, int], packet: bytes) -> None:
    cf = next((f for f in [*spec.header, *spec.trailer] if f.kind == "checksum"), None)
    if cf is not None:
        at = offset_of(spec, cf, len(packet))
        values[cf.name] = int.from_bytes(
            packet[at : at + cf.bits // 8], "little" if cf.little else "big"
        )


@FUZZ
@given(st.data())
def test_generated_profile_packs_to_the_modelled_wire_and_unpacks_to_its_values(
    data: st.DataObject,
) -> None:
    spec = data.draw(specs(data.draw(st.sampled_from(["telemetry", "command"]))))
    profile = build(spec)
    case = Case(spec, data)
    values = case.values()
    if isinstance(values, tuple):
        with pytest.raises(IronbirdError) as info:
            pack_case(profile, case)
        assert info.value.code is values[0]
        return
    packet = pack_case(profile, case)
    assert packet == expect_wire(spec, values, case.payload, checksum_model)
    fill_checksum(spec, values, packet)

    parsed = header.unpack(profile, packet)
    assert isinstance(parsed, Header), parsed
    assert dict(parsed.values) == values
    assert parsed.mid == case.mid == header.peek_mid(profile, packet)
    assert parsed.size == spec.header_bytes
    assert header.check(profile, packet) is None
    sequence = next((f for f in spec.header if f.kind == "sequence"), None)
    assert parsed.sequence == (values[sequence.name] if sequence else None)

    seconds = next((f for f in spec.header if f.kind == "time_seconds"), None)
    sub = next((f for f in spec.header if f.kind == "time_sub"), None)
    if seconds is None:
        assert parsed.time_ns is None
    else:
        fraction = (values[sub.name] * 10**9) >> sub.bits if sub is not None else 0
        counter_base = values[seconds.name]
        assert parsed.time_ns == (counter_base - spec.epoch_offset()) * 10**9 + fraction

    # The MID split is the inverse of the MID join.
    split = header.split_mid(profile, case.mid)
    assert isinstance(split, dict)
    assert all(values[name] == part for name, part in split.items())


@FUZZ
@given(st.data())
def test_frame_length_answers_on_every_prefix(data: st.DataObject) -> None:
    spec = data.draw(specs("telemetry"))
    profile = build(spec)
    if profile.length_field is None:
        assert profile.length_window is None
        return
    case = Case(spec, data)
    if isinstance(case.values(), tuple):
        return
    packet = pack_case(profile, case)
    window = profile.length_window
    assert window is not None and window <= spec.header_bytes
    for cut in range(len(packet) + 1):
        got = profile.frame_length(packet[:cut])
        assert got == (None if cut < window else len(packet)), (cut, window)
    # Bytes past the packet (the next one in a stream) change nothing.
    assert profile.frame_length(packet + b"\xff" * 9) == len(packet)


@FUZZ
@given(st.data())
def test_a_bit_flipped_under_the_checksum_is_caught(data: st.DataObject) -> None:
    kind = data.draw(st.sampled_from(["telemetry", "command"]))
    spec = data.draw(specs(kind, with_checksum=True))
    profile = build(spec)
    case = Case(spec, data)
    if isinstance(case.values(), tuple):
        return
    packet = pack_case(profile, case)
    cf = profile.checksum_field
    assert cf is not None
    start, end = span_of(spec, spec.by_name()[cf.name], len(packet))
    at = offset_of(spec, spec.by_name()[cf.name], len(packet))
    covered = [*range(start, end), *range(at, at + cf.bits // 8)]
    for _ in range(8):
        bit = data.draw(st.sampled_from(covered)) * 8 + data.draw(st.integers(0, 7))
        flipped = bytearray(packet)
        flipped[bit // 8] ^= 0x80 >> (bit % 8)
        result = header.check(profile, bytes(flipped))
        assert isinstance(result, DecodeError), (bit, spec.doc())


@FUZZ
@given(st.data())
def test_no_bytes_make_a_generated_profile_raise(data: st.DataObject) -> None:
    spec = data.draw(specs(data.draw(st.sampled_from(["telemetry", "command"]))))
    profile = build(spec)
    blob = data.draw(st.binary(max_size=200))
    for candidate in (blob, bytes(spec.header_bytes + spec.trailer_bytes + 3)):
        result = header.unpack(profile, candidate)
        assert isinstance(result, Header | DecodeError)
        assert (header.check(profile, candidate) is None) == isinstance(result, Header)
        peeked = header.peek_mid(profile, candidate)
        assert peeked is None or 0 <= peeked < 1 << profile.mid_bits
        framed = profile.frame_length(candidate) if profile.length_window else None
        assert framed is None or isinstance(framed, int)
        if isinstance(result, Header):
            assert result.mid == peeked


@FUZZ
@given(st.data())
def test_a_valid_packet_with_any_byte_changed_never_raises(data: st.DataObject) -> None:
    spec = data.draw(specs(data.draw(st.sampled_from(["telemetry", "command"]))))
    profile = build(spec)
    case = Case(spec, data)
    if isinstance(case.values(), tuple):
        return
    packet = bytearray(pack_case(profile, case))
    for _ in range(data.draw(st.integers(1, 4))):
        packet[data.draw(st.integers(0, len(packet) - 1))] = data.draw(st.integers(0, 255))
    cut = data.draw(st.integers(0, len(packet)))
    for candidate in (bytes(packet), bytes(packet[:cut]), bytes(packet) + b"\x00"):
        assert isinstance(header.unpack(profile, candidate), Header | DecodeError)


# -- profiles that use nothing new agree with the pre-2i implementation -----------------


@FUZZ
@given(st.data())
def test_a_pre_2i_profile_packs_and_checks_exactly_as_before(data: st.DataObject) -> None:
    spec = data.draw(specs(data.draw(st.sampled_from(["telemetry", "command"])), legacy=True))
    profile = build(spec)
    case = Case(spec, data)
    if isinstance(case.values(), tuple):
        return
    packet = pack_case(profile, case)
    old = ref.pack(
        profile,
        case.payload,
        mid=case.mid,
        inputs=case.inputs,
        sequence=case.sequence,
        now=case.now,
    )
    assert packet == old
    for candidate in _variants(data, packet):
        got = header.check_word(profile, candidate, max_size=60)
        want = ref.check_word(profile, candidate, 60)
        assert got == want, (candidate.hex(), spec.doc())
        assert header.peek_mid(profile, candidate) == ref.peek_mid(profile, candidate)
        if isinstance(got, int):
            unpacked = header.unpack(profile, candidate, max_size=60)
            assert isinstance(unpacked, Header)
            assert dict(unpacked.values) == ref.values_of(profile, got)


def _variants(data: st.DataObject, packet: bytes) -> list[bytes]:
    rng = random.Random(data.draw(st.integers(0, 2**32)))
    out = [packet, packet + b"\x00", packet[: len(packet) // 2], b"", bytes(70)]
    for _ in range(8):
        buf = bytearray(packet)
        for _ in range(rng.randrange(1, 4)):
            buf[rng.randrange(len(buf))] ^= 1 << rng.randrange(8)
        out.append(bytes(buf))
    return out


# -- the same, through the codec -----------------------------------------------------------


def _codec_for(
    spec: Spec, header_values: dict[str, int], function_code: int = 0
) -> tuple[Codec, str]:
    """A codec whose profile of ``spec.kind`` is ``spec`` and whose message is called ``GEN``."""
    by = spec.by_name()
    parts = {n: (by[n].value if by[n].kind == "fixed" else 1) for n in spec.mid_order}
    mid = expected_mid(spec, {n: min(p, (1 << by[n].bits) - 1) for n, p in parts.items()})
    message: dict[str, Any] = {
        "name": "GEN",
        "kind": spec.kind,
        "mid": mid,
        "header": header_values,
        "fields": [
            {"name": "a", "type": "uint16"},
            {"name": "b", "type": "int32", "endian": "big"},
            {"name": "c", "type": "char[4]"},
        ],
    }
    if spec.kind == "command":
        message["function_code"] = function_code
    else:
        message["category"] = "status"
    model = parse_model(
        {"schema": "ironbird-ccdd/0", "target": "gen", "messages": [message]}, "gen.json"
    )
    shipped_cmd = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
    shipped_tlm = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml")
    generated = build(spec)
    command, telemetry = (
        (generated, shipped_tlm) if spec.kind == "command" else (shipped_cmd, generated)
    )
    return Codec(model, command, telemetry, clock=lambda: 2_000_000_000.5), "GEN"


@FUZZ
@given(st.data())
def test_codec_round_trips_through_a_generated_profile(data: st.DataObject) -> None:
    kind = data.draw(st.sampled_from([MessageKind.TELEMETRY, MessageKind.COMMAND]))
    spec = data.draw(specs(kind.value))
    by = spec.by_name()
    settable = [f for f in spec.header if f.kind == "input" and f.name != spec.fc_field]
    chosen = {
        f.name: data.draw(st.integers(0, (1 << f.bits) - 1))
        for f in settable
        if data.draw(st.booleans())
    }
    function_code = 0
    if spec.fc_field is not None:
        function_code = data.draw(st.integers(0, min(255, (1 << by[spec.fc_field].bits) - 1)))
    try:
        codec, name = _codec_for(spec, chosen, function_code)
    except IronbirdError as err:
        # A profile whose length field cannot hold the packet, or a checksum span that does not
        # fit the packet, is refused up front with a code, never half-built.
        assert err.code is Code.ENC_FIT
        return
    profile = codec.profile(kind)
    values = {"a": data.draw(st.integers(0, 65535)), "b": -5, "c": "xyz"}
    sent = {
        n: data.draw(st.integers(0, (1 << by[n].bits) - 1))
        for n in chosen
        if data.draw(st.booleans())
    }
    try:
        if kind is MessageKind.COMMAND:
            encoded = codec.encode_command(name, values, header=sent)
        else:
            encoded = codec.encode_telemetry(name, values, header=sent)
    except IronbirdError as err:
        assert err.code in (Code.ENC_FIT, Code.HDR_PACK)
        return
    decoded = codec.decode(kind, encoded.data)
    assert isinstance(decoded, DecodedPacket), decoded
    assert decoded.as_dict() == values
    assert codec.check(kind, encoded.data) is None
    # The header values reached the wire: the send's over the message's over the default.
    for fname, fvalue in {**chosen, **sent}.items():
        assert decoded.header.values[fname] == fvalue
    if spec.fc_field is not None:
        assert decoded.header.values[spec.fc_field] == function_code
        assert decoded.message.function_code == function_code
    assert decoded.header.mid == encoded.mid == codec.peek_mid(kind, encoded.data)
    assert decoded.size == profile.min_bytes + 10
    # Every truncation and extension is a typed error, not an exception.
    for cut in range(len(encoded.data)):
        assert isinstance(codec.decode(kind, encoded.data[:cut]), DecodeError)
    assert isinstance(codec.decode(kind, encoded.data + b"\0"), DecodeError)


@FUZZ
@given(st.data(), st.binary(max_size=300))
def test_codec_decode_never_raises_on_a_generated_profile(data: st.DataObject, blob: bytes) -> None:
    kind = data.draw(st.sampled_from([MessageKind.TELEMETRY, MessageKind.COMMAND]))
    spec = data.draw(specs(kind.value))
    try:
        codec, _ = _codec_for(spec, {})
    except IronbirdError:
        return
    result = codec.decode(kind, blob)
    assert isinstance(result, DecodedPacket | DecodeError)
    assert (codec.check(kind, blob) is None) == isinstance(result, DecodedPacket)


def test_the_epoch_table_of_the_generator_matches_the_calendar() -> None:
    from datetime import UTC, datetime

    for text, seconds in EPOCH_SECONDS.items():
        assert int(datetime.fromisoformat(text).astimezone(UTC).timestamp()) == seconds


def _non_contiguous(spec: Spec) -> bool:
    order = [f.name for f in spec.header]
    idx = [order.index(n) for n in spec.mid_order]
    return idx != list(range(idx[0], idx[0] + len(idx)))


@pytest.mark.parametrize(
    ("what", "predicate"),
    [
        (
            "a little-endian multi-byte field",
            lambda s: any(f.little and f.bits > 8 for f in s.header),
        ),
        (
            "a little-endian length field",
            lambda s: any(f.kind == "length" and f.little for f in s.header),
        ),
        ("a trailer", lambda s: bool(s.trailer)),
        ("a trailer checksum", lambda s: any(f.kind == "checksum" for f in s.trailer)),
        (
            "a little-endian checksum",
            lambda s: any(f.kind == "checksum" and f.little for f in [*s.header, *s.trailer]),
        ),
        ("a range span", lambda s: any(isinstance(f.span, tuple) for f in [*s.header, *s.trailer])),
        ("a payload span", lambda s: any(f.span == "payload" for f in [*s.header, *s.trailer])),
        ("a header span", lambda s: any(f.span == "header" for f in [*s.header, *s.trailer])),
        ("a MID out of header order", lambda s: len(s.mid_order) > 1 and _non_contiguous(s)),
        (
            "a MID of several fields in reverse order",
            lambda s: (
                s.mid_order[::-1] == [f.name for f in s.header if f.name in s.mid_order]
                and len(s.mid_order) > 1
            ),
        ),
        ("a time epoch", lambda s: s.epoch is not None and s.epoch != "1970-01-01T00:00:00Z"),
        ("leap seconds", lambda s: s.leap != 0),
        ("a length field", lambda s: any(f.kind == "length" for f in s.header)),
        ("a command profile with a function code", lambda s: s.fc_field is not None),
    ],
)
def test_the_generator_reaches_each_feature(what: str, predicate: Any) -> None:
    kind = "command" if "command" in what else "telemetry"
    find(specs(kind), predicate, settings=settings(max_examples=2000, database=None))
