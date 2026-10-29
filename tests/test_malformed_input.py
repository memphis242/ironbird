"""Malformed-input tests: corpus replay plus property-based checks on every parser.

Invariants: never an uncaught exception, never a hang (Hypothesis deadline), and valid input
round-trips. Add a corpus file under tests/corpus/malformed/ for every parser bug.
"""

from __future__ import annotations

import copy
import json
import tempfile
import tomllib
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import CORPUS, EXAMPLES
from ironbird import header
from ironbird.ccdd.json_adapter import JsonAdapter, parse_model
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec, DecodedPacket
from ironbird.config import load_config, parse_config
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.events import extract_event

CMD, TLM = MessageKind.COMMAND, MessageKind.TELEMETRY
CODEC = Codec(
    load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
    header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
    header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
    clock=lambda: 1_000_000.25,
)
CMD_PROFILE = CODEC.profile(CMD)
TLM_PROFILE = CODEC.profile(TLM)
MOCK_JSON: dict[str, Any] = json.loads((EXAMPLES / "ccdd" / "mock.json").read_text())
MOCK_TOML: dict[str, Any] = tomllib.loads((EXAMPLES / "mock.toml").read_text())
CMD_TOML: dict[str, Any] = tomllib.loads(
    (EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml").read_text()
)

FUZZ = settings(
    max_examples=300,
    deadline=1000,  # a parser that takes a second on a few KB is a hang for our purposes
    suppress_health_check=[HealthCheck.too_slow],
)

VALID_CMDS = [
    CODEC.encode_command("MOCK_NOOP").data,
    CODEC.encode_command("MOCK_SET_MODE", {"mode": "SAFE", "pad": "0"}).data,
    CODEC.encode_command("MOCK_SET_LABEL", {"label": "abc"}).data,
]
VALID_TLMS = [
    CODEC.encode_telemetry("MOCK_HK", {"cmd_counter": 5, "label": "x"}).data,
    CODEC.encode_telemetry("MOCK_STATUS", {"temperature_c": 1.5}).data,
]


def _well_formed(result: object) -> bool:
    return isinstance(result, DecodedPacket | DecodeError)


# -- corpus ---------------------------------------------------------------------------


def _corpus(pattern: str) -> list[Path]:
    files = sorted(CORPUS.glob(pattern))
    assert files, f"corpus has no {pattern}"
    return files


@pytest.mark.parametrize("path", _corpus("packet-cmd-*.bin"), ids=lambda p: p.stem)
def test_corpus_command_packets_are_rejected_with_typed_errors(path: Path) -> None:
    data = path.read_bytes()
    result = CODEC.decode(CMD, data)
    assert isinstance(result, DecodeError), result
    assert _well_formed(CODEC.decode(TLM, data))  # wrong direction must not crash either


@pytest.mark.parametrize("path", _corpus("packet-tlm-*.bin"), ids=lambda p: p.stem)
def test_corpus_telemetry_packets_are_rejected_with_typed_errors(path: Path) -> None:
    data = path.read_bytes()
    assert isinstance(CODEC.decode(TLM, data), DecodeError)
    assert _well_formed(CODEC.decode(CMD, data))


@pytest.mark.parametrize("path", _corpus("ccdd-*.json"), ids=lambda p: p.stem)
def test_corpus_ccdd_files_are_rejected_with_coded_errors(path: Path) -> None:
    with pytest.raises(IronbirdError) as info:
        load_ccdd(path)
    assert info.value.code.value.startswith("IB-CCD-")
    assert "\n" not in info.value.user_line()


@pytest.mark.parametrize("path", _corpus("hdr-*.toml"), ids=lambda p: p.stem)
def test_corpus_header_profiles_are_rejected_with_coded_errors(path: Path) -> None:
    with pytest.raises(IronbirdError) as info:
        header.load_profile(path)
    assert info.value.code.value.startswith("IB-HDR-")


@pytest.mark.parametrize("path", _corpus("cfg-*.toml"), ids=lambda p: p.stem)
def test_corpus_configs_are_rejected_with_coded_errors(path: Path) -> None:
    with pytest.raises(IronbirdError) as info:
        load_config(path)
    assert info.value.code.value.startswith("IB-CFG-")


# -- packets: header.unpack / codec.decode ---------------------------------------------


@FUZZ
@given(st.binary(max_size=3000), st.sampled_from([CMD, TLM]))
def test_decode_arbitrary_bytes_never_raises(data: bytes, kind: MessageKind) -> None:
    assert _well_formed(CODEC.decode(kind, data))
    parsed = header.unpack(CODEC.profile(kind), data)
    assert isinstance(parsed, header.Header | DecodeError)
    if isinstance(parsed, header.Header):
        assert parsed.size <= len(data) <= CODEC.max_packet_bytes


@FUZZ
@given(st.binary(min_size=0, max_size=64), st.sampled_from(VALID_CMDS + VALID_TLMS))
def test_decode_garbage_after_valid_prefix(tail: bytes, valid: bytes) -> None:
    for kind in (CMD, TLM):
        assert _well_formed(CODEC.decode(kind, valid + tail))
        assert _well_formed(CODEC.decode(kind, valid[: len(valid) // 2] + tail))


@pytest.mark.parametrize(("kind", "packets"), [(CMD, VALID_CMDS), (TLM, VALID_TLMS)])
def test_truncation_at_every_offset(kind: MessageKind, packets: list[bytes]) -> None:
    for packet in packets:
        assert isinstance(CODEC.decode(kind, packet), DecodedPacket)
        for cut in range(len(packet)):
            result = CODEC.decode(kind, packet[:cut])
            assert isinstance(result, DecodeError), f"prefix {cut} of {len(packet)} accepted"


def test_every_single_bit_flip_in_a_command_is_rejected() -> None:
    # The command profile carries a checksum over the whole packet.
    for packet in VALID_CMDS:
        for bit in range(len(packet) * 8):
            flipped = bytearray(packet)
            flipped[bit // 8] ^= 1 << (bit % 8)
            assert isinstance(CODEC.decode(CMD, bytes(flipped)), DecodeError), bit


@FUZZ
@given(
    st.integers(0, 0xFFFF),
    st.sampled_from([(CMD, p) for p in VALID_CMDS] + [(TLM, p) for p in VALID_TLMS]),
)
def test_length_field_that_lies_in_either_direction(
    claimed: int, kind_and_packet: tuple[MessageKind, bytes]
) -> None:
    kind, packet = kind_and_packet
    profile = CODEC.profile(kind)
    lf = profile.length_field
    assert lf is not None
    offset = profile.byte_offset(lf)
    forged = packet[:offset] + claimed.to_bytes(2, "big") + packet[offset + 2 :]
    result = CODEC.decode(kind, forged)
    assert _well_formed(result)
    if claimed - lf.bias != len(packet):
        assert isinstance(result, DecodeError) and result.code is Code.DEC_LENGTH


@FUZZ
@given(
    mode=st.integers(0, 255),
    pad=st.integers(0, 255),
    label=st.text(alphabet=st.characters(min_codepoint=33, max_codepoint=126), max_size=8),
)
def test_command_round_trip(mode: int, pad: int, label: str) -> None:
    wire = CODEC.encode_command("MOCK_SET_MODE", {"mode": mode, "pad": pad}).data
    decoded = CODEC.decode(CMD, wire)
    assert isinstance(decoded, DecodedPacket) and decoded.as_dict() == {"mode": mode, "pad": pad}
    wire = CODEC.encode_command("MOCK_SET_LABEL", {"label": label}).data
    decoded = CODEC.decode(CMD, wire)
    assert isinstance(decoded, DecodedPacket) and decoded.as_dict() == {"label": label}


@FUZZ
@given(
    temp=st.floats(width=32, allow_nan=False),
    volts=st.floats(width=32, allow_nan=False),
    uptime=st.integers(0, 2**32 - 1),
)
def test_telemetry_round_trip(temp: float, volts: float, uptime: int) -> None:
    values = {"temperature_c": temp, "bus_voltage_v": volts, "uptime_s": uptime}
    decoded = CODEC.decode(TLM, CODEC.encode_telemetry("MOCK_STATUS", values).data)
    assert isinstance(decoded, DecodedPacket) and decoded.as_dict() == values


# -- structured files: CCDD / header profile / config -----------------------------------

JSON_VALUES = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats() | st.text(max_size=20),
    lambda inner: (
        st.lists(inner, max_size=4) | st.dictionaries(st.text(max_size=12), inner, max_size=4)
    ),
    max_leaves=25,
)


def _scalar_paths(doc: Any, prefix: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    paths: list[tuple[Any, ...]] = []
    if isinstance(doc, dict):
        for key, value in doc.items():
            paths.append((*prefix, key))
            paths += _scalar_paths(value, (*prefix, key))
    elif isinstance(doc, list):
        for index, value in enumerate(doc):
            paths.append((*prefix, index))
            paths += _scalar_paths(value, (*prefix, index))
    return paths


def _replace(doc: Any, path: tuple[Any, ...], value: Any) -> Any:
    clone = copy.deepcopy(doc)
    target = clone
    for step in path[:-1]:
        target = target[step]
    target[path[-1]] = value
    return clone


def _mutants(base: Any) -> st.SearchStrategy[Any]:
    paths = _scalar_paths(base)
    return st.builds(lambda p, v: _replace(base, p, v), st.sampled_from(paths), JSON_VALUES)


@FUZZ
@given(JSON_VALUES)
def test_ccdd_parser_on_arbitrary_json(doc: Any) -> None:
    try:
        parse_model(doc, "fuzz.json")
    except IronbirdError as err:
        assert err.code.value.startswith("IB-CCD-")


@FUZZ
@given(_mutants(MOCK_JSON))
def test_ccdd_parser_on_mutated_valid_files(doc: Any) -> None:
    try:
        model = parse_model(doc, "fuzz.json")
    except IronbirdError as err:
        assert err.code.value.startswith("IB-CCD-")
        return
    # Whatever survives validation must also be usable by the codec layer.
    try:
        Codec(model, CMD_PROFILE, TLM_PROFILE)
    except IronbirdError as err:
        assert err.code is Code.ENC_FIT


EVENT_SCHEMA_PATHS = [
    path
    for path in _scalar_paths(MOCK_JSON)
    if "category" in path or "event" in path or path[:2] == ("messages", 6)
]


@FUZZ
@given(st.sampled_from(EVENT_SCHEMA_PATHS), JSON_VALUES)
def test_ccdd_category_and_event_mapping_mutations(path: tuple[Any, ...], value: Any) -> None:
    try:
        model = parse_model(_replace(MOCK_JSON, path, value), "fuzz.json")
    except IronbirdError as err:
        assert err.code.value.startswith("IB-CCD-")
        return
    # Whatever survives validation must extract events from any packet the codec decodes.
    try:
        codec = Codec(model, CMD_PROFILE, TLM_PROFILE, clock=lambda: 1_000_000.25)
    except IronbirdError as err:
        assert err.code is Code.ENC_FIT
        return
    for msg in model.event_messages():
        decoded = codec.decode(TLM, codec.encode_telemetry(msg.name).data)
        assert isinstance(decoded, DecodedPacket)
        assert extract_event("fc", 1, decoded) is not None


@FUZZ
@given(st.binary(max_size=2000))
def test_ccdd_loader_on_arbitrary_file_bytes(data: bytes) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "fuzz.json"
        path.write_bytes(data)
        try:
            JsonAdapter().load(path)
        except IronbirdError as err:
            assert err.code.value.startswith("IB-CCD-")


@FUZZ
@given(_mutants(CMD_TOML))
def test_header_profile_parser_on_mutated_valid_profiles(doc: Any) -> None:
    try:
        profile = header.parse_profile(doc, "fuzz.toml")
    except IronbirdError as err:
        assert err.code is Code.HDR_SCHEMA
        return
    # A profile that validates must pack and unpack its own output without raising.
    try:
        packet = header.pack(
            profile,
            b"\x00\x01",
            mid=0x1890 if profile.mid_fields else None,
            inputs={f.name: f.value or 0 for f in profile.input_fields},
        )
    except IronbirdError:
        return
    assert isinstance(header.unpack(profile, packet), header.Header | DecodeError)


@FUZZ
@given(_mutants(MOCK_TOML))
def test_config_parser_on_mutated_valid_configs(doc: Any) -> None:
    try:
        parse_config(doc, Path("fuzz.toml"))
    except IronbirdError as err:
        assert err.code.value.startswith("IB-CFG-")


@pytest.mark.parametrize("path", _corpus("codec-hdr-*.toml"), ids=lambda p: p.stem)
def test_corpus_profiles_the_codec_cannot_drive_are_rejected_at_load(path: Path) -> None:
    from ironbird.ccdd.model import CcddModel

    profile = header.load_profile(path)  # parses on its own
    empty = CcddModel.build("T", "t", ())
    with pytest.raises(IronbirdError) as info:
        Codec(empty, CMD_PROFILE, profile)
    assert info.value.code is Code.HDR_SCHEMA
    assert "\n" not in info.value.user_line()
