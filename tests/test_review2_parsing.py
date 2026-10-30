"""Regression tests for the second antagonistic review of config/CCDD/header/codec parsing."""

from __future__ import annotations

import json
import math
import tomllib
from pathlib import Path
from typing import Any

import pytest

from conftest import CORPUS, EXAMPLES
from ironbird import header, sanitize
from ironbird.ccdd.json_adapter import JsonAdapter, parse_model
from ironbird.ccdd.model import FieldDef, MessageKind, parse_type
from ironbird.codec import Codec, coerce_value
from ironbird.config import load_config, parse_config
from ironbird.errors import Code, IronbirdError

CMD, TLM = MessageKind.COMMAND, MessageKind.TELEMETRY
MOCK_JSON: dict[str, Any] = json.loads((EXAMPLES / "ccdd" / "mock.json").read_text())
MOCK_TOML: dict[str, Any] = tomllib.loads((EXAMPLES / "mock.toml").read_text())


def _model_with(**message: Any) -> dict[str, Any]:
    base: dict[str, Any] = {"name": "A", "kind": "telemetry", "mid": "0x0890", "fields": []}
    base.update(message)
    return {"schema": "ironbird-ccdd/0", "target": "t", "messages": [base]}


def _load_error(path: Path, text: str) -> IronbirdError:
    path.write_text(text)
    with pytest.raises(IronbirdError) as info:
        JsonAdapter().load(path)
    return info.value


# -- 1. fullmatch: a trailing newline is not part of a name -------------------------------


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d["messages"][0].update(name="A\n"),
        lambda d: d["messages"][0].update(mid="0x0890\n"),
        lambda d: d.update(target="t\n"),
        lambda d: d["messages"][0].update(fields=[{"name": "f\n", "type": "uint8"}]),
        lambda d: d["messages"][0].update(fields=[{"name": "f", "type": "char[4]\n"}]),
        lambda d: d["messages"][0].update(
            fields=[{"name": "f", "type": "uint8", "enum": {"OK\n": 0}}]
        ),
    ],
)
def test_ccdd_names_and_ids_reject_a_trailing_newline(mutate: Any) -> None:
    doc = _model_with()
    mutate(doc)
    with pytest.raises(IronbirdError) as info:
        parse_model(doc, "t.json")
    assert info.value.code.value.startswith("IB-CCD-")


def test_config_names_hosts_and_link_names_reject_a_trailing_newline() -> None:
    def cfg() -> dict[str, Any]:
        return json.loads(json.dumps(MOCK_TOML))

    def rejected(doc: dict[str, Any]) -> None:
        with pytest.raises(IronbirdError) as info:
            parse_config(doc, Path("c.toml"))
        assert info.value.code.value.startswith("IB-CFG-")

    doc = cfg()
    doc["nodes"]["fc"]["alias"] = "flight\n"
    rejected(doc)
    doc = cfg()
    doc["links"][0]["command_endpoint"] = "localhost\n:41234"
    rejected(doc)
    doc = cfg()
    doc["links"][0]["name"] = "ground-fc\n"
    rejected(doc)
    doc = cfg()
    doc["links"][0]["name"] = "has space"
    rejected(doc)
    doc = cfg()
    doc["nodes"]["fc"]["responses"][0]["reply"] = "MOCK_HK\n"
    rejected(doc)


def test_config_response_field_and_echo_names_must_be_identifiers() -> None:
    for table, key in (("fields", "bad key"), ("echo", "bad key")):
        doc = json.loads(json.dumps(MOCK_TOML))
        doc["nodes"]["fc"]["responses"][0][table] = {key: "x" if table == "echo" else 1}
        with pytest.raises(IronbirdError) as info:
            parse_config(doc, Path("c.toml"))
        assert info.value.code is Code.CFG_SCHEMA
    doc = json.loads(json.dumps(MOCK_TOML))
    doc["nodes"]["fc"]["responses"][0]["echo"] = {"mode": "not an arg\n"}
    with pytest.raises(IronbirdError):
        parse_config(doc, Path("c.toml"))


def test_valid_example_config_still_loads() -> None:
    assert parse_config(MOCK_TOML, EXAMPLES / "mock.toml").links


# -- 2. TOML loaders: huge ints and deep nesting are coded errors ---------------------------


def test_config_huge_int_port_is_a_coded_schema_error() -> None:
    with pytest.raises(IronbirdError) as info:
        load_config(CORPUS / "cfg-huge-int-port.toml")
    assert info.value.code is Code.CFG_SCHEMA and info.value.exit_code == 2


@pytest.mark.parametrize("name", ["cfg-huge-int-value.toml", "cfg-deep-nesting.toml"])
def test_config_toml_resource_limits_are_syntax_errors(name: str) -> None:
    with pytest.raises(IronbirdError) as info:
        load_config(CORPUS / name)
    assert info.value.code is Code.CFG_SYNTAX and info.value.exit_code == 2
    assert name in info.value.user_line()


@pytest.mark.parametrize("name", ["hdr-huge-int.toml", "hdr-deep-nesting.toml"])
def test_profile_toml_resource_limits_are_file_errors(name: str) -> None:
    with pytest.raises(IronbirdError) as info:
        header.load_profile(CORPUS / name)
    assert info.value.code is Code.HDR_FILE and info.value.exit_code == 2
    assert name in info.value.user_line()


# -- 4. duplicate JSON keys -----------------------------------------------------------------


def test_duplicate_json_keys_are_rejected_not_last_wins(tmp_path: Path) -> None:
    err = _load_error(tmp_path / "d.json", (CORPUS / "ccdd-duplicate-keys.json").read_text())
    assert err.code is Code.CCD_FILE
    assert "duplicate key" in err.summary and "'mid'" in err.summary
    assert str(tmp_path / "d.json") in err.summary


def test_duplicate_keys_in_a_nested_object_are_rejected(tmp_path: Path) -> None:
    err = _load_error(tmp_path / "d.json", '{"a": {"b": 1, "b": 2}}')
    assert err.code is Code.CCD_FILE


def test_duplicate_toml_keys_are_already_rejected(tmp_path: Path) -> None:
    path = tmp_path / "c.toml"
    path.write_text('[nodes.a]\nalias = "x"\nalias = "y"\n')
    with pytest.raises(IronbirdError) as info:
        load_config(path)
    assert info.value.code is Code.CFG_SYNTAX
    path.write_text('name = "a"\nname = "b"\n')
    with pytest.raises(IronbirdError) as info:
        header.load_profile(path)
    assert info.value.code is Code.HDR_FILE


# -- 5. enums: reverse map and cap ---------------------------------------------------------


def test_enum_label_is_a_dict_lookup_and_agrees_with_the_forward_map() -> None:
    doc = _model_with(
        fields=[{"name": "f", "type": "uint16", "enum": {"A": 1, "B": 2, "C": 40000}}]
    )
    fdef = parse_model(doc, "t.json").messages[0].fields[0]
    assert [fdef.enum_label(v) for v in (1, 2, 40000, 3, True)] == ["A", "B", "C", None, None]
    assert fdef.enum_label("A") is None


def test_enum_label_on_a_field_built_by_hand_has_a_reverse_map() -> None:
    ftype = parse_type("uint8")
    assert not isinstance(ftype, str)
    fdef = FieldDef("f", ftype, 0, enum={"X": 7})
    assert fdef.enum_label(7) == "X" and fdef.enum_label(8) is None


def test_enum_with_too_many_entries_is_a_coded_error() -> None:
    from ironbird.ccdd.json_adapter import MAX_ENUM_ENTRIES

    ok = {f"E{i}": i for i in range(MAX_ENUM_ENTRIES)}
    doc = _model_with(fields=[{"name": "f", "type": "uint32", "enum": ok}])
    assert parse_model(doc, "t.json")
    doc["messages"][0]["fields"][0]["enum"]["ONE_TOO_MANY"] = MAX_ENUM_ENTRIES
    with pytest.raises(IronbirdError) as info:
        parse_model(doc, "t.json")
    assert info.value.code is Code.CCD_SCHEMA and "enum" in info.value.summary


def test_enum_error_hint_stays_short(mock_codec: Codec) -> None:
    msg = mock_codec.model.by_name["MOCK_SET_MODE"]
    fdef = msg.fields[0]
    assert fdef.enum is not None
    with pytest.raises(IronbirdError) as info:
        coerce_value(msg, fdef, "NOT_A_NAME")
    assert info.value.code is Code.ENC_ARG


# -- 6. floats -------------------------------------------------------------------------------


def _float_field(type_text: str) -> FieldDef:
    ftype = parse_type(type_text)
    assert not isinstance(ftype, str)
    return FieldDef("v", ftype, 0)


def test_float32_accepts_values_that_round_to_a_finite_float32(mock_codec: Codec) -> None:
    msg = mock_codec.model.by_name["MOCK_STATUS"]
    f32 = _float_field("float32")
    for text in ("3.4028235e38", "-3.4028235e38", "3.4028234663852886e38", "1e-50", "0"):
        assert coerce_value(msg, f32, text) is not None
    packet = mock_codec.encode_telemetry("MOCK_STATUS", {"temperature_c": "3.4028235e38"})
    assert packet.data


@pytest.mark.parametrize("text", ["3.5e38", "1e39", "1e400", "-1e400", "1e999999"])
def test_float_text_that_overflows_is_rejected_not_turned_into_inf(
    mock_codec: Codec, text: str
) -> None:
    msg = mock_codec.model.by_name["MOCK_STATUS"]
    with pytest.raises(IronbirdError) as info:
        coerce_value(msg, _float_field("float32"), text)
    assert info.value.code is Code.ENC_ARG
    if text.endswith("400") or text.endswith("999"):
        with pytest.raises(IronbirdError):
            coerce_value(msg, _float_field("float64"), text)


@pytest.mark.parametrize("text", ["inf", "-inf", "+Infinity", "nan", "NaN", " inf "])
def test_float_inf_and_nan_are_accepted_when_spelled(mock_codec: Codec, text: str) -> None:
    msg = mock_codec.model.by_name["MOCK_STATUS"]
    for type_text in ("float32", "float64"):
        value = coerce_value(msg, _float_field(type_text), text)
        assert isinstance(value, float) and not math.isfinite(value)


def test_float_python_values_and_huge_ints(mock_codec: Codec) -> None:
    msg = mock_codec.model.by_name["MOCK_STATUS"]
    f32 = _float_field("float32")
    assert coerce_value(msg, f32, float("inf")) == float("inf")
    with pytest.raises(IronbirdError) as info:
        coerce_value(msg, f32, 10**400)  # float() would raise OverflowError
    assert info.value.code is Code.ENC_ARG
    with pytest.raises(IronbirdError):
        coerce_value(msg, f32, 3.5e38)


# -- 7. load-time length-field fit --------------------------------------------------------------


def test_message_too_long_for_the_length_field_fails_at_codec_load() -> None:
    narrow = header.load_profile(CORPUS / "codec-narrow-length-field.toml")
    cmd = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
    big = parse_model(
        _model_with(fields=[{"name": "t", "type": "char[300]"}]),
        "t.json",
    )
    with pytest.raises(IronbirdError) as info:
        Codec(big, cmd, narrow)
    assert info.value.code is Code.ENC_FIT and "length" in info.value.summary
    small = parse_model(_model_with(fields=[{"name": "t", "type": "char[200]"}]), "t.json")
    codec = Codec(small, cmd, narrow)
    assert len(codec.encode_telemetry("A").data) == 203
    edge = parse_model(_model_with(fields=[{"name": "t", "type": "char[253]"}]), "t.json")
    with pytest.raises(IronbirdError):  # 3 + 253 = 256 > 255
        Codec(edge, cmd, narrow)


# -- 8. the cheap half of the decode boundary ------------------------------------------------------


def test_check_separates_known_unknown_and_malformed(mock_codec: Codec) -> None:
    good = mock_codec.encode_telemetry("MOCK_HK").data
    assert mock_codec.check(TLM, good) is None
    unknown = bytearray(good)
    unknown[1] ^= 0x7F  # another APID
    error = mock_codec.check(TLM, bytes(unknown))
    assert error is not None and error.code is Code.DEC_UNKNOWN and error.mid is not None
    expected = {
        b"": Code.DEC_SHORT,
        good[:5]: Code.DEC_SHORT,
        good + bytes(mock_codec.max_packet_bytes): Code.DEC_OVERSIZE,
        good + b"\0": Code.DEC_LENGTH,  # the length field disagrees with the datagram
    }
    for bad, code in expected.items():
        error = mock_codec.check(TLM, bad)
        assert error is not None and error.code is code, (bad[:8], code)
    # An unknown MID is reported as such even when the rest of the header is also wrong.
    assert mock_codec.check(TLM, bytes(unknown) + b"\0").code is Code.DEC_UNKNOWN  # type: ignore[union-attr]


# -- 3. sanitize ----------------------------------------------------------------------------------


def test_clean_escapes_the_replacement_character() -> None:
    assert sanitize.clean("a�b") == "a\\ufffdb"


def test_clean_terminal_output_is_ascii_and_printable_on_any_stdout() -> None:
    bidi_override, replacement, degree = chr(0x202E), chr(0xFFFD), chr(0xB0)
    nasty = f"ok\x1b[31m{replacement}{degree}C{bidi_override}\U0001f600\ud800\n\x00"
    out = sanitize.clean_terminal(nasty)
    assert out.isascii() and out.isprintable()
    out.encode("ascii")
    assert "\x1b" not in out and "\\x1b" in out and "\\u00b0" not in out
    assert sanitize.clean_terminal("a" * 1000, 10) == "a" * 10 + "..."


def test_clean_terminal_keeps_what_the_stream_can_encode() -> None:
    assert sanitize.clean_terminal("5°C", encoding="utf-8") == "5°C"
    assert sanitize.clean_terminal("5°C", encoding="latin-1") == "5°C"
    assert sanitize.clean_terminal("€", encoding="latin-1") == "\\u20ac"
    assert sanitize.clean_terminal("�", encoding="utf-8") == "\\ufffd"
    assert sanitize.clean_terminal("\x1b", encoding="utf-8") == "\\x1b"
    assert sanitize.clean_terminal("°", encoding="no-such-codec") == "\\xb0"
    assert sanitize.clean_terminal("°", encoding=None) == "\\xb0"


def test_clean_json_keeps_text_whole_and_leaves_escaping_to_json() -> None:
    long_text = "x" * 1000 + "\x1b[31m�"
    out = sanitize.clean_json(long_text)
    assert out == long_text  # no truncation, no literal "\x1b" text
    assert json.loads(json.dumps(out)) == out
    surrogate = sanitize.clean_json("a\ud800b")
    assert surrogate.encode("utf-8") and "\ud800" not in surrogate
