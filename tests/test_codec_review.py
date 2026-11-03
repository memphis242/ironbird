"""Regression tests for the encode-side and load-time findings of the codec review.

Each test names the behaviour it protects: the sequence number a packet is reported with, list
text that must not be repaired, response rules that can never answer, a link whose two endpoints
are one socket, a profile that cannot supply its function code, a header file of the wrong kind,
text with a NUL, digits that are not ASCII, a value that rounds to zero, and an integer that no
enum names.
"""

from __future__ import annotations

import asyncio
import json
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from conftest import EXAMPLES
from ironbird import header as hdr
from ironbird.ccdd.json_adapter import parse_model
from ironbird.ccdd.model import MessageKind
from ironbird.cli.main import _parse_header_values, _parse_mids, app
from ironbird.codec import Codec, DecodedPacket, parse_ascii_int
from ironbird.config import ResponseRule, SystemConfig, load_config
from ironbird.errors import Code, IronbirdError
from ironbird.ledger import Ledger
from ironbird.session import Session, _rule_failure, _validate_rules

CMD, TLM = MessageKind.COMMAND, MessageKind.TELEMETRY
MOCK: dict[str, Any] = json.loads((EXAMPLES / "ccdd" / "mock.json").read_text())
TLM_TOML: str = (EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml").read_text()


def _codec(ccdd: dict[str, Any] | None = None, telemetry: hdr.HeaderProfile | None = None) -> Codec:
    return Codec(
        parse_model(ccdd if ccdd is not None else MOCK, "mock.json"),
        hdr.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        telemetry or hdr.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
        clock=lambda: 1_000_000.25,
    )


def _refused(call: Callable[[], object], code: Code = Code.ENC_ARG) -> IronbirdError:
    with pytest.raises(IronbirdError) as info:
        call()
    assert info.value.code is code, info.value.user_line()
    assert "\n" not in info.value.user_line()
    return info.value


# -- the reported sequence number is the one on the wire -------------------------------------------


def test_reported_sequence_is_the_wire_value_after_the_field_wraps(mock_codec: Codec) -> None:
    encoded = mock_codec.encode_telemetry("MOCK_HK")
    for _ in range(16384):  # one more packet than the 14-bit sequence field counts
        encoded = mock_codec.encode_telemetry("MOCK_HK")
    decoded = mock_codec.decode(TLM, encoded.data)
    assert isinstance(decoded, DecodedPacket)
    assert encoded.sequence == decoded.header.sequence == 0


def test_reported_sequence_is_none_when_the_profile_has_no_sequence_field() -> None:
    text = TLM_TOML.replace('computed = "sequence"', "value = 0")
    assert text != TLM_TOML
    codec = _codec(telemetry=hdr.parse_profile(tomllib.loads(text), "<no-sequence>"))
    assert codec.profile(TLM).sequence_field is None
    assert codec.encode_telemetry("MOCK_HK").sequence is None
    assert codec.encode_command("MOCK_NOOP").sequence == 0  # the command profile has one


# -- list text is never repaired -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "position"),
    [
        ("1,,2", 1),
        (",1", 0),
        ("1 , , 2", 1),
        ("[,]", 0),
        ("[1,,2]", 1),
        ("1,2,,", 2),
        (",", 0),
        ("1, ,3", 1),
    ],
)
def test_empty_list_element_is_refused_with_its_position(
    mock_codec: Codec, text: str, position: int
) -> None:
    error = _refused(
        lambda: mock_codec.encode_command("MOCK_LOAD_DATA", {"Entries": text, "Text": ""})
    )
    assert f"Entries[{position}]" in error.summary and "empty" in error.summary


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1,2,3", (1, 2, 3)),
        ("[1, 2, 3]", (1, 2, 3)),
        ("1 2 3", (1, 2, 3)),
        ("1, 2, 3", (1, 2, 3)),
        ("1,2,3,", (1, 2, 3)),  # one trailing comma is a habit, not a missing element
        ("1, 2 3", (1, 2, 3)),
        ("", ()),
        ("[]", ()),
        ("  ", ()),
    ],
)
def test_well_formed_lists_still_encode(
    mock_codec: Codec, text: str, expected: tuple[int, ...]
) -> None:
    wire = mock_codec.encode_command("MOCK_LOAD_DATA", {"Entries": text, "Text": ""}).data
    decoded = mock_codec.decode(CMD, wire)
    assert isinstance(decoded, DecodedPacket)
    assert decoded.as_dict()["Entries"] == expected


# -- response rules are fully checked at load ------------------------------------------------------

_CONFIG = """[ironbird]
acts_as = "ground"
[nodes.ground]
[nodes.fc]
ccdd = "{ex}/ccdd/mock.json"
command_header = "{ex}/headers/ccsds_v1_cfs_cmd.toml"
telemetry_header = "{ex}/headers/ccsds_v1_cfs_tlm.toml"
{rule}
[[links]]
name = "ground-fc"
from = "ground"
to = "fc"
command_endpoint = "127.0.0.1:{command_port}"
telemetry_endpoint = "127.0.0.1:{telemetry_port}"
"""


def _config(
    tmp_path: Path, rule: str = "", *, command_port: int = 47201, telemetry_port: int = 47202
) -> SystemConfig:
    path = tmp_path / "c.toml"
    body = f"[[nodes.fc.responses]]\n{rule}" if rule else ""
    path.write_text(
        _CONFIG.format(
            ex=EXAMPLES, rule=body, command_port=command_port, telemetry_port=telemetry_port
        )
    )
    return load_config(path)


def _validated(tmp_path: Path, rule: str, codec: Codec) -> None:
    config = _config(tmp_path, rule)
    _validate_rules(config.nodes["fc"], codec, config)


@pytest.mark.parametrize(
    ("rule", "needle"),
    [
        # a char[8] into a uint8, a uint8 into a char[8]: different type classes
        ('on = "MOCK_SET_LABEL"\nreply = "MOCK_HK"\necho = { cmd_counter = "label" }', "label"),
        ('on = "MOCK_SET_MODE"\nreply = "MOCK_HK"\necho = { label = "mode" }', "mode"),
        # a uint16 into a uint8 does not always fit
        ('on = "MOCK_RAISE_EVENT"\nreply = "MOCK_HK"\necho = { cmd_counter = "event_id" }', "fit"),
        # a char[32] into a char[8]
        ('on = "MOCK_RAISE_EVENT"\nreply = "MOCK_HK"\necho = { label = "message" }', "fit"),
        # a count echoed without the data it sizes
        ('on = "MOCK_LOAD_DATA"\nreply = "MOCK_DATA"\necho = { Count = "Count" }', "Count"),
        # a count echoed with one of the two arrays it does not size
        (
            'on = "MOCK_LOAD_DATA"\nreply = "MOCK_DATA"\necho = { Count = "Count", Text = "Text" }',
            "Count",
        ),
        # the two counters write one field
        (
            'on = "MOCK_NOOP"\nreply = "MOCK_HK"\naccepted_counter = "cmd_counter"\n'
            'rejected_counter = "cmd_counter"',
            "cmd_counter",
        ),
        # a counter that a literal also sets
        (
            'on = "MOCK_NOOP"\nreply = "MOCK_HK"\naccepted_counter = "cmd_counter"\n'
            "fields = { cmd_counter = 3 }",
            "cmd_counter",
        ),
        # a counter that an echo also sets
        (
            'on = "MOCK_SET_MODE"\nreply = "MOCK_HK"\nrejected_counter = "mode"\n'
            'echo = { mode = "mode" }',
            "mode",
        ),
        # a field that a literal and an echo both set
        (
            'on = "MOCK_SET_MODE"\nreply = "MOCK_HK"\nfields = { mode = 1 }\n'
            'echo = { mode = "mode" }',
            "mode",
        ),
    ],
)
def test_response_rule_that_cannot_answer_is_refused_at_load(
    tmp_path: Path, mock_codec: Codec, rule: str, needle: str
) -> None:
    with pytest.raises(IronbirdError) as info:
        _validated(tmp_path, rule, mock_codec)
    error = info.value
    assert error.code in (Code.CFG_SCHEMA, Code.ENC_ARG), error.user_line()
    assert "\n" not in error.user_line()
    assert "nodes.fc.responses[0]" in error.summary  # the rule is named
    assert needle in error.summary


@pytest.mark.parametrize(
    "rule",
    [
        'on = "MOCK_SET_MODE"\nreply = "MOCK_HK"\necho = { mode = "mode" }',
        'on = "MOCK_SET_LABEL"\nreply = "MOCK_HK"\necho = { label = "label" }',
        'on = "MOCK_LOAD_DATA"\nreply = "MOCK_DATA"\necho = { Entries = "Entries", Text = "Text" }',
        'on = "MOCK_LOAD_DATA"\nreply = "MOCK_DATA"\n'
        'echo = { Count = "Count", Entries = "Entries", Text = "Text" }',
        'on = "MOCK_SET_MODE"\nreply = "MOCK_HK"\naccepted_counter = "cmd_counter"\n'
        'rejected_counter = "err_counter"\nfields = { label = "x" }',
        'on = "*"\nreply = "MOCK_HK"\naccepted_counter = "cmd_counter"',
    ],
)
def test_response_rule_that_can_answer_loads(tmp_path: Path, mock_codec: Codec, rule: str) -> None:
    _validated(tmp_path, rule, mock_codec)


def test_example_config_rules_load(mock_codec: Codec) -> None:
    config = load_config(EXAMPLES / "mock.toml")
    assert _validate_rules(config.nodes["fc"], mock_codec, config)


# -- a UDP link's two endpoints are two sockets ----------------------------------------------------


def test_udp_link_with_identical_endpoints_is_a_config_error(tmp_path: Path) -> None:
    path = tmp_path / "c.toml"
    path.write_text(_CONFIG.format(ex=EXAMPLES, rule="", command_port=47203, telemetry_port=47203))
    error = _refused(lambda: load_config(path), Code.CFG_SCHEMA)
    assert "command_endpoint" in error.summary and "telemetry_endpoint" in error.summary
    assert error.exit_code == 2


def test_identical_endpoints_are_compared_after_normalisation(tmp_path: Path) -> None:
    path = tmp_path / "c.toml"
    text = _CONFIG.format(ex=EXAMPLES, rule="", command_port=47203, telemetry_port=47203)
    path.write_text(
        text.replace('command_endpoint = "127.0.0.1', 'command_endpoint = "LocalHost').replace(
            'telemetry_endpoint = "127.0.0.1', 'telemetry_endpoint = "localhost'
        )
    )
    _refused(lambda: load_config(path), Code.CFG_SCHEMA)


def test_udp_link_with_different_ports_or_hosts_loads(tmp_path: Path) -> None:
    _config(tmp_path, command_port=47203, telemetry_port=47204)
    path = tmp_path / "c.toml"
    path.write_text(
        _CONFIG.format(ex=EXAMPLES, rule="", command_port=47203, telemetry_port=47203).replace(
            'telemetry_endpoint = "127.0.0.1', 'telemetry_endpoint = "127.0.0.2'
        )
    )
    load_config(path)


# -- a telemetry profile's function code field must be supplied -----------------------------------


def _telemetry_profile_with_function_code(default: int | None) -> hdr.HeaderProfile:
    extra = "" if default is None else f"\ndefault = {default}"
    text = TLM_TOML.replace("mid_fields =", 'function_code_field = "fc"\nmid_fields =').replace(
        '[[fields]]\nname = "spare"\nbits = 32',
        f'[[fields]]\nname = "fc"\nbits = 8{extra}\n\n[[fields]]\nname = "spare"\nbits = 24',
    )
    assert "function_code_field" in text
    return hdr.parse_profile(tomllib.loads(text), "<fc-probe>")


def test_telemetry_function_code_field_without_default_is_refused_at_load() -> None:
    profile = _telemetry_profile_with_function_code(None)
    error = _refused(lambda: _codec(telemetry=profile), Code.HDR_SCHEMA)
    assert "fc" in error.summary and "default" in error.summary


def test_telemetry_function_code_field_with_default_is_an_ordinary_input() -> None:
    codec = _codec(telemetry=_telemetry_profile_with_function_code(5))
    decoded = codec.decode(TLM, codec.encode_telemetry("MOCK_HK").data)
    assert isinstance(decoded, DecodedPacket) and decoded.header.values["fc"] == 5
    again = codec.decode(TLM, codec.encode_telemetry("MOCK_HK", header={"fc": 9}).data)
    assert isinstance(again, DecodedPacket) and again.header.values["fc"] == 9


# -- a header file of the wrong kind is a configuration error naming the key ----------------------


def test_codec_refuses_profiles_of_the_wrong_kind_with_a_header_code() -> None:
    telemetry = hdr.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml")
    command = hdr.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
    model = parse_model(MOCK, "mock.json")
    swapped = _refused(lambda: Codec(model, telemetry, telemetry), Code.HDR_SCHEMA)
    assert "not a command header profile" in swapped.summary
    swapped = _refused(lambda: Codec(model, command, command), Code.HDR_SCHEMA)
    assert "not a telemetry header profile" in swapped.summary


def _swapped_config(tmp_path: Path, key: str) -> Path:
    other = "telemetry_header" if key == "command_header" else "command_header"
    path = tmp_path / "c.toml"
    text = _CONFIG.format(ex=EXAMPLES, rule="", command_port=47205, telemetry_port=47206)
    line = next(ln for ln in text.splitlines() if ln.startswith(f"{key} = "))
    other_line = next(ln for ln in text.splitlines() if ln.startswith(f"{other} = "))
    path.write_text(text.replace(line, f"{key} = {other_line.split(' = ', 1)[1]}"))
    return path


def _session_error(path: Path, tmp_path: Path) -> IronbirdError:
    config = load_config(path)

    async def main() -> None:
        async with Session(
            config,
            role="ground",
            ledger=Ledger(tmp_path / "ledgers", session_id="s"),
            notify=lambda _note: None,
        ) as session:
            session.codec_for("fc")

    with pytest.raises(IronbirdError) as info:
        asyncio.run(main())
    return info.value


@pytest.mark.parametrize("key", ["command_header", "telemetry_header"])
def test_session_names_the_config_key_of_a_header_of_the_wrong_kind(
    tmp_path: Path, key: str
) -> None:
    error = _session_error(_swapped_config(tmp_path, key), tmp_path)
    assert error.code is Code.CFG_SCHEMA and error.exit_code == 2
    assert f"nodes.fc.{key}" in error.summary
    assert "\n" not in error.user_line()


def test_cli_exits_2_naming_the_key_of_a_header_of_the_wrong_kind(tmp_path: Path) -> None:
    path = _swapped_config(tmp_path, "command_header")
    result = CliRunner().invoke(app, ["-c", str(path), "send", "fc", "MOCK_NOOP"])
    assert result.exit_code == 2
    assert "nodes.fc.command_header" in result.output


# -- text -----------------------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["label", "message"])
def test_nul_in_fixed_text_is_refused_like_variable_text(mock_codec: Codec, name: str) -> None:
    command, args = (
        ("MOCK_SET_LABEL", {"label": "A\x00B"})
        if name == "label"
        else ("MOCK_RAISE_EVENT", {"app": "a", "event_id": 1, "event_type": 1, "message": "A\x00B"})
    )
    fixed = _refused(lambda: mock_codec.encode_command(command, args))
    assert "NUL" in fixed.summary
    variable = _refused(
        lambda: mock_codec.encode_command("MOCK_LOAD_DATA", {"Entries": "1", "Text": "A\x00B"})
    )
    assert "NUL" in variable.summary


def test_fixed_text_without_nul_round_trips(mock_codec: Codec) -> None:
    wire = mock_codec.encode_command("MOCK_SET_LABEL", {"label": "AB"}).data
    decoded = mock_codec.decode(CMD, wire)
    assert isinstance(decoded, DecodedPacket) and decoded.as_dict() == {"label": "AB"}


# -- digits are ASCII -----------------------------------------------------------------------------

# Arabic-Indic, fullwidth, mathematical bold, hex prefix with a fullwidth digit, Devanagari
LOOKALIKE_DIGITS = ["\u0661", "\uff11", "\U0001d7cf", "0x\uff11", "\u0967\u0968"]


@pytest.mark.parametrize("text", LOOKALIKE_DIGITS)
def test_integer_argument_is_ascii_only(mock_codec: Codec, text: str) -> None:
    error = _refused(lambda: mock_codec.encode_command("MOCK_SET_MODE", {"mode": text, "pad": "0"}))
    assert "not an integer" in error.summary
    _refused(lambda: mock_codec.encode_command("MOCK_SET_MODE", {"mode": "1", "pad": text}))
    _refused(
        lambda: mock_codec.encode_command("MOCK_LOAD_DATA", {"Entries": f"1,{text}", "Text": ""})
    )


@pytest.mark.parametrize("text", ["\u0661.\u0665", "\uff11.\uff15", "1.\uff15", "\U0001d7cf"])
def test_float_argument_is_ascii_only(mock_codec: Codec, text: str) -> None:
    error = _refused(lambda: mock_codec.encode_telemetry("MOCK_STATUS", {"temperature_c": text}))
    assert "not a number" in error.summary


@pytest.mark.parametrize("text", LOOKALIKE_DIGITS)
def test_header_option_integer_is_ascii_only(text: str) -> None:
    error = _refused(lambda: _parse_header_values([f"priority={text}"]), Code.CLI_USAGE)
    assert "integer" in error.summary
    assert _parse_header_values(["priority=0x10", "dest=7"]) == {"priority": 16, "dest": 7}


@pytest.mark.parametrize("text", LOOKALIKE_DIGITS)
def test_mid_option_integer_is_ascii_only(tmp_path: Path, text: str) -> None:
    config = load_config(EXAMPLES / "mock.toml")

    async def main() -> frozenset[int] | None:
        async with Session(
            config,
            role="ground",
            ledger=Ledger(tmp_path / "ledgers", session_id="s"),
            notify=lambda _note: None,
        ) as session:
            assert _parse_mids(session, "fc", ["0x890"], None) == frozenset({0x0890})
            return _parse_mids(session, "fc", [text], None)

    with pytest.raises(IronbirdError) as info:
        asyncio.run(main())
    assert info.value.code is Code.CLI_USAGE


def test_parse_ascii_int() -> None:
    assert [parse_ascii_int(t) for t in ("12", "-3", "0x1F", "0b11", "1_000")] == [
        12,
        -3,
        31,
        3,
        1000,
    ]
    for bad in ("", "x", "1.5", "\u0661", "0x\uff11"):
        with pytest.raises(ValueError):
            parse_ascii_int(bad)


# -- a value that rounds to zero is refused as overflow is ----------------------------------------


@pytest.mark.parametrize(
    "text", ["1e-50", "-1e-50", "1e-46", "0.000000000000000000000000000000001e-20"]
)
def test_float32_underflow_to_zero_is_refused(mock_codec: Codec, text: str) -> None:
    error = _refused(lambda: mock_codec.encode_telemetry("MOCK_STATUS", {"temperature_c": text}))
    assert "underflows float32" in error.summary
    _refused(lambda: mock_codec.encode_telemetry("MOCK_STATUS", {"temperature_c": float(text)}))


@pytest.mark.parametrize("text", ["0", "0.0", "-0", "0e5", "1e-45", "1.5e-44", "1e-30", "3e38"])
def test_float32_zero_and_small_representable_values_are_accepted(
    mock_codec: Codec, text: str
) -> None:
    mock_codec.encode_telemetry("MOCK_STATUS", {"temperature_c": text})


def test_float32_overflow_is_still_refused(mock_codec: Codec) -> None:
    error = _refused(lambda: mock_codec.encode_telemetry("MOCK_STATUS", {"temperature_c": "1e39"}))
    assert "float32" in error.summary


def test_float64_text_that_underflows_is_refused() -> None:
    ccdd = json.loads(json.dumps(MOCK))
    status = next(m for m in ccdd["messages"] if m["name"] == "MOCK_STATUS")
    status["fields"][0]["type"] = "float64"
    codec = _codec(ccdd)
    error = _refused(lambda: codec.encode_telemetry("MOCK_STATUS", {"temperature_c": "1e-400"}))
    assert "underflows" in error.summary
    codec.encode_telemetry("MOCK_STATUS", {"temperature_c": "1e-300"})
    codec.encode_telemetry("MOCK_STATUS", {"temperature_c": "0e-400"})


# -- an integer no enum names is refused unless the field allows raw values -----------------------


def _mode_field(ccdd: dict[str, Any], message: str = "MOCK_SET_MODE") -> dict[str, Any]:
    msg = next(m for m in ccdd["messages"] if m["name"] == message)
    return next(f for f in msg["fields"] if f["name"] == "mode")


@pytest.mark.parametrize("text", ["7", "0", "0x10"])
def test_integer_outside_the_enum_is_refused(mock_codec: Codec, text: str) -> None:
    error = _refused(lambda: mock_codec.encode_command("MOCK_SET_MODE", {"mode": text, "pad": "0"}))
    assert "mode" in error.summary and "SAFE" in error.summary and "allow_raw" in error.summary
    _refused(lambda: mock_codec.encode_command("MOCK_SET_MODE", {"mode": int(text, 0), "pad": 0}))


@pytest.mark.parametrize(("text", "value"), [("SAFE", 1), ("2", 2), ("0x3", 3), ("NOMINAL", 2)])
def test_integer_inside_the_enum_is_accepted(mock_codec: Codec, text: str, value: int) -> None:
    wire = mock_codec.encode_command("MOCK_SET_MODE", {"mode": text, "pad": "0"}).data
    decoded = mock_codec.decode(CMD, wire)
    assert isinstance(decoded, DecodedPacket) and decoded.as_dict()["mode"] == value


def test_telemetry_may_carry_an_integer_outside_the_enum(mock_codec: Codec) -> None:
    wire = mock_codec.encode_telemetry("MOCK_HK", {"mode": 9}).data  # a vehicle can report any
    decoded = mock_codec.decode(TLM, wire)
    assert isinstance(decoded, DecodedPacket) and decoded.as_dict()["mode"] == 9


def test_field_that_allows_raw_values_accepts_integers_outside_its_enum() -> None:
    ccdd = json.loads(json.dumps(MOCK))
    _mode_field(ccdd)["allow_raw"] = True
    codec = _codec(ccdd)
    wire = codec.encode_command("MOCK_SET_MODE", {"mode": "7", "pad": "0"}).data
    decoded = codec.decode(CMD, wire)
    assert isinstance(decoded, DecodedPacket) and decoded.as_dict()["mode"] == 7
    assert codec.model.by_name["MOCK_SET_MODE"].field("mode").allow_raw  # type: ignore[union-attr]
    # the range still holds
    _refused(lambda: codec.encode_command("MOCK_SET_MODE", {"mode": "300", "pad": "0"}))


@pytest.mark.parametrize(
    ("edit", "needle"),
    [
        (lambda f: f.update(allow_raw="yes"), "allow_raw"),
        (lambda f: f.update(allow_raw=1), "allow_raw"),
        (lambda f: f.pop("enum"), "allow_raw"),  # nothing to allow past
    ],
)
def test_allow_raw_must_be_true_or_false_and_needs_an_enum(
    edit: Callable[[dict[str, Any]], object], needle: str
) -> None:
    ccdd = json.loads(json.dumps(MOCK))
    field = _mode_field(ccdd)
    field["allow_raw"] = True
    edit(field)
    with pytest.raises(IronbirdError) as info:
        parse_model(ccdd, "bad.json")
    assert info.value.code is Code.CCD_SCHEMA and needle in info.value.summary


# -- a failed reply is worded as a failure of its rule --------------------------------------------


def test_reply_that_cannot_be_encoded_names_the_rule_field_and_argument(mock_codec: Codec) -> None:
    rule = ResponseRule(
        on="MOCK_SET_LABEL",
        reply="MOCK_HK",
        fields={},
        echo={"label": "label"},
        accepted_counter=None,
        rejected_counter=None,
    )
    values = {"label": "café"}  # what a decoded byte above 0x7F becomes is not ASCII
    with pytest.raises(IronbirdError) as info:
        mock_codec.encode_telemetry("MOCK_HK", values)
    error = _rule_failure(mock_codec, rule, values, info.value)
    assert error.code is Code.RSP_RULE and "\n" not in error.user_line()
    for needle in ("MOCK_SET_LABEL", "reply field label", "command argument label", "no reply"):
        assert needle in error.summary
    assert error.cause is info.value


def test_reply_failure_without_an_echoed_culprit_still_names_the_rule(mock_codec: Codec) -> None:
    rule = ResponseRule(
        on="*", reply="MOCK_HK", fields={}, echo={}, accepted_counter=None, rejected_counter=None
    )
    error = _rule_failure(mock_codec, rule, {}, IronbirdError(Code.ENC_ARG, "MOCK_HK: boom"))
    assert error.code is Code.RSP_RULE and "rule '*'" in error.summary and "boom" in error.summary
