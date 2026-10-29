from __future__ import annotations

import copy
import json
import tomllib
from pathlib import Path
from typing import Any

import pytest

from conftest import EXAMPLES
from ironbird.ccdd.json_adapter import parse_model
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.config import find_config, load_config, parse_config
from ironbird.errors import Code, IronbirdError

# -- config ---------------------------------------------------------------------------


def _config() -> dict[str, Any]:
    return tomllib.loads((EXAMPLES / "mock.toml").read_text())


def test_example_configs_load() -> None:
    mock = load_config(EXAMPLES / "mock.toml")
    assert mock.acts_as == "ground"
    assert mock.resolve("flight").name == "fc"  # alias
    assert mock.links[0].command_endpoint.port == 41234
    real = load_config(EXAMPLES / "ironbird.toml")
    assert (real.links[0].command_endpoint.port, real.links[0].telemetry_endpoint.port) == (
        1234,
        2234,
    )
    assert real.nodes["fc"].ccdd == EXAMPLES / "ccdd" / "cfs.json"


@pytest.mark.parametrize(
    ("edit", "code", "needle"),
    [
        (lambda d: d.update(bogus=1), Code.CFG_SCHEMA, "bogus: unknown key"),
        (lambda d: d["nodes"]["fc"].update(ccdd_file="x"), Code.CFG_SCHEMA, "nodes.fc.ccdd_file"),
        (lambda d: d["ironbird"].update(acts_as="nobody"), Code.CFG_REF, "ironbird.acts_as"),
        (lambda d: d["ironbird"].update(max_packet_bytes=10), Code.CFG_SCHEMA, "max_packet_bytes"),
        (lambda d: d["links"][0].update({"to": "ghost"}), Code.CFG_REF, "links[0].to"),
        (lambda d: d["links"][0].update({"to": "ground"}), Code.CFG_SCHEMA, "different nodes"),
        (lambda d: d["links"][0].update(command_endpoint="nonsense"), Code.CFG_SCHEMA, "host:port"),
        (
            lambda d: d["links"][0].update(command_endpoint="h:70000"),
            Code.CFG_SCHEMA,
            "port must be",
        ),
        (
            lambda d: d["links"][0].update(transport="serial"),
            Code.CFG_SCHEMA,
            "unsupported transport",
        ),
        (lambda d: d["nodes"]["fc"].update(alias="gnd"), Code.CFG_SCHEMA, "already used"),
        (lambda d: d["nodes"]["fc"].pop("ccdd"), Code.CFG_REF, "nodes.fc.ccdd"),
        (lambda d: d["nodes"]["fc"]["responses"][0].pop("reply"), Code.CFG_SCHEMA, "reply"),
    ],
)
def test_config_validation_errors(edit: Any, code: Code, needle: str) -> None:
    data = copy.deepcopy(_config())
    edit(data)
    with pytest.raises(IronbirdError) as info:
        parse_config(data, Path("sys.toml"))
    assert info.value.code is code
    assert info.value.summary.startswith("sys.toml: ")
    assert needle in info.value.summary


def test_config_file_errors(tmp_path: Path) -> None:
    with pytest.raises(IronbirdError) as missing:
        load_config(tmp_path / "nope.toml")
    assert missing.value.code is Code.CFG_FILE
    bad = tmp_path / "bad.toml"
    bad.write_text("[nodes\n")
    with pytest.raises(IronbirdError) as syntax:
        load_config(bad)
    assert syntax.value.code is Code.CFG_SYNTAX


def test_find_config_order_and_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(IronbirdError) as info:
        find_config(None, {"XDG_CONFIG_HOME": str(tmp_path / "xdg")})
    assert info.value.code is Code.CFG_FILE
    assert find_config(Path("x.toml"), {}) == Path("x.toml")
    assert find_config(None, {"IRONBIRD_CONFIG": "/etc/ib.toml"}) == Path("/etc/ib.toml")


# -- CCDD -----------------------------------------------------------------------------


def _ccdd() -> dict[str, Any]:
    return json.loads((EXAMPLES / "ccdd" / "mock.json").read_text())


def test_example_ccdd_loads_and_indexes() -> None:
    model = load_ccdd(EXAMPLES / "ccdd" / "mock.json")
    assert [m.name for m in model.commands()] == [
        "MOCK_NOOP",
        "MOCK_SET_MODE",
        "MOCK_SET_LABEL",
        "MOCK_RAISE_EVENT",
    ]
    assert model.commands_by_key[(0x1890, 1)].name == "MOCK_SET_MODE"
    assert model.telemetry_by_mid[0x0890].kind is MessageKind.TELEMETRY
    status = model.by_name["MOCK_STATUS"]
    assert status.payload_size == 12
    assert [f.offset for f in status.fields] == [0, 4, 8]  # sequential offsets


def test_explicit_offsets_and_payload_size() -> None:
    data = _ccdd()
    data["messages"][3]["fields"][1]["offset"] = 4
    data["messages"][3]["payload_size"] = 16
    msg = parse_model(data, "x.json").by_name["MOCK_HK"]
    assert msg.payload_size == 16
    assert msg.fields[1].offset == 4


@pytest.mark.parametrize(
    ("edit", "code", "needle"),
    [
        (lambda d: d.update(schema="other/1"), Code.CCD_SCHEMA, "schema"),
        (lambda d: d.update(extra=1), Code.CCD_SCHEMA, "extra: unknown key"),
        (
            lambda d: d["messages"][0].update(kind="query"),
            Code.CCD_SCHEMA,
            "messages[0](MOCK_NOOP).kind",
        ),
        (lambda d: d["messages"][0].update(mid="banana"), Code.CCD_SCHEMA, ".mid"),
        (lambda d: d["messages"][0].update(mid=True), Code.CCD_SCHEMA, ".mid"),
        (lambda d: d["messages"][0].pop("function_code"), Code.CCD_SCHEMA, "function_code"),
        (
            lambda d: d["messages"][3].update(function_code=1),
            Code.CCD_SCHEMA,
            "only valid on commands",
        ),
        (
            lambda d: d["messages"][1]["fields"][0].update(type="uint128"),
            Code.CCD_SCHEMA,
            "fields[0].type",
        ),
        (
            lambda d: d["messages"][1]["fields"][0].update(type="char[0]"),
            Code.CCD_SCHEMA,
            "array length",
        ),
        (
            lambda d: d["messages"][1]["fields"][0].update(endian="middle"),
            Code.CCD_SCHEMA,
            "endian",
        ),
        (
            lambda d: d["messages"][1]["fields"][0].update(colour="red"),
            Code.CCD_SCHEMA,
            "colour: unknown key",
        ),
        (
            lambda d: d["messages"][1]["fields"][0].update(enum={"BIG": 300}),
            Code.CCD_SCHEMA,
            "enum.BIG",
        ),
        (
            lambda d: d["messages"][1]["fields"][0].update(enum={"A": 1, "B": 1}),
            Code.CCD_LAYOUT,
            "duplicates",
        ),
        (lambda d: d["messages"][1]["fields"][1].update(offset=0), Code.CCD_LAYOUT, "overlap"),
        (
            lambda d: d["messages"][1]["fields"][1].update(name="mode"),
            Code.CCD_LAYOUT,
            "duplicate field",
        ),
        (lambda d: d["messages"][1].update(payload_size=1), Code.CCD_LAYOUT, "smaller than"),
        (lambda d: d["messages"][1].update(name="MOCK_NOOP"), Code.CCD_LAYOUT, "duplicate message"),
        (lambda d: d["messages"][1].update(function_code=0), Code.CCD_LAYOUT, "share MID"),
        (lambda d: d["messages"][3].update(mid="0x1890"), Code.CCD_LAYOUT, "used by both"),
        (lambda d: d["messages"][4].update(mid="0x0890"), Code.CCD_LAYOUT, "share"),
    ],
)
def test_ccdd_validation_errors(edit: Any, code: Code, needle: str) -> None:
    data = copy.deepcopy(_ccdd())
    edit(data)
    with pytest.raises(IronbirdError) as info:
        parse_model(data, "t.json")
    assert info.value.code is code
    assert info.value.summary.startswith("t.json: ")
    assert needle in info.value.summary


def test_ccdd_file_errors(tmp_path: Path) -> None:
    for name, content in {
        "missing": None,
        "syntax": "{nope",
        "root": "[]",
        "binary": b"\xff\xfe",
    }.items():
        path = tmp_path / f"{name}.json"
        if isinstance(content, str):
            path.write_text(content)
        elif content is not None:
            path.write_bytes(content)
        with pytest.raises(IronbirdError) as info:
            load_ccdd(path)
        assert info.value.code in {Code.CCD_FILE, Code.CCD_SCHEMA}
    with pytest.raises(IronbirdError) as unknown:
        load_ccdd(tmp_path / "x.json", adapter="csv")
    assert unknown.value.code is Code.CCD_FILE


def test_real_cfs_example_builds_a_codec() -> None:
    """examples/ironbird.toml + cfs.json + cFS header profiles fit together."""
    from ironbird.codec import Codec
    from ironbird.header import load_profile

    node = load_config(EXAMPLES / "ironbird.toml").nodes["fc"]
    assert node.ccdd and node.command_header and node.telemetry_header
    codec = Codec(
        load_ccdd(node.ccdd), load_profile(node.command_header), load_profile(node.telemetry_header)
    )
    # TIME/ES-style no-arg command: 8-byte header, length field 1, XOR-to-0xFF checksum
    assert codec.encode_command("ES_NOOP").data == bytes.fromhex("1806c00000010020")
