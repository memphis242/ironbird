"""Header profile schema and pack errors that a hand-edited profile can hit: each is a coded
error naming the file and the field path, never a crash."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from ironbird import header
from ironbird.errors import Code, IronbirdError
from ironbird.header import HeaderProfile


def profile_dict() -> dict[str, Any]:
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


def build(data: dict[str, Any]) -> HeaderProfile:
    return header.parse_profile(data, "p.toml")


@pytest.mark.parametrize(
    ("edit", "needle"),
    [
        (lambda d: d.update(description="x" * 5000), "longer than"),
        (lambda d: d["fields"][2].update(value=3, default=1), "default"),
        (lambda d: d["fields"][2].update(default="seven"), "default"),
        (lambda d: d["fields"][1].update(computed="mystery"), "unknown kind"),
        (lambda d: d["fields"][1].update(default=4), "only valid on input fields"),
        (lambda d: d["fields"][2].update(bias=1), "only valid on computed = 'length'"),
        (lambda d: d["fields"][2].update(algorithm="xor8_cfs"), "only valid on computed"),
        (lambda d: d["fields"][3].pop("algorithm"), "requires 'algorithm'"),
        (lambda d: d.update(function_code_field="mid"), "must name an input field"),
        (lambda d: d.update(function_code_field="nope"), "must name an input field"),
        (lambda d: d.pop("function_code_field"), "required for command profiles"),
        (lambda d: d.update(fields=[]), "non-empty"),
        (lambda d: d.update(fields="nope"), "non-empty"),
        (
            lambda d: d.update(fields=[{"name": f"f{i}", "bits": 8} for i in range(300)]),
            "too many",
        ),
        (lambda d: d["fields"].append("not a table"), "must be a table"),
        (lambda d: d["fields"][2].update(name="not an identifier"), "identifier"),
        (lambda d: d.update(byte_order="little"), "only 'big'"),
        (lambda d: d.update(trailer="nope"), "trailer"),
        (lambda d: d.update(trailer={"fields": [], "extra": 1}), "extra"),
        (lambda d: d.update(name="9starts-with-digit"), "identifier"),
    ],
)
def test_profile_defects_are_coded_schema_errors(edit: Any, needle: str) -> None:
    data = copy.deepcopy(profile_dict())
    edit(data)
    with pytest.raises(IronbirdError) as info:
        build(data)
    assert info.value.code is Code.HDR_SCHEMA
    assert info.value.summary.startswith("p.toml: ")
    assert needle in info.value.summary


def test_a_header_wider_than_the_limit_is_refused() -> None:
    data = profile_dict()
    data["fields"] = [{"name": f"f{i}", "bits": 64} for i in range(40)] + data["fields"]
    with pytest.raises(IronbirdError) as info:
        build(data)
    assert info.value.code is Code.HDR_SCHEMA and "exceeds" in info.value.summary


def test_a_checksum_not_on_a_byte_boundary_is_refused() -> None:
    data = profile_dict()
    data["fields"] = [
        {"name": "mid", "bits": 16, "computed": "from_mid"},
        {"name": "length", "bits": 16, "computed": "length", "bias": 0},
        {"name": "fc", "bits": 4},
        {"name": "checksum", "bits": 8, "computed": "checksum", "algorithm": "xor8_cfs"},
        {"name": "pad", "bits": 4, "value": 0},
    ]
    data["function_code_field"] = "fc"
    with pytest.raises(IronbirdError) as info:
        build(data)
    assert info.value.code is Code.HDR_SCHEMA and "byte aligned" in info.value.summary


# -- pack and split_mid errors --


def test_a_profile_with_no_message_id_fields_cannot_map_a_message_id() -> None:
    data = profile_dict()
    data["kind"] = "telemetry"
    data.pop("function_code_field")
    data.pop("mid_fields")
    data["fields"][0] = {"name": "word", "bits": 16, "value": 0}
    profile = build(data)
    assert header.split_mid(profile, 1) == "header profile defines no message ID fields"
    assert isinstance(header.split_mid(build(profile_dict()), 1 << 20), str)


@pytest.mark.parametrize(
    ("kwargs", "code", "needle"),
    [
        ({"mid": 1, "inputs": {"nope": 1}}, Code.HDR_PACK, "not an input field"),
        ({"mid": 1, "inputs": {"mid": 1}}, Code.HDR_PACK, "not an input field"),
        (
            {"mid": 1, "inputs": {"function_code": 1}, "tamper": {"nope": 1}},
            Code.HDR_PACK,
            "tamper",
        ),
        (
            {"mid": 1, "inputs": {"function_code": 1}, "tamper": {"function_code": 1}},
            Code.HDR_PACK,
            "tamper",
        ),
        ({"inputs": {"function_code": 1}}, Code.ENC_FIT, "needs a message ID"),
        ({"mid": 1 << 20, "inputs": {"function_code": 1}}, Code.ENC_FIT, "message ID"),
        ({"mid": 1, "inputs": {}}, Code.HDR_PACK, "no value for input field"),
    ],
)
def test_pack_refuses_what_the_profile_cannot_carry(
    kwargs: dict[str, Any], code: Code, needle: str
) -> None:
    profile = build(profile_dict())
    with pytest.raises(IronbirdError) as info:
        header.pack(profile, b"", **kwargs)
    assert info.value.code is code and needle in info.value.summary


def test_a_payload_that_overflows_the_length_field_is_refused() -> None:
    profile = build(profile_dict())
    with pytest.raises(IronbirdError) as info:
        header.pack(profile, b"\x00" * 70_000, mid=1, inputs={"function_code": 1})
    assert info.value.code is Code.ENC_FIT and "does not fit" in info.value.summary
