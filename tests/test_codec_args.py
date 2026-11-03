"""Argument coercion at the encode boundary: values typed on a command line (always strings) or
passed from Python that do not fit the field they are for are a coded ``IB-ENC-002`` that names
the field, never a raw exception and never a silently wrong byte."""

from __future__ import annotations

import json
import math
from functools import cache
from pathlib import Path
from typing import Any

import pytest

from conftest import EXAMPLES
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import FieldDef, Message, TypeClass
from ironbird.codec import coerce_value
from ironbird.errors import Code, IronbirdError


@cache
def model() -> object:
    return load_ccdd(EXAMPLES / "ccdd" / "cfs.json")


def find(type_class: TypeClass, *, size: int | None = None) -> tuple[Message, FieldDef]:
    for message in model().by_name.values():  # type: ignore[attr-defined]
        for field in message.fields:
            if (
                field.type.type_class is type_class
                and field.var is None
                and (size is None or field.type.size == size)
            ):
                return message, field
    raise AssertionError(f"no {type_class} field in the CCDD")


def refused(message: Message, field: FieldDef, value: object) -> IronbirdError:
    with pytest.raises(IronbirdError) as info:
        coerce_value(message, field, value)
    assert info.value.code is Code.ENC_ARG
    assert field.name in info.value.summary
    return info.value


FLOAT = find(TypeClass.FLOAT)
BYTES = find(TypeClass.BYTES)
INT = find(TypeClass.INT)


@pytest.mark.parametrize("value", ["not-a-number", "", "1.2.3", "0x", "12abc"])
def test_a_float_field_refuses_text_that_is_not_a_number(value: str) -> None:
    refused(*FLOAT, value)


@pytest.mark.parametrize("value", [None, [1.0], {"x": 1}, object()])
def test_a_float_field_refuses_values_of_the_wrong_kind(value: object) -> None:
    refused(*FLOAT, value)


def test_a_float_field_takes_numbers_and_spelled_specials() -> None:
    message, field = FLOAT
    assert coerce_value(message, field, "1.5") == 1.5
    assert coerce_value(message, field, 2) == 2.0
    assert math.isinf(coerce_value(message, field, "inf"))  # type: ignore[arg-type]
    assert math.isnan(coerce_value(message, field, "nan"))  # type: ignore[arg-type]


def test_a_float_field_refuses_an_integer_too_large_for_a_float() -> None:
    refused(*FLOAT, 10**400)


def test_a_bytes_field_needs_exactly_its_size() -> None:
    message, field = BYTES
    size = field.type.size
    good = "ab" * size
    assert coerce_value(message, field, good) == bytes.fromhex(good)
    refused(message, field, "ab" * (size + 1))
    refused(message, field, "ab" * (size - 1) if size > 1 else "")
    refused(message, field, b"\x00" * (size + 1))


@pytest.mark.parametrize("value", ["zz", "abc", "0x12", 12, None, ["ab"]])
def test_a_bytes_field_refuses_text_that_is_not_hex_and_values_of_the_wrong_kind(
    value: object,
) -> None:
    refused(*BYTES, value)


def test_a_bytes_field_refuses_hex_text_beyond_the_text_cap_without_decoding_it() -> None:
    refused(*BYTES, "ab" * 100_000)


@pytest.mark.parametrize("value", ["", "x", "1.5", "0b2", None, 1.5, True, [1]])
def test_an_integer_field_refuses_what_is_not_an_integer(value: object) -> None:
    refused(*INT, value)


def test_a_bad_array_element_is_named_by_position() -> None:
    from ironbird.codec import coerce_variable  # the counted-array path of a variable field

    for message in model().by_name.values():  # type: ignore[attr-defined]
        for field in message.fields:
            if field.var is not None and field.type.type_class is TypeClass.ARRAY:
                with pytest.raises(IronbirdError) as info:
                    coerce_variable(message, field, "1,2,not-a-number")
                assert info.value.code is Code.ENC_ARG and "[2]" in info.value.summary
                return
    raise AssertionError("no counted array in the CCDD")


# -- profile / definition fit, checked when the codec is built --

CMD_PROFILE = EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"
TLM_PROFILE = EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"


def build_codec(command: Path = CMD_PROFILE, telemetry: Path = TLM_PROFILE, **edits: Any) -> Any:
    from ironbird.ccdd.json_adapter import parse_model
    from ironbird.codec import Codec
    from ironbird.header import load_profile

    data = json.loads((EXAMPLES / "ccdd" / "mock.json").read_text())
    for edit in edits.values():
        edit(data)
    return Codec(parse_model(data, "edited.json"), load_profile(command), load_profile(telemetry))


def test_profiles_given_in_the_wrong_roles_are_refused() -> None:
    with pytest.raises(IronbirdError) as info:
        build_codec(command=TLM_PROFILE)
    assert info.value.code is Code.ENC_FIT and "not a command header profile" in info.value.summary
    with pytest.raises(IronbirdError) as info:
        build_codec(telemetry=CMD_PROFILE)
    assert info.value.code is Code.ENC_FIT and "not a telemetry" in info.value.summary


def test_a_message_id_wider_than_the_profile_can_carry_is_refused_at_load() -> None:
    def widen(data: dict[str, Any]) -> None:
        data["messages"][0]["mid"] = "0x1FFFFF"

    with pytest.raises(IronbirdError) as info:
        build_codec(widen=widen)
    assert info.value.code is Code.ENC_FIT and "MID 0x1FFFFF" in info.value.summary


def test_a_function_code_wider_than_its_header_field_is_refused_at_load() -> None:
    def too_big(data: dict[str, Any]) -> None:
        data["messages"][0]["function_code"] = 200  # the field is 7 bits wide

    with pytest.raises(IronbirdError) as info:
        build_codec(too_big=too_big)
    assert info.value.code is Code.ENC_FIT and "function code 200" in info.value.summary
