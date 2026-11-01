"""The ``ib_test`` cFS app's C header and ironbird's CCDD agree on every number they share.

The C side asserts its own sizes at compile time (``IB_TEST_ASSERT_SIZE`` in ``ib_test.h``); this
checks, without building cFS, that the constants the header defines (topic IDs, function codes,
generator limits and sizes) are the ones ``examples/ccdd/cfs.json`` describes, so the two cannot
drift apart between a cFS build and a CI run.
"""

from __future__ import annotations

import re

import pytest

from conftest import EXAMPLES, REPO
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec
from ironbird.header import load_profile

HEADER = REPO / "tools" / "cfs" / "apps" / "ib_test" / "fsw" / "src" / "ib_test.h"
SOURCE = HEADER.with_name("ib_test.c")
TLM_MID_BASE = 0x0800  # CFE_PLATFORM_TLM_TOPICID_TO_MIDV, v1 header on the native target
CMD_MID_BASE = 0x1800


def _defines() -> dict[str, int]:
    found: dict[str, int] = {}
    for match in re.finditer(
        r"^#define\s+(IB_TEST_\w+)\s+(0x[0-9A-Fa-f]+|\d+)\b", HEADER.read_text(), re.M
    ):
        found[match.group(1)] = int(match.group(2), 0)
    return found


@pytest.fixture(scope="module")
def codec() -> Codec:
    return Codec(
        load_ccdd(EXAMPLES / "ccdd" / "cfs.json"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm_cfe_time.toml"),
    )


def test_the_topic_ids_give_the_mids_the_ccdd_lists() -> None:
    defines = _defines()
    model = load_ccdd(EXAMPLES / "ccdd" / "cfs.json")
    by_name = {m.name: m for m in model.messages}
    expected = {
        "IB_TEST_HK": TLM_MID_BASE + defines["IB_TEST_HK_TOPICID"],
        "IB_TEST_VAR": TLM_MID_BASE + defines["IB_TEST_VAR_TOPICID"],
        "IB_TEST_GEN": TLM_MID_BASE + defines["IB_TEST_GEN_TOPICID"],
        "IB_TEST_GEN_STATUS": TLM_MID_BASE + defines["IB_TEST_GEN_STATUS_TOPICID"],
        "IB_TEST_NOOP": CMD_MID_BASE + defines["IB_TEST_CMD_TOPICID"],
        "IB_TEST_SEND_HK": CMD_MID_BASE + defines["IB_TEST_SEND_HK_TOPICID"],
    }
    assert {name: by_name[name].mid for name in expected} == expected


def test_every_function_code_is_the_ccdd_commands() -> None:
    defines = _defines()
    model = load_ccdd(EXAMPLES / "ccdd" / "cfs.json")
    commands = {
        m.name: m.function_code
        for m in model.messages
        if m.kind is MessageKind.COMMAND and m.mid == CMD_MID_BASE + defines["IB_TEST_CMD_TOPICID"]
    }
    codes = {
        f"IB_TEST_{name[len('IB_TEST_') : -len('_CC')]}": value
        for name, value in defines.items()
        if name.endswith("_CC")
    }
    assert codes == commands
    assert codes["IB_TEST_GEN_START"] == 4 and codes["IB_TEST_GEN_STOP"] == 5


def test_the_generator_packet_is_the_size_the_header_asserts(codec: Codec) -> None:
    defines = _defines()
    packet = codec.encode_telemetry("IB_TEST_GEN", {"Serial": 1, "Run": 2, "Fill": bytes(176)}).data
    assert len(packet) == defines["IB_TEST_GEN_PACKET_BYTES"]
    assert len(packet) == 16 + 4 + 4 + defines["IB_TEST_GEN_FILL_BYTES"]
    status = codec.encode_telemetry("IB_TEST_GEN_STATUS", {"State": 1}).data
    assert len(status) == 16 + 20
    start = codec.encode_command("IB_TEST_GEN_START", {"Rate": 10000, "Count": 0}).data
    assert len(start) == 8 + 4 + 4
    assert defines["IB_TEST_GEN_MAX_RATE"] == 10_000  # the plan's design target


def test_the_header_asserts_the_generator_layout_at_compile_time() -> None:
    text = HEADER.read_text()
    for name in (
        "GenStartCmd",
        "GenPayload",
        "GenTlm",
        "GenStatusPayload",
        "GenStatusTlm",
        "GenBurstFitsPipe",
        "GenRateFitsTicks",
    ):
        assert f"IB_TEST_ASSERT_SIZE({name}," in text, name


def test_the_generator_commands_are_dispatched_and_validated_by_the_app() -> None:
    source = SOURCE.read_text()
    for code in ("GEN_START", "GEN_STOP", "GEN_SEND_STATUS"):
        assert f"case IB_TEST_{code}_CC:" in source
    assert 'IB_TEST_Reject("GEN_START rate outside' in source
